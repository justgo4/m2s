#!/usr/bin/env python3
"""Drop one real accepted Merge Commit response on isolated services.

Uses only disposable m2s_e2e_contract. A loopback proxy forwards fixed FE/BE
ports, accepts no arbitrary upstream, and uploads only synthetic counters.
"""
import argparse
from decimal import Decimal
import hashlib
import http.client
import json
from pathlib import Path
import socket
import socketserver
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import j4
import e2e_contract as e2e
from starrocks_contract import configuration,execute,wait_ready


def proxy():
    outcome = dict(armed=False, dropped=False, txn_id=None, errors=[], request_counts={}, dropped_payload=None)
    lock = threading.Lock()
    def handle(client, address, server):
        stream, upstream = client.makefile('rb'), None
        try:
            request = stream.readline(8192).decode('ascii').strip().split()
            if len(request)!=3 or request[0]!='PUT':
                raise ValueError('isolated proxy accepts only stream-load PUT')
            path = request[1]
            port = 8040 if path.startswith('/be/') else 8030
            if port == 8040:
                path = path[3:]
            if not path.startswith('/api/'+e2e.DATABASE+'/') or not path.endswith('/_stream_load'):
                raise ValueError('isolated proxy path is outside disposable stream-load endpoint')
            headers = {}
            for _ in range(100):
                line = stream.readline(8192)
                if line == b'\r\n':
                    break
                name, value = line.decode('ascii').split(':',1)
                headers[name.lower()] = value.strip()
            else:
                raise ValueError('oversized request headers')
            length = int(headers.get('content-length','-1'))
            if not 0 <= length <= 4*1024**2:
                raise ValueError('bounded content-length required')
            if headers.pop('expect','').lower()=='100-continue':
                client.sendall(b'HTTP/1.1 100 Continue\r\n\r\n')
            body = stream.read(length)
            if len(body)!=length:
                raise ValueError('truncated synthetic body')
            headers.pop('host',None)
            headers.pop('connection',None)
            digest = hashlib.sha256(body).hexdigest()
            if port == 8040 and '/events_extra/' in path:
                with lock:
                    counts = outcome['request_counts']
                    counts[digest] = counts.get(digest,0)+1
            upstream = http.client.HTTPConnection('127.0.0.1',port,timeout=90)
            upstream.request('PUT',path,body,headers)
            response = upstream.getresponse()
            payload = response.read()
            result = {}
            try:
                result = json.loads(payload)
            except (ValueError,UnicodeError):
                pass
            with lock:
                drop = (outcome['armed'] and not outcome['dropped'] and
                        '/events_extra/' in path and result.get('Status')=='Success' and
                        result.get('TxnId') is not None)
                if drop:
                    outcome.update(dropped=True,txn_id=int(result['TxnId']),dropped_payload=digest)
            if drop:
                # Server has accepted it. The daemon sees EOF, never TxnId/Label.
                client.shutdown(socket.SHUT_RDWR)
                return
            outgoing = []
            for name,value in response.getheaders():
                if name.lower() in ('content-length','transfer-encoding','connection'):
                    continue
                if name.lower()=='location':
                    location = urlsplit(value)
                    if location.port!=8040 or not location.path.startswith('/api/'+e2e.DATABASE+'/'):
                        raise ValueError('unexpected isolated BE redirect')
                    value = 'http://127.0.0.1:'+str(server.server_address[1])+'/be'+location.path
                outgoing.append(name+': '+value+'\r\n')
            head = ('HTTP/1.1 '+str(response.status)+' '+response.reason+'\r\n'+
                    ''.join(outgoing)+'Content-Length: '+str(len(payload))+'\r\nConnection: close\r\n\r\n')
            client.sendall(head.encode('ascii')+payload)
        except Exception as exc:
            # No raw headers/credentials/payloads in diagnostics.
            with lock:
                outcome['errors'].append(type(exc).__name__)
        finally:
            if upstream is not None:
                upstream.close()
            stream.close()
    server = socketserver.ThreadingTCPServer(('127.0.0.1',0),handle)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever,daemon=True)
    thread.start()
    return server,thread,outcome,lock


def latest_status(directory):
    path = directory/'state.sqlite3.metrics.jsonl'
    if not path.exists():
        return {}
    for line in reversed(path.read_text().splitlines()):
        value = json.loads(line)
        if value.get('event')=='metrics':
            return value.get('state',value)
    return {}


def wait_healthy(proc,directory,source,cfg):
    deadline = time.monotonic()+120
    while time.monotonic()<deadline:
        e2e.live_process(proc)
        expected,actual = e2e.final_result(source,cfg)
        con = sqlite3.connect('file:'+str(directory/'state.sqlite3')+'?mode=ro',uri=True)
        try:
            uncertain = con.execute('SELECT DISTINCT table_name FROM merge_uncertain').fetchall()
            outstanding = con.execute('''SELECT
                (SELECT COUNT(*) FROM active_jobs WHERE table_name NOT IN (SELECT table_name FROM merge_uncertain))+
                (SELECT COUNT(*) FROM deliveries WHERE table_name NOT IN (SELECT table_name FROM merge_uncertain))''').fetchone()[0]
        finally:
            con.close()
        status = latest_status(directory)
        if actual==expected and not outstanding and len(uncertain)==1 and status.get('health')=='degraded':
            return len(actual),status
        time.sleep(.2)
    raise AssertionError('unrelated target did not remain exact and operational after unknown response')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--isolated',action='store_true',required=True)
    parser.add_argument('--output',type=Path,default=Path('benchmark-results/merge-quarantine.json'))
    args = parser.parse_args()
    cfg,options = configuration(),e2e.source_options()
    version = wait_ready(cfg)
    cfg['sr']['database'] = e2e.DATABASE
    execute(cfg,'DROP DATABASE IF EXISTS '+e2e.DATABASE)
    execute(cfg,'CREATE DATABASE '+e2e.DATABASE)
    e2e.create_target(cfg)
    source = j4.pymysql.connect(**options)
    server,thread,outcome,lock = proxy()
    cfg['sr']['http_port'] = server.server_address[1]
    proc,handle = None,None
    try:
        with source.cursor() as cur:
            cur.execute('SELECT @@GLOBAL.gtid_mode')
            gtid_mode = cur.fetchone()[0]
            cur.execute('DROP DATABASE IF EXISTS '+e2e.DATABASE)
            cur.execute('CREATE DATABASE '+e2e.DATABASE+' CHARACTER SET utf8mb4')
            cur.execute('CREATE TABLE '+e2e.DATABASE+'.events(id BIGINT NOT NULL,part INT NOT NULL,v BIGINT,note VARCHAR(64),amount DECIMAL(19,4),active TINYINT NOT NULL,PRIMARY KEY(id,part)) ENGINE=InnoDB')
            cur.executemany('INSERT INTO '+e2e.DATABASE+'.events VALUES(%s,0,%s,%s,%s,1)',
                            [(i,i,' initial🙂 ',Decimal('123.4567')) for i in range(128)])
        with tempfile.TemporaryDirectory(prefix='m2s-unknown-network-') as temp:
            directory = Path(temp)
            env = e2e.setup_catalog(directory,cfg,options,'merge_async',128)
            proc,handle = e2e.start(directory,env,1)
            e2e.wait_started(proc,directory)
            e2e.wait_equal(proc,directory,source,cfg)
            deployment = directory/'extra.sql'
            deployment.write_text('CREATE TABLE starrocks.events_extra AS SELECT id,part,v,note,amount FROM mysql.events WHERE active=1 AND v>=0;')
            installed = subprocess.run([sys.executable,str(ROOT/'j4.py'),'sql',str(deployment)],env=env,capture_output=True,timeout=120)
            if installed.returncode:
                raise AssertionError('synthetic live SQL deployment was not accepted')
            with lock:
                outcome['armed'] = True
            for sequence in range(20):
                e2e.change(source,sequence)
                time.sleep(.08)
            before_rows,status = wait_healthy(proc,directory,source,cfg)
            with lock:
                assert outcome['dropped'] and outcome['txn_id'] is not None, outcome
                txn = outcome['txn_id']
            # Independent positive proof: accepted dropped response later VISIBLE.
            visibility_cfg = dict(cfg,load_timeout=60)
            visible,_ = j4.wait_visible(visibility_cfg,txn,threading.Event())
            assert visible=='visible'
            e2e.stop(proc,handle,kill=True)
            proc,handle = None,None
            for sequence in range(20,40):
                e2e.change(source,sequence)
            proc,handle = e2e.start(directory,env,2)
            e2e.wait_started(proc,directory)
            after_rows,status = wait_healthy(proc,directory,source,cfg)
            e2e.stop(proc,handle)
            proc,handle = None,None
            con = sqlite3.connect('file:'+str(directory/'state.sqlite3')+'?mode=ro',uri=True)
            try:
                retained = con.execute('''SELECT COUNT(*) FROM merge_uncertain u JOIN load_parts p
                    ON u.delivery_id=p.delivery_id AND u.part=p.part WHERE p.txn_id IS NULL AND p.visible=0''').fetchone()[0]
            finally:
                con.close()
            assert retained==1 and len(status.get('quarantined_tables',{}))==1
            with lock:
                assert not outcome['errors'], outcome['errors']
                attempts = outcome['request_counts'][outcome['dropped_payload']]
                assert attempts == 1, 'unknown payload was replayed through the actual BE proxy'
            report = dict(format_version=1,kind='actual_accepted_merge_response_loss_quarantine',
                          protocol='merge_async',gtid_mode=gtid_mode,starrocks_version=version,
                          dropped_accepted_requests=1,independently_confirmed_visible=True,
                          unrelated_target_exact_before_restart=True,unrelated_target_exact_after_restart=True,
                          before_rows=before_rows,after_rows=after_rows,
                          durable_unknown_parts=retained,unknown_payload_not_replayed=True,
                          unknown_payload_forward_attempts=attempts,
                          health='degraded',scope='target_isolation_not_automatic_remote_reconciliation')
            args.output.parent.mkdir(parents=True,exist_ok=True)
            args.output.write_text(json.dumps(report,indent=2)+'\n')
            print(json.dumps(report),flush=True)
    finally:
        if proc is not None:
            e2e.stop(proc,handle,kill=True)
        server.shutdown()
        server.server_close()
        thread.join(5)
        source.close()
        execute(cfg,'DROP DATABASE IF EXISTS '+e2e.DATABASE)


if __name__=='__main__':
    main()
