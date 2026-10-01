#!/usr/bin/env python3
"""Offline fault/recovery regression suite for j4."""

from unittest.mock import MagicMock

import j4 as cdc

# Keep the historical tests compact: runtime functions/constants stay owned by
# j4, while test-local names resolve exactly as before. Fault
# injection writes through vars(cdc), so imported runtime functions see patches.
globals().update({
    name:value for name,value in vars(cdc).items()
    if not name.startswith("__")
})

def transaction_selftest(directory, engine, mapping, cfg):
    """Fault injection at the HTTP boundary; no real database credentials required."""
    from urllib.parse import urlsplit, parse_qs
    cfg = dict(cfg,txn_rows=1000,txn_bytes=65536,max_inflight_deliveries=8,commit_interval_ms=2000,
               max_prepared_bytes=1024**2,load_timeout=10,retry_max=3,pressure_max_seconds=1,
               sr=dict(host="test",http_port=8030,database="test",user="test",password="test"))
    original = vars(cdc)["curl_request"]
    for fault in ("none","begin","load","prepare","commit","pressure","load_timeout","expired"):
        con = init_state(os.path.join(directory,"txn_"+fault+".sqlite3"))
        bootstrap(con,"test","source",("binlog.000001",4),["t"])
        assert stage_snapshot(con,mapping,[{"id":i,"v":i} for i in range(12)],(11,),True,
                              ("binlog.000001",4),cfg)
        with tempfile.SpooledTemporaryFile() as spool:
            spool_rows(spool,mapping,[(1,{"id":3,"v":3}),(0,{"id":5,"v":99})],cfg)
            commit_spool(con,spool,("binlog.000001",20),time.time(),{"t":mapping})
        delivery = claim_table_delivery(con,"t",cfg)
        assert con.execute("""
            SELECT COUNT(DISTINCT j.lane)
            FROM active_jobs j JOIN job_assignments a ON a.job_id=j.id
            WHERE a.delivery_id=?
        """,(delivery,)).fetchone()[0] > 1
        assert con.execute("""
            SELECT COUNT(DISTINCT j.kind)
            FROM active_jobs j JOIN job_assignments a ON a.job_id=j.id
            WHERE a.delivery_id=?
        """,(delivery,)).fetchone()[0] == 2
        prepare_delivery(con,engine,mapping,delivery,cfg)
        remote, sink, trace = {}, {}, []
        injected = False

        def fake_http(handle, config, url, payload=None, headers=None, stop=None, method=None):
            nonlocal injected
            path = urlsplit(url).path
            if path.endswith("get_load_state"):
                label = parse_qs(urlsplit(url).query)["label"][0]
                tx = remote.get(label)
                if tx and tx["state"] == "COMMITTED":
                    for row in tx["rows"]:
                        row = dict(row)
                        if row.pop("__op"):
                            sink.pop(row["id"],None)
                        else:
                            sink[row["id"]] = row
                    tx["state"] = "VISIBLE"
                return 200,dict(state=tx["state"] if tx else "UNKNOWN",
                                reason=tx.get("reason","") if tx else "")
            op,label = path.rsplit("/",1)[-1],headers["label"]
            assert "enable_merge_commit" not in headers
            assert method == ("PUT" if op == "load" else "POST")
            trace.append((label,op))
            if op == "begin":
                assert label not in remote
                remote[label] = dict(state="PREPARE",rows=[],payloads=[])
            tx = remote[label]
            if op == "load":
                assert tx["state"] == "PREPARE"
                # Accept the bytes BEFORE losing the reply: replay here would duplicate data.
                assert payload not in tx["payloads"],"possibly accepted load was replayed"
                tx["payloads"].append(payload)
                raw = gzip.decompress(payload) if cfg["compression"] else payload
                tx["rows"].extend(orjson.loads(line) for line in raw.splitlines())
                if fault == "pressure" and not injected:
                    injected = True
                    tx.update(state="ABORTED",reason="too many versions current/limit: 1002/1000")
                    return 200,dict(Status="INTERNAL_ERROR",Message=tx["reason"])
                if fault == "load_timeout" and not injected:
                    injected = True
                    raise pycurl.error(28,"injected response timeout")
            elif op == "prepare":
                assert tx["state"] == "PREPARE"
                tx["state"] = "PREPARED"
            elif op == "commit":
                assert tx["state"] in ("PREPARED","COMMITTED","VISIBLE")
                tx["state"] = "COMMITTED"
            elif op == "rollback":
                assert tx["state"] in ("PREPARE","PREPARED","ABORTED")
                tx["state"] = "ABORTED"
            if not injected and (op == fault or (fault == "expired" and op == "commit")):
                injected = True
                if fault == "expired":
                    remote.clear()
                raise KeyboardInterrupt("injected process crash after server accepted "+op)
            result = dict(Status="OK",TxnId=123)
            if op == "prepare":
                result.update(NumberLoadedRows=len(tx["rows"]),NumberFilteredRows=0,NumberUnselectedRows=0)
            return 200,result

        runtime = dict(stop=threading.Event(),pressure_until={"t":0},table_interval={"t":2})
        vars(cdc)["curl_request"] = fake_http
        try:
            try:
                load_transaction(None,con,mapping,delivery,cfg,runtime)
            except KeyboardInterrupt:
                con.close()
                con = open_state(os.path.join(directory,"txn_"+fault+".sqlite3"))
                assert claim_table_delivery(con,"t",cfg) == delivery
                if fault == "expired":
                    try:
                        load_transaction(None,con,mapping,delivery,cfg,runtime)
                    except ValueError:
                        assert con.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] > 0
                    else:
                        raise AssertionError("missing transaction identity must stop replay")
                    continue
                load_transaction(None,con,mapping,delivery,cfg,runtime)
            expected = {i:{"id":i,"v":99 if i == 5 else i} for i in range(12) if i != 3}
            assert sink == expected,(fault,sink)
            assert sum(tx["state"] == "VISIBLE" for tx in remote.values()) == 1
            remote.clear()
            load_transaction(None,con,mapping,delivery,cfg,runtime)
            acknowledge_delivery(con,delivery)
            assert not con.execute("SELECT 1 FROM active_jobs").fetchone()
            assert con.execute("SELECT snapshot_done FROM table_state").fetchone()[0] == 1
        finally:
            vars(cdc)["curl_request"] = original
            con.close()


def transaction_http_selftest(mapping):
    """Exercise actual libcurl POST/PUT/GET and a 307 redirect on loopback only."""
    import socket
    server = socket.socket()
    server.bind(("127.0.0.1",0))
    server.listen(4)
    server.settimeout(0.2)
    port = server.getsockname()[1]
    done,errors,requests = threading.Event(),[],[]

    def serve():
        try:
            while not done.is_set() and len(requests) < 4:
                try:
                    client,_ = server.accept()
                except socket.timeout:
                    continue
                with client:
                    client.settimeout(3)
                    raw = b""
                    while b"\r\n\r\n" not in raw:
                        chunk = client.recv(4096)
                        if not chunk:
                            raise AssertionError("incomplete HTTP headers")
                        raw += chunk
                    head,body = raw.split(b"\r\n\r\n",1)
                    lines = head.decode().split("\r\n")
                    method,path,_ = lines[0].split()
                    headers = dict((k.lower(),v.strip()) for k,v in
                                   (line.split(":",1) for line in lines[1:]))
                    if headers.get("expect","").lower() == "100-continue":
                        client.sendall(b"HTTP/1.1 100 Continue\r\n\r\n")
                    length = int(headers.get("content-length",0))
                    while len(body) < length:
                        chunk = client.recv(length-len(body))
                        if not chunk:
                            raise AssertionError("incomplete HTTP body")
                        body += chunk
                    requests.append((method,path,headers,body))
                    if path == "/api/transaction/begin":
                        reply = (f"HTTP/1.1 307 Temporary Redirect\r\nLocation: http://127.0.0.1:{port}/be/begin\r\n"
                                 "Content-Length: 0\r\nConnection: close\r\n\r\n").encode()
                    else:
                        payload = orjson.dumps(dict(state="VISIBLE") if "get_load_state" in path else
                                               dict(Status="OK",TxnId=123))
                        reply = (f"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                                 f"Content-Length: {len(payload)}\r\nConnection: close\r\n\r\n").encode()+payload
                    client.sendall(reply)
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=serve)
    thread.start()
    handle = pycurl.Curl()
    cfg = dict(sr=dict(host="127.0.0.1",http_port=port,user="test",password="test",database="test"),
               load_timeout=5,compression="gzip")
    try:
        transaction_request(handle,cfg,mapping,"test_label","begin",done)
        wire = gzip.compress(b'{"id":1,"v":2,"__op":0}\n',mtime=0)
        transaction_request(handle,cfg,mapping,"test_label","load",done,wire)
        assert transaction_state(handle,cfg,"test_label",done)[0] == "VISIBLE"
    finally:
        done.set()
        thread.join(timeout=4)
        handle.close()
        server.close()
    assert not errors,errors
    assert [r[0] for r in requests] == ["POST","POST","PUT","GET"],requests
    assert requests[2][3] == wire
    assert requests[0][2]["authorization"] == requests[1][2]["authorization"]
    assert requests[2][2]["compression"] == "gzip"



def merge_async_selftest(directory, engine, mapping, cfg):
    from unittest.mock import patch
    cfg = dict(cfg,merge_commit_interval_ms=250,merge_commit_parallel=2,
               load_timeout=5,retry_max=3,pressure_max_seconds=2,
               sr=dict(host="test",http_port=8030,database="test",user="test",password="test"))
    path = os.path.join(directory,"merge_async.sqlite3")
    con = init_state(path)
    bootstrap(con,"test","source",("binlog.000001",4),["t"])
    rows = [{"id":i,"v":i} for i in range(20)]
    assert stage_snapshot(con,mapping,rows,(19,),True,("binlog.000001",4),cfg)
    original_http = vars(cdc)["curl_request"]
    original_wait = vars(cdc)["wait_visible"]
    requests = []
    try:
        def fake_http(handle, config, url, payload=None, headers=None, stop=None, method=None):
            assert url.endswith("/api/test/t/_stream_load")
            assert headers["enable_merge_commit"] == "true"
            assert headers["merge_commit_async"] == "true"
            assert headers["merge_commit_interval_ms"] == "250"
            assert headers["merge_commit_parallel"] == "2"
            assert headers["label"].startswith("cdc_")
            requests.append((headers["label"],payload))
            return 200,dict(Status="Success",TxnId=987,Label="merge_server_label",
                            RequestId=headers["label"],LoadBytes=len(payload),LeftMergeTimeMs=100)

        def fake_wait(config, txn_id, stop):
            assert txn_id == 987
            return "visible",{"TransactionStatus":"VISIBLE"}

        vars(cdc)["curl_request"] = fake_http
        vars(cdc)["wait_visible"] = fake_wait
        runtime = dict(stop=threading.Event(),pressure_until={"t":0},
                       control_lock=threading.Lock(),active_writers={"t":2},
                       last_pressure={"t":0},last_scale={"t":0},load_events={},
                       version_recovery={"t":False},version_recovery_good={"t":0})
        while True:
            row = con.execute("""
                SELECT j.lane
                FROM active_jobs j LEFT JOIN job_assignments a ON a.job_id=j.id
                WHERE a.job_id IS NULL ORDER BY j.id LIMIT 1
            """).fetchone()
            if not row:
                break
            delivery = claim_delivery(con,"t",row[0],cfg)
            prepare_delivery(con,engine,mapping,delivery,cfg)
            merge_async_delivery(None,con,mapping,delivery,cfg,runtime)
            assert con.execute(
                "SELECT COUNT(*) FROM load_parts WHERE delivery_id=? AND visible=0",(delivery,)).fetchone()[0] == 0
            acknowledge_delivery(con,delivery)
        assert requests
        assert not con.execute("SELECT 1 FROM active_jobs").fetchone()
        assert con.execute("SELECT snapshot_done FROM table_state").fetchone()[0] == 1

        uncertain_path = os.path.join(directory,"merge_uncertain.sqlite3")
        uncertain = init_state(uncertain_path)
        bootstrap(uncertain,"test","source",("binlog.000001",4),["t"])
        with tempfile.SpooledTemporaryFile() as spool:
            spool_rows(spool,mapping,[(0,{"id":99,"v":99})],cfg,engine)
            commit_spool(uncertain,spool,("binlog.000001",40),time.time(),{"t":mapping})
        uncertain_lane = uncertain.execute(
            "SELECT lane FROM jobs WHERE table_name='t' ORDER BY id LIMIT 1").fetchone()[0]
        uncertain_delivery = claim_delivery(uncertain,"t",uncertain_lane,cfg)
        prepare_delivery(uncertain,engine,mapping,uncertain_delivery,cfg)

        attempts = []
        def lost_response(handle, config, url, payload=None, headers=None, stop=None, method=None):
            attempts.append(headers["label"])
            raise pycurl.error(pycurl.E_RECV_ERROR,"response lost after request body")

        vars(cdc)["curl_request"] = lost_response
        try:
            merge_async_delivery(None,uncertain,mapping,uncertain_delivery,cfg,runtime)
        except RuntimeError:
            pass
        else:
            raise AssertionError("ambiguous Merge Commit response must fail closed")
        pending = unresolved_merge_uncertain(uncertain)
        assert len(pending) == 1,pending
        assert pending[0][0] == uncertain_delivery and pending[0][1] == 0
        assert len(attempts) == 1,"ambiguous request must never be auto-replayed"
        uncertain.close()
        uncertain = init_state(uncertain_path)
        assert len(unresolved_merge_uncertain(uncertain)) == 1,"uncertainty marker must survive restart"
        uncertain.close()

        retry_path = os.path.join(directory,"merge_presend_retry.sqlite3")
        retry_db = init_state(retry_path)
        bootstrap(retry_db,"test","source",("binlog.000001",4),["t"])
        with tempfile.SpooledTemporaryFile() as spool:
            spool_rows(spool,mapping,[(0,{"id":100,"v":100})],cfg,engine)
            commit_spool(retry_db,spool,("binlog.000001",50),time.time(),{"t":mapping})
        retry_lane = retry_db.execute(
            "SELECT lane FROM jobs WHERE table_name='t' ORDER BY id LIMIT 1").fetchone()[0]
        retry_delivery = claim_delivery(retry_db,"t",retry_lane,cfg)
        prepare_delivery(retry_db,engine,mapping,retry_delivery,cfg)
        retry_count = 0
        def connect_then_success(handle, config, url, payload=None, headers=None, stop=None, method=None):
            nonlocal retry_count
            retry_count += 1
            if retry_count <= cfg["retry_max"]+1:
                raise pycurl.error(pycurl.E_COULDNT_CONNECT,"connection refused before request")
            return 200,dict(Status="Success",TxnId=654,Label="merge_server_retry",
                            LeftMergeTimeMs=0)
        def retry_wait(config, txn_id, stop):
            assert txn_id == 654
            return "visible",{"TransactionStatus":"VISIBLE"}
        vars(cdc)["curl_request"] = connect_then_success
        vars(cdc)["wait_visible"] = retry_wait
        with patch.object(runtime["stop"],"wait",return_value=False):
            merge_async_delivery(None,retry_db,mapping,retry_delivery,cfg,runtime)
        assert retry_count == cfg["retry_max"]+2,"safe connection failures must outlive the old retry budget"
        assert not unresolved_merge_uncertain(retry_db),"successful safe retry must clear request marker"

        # A graceful stop in the middle of an accepted upload must still persist its identity.
        con.execute(
            "INSERT INTO deliveries(id,table_name,lane,prepared) "
            "VALUES('shutdown','t',0,1)")
        con.execute("""INSERT INTO load_parts(
            delivery_id,part,label,payload,nrows,visible,txn_id,json_bytes
        ) VALUES('shutdown',0,'cdc_shutdown',?,1,0,NULL,2)""",(b'{}',))
        def shutdown_response(handle, config, url, payload=None, headers=None, stop=None, method=None):
            assert stop is None,"do not abort an in-flight Merge Commit request on graceful shutdown"
            runtime["stop"].set()
            return 200,dict(Status="Success",TxnId=765,Label="merge_shutdown")
        vars(cdc)["curl_request"] = shutdown_response
        submit_merge_async(None,con,mapping,"shutdown",0,cfg,runtime["stop"],runtime)
        assert con.execute("SELECT txn_id FROM load_parts WHERE delivery_id='shutdown'").fetchone()[0] == 765
        assert not unresolved_merge_uncertain(con),"graceful shutdown must preserve known identity"
        retry_db.close()
    finally:
        vars(cdc)["curl_request"] = original_http
        vars(cdc)["wait_visible"] = original_wait
        con.close()


def recovery_selftest(directory, mapping, cfg):
    """Exact history recovery, transient disconnects, retained unknowns and process signals."""
    from unittest.mock import MagicMock, patch
    cfg = dict(cfg,load_timeout=0,sr=dict(database="test"))
    txn_id = 68333118
    missing = pymysql.err.ProgrammingError(1064,f"transaction with id {txn_id} does not exist.")
    finished = ("merge_commit_test","FINISHED",orjson.dumps(dict(txn_id=txn_id)),None)
    cancelled = ("merge_commit_test","CANCELLED",orjson.dumps(dict(txn_id=txn_id)),"too many versions")

    def exercise(show, live=(), history=(), expected="visible", disconnect=False, deliveries=False):
        stop,queries,waits = threading.Event(),[],[]
        connection,cursor = MagicMock(),MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor
        cursor.description = [("TransactionStatus",)]

        def execute(sql, params):
            queries.append((sql,params))
            if sql.startswith("SHOW TRANSACTION"):
                result = show(len(waits)) if callable(show) else show
                if isinstance(result,Exception):
                    raise result
                cursor.fetchone.return_value = result
            else:
                assert params == ("test",str(txn_id)),"history must scope DB and exact transaction ID"
                assert "get_json_string" in sql and "LIKE" not in sql
                result = history if "_statistics_" in sql else live
                result = result(len(waits)) if callable(result) else result
                if isinstance(result,Exception):
                    raise result
                cursor.fetchall.return_value = result

        def wait(delay):
            waits.append(delay)
            if len(waits) >= 3:
                stop.set()
            return stop.is_set()

        cursor.execute.side_effect = execute
        with patch.dict(vars(cdc),mysql_connect=MagicMock(return_value=connection)), \
                patch.object(stop,"wait",side_effect=wait), \
                patch.dict(vars(cdc),curl_request=MagicMock(side_effect=AssertionError("must not resend"))):
            if disconnect:
                vars(cdc)["mysql_connect"].side_effect = [
                    pymysql.err.OperationalError(2003,"connection refused"),connection]
            if deliveries:
                path = os.path.join(directory,"expired_merge_restart.sqlite3")
                con = init_state(path)
                bootstrap(con,"test","source",("binlog.000001",4),["t"])
                for lane in (1,3,6,8,9):
                    delivery = "expired_"+str(lane)
                    con.execute(
                        "INSERT INTO deliveries(id,table_name,lane,prepared) "
                        "VALUES(?,?,?,1)",(delivery,"t",lane))
                    con.execute("""INSERT INTO load_parts(
                        delivery_id,part,label,payload,nrows,visible,txn_id,json_bytes
                    ) VALUES(?,0,?,?,1,0,?,2)""",(delivery,delivery,b'{}',txn_id))
                    job_id = con.execute("""INSERT INTO jobs(
                                table_name,lane,kind,payload,nrows,source_file,source_pos,created
                            ) VALUES(?,?,'cdc',?,1,?,100,?) RETURNING id""",
                            ("t",lane,b'{}',"binlog.000001",time.time())).fetchone()[0]
                    con.execute(
                        "INSERT INTO job_assignments(job_id,delivery_id) VALUES(?,?)",
                        (job_id,delivery))
                con.close()
                con = init_state(path)
                try:
                    for delivery, in con.execute("SELECT id FROM deliveries").fetchall():
                        merge_async_delivery(None,con,mapping,delivery,cfg,dict(stop=stop))
                        acknowledge_delivery(con,delivery)
                    assert con.execute("SELECT COUNT(*) FROM active_jobs").fetchone()[0] == 0
                    assert con.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0] == 0
                    assert con.execute("SELECT COUNT(*) FROM applied WHERE source_pos=100").fetchone()[0] == 5
                finally:
                    con.close()
            else:
                try:
                    state,detail = wait_visible(cfg,txn_id,stop)
                except RuntimeError:
                    assert expected == "pending" and stop.is_set()
                else:
                    assert state == expected,(state,expected)
                    if state == "aborted" and history:
                        assert "too many versions" in detail["Message"]
            vars(cdc)["curl_request"].assert_not_called()
        return queries,waits

    queries,_ = exercise(("VISIBLE",))
    assert len(queries) == 1,"normal path must not scan history"
    exercise(("ABORTED",),expected="aborted")
    exercise(missing,history=[finished],deliveries=True)
    exercise(None,live=[finished])
    exercise(missing,history=[cancelled],expected="aborted")
    exercise(("VISIBLE",),disconnect=True)
    exercise(lambda n: pymysql.err.OperationalError(2013,"connection lost") if n == 0 else ("VISIBLE",))
    _,waits = exercise(lambda n: ("COMMITTED",) if n == 0 else ("VISIBLE",))
    assert waits,"visibility timeout must poll without aborting or resending"
    exercise(missing,history=lambda n: [] if n == 0 else [finished])
    exercise(missing,live=pymysql.err.OperationalError(1142,"access denied"),history=[finished])
    exercise(missing,history=[],expected="pending")
    exercise(missing,history=[("wrong","FINISHED",orjson.dumps(dict(txn_id=683331180)),None)],expected="pending")
    exercise(missing,history=[("pending","COMMITED",finished[2],None)],expected="pending")
    exercise(missing,history=[finished,cancelled],expected="pending")
    exercise(missing,history=pymysql.err.ProgrammingError(1146,"history unavailable"),expected="pending")
    try:
        exercise(pymysql.err.ProgrammingError(1064,"syntax error unrelated to missing transaction"))
    except pymysql.err.ProgrammingError:
        pass
    else:
        raise AssertionError("unrelated SQL syntax errors must not be swallowed")

    previous = {sig:signal.getsignal(sig) for sig in (signal.SIGINT,signal.SIGTERM)}
    with process_signals() as control:
        if hasattr(signal,"SIGHUP"):
            os.kill(os.getpid(),signal.SIGHUP)
            assert not signal_stop_requested(control)
        os.kill(os.getpid(),signal.SIGTERM)
        assert signal_stop_requested(control) and control["reason"] == "signal=SIGTERM"
    assert all(signal.getsignal(sig) == handler for sig,handler in previous.items())
    with process_signals() as control:
        os.kill(os.getpid(),signal.SIGINT)
        assert signal_stop_requested(control) and control["reason"] == "signal=SIGINT"

    runtime = dict(stop=threading.Event(),errors=[],error_lock=threading.Lock())
    worker = MagicMock(side_effect=[pymysql.err.OperationalError(2013,"snapshot connection lost"),None])
    with patch.dict(vars(cdc),snapshot_worker=worker),patch.object(runtime["stop"],"wait",return_value=False):
        guarded_worker(worker,runtime,mapping,cfg)
    assert worker.call_count == 2 and not runtime["errors"] and not runtime["stop"].is_set()


def selftest():
    """Offline regression suite: real DuckDB/Arrow/SQLite, simulated idempotent sink."""
    import random
    passed = []

    busy = resource_auto_defaults(16,65536,32768,8.0)
    idle = resource_auto_defaults(16,65536,49152,0.0)
    assert busy["cpu_cap"] == 8 and busy["cpu_target"] == 4
    assert busy["memory_mb"] == 4096 and busy["memory_reserve_mb"] == 16384
    assert idle["cpu_target"] == 8 and idle["memory_mb"] == 8192
    assert busy["ionice"] == "best_effort"
    frozen_names = (
        "CDC_RELEASE_RESOURCE_FROZEN","CDC_RELEASE_CPU_CAP",
        "CDC_RELEASE_CPU_TARGET","CDC_RELEASE_CPU_SOURCE",
        "CDC_RELEASE_MEMORY_MB","CDC_RELEASE_MEMORY_SOURCE",
        "CDC_RELEASE_DETECTED_CPU_COUNT","CDC_RELEASE_CPU_RESERVE_CORES")
    frozen_old = {name:os.environ.get(name) for name in frozen_names}
    try:
        os.environ["CDC_RELEASE_RESOURCE_FROZEN"] = "1"
        os.environ["CDC_RELEASE_CPU_CAP"] = "3"
        os.environ["CDC_RELEASE_CPU_TARGET"] = "2"
        os.environ["CDC_RELEASE_CPU_SOURCE"] = "auto"
        os.environ["CDC_RELEASE_MEMORY_MB"] = "1024"
        os.environ["CDC_RELEASE_MEMORY_SOURCE"] = "auto"
        os.environ["CDC_RELEASE_DETECTED_CPU_COUNT"] = str(max(3,len(available_cpu_ids())))
        os.environ["CDC_RELEASE_CPU_RESERVE_CORES"] = "1"
        frozen_probe = read_resource_policy()
        assert frozen_probe["cpu_cap"] <= 3 and frozen_probe["cpu_target"] <= 2
        assert frozen_probe["cpu_capacity_count"] == max(3,len(available_cpu_ids()))
        assert frozen_probe["cpu_source"] == "auto"
        expected_frozen_memory = min(
            1024,system_memory_stats()["total_mb"] or 1024)
        assert frozen_probe["memory_mb"] == expected_frozen_memory
        assert frozen_probe["memory_source"] == "auto"
        assert frozen_probe["cpu_reserve_cores"] == 1
    finally:
        for name,value in frozen_old.items():
            if value is None:
                os.environ.pop(name,None)
            else:
                os.environ[name] = value
    assert runtime_cpu_target(16,8,4,6.0) == 6
    assert memory_limit_bytes("1GB") == 1024**3
    assert memory_limit_bytes("512MiB") == 512*1024**2
    budget_cfg = dict(
        resource=dict(cpu_cap=1,cpu_target=1,memory_mb=1024),
        writer_min=1,writer_initial=4,writer_max=8,
        snapshot_workers=4,merge_commit_parallel=4,
        max_inflight_deliveries=8,max_prepared_bytes=1 << 60,
        duckdb_memory="512MB",load_mode="merge_async")
    enforce_resource_config(budget_cfg,1)
    assert budget_cfg["writer_max"] == 1
    assert budget_cfg["snapshot_workers"] == 1
    assert budget_cfg["duckdb_engine_slots"] == 4
    assert budget_cfg["duckdb_memory"] == "128MB"
    assert budget_cfg["max_prepared_bytes"] == 128*1024**2
    assert budget_cfg["duckdb_memory_requested_bytes"] == 512*1024**2

    production_budget_cfg = dict(
        resource=dict(cpu_cap=8,cpu_target=8,memory_mb=6708),
        writer_min=1,writer_initial=4,writer_max=8,
        snapshot_workers=2,merge_commit_parallel=2,
        max_inflight_deliveries=16,max_prepared_bytes=512*1024**2,
        duckdb_memory="256MB",load_mode="merge_async")
    enforce_resource_config(production_budget_cfg,1)
    assert production_budget_cfg["writer_max"] == 5
    assert production_budget_cfg["writer_initial"] == 4
    assert production_budget_cfg["duckdb_engine_slots"] == 13
    assert production_budget_cfg["duckdb_memory"] == "256MB"
    assert production_budget_cfg["duckdb_memory_writer_cap"] == 5
    assert duckdb_merge_writer_cap(production_budget_cfg,2) == 2
    assert duckdb_oom_recovery_memory_bytes(
        production_budget_cfg) == 512*1024**2

    bundle_cfg = dict(
        production_budget_cfg,key_partitions=16,
        snapshot_bundle_max_lanes=8,batch_bytes=16*1024**2)
    bundle_runtime = dict(
        control_lock=threading.Lock(),
        active_writers={"oom_table":4},
        resource_writer_cap=4,
        snapshot_transform_bytes_cap={"oom_table":16*1024**2})
    assert snapshot_bundle_width(bundle_cfg,bundle_runtime,"oom_table") == 4
    bundle_runtime["active_writers"]["oom_table"] = 2
    assert snapshot_bundle_width(bundle_cfg,bundle_runtime,"oom_table") == 4
    cap = snapshot_transform_oom_backoff(
        bundle_cfg,bundle_runtime,"oom_table",8*1024**2,2*1024**2)
    assert cap == 4*1024**2
    cap = snapshot_transform_oom_backoff(
        bundle_cfg,bundle_runtime,"oom_table",4*1024**2,2*1024**2)
    assert cap == 2*1024**2

    oom_path = os.path.join(
        tempfile.gettempdir(),"cdc_duckdb_oom_recovery_selftest.sqlite3")
    for suffix in ("","-wal","-shm"):
        with contextlib.suppress(FileNotFoundError):
            os.remove(oom_path+suffix)
    oom_db = init_state(oom_path)
    bootstrap(
        oom_db,"duckdb-oom-selftest","source",
        ("binlog.000001",4),["oom_table"])
    now = time.time()
    for lane,nrows,nbytes in (
            (0,10,100),(0,20,200),(1,30,300),(2,40,400)):
        oom_db.execute("""
            INSERT INTO jobs(
                table_name,lane,kind,payload,nrows,logical_bytes,
                plan_version,created)
            VALUES('oom_table',?,'snapshot',X'00',?,?,1,?)
        """,(lane,nrows,nbytes,now))
    job_ids = [
        int(row[0]) for row in oom_db.execute(
            "SELECT id FROM jobs WHERE table_name='oom_table' ORDER BY id")]
    oom_db.execute("""
        INSERT INTO deliveries(id,table_name,lane,plan_version,prepared)
        VALUES('oom_delivery','oom_table',0,1,0)
    """)
    assign_jobs(oom_db,"oom_delivery",job_ids)
    oom_db.execute(
        "INSERT INTO prepare_reservations VALUES('oom_delivery',1000)")
    oom_db.execute("""
        INSERT INTO prepare_requirements(
            delivery_id,required_bytes,full_prepare_attempts,updated)
        VALUES('oom_delivery',1000,1,?)
    """,(now,))
    first_shrink = shrink_unprepared_delivery(oom_db,"oom_delivery")
    assert first_shrink["shrunk"]
    assert first_shrink["mode"] == "drop_secondary_lanes"
    assert first_shrink["jobs_before"] == 4 and first_shrink["jobs_after"] == 2
    assert delivery_lanes(oom_db,"oom_delivery") == [0]
    assert oom_db.execute(
        "SELECT COUNT(*) FROM prepare_reservations").fetchone()[0] == 0
    assert oom_db.execute(
        "SELECT COUNT(*) FROM prepare_requirements").fetchone()[0] == 0
    second_shrink = shrink_unprepared_delivery(oom_db,"oom_delivery")
    assert second_shrink["shrunk"]
    assert second_shrink["mode"] == "halve_owner_prefix"
    assert second_shrink["jobs_before"] == 2 and second_shrink["jobs_after"] == 1
    final_shrink = shrink_unprepared_delivery(oom_db,"oom_delivery")
    assert not final_shrink["shrunk"] and final_shrink["reason"] == "single_job"
    assert oom_db.execute("""
        SELECT COUNT(*) FROM active_jobs j
        LEFT JOIN job_assignments a ON a.job_id=j.id
        WHERE j.table_name='oom_table' AND a.job_id IS NULL
    """).fetchone()[0] == 3
    oom_db.close()
    passed.append(
        "DuckDB OOM recovery shrinks only unsent durable delivery prefixes, "
        "adapts snapshot transform bytes, and preserves queued work")

    passed.append(
        "resource budget preserves per-engine DuckDB working set before writer concurrency")

    passed.append(
        "resource budget separates startup CPU capacity and bounds aggregate hot-plan DuckDB memory")

    zero_mem,zero_disk = resource_pressure_flags(
        0,True,1024,0,True,4096,0.0,True,4096)
    unknown_mem,unknown_disk = resource_pressure_flags(
        8192,False,1024,10**12,False,4096,1.0,True,4096)
    assert zero_mem and zero_disk and unknown_mem and unknown_disk
    tree_stats = process_tree_rss_stats()
    assert tree_stats["known"] and tree_stats["rss_mb"] > 0
    metric_probe = metric_bucket()
    for index in range(METRIC_SAMPLE_LIMIT*3):
        metric_sample_add(metric_probe,"visible_seconds",float(index))
    assert len(metric_probe["visible_seconds"]) == METRIC_SAMPLE_LIMIT
    metric_stats = metric_sample_summary(metric_probe,"visible_seconds")
    assert metric_stats["scope"] == "recent_window"
    assert metric_stats["total_n"] == METRIC_SAMPLE_LIMIT*3
    assert metric_stats["total_max"] == float(METRIC_SAMPLE_LIMIT*3-1)
    assert metric_stats["total_avg"] == float(METRIC_SAMPLE_LIMIT*3-1)/2
    assert metric_probe["visible_seconds"][0] == float(METRIC_SAMPLE_LIMIT*2)
    metric_probe["snapshot_read_rows"] = 100
    metric_probe["snapshot_source_rows"] = 250
    metric_probe["snapshot_read_seconds_total"] = 2.0
    metric_summary = metric_bucket_summary(metric_probe,False)
    assert metric_summary["snapshot_read_rows_per_second"] == 50
    assert metric_summary["snapshot_read_amplification"] == 2.5
    class CloseProbe:
        def __init__(self):
            self.closed = 0
        def close(self):
            self.closed += 1
    close_probe = CloseProbe()
    close_runtime = {"stream":{"con":close_probe}}
    close_stream = close_runtime.get("stream")
    close_runtime["stream"] = None
    replication_close_stream(close_stream)
    assert close_probe.closed == 1 and close_runtime["stream"] is None
    passed.append("graceful stop closes the real replication connection")

    def check(condition, message):
        if not condition:
            raise AssertionError(message)

    def rejects(function, message):
        try:
            function()
        except (ValueError,RuntimeError,KeyError):
            return
        raise AssertionError(message)

    cfg = dict(key_partitions=3,batch_rows=100,batch_bytes=512,max_row_bytes=4096,
               compression="gzip",duckdb_memory="64MB",batch_ms=0,txn_rows=100,txn_bytes=4096,
               max_inflight_deliveries=8,max_prepared_bytes=1024**2,
               snapshot_rows=50000,snapshot_chunk_bytes=256*1024,
               freshness_seconds=2,txn_spool_max_bytes=1024,
               state=os.path.join(tempfile.gettempdir(),"cdc_selftest_state.sqlite3"),
               min_free_bytes=0)
    reservation_path = os.path.join(tempfile.gettempdir(),"cdc_prepare_reservation_selftest.sqlite3")
    for suffix in ("","-wal","-shm"):
        with contextlib.suppress(FileNotFoundError):
            os.remove(reservation_path+suffix)
    reservation_db = init_state(reservation_path)
    bootstrap(reservation_db,"reservation-test","source",("binlog.000001",4),["t"])
    reservation_cfg = dict(cfg,max_prepared_bytes=100,batch_bytes=60)
    with state_transaction(reservation_db):
        reservation_db.execute(
            "INSERT INTO deliveries(id,table_name,lane) VALUES('r1','t',0)")
        check(
            prepare_reservation_set_locked(reservation_db,"r1",60,reservation_cfg),
            "first prepare reservation must fit")
        reservation_db.execute(
            "INSERT INTO deliveries(id,table_name,lane) VALUES('r2','t',1)")
        check(
            not prepare_reservation_set_locked(reservation_db,"r2",60,reservation_cfg),
            "concurrent reservation must honor the global byte ceiling")
    reservation_db.close()
    reservation_db = init_state(reservation_path)
    check(
        prepared_budget_used(reservation_db) == 60,
        "prepare reservation must survive restart")
    with state_transaction(reservation_db):
        reservation_db.execute(
            "DELETE FROM prepare_reservations WHERE delivery_id='r1'")
        check(
            prepare_reservation_set_locked(reservation_db,"r2",80,reservation_cfg),
            "released reservation must make capacity immediately reusable")
    reservation_db.close()
    passed.append("prepared payload reservations are durable and globally byte-bounded")

    mapping = dict(src_table="t",sr_table="t",primary_key="id",
                   sql="SELECT id,v FROM arrow_batch",full_filter="v >= 0",
                   _schema=[("id",pa.int64()),("v",pa.int64())],
                   _schema_signature=[
                       ("id","bigint","bigint",None,None,False),
                       ("v","bigint","bigint",None,None,True),
                   ],
                   _target_sequence=False,_output_columns=["id","v"])
    validate_mapping(mapping)
    engine = transform_engine(cfg)

    known_wait_path = os.path.join(
        tempfile.gettempdir(),"cdc_prepare_known_wait_selftest.sqlite3")
    for suffix in ("","-wal","-shm"):
        with contextlib.suppress(FileNotFoundError):
            os.remove(known_wait_path+suffix)
    known_wait = init_state(known_wait_path)
    bootstrap(known_wait,"known-wait","source",("binlog.000001",4),["t"])
    known_cfg = dict(cfg,max_prepared_bytes=100,batch_bytes=20)
    with state_transaction(known_wait):
        known_wait.execute(
            "INSERT INTO deliveries(id,table_name,lane,prepared) "
            "VALUES('holder','t',0,1)")
        known_wait.execute("""
            INSERT INTO load_parts(
                delivery_id,part,label,payload,nrows,visible,json_bytes)
            VALUES('holder',0,'holder',?,1,0,60)
        """,(b"x"*60,))
        known_wait.execute(
            "INSERT INTO deliveries(id,table_name,lane) VALUES('waiting','t',1)")
        prepare_requirement_set_locked(known_wait,"waiting",80)
        known_wait.execute("""
            UPDATE prepare_requirements SET full_prepare_attempts=1
            WHERE delivery_id='waiting'
        """)
    known_wait.close()
    known_wait = init_state(known_wait_path)
    check(
        not prepare_delivery(known_wait,engine,mapping,"waiting",known_cfg),
        "known exact prepare size must wait before Arrow/DuckDB work when capacity is short")
    check(
        prepare_requirement_get(known_wait,"waiting") == (80,1),
        "capacity-only retry must not increment full prepare attempts")
    with state_transaction(known_wait):
        known_wait.execute("DELETE FROM load_parts WHERE delivery_id='holder'")
        known_wait.execute("DELETE FROM deliveries WHERE id='holder'")
        known_wait.execute("DELETE FROM deliveries WHERE id='waiting'")
    check(
        known_wait.execute(
            "SELECT 1 FROM prepare_requirements WHERE delivery_id='waiting'"
        ).fetchone() is None,
        "prepare requirement must follow delivery lifecycle")
    known_wait.close()
    passed.append("known prepared size waits cheaply across restart without recomputation")

    partial_path = os.path.join(
        tempfile.gettempdir(),"cdc_prepare_partial_restart_selftest.sqlite3")
    for suffix in ("","-wal","-shm"):
        with contextlib.suppress(FileNotFoundError):
            os.remove(partial_path+suffix)
    partial_db = init_state(partial_path)
    bootstrap(partial_db,"prepare-restart","source",("binlog.000001",4),["t"])
    partial_payload = arrow_job_payload(
        mapping,[(0,{"id":501,"v":501})],cfg,engine)
    with state_transaction(partial_db):
        job_id = partial_db.execute("""
            INSERT INTO jobs(
                table_name,lane,kind,payload,nrows,logical_bytes,
                source_file,source_pos,source_time,created)
            VALUES('t',0,'cdc',?,1,?,'binlog.000001',10,?,?) RETURNING id
        """,(partial_payload,arrow_payload_logical_bytes(partial_payload),
             time.time(),time.time())).fetchone()[0]
        partial_db.execute(
            "INSERT INTO deliveries(id,table_name,lane) VALUES('partial','t',0)")
        partial_db.execute(
            "INSERT INTO job_assignments(job_id,delivery_id) VALUES(?,'partial')",
            (job_id,))
        partial_db.execute(
            "INSERT INTO prepare_reservations(delivery_id,reserved_bytes) "
            "VALUES('partial',128)")
        partial_db.execute("""
            INSERT INTO load_parts(delivery_id,part,label,payload,nrows,json_bytes)
            VALUES('partial',0,'stale_partial',X'010203',1,3)
        """)
    partial_db.close()
    partial_db = init_state(partial_path)
    check(
        prepare_delivery(partial_db,engine,mapping,"partial",cfg),
        "interrupted unprepared delivery must regenerate after restart")
    check(
        partial_db.execute(
            "SELECT prepared FROM deliveries WHERE id='partial'").fetchone()[0] == 1,
        "restart prepare must become durable")
    check(
        partial_db.execute(
            "SELECT 1 FROM prepare_reservations WHERE delivery_id='partial'").fetchone()
            is None,
        "successful restart prepare must fully release its reservation")
    check(
        partial_db.execute(
            "SELECT label FROM load_parts WHERE delivery_id='partial' ORDER BY part LIMIT 1"
        ).fetchone()[0] == "cdc_partial_0",
        "stale partially written load parts must be regenerated, not reused")
    check(
        prepared_budget_used(partial_db) <= cfg["max_prepared_bytes"],
        "restart regeneration must retain the global prepared-byte ceiling")
    partial_db.close()
    passed.append("interrupted prepared payload rebuild is restart-safe")

    gtid_sid = "24bc7856-9a4d-11ee-8c99-0242ac120002"
    merged_gtid = gtid_add(gtid_sid+":1-3:5",gtid_sid+":4")
    check(merged_gtid == gtid_sid+":1-5","GTID adjacent intervals must merge")
    check(gtid_contains(merged_gtid,gtid_sid+":2-4"),"GTID containment failed")
    check(not gtid_contains(gtid_sid+":1-2",gtid_sid+":3"),"GTID containment false positive")
    encoded_gtid = gtid_encode_set(gtid_sid+":1-5")
    check(
        struct.unpack_from("<Q",encoded_gtid,0)[0] == 1 and
        encoded_gtid[8:24] == uuid.UUID(gtid_sid).bytes and
        struct.unpack_from("<QQ",encoded_gtid,32) == (1,6),
        "GTID wire encoding differs from COM_BINLOG_DUMP_GTID format")
    passed.append("native GTID parse/merge/contain/wire encoding")

    # Snapshot and CDC Arrow representations must converge on one routing representation.
    # Both source paths must converge on one native routing representation.
    cdc_routed = route_arrow(
        engine,mapping,raw_arrow(mapping,[(0,{"id":7,"v":70})]),cfg["key_partitions"])
    snapshot_routed = route_arrow(
        engine,mapping,snapshot_arrow(mapping,[(7,70)]),cfg["key_partitions"])
    check(
        cdc_routed.column("_sync_key").to_pylist() == snapshot_routed.column("_sync_key").to_pylist() and
        cdc_routed.column("_sync_lane").to_pylist() == snapshot_routed.column("_sync_lane").to_pylist(),
        "CDC dict and snapshot tuple paths must produce the same canonical key and lane")

    route_batch_cfg = dict(cfg,batch_rows=1000,batch_bytes=1024**2,max_row_bytes=1024**2)
    with tempfile.SpooledTemporaryFile() as route_spool:
        route_batches = transaction_batch_new()
        transaction_batch_add(
            route_batches,mapping,
            raw_arrow(mapping,[(1,{"id":7,"v":1}),(0,{"id":7,"v":2})]),
            route_batch_cfg,engine,route_spool)
        transaction_batch_add(
            route_batches,mapping,
            raw_arrow(mapping,[(1,{"id":7,"v":2}),(0,{"id":7,"v":3})]),
            route_batch_cfg,engine,route_spool)
        check(route_spool.tell() == 0,
              "bounded transaction batching must delay routing below its row/byte limit")
        check(
            transaction_batch_flush_all(
                route_batches,route_batch_cfg,engine,route_spool) == 4,
            "transaction batch flush row count")
        route_spool.seek(0)
        record = read_spool_record(route_spool)
        check(record is not None and read_spool_record(route_spool) is None,
              "same-key row events in one transaction should coalesce into one lane job")
        _,_,route_payload,route_count,route_logical_bytes = record
        route_table = arrow_job_table(mapping,route_payload)
        check(
            route_count == 4 and route_logical_bytes >= route_table.nbytes and
            route_table.column("_sync_op").to_pylist() == [1,0,1,0] and
            route_table.column("_sync_order").to_pylist() == [0,1,2,3] and
            route_table.column("v").to_pylist() == [1,2,2,3],
            "transaction routing batch must preserve same-key mutation order")

    mixed_raw = raw_arrow(mapping,[
        (index & 1,{"id":index,"v":index*10}) for index in range(64)])
    mixed_routed = route_arrow(
        engine,mapping,mixed_raw,route_batch_cfg["key_partitions"])
    expected_lanes = {}
    for row in mixed_routed.select(
            ["id","v","_sync_op","_sync_lane"]).to_pylist():
        expected_lanes.setdefault(int(row["_sync_lane"]),[]).append(
            (row["id"],row["v"],row["_sync_op"]))
    with tempfile.SpooledTemporaryFile() as mixed_spool:
        spool_routed(mixed_spool,mapping,mixed_routed,route_batch_cfg)
        mixed_spool.seek(0)
        actual_lanes = {}
        lane_sequence = []
        while True:
            record = read_spool_record(mixed_spool)
            if record is None:
                break
            _,lane,payload,_,_ = record
            lane_sequence.append(lane)
            table = arrow_job_table(mapping,payload)
            actual_lanes.setdefault(lane,[]).extend(zip(
                table.column("id").to_pylist(),
                table.column("v").to_pylist(),
                table.column("_sync_op").to_pylist()))
        check(
            lane_sequence == sorted(lane_sequence)
            and actual_lanes == expected_lanes,
            "single-pass lane partition must preserve deterministic lane order and source order")
    passed.append("bounded source-transaction Arrow routing coalesces events without reordering")

    fanout_a = dict(
        mapping,sr_table="fanout_a",_catalog_sink="starrocks.fanout_a")
    fanout_b = dict(
        mapping,sr_table="fanout_b",_catalog_sink="starrocks.fanout_b",
        sql="SELECT id,v+1 AS v FROM arrow_batch")
    validate_mapping(fanout_a)
    validate_mapping(fanout_b)
    fanout_plan = runtime_plan_entry(9,[fanout_a,fanout_b],fingerprint="fanout")
    check(
        len(fanout_plan["by_source"]["t"]) == 2
        and len(fanout_plan["source_prepared"]) == 1
        and set(fanout_plan["by_table"]) == {
            "starrocks.fanout_a","starrocks.fanout_b"},
        "fan-out runtime plan must decode one source and own two independent sink identities")
    with tempfile.TemporaryDirectory(prefix="cdc_fanout_selftest_") as fanout_dir:
        fanout_state = init_state(os.path.join(fanout_dir,"state.sqlite3"))
        bootstrap(
            fanout_state,"fanout","source",("binlog.000001",4),
            [mapping_key(fanout_a),mapping_key(fanout_b)])
        raw = raw_arrow(mapping,[(0,{"id":7,"v":70})])
        with tempfile.SpooledTemporaryFile() as fanout_spool:
            batches = transaction_batch_new()
            transaction_batch_add(
                batches,fanout_a,raw,route_batch_cfg,engine,fanout_spool)
            transaction_batch_add(
                batches,fanout_b,raw,route_batch_cfg,engine,fanout_spool)
            check(
                transaction_batch_flush_all(
                    batches,route_batch_cfg,engine,fanout_spool) == 2,
                "one decoded source row must fan out once per sink")
            commit_spool(
                fanout_state,fanout_spool,("binlog.000001",20),time.time(),
                {
                    mapping_key(fanout_a):fanout_a,
                    mapping_key(fanout_b):fanout_b,
                },
                plan_version=9)
        fanout_jobs = dict(fanout_state.execute("""
            SELECT table_name,COUNT(*) FROM jobs GROUP BY table_name
        """).fetchall())
        fanout_touched = dict(fanout_state.execute("""
            SELECT table_name,COUNT(*) FROM touched GROUP BY table_name
        """).fetchall())
        check(
            fanout_jobs == {
                "starrocks.fanout_a":1,"starrocks.fanout_b":1}
            and fanout_touched == {
                "starrocks.fanout_a":1,"starrocks.fanout_b":1},
            "fan-out sinks must receive independent durable jobs and touched sets")
        fanout_state.close()

        legacy_state = init_state(os.path.join(fanout_dir,"legacy.sqlite3"))
        bootstrap(
            legacy_state,"legacy","source",("binlog.000001",4),["t"])
        with state_transaction(legacy_state):
            legacy_state.execute(
                "INSERT INTO touched(table_name,pk) VALUES('t','7')")
            legacy_state.execute(
                "INSERT INTO snapshot_groups(id,table_name,cursor,is_last) "
                "VALUES('g','t',NULL,0)")
            legacy_state.execute("""
                INSERT INTO jobs(
                    table_name,lane,kind,payload,nrows,logical_bytes,created)
                VALUES('t',0,'cdc',X'00',1,1,?)
            """,(time.time(),))
            legacy_state.execute(
                "INSERT INTO deliveries(id,table_name,lane) VALUES('d','t',0)")
            legacy_state.execute(
                "INSERT INTO applied(table_name,lane,source_file,source_pos) "
                "VALUES('t',0,'binlog.000001',4)")
        legacy_mapping = dict(
            mapping,sr_table="legacy",_catalog_sink="starrocks.legacy")
        changed = migrate_sink_identity(
            legacy_state,"",0,[legacy_mapping],fresh=False)
        for table,column in (
            ("table_state","name"),("touched","table_name"),
            ("snapshot_groups","table_name"),("jobs","table_name"),
            ("deliveries","table_name"),("applied","table_name")):
            check(
                legacy_state.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE {column}='starrocks.legacy'"
                ).fetchone()[0] == 1,
                f"{table} legacy source key was not migrated to sink identity")
        check(
            changed == 6
            and meta_get(legacy_state,"sink_identity_v1") == 1
            and migrate_sink_identity(
                legacy_state,"",0,[legacy_mapping],fresh=False) == 0,
            "sink identity migration must be complete and idempotent")
        legacy_state.close()
    passed.append("single decode fan-out owns independent durable sink state and migrates legacy keys")

    with tempfile.SpooledTemporaryFile() as guard_spool:
        guard_spool.write(b"x"*2048)
        rejects(lambda: transaction_spool_guard(guard_spool,cfg,force=True),
                "oversized uncommitted transaction spool must fail before cursor advance")
    final_guard_cfg = dict(
        cfg,batch_rows=100000,batch_bytes=1024**2,txn_spool_max_bytes=256)
    final_batches = transaction_batch_new()
    with tempfile.SpooledTemporaryFile() as final_spool:
        final_raw = raw_arrow(mapping,[
            (0,{"id":index,"v":index}) for index in range(200)])
        check(
            transaction_batch_add(
                final_batches,mapping,final_raw,final_guard_cfg,engine,final_spool) == 0,
            "tail batch must remain buffered until source COMMIT")
        transaction_batch_flush_all(
            final_batches,final_guard_cfg,engine,final_spool)
        check(
            final_spool.tell() > final_guard_cfg["txn_spool_max_bytes"],
            "final flush regression fixture must actually cross the spool limit")
        rejects(
            lambda: transaction_spool_guard(
                final_spool,final_guard_cfg,force=True),
            "final COMMIT flush must recheck spool limit before SQLite commit")
    check(
        state_temp_dir(dict(state=os.path.join("/tmp","cdc","state.sqlite3")))
        == os.path.join("/tmp","cdc"),
        "all spill files must resolve to the monitored state filesystem")
    wide_next = snapshot_next_row_limit(
        50000,100,256*1024,0.1,dict(cfg,snapshot_chunk_bytes=256*1024))
    check(wide_next <= 100,
          "snapshot row target must shrink from observed byte width before next query")
    passed.append("bounded snapshot byte target and source-transaction spool guard")

    def commit(con, ops, offset):
        with tempfile.SpooledTemporaryFile(max_size=64) as spool:
            spool_rows(spool,mapping,ops,cfg,engine)
            commit_spool(con,spool,("binlog.000001",offset),time.time(),{"t":mapping})

    def apply_part(con, delivery, part, sink, labels):
        label,payload = con.execute(
            "SELECT label,payload FROM load_parts WHERE delivery_id=? AND part=?",(delivery,part)).fetchone()
        if label not in labels:
            for line in gzip.decompress(payload).splitlines():
                row = orjson.loads(line)
                if row.pop("__op"):
                    sink.pop(row["id"],None)
                else:
                    sink[row["id"]] = row
            labels.add(label)

    def drain(con, sink, labels, order=(0,1,2), table_mode=False):
        for lane in ((-1,) if table_mode else order):
            while True:
                delivery = claim_table_delivery(con,"t",cfg) if table_mode else claim_delivery(con,"t",lane,cfg)
                if delivery is None:
                    break
                prepare_delivery(con,engine,mapping,delivery,cfg)
                for (part,) in con.execute(
                    "SELECT part FROM load_parts WHERE delivery_id=? ORDER BY part",(delivery,)).fetchall():
                    apply_part(con,delivery,part,sink,labels)
                    with state_transaction(con):
                        con.execute("UPDATE load_parts SET visible=1 WHERE delivery_id=? AND part=?",(delivery,part))
                acknowledge_delivery(con,delivery)

    with tempfile.TemporaryDirectory(prefix="cdc_selftest_") as directory:
        legacy_path = os.path.join(directory,"legacy.sqlite3")
        legacy = sqlite3.connect(legacy_path)
        legacy.execute("CREATE TABLE meta(key TEXT PRIMARY KEY,value BLOB NOT NULL)")
        legacy.execute("INSERT INTO meta VALUES('fingerprint',?)",(pack("legacy"),))
        legacy.commit()
        legacy.close()
        rejects(lambda: init_state(legacy_path),
                "legacy CDC state without an explicit state_format must be rejected")

        v2_path = os.path.join(directory,"state_v2.sqlite3")
        v2 = sqlite3.connect(v2_path)
        v2.execute("CREATE TABLE meta(key TEXT PRIMARY KEY,value BLOB NOT NULL)")
        v2.execute(
            "INSERT INTO meta VALUES('state_format',?)",(pack(2),))
        v2.commit()
        v2.close()
        migrated = init_state(v2_path)
        check(
            meta_get(migrated,"state_format") == 4
            and meta_get(migrated,"state_migrated_from") == 2,
            "state format 2 must migrate one-way to format 4")
        migrated.close()

        v3_path = os.path.join(directory,"state_v3.sqlite3")
        v3 = sqlite3.connect(v3_path)
        v3.execute("CREATE TABLE meta(key TEXT PRIMARY KEY,value BLOB NOT NULL)")
        v3.execute(
            "INSERT INTO meta VALUES('state_format',?)",(pack(3),))
        v3.execute("""
            CREATE TABLE table_state(
                name TEXT PRIMARY KEY,cursor BLOB,upper_key BLOB,
                upper_set INTEGER NOT NULL DEFAULT 0,
                snapshot_done INTEGER NOT NULL DEFAULT 0)
        """)
        v3.execute(
            "INSERT INTO table_state(name,cursor) VALUES('t',?)",
            (pack((10,)),))
        v3.execute("""
            CREATE TABLE snapshot_groups(
                id TEXT PRIMARY KEY,table_name TEXT NOT NULL,
                cursor BLOB,is_last INTEGER NOT NULL)
        """)
        v3.execute(
            "INSERT INTO snapshot_groups VALUES('pending','t',?,0)",
            (pack((20,)),))
        v3.commit()
        v3.close()
        migrated3 = init_state(v3_path)
        migrated_row = migrated3.execute("""
            SELECT cursor,staged_cursor,snapshot_done,staged_done
            FROM table_state WHERE name='t'
        """).fetchone()
        check(
            meta_get(migrated3,"state_format") == 4
            and meta_get(migrated3,"state_migrated_from") == 3
            and unpack(migrated_row[0]) == (10,)
            and unpack(migrated_row[1]) == (20,)
            and not migrated_row[2] and not migrated_row[3]
            and migrated3.execute(
                "SELECT stage_seq FROM snapshot_groups WHERE id='pending'"
            ).fetchone()[0] > 0,
            "state format 3 pending snapshot must promote its durable cursor "
            "without advancing visible progress")
        migrated3.close()
        passed.append(
            "state format 2/3 -> 4 migration preserves visible and staged cursors")

        path = os.path.join(directory,"state.sqlite3")
        con = init_state(path)
        bootstrap(con,"test","source",("binlog.000001",4),["t"])
        sink,labels = {},set()
        old = [{"id":i,"v":i} for i in range(1,7)]
        commit(con,[(1,old[0]),(1,old[1]),(0,{"id":2,"v":90}),
                    (1,old[2]),(0,{"id":3,"v":-1}),(0,{"id":7,"v":70})],30)
        journal_payload = con.execute("SELECT payload FROM jobs ORDER BY id LIMIT 1").fetchone()[0]
        check(journal_payload.startswith(ARROW_JOB_MAGIC),
              "new durable jobs must use Arrow IPC rather than pickled Python rows")
        journal_table = arrow_job_table(mapping,journal_payload)
        check(journal_table.num_rows > 0,
              "compressed Arrow IPC journal payload must be readable without Python row reconstruction")
        legacy_sink = pa.BufferOutputStream()
        legacy_sink.write(ARROW_JOB_MAGIC)
        with pa.ipc.new_stream(legacy_sink,journal_table.schema) as legacy_writer:
            legacy_writer.write_table(journal_table)
        check(arrow_job_table(mapping,memoryview(legacy_sink.getvalue())).num_rows == journal_table.num_rows,
              "legacy uncompressed ARW2IPC0 jobs must remain readable after journal compression")
        original_route = vars(cdc)["route_arrow"]
        route_calls = [0]
        def counted_route(*args,**kwargs):
            route_calls[0] += 1
            return original_route(*args,**kwargs)
        vars(cdc)["route_arrow"] = counted_route
        try:
            check(not stage_snapshot(
                con,mapping,old,(6,),True,("binlog.000001",50),cfg,engine),
                "backfill must wait for its high watermark")
            check(route_calls[0] == 0,
                  "barrier wait must not repeatedly route the same snapshot chunk")
        finally:
            vars(cdc)["route_arrow"] = original_route
        commit(con,[],50)
        check(stage_snapshot(con,mapping,old,(6,),True,("binlog.000001",50),cfg,engine),"snapshot stage")
        check(not con.execute("SELECT snapshot_done FROM table_state").fetchone()[0],
              "snapshot cursor cannot advance before all lanes are visible")
        # This DELETE arrives after snapshot admission, and must remain after its snapshot job.
        commit(con,[(1,old[3])],60)
        drain(con,sink,labels,order=(2,0,1))
        check(sink == {2:{"id":2,"v":90},5:old[4],6:old[5],7:{"id":7,"v":70}},
              "delete/filter transition must never be resurrected by backfill")
        check(con.execute("SELECT snapshot_done FROM table_state").fetchone()[0] == 1,"snapshot completion")
        check(con.execute("SELECT COUNT(*) FROM touched").fetchone()[0] == 0,"safe touched-key cleanup")
        passed.append("backfill barriers, delete resurrection, filter exits, parallel lanes")

        pipeline_path = os.path.join(directory,"snapshot_read_ahead.sqlite3")
        pipeline = init_state(pipeline_path)
        bootstrap(
            pipeline,"pipeline","source",("binlog.000001",4),["t"])
        pipeline_cfg = dict(cfg,snapshot_read_ahead_groups=2)
        check(stage_snapshot(
            pipeline,mapping,
            [{"id":1001,"v":1001},{"id":1002,"v":1002}],
            (1002,),False,("binlog.000001",4),pipeline_cfg,engine),
            "first read-ahead snapshot group must stage")
        check(stage_snapshot(
            pipeline,mapping,
            [{"id":1003,"v":1003},{"id":1004,"v":1004}],
            (1004,),True,("binlog.000001",4),pipeline_cfg,engine),
            "second read-ahead snapshot group must stage before first is visible")
        state = pipeline.execute("""
            SELECT cursor,staged_cursor,snapshot_done,staged_done
            FROM table_state WHERE name='t'
        """).fetchone()
        groups = pipeline.execute("""
            SELECT id,stage_seq FROM snapshot_groups
            WHERE table_name='t' ORDER BY stage_seq
        """).fetchall()
        check(
            state[0] is None
            and unpack(state[1]) == (1004,)
            and not state[2] and state[3] == 1
            and len(groups) == 2,
            "read-ahead must advance only durable staged progress")

        first_group,second_group = groups[0][0],groups[1][0]
        with state_transaction(pipeline):
            pipeline.execute(
                "DELETE FROM jobs WHERE group_id=?",(second_group,))
            finish_snapshot_group(pipeline,second_group)
        state = pipeline.execute("""
            SELECT cursor,snapshot_done FROM table_state WHERE name='t'
        """).fetchone()
        check(
            state[0] is None and not state[1]
            and pipeline.execute(
                "SELECT 1 FROM snapshot_groups WHERE id=?",
                (second_group,)).fetchone() is not None,
            "later snapshot group completion must not pass an earlier group")

        with state_transaction(pipeline):
            pipeline.execute(
                "DELETE FROM jobs WHERE group_id=?",(first_group,))
            finish_snapshot_group(pipeline,first_group)
        state = pipeline.execute("""
            SELECT cursor,staged_cursor,snapshot_done,staged_done
            FROM table_state WHERE name='t'
        """).fetchone()
        check(
            unpack(state[0]) == (1004,)
            and unpack(state[1]) == (1004,)
            and state[2] == 1 and state[3] == 1
            and pipeline.execute(
                "SELECT COUNT(*) FROM snapshot_groups"
            ).fetchone()[0] == 0,
            "visible snapshot cursor must retire the completed stage prefix in order")
        pipeline.close()
        passed.append(
            "durable snapshot read-ahead pipelines two groups without visible cursor reordering")

        commit(con,[(0,{"id":2,"v":200})],70)
        lane = con.execute("SELECT lane FROM jobs WHERE table_name='t' ORDER BY id LIMIT 1").fetchone()[0]
        delivery = claim_delivery(con,"t",lane,cfg)
        prepare_delivery(con,engine,mapping,delivery,cfg)
        part = con.execute("SELECT part FROM load_parts WHERE delivery_id=?",(delivery,)).fetchone()[0]
        stored_payload,stored_json_bytes = con.execute(
            "SELECT payload,json_bytes FROM load_parts WHERE delivery_id=? AND part=?",
            (delivery,part)).fetchone()
        check(stored_json_bytes == len(gzip.decompress(stored_payload)),
              "load_parts json_bytes must equal exact uncompressed NDJSON Stream Load bytes")
        apply_part(con,delivery,part,sink,labels)
        before = con.execute("SELECT label,payload FROM load_parts").fetchall()
        # Crash window: server committed, local visible flag was never written.
        con.close()
        con = open_state(path)
        check(claim_delivery(con,"t",lane,cfg) == delivery,"delivery identity must survive restart")
        check(claim_table_delivery(con,"t",cfg) == delivery,"table writer must drain legacy delivery first")
        check(not con.execute("SELECT 1 FROM load_transactions WHERE delivery_id=?",(delivery,)).fetchone(),
              "legacy delivery must not be silently changed to a transaction")
        prepare_delivery(con,engine,mapping,delivery,cfg)
        check(before == con.execute("SELECT label,payload FROM load_parts").fetchall(),"stable retry bytes and label")
        drain(con,sink,labels)
        check(sink[2]["v"] == 200,"recovery after lost commit response")
        commit(con,[(0,{"id":2,"v":999})],70)
        check(con.execute("SELECT COUNT(*) FROM active_jobs").fetchone()[0] == 0,"duplicate source position")
        passed.append("restart, persisted request identity, duplicate source position")

        before_position = meta_get(con,"read_position")
        with tempfile.SpooledTemporaryFile() as spool:
            spool_rows(spool,mapping,[(0,{"id":8,"v":8})],cfg,engine)
            write_spool_record(spool,"missing_table",0,b"invalid",1,7)
            rejects(lambda: commit_spool(con,spool,("binlog.000001",80),0,{"t":mapping}),
                    "broken source transaction must fail")
        check(meta_get(con,"read_position") == before_position,"read cursor must roll back with the source transaction")
        check(con.execute("SELECT COUNT(*) FROM active_jobs").fetchone()[0] == 0,"no partial local source transaction")
        rejects(lambda: bootstrap(con,"changed","source",before_position,["t"]),"config drift must fail")
        check(position_ge(("binlog.000010",4),("binlog.000009",999999)),"binlog rotation ordering")
        rejects(lambda: position_ge(("other.000010",4),("binlog.000009",4)),"source prefix reset")
        sid = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
        gtid_path = os.path.join(directory,"gtid.sqlite3")
        gtid_con = init_state(gtid_path)
        bootstrap(gtid_con,"test","source",("binlog.000001",4),["t"],sid+":1-3")
        with tempfile.SpooledTemporaryFile() as empty:
            commit_spool(gtid_con,empty,("binlog.000001",80),0,{"t":mapping},sid+":4")
        check(meta_get(gtid_con,"gtid_set") == sid+":1-4","GTID checkpoint merge")
        check(gtid_contains(sid+":1-9",meta_get(gtid_con,"gtid_set")),"GTID containment")
        gtid_con.close()
        passed.append("transaction rollback, GTID recovery, source continuity, configuration drift")
        con.close()

        scheduler_path = os.path.join(directory,"scheduler.sqlite3")
        scheduler = init_state(scheduler_path)
        check(scheduler.execute("PRAGMA wal_autocheckpoint").fetchone()[0] == 0,
              "state writers must never checkpoint on the commit hot path")
        bootstrap(scheduler,"test","source",("binlog.000001",4),["t"])
        before_changes = scheduler.total_changes
        with state_transaction(scheduler):
            cursor_advance(scheduler,("binlog.000001",4))
        check(scheduler.total_changes == before_changes,"equal heartbeat position must not rewrite state")
        now = time.time()
        scheduler.executemany("""
            INSERT INTO jobs(table_name,lane,kind,payload,nrows,logical_bytes,created)
            VALUES('t',?,'cdc',?,1,?,?)
        """,((0,b"a"*300,300,now),(0,b"b"*300,300,now),(1,b"c",1,now)))
        check(pending_batch(scheduler,"t",0,cfg)[4],"batch must flush when the next job cannot fit")
        limited = dict(cfg,max_inflight_deliveries=1,max_prepared_bytes=1024)
        check(claim_delivery(scheduler,"t",0,limited) is not None,"first delivery claim")
        check(claim_delivery(scheduler,"t",1,limited) is None,"global in-flight delivery budget")
        scheduler.close()
        passed.append("idle cursor writes, full-prefix batching, global in-flight budget")

        version_path = os.path.join(directory,"plan_versions.sqlite3")
        version_db = init_state(version_path)
        bootstrap(version_db,"test","source",("binlog.000001",4),["t"])
        with tempfile.SpooledTemporaryFile() as spool:
            spool_rows(spool,mapping,[(0,{"id":201,"v":1})],cfg,engine)
            commit_spool(
                version_db,spool,("binlog.000001",10),time.time(),
                {"t":mapping},plan_version=7)
        with tempfile.SpooledTemporaryFile() as spool:
            spool_rows(spool,mapping,[(0,{"id":202,"v":2})],cfg,engine)
            commit_spool(
                version_db,spool,("binlog.000001",20),time.time(),
                {"t":mapping},plan_version=8)
        version_delivery = claim_table_delivery(version_db,"t",cfg)
        check(version_delivery is not None,"plan-version delivery claim")
        check(
            version_db.execute(
                "SELECT plan_version FROM deliveries WHERE id=?",
                (version_delivery,)).fetchone()[0] == 7,
            "delivery must inherit its FIFO head plan version")
        check(
            version_db.execute("""
                SELECT COUNT(DISTINCT j.plan_version)
                FROM active_jobs j
                JOIN job_assignments a ON a.job_id=j.id
                WHERE a.delivery_id=?
            """,(version_delivery,)).fetchone()[0] == 1,
            "one delivery must never mix plan versions")
        check(
            version_db.execute("""
                SELECT COUNT(*) FROM active_jobs j
                LEFT JOIN job_assignments a ON a.job_id=j.id
                WHERE j.plan_version=8 AND a.job_id IS NULL
            """).fetchone()[0] > 0,
            "new-plan jobs must remain queued behind old-plan delivery")

        old_plan = runtime_plan_entry(7,[mapping],fingerprint="plan7")
        new_plan = runtime_plan_entry(8,[mapping],fingerprint="plan8")
        hot_runtime = dict(
            plan_lock=threading.RLock(),active_plan_version=7,
            plans={7:old_plan,8:new_plan},pending_plan=new_plan,
            deferred_plan=None,load_events={})
        reset_calls = []
        original_native_reset = vars(cdc)["native_reset"]
        vars(cdc)["native_reset"] = lambda decoder,config,prepared: reset_calls.append(
            [item["_plan_version"] for item in prepared])
        try:
            activated = activate_pending_plan(
                version_db,None,cfg,hot_runtime,("binlog.000001",30))
        finally:
            vars(cdc)["native_reset"] = original_native_reset
        check(activated["version"] == 8 and runtime_active_version(hot_runtime) == 8,
              "pending plan must become active at the explicit safe boundary")
        check(meta_get(version_db,"active_plan_version") == 8 and
              meta_get(version_db,"fingerprint") == "plan8" and
              meta_get(version_db,"plan_cutover_position") == ("binlog.000001",30),
              "hot cutover identity and fingerprint must be durable before new decoding")
        check(reset_calls == [[8]],"hot cutover must reset native decoder to the new plan")
        check(version_db.execute(
            "SELECT COUNT(*) FROM active_jobs WHERE plan_version=7").fetchone()[0] > 0,
              "hot cutover must not relabel old durable jobs")

        mapping_u = dict(mapping,src_table="u",sr_table="u")
        mapping_u["_schema_signature"] = mapping.get("_schema_signature")
        two_table_plan = runtime_plan_entry(
            9,[mapping,mapping_u],fingerprint="plan9")
        drop_plan = runtime_plan_entry(10,[mapping],fingerprint="plan10")
        compatible,reason = runtime_plan_compatible(two_table_plan,drop_plan)
        check(compatible and "drop-only" in reason,
              "drop-only sink topology must be hot-compatible")
        compatible,reason = runtime_plan_compatible(drop_plan,two_table_plan)
        check(compatible and "hot-add" in reason,
              "adding a source/sink must be hot-compatible via snapshot+live CDC")

        semantic_change = dict(
            mapping,sql="SELECT id,v+1 AS v FROM arrow_batch")
        validate_mapping(semantic_change)
        semantic_plan = runtime_plan_entry(
            11,[semantic_change],fingerprint="plan11")
        compatible,reason = runtime_plan_compatible(drop_plan,semantic_plan)
        check(
            not compatible and "semantics changed" in reason,
            "retained sink SQL/filter/macro/UDF semantics must never switch "
            "forward-hot without rebuilding historical target state")

        class SubmitProbe:
            def __init__(self):
                self.calls = []
            def submit(self,*args):
                self.calls.append(args)
                return None
        add_cfg = dict(
            cfg,load_mode="merge_async",writer_max=2,writer_initial=2,
            writer_min=1,commit_interval_ms=1000,
            resource=dict(cpu_target=4,memory_mb=1024))
        submit_probe = SubmitProbe()
        add_runtime = dict(
            stop=threading.Event(),control_lock=threading.Lock(),
            worker_keys={"t"},worker_mappings=[mapping],
            pressure_until={"t":0},table_interval={"t":1.0},
            active_writers={"t":2},last_pressure={"t":time.time()},
            last_scale={"t":time.time()},max_rowset={"t":-1},
            version_recovery={"t":False},version_recovery_good={"t":0},
            resource_writer_cap=2,
            load_events={"t":threading.Event()},
            lane_locks={
                ("t",lane):threading.Lock()
                for lane in range(add_cfg["key_partitions"])},
            metrics=init_run_metrics([mapping]),
            thread_lock=threading.Lock(),worker_threads=[],
            snapshot_executor=submit_probe)
        thread_names = []
        original_thread_register = vars(cdc)["runtime_thread_register"]
        vars(cdc)["runtime_thread_register"] = (
            lambda runtime,thread: thread_names.append(thread.name) or thread)
        try:
            check(
                runtime_add_sink(mapping_u,add_cfg,add_runtime),
                "new sink must be registered exactly once")
        finally:
            vars(cdc)["runtime_thread_register"] = original_thread_register
        expected_hot_add_cap = max(
            1,min(
                add_cfg["writer_max"],
                duckdb_merge_writer_cap(add_cfg,2),
                int(add_cfg["resource"]["cpu_target"])//2))
        check(
            "u" in add_runtime["worker_keys"]
            and add_runtime["worker_mappings"][-1] is mapping_u
            and add_runtime["active_writers"]["u"] == expected_hot_add_cap
            and add_runtime["active_writers"]["t"] == expected_hot_add_cap
            and add_runtime["resource_writer_cap"] == expected_hot_add_cap
            and all(
                ("u",lane) in add_runtime["lane_locks"]
                for lane in range(add_cfg["key_partitions"])),
            "hot-add registration must initialize sink-local scheduling state "
            "and rebalance existing writers to the joint CPU/memory cap")
        check(
            len(thread_names) == add_cfg["writer_max"]
            and len(submit_probe.calls) == 1
            and "u" in add_runtime["metrics"]["tables"],
            "hot-add registration must schedule writers, snapshot and metrics")
        check(
            not runtime_add_sink(mapping_u,add_cfg,add_runtime),
            "hot-add registration must be idempotent within one runtime")

        retire_path = os.path.join(directory,"retire_worker.sqlite3")
        retire_db = init_state(retire_path)
        check(
            sink_durable_drained(retire_db,"u"),
            "empty retired sink must be durably drained")
        check(
            runtime_mark_sink_retiring(add_runtime,"u",add_cfg)
            and runtime_sink_retiring(add_runtime,"u"),
            "drop must put physical workers into explicit draining state")
        check(
            not runtime_retiring_worker_done(add_runtime,"u",add_cfg)
            and "u" in add_runtime["worker_keys"],
            "first merge worker exit must not tear down shared sink runtime")
        check(
            runtime_retiring_worker_done(add_runtime,"u",add_cfg)
            and "u" not in add_runtime["worker_keys"]
            and not runtime_sink_retiring(add_runtime,"u")
            and all(
                mapping_key(item) != "u"
                for item in add_runtime["worker_mappings"])
            and all(
                key[0] != "u"
                for key in add_runtime["lane_locks"]),
            "last retired worker must release sink-local runtime resources")
        expected_after_drop_cap = max(
            1,min(
                add_cfg["writer_max"],
                duckdb_merge_writer_cap(add_cfg,1),
                int(add_cfg["resource"]["cpu_target"])))
        check(
            add_runtime["resource_writer_cap"] == expected_after_drop_cap,
            "retired sink cleanup must recompute the remaining writer cap")
        retire_db.close()

        target_mapping_u = two_table_plan["by_table"]["u"]
        target_mapping_u["_target_ddl"] = "CREATE TABLE IF NOT EXISTS `u` (`id` BIGINT)"
        target_probe = MagicMock()
        target_probe.__enter__.return_value = target_probe
        target_probe.__exit__.return_value = False
        target_cursor = MagicMock()
        target_probe.cursor.return_value.__enter__.return_value = target_cursor
        target_probe.cursor.return_value.__exit__.return_value = False
        original_mysql_connect = vars(cdc)["mysql_connect"]
        original_target_exists = vars(cdc)["target_table_exists"]
        try:
            vars(cdc)["mysql_connect"] = lambda config,target=False: target_probe
            exists_calls = iter((False,True))
            vars(cdc)["target_table_exists"] = (
                lambda cur,config,table: next(exists_calls))
            ensure_hot_add_targets(add_cfg,two_table_plan,["u"])
            check(
                target_cursor.execute.call_args_list[0].args[0]
                == target_mapping_u["_target_ddl"],
                "hot-add install must execute only the preflight-produced target DDL")

            target_cursor.reset_mock()
            target_cursor.fetchone.return_value = (1,)
            vars(cdc)["target_table_exists"] = (
                lambda cur,config,table: True)
            rejects(
                lambda: ensure_hot_add_targets(
                    add_cfg,two_table_plan,["u"]),
                "non-empty hot-add target must fail before activation")
        finally:
            vars(cdc)["mysql_connect"] = original_mysql_connect
            vars(cdc)["target_table_exists"] = original_target_exists

        hotadd_path = os.path.join(directory,"hotadd_cutover.sqlite3")
        hotadd_db = init_state(hotadd_path)
        bootstrap(
            hotadd_db,"plan8","source",("binlog.000001",30),["t"])
        with state_transaction(hotadd_db):
            meta_set(hotadd_db,"active_plan_version",8)
            meta_set(hotadd_db,"fingerprint","plan8")
        hotadd_runtime = dict(
            plan_lock=threading.RLock(),active_plan_version=8,
            plans={8:new_plan,9:two_table_plan},pending_plan=two_table_plan,
            deferred_plan=None,load_events={})
        add_calls = []
        reset_calls = []
        target_rechecks = []
        original_native_reset = vars(cdc)["native_reset"]
        original_runtime_add_sink = vars(cdc)["runtime_add_sink"]
        original_ensure_hot_targets = vars(cdc)["ensure_hot_add_targets"]
        vars(cdc)["native_reset"] = lambda decoder,config,prepared: reset_calls.append(
            sorted(item["src_table"] for item in prepared))
        vars(cdc)["runtime_add_sink"] = (
            lambda item,config,runtime: add_calls.append(mapping_key(item)) or True)
        vars(cdc)["ensure_hot_add_targets"] = (
            lambda config,candidate,added: target_rechecks.append(list(added)))
        try:
            activated = activate_pending_plan(
                hotadd_db,None,cfg,hotadd_runtime,("binlog.000001",40))
        finally:
            vars(cdc)["native_reset"] = original_native_reset
            vars(cdc)["runtime_add_sink"] = original_runtime_add_sink
            vars(cdc)["ensure_hot_add_targets"] = original_ensure_hot_targets
        check(
            activated["version"] == 9
            and runtime_active_version(hotadd_runtime) == 9
            and meta_get(hotadd_db,"active_plan_version") == 9
            and meta_get(hotadd_db,"plan_history_mode") == "snapshot_plus_live_cdc",
            "hot-add cutover must durably activate snapshot+live CDC mode")
        check(
            hotadd_db.execute(
                "SELECT snapshot_done FROM table_state WHERE name='u'"
            ).fetchone() == (0,),
            "hot-add sink must enter durable table_state as snapshot incomplete")
        check(
            target_rechecks == [["u"]]
            and reset_calls == [["t","u"]] and add_calls == ["u"],
            "hot-add cutover must recheck target, reset source decoder and then start the sink worker")
        hotadd_db.close()

        check(
            durable_draining_plan_versions(version_db,8) == [7],
            "new hot publish must observe the previous durable plan while it drains")
        with state_transaction(version_db):
            version_db.execute(
                "DELETE FROM job_assignments WHERE delivery_id=?",
                (version_delivery,))
            version_db.execute(
                "DELETE FROM load_transactions WHERE delivery_id=?",
                (version_delivery,))
            version_db.execute(
                "DELETE FROM prepare_reservations WHERE delivery_id=?",
                (version_delivery,))
            version_db.execute(
                "DELETE FROM prepare_requirements WHERE delivery_id=?",
                (version_delivery,))
            version_db.execute(
                "DELETE FROM load_parts WHERE delivery_id=?",
                (version_delivery,))
            version_db.execute(
                "DELETE FROM deliveries WHERE id=?",(version_delivery,))
            version_db.execute(
                "DELETE FROM jobs WHERE plan_version=7")
        check(
            durable_draining_plan_versions(version_db,8) == [],
            "a later hot plan may advance only after the prior version fully drains")
        version_db.close()
        passed.append(
            "durable plan-version FIFO separation, hot-add snapshot cutover and serialized draining")

        bundle_path = os.path.join(directory,"snapshot_bundle.sqlite3")
        bundle = init_state(bundle_path)
        bootstrap(bundle,"test","source",("binlog.000001",4),["t"])
        group = "bundle_group"
        with state_transaction(bundle):
            bundle.execute(
                "INSERT INTO snapshot_groups(id,table_name,cursor,is_last) "
                "VALUES(?,?,?,?)",(group,"t",pack((3,)),0))
            now = time.time()
            for lane,row_id in enumerate((100,101,102)):
                payload = arrow_job_payload(mapping,[(0,{"id":row_id,"v":row_id})],cfg,engine)
                bundle.execute("""
                    INSERT INTO jobs(table_name,lane,kind,payload,nrows,created,group_id)
                    VALUES('t',?,'snapshot',?,1,?,?)
                """,(lane,payload,now,group))
            bundle.execute("UPDATE jobs SET logical_bytes=length(payload) WHERE logical_bytes=0")
            meta_set(bundle,"pending_bytes",sum(
                r[0] for r in bundle.execute("SELECT logical_bytes FROM jobs").fetchall()))
        bundle_runtime = dict(control_lock=threading.Lock(),active_writers={"t":1})
        bundle_cfg = dict(cfg,snapshot_bundle_max_lanes=8,batch_bytes=65536,max_row_bytes=65536)
        delivery = claim_snapshot_bundle(bundle,"t",0,bundle_cfg,bundle_runtime)
        check(delivery is not None,"snapshot bundle claim")
        check(delivery_lanes(bundle,delivery) == [0,1,2],"snapshot bundle must retain logical lanes")
        check(bundle.execute(
            "SELECT COUNT(*) FROM job_assignments WHERE delivery_id=?",(delivery,)).fetchone()[0] == 3,
              "snapshot bundle must coalesce lane heads into one delivery")
        # Later CDC in a bundled lane must remain behind the assigned snapshot after a restart.
        with state_transaction(bundle):
            bundle.execute("""
                INSERT INTO jobs(table_name,lane,kind,payload,nrows,source_file,source_pos,source_time,created)
                VALUES('t',1,'cdc',?,1,'binlog.000001',9,?,?)
            """,(arrow_job_payload(mapping,[(0,{"id":101,"v":999})],cfg,engine),
                  time.time(),time.time()))
        bundle.close()
        bundle = open_state(bundle_path)
        check(lane_blocking_delivery(bundle,"t",1) == delivery,
              "restart must preserve bundle member lane blocking")
        check(merge_candidate_lanes(bundle,"t") == [0],
              "bundle member lanes must not expose later CDC before snapshot VISIBLE")
        prepare_delivery(bundle,engine,mapping,delivery,bundle_cfg)
        with state_transaction(bundle):
            bundle.execute("UPDATE load_parts SET visible=1 WHERE delivery_id=?",(delivery,))
        acknowledge_delivery(bundle,delivery)
        check(lane_blocking_delivery(bundle,"t",1) is None,
              "bundle lane must unblock only after snapshot acknowledgement")
        check(1 in merge_candidate_lanes(bundle,"t"),
              "later CDC must become schedulable after bundled snapshot is visible")
        bundle.close()
        passed.append("snapshot lane bundling preserves FIFO and restart blocking")

        cdc_bundle_path = os.path.join(directory,"cdc_bundle.sqlite3")
        cdc_bundle = init_state(cdc_bundle_path)
        bootstrap(cdc_bundle,"test","source",("binlog.000001",4),["t"])
        now = time.time()
        with state_transaction(cdc_bundle):
            for lane,row_id in enumerate((200,201,202,203,204)):
                payload = arrow_job_payload(mapping,[(0,{"id":row_id,"v":row_id})],cfg,engine)
                cdc_bundle.execute("""
                    INSERT INTO jobs(
                        table_name,lane,kind,payload,nrows,source_file,source_pos,source_time,created
                    ) VALUES('t',?,'cdc',?,1,'binlog.000001',?,?,?)
                """,(lane,payload,20+lane,now,now))
        cdc_runtime = dict(control_lock=threading.Lock(),active_writers={"t":4})
        cdc_cfg = dict(
            cfg,key_partitions=16,batch_rows=1000,batch_bytes=1024**2,max_row_bytes=1024**2)
        delivery = claim_cdc_bundle(cdc_bundle,"t",0,cdc_cfg,cdc_runtime)
        check(delivery is not None,"CDC bundle claim")
        check(delivery_lanes(cdc_bundle,delivery) == [0,1,2,3],
              "CDC bundle must adapt to four lanes at four active writers")
        check(4 in merge_candidate_lanes(cdc_bundle,"t"),
              "unbundled CDC lane must remain schedulable")
        with state_transaction(cdc_bundle):
            payload = arrow_job_payload(mapping,[(0,{"id":301,"v":301})],cfg,engine)
            cdc_bundle.execute("""
                INSERT INTO jobs(
                    table_name,lane,kind,payload,nrows,source_file,source_pos,source_time,created
                ) VALUES('t',1,'cdc',?,1,'binlog.000001',30,?,?)
            """,(payload,now,now))
        cdc_bundle.close()
        cdc_bundle = open_state(cdc_bundle_path)
        check(lane_blocking_delivery(cdc_bundle,"t",1) == delivery,
              "CDC bundle member lane must stay blocked across restart")
        check(1 not in merge_candidate_lanes(cdc_bundle,"t"),
              "later CDC must not pass an assigned bundled lane head")
        prepare_delivery(cdc_bundle,engine,mapping,delivery,cdc_cfg)
        with state_transaction(cdc_bundle):
            cdc_bundle.execute("UPDATE load_parts SET visible=1 WHERE delivery_id=?",(delivery,))
        acknowledge_delivery(cdc_bundle,delivery)
        check(lane_blocking_delivery(cdc_bundle,"t",1) is None,
              "CDC bundle lane must unblock after acknowledgement")
        check(1 in merge_candidate_lanes(cdc_bundle,"t"),
              "later CDC must become schedulable after bundled CDC is visible")
        cdc_bundle.close()
        passed.append("CDC lane bundling preserves FIFO, restart blocking and adaptive width")

        sidecar_path = os.path.join(directory,"assignment_sidecar.sqlite3")
        sidecar = init_state(sidecar_path)
        bootstrap(sidecar,"test","source",("binlog.000001",4),["t"])
        payload = arrow_job_payload(mapping,[(0,{"id":777,"v":777})],cfg,engine)
        with state_transaction(sidecar):
            sidecar.execute("""
                INSERT INTO jobs(table_name,lane,kind,payload,nrows,created)
                VALUES('t',0,'cdc',?,1,?)
            """,(payload,time.time()))
        delivery = claim_delivery(sidecar,"t",0,cfg)
        check(delivery is not None,"sidecar delivery claim")
        check(sidecar.execute(
            "SELECT delivery_id FROM jobs ORDER BY id LIMIT 1").fetchone()[0] is None,
            "claim must not rewrite legacy delivery_id inside Arrow BLOB job rows")
        check(sidecar.execute(
            "SELECT COUNT(*) FROM job_assignments WHERE delivery_id=?",(delivery,)).fetchone()[0] == 1,
            "delivery ownership must live in the sidecar table")
        sidecar.close()
        sidecar = init_state(sidecar_path)
        check(lane_blocking_delivery(sidecar,"t",0) == delivery,
              "sidecar assignment must survive restart")
        sidecar.close()

        legacy_path = os.path.join(directory,"assignment_legacy_migration.sqlite3")
        legacy = init_state(legacy_path)
        bootstrap(legacy,"test","source",("binlog.000001",4),["t"])
        with state_transaction(legacy):
            legacy.execute(
                "INSERT INTO deliveries(id,table_name,lane,prepared) "
                "VALUES('legacy_delivery','t',0,0)")
            legacy.execute("""
                INSERT INTO jobs(
                    table_name,lane,kind,payload,nrows,logical_bytes,created,delivery_id)
                VALUES('t',0,'cdc',?,1,0,?,'legacy_delivery')
            """,(payload,time.time()))
            legacy.execute("DELETE FROM job_assignments")
            legacy.execute("""
                DELETE FROM meta
                WHERE key IN ('job_assignment_sidecar_v1','logical_job_bytes_v1')
            """)
        legacy.close()
        legacy = init_state(legacy_path)
        check(legacy.execute("""
            SELECT a.delivery_id
            FROM job_assignments a JOIN jobs j ON j.id=a.job_id
        """).fetchone()[0] == "legacy_delivery",
            "first startup must migrate legacy jobs.delivery_id into the sidecar")
        check(legacy.execute(
            "SELECT logical_bytes FROM jobs LIMIT 1").fetchone()[0] > 0,
            "first startup must restore logical byte budgets for legacy jobs")
        check(meta_get(legacy,"logical_job_bytes_v1") == 1,
            "logical byte migration marker must commit after legacy job sizing")
        legacy.close()
        passed.append("job assignment sidecar avoids mutable Arrow BLOB rows and migrates legacy state")

        retire_path = os.path.join(directory,"retired_jobs.sqlite3")
        retire = init_state(retire_path)
        bootstrap(retire,"test","source",("binlog.000001",4),["t"])
        retire_group = "retire_group"
        payload = arrow_job_payload(mapping,[(0,{"id":901,"v":901})],cfg,engine)
        with state_transaction(retire):
            retire.execute(
                "INSERT INTO snapshot_groups(id,table_name,cursor,is_last) "
                "VALUES(?,?,?,1)",
                (retire_group,"t",pack((901,))))
            job_id = retire.execute("""
                INSERT INTO jobs(
                    table_name,lane,kind,payload,nrows,logical_bytes,created,group_id)
                VALUES('t',0,'snapshot',?,1,?,?,?) RETURNING id
            """,(payload,arrow_payload_logical_bytes(payload),time.time(),retire_group)).fetchone()[0]
            retire.execute(
                "INSERT INTO deliveries(id,table_name,lane,prepared) VALUES('retire_d','t',0,1)")
            retire.execute(
                "INSERT INTO job_assignments(job_id,delivery_id) VALUES(?,'retire_d')",(job_id,))
            retire.execute("""
                INSERT INTO load_parts(delivery_id,part,label,payload,nrows,visible,json_bytes)
                VALUES('retire_d',0,'retire_label',X'00',1,1,1)
            """)
            meta_set(retire,"pending_bytes",arrow_payload_logical_bytes(payload))
        acknowledge_delivery(retire,"retire_d")
        check(retire.execute("SELECT 1 FROM jobs WHERE id=?",(job_id,)).fetchone() is not None,
              "ack must defer physical Arrow BLOB deletion")
        check(retire.execute("SELECT 1 FROM retired_jobs WHERE job_id=?",(job_id,)).fetchone() is not None,
              "ack must durably tombstone the completed job")
        check(retire.execute("SELECT 1 FROM active_jobs WHERE id=?",(job_id,)).fetchone() is None,
              "retired job must disappear from the logical queue immediately")
        check(retire.execute(
            "SELECT 1 FROM snapshot_groups WHERE id=?",(retire_group,)).fetchone() is None,
              "retirement must complete snapshot group before physical GC")
        retire.close()
        retire = init_state(retire_path)
        check(merge_candidate_lanes(retire,"t") == [],
              "restart must never reschedule a durable retired job")
        count,gc_bytes = retired_job_gc_batch(retire)
        check(count == 1 and gc_bytes > 0 and
              retire.execute("SELECT 1 FROM jobs WHERE id=?",(job_id,)).fetchone() is None and
              retire.execute("SELECT 1 FROM retired_jobs WHERE job_id=?",(job_id,)).fetchone() is None,
              "background GC must remove retired Arrow jobs and tombstones")
        retire.close()
        passed.append("durable job retirement survives restart before bounded background GC")

        # Random source transactions, changing keys, filters, backfill races, arbitrary lane completion order.
        for seed in range(24):
            rng = random.Random(seed)
            con = init_state(os.path.join(directory,f"random_{seed}.sqlite3"))
            bootstrap(con,"test","source",("binlog.000001",4),["t"])
            model = {i:{"id":i,"v":i} for i in range(12)}
            snapshot = [dict(v) for v in model.values()]
            sink,labels = {},set()
            for tick in range(1,31):
                key = rng.randrange(20)
                before = model.get(key)
                ops = []
                if before:
                    ops.append((1,dict(before)))
                if rng.random() < 0.7:
                    after = {"id":key,"v":rng.randrange(-3,100)}
                    model[key] = after
                    ops.append((0,dict(after)))
                else:
                    model.pop(key,None)
                commit(con,ops,10+tick)
                if tick == 15:
                    stage_snapshot(con,mapping,snapshot,(11,),True,("binlog.000001",25),cfg,engine)
                if rng.random() < 0.25:
                    order = list(range(cfg["key_partitions"]))
                    rng.shuffle(order)
                    drain(con,sink,labels,order,table_mode=bool(seed%2))
                    con.close()
                    con = open_state(os.path.join(directory,f"random_{seed}.sqlite3"))
            drain(con,sink,labels,table_mode=bool(seed%2))
            expected = {k:v for k,v in model.items() if v["v"] >= 0}
            check(sink == expected,f"random recovery/backfill model differs, seed={seed}")
            check(con.execute("SELECT COUNT(*) FROM active_jobs").fetchone()[0] == 0,"journal drained")
            con.close()
        passed.append("24 randomized traces / 720 source transactions")

        typed = dict(src_table="typed",sr_table="typed",primary_key=["a","b"],full_filter=None,
                     sql="SELECT a,b,to_base64(blob) AS encoded,amount,json(doc) AS doc FROM arrow_batch",
                     _schema=[("a",pa.int64()),("b",pa.large_string()),("blob",pa.large_binary()),
                              ("amount",pa.decimal128(30,6)),("doc",pa.large_string())])
        validate_mapping(typed)
        row = {"a":1,"b":"中文","blob":None,"amount":decimal.Decimal("12345678901234567890.123456"),
               "doc":{b"key":[b"value",None]}}
        lines = transformed_lines(engine,typed,[(0,row)])
        joined = b"".join(p for p,_ in payload_chunks(lines,cfg))
        check(b"12345678901234567890.123456" in joined,"decimal precision loss")
        check(orjson.loads(joined)["doc"] == {"key":["value",None]},"binary JSON values")
        check(orjson.loads(joined)["encoded"] is None,"typed all-null BLOB")

        native_direct = dict(
            src_table="native_json",sr_table="native_json",
            primary_key="id",full_filter=None,
            sql="SELECT id,name,amount,payload FROM arrow_batch",
            _schema=[
                ("id",pa.uint64()),("name",pa.large_string()),
                ("amount",pa.int64()),("payload",pa.large_binary())],
            _target_sequence=True)
        validate_mapping(native_direct)
        native_direct["_output_columns"] = ["id","name","amount","payload"]
        native_direct["_binary_output_columns"] = {"payload"}
        native_direct["_target_json_columns"] = set()
        native_direct["_target_constraints"] = {}
        native_direct["_size_columns"] = {}
        wide_text = ("中文🙂\\\"\\\\\\n\\t" * 40) + "\x01"
        native_raw = pa.table({
            "id":pa.array([1,2,3],type=pa.uint64()),
            "name":pa.array([wide_text,None,"plain"],type=pa.large_string()),
            "amount":pa.array(
                [-9223372036854775808,0,9223372036854775807],
                type=pa.int64()),
            "payload":pa.array(
                [b"\\x00\\x01abc",None,b"xyz"],type=pa.large_binary()),
            "_sync_op":pa.array([0,1,0],type=pa.int8()),
            "_sync_order":pa.array([0,1,2],type=pa.int64()),
        })
        previous_native_json = os.environ.get("CDC_NATIVE_JSON")
        try:
            os.environ["CDC_NATIVE_JSON"] = "off"
            duckdb_lines = transformed_lines(
                engine,native_direct,native_raw,sequence=9,
                delivery_dense_order=True)
            duckdb_wire = b"".join(
                p for p,_ in payload_chunks(duckdb_lines,cfg))
            os.environ["CDC_NATIVE_JSON"] = "required"
            native_lines = transformed_lines(
                engine,native_direct,native_raw,sequence=9,
                delivery_dense_order=True)
            native_wire = b"".join(
                p for p,_ in payload_chunks(native_lines,cfg))
        finally:
            if previous_native_json is None:
                os.environ.pop("CDC_NATIVE_JSON",None)
            else:
                os.environ["CDC_NATIVE_JSON"] = previous_native_json
        check(
            native_wire == duckdb_wire,
            "native JSON must equal DuckDB JSON byte-for-byte")
        check(
            [orjson.loads(line) for line in native_wire.splitlines()]
            == [orjson.loads(line) for line in duckdb_wire.splitlines()],
            "native JSON semantic rows must equal DuckDB")
        passed.append(
            "native Arrow JSON matches DuckDB for UTF-8 escaping, NULL, "
            "Base64, integer bounds and sequence")

        stream_rows = [
            (0,dict(
                a=i,b=f"k{i}",blob=None,
                amount=decimal.Decimal(f"{i}.000001"),
                doc={"i":i}))
            for i in range(7)]
        materialized = transformed_lines(engine,typed,stream_rows)
        stream = transformed_line_batches(
            engine,typed,stream_rows,batch_rows=2)
        streamed_parts = []
        streamed_overflow = []
        try:
            for batch_lines,batch_overflow in stream:
                streamed_parts.extend(
                    payload for payload,_ in payload_chunks(batch_lines,cfg))
                streamed_overflow.extend(batch_overflow)
        finally:
            stream.close()
        check(
            b"".join(streamed_parts)
            == b"".join(p for p,_ in payload_chunks(materialized,cfg))
            and not streamed_overflow,
            "RecordBatch streaming JSON must equal materialized output")

        star = dict(mapping,sql="SELECT * FROM arrow_batch",full_filter=None)
        validate_mapping(star)
        check(len(transformed_lines(engine,star,[(0,{"id":1,"v":None})])) == 1,"star projection metadata")
        sliced = pa.chunked_array([pa.array(["unused","甲乙\n","quote\"\\\n","z\n"]).slice(1)])
        small = dict(cfg,batch_bytes=7)
        check(b"".join(p for p,_ in payload_chunks(sliced,small)) == '甲乙\nquote"\\\nz\n'.encode(),
              "Arrow string offset slicing")
        passed.append("composite keys, exact decimal JSON, null BLOB, UTF-8, Arrow slices")
        for sql in ("SELECT id,sum(v) FROM arrow_batch GROUP BY id",
                    "SELECT id,v FROM arrow_batch LIMIT 1",
                    "SELECT id,random() AS v FROM arrow_batch",
                    "SELECT id,v FROM another_table"):
            rejects(lambda sql=sql: validate_mapping(dict(mapping,sql=sql)),"unsupported SQL accepted")
        passed.append("fail-closed SQL validation")
        auto_mapping = dict(src_table="auto_src",sr_table="auto_dst",primary_key="id")
        auto_columns = [
            ("id","bigint","bigint unsigned","NO",None,""),
            ("name","varchar","varchar(32)","YES","utf8mb4_general_ci",""),
            ("created","datetime","datetime(6)","YES",None,""),
            ("body","longtext","longtext","YES","utf8mb4_general_ci",""),
            ("payload","longblob","longblob","YES",None,""),
        ]
        auto_ddl = target_create_ddl(
            auto_mapping,
            [("id","UBIGINT"),("name","VARCHAR"),("created","TIMESTAMP"),
             ("body","VARCHAR"),("payload","BLOB"),("derived","BLOB")],
            auto_columns,
            {"id":"row id","name":"display name","body":"large body","payload":"large binary"},
            'source "comment"')
        check("LARGEINT NOT NULL COMMENT \"row id\"" in auto_ddl and
              "PRIMARY KEY(" in auto_ddl,
              "automatic target DDL must preserve a non-null widened primary key and source comment")
        check("VARCHAR(128) NULL COMMENT \"display name\"" in auto_ddl and
              "`created` DATETIME NULL" in auto_ddl and
              "DATETIME(" not in auto_ddl and
              "VARCHAR(1048576) NULL COMMENT \"large body\"" in auto_ddl and
              "size_body" in auto_ddl and
              "VARCHAR(1048576) NULL COMMENT \"large binary\"" in auto_ddl and
              "size_payload" in auto_ddl and
              "size_derived" in auto_ddl and
              'COMMENT "source \\"comment\\""' in auto_ddl and
              "DISTRIBUTED BY HASH(" in auto_ddl,
              "automatic target DDL must adapt MySQL types, copy comments, and track oversized/unknown values")
        binary_mapping = dict(
            src_table="binary",sr_table="binary",primary_key="id",full_filter=None,
            sql="SELECT id,payload FROM arrow_batch",
            _schema=[("id",pa.int64()),("payload",pa.large_binary())],
            _target_sequence=False,
        )
        validate_mapping(binary_mapping)
        binary_desc = mapping_output_description(binary_mapping,cfg)
        binary_mapping["_binary_output_columns"] = {
            name for name,duck_type in binary_desc if str(duck_type).upper() == "BLOB"
        }
        binary_json = b"".join(
            payload for payload,_ in payload_chunks(
                transformed_lines(engine,binary_mapping,[(0,{"id":1,"payload":b"\x00\xffabc"})]),cfg))
        check(orjson.loads(binary_json)["payload"] == "AP9hYmM=",
              "direct BLOB output must be Base64 before JSON Stream Load")

        overflow_mapping = dict(
            src_table="overflow",sr_table="overflow",primary_key="id",full_filter=None,
            sql="SELECT id,value,required FROM arrow_batch",
            _schema=[("id",pa.int64()),("value",pa.large_string()),("required",pa.large_string())],
            _target_sequence=False,_output_columns=["id","value","required"],
            _size_columns={"value":"size_value"},
            _target_constraints={
                "value":dict(target_type="varchar(4)",limit_bytes=4,nullable=True,
                             primary_key=False,value_encoding="utf8"),
                "required":dict(target_type="varchar(4)",limit_bytes=4,nullable=False,
                                primary_key=False,value_encoding="utf8"),
            },
        )
        validate_mapping(overflow_mapping)
        safe_lines,safe_overflow = transformed_lines(
            engine,overflow_mapping,[(0,{"id":1,"value":"12345","required":"1234"})],
            collect_overflow=True)
        safe_row = orjson.loads(b"".join(p for p,_ in payload_chunks(safe_lines,cfg)))
        check(safe_row["value"] is None and safe_row["size_value"] == 5 and
              safe_row["required"] == "1234",
              "nullable target overflow must become NULL while preserving its actual byte size")
        check(len(safe_overflow) == 1 and not safe_overflow[0]["fatal"] and
              safe_overflow[0]["actual_bytes"] == 5 and safe_overflow[0]["limit_bytes"] == 4,
              "nullable overflow must be identified with exact byte sizes")
        _,fatal_overflow = transformed_lines(
            engine,overflow_mapping,[(0,{"id":2,"value":"ok","required":"12345"})],
            collect_overflow=True)
        check(len(fatal_overflow) == 1 and fatal_overflow[0]["fatal"],
              "NOT NULL overflow must be fail-closed")
        unicode_lines,unicode_overflow = transformed_lines(
            engine,overflow_mapping,[
                (0,{"id":3,"value":"中文","required":"ok"}),
                (0,{"id":4,"value":"😀","required":"ok"}),
                (0,{"id":5,"value":"\\x41","required":"ok"}),
            ],collect_overflow=True)
        unicode_rows = [orjson.loads(line) for line in
                        b"".join(p for p,_ in payload_chunks(unicode_lines,cfg)).splitlines()]
        check(unicode_rows[0]["value"] is None and unicode_rows[1]["value"] == "😀" and
              unicode_rows[2]["value"] == "\\x41",
              "UTF-8 byte limits must not use VARCHAR-to-BLOB escape parsing")
        sizes = {(item["pk_json"],item["actual_bytes"]) for item in unicode_overflow}
        check(any('"id":3' in pk and size == 6 for pk,size in sizes),
              "Chinese VARCHAR length must be six UTF-8 bytes")
        check(not any('"id":4' in pk for pk,_ in sizes),
              "emoji exactly at four UTF-8 bytes must fit varchar(4)")
        check(not any('"id":5' in pk for pk,_ in sizes),
              "literal backslash-x text must keep its four UTF-8 bytes")

        overflow_path = os.path.join(directory,"field_overflow.sqlite3")
        overflow_db = init_state(overflow_path)
        overflow_jobs = [(1,b"","binlog.000001",123,1700000000.0)]
        persist_field_overflows(
            overflow_db,overflow_mapping,"delivery_overflow",safe_overflow,overflow_jobs)
        saved = overflow_db.execute("""
            SELECT pk_json,column_name,actual_bytes,limit_bytes,value_encoding,value,action,source_pos
            FROM field_overflow WHERE delivery_id='delivery_overflow'
        """).fetchall()
        check(len(saved) == 1 and saved[0][1:] ==
              ("value",5,4,"utf8",b"12345","null",123),
              "nullable overflow must be durably journaled with recoverable value")
        persist_field_overflows(
            overflow_db,overflow_mapping,"delivery_overflow",safe_overflow,overflow_jobs)
        check(overflow_db.execute("""
            SELECT COUNT(*) FROM field_overflow WHERE delivery_id='delivery_overflow'
        """).fetchone()[0] == 1,"overflow journal regeneration must be idempotent")
        rejects(lambda: persist_field_overflows(
            overflow_db,overflow_mapping,"delivery_fatal",fatal_overflow,overflow_jobs),
            "required overflow must stop after durable journaling")
        check(overflow_db.execute("""
            SELECT action FROM field_overflow WHERE delivery_id='delivery_fatal'
        """).fetchone()[0] == "stop","fatal overflow must be saved before stopping")
        overflow_db.close()
        passed.append("durable field overflow journal, idempotent regeneration, fatal-before-stop evidence")

        check(
            starrocks_output_type("TIMESTAMP") == "DATETIME"
            and mysql_pk_target_type(
                ("ts","datetime","datetime(6)","NO",None,"")) == "DATETIME"
            and mysql_output_target_type(
                ("ts","datetime","datetime(6)","YES",None,""),"TIMESTAMP")[0] == "DATETIME",
            "StarRocks automatic temporal DDL must use DATETIME without precision syntax")
        rejects(lambda: starrocks_output_type("STRUCT(a INTEGER)"),
                "unsupported complex output type must require an explicit target")
        passed.append("automatic target DDL, Base64 BLOB transport, target-aware NULL overflow guard")
        recovery_selftest(directory,mapping,cfg)
        passed.append("expired merge transaction recovery, five-lane restart without resend, read retries and signals")
        transaction_selftest(directory,engine,mapping,cfg)
        passed.append("table-wide mixed batching; 2PC crashes at begin/load/prepare/commit; timeout, pressure, expired identity")
        transaction_http_selftest(mapping)
        passed.append("real libcurl transaction POST/PUT/GET, gzip and authenticated 307 redirect on loopback")
        merge_async_selftest(directory,engine,mapping,cfg)
        passed.append("Merge Commit async headers, server TxnId persistence and VISIBLE gating")
        scale_cfg = dict(writer_min=2,writer_max=8,rowset_yellow=500,version_recovery_checks=2)
        scale_runtime = dict(control_lock=threading.Lock(),active_writers={"t":8},
                             last_pressure={"t":0},last_scale={"t":0},
                             pressure_until={"t":0},load_events={},
                             version_recovery={"t":False},version_recovery_good={"t":0})
        writer_pressure(scale_runtime,"t",scale_cfg,"selftest compaction pressure",severe=True)
        check(writer_target(scale_runtime,"t") == 4,"pressure must halve writer concurrency")
        set_writer_target(scale_runtime,"t",scale_cfg,5,"selftest recovery")
        check(writer_target(scale_runtime,"t") == 5,"healthy recovery must increase gradually")
        enter_version_recovery(scale_runtime,"t",scale_cfg,"selftest too many versions")
        check(version_recovery_active(scale_runtime,"t"),"too many versions must hard-pause writers")
        check(writer_target(scale_runtime,"t") == 2,"version recovery must fall to minimum writers")
        check(not update_version_recovery(scale_runtime,"t",scale_cfg,499),
              "one healthy rowset check must not resume writes")
        check(version_recovery_active(scale_runtime,"t"),"version recovery requires consecutive confirmation")
        check(update_version_recovery(scale_runtime,"t",scale_cfg,499),
              "second healthy rowset check must resume writes")
        check(not version_recovery_active(scale_runtime,"t"),"version recovery must clear after confirmation")
        passed.append("adaptive writer AIMD plus hard version pause and rowset-confirmed recovery")

        original_preflight = vars(cdc)["preflight"]
        original_read_config = vars(cdc)["read_config"]
        try:
            candidate_probe = {}
            def fake_candidate_preflight(config, **kwargs):
                candidate_probe["mysql_host"] = config["mysql"]["host"]
                candidate_probe["state"] = config["state"]
                candidate_probe["create_missing"] = kwargs.get("create_missing")
                candidate_probe["allow_missing_targets"] = kwargs.get(
                    "allow_missing_targets")
                return [],"source",("binlog.000001",4),None,set(),"fingerprint"
            vars(cdc)["preflight"] = fake_candidate_preflight
            candidate_state = os.path.join(directory,"candidate-state.sqlite3")
            candidate_variables = {
                "CDC_MYSQL_HOST":"txn-mysql",
                "CDC_MYSQL_PORT":"3306",
                "CDC_MYSQL_USER":"u",
                "CDC_MYSQL_PASSWORD":"p",
                "CDC_MYSQL_SCHEMA":"db",
                "CDC_SR_FE_HOST":"txn-sr",
                "CDC_SR_FE_PORT":"8030",
                "CDC_SR_QUERY_PORT":"9030",
                "CDC_SR_USER":"u",
                "CDC_SR_PASSWORD":"p",
                "CDC_SR_DB":"db",
                "CDC_SERVER_ID":"10086",
                "CDC_STATE_FILE":candidate_state,
            }
            validate_local_catalog_publish(
                dict(
                    version=99,revision=1,plan_hash="candidate",
                    mappings=[dict(
                        src_table="t",sr_table="t",primary_key=None,
                        full_filter=None,sql="SELECT id FROM arrow_batch")],
                    macros=[],udfs=[],_variables=candidate_variables),
                "validate")
            check(
                candidate_probe == dict(
                    mysql_host="txn-mysql",
                    state=os.path.abspath(candidate_state),
                    create_missing=False,
                    allow_missing_targets=True),
                "local publish validation must use the uncommitted SQL transaction config")
            vars(cdc)["read_config"] = lambda: dict(
                mysql=dict(host="txn-mysql"),
                state=os.path.abspath(candidate_state),
                catalog=os.path.join(directory,"candidate-catalog.sqlite3"))
            candidate_probe.clear()
            install_result = validate_local_catalog_publish(
                dict(
                    version=99,revision=1,plan_hash="candidate",
                    mappings=[dict(
                        src_table="t",sr_table="t",primary_key=None,
                        full_filter=None,sql="SELECT id FROM arrow_batch")],
                    macros=[],udfs=[]),
                "install")
            check(
                install_result["status"] == "installed_offline"
                and install_result["target_creation"] == "created_or_verified"
                and candidate_probe["mysql_host"] == "txn-mysql"
                and candidate_probe["create_missing"] is True
                and candidate_probe["allow_missing_targets"] is None,
                "offline install must create missing targets after catalog commit")
            candidate_probe.clear()
            config_result = validate_local_catalog_publish(
                dict(
                    version=99,revision=1,plan_hash="candidate",
                    mappings=[],macros=[],udfs=[],
                    _variables=candidate_variables),
                "validate_config")
            check(
                config_result["status"] == "validated_config"
                and candidate_probe["mysql_host"] == "txn-mysql"
                and candidate_probe["create_missing"] is False
                and candidate_probe["allow_missing_targets"] is True,
                "complete config-only deployments must run online preflight before commit")
            incomplete_result = validate_local_catalog_publish(
                dict(
                    version=0,revision=0,plan_hash="",
                    mappings=[],macros=[],udfs=[],
                    _variables={"CDC_STATUS_SECONDS":"45"}),
                "validate_config")
            check(
                incomplete_result["status"] == "config_incomplete",
                "incomplete config-only drafts must persist without pretending online validation")
        finally:
            vars(cdc)["preflight"] = original_preflight
            vars(cdc)["read_config"] = original_read_config
        passed.append(
            "transactional catalog preflight validates before commit and creates targets after commit")

        bootstrap_catalog = os.path.join(directory,"bootstrap-catalog.sqlite3")
        bootstrap_paths = dict(
            catalog=bootstrap_catalog,
            socket=os.path.join(directory,"bootstrap.sock"),
            seed=None,
            state=os.path.join(directory,"bootstrap-state.sqlite3"))
        check(
            not catalog_bootstrap_ready(bootstrap_paths),
            "empty catalog must keep daemon in bootstrap control-plane mode")
        bootstrap_ready = threading.Event()
        cdc_catalog.execute(
            bootstrap_catalog,
            "CREATE TABLE starrocks.target AS SELECT id FROM mysql.source")
        waiting = bootstrap_catalog_publish_callback(
            bootstrap_paths,bootstrap_ready,dict(version=0),"install_config")
        check(
            waiting["status"] == "bootstrap_waiting"
            and not bootstrap_ready.is_set(),
            "draft sink plus incomplete config must remain control-plane only")
        bootstrap_values = {
            "CDC_MYSQL_HOST":"mysql",
            "CDC_MYSQL_PORT":"3306",
            "CDC_MYSQL_USER":"u",
            "CDC_MYSQL_PASSWORD":"p",
            "CDC_MYSQL_SCHEMA":"db",
            "CDC_SR_FE_HOST":"starrocks",
            "CDC_SR_FE_PORT":"8030",
            "CDC_SR_QUERY_PORT":"9030",
            "CDC_SR_USER":"u",
            "CDC_SR_PASSWORD":"p",
            "CDC_SR_DB":"db",
            "CDC_SERVER_ID":"10086",
        }
        for name,value in bootstrap_values.items():
            literal = value if name.endswith(("PORT","SERVER_ID")) else "'" + value + "'"
            cdc_catalog.execute(
                bootstrap_catalog,f"SET VARIABLE {name} = {literal}")
        check(
            not catalog_bootstrap_ready(bootstrap_paths),
            "complete config must not start before a validated plan is published")
        original_local_validate = vars(cdc)["validate_local_catalog_publish"]
        try:
            vars(cdc)["validate_local_catalog_publish"] = (
                lambda result,phase: dict(
                    status="synthetic_validated",
                    version=int(result.get("version",0))))
            starting = bootstrap_catalog_publish_callback(
                bootstrap_paths,bootstrap_ready,dict(version=0),"install_config")
        finally:
            vars(cdc)["validate_local_catalog_publish"] = original_local_validate
        check(
            starting["status"] == "daemon_starting"
            and starting.get("promoted_draft")
            and bootstrap_ready.is_set(),
            "last config commit must validate/promote an earlier draft sink and wake daemon")
        promoted = cdc_catalog.load_plan(bootstrap_catalog)
        check(
            len(promoted["mappings"]) == 1
            and promoted["mappings"][0]["src_table"] == "source"
            and promoted["mappings"][0]["sr_table"] == "target",
            "bootstrap draft promotion must persist the intended source/sink plan")
        passed.append(
            "daemon-first bootstrap waits for a sink and promotes config-completed drafts")

        startup_state = dict(
            lock=threading.RLock(),mode="starting",cfg=None,runtime=None,
            paths=bootstrap_paths,ready=threading.Event(),
            listening=threading.Event(),errors=[],
            process_control=dict(
                stop=threading.Event(),reason="stopped",requested=False))
        try:
            catalog_control_publish(
                startup_state,dict(version=1),"validate")
            raise AssertionError("starting catalog accepted a mutation")
        except cdc_catalog.CatalogBusyError:
            pass

        original_publish_callback = vars(cdc)["catalog_publish_callback"]
        try:
            vars(cdc)["catalog_publish_callback"] = (
                lambda cfg,runtime,result,phase: dict(
                    status="routed_running",phase=phase,
                    version=int(result.get("version",0))))
            running_runtime = dict(
                stop=threading.Event(),error_lock=threading.Lock(),errors=[],
                plan_lock=threading.RLock(),active_plan_version=7)
            with startup_state["lock"]:
                startup_state["mode"] = "running"
                startup_state["cfg"] = dict(catalog=bootstrap_catalog)
                startup_state["runtime"] = running_runtime
            routed = catalog_control_publish(
                startup_state,dict(version=8),"install")
            check(
                routed["status"] == "routed_running"
                and routed["phase"] == "install",
                "running catalog control must route callbacks to hot-plan logic")
        finally:
            vars(cdc)["catalog_publish_callback"] = original_publish_callback

        original_catalog_server = cdc_catalog.server_worker
        try:
            def failed_catalog_server(*args, **kwargs):
                raise RuntimeError("synthetic catalog bind failure")
            cdc_catalog.server_worker = failed_catalog_server
            process_control = dict(
                stop=threading.Event(),reason="stopped",requested=False)
            catalog_runtime = dict(
                stop=process_control["stop"],
                error_lock=threading.Lock(),errors=[])
            control_state = dict(
                lock=threading.RLock(),mode="running",
                cfg=dict(catalog=bootstrap_catalog),runtime=catalog_runtime,
                paths=bootstrap_paths,ready=threading.Event(),
                listening=threading.Event(),errors=[],
                process_control=process_control)
            catalog_control_worker(control_state)
            check(
                process_control["stop"].is_set()
                and catalog_runtime["errors"]
                and catalog_runtime["errors"][0][0] == "catalog_control_worker",
                "catalog control failure must fail-stop the running data plane")
        finally:
            cdc_catalog.server_worker = original_catalog_server
        passed.append(
            "catalog control transition rejects startup writes, routes running callbacks and fail-stops")

        report_state = os.path.join(directory,"reporting.sqlite3")
        report_cfg = dict(cfg,state=report_state,load_mode="merge_async",writer_max=8)
        report_runtime = dict(
            metrics=init_run_metrics([mapping]),control_lock=threading.Lock(),
            active_writers={"t":4},max_rowset={"t":42},version_recovery={"t":False},
        )
        metric_add_merge(report_runtime,"t",1001,100,900)
        metric_add_merge(report_runtime,"t",1001,120,700)
        metric_add_merge(report_runtime,"t",1002,80,500)
        metric_add_visible(report_runtime,"t","snapshot",3000,400000,1,1,1.2,0,2,3000,600000)
        metric_add_visible(report_runtime,"t","cdc",5,1200,1,1,1.1,3.0,1,5,1250)
        metric_add_visible(report_runtime,"t","cdc",7,1600,1,1,1.3,7.0,1,7,2100)
        record = metrics_status_record(
            report_runtime,report_cfg,[mapping],
            dict(backfill_done=0,backfill_tables=1,pending_jobs=2,pending_bytes=100,
                 prepared_bytes=0,inflight=1,oldest_queue_seconds=1.0,
                 durable_position="binlog.000001:100",stream_silence_seconds=0.0,
                 data_silence_seconds=0.0,heartbeats=1,cdc_transactions=2,
                 field_overflow_rows=0,merge_uncertain_rows=0))
        metrics_path,summary_path = report_paths(report_cfg)
        check(os.path.exists(metrics_path) and os.path.exists(summary_path),
              "automatic run reporting must create JSONL metrics and summary files")
        saved_summary = orjson.loads(open(summary_path,"rb").read())
        interval = saved_summary["tables"]["t"]["interval"]
        check(interval["snapshot_rows"] == 3000 and interval["cdc_rows"] == 12,
              "automatic reporting must retain snapshot and CDC row totals")
        check(interval["cdc_age_seconds"]["p50"] == 5.0 and
              interval["cdc_age_seconds"]["p99"] > 6.9,
              "automatic reporting must calculate CDC latency quantiles")
        check(interval["merge_requests"] == 3 and interval["merge_distinct_txns"] == 2,
              "automatic reporting must summarize Merge Commit grouping")
        check(interval["visible_json_rows"] == 3012 and
              abs(interval["avg_json_row_bytes"]-(603350/3012)) < 0.001,
              "automatic reporting must expose exact pre-Stream-Load JSON average row bytes")
        check(record["tables"]["t"]["active_writers"] == 4 and
              record["tables"]["t"]["max_rowset"] == 42,
              "automatic reporting must include writer and Rowset state")
        for index in range(METRIC_SAMPLE_LIMIT*3):
            metric_add_visible(
                report_runtime,"t","cdc",1,1,1,1,0.01,float(index),1,1,1)
        check(
            len(report_runtime["metrics"]["tables"]["t"]["total"]["visible_seconds"])
            <= METRIC_SAMPLE_LIMIT,
            "total metric samples must remain bounded during long runs")
        passed.append("automatic long-run JSONL metrics, summary, quantiles and merge grouping")
    engine.close()
    for name in passed:
        log("SELFTEST PASS "+name)
    log(f"SELFTEST OK: {len(passed)} offline regression groups")
    return 0



if __name__ == "__main__":
    raise SystemExit(selftest())
