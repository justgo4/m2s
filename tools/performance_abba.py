#!/usr/bin/env python3
"""Run immutable A/B/B/A on one host with fresh disposable services per trial."""
import argparse
import json
import os
from pathlib import Path
import platform
import re
import socket
import subprocess
import sys
import time
import uuid

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import validation_profiles
from tools import validation_run


def command(argv,cwd=ROOT,env=None,log=None):
    return subprocess.run([str(item) for item in argv],cwd=cwd,env=env,
                          stdout=log if log else subprocess.PIPE,
                          stderr=subprocess.STDOUT,text=True,check=True)


def resolve_revision(value,root=ROOT):
    if not re.fullmatch('[0-9a-f]{40}',value):
        raise ValueError('A/B must be full immutable commit SHAs')
    actual=command(['git','rev-parse',value+'^{commit}'],root).stdout.strip()
    if actual!=value:
        raise ValueError('revision is not the requested commit')
    return actual


def experiment_plan(a,b,directory,profile='small',root=ROOT):
    directory=Path(directory).resolve();root=Path(root).resolve()
    if directory.exists():
        raise ValueError('experiment directory must be new; preserve previous results')
    if directory==root or root in directory.parents:
        raise ValueError('experiment directory must be outside the tested checkout')
    if profile not in ('smoke','small'):
        raise ValueError('controlled hosted comparisons support smoke/small only')
    a=resolve_revision(a,root);b=resolve_revision(b,root)
    validation_run.revision(root)
    return dict(kind='same_host_abba_diagnostic',certification=False,
                profile=profile,parameters=validation_profiles.parameters(profile),
                order=[dict(label=label,revision=rev) for label,rev in
                       [('A',a),('B',b),('B',b),('A',a)]],
                same_revision_control=a==b,directory=str(directory))


def require_free_ports(ports=(3306,8030,8040,9030)):
    for port in ports:
        with socket.socket() as sock:
            sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
            sock.bind(('127.0.0.1',port))


def wait_mysql():
    import pymysql
    for _ in range(180):
        try:
            con=pymysql.connect(host='127.0.0.1',user='root',connect_timeout=2)
            con.close();return
        except pymysql.MySQLError:
            time.sleep(1)
    raise RuntimeError('disposable MySQL did not become ready')


def image_identity(image):
    info=json.loads(command(['docker','image','inspect',image]).stdout)[0]
    return dict(id=info['Id'],digests=info.get('RepoDigests',[]))


def run(plan,trace_enabled):
    require_free_ports()
    directory=Path(plan['directory']);directory.mkdir(mode=0o700)
    env=dict(os.environ,CDC_RESOURCE_CPU_CORES='2',CDC_SQLITE_WRITE_TIMING='1',
             CDC_EVENT_TRACE=str(trace_enabled),CDC_EVENT_TRACE_EVERY='16',
             CDC_EVENT_TRACE_LIMIT='2048')
    # Resolve tags once; all four trials run the exact local image IDs.
    images={}
    for key,tag in [('mysql','mysql:8.4.6'),('starrocks','starrocks/allin1-ubuntu:4.1.1')]:
        command(['docker','pull',tag]);images[key]=image_identity(tag)
    plan.update(host=dict(machine=platform.machine(),kernel=platform.release(),
                         cpus=os.cpu_count(),boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip()),
                python=sys.version,dependencies=command([sys.executable,'-m','pip','freeze']).stdout.splitlines(),
                images=images,environment={key:env[key] for key in
                    ('CDC_RESOURCE_CPU_CORES','CDC_SQLITE_WRITE_TIMING','CDC_EVENT_TRACE',
                     'CDC_EVENT_TRACE_EVERY','CDC_EVENT_TRACE_LIMIT',
                     'CDC_MERGE_VISIBILITY_PIPELINE','CDC_MERGE_VISIBILITY_PER_SINK',
                     'CDC_COLD_BUILD_ADMISSION','CDC_COLD_BUILD_ROWS') if key in env},trials=[])
    validation_run.atomic_json(directory/'experiment.json',plan)
    for index,trial in enumerate(plan['order'],1):
        trial=dict(trial,index=index);plan['trials'].append(trial)
        trial_dir=directory/('%d-%s'%(index,trial['label']));trial_dir.mkdir(mode=0o700)
        checkout=trial_dir/'checkout';names=[]
        try:
            require_free_ports()
            command(['git','worktree','add','--detach',checkout,trial['revision']])
            with (trial_dir/'setup.log').open('w') as log:
                command(['cmake','-S','native','-B','build/native','-DCMAKE_BUILD_TYPE=Release'],checkout,log=log)
                command(['cmake','--build','build/native','--parallel','2'],checkout,log=log)
                validation_run.revision(checkout)
                parameters=json.loads(command([sys.executable,'-c',
                    'import json,validation_profiles; print(json.dumps(validation_profiles.parameters('+repr(plan['profile'])+')))'],checkout).stdout)
                if parameters!=plan['parameters']:
                    raise RuntimeError('profile parameters differ between revisions')
                if trace_enabled and not (checkout/'cdc_event_trace.py').exists():
                    raise RuntimeError('trace enabled requires instrumentation in both revisions')
                for key in ('mysql','starrocks'):
                    name='m2s-abba-'+uuid.uuid4().hex;names.append(name)
                    args=['docker','run','--detach','--name',name]
                    if key=='mysql':
                        args+=['-p','127.0.0.1:3306:3306','-e','MYSQL_ALLOW_EMPTY_PASSWORD=yes',
                               images[key]['id'],'--server-id=1','--log-bin=mysql-bin','--gtid-mode=ON',
                               '--enforce-gtid-consistency=ON','--binlog-format=ROW',
                               '--binlog-row-image=FULL','--binlog-row-metadata=FULL']
                    else:args+=['--network','host',images[key]['id']]
                    command(args,log=log)
                wait_mysql()
                inspections=[json.loads(command(['docker','inspect',name]).stdout)[0] for name in names]
                trial['services']=[dict(image=item['Image'],resources={key:item['HostConfig'][key]
                    for key in ('Memory','NanoCpus','CpuQuota','CpuPeriod')}) for item in inspections]
            if index>1 and trial['services']!=plan['trials'][0].get('services'):
                raise RuntimeError('service image/resource fingerprints differ')
            pids=[item['State']['Pid'] for item in inspections]
            if any(pid<=0 for pid in pids):raise RuntimeError('service process missing')
            validation_run.atomic_json(directory/'experiment.json',plan)
            with (trial_dir/'supervisor.log').open('w') as log:
                result=subprocess.run([sys.executable,str(checkout/'tools/validation_run.py'),
                    'run','--isolated','--profile',plan['profile'],'--run-directory',str(trial_dir/'run'),
                    '--mysql-resource-pid',str(pids[0]),'--starrocks-fe-resource-pid',str(pids[1]),
                    '--starrocks-be-resource-pid',str(pids[1])],cwd=checkout,env=env,
                    stdout=log,stderr=subprocess.STDOUT)
            trial['exit']=result.returncode
            status=validation_run.read_json(trial_dir/'run/status.json')
            if status['revision']!=trial['revision']:raise RuntimeError('trial revision mismatch')
            trial['status']=status
            metrics=trial_dir/'run/longhaul-daemon-metrics.jsonl'
            if metrics.exists() and (checkout/'tools/event_trace_report.py').exists():
                command([sys.executable,checkout/'tools/event_trace_report.py',metrics,
                         '--output',trial_dir/'event-trace-report.json'],checkout)
        except Exception as exc:
            trial.update(exit=1,error=str(exc))
        finally:
            for name in names:
                with (trial_dir/'services.log').open('a') as log:
                    subprocess.run(['docker','logs',name],stdout=log,stderr=subprocess.STDOUT)
                    subprocess.run(['docker','rm','--force','--volumes',name],stdout=log,stderr=subprocess.STDOUT)
            validation_run.atomic_json(directory/'experiment.json',plan)
        print(json.dumps(dict(index=index,label=trial['label'],revision=trial['revision'],
                              exit=trial['exit'])),flush=True)
        # A failed gate is retained and the next independent trial still runs.
    plan['all_gates_passed']=all(trial.get('exit')==0 and trial.get('status',{}).get('gate_passed')
                                 for trial in plan['trials'])
    validation_run.atomic_json(directory/'experiment.json',plan)
    return 0 if plan['all_gates_passed'] else 1


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--a',required=True);parser.add_argument('--b',required=True)
    parser.add_argument('--run-directory',type=Path,required=True)
    parser.add_argument('--profile',choices=('smoke','small'),default='small')
    parser.add_argument('--trace',type=int,choices=(0,1),default=1)
    parser.add_argument('--isolated',action='store_true');parser.add_argument('--plan-only',action='store_true')
    args=parser.parse_args()
    plan=experiment_plan(args.a,args.b,args.run_directory,args.profile)
    if args.plan_only:print(json.dumps(plan,indent=2),flush=True);return 0
    if not args.isolated:parser.error('--isolated is required for disposable databases')
    return run(plan,args.trace)


if __name__=='__main__':raise SystemExit(main())
