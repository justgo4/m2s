#!/usr/bin/env python3
"""Bounded optional observations; never authoritative recovery state.

Only fixed stage names, hashed identities and numeric metadata are retained.
Monotonic times are comparable only within one instance. Ring loss and restart
make a chain incomplete, not successful. Default-off calls do no SQL or I/O.
"""
from collections import deque
from contextvars import ContextVar
import hashlib
import json
import math
import os
import threading
import time
import uuid


STAGES=frozenset({'source_durable','base_applied','task_frontier',
    'delivery_selected','prepare_begin','prepare_end','http_begin','http_accepted',
    'acceptance_saved','visible_wait_begin','remote_visible','visible_saved',
    'ack_begin','ack_end'})
NUMBERS=frozenset({'seq','part','txn','duration_seconds','nrows','logical_bytes',
    'lanes','plan_version','source_time_wall','oldest_created_wall','frontier'})
LOCK=threading.Lock()
CURRENT=ContextVar('cdc_event_trace_delivery',default=None)
STATE=None


def identity(value):
    return hashlib.sha256(str(value).encode('utf-8')).hexdigest()


def marker_key(value):
    """Hash the decimal form of one integer primary key.

    The longhaul sentinel and source_durable share this key. Raw key
    material is not stored in the trace.
    """
    if isinstance(value,bool) or not isinstance(value,int):
        raise ValueError('trace marker must be an integer primary key')
    return identity(str(int(value)))


def sampled(seq):
    state=STATE
    if state is None or seq is None:
        return False
    return int(seq)%state['every']==0


def note_metadata_error():
    state=STATE
    if state is None:
        return
    with LOCK:
        if STATE is state:
            state['metadata_errors']+=1


def _close_trace_file(state):
    handle=None if state is None else state.get('file')
    if handle is None:
        return
    state['file']=None
    try:
        handle.close()
    except Exception:
        pass


def configure(epoch,enabled=False,limit=2048,every=16,path=None,file_limit=8*1024*1024):
    global STATE
    new=None
    if enabled:
        new=dict(instance=uuid.uuid4().hex,epoch=identity(epoch),
                 limit=max(1,min(int(limit),8192)),every=max(1,int(every)),
                 next_id=0,dropped=0,metadata_errors=0,frontiers={},file=None,
                 file_bytes=0,file_limit=max(1,int(file_limit)),file_dropped=0,
                 events=deque(maxlen=max(1,min(int(limit),8192))))
        if path:
            try:
                parent=os.path.dirname(path)
                if parent:
                    os.makedirs(parent,exist_ok=True)
                handle=open(path,'a',encoding='utf-8')
                header=json.dumps(dict(
                    kind='m2s_event_trace_header',instance=new['instance'],
                    epoch=new['epoch'],every=new['every'],limit=new['limit']),
                    sort_keys=True,separators=(',',':'))+'\n'
                handle.write(header)
                handle.flush()
                new['file']=handle
                new['file_bytes']=len(header.encode('utf-8'))
            except Exception:
                new['metadata_errors']+=1
                new['file']=None
    with LOCK:
        old=STATE
        STATE=new
    _close_trace_file(old)
    CURRENT.set(None)


def enabled():
    return STATE is not None


def record(stage,seq=None,target=None,delivery=None,generation=None,marker=None,**numbers):
    state=STATE
    if state is None:
        return
    if stage not in STAGES or set(numbers)-NUMBERS:
        raise ValueError('unknown trace stage or numeric field')
    hashed_marker=None if marker is None else marker_key(marker)
    context=CURRENT.get()
    if context is not None and context.get("instance")!=state["instance"]:
        context=None
    if seq is not None and int(seq)%state['every']:
        return
    if seq is None and context is None and delivery is None:
        return
    event=dict(stage=stage,mono=time.monotonic(),wall=time.time())
    if context is not None:
        event.update(context)
    if seq is not None:
        event['seq']=int(seq)
    if target is not None:
        event['target']=identity(target)
    if delivery is not None:
        event['delivery']=identity(delivery)
    if generation is not None:
        event['generation']=identity(generation)
    if hashed_marker is not None:
        event['marker_key']=hashed_marker
    for key,value in numbers.items():
        if value is not None:
            if isinstance(value,bool) or not isinstance(value,(int,float)) or not math.isfinite(value):
                raise ValueError('trace metadata must be finite numeric values')
            event[key]=value
    with LOCK:
        if STATE is not state:
            return
        state['next_id']+=1
        event['id']=state['next_id']
        if len(state['events'])==state['limit']:
            state['dropped']+=1
        state['events'].append(event)
        handle=state.get('file')
        if handle is not None:
            line=json.dumps(dict(
                kind='m2s_event_trace_event',instance=state['instance'],
                epoch=state['epoch'],event=event),
                sort_keys=True,separators=(',',':'))+'\n'
            encoded=line.encode('utf-8')
            if state['file_bytes']+len(encoded)>state['file_limit']:
                state['file_dropped']+=1
            else:
                try:
                    handle.write(line)
                    handle.flush()
                    state['file_bytes']+=len(encoded)
                except Exception:
                    state['metadata_errors']+=1
                    state['file']=None
                    try:
                        handle.close()
                    except Exception:
                        pass


def select_delivery(con,table,delivery):
    state=STATE
    if state is None:
        return None
    # Observational reads cannot affect the journal/recovery outcome.
    try:
        rows=con.execute('''
            SELECT DISTINCT j.source_seq FROM jobs j
            JOIN job_assignments a ON a.job_id=j.id
            WHERE a.delivery_id=? AND j.source_seq IS NOT NULL
              AND j.source_seq % ? = 0 ORDER BY j.source_seq LIMIT 65
        ''',(delivery,state['every'])).fetchall()
        sequences=[int(row[0]) for row in rows[:64]]
        if not sequences and int(identity(delivery)[:8],16)%state['every']:
            return None
        row=con.execute('''
            SELECT SUM(j.nrows),SUM(j.logical_bytes),COUNT(DISTINCT j.lane),
                   MIN(j.created),MIN(j.plan_version)
            FROM jobs j JOIN job_assignments a ON a.job_id=j.id
            WHERE a.delivery_id=?
        ''',(delivery,)).fetchone()
        numbers=dict(nrows=int(row[0] or 0),logical_bytes=int(row[1] or 0),
                     lanes=int(row[2] or 0),oldest_created_wall=row[3],plan_version=row[4])
        if any(isinstance(value,bool) or not isinstance(value,(int,float))
               or not math.isfinite(value) for value in numbers.values() if value is not None):
            raise ValueError('invalid diagnostic metadata')
    except Exception:
        with LOCK:
            state['metadata_errors']+=1
        return None
    token=CURRENT.set(dict(instance=state['instance'],target=identity(table),delivery=identity(delivery),
                           sequences=sequences,sequences_truncated=len(rows)>64))
    record('delivery_selected',**numbers)
    return token


def finish_delivery(token):
    if token is not None:
        CURRENT.reset(token)


def task_frontier(target,generation,seq,seconds):
    state=STATE
    if state is None or seq is None:
        return
    key=(identity(target),identity(generation))
    with LOCK:
        if STATE is not state or state['frontiers'].get(key)==int(seq):
            return
        if key not in state['frontiers'] and len(state['frontiers'])>=128:
            state['frontiers'].pop(next(iter(state['frontiers'])))
        state['frontiers'][key]=int(seq)
    record('task_frontier',seq=seq,target=target,generation=generation,
           duration_seconds=seconds,frontier=1)


def snapshot():
    with LOCK:
        state=STATE
        if state is None:
            return dict(enabled=False)
        return dict(enabled=True,instance=state['instance'],epoch=state['epoch'],
                    clock='process_monotonic',source_time='binlog wall time; precision not guaranteed',
                    coverage='shared source durable/base; merge_async output; observed task frontier; source_durable.marker_key hashes the decimal max single-column integer primary key when that seq is sampled',
                    authoritative=False,every=state['every'],limit=state['limit'],
                    dropped=state['dropped'],metadata_errors=state['metadata_errors'],
                    events=[dict(event,**({'sequences':list(event['sequences'])}
                              if 'sequences' in event else {}))
                            for event in state['events']])
