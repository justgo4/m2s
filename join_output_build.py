#!/usr/bin/env python3
"""Resumable fixed-W pair output construction for private JOIN generations.

Every chunk validates the frozen backing state before inserting bounded output.
Unsealed commits are invisible to readers/writers. The source generation pin is
owned by join_generation until it atomically establishes its source consumer.
"""
import pickle
import time

import join_outbox
import join_state


def install(con):
    if con.in_transaction:
        raise RuntimeError('JOIN output build install requires no transaction')
    join_outbox.ensure_installed(con)
    con.execute('''
        CREATE TABLE IF NOT EXISTS join_output_builds(
            consumer_id TEXT NOT NULL, source_seq INTEGER NOT NULL,
            state_id TEXT NOT NULL REFERENCES join_states(state_id) ON DELETE CASCADE,
            spec_hash TEXT NOT NULL,
            left_cursor BLOB, current_left BLOB, right_cursor BLOB,
            scan_complete INTEGER NOT NULL DEFAULT 0,
            revision INTEGER NOT NULL DEFAULT 0,
            abandoned INTEGER NOT NULL DEFAULT 0,
            updated REAL NOT NULL,
            PRIMARY KEY(consumer_id,source_seq),
            FOREIGN KEY(consumer_id,source_seq)
                REFERENCES join_output_commits(consumer_id,source_seq) ON DELETE CASCADE)
    ''')


def _manifest(con,consumer_id,fixed_w):
    return con.execute('''
        SELECT state_id,spec_hash,left_cursor,current_left,right_cursor,scan_complete,revision,abandoned
        FROM join_output_builds WHERE consumer_id=? AND source_seq=?
    ''',(consumer_id,int(fixed_w))).fetchone()


def _validate_state(con,state_id,fixed_w,spec_hash):
    state=join_state.state_info(con,state_id)
    if (not state['bootstrap_complete'] or int(state['watermark'])!=int(fixed_w)
            or state['spec_hash']!=spec_hash):
        raise RuntimeError('JOIN output build backing state moved from fixed-W')
    return state


def _scan(con,state,manifest,row_limit,scan_limit,byte_limit,max_row_bytes):
    left_cursor,current_left,right_cursor=manifest[2:5]
    rows=[]
    work=size=0
    complete=False
    while work<scan_limit and len(rows)<row_limit:
        if current_left is None:
            if left_cursor is None:
                left=con.execute("""SELECT pk_blob,join_blob,row_payload FROM join_rows
                    WHERE state_id=? AND side='left' ORDER BY pk_blob LIMIT 1""",
                    (state['state_id'],)).fetchone()
            else:
                left=con.execute("""SELECT pk_blob,join_blob,row_payload FROM join_rows
                    WHERE state_id=? AND side='left' AND pk_blob>? ORDER BY pk_blob LIMIT 1""",
                    (state['state_id'],left_cursor)).fetchone()
            work+=1
            if left is None:
                complete=True
                break
            current_left=bytes(left[0])
            right_cursor=None
        else:
            left=con.execute("""SELECT pk_blob,join_blob,row_payload FROM join_rows
                WHERE state_id=? AND side='left' AND pk_blob=?""",
                (state['state_id'],current_left)).fetchone()
            work+=1
            if left is None:
                raise RuntimeError('JOIN output build left cursor disappeared')
        if left[1] is None:
            left_cursor=current_left
            current_left=right_cursor=None
            continue
        if work>=scan_limit:
            break
        limit=min(row_limit-len(rows),scan_limit-work)
        if right_cursor is None:
            rights=con.execute("""SELECT pk_blob,row_payload FROM join_rows INDEXED BY join_rows_by_key
                WHERE state_id=? AND side='right' AND join_blob=? ORDER BY pk_blob LIMIT ?""",
                (state['state_id'],left[1],limit))
        else:
            rights=con.execute("""SELECT pk_blob,row_payload FROM join_rows INDEXED BY join_rows_by_key
                WHERE state_id=? AND side='right' AND join_blob=? AND pk_blob>? ORDER BY pk_blob LIMIT ?""",
                (state['state_id'],left[1],right_cursor,limit))
        fetched=0
        left_row=pickle.loads(left[2])
        try:
            for right_pk,right_payload in rights:
                fetched+=1
                work+=1
                pair_id=join_state._pair_id(current_left,right_pk)
                projected=join_state._project(state['spec'],left_row,pickle.loads(right_payload))
                payload=pickle.dumps(projected,protocol=5)
                nbytes=len(pair_id)+len(payload)
                if nbytes>max_row_bytes:
                    raise ValueError('JOIN output build row exceeds max_row_bytes')
                if rows and size+nbytes>byte_limit:
                    return rows,(left_cursor,current_left,right_cursor),False,work,size
                rows.append((pair_id,0,payload))
                size+=nbytes
                right_cursor=bytes(right_pk)
        finally:
            rights.close()
        if not fetched:
            work+=1
        if fetched<limit:
            left_cursor=current_left
            current_left=right_cursor=None
    return rows,(left_cursor,current_left,right_cursor),complete,work,size


def _commit_chunk(con,consumer_id,fixed_w,expected,rows,cursor,complete):
    with join_outbox.transaction(con):
        current=_manifest(con,consumer_id,fixed_w)
        if current!=expected:
            return False
        _validate_state(con,expected[0],fixed_w,expected[1])
        for pair_id,op,payload in rows:
            join_outbox._register_pair_identity_locked(con,consumer_id,pair_id)
            con.execute('''INSERT INTO join_output_rows(consumer_id,source_seq,pair_id,op,row_payload)
                VALUES(?,?,?,?,?)''',(consumer_id,int(fixed_w),pair_id,op,payload))
        con.execute('''UPDATE join_output_commits SET nrows=nrows+?,updated=?
            WHERE consumer_id=? AND source_seq=? AND sealed=0''',
            (len(rows),time.time(),consumer_id,int(fixed_w)))
        con.execute('''UPDATE join_output_builds SET left_cursor=?,current_left=?,right_cursor=?,
            scan_complete=?,revision=revision+1,updated=? WHERE consumer_id=? AND source_seq=?''',
            (*cursor,int(complete),time.time(),consumer_id,int(fixed_w)))
    return True


def _seal(con,consumer_id,fixed_w,expected):
    # The full canonical digest is a read, outside the write transaction. No
    # final full-table copy/update is hidden in the publication boundary.
    ordered=con.execute('''SELECT pair_id,op,row_payload FROM join_output_rows
        WHERE consumer_id=? AND source_seq=? ORDER BY pair_id''',(consumer_id,int(fixed_w)))
    try:
        digest=join_outbox._digest('bootstrap',ordered)
    finally:
        ordered.close()
    with join_outbox.transaction(con):
        if _manifest(con,consumer_id,fixed_w)!=expected:
            return False
        _validate_state(con,expected[0],fixed_w,expected[1])
        con.execute('''UPDATE join_output_commits SET sealed=1,digest=?,updated=?
            WHERE consumer_id=? AND source_seq=? AND sealed=0''',
            (digest,time.time(),consumer_id,int(fixed_w)))
    return True


def step(con,consumer_id,state_id,plan_version,generation_id,fixed_w,
         row_limit=1000,scan_limit=4000,byte_limit=16*1024**2,max_row_bytes=64*1024**2):
    if con.in_transaction:
        raise RuntimeError('JOIN output build requires independent chunk transactions')
    consumer_id=join_outbox._text(consumer_id,'consumer_id')
    state_id=join_outbox._text(state_id,'state_id')
    row_limit=max(1,min(int(row_limit),1000))
    scan_limit=max(2,min(int(scan_limit),4000))
    byte_limit=max(1,min(int(byte_limit),16*1024**2))
    max_row_bytes=max(1,min(int(max_row_bytes),64*1024**2))
    install(con)
    state=join_state.state_info(con,state_id)
    _validate_state(con,state_id,fixed_w,state['spec_hash'])
    join_outbox.ensure_stream(con,consumer_id,state_id,plan_version,generation_id,fixed_w)
    with join_outbox.transaction(con):
        _validate_state(con,state_id,fixed_w,state['spec_hash'])
        try:
            output=join_outbox.commit_info(con,consumer_id,fixed_w)
        except KeyError:
            now=time.time()
            con.execute('''INSERT INTO join_output_commits(
                consumer_id,source_seq,kind,nrows,digest,sealed,visible,created,updated)
                VALUES(?,?,'bootstrap',0,'',0,0,?,?)''',(consumer_id,int(fixed_w),now,now))
            con.execute('''INSERT INTO join_output_builds(consumer_id,source_seq,state_id,spec_hash,updated)
                VALUES(?,?,?,?,?)''',(consumer_id,int(fixed_w),state_id,state['spec_hash'],now))
            output=join_outbox.commit_info(con,consumer_id,fixed_w)
        if output['kind']!='bootstrap':
            raise RuntimeError('JOIN output build conflicts with existing commit')
        if output['sealed']:
            return dict(done=True,nrows=0,scan_work=0,serialized_bytes=0)
        manifest=_manifest(con,consumer_id,fixed_w)
        if manifest is None or manifest[:2]!=(state_id,state['spec_hash']):
            raise RuntimeError('JOIN output build manifest missing or changed')
        if manifest[7]:
            raise RuntimeError('JOIN output build is being discarded')
    rows=[]
    work=size=0
    if not manifest[5]:
        rows,cursor,complete,work,size=_scan(con,state,manifest,row_limit,scan_limit,byte_limit,max_row_bytes)
        if not _commit_chunk(con,consumer_id,fixed_w,manifest,rows,cursor,complete):
            return dict(done=False,nrows=0,scan_work=work,serialized_bytes=size)
        manifest=_manifest(con,consumer_id,fixed_w)
    if manifest[5]:
        _seal(con,consumer_id,fixed_w,manifest)
    return dict(done=join_outbox.commit_info(con,consumer_id,fixed_w)['sealed'],
                nrows=len(rows),scan_work=work,serialized_bytes=size)


def discard_unactivated(con,consumer_id,fixed_w,limit=1000):
    """Drain a stopped candidate's unpublished bytes in restartable chunks.

    Caller holds a durable retirement intent and has stopped the worker. Its
    generation pin is released only after successful cleanup; failure retains
    both progress and source history for the next retirement attempt.
    """
    if con.in_transaction:
        raise RuntimeError('JOIN build discard requires independent transactions')
    if not con.execute("SELECT 1 FROM sqlite_master WHERE name='join_output_builds'").fetchone():
        return False
    if _manifest(con,consumer_id,fixed_w) is None:
        return False
    if con.execute('SELECT 1 FROM source_consumers WHERE consumer_id=?',(consumer_id,)).fetchone():
        raise RuntimeError('cannot discard activated JOIN build')
    if con.execute('SELECT 1 FROM join_job_links WHERE consumer_id=? LIMIT 1',(consumer_id,)).fetchone():
        raise RuntimeError('cannot discard JOIN build with durable jobs')
    limit=max(1,min(int(limit),1000))
    with join_outbox.transaction(con):
        if con.execute('SELECT 1 FROM source_consumers WHERE consumer_id=?',(consumer_id,)).fetchone():
            raise RuntimeError('cannot discard activated JOIN build')
        if con.execute('SELECT 1 FROM join_job_links WHERE consumer_id=? LIMIT 1',(consumer_id,)).fetchone():
            raise RuntimeError('cannot discard JOIN build with durable jobs')
        con.execute('''UPDATE join_output_commits SET sealed=0,updated=?
            WHERE consumer_id=? AND source_seq=?''',(time.time(),consumer_id,int(fixed_w)))
        con.execute('''UPDATE join_output_builds SET abandoned=1,revision=revision+1,updated=?
            WHERE consumer_id=? AND source_seq=?''',(time.time(),consumer_id,int(fixed_w)))
    for table,key in [('join_output_rows','pair_id'),('join_output_identities','target_id')]:
        while True:
            with join_outbox.transaction(con):
                size_column='row_payload' if table=='join_output_rows' else 'pair_id'
                keys=con.execute('SELECT '+key+',length('+size_column+')+length('+key+') FROM '+table+' WHERE consumer_id=? ORDER BY '+key+' LIMIT ?',
                                 (consumer_id,limit)).fetchall()
                size=0
                for value,nbytes in keys:
                    if size and size+int(nbytes)>16*1024**2:
                        break
                    con.execute('DELETE FROM '+table+' WHERE consumer_id=? AND '+key+'=?',(consumer_id,value))
                    size+=int(nbytes)
            if not keys:
                break
    with join_outbox.transaction(con):
        con.execute('DELETE FROM join_output_streams WHERE consumer_id=?',(consumer_id,))
    return True
