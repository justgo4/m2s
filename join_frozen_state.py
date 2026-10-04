#!/usr/bin/env python3
"""Durable cuts of moving JOIN state, copied with bounded write transactions.

Pins protect backing bytes, not only catalog metadata. A change after W stores
its before-image in the same transaction as state/outbox advancement. The first
change after W supplies the row at W; a NULL payload represents absence.
"""
import pickle

import join_state
import source_state


def pin(con,owner,state_id,watermark):
    with join_state.transaction(con):
        state=join_state.state_info(con,state_id)
        existing=con.execute('''SELECT state_id,watermark,spec_hash
            FROM join_frozen_pins WHERE owner=?''',(owner,)).fetchone()
        expected=(state_id,int(watermark),state['spec_hash'])
        if existing is not None:
            if existing!=expected:
                raise RuntimeError('JOIN frozen pin identity changed')
        else:
            if not state['bootstrap_complete'] or state['watermark']!=int(watermark):
                raise RuntimeError('JOIN frozen pin requires current complete cut')
            con.execute('INSERT INTO join_frozen_pins VALUES(?,?,?,?)',(owner,*expected))
    return expected


def info(con,owner):
    row=con.execute('SELECT state_id,watermark,spec_hash FROM join_frozen_pins WHERE owner=?',
                    (owner,)).fetchone()
    if row is None:
        raise RuntimeError('JOIN frozen pin is missing')
    state=join_state.state_info(con,row[0])
    if not state['bootstrap_complete'] or state['watermark']<row[1] or state['spec_hash']!=row[2]:
        raise RuntimeError('JOIN frozen backing identity changed')
    return row


def release(con,owner):
    con.execute('DELETE FROM join_frozen_pins WHERE owner=?',(owner,))


def copy_step(con,owner,target_state_id,target_spec,limit=1000,
              byte_limit=16*1024**2,max_row_bytes=64*1024**2):
    if con.in_transaction:
        raise RuntimeError('JOIN frozen copy requires independent transactions')
    limit=max(1,min(int(limit),1000))
    byte_limit=max(1,min(int(byte_limit),16*1024**2))
    with source_state.read_snapshot(con):
        source,w,spec_hash=info(con,owner)
        original=join_state.state_info(con,source)['spec']
        if (original['sources']!=target_spec['sources']
                or original['semantics']!=target_spec['semantics']
                or any(p not in original['projections'] for p in target_spec['projections'])):
            raise RuntimeError('JOIN frozen copy target is not a projection subview')
    target=join_state.begin_bootstrap(con,target_state_id,target_spec,w)
    if target['bootstrap_complete']:
        return dict(done=True,nrows=0,scan_work=0,serialized_bytes=0)
    side='left' if not target['left_complete'] else 'right'
    cursor=target[side+'_cursor']
    rows=[]
    size=work=0
    next_cursor=cursor
    with source_state.read_snapshot(con):
        if info(con,owner)!=(source,w,spec_hash):
            raise RuntimeError('JOIN frozen cut changed')
        # Merge ordered PK ranges via both primary indexes. Deleted PKs remain
        # discoverable in history; newly inserted PKs resolve to NULL at W.
        lower=b'' if cursor is None else cursor
        keys=con.execute('''
            SELECT pk_blob FROM join_rows WHERE state_id=? AND side=? AND pk_blob>?
            UNION SELECT pk_blob FROM join_row_before_images
              WHERE state_id=? AND side=? AND pk_blob>?
            ORDER BY pk_blob LIMIT ?''',
            (source,side,lower,source,side,lower,limit)).fetchall()
        for (pk,) in keys:
            old=con.execute('''SELECT join_blob,row_payload FROM join_row_before_images
                WHERE state_id=? AND side=? AND pk_blob=? AND change_seq>?
                ORDER BY change_seq LIMIT 1''',(source,side,pk,w)).fetchone()
            if old is None:
                old=con.execute('''SELECT join_blob,row_payload FROM join_rows
                    WHERE state_id=? AND side=? AND pk_blob=?''',(source,side,pk)).fetchone()
            nbytes=len(pk)+(0 if old is None or old[1] is None else len(old[1])
                           +(0 if old[0] is None else len(old[0])))
            if nbytes>int(max_row_bytes):
                raise ValueError('JOIN frozen copy row exceeds max_row_bytes')
            if work and size+nbytes>byte_limit:
                break
            work+=1
            size+=nbytes
            next_cursor=bytes(pk)
            if old is not None and old[1] is not None:
                rows.append(join_state._source_row(target_spec,side,pickle.loads(old[1])))
        complete=len(keys)<limit and work==len(keys)
    with join_state.transaction(con):
        info(con,owner)
        current=join_state.state_info(con,target_state_id)
        if current!=target:
            return dict(done=current['bootstrap_complete'],nrows=0,scan_work=work,serialized_bytes=size)
        join_state.apply_bootstrap_chunk(con,target_state_id,w,side,rows,next_cursor,complete)
    return dict(done=join_state.state_info(con,target_state_id)['bootstrap_complete'],
                nrows=len(rows),scan_work=work,serialized_bytes=size)


def discard_step(con,state_id,limit=1000,byte_limit=16*1024**2):
    """Only temporary snapshot backing is removed; caller fences its builder."""
    byte_limit=max(1,min(int(byte_limit),16*1024**2))
    with join_state.transaction(con):
        keys=con.execute('''SELECT side,pk_blob,length(row_payload)+length(pk_blob)
            +coalesce(length(join_blob),0)
            FROM join_rows WHERE state_id=? ORDER BY side,pk_blob LIMIT ?''',
            (state_id,max(1,min(int(limit),1000)))).fetchall()
        size=0
        for side,pk,nbytes in keys:
            if size and size+nbytes>int(byte_limit):
                break
            con.execute('DELETE FROM join_rows WHERE state_id=? AND side=? AND pk_blob=?',
                        (state_id,side,pk))
            size+=nbytes
        return not con.execute('SELECT 1 FROM join_rows WHERE state_id=? LIMIT 1',
                               (state_id,)).fetchone()


def gc_step(con,limit=1000):
    # Keep every before-image newer than the oldest live cut. A stale reader's
    # scan and pin release serialize with this transaction; no catalog-only pin.
    if not con.execute('SELECT 1 FROM join_row_before_images LIMIT 1').fetchone():
        return 0
    with join_state.transaction(con):
        limit=max(1,min(int(limit),1000))
        cursor=con.execute('SELECT state_id FROM join_frozen_gc_cursor WHERE singleton=1').fetchone()[0]
        states=con.execute('SELECT state_id,watermark FROM join_states WHERE state_id>? ORDER BY state_id LIMIT 64',
                           (cursor,)).fetchall()
        if not states and cursor:
            states=con.execute('SELECT state_id,watermark FROM join_states ORDER BY state_id LIMIT 64').fetchall()
        rows=[]
        last=''
        for state,watermark in states:
            last=state
            pinned=con.execute('SELECT min(watermark) FROM join_frozen_pins WHERE state_id=?',
                               (state,)).fetchone()[0]
            floor=watermark if pinned is None else int(pinned)
            rows.extend(con.execute('''SELECT state_id,side,pk_blob,change_seq,
                coalesce(length(row_payload),0)+length(pk_blob)+coalesce(length(join_blob),0)
                FROM join_row_before_images INDEXED BY join_row_before_images_gc
                WHERE state_id=? AND change_seq<=? ORDER BY change_seq,side,pk_blob LIMIT ?''',
                (state,floor,limit-len(rows))).fetchall())
            if len(rows)==limit:
                break
        con.execute('UPDATE join_frozen_gc_cursor SET state_id=? WHERE singleton=1',(last,))
        size=count=0
        for state,side,pk,seq,nbytes in rows:
            if size and size+nbytes>16*1024**2:
                break
            con.execute('''DELETE FROM join_row_before_images
                WHERE state_id=? AND side=? AND pk_blob=? AND change_seq=?''',
                (state,side,pk,seq))
            size+=nbytes
            count+=1
        return count
