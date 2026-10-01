#!/usr/bin/env python3
"""Durable output journal for aggregate generations.

Every source_seq gets one output commit marker, even when filtering produces no
rows. Rows are final per-group mutations for that commit: upsert(0) contains the
full aggregate row; delete(1) contains only the group key columns.

The journal is intentionally target-neutral. A later StarRocks adapter may
deliver commits out of order, but visible_frontier only advances through one
continuous prefix.
"""
import contextlib
import hashlib
import pickle
import time

import aggregate_state


@contextlib.contextmanager
def transaction(con):
    if con.in_transaction:
        yield
        return
    con.execute("BEGIN IMMEDIATE")
    try:
        yield
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise


def install(con):
    con.executescript("""
        CREATE TABLE IF NOT EXISTS aggregate_output_streams(
            consumer_id TEXT PRIMARY KEY,
            state_id TEXT NOT NULL,
            plan_version INTEGER NOT NULL,
            generation_id TEXT NOT NULL,
            fixed_w INTEGER NOT NULL,
            visible_seq INTEGER NOT NULL,
            created REAL NOT NULL,
            updated REAL NOT NULL);

        CREATE TABLE IF NOT EXISTS aggregate_output_commits(
            consumer_id TEXT NOT NULL
                REFERENCES aggregate_output_streams(consumer_id)
                ON DELETE CASCADE,
            source_seq INTEGER NOT NULL,
            kind TEXT NOT NULL,
            nrows INTEGER NOT NULL,
            digest TEXT NOT NULL,
            visible INTEGER NOT NULL DEFAULT 0,
            created REAL NOT NULL,
            updated REAL NOT NULL,
            PRIMARY KEY(consumer_id,source_seq));

        CREATE TABLE IF NOT EXISTS aggregate_output_rows(
            consumer_id TEXT NOT NULL,
            source_seq INTEGER NOT NULL,
            key_blob BLOB NOT NULL,
            op INTEGER NOT NULL CHECK(op IN (0,1)),
            row_payload BLOB NOT NULL,
            PRIMARY KEY(consumer_id,source_seq,key_blob),
            FOREIGN KEY(consumer_id,source_seq)
                REFERENCES aggregate_output_commits(consumer_id,source_seq)
                ON DELETE CASCADE);
        CREATE INDEX IF NOT EXISTS aggregate_output_pending
            ON aggregate_output_commits(consumer_id,visible,source_seq);
    """)


def _text(value,name):
    value=str(value or "").strip()
    if not value:
        raise ValueError(name+" must be non-empty")
    return value


def stream_info(con,consumer_id):
    row=con.execute("""
        SELECT state_id,plan_version,generation_id,fixed_w,
               visible_seq,created,updated
        FROM aggregate_output_streams
        WHERE consumer_id=?
    """,(_text(consumer_id,"consumer_id"),)).fetchone()
    if not row:
        raise KeyError("aggregate output stream does not exist")
    return dict(
        consumer_id=str(consumer_id),
        state_id=str(row[0]),
        plan_version=int(row[1]),
        generation_id=str(row[2]),
        fixed_w=int(row[3]),
        visible_seq=int(row[4]),
        created=float(row[5]),
        updated=float(row[6]),
    )


def ensure_stream(
        con,consumer_id,state_id,plan_version,generation_id,fixed_w
):
    consumer_id=_text(consumer_id,"consumer_id")
    state_id=_text(state_id,"state_id")
    generation_id=_text(generation_id,"generation_id")
    plan_version=int(plan_version)
    fixed_w=int(fixed_w)
    if fixed_w<0:
        raise ValueError("aggregate output fixed_w cannot be negative")
    try:
        current=stream_info(con,consumer_id)
    except KeyError:
        now=time.time()
        with transaction(con):
            con.execute("""
                INSERT INTO aggregate_output_streams(
                    consumer_id,state_id,plan_version,generation_id,
                    fixed_w,visible_seq,created,updated)
                VALUES(?,?,?,?,?,?,?,?)
            """,(
                consumer_id,state_id,plan_version,generation_id,
                fixed_w,fixed_w-1,now,now))
        return stream_info(con,consumer_id)
    expected=(state_id,plan_version,generation_id,fixed_w)
    actual=(
        current["state_id"],current["plan_version"],
        current["generation_id"],current["fixed_w"])
    if actual!=expected:
        raise RuntimeError(
            "aggregate output stream identity changed across restart")
    return current


def _digest(kind,rows):
    digest=hashlib.sha256()
    digest.update(str(kind).encode("utf-8"))
    for key_blob,op,payload in rows:
        digest.update(len(key_blob).to_bytes(8,"big"))
        digest.update(key_blob)
        digest.update(bytes([int(op)]))
        digest.update(len(payload).to_bytes(8,"big"))
        digest.update(payload)
    return digest.hexdigest()


def _insert_commit(con,consumer_id,source_seq,kind,rows):
    consumer_id=_text(consumer_id,"consumer_id")
    source_seq=int(source_seq)
    rows=sorted(rows,key=lambda item:item[0])
    digest=_digest(kind,rows)
    existing=con.execute("""
        SELECT kind,nrows,digest FROM aggregate_output_commits
        WHERE consumer_id=? AND source_seq=?
    """,(consumer_id,source_seq)).fetchone()
    if existing:
        if (
            str(existing[0])!=str(kind)
            or int(existing[1])!=len(rows)
            or str(existing[2])!=digest
        ):
            raise RuntimeError(
                "aggregate output commit retry has different payload")
        return False
    now=time.time()
    con.execute("""
        INSERT INTO aggregate_output_commits(
            consumer_id,source_seq,kind,nrows,digest,visible,created,updated)
        VALUES(?,?,?,?,?,0,?,?)
    """,(consumer_id,source_seq,str(kind),len(rows),digest,now,now))
    for key_blob,op,payload in rows:
        con.execute("""
            INSERT INTO aggregate_output_rows(
                consumer_id,source_seq,key_blob,op,row_payload)
            VALUES(?,?,?,?,?)
        """,(consumer_id,source_seq,key_blob,int(op),payload))
    return True


def seed_bootstrap(
        con,consumer_id,state_id,plan_version,generation_id,fixed_w
):
    ensure_stream(
        con,consumer_id,state_id,plan_version,generation_id,fixed_w)
    rows=[]
    spec=aggregate_state.state_info(con,state_id)["spec"]
    for row in aggregate_state.read_rows(con,state_id):
        key_blob,_=aggregate_state._group_key(spec,row)
        rows.append((
            bytes(key_blob),0,pickle.dumps(row,protocol=5)))
    with transaction(con):
        _insert_commit(
            con,consumer_id,int(fixed_w),"bootstrap",rows)
    return commit_info(con,consumer_id,fixed_w)


def enqueue_incremental(
        con,consumer_id,state_id,source_seq,changes
):
    stream=stream_info(con,consumer_id)
    if stream["state_id"]!=str(state_id):
        raise RuntimeError(
            "aggregate output stream points to another state")
    info=aggregate_state.state_info(con,state_id)
    if int(info["watermark"])!=int(source_seq):
        raise RuntimeError(
            "aggregate output must be journaled at backing state watermark")
    rows=[]
    for key_blob,key_payload in aggregate_state.affected_group_keys(
        info["spec"],changes
    ):
        current=aggregate_state.read_group(con,state_id,key_blob)
        if current is None:
            keys=pickle.loads(key_payload)
            payload=pickle.dumps(
                dict(zip(info["spec"]["group_keys"],keys)),
                protocol=5)
            rows.append((key_blob,1,payload))
        else:
            rows.append((
                key_blob,0,pickle.dumps(current,protocol=5)))
    with transaction(con):
        _insert_commit(
            con,consumer_id,int(source_seq),"incremental",rows)
    return commit_info(con,consumer_id,source_seq)


def commit_info(con,consumer_id,source_seq):
    row=con.execute("""
        SELECT kind,nrows,digest,visible,created,updated
        FROM aggregate_output_commits
        WHERE consumer_id=? AND source_seq=?
    """,(_text(consumer_id,"consumer_id"),int(source_seq))).fetchone()
    if not row:
        raise KeyError("aggregate output commit does not exist")
    return dict(
        consumer_id=str(consumer_id),source_seq=int(source_seq),
        kind=str(row[0]),nrows=int(row[1]),digest=str(row[2]),
        visible=bool(row[3]),created=float(row[4]),updated=float(row[5]),
    )


def commit_rows(con,consumer_id,source_seq):
    result=[]
    for key_blob,op,payload in con.execute("""
        SELECT key_blob,op,row_payload
        FROM aggregate_output_rows
        WHERE consumer_id=? AND source_seq=?
        ORDER BY key_blob
    """,(_text(consumer_id,"consumer_id"),int(source_seq))):
        result.append(dict(
            key_blob=bytes(key_blob),op=int(op),
            row=pickle.loads(payload)))
    return result


def pending_commits(con,consumer_id,limit=100):
    return [
        commit_info(con,consumer_id,row[0])
        for row in con.execute("""
            SELECT source_seq FROM aggregate_output_commits
            WHERE consumer_id=? AND visible=0
            ORDER BY source_seq LIMIT ?
        """,(_text(consumer_id,"consumer_id"),max(1,int(limit))))
    ]


def mark_visible(con,consumer_id,source_seq):
    consumer_id=_text(consumer_id,"consumer_id")
    source_seq=int(source_seq)
    now=time.time()
    with transaction(con):
        stream=stream_info(con,consumer_id)
        row=con.execute("""
            SELECT visible FROM aggregate_output_commits
            WHERE consumer_id=? AND source_seq=?
        """,(consumer_id,source_seq)).fetchone()
        if not row:
            raise KeyError("aggregate output commit does not exist")
        if not int(row[0]):
            con.execute("""
                UPDATE aggregate_output_commits
                SET visible=1,updated=?
                WHERE consumer_id=? AND source_seq=?
            """,(now,consumer_id,source_seq))
        frontier=int(stream["visible_seq"])
        while True:
            next_seq=frontier+1
            visible=con.execute("""
                SELECT visible FROM aggregate_output_commits
                WHERE consumer_id=? AND source_seq=?
            """,(consumer_id,next_seq)).fetchone()
            if visible is None or not int(visible[0]):
                break
            frontier=next_seq
        con.execute("""
            UPDATE aggregate_output_streams
            SET visible_seq=?,updated=?
            WHERE consumer_id=?
        """,(frontier,now,consumer_id))
    return stream_info(con,consumer_id)


def visible_frontier(con,consumer_id):
    return stream_info(con,consumer_id)["visible_seq"]
