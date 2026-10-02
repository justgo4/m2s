#!/usr/bin/env python3
"""Durable output journal for INNER JOIN generations.

The outbox is committed atomically with JOIN state and the source consumer.
Each source_seq has a marker even when it produces no output delta. Pair identity
is the stable (left source PK, right source PK) identity from join_state, so
duplicate projected rows remain independently retractable.
"""
import base64
import contextlib
import hashlib
import pickle
import time

import join_state


IDENTITY_FORMAT_LEGACY="pair-base64-v1"
IDENTITY_FORMAT_HASH="sha256-hex-checked-v1"
DEFAULT_IDENTITY_FORMAT=IDENTITY_FORMAT_HASH


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
    if con.in_transaction:
        raise RuntimeError(
            "JOIN outbox schema must be installed before transactional use")
    con.executescript("""
        CREATE TABLE IF NOT EXISTS join_output_streams(
            consumer_id TEXT PRIMARY KEY,
            state_id TEXT NOT NULL,
            plan_version INTEGER NOT NULL,
            generation_id TEXT NOT NULL,
            fixed_w INTEGER NOT NULL,
            identity_format TEXT NOT NULL DEFAULT 'sha256-hex-checked-v1',
            visible_seq INTEGER NOT NULL,
            created REAL NOT NULL,
            updated REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS join_output_commits(
            consumer_id TEXT NOT NULL
                REFERENCES join_output_streams(consumer_id)
                ON DELETE CASCADE,
            source_seq INTEGER NOT NULL,
            kind TEXT NOT NULL,
            nrows INTEGER NOT NULL,
            digest TEXT NOT NULL,
            visible INTEGER NOT NULL DEFAULT 0,
            created REAL NOT NULL,
            updated REAL NOT NULL,
            PRIMARY KEY(consumer_id,source_seq));
        CREATE TABLE IF NOT EXISTS join_output_rows(
            consumer_id TEXT NOT NULL,
            source_seq INTEGER NOT NULL,
            pair_id BLOB NOT NULL,
            op INTEGER NOT NULL CHECK(op IN (0,1)),
            row_payload BLOB NOT NULL,
            PRIMARY KEY(consumer_id,source_seq,pair_id),
            FOREIGN KEY(consumer_id,source_seq)
                REFERENCES join_output_commits(consumer_id,source_seq)
                ON DELETE CASCADE);
        CREATE INDEX IF NOT EXISTS join_output_pending
            ON join_output_commits(consumer_id,visible,source_seq);
        CREATE TABLE IF NOT EXISTS join_output_identities(
            consumer_id TEXT NOT NULL
                REFERENCES join_output_streams(consumer_id)
                ON DELETE CASCADE,
            target_id TEXT NOT NULL,
            pair_id BLOB NOT NULL,
            created REAL NOT NULL,
            PRIMARY KEY(consumer_id,target_id),
            UNIQUE(consumer_id,pair_id));
    """)
    columns={
        str(row[1]) for row in con.execute(
            "PRAGMA table_info(join_output_streams)").fetchall()
    }
    if "identity_format" not in columns:
        con.execute(
            "ALTER TABLE join_output_streams "
            "ADD COLUMN identity_format TEXT NOT NULL "
            "DEFAULT 'pair-base64-v1'")


def ensure_installed(con):
    stream=con.execute("""
        SELECT 1 FROM sqlite_master
        WHERE type='table' AND name='join_output_streams'
    """).fetchone()
    identities=con.execute("""
        SELECT 1 FROM sqlite_master
        WHERE type='table' AND name='join_output_identities'
    """).fetchone()
    columns={
        str(row[1]) for row in con.execute(
            "PRAGMA table_info(join_output_streams)").fetchall()
    } if stream else set()
    if stream and identities and "identity_format" in columns:
        return
    if con.in_transaction:
        raise RuntimeError(
            "JOIN outbox schema migration is required before transactional use")
    install(con)


def _text(value,name):
    value=str(value or "").strip()
    if not value:
        raise ValueError(name+" must be non-empty")
    return value


def stream_info(con,consumer_id):
    row=con.execute("""
        SELECT state_id,plan_version,generation_id,fixed_w,
               identity_format,visible_seq,created,updated
        FROM join_output_streams
        WHERE consumer_id=?
    """,(_text(consumer_id,"consumer_id"),)).fetchone()
    if not row:
        raise KeyError("JOIN output stream does not exist")
    return dict(
        consumer_id=str(consumer_id),
        state_id=str(row[0]),
        plan_version=int(row[1]),
        generation_id=str(row[2]),
        fixed_w=int(row[3]),
        identity_format=str(row[4]),
        visible_seq=int(row[5]),
        created=float(row[6]),
        updated=float(row[7]),
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
        raise ValueError("JOIN output fixed-W cannot be negative")
    try:
        current=stream_info(con,consumer_id)
    except KeyError:
        now=time.time()
        with transaction(con):
            con.execute("""
                INSERT INTO join_output_streams(
                    consumer_id,state_id,plan_version,generation_id,
                    fixed_w,identity_format,visible_seq,created,updated)
                VALUES(?,?,?,?,?,?,?,?,?)
            """,(
                consumer_id,state_id,plan_version,generation_id,
                fixed_w,DEFAULT_IDENTITY_FORMAT,fixed_w-1,now,now))
        return stream_info(con,consumer_id)
    expected=(state_id,plan_version,generation_id,fixed_w)
    actual=(
        current["state_id"],current["plan_version"],
        current["generation_id"],current["fixed_w"])
    if actual!=expected:
        raise RuntimeError(
            "JOIN output stream identity changed across restart")
    return current


def target_id(pair_id,identity_format=DEFAULT_IDENTITY_FORMAT):
    pair_id=bytes(pair_id)
    identity_format=str(identity_format)
    if identity_format==IDENTITY_FORMAT_LEGACY:
        return base64.urlsafe_b64encode(pair_id).decode("ascii")
    if identity_format==IDENTITY_FORMAT_HASH:
        return hashlib.sha256(pair_id).hexdigest()
    raise RuntimeError(
        "unsupported JOIN target identity format: "+identity_format)


def _register_pair_identity_locked(con,consumer_id,pair_id):
    consumer_id=_text(consumer_id,"consumer_id")
    pair_id=bytes(pair_id)
    stream=stream_info(con,consumer_id)
    identity_format=stream["identity_format"]
    value=target_id(pair_id,identity_format)
    if identity_format==IDENTITY_FORMAT_LEGACY:
        return value
    row=con.execute("""
        SELECT pair_id FROM join_output_identities
        WHERE consumer_id=? AND target_id=?
    """,(consumer_id,value)).fetchone()
    if row is not None:
        if bytes(row[0])!=pair_id:
            raise RuntimeError(
                "JOIN target identity collision; exact pair identity differs "
                "for target_id="+value)
        return value
    con.execute("""
        INSERT INTO join_output_identities(
            consumer_id,target_id,pair_id,created)
        VALUES(?,?,?,?)
    """,(consumer_id,value,pair_id,time.time()))
    return value


def target_id_for_pair(con,consumer_id,pair_id):
    consumer_id=_text(consumer_id,"consumer_id")
    pair_id=bytes(pair_id)
    stream=stream_info(con,consumer_id)
    value=target_id(pair_id,stream["identity_format"])
    if stream["identity_format"]==IDENTITY_FORMAT_LEGACY:
        return value
    row=con.execute("""
        SELECT pair_id FROM join_output_identities
        WHERE consumer_id=? AND target_id=?
    """,(consumer_id,value)).fetchone()
    if row is None or bytes(row[0])!=pair_id:
        raise RuntimeError(
            "JOIN target identity registry is missing or inconsistent "
            "for target_id="+value)
    return value


def _digest(kind,rows):
    digest=hashlib.sha256()
    digest.update(str(kind).encode("utf-8"))
    for pair_id,op,payload in rows:
        digest.update(len(pair_id).to_bytes(8,"big"))
        digest.update(pair_id)
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
        SELECT kind,nrows,digest
        FROM join_output_commits
        WHERE consumer_id=? AND source_seq=?
    """,(consumer_id,source_seq)).fetchone()
    if existing:
        if (
            str(existing[0])!=str(kind)
            or int(existing[1])!=len(rows)
            or str(existing[2])!=digest
        ):
            raise RuntimeError(
                "JOIN output commit retry has different payload")
        return False
    now=time.time()
    con.execute("""
        INSERT INTO join_output_commits(
            consumer_id,source_seq,kind,nrows,digest,
            visible,created,updated)
        VALUES(?,?,?,?,?,0,?,?)
    """,(
        consumer_id,source_seq,str(kind),
        len(rows),digest,now,now))
    for pair_id,op,payload in rows:
        con.execute("""
            INSERT INTO join_output_rows(
                consumer_id,source_seq,pair_id,op,row_payload)
            VALUES(?,?,?,?,?)
        """,(
            consumer_id,source_seq,bytes(pair_id),
            int(op),bytes(payload)))
    return True


def seed_bootstrap(
        con,consumer_id,state_id,plan_version,generation_id,fixed_w
):
    ensure_stream(
        con,consumer_id,state_id,plan_version,
        generation_id,fixed_w)
    state=join_state.state_info(con,state_id)
    if not state["bootstrap_complete"]:
        raise RuntimeError(
            "cannot seed JOIN outbox from incomplete state")
    if int(state["watermark"])!=int(fixed_w):
        raise RuntimeError(
            "JOIN bootstrap outbox watermark differs from state")
    rows=[
        (
            bytes(item["pair_id"]),0,
            pickle.dumps(item["row"],protocol=5),
        )
        for item in join_state.read_pairs(con,state_id)
    ]
    with transaction(con):
        for pair_id,_,_ in rows:
            _register_pair_identity_locked(
                con,consumer_id,pair_id)
        _insert_commit(
            con,consumer_id,int(fixed_w),
            "bootstrap",rows)
    return commit_info(
        con,consumer_id,fixed_w)


def enqueue_incremental(
        con,consumer_id,state_id,source_seq,deltas
):
    stream=stream_info(con,consumer_id)
    if stream["state_id"]!=str(state_id):
        raise RuntimeError(
            "JOIN output stream points to another state")
    state=join_state.state_info(con,state_id)
    if int(state["watermark"])!=int(source_seq):
        raise RuntimeError(
            "JOIN output must be journaled at state watermark")
    rows=[]
    for item in deltas or ():
        if not isinstance(item,dict) or set(item)!={
            "pair_id","op","row"
        }:
            raise ValueError("invalid JOIN output delta")
        op=int(item["op"])
        if op not in (0,1):
            raise ValueError("JOIN output op must be 0 or 1")
        row=item["row"]
        if not isinstance(row,dict):
            raise ValueError("JOIN output row must be a dict")
        rows.append((
            bytes(item["pair_id"]),op,
            pickle.dumps(row,protocol=5),
        ))
    with transaction(con):
        for pair_id,_,_ in rows:
            _register_pair_identity_locked(
                con,consumer_id,pair_id)
        _insert_commit(
            con,consumer_id,int(source_seq),
            "incremental",rows)
    return commit_info(
        con,consumer_id,source_seq)


def copy_commit(
        con,source_consumer_id,target_consumer_id,source_seq
):
    """Copy one durable JOIN output commit and pair identities exactly."""
    source_consumer_id=_text(
        source_consumer_id,"source_consumer_id")
    target_consumer_id=_text(
        target_consumer_id,"target_consumer_id")
    source_seq=int(source_seq)
    if source_consumer_id==target_consumer_id:
        raise ValueError(
            "JOIN output copy source and target consumers must differ")
    source=commit_info(
        con,source_consumer_id,source_seq)
    source_stream=stream_info(
        con,source_consumer_id)
    target_stream=stream_info(
        con,target_consumer_id)
    if source_stream["identity_format"]!=target_stream["identity_format"]:
        raise RuntimeError(
            "JOIN output identity format differs across shared consumers")
    if source_seq<int(target_stream["fixed_w"]):
        raise RuntimeError(
            "JOIN copied output commit predates target fixed-W")
    previous=con.execute("""
        SELECT MAX(source_seq)
        FROM join_output_commits
        WHERE consumer_id=?
    """,(target_consumer_id,)).fetchone()[0]
    if previous is not None and source_seq>int(previous)+1:
        raise RuntimeError(
            "JOIN copied output commit would create a target gap")
    rows=[
        (bytes(pair_id),int(op),bytes(payload))
        for pair_id,op,payload in con.execute("""
            SELECT pair_id,op,row_payload
            FROM join_output_rows
            WHERE consumer_id=? AND source_seq=?
            ORDER BY pair_id
        """,(source_consumer_id,source_seq)).fetchall()
    ]
    if len(rows)!=int(source["nrows"]):
        raise RuntimeError(
            "JOIN source output commit row count is inconsistent")
    with transaction(con):
        for pair_id,_,_ in rows:
            _register_pair_identity_locked(
                con,target_consumer_id,pair_id)
        _insert_commit(
            con,target_consumer_id,source_seq,
            source["kind"],rows)
    copied=commit_info(
        con,target_consumer_id,source_seq)
    if copied["digest"]!=source["digest"]:
        raise RuntimeError(
            "JOIN copied output commit digest differs from source")
    return copied


def commit_info(con,consumer_id,source_seq):
    row=con.execute("""
        SELECT kind,nrows,digest,visible,created,updated
        FROM join_output_commits
        WHERE consumer_id=? AND source_seq=?
    """,(
        _text(consumer_id,"consumer_id"),
        int(source_seq),
    )).fetchone()
    if not row:
        raise KeyError("JOIN output commit does not exist")
    return dict(
        consumer_id=str(consumer_id),
        source_seq=int(source_seq),
        kind=str(row[0]),nrows=int(row[1]),
        digest=str(row[2]),visible=bool(row[3]),
        created=float(row[4]),updated=float(row[5]),
    )


def commit_rows(con,consumer_id,source_seq):
    return [
        dict(
            pair_id=bytes(pair_id),op=int(op),
            row=pickle.loads(payload),
        )
        for pair_id,op,payload in con.execute("""
            SELECT pair_id,op,row_payload
            FROM join_output_rows
            WHERE consumer_id=? AND source_seq=?
            ORDER BY pair_id
        """,(
            _text(consumer_id,"consumer_id"),
            int(source_seq),
        )).fetchall()
    ]


def pending_commits(con,consumer_id,limit=100):
    return [
        commit_info(con,consumer_id,row[0])
        for row in con.execute("""
            SELECT source_seq FROM join_output_commits
            WHERE consumer_id=? AND visible=0
            ORDER BY source_seq LIMIT ?
        """,(
            _text(consumer_id,"consumer_id"),
            max(1,int(limit)),
        )).fetchall()
    ]


def mark_visible(con,consumer_id,source_seq):
    consumer_id=_text(consumer_id,"consumer_id")
    source_seq=int(source_seq)
    now=time.time()
    with transaction(con):
        stream=stream_info(con,consumer_id)
        row=con.execute("""
            SELECT visible FROM join_output_commits
            WHERE consumer_id=? AND source_seq=?
        """,(consumer_id,source_seq)).fetchone()
        if not row:
            raise KeyError("JOIN output commit does not exist")
        if not int(row[0]):
            con.execute("""
                UPDATE join_output_commits
                SET visible=1,updated=?
                WHERE consumer_id=? AND source_seq=?
            """,(now,consumer_id,source_seq))
        frontier=int(stream["visible_seq"])
        while True:
            next_seq=frontier+1
            row=con.execute("""
                SELECT visible FROM join_output_commits
                WHERE consumer_id=? AND source_seq=?
            """,(consumer_id,next_seq)).fetchone()
            if row is None or not int(row[0]):
                break
            frontier=next_seq
        con.execute("""
            UPDATE join_output_streams
            SET visible_seq=?,updated=?
            WHERE consumer_id=?
        """,(frontier,now,consumer_id))
    return stream_info(con,consumer_id)


def visible_frontier(con,consumer_id):
    return stream_info(
        con,consumer_id)["visible_seq"]
