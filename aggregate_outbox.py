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

import aggregate_frozen
import aggregate_state
import source_state


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
            "aggregate outbox schema must be installed before transactional use")
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

        CREATE TABLE IF NOT EXISTS aggregate_output_seeds(
            consumer_id TEXT NOT NULL,
            source_seq INTEGER NOT NULL,
            state_id TEXT NOT NULL,
            spec_hash TEXT NOT NULL,
            cursor_key BLOB,
            staged_rows INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(consumer_id,source_seq));
    """)


def _ensure_seed_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS aggregate_output_seeds(
            consumer_id TEXT NOT NULL,
            source_seq INTEGER NOT NULL,
            state_id TEXT NOT NULL,
            spec_hash TEXT NOT NULL,
            cursor_key BLOB,
            staged_rows INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(consumer_id,source_seq))
    """)
    columns={
        row[1] for row in con.execute(
            "PRAGMA table_info(aggregate_output_seeds)").fetchall()
    }
    if "cursor_key" not in columns:
        con.execute(
            "ALTER TABLE aggregate_output_seeds ADD COLUMN cursor_key BLOB")
    if "staged_rows" not in columns:
        con.execute(
            "ALTER TABLE aggregate_output_seeds "
            "ADD COLUMN staged_rows INTEGER NOT NULL DEFAULT 0")


def ensure_installed(con):
    exists=con.execute("""
        SELECT 1 FROM sqlite_master
        WHERE type='table' AND name='aggregate_output_streams'
    """).fetchone()
    if not exists:
        install(con)
        return
    _ensure_seed_table(con)


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


def _public_row(spec,row):
    names=list(spec["group_keys"])+[
        item["output"] for item in spec["aggregates"]]
    return {
        name:row[name]
        for name in names
    }


def _seed_open(con,consumer_id,source_seq):
    _ensure_seed_table(con)
    return con.execute("""
        SELECT state_id,spec_hash,cursor_key,staged_rows
        FROM aggregate_output_seeds
        WHERE consumer_id=? AND source_seq=?
    """,(consumer_id,int(source_seq))).fetchone()


def _seed_cursor(row):
    if row is None or row[2] is None:
        return None
    return bytes(row[2])


def _hash_bootstrap(con,consumer_id,source_seq):
    """Digest committed bootstrap rows without holding the writer lock."""
    with source_state.read_snapshot(con):
        digest=hashlib.sha256()
        digest.update(b"bootstrap")
        count=0
        max_key=None
        for key_blob,op,payload in con.execute("""
            SELECT key_blob,op,row_payload FROM aggregate_output_rows
            WHERE consumer_id=? AND source_seq=? ORDER BY key_blob
        """,(consumer_id,int(source_seq))):
            key_blob=bytes(key_blob)
            payload=bytes(payload)
            digest.update(len(key_blob).to_bytes(8,"big"))
            digest.update(key_blob)
            digest.update(bytes([int(op)]))
            digest.update(len(payload).to_bytes(8,"big"))
            digest.update(payload)
            count+=1
            max_key=key_blob
        return digest.hexdigest(),count,max_key


def bootstrap_sealed(con,consumer_id,fixed_w):
    """True only after every bootstrap group has been durable and digested."""
    consumer_id=_text(consumer_id,"consumer_id")
    fixed_w=int(fixed_w)
    if _seed_open(con,consumer_id,fixed_w) is not None:
        return False
    row=con.execute("""
        SELECT kind FROM aggregate_output_commits
        WHERE consumer_id=? AND source_seq=?
    """,(consumer_id,fixed_w)).fetchone()
    return row is not None and str(row[0])=="bootstrap"


def _seal_bootstrap(con,consumer_id,fixed_w):
    digest,count,max_key=_hash_bootstrap(con,consumer_id,fixed_w)
    with transaction(con):
        existing=_seed_open(con,consumer_id,fixed_w)
        if existing is None:
            return True
        if int(existing[3])!=count:
            return False
        stored=con.execute("""
            SELECT MAX(key_blob) FROM aggregate_output_rows
            WHERE consumer_id=? AND source_seq=?
        """,(consumer_id,int(fixed_w))).fetchone()[0]
        if count==0:
            if stored is not None:
                return False
        elif bytes(stored)!=max_key:
            return False
        con.execute("""
            UPDATE aggregate_output_commits
            SET nrows=?,digest=?,updated=?
            WHERE consumer_id=? AND source_seq=?
        """,(count,digest,time.time(),consumer_id,int(fixed_w)))
        con.execute("""
            DELETE FROM aggregate_output_seeds
            WHERE consumer_id=? AND source_seq=?
        """,(consumer_id,int(fixed_w)))
    aggregate_frozen.release(
        con,aggregate_frozen.seed_owner(consumer_id))
    aggregate_frozen.gc_step(con)
    return True


def seed_chunk(
        con,consumer_id,state_id,plan_version,generation_id,fixed_w,
        limit=1000,byte_limit=16*1024**2,target_spec=None
):
    """Insert one bounded, restartable page of a fixed-W bootstrap journal.

    An unsealed page is invisible to stage/copy. The scan cursor, not the
    last emitted key, is what the next call resumes from, so keys that were
    absent at the pinned cut cannot stall the scan. The digest is written
    only after the last page, and that hash does not hold the writer lock.
    """
    if con.in_transaction:
        raise RuntimeError(
            "aggregate bootstrap chunk requires its own transaction")
    limit=max(1,min(int(limit),1000))
    byte_limit=max(1,min(int(byte_limit),16*1024**2))
    ensure_stream(
        con,consumer_id,state_id,plan_version,generation_id,fixed_w)
    consumer_id=_text(consumer_id,"consumer_id")
    state_id=_text(state_id,"state_id")
    fixed_w=int(fixed_w)
    if bootstrap_sealed(con,consumer_id,fixed_w):
        aggregate_frozen.release(
            con,aggregate_frozen.seed_owner(consumer_id))
        aggregate_frozen.gc_step(con)
        return dict(done=True,nrows=0)
    owner=aggregate_frozen.seed_owner(consumer_id)
    aggregate_frozen.pin(con,owner,state_id,fixed_w)
    source=aggregate_state.state_info(con,state_id)
    if target_spec is not None:
        target_spec=_projected_spec(source["spec"],target_spec)
    opened=_seed_open(con,consumer_id,fixed_w)
    cursor=_seed_cursor(opened)
    page,exhausted=aggregate_frozen.group_page_at(
        con,state_id,fixed_w,after_key=cursor,limit=limit+1)
    rows=[]
    size=0
    consumed=0
    last=cursor
    for key_blob,row in page:
        if row is None:
            last=key_blob
            consumed+=1
            continue
        if target_spec is None:
            public=_public_row(source["spec"],row)
        else:
            public=_project_row(target_spec,row,delete=False)
        payload=pickle.dumps(public,protocol=5)
        nbytes=len(key_blob)+len(payload)
        if rows and (len(rows)>=limit or size+nbytes>byte_limit):
            break
        rows.append((key_blob,0,payload))
        size+=nbytes
        last=key_blob
        consumed+=1
    reached_end=consumed==len(page) and exhausted
    with transaction(con):
        aggregate_frozen.info(con,owner)
        current=aggregate_state.state_info(con,state_id)
        if (
            current["spec_hash"]!=source["spec_hash"]
            or not current["bootstrap_complete"]
            or int(current["watermark"])<fixed_w
        ):
            raise RuntimeError(
                "aggregate bootstrap state changed while seeding output")
        existing=_seed_open(con,consumer_id,fixed_w)
        commit=con.execute("""
            SELECT kind FROM aggregate_output_commits
            WHERE consumer_id=? AND source_seq=?
        """,(consumer_id,fixed_w)).fetchone()
        if commit is not None and str(commit[0])!="bootstrap":
            raise RuntimeError(
                "aggregate bootstrap seed conflicts with another commit")
        if existing is None and commit is not None:
            aggregate_frozen.release(con,owner)
            return dict(done=True,nrows=0)
        if existing is not None and _seed_cursor(existing)!=cursor:
            return dict(done=False,nrows=0)
        if existing is not None and (
            existing[0]!=state_id or existing[1]!=source["spec_hash"]
        ):
            raise RuntimeError(
                "aggregate bootstrap seed identity changed across restart")
        now=time.time()
        if commit is None:
            con.execute("""
                INSERT INTO aggregate_output_commits(
                    consumer_id,source_seq,kind,nrows,digest,visible,created,updated)
                VALUES(?,?,'bootstrap',0,'',0,?,?)
            """,(consumer_id,fixed_w,now,now))
            con.execute("""
                INSERT INTO aggregate_output_seeds(
                    consumer_id,source_seq,state_id,spec_hash,cursor_key,staged_rows)
                VALUES(?,?,?,?,NULL,0)
            """,(consumer_id,fixed_w,state_id,source["spec_hash"]))
            existing=_seed_open(con,consumer_id,fixed_w)
        for key_blob,op,payload in rows:
            con.execute("""
                INSERT INTO aggregate_output_rows(
                    consumer_id,source_seq,key_blob,op,row_payload)
                VALUES(?,?,?,?,?)
            """,(consumer_id,fixed_w,key_blob,int(op),payload))
        con.execute("""
            UPDATE aggregate_output_seeds
            SET cursor_key=?,staged_rows=?
            WHERE consumer_id=? AND source_seq=?
        """,(
            last,int(existing[3])+len(rows),consumer_id,fixed_w))
        if not reached_end:
            return dict(done=False,nrows=len(rows))
    if not _seal_bootstrap(con,consumer_id,fixed_w):
        return dict(done=False,nrows=len(rows))
    return dict(done=True,nrows=len(rows))


def seed_bootstrap(
        con,consumer_id,state_id,plan_version,generation_id,fixed_w,
        limit=1000,byte_limit=16*1024**2
):
    while not seed_chunk(
        con,consumer_id,state_id,plan_version,generation_id,fixed_w,
        limit=limit,byte_limit=byte_limit
    )["done"]:
        pass
    return commit_info(con,consumer_id,fixed_w)


def _projected_spec(source_spec,target_spec):
    source_spec=aggregate_state.validate_spec(source_spec)
    target_spec=aggregate_state.validate_spec(target_spec)
    if source_spec["group_keys"]!=target_spec["group_keys"]:
        raise RuntimeError(
            "aggregate projected outbox GROUP BY keys differ")
    source_outputs={
        item["output"]:item
        for item in source_spec["aggregates"]
    }
    for item in target_spec["aggregates"]:
        if source_outputs.get(item["output"])!=item:
            raise RuntimeError(
                "aggregate projected outbox output is not a source subview: "
                +item["output"])
    return target_spec


def _project_row(target_spec,row,delete=False):
    names=list(target_spec["group_keys"])
    if not delete:
        names.extend(
            item["output"] for item in target_spec["aggregates"])
    missing=[
        name for name in names
        if name not in row
    ]
    if missing:
        raise RuntimeError(
            "aggregate projected outbox row is missing columns: "
            +repr(missing))
    return {
        name:row[name]
        for name in names
    }


def seed_bootstrap_projected(
        con,consumer_id,source_state_id,plan_version,generation_id,
        fixed_w,target_spec,limit=1000,byte_limit=16*1024**2
):
    """Seed a follower bootstrap from a compatible aggregate superset state."""
    source=aggregate_state.state_info(
        con,source_state_id)
    target_spec=_projected_spec(
        source["spec"],target_spec)
    ensure_stream(
        con,consumer_id,source_state_id,
        plan_version,generation_id,fixed_w)
    if int(source["watermark"])!=int(fixed_w):
        raise RuntimeError(
            "aggregate projected bootstrap source moved from fixed-W")
    while not seed_chunk(
        con,consumer_id,source_state_id,plan_version,generation_id,
        fixed_w,target_spec=target_spec,limit=limit,byte_limit=byte_limit
    )["done"]:
        pass
    return commit_info(
        con,consumer_id,fixed_w)


def copy_commit_projected(
        con,source_consumer_id,target_consumer_id,
        source_seq,target_spec
):
    """Project one durable superset aggregate commit into a follower journal."""
    source_consumer_id=_text(
        source_consumer_id,"source_consumer_id")
    target_consumer_id=_text(
        target_consumer_id,"target_consumer_id")
    source_seq=int(source_seq)
    if source_consumer_id==target_consumer_id:
        raise ValueError(
            "aggregate projected copy source and target consumers must differ")
    _require_sealed(con,source_consumer_id,source_seq)
    source_commit=commit_info(
        con,source_consumer_id,source_seq)
    source_stream=stream_info(
        con,source_consumer_id)
    target_stream=stream_info(
        con,target_consumer_id)
    source_state=aggregate_state.state_info(
        con,source_stream["state_id"])
    target_spec=_projected_spec(
        source_state["spec"],target_spec)
    if target_stream["state_id"]!=source_stream["state_id"]:
        raise RuntimeError(
            "aggregate projected follower stream no longer shares source state")
    if source_seq<int(target_stream["fixed_w"]):
        raise RuntimeError(
            "aggregate projected output commit predates target fixed-W")
    previous=con.execute("""
        SELECT MAX(source_seq)
        FROM aggregate_output_commits
        WHERE consumer_id=?
    """,(target_consumer_id,)).fetchone()[0]
    if previous is not None and source_seq>int(previous)+1:
        raise RuntimeError(
            "aggregate projected output commit would create a target gap")

    rows=[]
    for key_blob,op,payload in con.execute("""
        SELECT key_blob,op,row_payload
        FROM aggregate_output_rows
        WHERE consumer_id=? AND source_seq=?
        ORDER BY key_blob
    """,(source_consumer_id,source_seq)).fetchall():
        row=pickle.loads(payload)
        projected=_project_row(
            target_spec,row,delete=bool(op))
        rows.append((
            bytes(key_blob),int(op),
            pickle.dumps(projected,protocol=5)))
    if len(rows)!=int(source_commit["nrows"]):
        raise RuntimeError(
            "aggregate projected source output row count is inconsistent")
    with transaction(con):
        _insert_commit(
            con,target_consumer_id,source_seq,
            source_commit["kind"],rows)
    return commit_info(
        con,target_consumer_id,source_seq)


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
                key_blob,0,
                pickle.dumps(
                    _public_row(info["spec"],current),
                    protocol=5)))
    with transaction(con):
        _insert_commit(
            con,consumer_id,int(source_seq),"incremental",rows)
    return commit_info(con,consumer_id,source_seq)


def copy_commit(
        con,source_consumer_id,target_consumer_id,source_seq
):
    """Copy one durable serialized output commit between identical consumers.

    The target stream must already exist. Copying bytes instead of recomputing
    aggregate rows is the steady-state sharing primitive for identical queries.
    """
    source_consumer_id=_text(
        source_consumer_id,"source_consumer_id")
    target_consumer_id=_text(
        target_consumer_id,"target_consumer_id")
    source_seq=int(source_seq)
    if source_consumer_id==target_consumer_id:
        raise ValueError(
            "aggregate output copy source and target consumers must differ")
    _require_sealed(con,source_consumer_id,source_seq)
    source=commit_info(
        con,source_consumer_id,source_seq)
    stream=stream_info(
        con,target_consumer_id)
    if source_seq<int(stream["fixed_w"]):
        raise RuntimeError(
            "aggregate copied output commit predates target fixed-W")
    previous=con.execute("""
        SELECT MAX(source_seq)
        FROM aggregate_output_commits
        WHERE consumer_id=?
    """,(target_consumer_id,)).fetchone()[0]
    if previous is not None and source_seq>int(previous)+1:
        raise RuntimeError(
            "aggregate copied output commit would create a target gap")
    rows=[
        (bytes(key_blob),int(op),bytes(payload))
        for key_blob,op,payload in con.execute("""
            SELECT key_blob,op,row_payload
            FROM aggregate_output_rows
            WHERE consumer_id=? AND source_seq=?
            ORDER BY key_blob
        """,(source_consumer_id,source_seq)).fetchall()
    ]
    if len(rows)!=int(source["nrows"]):
        raise RuntimeError(
            "aggregate source output commit row count is inconsistent")
    with transaction(con):
        inserted=_insert_commit(
            con,target_consumer_id,source_seq,
            source["kind"],rows)
    copied=commit_info(
        con,target_consumer_id,source_seq)
    if copied["digest"]!=source["digest"]:
        raise RuntimeError(
            "aggregate copied output commit digest differs from source")
    return copied


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


def _require_sealed(con,consumer_id,source_seq):
    _ensure_seed_table(con)
    if con.execute("""
        SELECT 1 FROM aggregate_output_seeds
        WHERE consumer_id=? AND source_seq=?
    """,(consumer_id,int(source_seq))).fetchone():
        raise RuntimeError(
            "aggregate output commit is still being seeded")


def pending_commits(con,consumer_id,limit=100):
    _ensure_seed_table(con)
    consumer_id=_text(consumer_id,"consumer_id")
    return [
        commit_info(con,consumer_id,row[0])
        for row in con.execute("""
            SELECT c.source_seq FROM aggregate_output_commits c
            WHERE c.consumer_id=? AND c.visible=0
              AND NOT EXISTS (
                SELECT 1 FROM aggregate_output_seeds s
                WHERE s.consumer_id=c.consumer_id AND s.source_seq=c.source_seq)
            ORDER BY c.source_seq LIMIT ?
        """,(consumer_id,max(1,int(limit))))
    ]


def mark_visible(con,consumer_id,source_seq):
    consumer_id=_text(consumer_id,"consumer_id")
    source_seq=int(source_seq)
    now=time.time()
    _require_sealed(con,consumer_id,source_seq)
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
