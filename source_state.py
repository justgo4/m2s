#!/usr/bin/env python3
"""Correctness-first shared source state for P6A/P6B.

The authoritative source log and the materialized base deliberately have
separate progress.  A source transaction is first durably logged.  Base apply is
a replayable second phase.  fixed-W readers may only choose an applied,
complete watermark and pin both the versions needed by S(W) and the changelog
needed after W.

This SQLite implementation is the first protocol implementation, not the final
storage-engine decision.
"""
import base64
import contextlib
import datetime
import decimal
import hashlib
import json
import pickle
import time
import uuid

import pyarrow as pa


SOURCE_ARROW_MAGIC = b"M2SSRC1\0"
SOURCE_ARROW_WRITE_OPTIONS = pa.ipc.IpcWriteOptions(compression="zstd")


def temp_store_info(con):
    mode=int(con.execute("PRAGMA temp_store").fetchone()[0])
    options=[
        str(row[0])
        for row in con.execute("PRAGMA compile_options")
        if str(row[0]).startswith("TEMP_STORE=")
    ]
    return dict(
        mode=mode,
        name={0:"default",1:"file",2:"memory"}.get(
            mode,"unknown"),
        compile_option=(
            options[0] if len(options)==1
            else ",".join(sorted(options)) or None
        ),
    )


def require_file_temp_store(con):
    info=temp_store_info(con)
    if info["mode"]==1:
        return info
    if con.execute("""
        SELECT 1 FROM sqlite_temp_master
        LIMIT 1
    """).fetchone() is not None:
        raise RuntimeError(
            "SQLite temp_store must be configured before TEMP objects exist")
    con.execute("PRAGMA temp_store=FILE")
    info=temp_store_info(con)
    if info["mode"]!=1:
        raise RuntimeError(
            "SQLite temp_store=FILE is required for bounded source staging; "
            "actual=%s compile_option=%s"
            % (info["name"],info["compile_option"]))
    return info


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
        CREATE TABLE IF NOT EXISTS source_state_meta(
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL);

        CREATE TABLE IF NOT EXISTS source_relations(
            table_name TEXT PRIMARY KEY,
            source_epoch TEXT NOT NULL,
            schema_epoch INTEGER NOT NULL,
            schema_hash TEXT NOT NULL,
            schema_bytes BLOB NOT NULL,
            columns_json TEXT NOT NULL,
            pk_json TEXT NOT NULL,
            complete_seq INTEGER,
            snapshot_cursor BLOB,
            snapshot_upper BLOB,
            snapshot_upper_set INTEGER NOT NULL DEFAULT 0);

        CREATE TABLE IF NOT EXISTS source_commits(
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            source_epoch TEXT NOT NULL,
            source_file TEXT NOT NULL,
            source_pos INTEGER NOT NULL,
            gtid TEXT,
            created REAL NOT NULL,
            base_applied INTEGER NOT NULL DEFAULT 0,
            UNIQUE(source_epoch,source_file,source_pos));

        CREATE TABLE IF NOT EXISTS source_commit_parts(
            seq INTEGER NOT NULL REFERENCES source_commits(seq) ON DELETE CASCADE,
            part INTEGER NOT NULL,
            table_name TEXT NOT NULL,
            schema_epoch INTEGER NOT NULL,
            payload BLOB NOT NULL,
            nrows INTEGER NOT NULL,
            PRIMARY KEY(seq,part));

        CREATE INDEX IF NOT EXISTS source_commit_parts_table
            ON source_commit_parts(table_name,seq,part);

        CREATE TABLE IF NOT EXISTS source_versions(
            table_name TEXT NOT NULL,
            pk BLOB NOT NULL,
            valid_from INTEGER NOT NULL,
            valid_to INTEGER,
            deleted INTEGER NOT NULL,
            row_payload BLOB,
            schema_epoch INTEGER NOT NULL,
            PRIMARY KEY(table_name,pk,valid_from));

        CREATE INDEX IF NOT EXISTS source_versions_gc
            ON source_versions(valid_to)
            WHERE valid_to IS NOT NULL;

        CREATE TABLE IF NOT EXISTS source_pins(
            pin_id TEXT PRIMARY KEY,
            watermark INTEGER NOT NULL,
            owner TEXT NOT NULL,
            created REAL NOT NULL);

        CREATE INDEX IF NOT EXISTS source_pins_watermark
            ON source_pins(watermark);
        CREATE UNIQUE INDEX IF NOT EXISTS source_pins_owner
            ON source_pins(owner);

        CREATE TABLE IF NOT EXISTS source_consumers(
            consumer_id TEXT PRIMARY KEY,
            watermark INTEGER NOT NULL,
            owner TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            created REAL NOT NULL,
            updated REAL NOT NULL);

        CREATE INDEX IF NOT EXISTS source_consumers_watermark
            ON source_consumers(watermark);

        CREATE TABLE IF NOT EXISTS source_touched(
            table_name TEXT NOT NULL,
            pk BLOB NOT NULL,
            PRIMARY KEY(table_name,pk)) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS source_log_stats(
            table_name TEXT PRIMARY KEY,
            commits INTEGER NOT NULL CHECK(commits>=0),
            event_rows INTEGER NOT NULL CHECK(event_rows>=0),
            payload_bytes INTEGER NOT NULL CHECK(payload_bytes>=0));

        CREATE TABLE IF NOT EXISTS source_pipeline_stats(
            id INTEGER PRIMARY KEY CHECK(id=1),
            log_commits INTEGER NOT NULL CHECK(log_commits>=0),
            log_parts INTEGER NOT NULL CHECK(log_parts>=0),
            log_rows INTEGER NOT NULL CHECK(log_rows>=0),
            log_payload_bytes INTEGER NOT NULL CHECK(log_payload_bytes>=0),
            log_work_ns INTEGER NOT NULL CHECK(log_work_ns>=0),
            apply_commits INTEGER NOT NULL CHECK(apply_commits>=0),
            apply_input_rows INTEGER NOT NULL CHECK(apply_input_rows>=0),
            apply_actions INTEGER NOT NULL CHECK(apply_actions>=0),
            apply_work_ns INTEGER NOT NULL CHECK(apply_work_ns>=0));
        INSERT OR IGNORE INTO source_pipeline_stats(
            id,log_commits,log_parts,log_rows,log_payload_bytes,
            log_work_ns,apply_commits,apply_input_rows,apply_actions,
            apply_work_ns)
        VALUES(1,0,0,0,0,0,0,0,0,0);
    """)
    # These names were durable tables in older builds, but both are rebuildable
    # one-transaction scratch. Dropping legacy copies is safe because recovery
    # comes from source_commit_parts/source_versions plus the durable cursors.
    # Qualify main explicitly so an already-created TEMP table is never dropped.
    con.execute("DROP TABLE IF EXISTS main.source_apply_actions")
    con.execute("DROP TABLE IF EXISTS main.source_snapshot_rows")
    # Older builds maintained a wide visibility index. Current point/update
    # paths explicitly use the source_versions primary-key index and history
    # reclamation explicitly uses source_versions_gc, so retaining the old
    # B-tree only amplifies every version INSERT/UPDATE/DELETE.
    con.execute("DROP INDEX IF EXISTS source_versions_visible")
    indexes={
        str(row[1]):str(row[3])
        for row in con.execute(
            "PRAGMA index_list(source_versions)")
    }
    if indexes.get(
        "sqlite_autoindex_source_versions_1"
    )!="pk":
        raise RuntimeError(
            "source_versions primary-key index contract changed")
    if "source_versions_visible" in indexes:
        raise RuntimeError(
            "legacy source_versions visibility index survived migration")
    if indexes.get("source_versions_gc")!="c":
        raise RuntimeError(
            "source_versions GC index contract changed")
    if _meta_int(con, "log_durable_seq", None) is None:
        with transaction(con):
            _meta_set_int(con, "log_durable_seq", 0)
            _meta_set_int(con, "base_applied_seq", 0)
            _meta_set_int(con, "min_readable_seq", 0)
            _meta_set_int(con, "apply_pending_bytes", 0)
    elif _meta_int(con, "min_readable_seq", None) is None:
        # Older shared-state builds could already have GCed historical
        # versions without recording the physical history floor. Migrate
        # conservatively: current applied state is always readable.
        with transaction(con):
            _meta_set_int(
                con, "min_readable_seq", base_applied_seq(con))
    if _meta_int(con,"apply_pending_bytes",None) is None:
        with transaction(con):
            pending_bytes=int(con.execute("""
                SELECT COALESCE(SUM(length(p.payload)),0)
                FROM source_commit_parts p
                JOIN source_commits c ON c.seq=p.seq
                WHERE c.base_applied=0
            """).fetchone()[0] or 0)
            _meta_set_int(
                con,"apply_pending_bytes",pending_bytes)
    if _meta_int(con,"log_stats_v1",0)!=1:
        with transaction(con):
            retained=con.execute("""
                SELECT MIN(p.seq),MIN(c.created)
                FROM source_commit_parts p
                JOIN source_commits c ON c.seq=p.seq
            """).fetchone()
            first_seq=(
                int(retained[0])
                if retained and retained[0] is not None
                else log_durable_seq(con)+1
            )
            first_time=(
                float(retained[1])
                if retained and retained[1] is not None
                else time.time()
            )
            con.execute("DELETE FROM source_log_stats")
            con.execute("""
                INSERT INTO source_log_stats(
                    table_name,commits,event_rows,payload_bytes)
                SELECT table_name,COUNT(DISTINCT seq),
                       COALESCE(SUM(nrows),0),
                       COALESCE(SUM(length(payload)),0)
                FROM source_commit_parts
                GROUP BY table_name
            """)
            _meta_set_int(
                con,"log_stats_started_seq",first_seq)
            _meta_set_float(
                con,"log_stats_started_at",first_time)
            _meta_set_int(
                con,"log_stats_v1",1)


def _meta_int(con, key, default=0):
    row = con.execute(
        "SELECT value FROM source_state_meta WHERE key=?", (key,)
    ).fetchone()
    if not row:
        return default
    return int(row[0])


def _meta_set_int(con, key, value):
    con.execute("""
        INSERT INTO source_state_meta(key,value) VALUES(?,?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
    """, (key, str(int(value))))


def _meta_float(con,key,default=None):
    row=con.execute(
        "SELECT value FROM source_state_meta WHERE key=?",
        (str(key),)
    ).fetchone()
    if row is None:
        return default
    return float(row[0])


def _meta_set_float(con,key,value):
    con.execute("""
        INSERT INTO source_state_meta(key,value) VALUES(?,?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
    """,(str(key),repr(float(value))))


def log_durable_seq(con):
    return int(_meta_int(con, "log_durable_seq", 0))


def base_applied_seq(con):
    return int(_meta_int(con, "base_applied_seq", 0))


def apply_pending_bytes(con):
    return int(_meta_int(con, "apply_pending_bytes", 0))


def min_readable_seq(con):
    return int(_meta_int(con, "min_readable_seq", 0))


def _schema_bytes(schema):
    return schema.serialize().to_pybytes()


def _schema_from_bytes(value):
    return pa.ipc.read_schema(pa.BufferReader(value))


def _relation_hash(schema_bytes, pk_columns):
    digest = hashlib.sha256()
    digest.update(schema_bytes)
    digest.update(b"\0")
    digest.update(json.dumps(
        list(pk_columns), ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8"))
    return digest.hexdigest()


def register_relation(
        con, table_name, source_epoch, schema, pk_columns, schema_epoch=1
):
    table_name = str(table_name)
    source_epoch = str(source_epoch)
    pk_columns = [str(value) for value in pk_columns]
    if not table_name or not source_epoch or not pk_columns:
        raise ValueError("table_name, source_epoch and primary key are required")
    names = list(schema.names)
    if len(set(names)) != len(names):
        raise ValueError("source schema contains duplicate columns")
    if any(name not in names for name in pk_columns):
        raise ValueError("primary key column is absent from source schema")
    schema_epoch = int(schema_epoch)
    if schema_epoch < 1:
        raise ValueError("schema_epoch must be >= 1")
    schema_bytes = _schema_bytes(schema)
    schema_hash = _relation_hash(schema_bytes, pk_columns)
    columns_json = json.dumps(names, ensure_ascii=False, separators=(",", ":"))
    pk_json = json.dumps(pk_columns, ensure_ascii=False, separators=(",", ":"))
    row = con.execute("""
        SELECT source_epoch,schema_epoch,schema_hash,columns_json,pk_json
        FROM source_relations WHERE table_name=?
    """, (table_name,)).fetchone()
    if row:
        expected = (
            source_epoch, schema_epoch, schema_hash, columns_json, pk_json
        )
        actual = (str(row[0]), int(row[1]), str(row[2]), row[3], row[4])
        if actual != expected:
            raise RuntimeError(
                "source relation identity/schema changed; explicit migration or "
                "rebuild is required"
            )
        return schema_hash
    con.execute("""
        INSERT INTO source_relations(
            table_name,source_epoch,schema_epoch,schema_hash,schema_bytes,
            columns_json,pk_json)
        VALUES(?,?,?,?,?,?,?)
    """, (
        table_name, source_epoch, schema_epoch, schema_hash, schema_bytes,
        columns_json, pk_json,
    ))
    return schema_hash


def relation_info(con, table_name):
    row = con.execute("""
        SELECT source_epoch,schema_epoch,schema_hash,schema_bytes,
               columns_json,pk_json,complete_seq,snapshot_cursor,
               snapshot_upper,snapshot_upper_set
        FROM source_relations WHERE table_name=?
    """, (str(table_name),)).fetchone()
    if not row:
        raise KeyError("unknown source relation: " + str(table_name))
    return dict(
        table_name=str(table_name),
        source_epoch=str(row[0]),
        schema_epoch=int(row[1]),
        schema_hash=str(row[2]),
        schema=_schema_from_bytes(row[3]),
        columns=json.loads(row[4]),
        pk_columns=json.loads(row[5]),
        complete_seq=None if row[6] is None else int(row[6]),
        snapshot_cursor=None if row[7] is None else pickle.loads(row[7]),
        snapshot_upper=None if row[8] is None else pickle.loads(row[8]),
        snapshot_upper_set=bool(row[9]),
    )


def encode_batch(table):
    sink = pa.BufferOutputStream()
    sink.write(SOURCE_ARROW_MAGIC)
    with pa.ipc.new_stream(
        sink, table.schema, options=SOURCE_ARROW_WRITE_OPTIONS
    ) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def decode_batch(payload):
    view = memoryview(payload)
    if len(view) < len(SOURCE_ARROW_MAGIC):
        raise ValueError("truncated source Arrow payload")
    if view[:len(SOURCE_ARROW_MAGIC)].tobytes() != SOURCE_ARROW_MAGIC:
        raise ValueError("invalid source Arrow payload magic")
    return pa.ipc.open_stream(
        pa.BufferReader(view[len(SOURCE_ARROW_MAGIC):])
    ).read_all()


def prepare_part(table_name, table, schema_epoch=1):
    required = {"_sync_op", "_sync_order"}
    if not required.issubset(table.column_names):
        raise ValueError("source batch lacks _sync_op/_sync_order")
    return dict(
        table_name=str(table_name),
        schema_epoch=int(schema_epoch),
        payload=encode_batch(table),
        nrows=int(table.num_rows),
    )


def _validate_commit_replay(con, seq, gtid, parts):
    seq=int(seq)
    stored=con.execute(
        "SELECT gtid FROM source_commits WHERE seq=?",
        (seq,)
    ).fetchone()
    if stored is None:
        raise RuntimeError(
            "source commit disappeared during replay validation")
    incoming_gtid=(
        None if gtid is None else str(gtid))
    if stored[0]!=incoming_gtid:
        raise RuntimeError(
            "source position replay GTID mismatch")
    incoming=iter(parts)
    durable=con.execute("""
        SELECT part,table_name,schema_epoch,payload,nrows
        FROM source_commit_parts
        WHERE seq=?
        ORDER BY part
    """,(seq,))
    for part_no,table_name,schema_epoch,payload,nrows in durable:
        try:
            part=next(incoming)
        except StopIteration as exc:
            raise RuntimeError(
                "source position replay has fewer parts "
                "than the durable commit") from exc
        incoming_payload=sqlite_blob(part["payload"])
        if (
            int(part_no)<0
            or str(part["table_name"])!=str(table_name)
            or int(part["schema_epoch"])!=int(schema_epoch)
            or int(part["nrows"])!=int(nrows)
            or incoming_payload!=bytes(payload)
        ):
            raise RuntimeError(
                "source position replay differs from the durable commit")
    try:
        next(incoming)
    except StopIteration:
        return seq
    raise RuntimeError(
        "source position replay has more parts "
        "than the durable commit")


def log_commit_tx(con, source_epoch, position, gtid, parts):
    source_epoch = str(source_epoch)
    source_file, source_pos = str(position[0]), int(position[1])
    existing = con.execute("""
        SELECT seq FROM source_commits
        WHERE source_epoch=? AND source_file=? AND source_pos=?
    """, (source_epoch, source_file, source_pos)).fetchone()
    if existing:
        return _validate_commit_replay(
            con,int(existing[0]),gtid,parts)

    started_ns=time.perf_counter_ns()
    part_count=0
    row_count=0
    payload_bytes=0
    cursor = con.execute("""
        INSERT INTO source_commits(
            source_epoch,source_file,source_pos,gtid,created,base_applied)
        VALUES(?,?,?,?,?,0)
    """, (
        source_epoch, source_file, source_pos,
        None if gtid is None else str(gtid), time.time(),
    ))
    seq = int(cursor.lastrowid)
    for part_no, part in enumerate(parts):
        info = relation_info(con, part["table_name"])
        if info["source_epoch"] != source_epoch:
            raise RuntimeError("source epoch mismatch while logging source commit")
        if int(part["schema_epoch"]) != info["schema_epoch"]:
            raise RuntimeError("schema epoch mismatch while logging source commit")
        payload=sqlite_blob(part["payload"])
        nrows=int(part["nrows"])
        part_count+=1
        row_count+=nrows
        payload_bytes+=len(payload)
        con.execute("""
            INSERT INTO source_commit_parts(
                seq,part,table_name,schema_epoch,payload,nrows)
            VALUES(?,?,?,?,?,?)
        """, (
            seq, int(part_no), part["table_name"],
            int(part["schema_epoch"]), payload,nrows,
        ))
    # One source transaction can be split into multiple durable parts for the
    # same table. Aggregate by (seq, table) so "commits" keeps the same meaning
    # as install()'s COUNT(DISTINCT seq) rebuild instead of counting parts.
    con.execute("""
        INSERT INTO source_log_stats(
            table_name,commits,event_rows,payload_bytes)
        SELECT table_name,1,
               COALESCE(SUM(nrows),0),
               COALESCE(SUM(length(payload)),0)
        FROM source_commit_parts
        WHERE seq=?
        GROUP BY table_name
        ON CONFLICT(table_name) DO UPDATE SET
            commits=source_log_stats.commits+1,
            event_rows=source_log_stats.event_rows+excluded.event_rows,
            payload_bytes=(
                source_log_stats.payload_bytes+excluded.payload_bytes)
    """,(seq,))
    previous = log_durable_seq(con)
    if seq <= previous:
        raise RuntimeError("source commit sequence did not advance")
    pending_bytes=apply_pending_bytes(con)
    _meta_set_int(
        con,"apply_pending_bytes",
        pending_bytes+int(payload_bytes))
    _meta_set_int(con, "log_durable_seq", seq)
    elapsed_ns=max(
        0,time.perf_counter_ns()-started_ns)
    con.execute("""
        UPDATE source_pipeline_stats
        SET log_commits=log_commits+1,
            log_parts=log_parts+?,
            log_rows=log_rows+?,
            log_payload_bytes=log_payload_bytes+?,
            log_work_ns=log_work_ns+?
        WHERE id=1
    """,(
        int(part_count),int(row_count),
        int(payload_bytes),int(elapsed_ns),
    ))
    return seq


def log_commit(con, source_epoch, position, gtid, parts):
    with transaction(con):
        return log_commit_tx(con, source_epoch, position, gtid, parts)


def sqlite_blob(value):
    # sqlite3 accepts bytes-like objects, but bytes keeps ownership explicit
    # after Arrow buffers leave scope.
    return bytes(value)


def _tag_key_value(value):
    if value is None:
        return ["null"]
    if isinstance(value, bool):
        return ["bool", 1 if value else 0]
    if isinstance(value, int):
        return ["int", str(value)]
    if isinstance(value, decimal.Decimal):
        return ["decimal", format(value, "f")]
    if isinstance(value, str):
        return ["str", value]
    if isinstance(value, (bytes, bytearray, memoryview)):
        return ["bytes", base64.b64encode(bytes(value)).decode("ascii")]
    if isinstance(value, datetime.datetime):
        return ["datetime", value.isoformat()]
    if isinstance(value, datetime.date):
        return ["date", value.isoformat()]
    if isinstance(value, datetime.time):
        return ["time", value.isoformat()]
    if isinstance(value, float):
        return ["float", value.hex()]
    raise TypeError("unsupported primary key value type: " + type(value).__name__)


def key_bytes(values):
    tagged = [_tag_key_value(value) for value in values]
    return json.dumps(
        tagged, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")


def _batch_values(batch, columns):
    return {
        str(name):batch.column(name).to_pylist()
        for name in columns
    }


def _values_tuple(values, row_index, columns):
    return tuple(
        values[name][row_index]
        for name in columns)


def _values_key(values, row_index, pk_columns):
    return key_bytes(
        values[name][row_index]
        for name in pk_columns)


def _ensure_apply_staging(con):
    # Durable replay comes from source_commit_parts. This is one-transaction
    # scratch, so keeping it in the main WAL only duplicates write traffic.
    # FILE is a hard contract so a large transaction cannot silently turn the
    # whole net-change table into process RSS under a MEMORY/default build.
    require_file_temp_store(con)
    con.execute("""
        CREATE TEMP TABLE IF NOT EXISTS source_apply_actions(
            seq INTEGER NOT NULL,
            table_name TEXT NOT NULL,
            pk BLOB NOT NULL,
            deleted INTEGER NOT NULL,
            row_payload BLOB,
            schema_epoch INTEGER NOT NULL,
            PRIMARY KEY(seq,table_name,pk)
        ) WITHOUT ROWID
    """)


def _ensure_snapshot_staging(con):
    # Snapshot cursor/source_versions are durable. Page scratch is rebuildable
    # and therefore belongs in the connection-local TEMP schema.
    require_file_temp_store(con)
    con.execute("""
        CREATE TEMP TABLE IF NOT EXISTS source_snapshot_rows(
            table_name TEXT NOT NULL,
            pk BLOB NOT NULL,
            row_payload BLOB NOT NULL,
            schema_epoch INTEGER NOT NULL,
            PRIMARY KEY(table_name,pk)
        ) WITHOUT ROWID
    """)


def _stage_commit_actions(con, seq):
    """Net one source transaction into SQLite without materializing a Python dict."""
    seq=int(seq)
    con.execute(
        "DELETE FROM source_apply_actions WHERE seq=?",
        (seq,))
    parts=con.execute("""
        SELECT part,table_name,schema_epoch,payload,nrows
        FROM source_commit_parts
        WHERE seq=?
        ORDER BY part
    """,(seq,))
    relations={}
    for _,table_name,schema_epoch,payload,nrows in parts:
        info=relations.get(table_name)
        if info is None:
            info=relation_info(con,table_name)
            relations[table_name]=info
        if int(schema_epoch)!=info["schema_epoch"]:
            raise RuntimeError(
                "source commit schema epoch changed before apply")
        batch=decode_batch(payload)
        if int(batch.num_rows)!=int(nrows):
            raise RuntimeError(
                "source commit part row count differs from durable metadata")
        expected=(
            info["columns"]
            +["_sync_op","_sync_order"]
        )
        if batch.column_names!=expected:
            raise RuntimeError(
                table_name
                +": source log Arrow schema differs from relation"
            )
        values=_batch_values(
            batch,expected)
        op_values=values["_sync_op"]
        order_values=values["_sync_order"]
        previous_order=None
        for row_index in range(batch.num_rows):
            row_order=int(
                order_values[row_index])
            if (
                previous_order is not None
                and previous_order>row_order
            ):
                raise RuntimeError(
                    "source batch order is not monotonic")
            previous_order=row_order
            op=int(
                op_values[row_index])
            if op not in (0,1):
                raise RuntimeError(
                    "unsupported source mutation op")
            pk=_values_key(
                values,row_index,
                info["pk_columns"])
            if op==1:
                deleted=1
                row_payload=None
            else:
                deleted=0
                row=_values_tuple(
                    values,row_index,
                    info["columns"])
                row_payload=pickle.dumps(
                    row,protocol=5)
            con.execute("""
                INSERT INTO source_apply_actions(
                    seq,table_name,pk,deleted,
                    row_payload,schema_epoch)
                VALUES(?,?,?,?,?,?)
                ON CONFLICT(seq,table_name,pk)
                DO UPDATE SET
                    deleted=excluded.deleted,
                    row_payload=excluded.row_payload,
                    schema_epoch=excluded.schema_epoch
            """,(
                seq,str(table_name),pk,
                int(deleted),row_payload,
                int(info["schema_epoch"]),
            ))
    return int(con.execute("""
        SELECT COUNT(*)
        FROM source_apply_actions
        WHERE seq=?
    """,(seq,)).fetchone()[0])


def apply_one(con, seq):
    seq=int(seq)
    started_ns=time.perf_counter_ns()
    input_rows,input_payload_bytes=con.execute("""
        SELECT COALESCE(SUM(nrows),0),
               COALESCE(SUM(length(payload)),0)
        FROM source_commit_parts
        WHERE seq=?
    """,(seq,)).fetchone()
    input_rows=int(input_rows or 0)
    input_payload_bytes=int(input_payload_bytes or 0)
    _ensure_apply_staging(con)
    with transaction(con):
        applied=base_applied_seq(con)
        if seq<=applied:
            return False
        if seq!=applied+1:
            raise RuntimeError(
                "source base apply gap: expected %d got %d"
                % (applied+1,seq)
            )
        row=con.execute(
            "SELECT base_applied "
            "FROM source_commits WHERE seq=?",
            (seq,)
        ).fetchone()
        if not row:
            raise RuntimeError(
                "source commit disappeared before base apply")
        if int(row[0]):
            raise RuntimeError(
                "source commit marked applied ahead of base watermark")

        action_count=_stage_commit_actions(
            con,seq)
        missing_relation=con.execute("""
            SELECT a.table_name
            FROM source_apply_actions a
            LEFT JOIN source_relations r
              ON r.table_name=a.table_name
            WHERE a.seq=? AND r.table_name IS NULL
            LIMIT 1
        """,(seq,)).fetchone()
        if missing_relation is not None:
            raise RuntimeError(
                "source relation disappeared before base apply")

        duplicate_current=con.execute("""
            SELECT v.table_name,v.pk
            FROM source_versions v INDEXED BY sqlite_autoindex_source_versions_1
            JOIN source_apply_actions a
              ON a.seq=?
             AND a.table_name=v.table_name
             AND a.pk=v.pk
            WHERE v.valid_to IS NULL
            GROUP BY v.table_name,v.pk
            HAVING COUNT(*)>1
            LIMIT 1
        """,(seq,)).fetchone()
        if duplicate_current is not None:
            raise RuntimeError(
                "multiple current source versions for one key")

        con.execute("""
            UPDATE source_versions
            SET valid_to=?
            WHERE rowid IN (
                SELECT v.rowid
                FROM source_apply_actions a
                JOIN source_versions v INDEXED BY sqlite_autoindex_source_versions_1
                  ON v.table_name=a.table_name
                 AND v.pk=a.pk
                 AND v.valid_to IS NULL
                WHERE a.seq=?
            )
        """,(seq,seq))
        con.execute("""
            INSERT INTO source_versions(
                table_name,pk,valid_from,
                valid_to,deleted,row_payload,
                schema_epoch)
            SELECT table_name,pk,?,NULL,
                   deleted,row_payload,schema_epoch
            FROM source_apply_actions
            WHERE seq=?
            ORDER BY table_name,pk
        """,(seq,seq))
        con.execute("""
            INSERT OR IGNORE INTO source_touched(
                table_name,pk)
            SELECT a.table_name,a.pk
            FROM source_apply_actions a
            JOIN source_relations r
              ON r.table_name=a.table_name
            WHERE a.seq=? AND r.complete_seq IS NULL
        """,(seq,))

        con.execute(
            "DELETE FROM source_apply_actions "
            "WHERE seq=?",
            (seq,))
        con.execute(
            "UPDATE source_commits "
            "SET base_applied=1 "
            "WHERE seq=?",
            (seq,))
        pending_bytes=apply_pending_bytes(con)
        if input_payload_bytes>pending_bytes:
            raise RuntimeError(
                "source apply pending byte counter underflow")
        _meta_set_int(
            con,"apply_pending_bytes",
            pending_bytes-input_payload_bytes)
        _meta_set_int(
            con,"base_applied_seq",seq)
        elapsed_ns=max(
            0,time.perf_counter_ns()-started_ns)
        con.execute("""
            UPDATE source_pipeline_stats
            SET apply_commits=apply_commits+1,
                apply_input_rows=apply_input_rows+?,
                apply_actions=apply_actions+?,
                apply_work_ns=apply_work_ns+?
            WHERE id=1
        """,(
            int(input_rows),
            int(action_count),
            int(elapsed_ns),
        ))
    return True


def apply_pending(con, max_commits=None):
    count = 0
    while max_commits is None or count < int(max_commits):
        next_seq = base_applied_seq(con) + 1
        row = con.execute(
            "SELECT seq FROM source_commits WHERE seq=?", (next_seq,)
        ).fetchone()
        if not row:
            break
        if apply_one(con, next_seq):
            count += 1
    return count


def snapshot_set_upper(con, table_name, upper):
    with transaction(con):
        info = relation_info(con, table_name)
        if info["snapshot_upper_set"]:
            if info["snapshot_upper"] != upper:
                raise RuntimeError("source snapshot upper bound changed")
            return
        con.execute("""
            UPDATE source_relations
            SET snapshot_upper=?,snapshot_upper_set=1
            WHERE table_name=?
        """, (pickle.dumps(upper, protocol=5), str(table_name)))


def _baseline_rows(con, table_name, batch):
    info = relation_info(con, table_name)
    expected_prefix = info["columns"]
    if batch.column_names[:len(expected_prefix)] != expected_prefix:
        raise RuntimeError("snapshot source columns differ from registered relation")

    # Snapshot rows are staged once, then filtered against CDC-touched/current
    # keys set-wise. This keeps the correctness rule identical to the former
    # per-row probes without issuing two indexed SELECTs for every history row.
    con.execute(
        "DELETE FROM source_snapshot_rows WHERE table_name=?",
        (str(table_name),))
    values=_batch_values(
        batch,info["columns"])
    rows = (
        (
            str(table_name),
            _values_key(
                values,row_index,
                info["pk_columns"]),
            pickle.dumps(
                _values_tuple(
                    values,row_index,
                    info["columns"]),
                protocol=5),
            int(info["schema_epoch"]),
        )
        for row_index in range(batch.num_rows)
    )
    con.executemany("""
        INSERT INTO source_snapshot_rows(
            table_name,pk,row_payload,schema_epoch)
        VALUES(?,?,?,?)
    """, rows)

    con.execute("""
        INSERT INTO source_versions(
            table_name,pk,valid_from,valid_to,
            deleted,row_payload,schema_epoch)
        SELECT
            s.table_name,s.pk,0,NULL,0,
            s.row_payload,s.schema_epoch
        FROM source_snapshot_rows s
        WHERE s.table_name=?
          AND NOT EXISTS(
              SELECT 1
              FROM source_touched t
              WHERE t.table_name=s.table_name
                AND t.pk=s.pk
          )
          AND NOT EXISTS(
              SELECT 1
              FROM source_versions v INDEXED BY sqlite_autoindex_source_versions_1
              WHERE v.table_name=s.table_name
                AND v.pk=s.pk
                AND v.valid_to IS NULL
          )
        ORDER BY s.pk
    """, (str(table_name),))
    inserted = int(con.execute(
        "SELECT changes()").fetchone()[0])
    con.execute(
        "DELETE FROM source_snapshot_rows WHERE table_name=?",
        (str(table_name),))
    return inserted


def stage_snapshot_batch(con, table_name, batch, cursor, is_last=False):
    table_name = str(table_name)
    _ensure_snapshot_staging(con)
    with transaction(con):
        info = relation_info(con, table_name)
        if info["complete_seq"] is not None:
            return 0
        inserted = _baseline_rows(con, table_name, batch)
        con.execute("""
            UPDATE source_relations SET snapshot_cursor=? WHERE table_name=?
        """, (pickle.dumps(cursor, protocol=5), table_name))
        if is_last:
            if log_durable_seq(con) != base_applied_seq(con):
                raise RuntimeError(
                    "cannot mark source relation complete while base apply lags log"
                )
            complete = base_applied_seq(con)
            con.execute("""
                UPDATE source_relations SET complete_seq=? WHERE table_name=?
            """, (complete, table_name))
            con.execute(
                "DELETE FROM source_touched WHERE table_name=?", (table_name,)
            )
    return inserted


def snapshot_safe_watermark(con, table_names):
    table_names = [str(value) for value in table_names]
    if not table_names:
        raise ValueError("at least one relation is required")
    applied = base_applied_seq(con)
    for table_name in table_names:
        info = relation_info(con, table_name)
        if info["complete_seq"] is None or info["complete_seq"] > applied:
            return None
    return applied


def acquire_pin(con, owner, table_names):
    owner = str(owner or "").strip()
    if not owner:
        raise ValueError("pin owner is required")
    with transaction(con):
        watermark = snapshot_safe_watermark(con, table_names)
        if watermark is None:
            raise RuntimeError("source relation is not complete at an applied watermark")
        pin_id = uuid.uuid4().hex
        con.execute("""
            INSERT INTO source_pins(pin_id,watermark,owner,created)
            VALUES(?,?,?,?)
        """, (pin_id, int(watermark), owner, time.time()))
    return dict(pin_id=pin_id, watermark=int(watermark))


def acquire_or_resume_pin(con, owner, table_names):
    owner = str(owner or "").strip()
    if not owner:
        raise ValueError("pin owner is required")
    row = con.execute("""
        SELECT pin_id,watermark FROM source_pins WHERE owner=?
    """, (owner,)).fetchone()
    if row:
        watermark = int(row[1])
        for table_name in table_names:
            info = relation_info(con, table_name)
            if info["complete_seq"] is None or info["complete_seq"] > watermark:
                raise RuntimeError(
                    "durable source-state pin predates relation completeness"
                )
        if watermark > base_applied_seq(con):
            raise RuntimeError("durable source-state pin is ahead of applied base")
        if watermark < min_readable_seq(con):
            raise RuntimeError(
                "durable source-state pin is behind retained history")
        return dict(pin_id=str(row[0]), watermark=watermark)
    return acquire_pin(con, owner, table_names)


def release_pin(con, pin_id):
    with transaction(con):
        con.execute("DELETE FROM source_pins WHERE pin_id=?", (str(pin_id),))


def pin_watermark(con, pin_id):
    row = con.execute(
        "SELECT watermark FROM source_pins WHERE pin_id=?", (str(pin_id),)
    ).fetchone()
    if not row:
        raise KeyError("source-state pin does not exist")
    return int(row[0])


def read_snapshot_batch(con, pin_id, table_name, after_key=None, limit=1000):
    watermark = pin_watermark(con, pin_id)
    if watermark < min_readable_seq(con):
        raise RuntimeError(
            "source snapshot pin is behind retained history")
    info = relation_info(con, table_name)
    if info["complete_seq"] is None or info["complete_seq"] > watermark:
        raise RuntimeError("relation was not complete at pinned watermark")
    limit = max(1, int(limit))
    params = [str(table_name), watermark, watermark]
    after_sql = ""
    if after_key is not None:
        after_sql = " AND pk>?"
        params.append(bytes(after_key))
    params.append(limit)
    rows = con.execute("""
        SELECT pk,row_payload
        FROM source_versions INDEXED BY sqlite_autoindex_source_versions_1
        WHERE table_name=?
          AND valid_from<=?
          AND (valid_to IS NULL OR valid_to>?)
          AND deleted=0
    """ + after_sql + " ORDER BY pk LIMIT ?", params).fetchall()

    records = []
    next_key = None
    for pk, payload in rows:
        values = pickle.loads(payload)
        records.append(dict(zip(info["columns"], values)))
        next_key = bytes(pk)
    table = pa.Table.from_pylist(records, schema=info["schema"])
    table = table.append_column(
        "_sync_op", pa.array([0] * len(records), type=pa.int8())
    ).append_column(
        "_sync_order", pa.array(range(len(records)), type=pa.int64())
    )
    return table, next_key


def read_commits(
        con, after_seq, through_seq=None, limit=100, allow_truncated=False
):
    after_seq = int(after_seq)
    retained = min_readable_seq(con)
    if after_seq < retained and not allow_truncated:
        raise RuntimeError(
            "source changelog gap: requested watermark=%d retained=%d"
            % (after_seq,retained))
    clauses = ["seq>?"]
    params = [after_seq]
    if through_seq is not None:
        clauses.append("seq<=?")
        params.append(int(through_seq))
    params.append(max(1, int(limit)))
    commits = []
    for seq, source_file, source_pos, gtid in con.execute(
        "SELECT seq,source_file,source_pos,gtid FROM source_commits WHERE "
        + " AND ".join(clauses) + " ORDER BY seq LIMIT ?",
        params,
    ):
        parts = []
        for part, table_name, schema_epoch, payload, nrows in con.execute("""
            SELECT part,table_name,schema_epoch,payload,nrows
            FROM source_commit_parts WHERE seq=? ORDER BY part
        """, (int(seq),)):
            parts.append(dict(
                part=int(part), table_name=table_name,
                schema_epoch=int(schema_epoch), payload=bytes(payload),
                nrows=int(nrows),
            ))
        commits.append(dict(
            seq=int(seq), position=(source_file, int(source_pos)),
            gtid=gtid, parts=parts,
        ))
    return commits


def consumer_info(con, consumer_id):
    row = con.execute("""
        SELECT consumer_id,watermark,owner,metadata_json,created,updated
        FROM source_consumers WHERE consumer_id=?
    """, (str(consumer_id),)).fetchone()
    if not row:
        raise KeyError("source-state consumer does not exist")
    return dict(
        consumer_id=str(row[0]),
        watermark=int(row[1]),
        owner=str(row[2]),
        metadata=json.loads(row[3]),
        created=float(row[4]),
        updated=float(row[5]),
    )


def register_consumer(
        con, consumer_id, watermark, owner=None, metadata=None
):
    consumer_id = str(consumer_id or "").strip()
    owner = str(owner or consumer_id).strip()
    if not consumer_id or not owner:
        raise ValueError("consumer_id and owner are required")
    watermark = int(watermark)
    applied = base_applied_seq(con)
    if watermark < 0 or watermark > applied:
        raise ValueError("consumer watermark is outside applied source history")
    metadata_json = json.dumps(
        metadata or {}, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"))
    now = time.time()
    with transaction(con):
        row = con.execute("""
            SELECT watermark,owner,metadata_json
            FROM source_consumers WHERE consumer_id=?
        """, (consumer_id,)).fetchone()
        if row:
            if str(row[1]) != owner or str(row[2]) != metadata_json:
                raise RuntimeError(
                    "source-state consumer identity changed; remove or migrate "
                    "the existing consumer explicitly"
                )
            if int(row[0]) != watermark:
                raise RuntimeError(
                    "source-state consumer already exists at a different "
                    "watermark; use advance_consumer"
                )
            return consumer_info(con, consumer_id)
        con.execute("""
            INSERT INTO source_consumers(
                consumer_id,watermark,owner,metadata_json,created,updated)
            VALUES(?,?,?,?,?,?)
        """, (
            consumer_id,watermark,owner,metadata_json,now,now,
        ))
    return consumer_info(con, consumer_id)


def advance_consumer(con, consumer_id, watermark):
    consumer_id = str(consumer_id)
    watermark = int(watermark)
    with transaction(con):
        row = con.execute("""
            SELECT watermark FROM source_consumers WHERE consumer_id=?
        """, (consumer_id,)).fetchone()
        if not row:
            raise KeyError("source-state consumer does not exist")
        current = int(row[0])
        if watermark < current:
            raise ValueError(
                "source-state consumer watermark cannot move backwards")
        applied = base_applied_seq(con)
        if watermark > applied:
            raise ValueError(
                "source-state consumer cannot advance beyond applied base")
        if watermark != current:
            con.execute("""
                UPDATE source_consumers
                SET watermark=?,updated=?
                WHERE consumer_id=?
            """, (watermark,time.time(),consumer_id))
    return consumer_info(con, consumer_id)


def remove_consumer(con, consumer_id):
    with transaction(con):
        con.execute(
            "DELETE FROM source_consumers WHERE consumer_id=?",
            (str(consumer_id),))


def retention_floor(con, consumer_watermarks=()):
    applied = base_applied_seq(con)
    values = [applied]
    values.extend(int(value) for value in consumer_watermarks or ())
    values.extend(
        int(row[0]) for row in con.execute("SELECT watermark FROM source_pins")
    )
    values.extend(
        int(row[0]) for row in con.execute(
            "SELECT watermark FROM source_consumers")
    )
    if any(value < 0 or value > applied for value in values):
        raise ValueError("retention watermark is outside applied source history")
    return min(values)


def incomplete_relations(con):
    return [
        str(row[0]) for row in con.execute(
            "SELECT table_name FROM source_relations "
            "WHERE complete_seq IS NULL ORDER BY table_name"
        )
    ]


def gc(
        con, consumer_watermarks=(), version_limit=None, commit_limit=None
):
    floor = retention_floor(con, consumer_watermarks)
    version_limit = (
        None if version_limit is None else int(version_limit)
    )
    commit_limit = (
        None if commit_limit is None else int(commit_limit)
    )
    if version_limit is not None and version_limit < 1:
        raise ValueError("version_limit must be >= 1")
    if commit_limit is not None and commit_limit < 1:
        raise ValueError("commit_limit must be >= 1")
    with transaction(con):
        current_min = min_readable_seq(con)
        if floor < current_min:
            raise RuntimeError(
                "source-state GC floor moved behind physical history frontier")
        # Advance the readability frontier in the same transaction as the
        # first bounded delete. A crash may leave extra obsolete rows behind,
        # but can never leave metadata claiming an already-deleted W is valid.
        _meta_set_int(con, "min_readable_seq", floor)

        if version_limit is None:
            version_rows = con.execute("""
                DELETE FROM source_versions
                WHERE valid_to IS NOT NULL AND valid_to<=?
            """, (floor,)).rowcount
        else:
            version_rows = con.execute("""
                DELETE FROM source_versions
                WHERE rowid IN (
                    SELECT rowid
                    FROM source_versions INDEXED BY source_versions_gc
                    WHERE valid_to IS NOT NULL AND valid_to<=?
                    ORDER BY valid_to,rowid
                    LIMIT ?
                )
            """, (floor,version_limit)).rowcount

        if commit_limit is None:
            commit_rows = con.execute("""
                DELETE FROM source_commits
                WHERE seq<? AND base_applied=1
            """, (floor,)).rowcount
        else:
            commit_rows = con.execute("""
                DELETE FROM source_commits
                WHERE seq IN (
                    SELECT seq FROM source_commits
                    WHERE seq<? AND base_applied=1
                    ORDER BY seq
                    LIMIT ?
                )
            """, (floor,commit_limit)).rowcount

        versions_pending = con.execute("""
            SELECT 1
            FROM source_versions INDEXED BY source_versions_gc
            WHERE valid_to IS NOT NULL AND valid_to<=?
            LIMIT 1
        """, (floor,)).fetchone() is not None
        commits_pending = con.execute("""
            SELECT 1 FROM source_commits
            WHERE seq<? AND base_applied=1
            LIMIT 1
        """, (floor,)).fetchone() is not None
    return dict(
        floor=int(floor),
        min_readable_seq=min_readable_seq(con),
        versions=max(0, int(version_rows)),
        commits=max(0, int(commit_rows)),
        versions_pending=bool(versions_pending),
        commits_pending=bool(commits_pending),
        complete=not versions_pending and not commits_pending,
    )


def status(con):
    has_pipeline=con.execute("""
        SELECT 1 FROM sqlite_master
        WHERE type='table' AND name='source_pipeline_stats'
    """).fetchone() is not None
    pipeline_row=(
        con.execute("""
            SELECT log_commits,log_parts,log_rows,log_payload_bytes,
                   log_work_ns,apply_commits,apply_input_rows,
                   apply_actions,apply_work_ns
            FROM source_pipeline_stats
            WHERE id=1
        """).fetchone()
        if has_pipeline else None
    )
    if pipeline_row is None:
        pipeline_row=(0,0,0,0,0,0,0,0,0)
    has_apply_staging=con.execute("""
        SELECT 1 FROM sqlite_temp_master
        WHERE type='table' AND name='source_apply_actions'
    """).fetchone() is not None
    apply_staging_rows=(
        int(con.execute(
            "SELECT COUNT(*) FROM temp.source_apply_actions"
        ).fetchone()[0])
        if has_apply_staging else 0
    )
    has_snapshot_staging=con.execute("""
        SELECT 1 FROM sqlite_temp_master
        WHERE type='table' AND name='source_snapshot_rows'
    """).fetchone() is not None
    snapshot_staging_rows=(
        int(con.execute(
            "SELECT COUNT(*) FROM temp.source_snapshot_rows"
        ).fetchone()[0])
        if has_snapshot_staging else 0
    )
    pipeline=dict(
        log_commits=int(pipeline_row[0]),
        log_parts=int(pipeline_row[1]),
        log_rows=int(pipeline_row[2]),
        log_payload_bytes=int(pipeline_row[3]),
        log_work_seconds=float(pipeline_row[4])/1e9,
        log_rows_per_second=(
            float(pipeline_row[2])*1e9/float(pipeline_row[4])
            if int(pipeline_row[4])>0 else None),
        apply_commits=int(pipeline_row[5]),
        apply_input_rows=int(pipeline_row[6]),
        apply_actions=int(pipeline_row[7]),
        apply_work_seconds=float(pipeline_row[8])/1e9,
        apply_rows_per_second=(
            float(pipeline_row[6])*1e9/float(pipeline_row[8])
            if int(pipeline_row[8])>0 else None),
        apply_staging_rows=apply_staging_rows,
        snapshot_staging_rows=snapshot_staging_rows,
    )
    incomplete = incomplete_relations(con)
    pins = [
        dict(pin_id=row[0], watermark=int(row[1]), owner=row[2])
        for row in con.execute(
            "SELECT pin_id,watermark,owner FROM source_pins ORDER BY created,pin_id"
        )
    ]
    consumers = [
        dict(consumer_id=row[0], watermark=int(row[1]), owner=row[2])
        for row in con.execute("""
            SELECT consumer_id,watermark,owner
            FROM source_consumers ORDER BY consumer_id
        """)
    ]
    log_stats = {
        str(row[0]):dict(
            commits=int(row[1]),
            event_rows=int(row[2]),
            payload_bytes=int(row[3]),
        )
        for row in con.execute("""
            SELECT table_name,commits,event_rows,payload_bytes
            FROM source_log_stats ORDER BY table_name
        """)
    }
    return dict(
        log_durable_seq=log_durable_seq(con),
        base_applied_seq=base_applied_seq(con),
        apply_pending_bytes=apply_pending_bytes(con),
        log_stats_started_seq=_meta_int(
            con,"log_stats_started_seq",1),
        log_stats_started_at=_meta_float(
            con,"log_stats_started_at",None),
        min_readable_seq=min_readable_seq(con),
        snapshot_safe_seq=None if incomplete else base_applied_seq(con),
        retention_floor=retention_floor(con),
        incomplete_relations=incomplete,
        pins=pins,
        consumers=consumers,
        log_stats=log_stats,
        pipeline_stats=pipeline,
    )
