#!/usr/bin/env python3
"""Durable correctness-first state for the first INNER JOIN candidate.

State is keyed by each source relation's stable primary key. Join output identity
is the pair (left source PK, right source PK), not projected values, so bag
semantics and duplicate projected rows remain retractable. A source transaction
is applied atomically by diffing affected join-key pair sets before and after
all row changes, which avoids publishing intermediate pairs when both sides
change in the same transaction.
"""
import base64
import contextlib
import datetime
import decimal
import hashlib
import json
import math
import pickle
import time


FORMAT_VERSION=1


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


def canonical_bytes(value):
    return json.dumps(
        value,ensure_ascii=False,sort_keys=True,
        separators=(",",":")).encode("utf-8")


def _text(value,name):
    value=str(value or "").strip()
    if not value:
        raise ValueError(name+" must be non-empty")
    return value


def _tag(value):
    if value is None:
        return ["null"]
    if isinstance(value,bool):
        return ["bool",1 if value else 0]
    if isinstance(value,int):
        return ["int",str(value)]
    if isinstance(value,decimal.Decimal):
        return ["decimal",format(value,"f")]
    if isinstance(value,float):
        if not math.isfinite(value):
            raise ValueError("non-finite JOIN values are unsupported")
        return ["float",value.hex()]
    if isinstance(value,str):
        return ["str",value]
    if isinstance(value,(bytes,bytearray,memoryview)):
        return ["bytes",base64.b64encode(bytes(value)).decode("ascii")]
    if isinstance(value,datetime.datetime):
        return ["datetime",value.isoformat()]
    if isinstance(value,datetime.date):
        return ["date",value.isoformat()]
    if isinstance(value,datetime.time):
        return ["time",value.isoformat()]
    raise TypeError("unsupported JOIN value type: "+type(value).__name__)


def validate_spec(spec):
    if not isinstance(spec,dict):
        raise ValueError("JOIN state spec must be a dict")
    if set(spec)!={
        "format_version","kind","sources","projections","semantics"
    }:
        raise ValueError("JOIN state spec fields differ from format v1")
    if int(spec["format_version"])!=FORMAT_VERSION:
        raise ValueError("unsupported JOIN state spec format")
    if spec["kind"]!="inner_join_state":
        raise ValueError("unsupported JOIN state kind")
    sources=spec["sources"]
    if not isinstance(sources,dict) or set(sources)!={"left","right"}:
        raise ValueError("JOIN state requires left/right sources")
    for side in ("left","right"):
        source=sources[side]
        if not isinstance(source,dict) or set(source)!={
            "relation","primary_key","join_key"
        }:
            raise ValueError("invalid "+side+" JOIN state source")
        _text(source["relation"],side+" relation")
        for name in ("primary_key","join_key"):
            values=source[name]
            if (
                not isinstance(values,list)
                or not values
                or any(not isinstance(value,str) or not value for value in values)
                or len(values)!=len(set(values))
            ):
                raise ValueError("invalid "+side+" "+name)
    if len(sources["left"]["join_key"])!=len(
        sources["right"]["join_key"]
    ):
        raise ValueError("JOIN key arity differs between sources")
    if sources["left"]["relation"]==sources["right"]["relation"]:
        raise ValueError("JOIN state sources must be distinct")

    projections=spec["projections"]
    if not isinstance(projections,list) or not projections:
        raise ValueError("JOIN state requires projections")
    outputs=set()
    for item in projections:
        if not isinstance(item,dict) or set(item)!={
            "output","source","column"
        }:
            raise ValueError("invalid JOIN projection")
        output=_text(item["output"],"JOIN output")
        source=str(item["source"])
        _text(item["column"],"JOIN projection column")
        if source not in {"left","right"}:
            raise ValueError("invalid JOIN projection source")
        if output in outputs or output.startswith("_sync_"):
            raise ValueError("JOIN output name is duplicate/reserved")
        outputs.add(output)
    if spec["semantics"]!=dict(
        bag=True,nulls="sql",retract="source_pk_pair_identity"
    ):
        raise ValueError("unsupported JOIN state semantics")
    return spec


def semantic_id(spec):
    validate_spec(spec)
    return hashlib.sha256(canonical_bytes(spec)).hexdigest()


def install(con):
    con.executescript("""
        CREATE TABLE IF NOT EXISTS join_states(
            state_id TEXT PRIMARY KEY,
            spec_hash TEXT NOT NULL,
            spec_json TEXT NOT NULL,
            watermark INTEGER NOT NULL,
            last_digest TEXT,
            bootstrap_complete INTEGER NOT NULL,
            left_complete INTEGER NOT NULL,
            right_complete INTEGER NOT NULL,
            left_cursor BLOB,
            right_cursor BLOB,
            created REAL NOT NULL,
            updated REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS join_rows(
            state_id TEXT NOT NULL
                REFERENCES join_states(state_id) ON DELETE CASCADE,
            side TEXT NOT NULL CHECK(side IN ('left','right')),
            pk_blob BLOB NOT NULL,
            join_blob BLOB,
            row_payload BLOB NOT NULL,
            PRIMARY KEY(state_id,side,pk_blob));
        CREATE INDEX IF NOT EXISTS join_rows_by_key
            ON join_rows(state_id,side,join_blob,pk_blob);
    """)


def clone_projected_state(
        con,source_state_id,target_state_id,target_spec,watermark
):
    """Atomically clone a projection-only JOIN subview at exact W."""
    source_state_id=_text(
        source_state_id,"source_state_id")
    target_state_id=_text(
        target_state_id,"target_state_id")
    if source_state_id==target_state_id:
        raise ValueError(
            "JOIN projected clone source and target state ids must differ")
    target_spec=validate_spec(target_spec)
    watermark=int(watermark)
    if watermark<0:
        raise ValueError(
            "JOIN projected clone watermark cannot be negative")

    with transaction(con):
        if con.execute(
            "SELECT 1 FROM join_states WHERE state_id=?",
            (target_state_id,)
        ).fetchone():
            raise RuntimeError(
                "JOIN projected clone target state already exists")
        source=state_info(
            con,source_state_id)
        if (
            not source["bootstrap_complete"]
            or not source["left_complete"]
            or not source["right_complete"]
        ):
            raise RuntimeError(
                "JOIN projected clone source bootstrap is incomplete")
        if int(source["watermark"])!=watermark:
            raise RuntimeError(
                "JOIN projected clone source watermark changed")
        source_spec=source["spec"]
        if (
            source_spec["sources"]!=target_spec["sources"]
            or source_spec["semantics"]!=target_spec["semantics"]
        ):
            raise RuntimeError(
                "JOIN projected clone source/join semantics differ")
        source_outputs={
            item["output"]:item
            for item in source_spec["projections"]
        }
        for item in target_spec["projections"]:
            if source_outputs.get(item["output"])!=item:
                raise RuntimeError(
                    "JOIN projected clone output is not a leader subview: "
                    +item["output"])

        now=time.time()
        con.execute("""
            INSERT INTO join_states(
                state_id,spec_hash,spec_json,watermark,last_digest,
                bootstrap_complete,left_complete,right_complete,
                left_cursor,right_cursor,created,updated)
            VALUES(?,?,?,?,NULL,1,1,1,NULL,NULL,?,?)
        """,(
            target_state_id,semantic_id(target_spec),
            canonical_bytes(target_spec).decode("utf-8"),
            watermark,now,now,
        ))
        rows=con.execute("""
            SELECT side,row_payload
            FROM join_rows
            WHERE state_id=?
            ORDER BY side,pk_blob
        """,(source_state_id,)).fetchall()
        for side,payload in rows:
            row=pickle.loads(payload)
            _put_row_locked(
                con,target_state_id,str(side),
                target_spec,row)
    return state_info(
        con,target_state_id)


def clone_complete_state(
        con,source_state_id,target_state_id,spec,watermark
):
    """Atomically clone one complete current JOIN state at exact W."""
    source_state_id=_text(
        source_state_id,"source_state_id")
    target_state_id=_text(
        target_state_id,"target_state_id")
    if source_state_id==target_state_id:
        raise ValueError(
            "JOIN clone source and target state ids must differ")
    spec=validate_spec(spec)
    watermark=int(watermark)
    if watermark<0:
        raise ValueError("JOIN clone watermark cannot be negative")

    with transaction(con):
        if con.execute(
            "SELECT 1 FROM join_states WHERE state_id=?",
            (target_state_id,)
        ).fetchone():
            raise RuntimeError(
                "JOIN clone target state already exists")
        source=state_info(
            con,source_state_id)
        if (
            not source["bootstrap_complete"]
            or not source["left_complete"]
            or not source["right_complete"]
        ):
            raise RuntimeError(
                "JOIN clone source bootstrap is incomplete")
        if int(source["watermark"])!=watermark:
            raise RuntimeError(
                "JOIN clone source watermark changed "
                "expected=%d actual=%d"
                % (watermark,int(source["watermark"])))
        if source["spec_hash"]!=semantic_id(spec):
            raise RuntimeError(
                "JOIN clone source spec differs")
        now=time.time()
        con.execute("""
            INSERT INTO join_states(
                state_id,spec_hash,spec_json,watermark,last_digest,
                bootstrap_complete,left_complete,right_complete,
                left_cursor,right_cursor,created,updated)
            VALUES(?,?,?,?,NULL,1,1,1,NULL,NULL,?,?)
        """,(
            target_state_id,semantic_id(spec),
            canonical_bytes(spec).decode("utf-8"),
            watermark,now,now,
        ))
        con.execute("""
            INSERT INTO join_rows(
                state_id,side,pk_blob,join_blob,row_payload)
            SELECT ?,side,pk_blob,join_blob,row_payload
            FROM join_rows
            WHERE state_id=?
        """,(target_state_id,source_state_id))
    return state_info(
        con,target_state_id)


def _state_row(con,state_id):
    row=con.execute("""
        SELECT spec_hash,spec_json,watermark,last_digest,
               bootstrap_complete,left_complete,right_complete,
               left_cursor,right_cursor,created,updated
        FROM join_states WHERE state_id=?
    """,(_text(state_id,"state_id"),)).fetchone()
    if not row:
        raise KeyError("JOIN state does not exist")
    spec=json.loads(row[1])
    actual=semantic_id(spec)
    if actual!=str(row[0]):
        raise RuntimeError("JOIN state spec hash does not match persisted spec")
    return dict(
        state_id=str(state_id),spec_hash=actual,spec=spec,
        watermark=int(row[2]),
        last_digest=None if row[3] is None else str(row[3]),
        bootstrap_complete=bool(row[4]),
        left_complete=bool(row[5]),
        right_complete=bool(row[6]),
        left_cursor=None if row[7] is None else bytes(row[7]),
        right_cursor=None if row[8] is None else bytes(row[8]),
        created=float(row[9]),updated=float(row[10]),
    )


def state_info(con,state_id):
    return _state_row(con,state_id)


def create_state(con,state_id,spec,watermark=0,bootstrap_complete=True):
    state_id=_text(state_id,"state_id")
    spec=validate_spec(spec)
    watermark=int(watermark)
    if watermark<0:
        raise ValueError("JOIN watermark cannot be negative")
    complete=bool(bootstrap_complete)
    now=time.time()
    with transaction(con):
        con.execute("""
            INSERT INTO join_states(
                state_id,spec_hash,spec_json,watermark,last_digest,
                bootstrap_complete,left_complete,right_complete,
                left_cursor,right_cursor,created,updated)
            VALUES(?,?,?,?,NULL,?,?,?,NULL,NULL,?,?)
        """,(
            state_id,semantic_id(spec),
            canonical_bytes(spec).decode("utf-8"),
            watermark,1 if complete else 0,
            1 if complete else 0,1 if complete else 0,
            now,now,
        ))
    return state_info(con,state_id)


def ensure_state(con,state_id,spec,watermark=0):
    try:
        current=state_info(con,state_id)
    except KeyError:
        return create_state(
            con,state_id,spec,watermark,bootstrap_complete=True)
    if current["spec_hash"]!=semantic_id(spec):
        raise RuntimeError("JOIN state id was reused for new semantics")
    if not current["bootstrap_complete"]:
        raise RuntimeError("JOIN state bootstrap is incomplete")
    if current["watermark"]<int(watermark):
        raise RuntimeError(
            "existing JOIN state is behind requested watermark")
    return current


def begin_bootstrap(con,state_id,spec,fixed_w):
    state_id=_text(state_id,"state_id")
    spec=validate_spec(spec)
    fixed_w=int(fixed_w)
    if fixed_w<0:
        raise ValueError("JOIN fixed-W cannot be negative")
    try:
        current=state_info(con,state_id)
    except KeyError:
        return create_state(
            con,state_id,spec,fixed_w,bootstrap_complete=False)
    if current["spec_hash"]!=semantic_id(spec):
        raise RuntimeError(
            "JOIN bootstrap state id was reused for new semantics")
    if current["watermark"]!=fixed_w:
        raise RuntimeError("JOIN bootstrap fixed-W changed across restart")
    return current


def _required_columns(spec,side):
    source=spec["sources"][side]
    result=[]
    for name in source["primary_key"]+source["join_key"]:
        if name not in result:
            result.append(name)
    for item in spec["projections"]:
        if item["source"]==side and item["column"] not in result:
            result.append(item["column"])
    return result


def _source_row(spec,side,row):
    if not isinstance(row,dict):
        raise ValueError("JOIN source row must be a dict")
    result={}
    for name in _required_columns(spec,side):
        if name not in row:
            raise ValueError(
                side+" JOIN row lacks required column "+name)
        result[name]=row[name]
    return result


def _pk_values(spec,side,row):
    values=tuple(
        row[name] for name in spec["sources"][side]["primary_key"])
    if any(value is None for value in values):
        raise ValueError("JOIN source primary key cannot contain NULL")
    return values


def _pk_blob(spec,side,row):
    return canonical_bytes([
        _tag(value) for value in _pk_values(spec,side,row)
    ])


def _join_blob(spec,side,row):
    values=tuple(
        row[name] for name in spec["sources"][side]["join_key"])
    if any(value is None for value in values):
        return None
    return canonical_bytes([_tag(value) for value in values])


def _stored_row(con,state_id,side,pk_blob):
    row=con.execute("""
        SELECT row_payload FROM join_rows
        WHERE state_id=? AND side=? AND pk_blob=?
    """,(str(state_id),str(side),bytes(pk_blob))).fetchone()
    return None if row is None else pickle.loads(row[0])


def _put_row_locked(con,state_id,side,spec,row):
    row=_source_row(spec,side,row)
    pk_blob=_pk_blob(spec,side,row)
    join_blob=_join_blob(spec,side,row)
    con.execute("""
        INSERT INTO join_rows(
            state_id,side,pk_blob,join_blob,row_payload)
        VALUES(?,?,?,?,?)
        ON CONFLICT(state_id,side,pk_blob) DO UPDATE SET
            join_blob=excluded.join_blob,
            row_payload=excluded.row_payload
    """,(
        str(state_id),str(side),pk_blob,join_blob,
        pickle.dumps(row,protocol=5),
    ))
    return pk_blob


def _delete_row_locked(con,state_id,side,spec,row):
    row=_source_row(spec,side,row)
    pk_blob=_pk_blob(spec,side,row)
    current=_stored_row(con,state_id,side,pk_blob)
    if current is None:
        raise RuntimeError(
            side+" JOIN retract references a missing source row")
    if current!=row:
        raise RuntimeError(
            side+" JOIN retract before-image differs from durable row")
    con.execute("""
        DELETE FROM join_rows
        WHERE state_id=? AND side=? AND pk_blob=?
    """,(str(state_id),str(side),pk_blob))
    return pk_blob


def _rows_for_join_blob(con,state_id,side,join_blob):
    if join_blob is None:
        return []
    return [
        (bytes(pk_blob),pickle.loads(payload))
        for pk_blob,payload in con.execute("""
            SELECT pk_blob,row_payload
            FROM join_rows
            WHERE state_id=? AND side=? AND join_blob=?
            ORDER BY pk_blob
        """,(str(state_id),str(side),bytes(join_blob))).fetchall()
    ]


def _pair_id(left_pk,right_pk):
    return canonical_bytes([
        ["left",base64.b64encode(bytes(left_pk)).decode("ascii")],
        ["right",base64.b64encode(bytes(right_pk)).decode("ascii")],
    ])


def _project(spec,left_row,right_row):
    result={}
    for item in spec["projections"]:
        source=left_row if item["source"]=="left" else right_row
        result[item["output"]]=source[item["column"]]
    return result


def _pairs_for_keys_locked(con,state_id,spec,join_blobs):
    result={}
    for join_blob in sorted({
        bytes(value) for value in join_blobs if value is not None
    }):
        left_rows=_rows_for_join_blob(
            con,state_id,"left",join_blob)
        right_rows=_rows_for_join_blob(
            con,state_id,"right",join_blob)
        for left_pk,left_row in left_rows:
            for right_pk,right_row in right_rows:
                pair=_pair_id(left_pk,right_pk)
                result[pair]=_project(
                    spec,left_row,right_row)
    return result


def _rows_for_keys_locked(con,state_id,join_blobs,changed):
    # A changed left PK needs every matching right, but unchanged left PKs
    # cannot contribute a delta unless a right PK on this key also changed.
    # Fetch changed rows by the source-PK index before choosing each range scan.
    changed_by_key=dict(left={},right={})
    for side in ("left","right"):
        for pk_blob in sorted(changed[side]):
            row=con.execute("""
                SELECT join_blob,row_payload FROM join_rows
                WHERE state_id=? AND side=? AND pk_blob=?
            """,(str(state_id),side,bytes(pk_blob))).fetchone()
            if row is not None and row[0] is not None:
                changed_by_key[side].setdefault(bytes(row[0]),[]).append(
                    (bytes(pk_blob),pickle.loads(row[1])))
    result={}
    for join_blob in sorted({
        bytes(value) for value in join_blobs if value is not None
    }):
        left_changed=changed_by_key["left"].get(join_blob,[])
        right_changed=changed_by_key["right"].get(join_blob,[])
        result[join_blob]=dict(
            left=(_rows_for_join_blob(con,state_id,"left",join_blob)
                  if right_changed else left_changed),
            right=(_rows_for_join_blob(con,state_id,"right",join_blob)
                   if left_changed else right_changed),
        )
    return result


def _pairs_for_changed_rows(spec,rows_by_key,changed):
    """Project only pairs whose source identity changed in this transaction.

    Any pair whose left and right source PKs are both unchanged is identical
    before/after the transaction and cannot contribute a net delta. Snapshots
    retain changed PKs and all opposite matches; bilateral changes include both
    complete ranges. Key moves, fan-out and bag identity keep before/after net
    semantics without reading an unchanged side that cannot contribute.
    """
    left_changed={bytes(value) for value in changed["left"]}
    right_changed={bytes(value) for value in changed["right"]}
    result={}
    for join_blob in sorted(rows_by_key):
        rows=rows_by_key[join_blob]
        left_rows=rows["left"]
        right_rows=rows["right"]

        for left_pk,left_row in left_rows:
            if left_pk not in left_changed:
                continue
            for right_pk,right_row in right_rows:
                pair=_pair_id(left_pk,right_pk)
                result[pair]=_project(
                    spec,left_row,right_row)

        for right_pk,right_row in right_rows:
            if right_pk not in right_changed:
                continue
            for left_pk,left_row in left_rows:
                if left_pk in left_changed:
                    continue
                pair=_pair_id(left_pk,right_pk)
                result[pair]=_project(
                    spec,left_row,right_row)
    return result


def _row_join_blob_from_store(con,state_id,side,spec,row):
    pk_blob=_pk_blob(spec,side,_source_row(spec,side,row))
    current=_stored_row(con,state_id,side,pk_blob)
    return (
        None if current is None
        else _join_blob(spec,side,current)
    )


def _change_digest(source_seq,changes):
    encoded=[]
    for side,row in changes:
        if side not in {"left","right"} or not isinstance(row,dict):
            raise ValueError("invalid JOIN change")
        encoded.append([
            side,
            [[str(name),_tag(row[name])] for name in sorted(row)],
        ])
    return hashlib.sha256(canonical_bytes(dict(
        source_seq=int(source_seq),changes=encoded))).hexdigest()


def apply_bootstrap_chunk(
        con,state_id,fixed_w,side,rows,next_cursor,is_last,
        fault_after_rows=None
):
    state_id=_text(state_id,"state_id")
    fixed_w=int(fixed_w)
    side=str(side)
    if side not in {"left","right"}:
        raise ValueError("JOIN bootstrap side must be left or right")
    rows=list(rows or ())
    with transaction(con):
        current=state_info(con,state_id)
        if current["watermark"]!=fixed_w:
            raise RuntimeError("JOIN bootstrap fixed-W changed")
        if current["bootstrap_complete"]:
            return False
        if current[side+"_complete"]:
            if rows or not is_last:
                raise RuntimeError(
                    "JOIN bootstrap side is already complete")
            return False
        spec=current["spec"]
        for row in rows:
            if "_sync_op" in row and int(row["_sync_op"])!=0:
                raise ValueError(
                    "JOIN bootstrap accepts snapshot upserts only")
            _put_row_locked(con,state_id,side,spec,row)
        if fault_after_rows is not None:
            fault_after_rows()
        cursor_column=side+"_cursor"
        complete_column=side+"_complete"
        con.execute(
            "UPDATE join_states SET "
            +cursor_column+"=?, "+complete_column+"=?, updated=? "
            "WHERE state_id=?",
            (
                None if next_cursor is None else bytes(next_cursor),
                1 if is_last else 0,time.time(),state_id,
            ))
        row=con.execute("""
            SELECT left_complete,right_complete
            FROM join_states WHERE state_id=?
        """,(state_id,)).fetchone()
        if bool(row[0]) and bool(row[1]):
            con.execute("""
                UPDATE join_states
                SET bootstrap_complete=1,updated=?
                WHERE state_id=?
            """,(time.time(),state_id))
    return True


def apply_transaction(
        con,state_id,source_seq,changes,fault_after_rows=None
):
    state_id=_text(state_id,"state_id")
    source_seq=int(source_seq)
    changes=[(str(side),dict(row)) for side,row in (changes or ())]
    digest=_change_digest(source_seq,changes)

    with transaction(con):
        current=state_info(con,state_id)
        if not current["bootstrap_complete"]:
            raise RuntimeError(
                "cannot consume source log before JOIN bootstrap completes")
        watermark=int(current["watermark"])
        if source_seq==watermark:
            if current["last_digest"]==digest:
                return dict(applied=False,deltas=[])
            raise RuntimeError(
                "JOIN source_seq retry has a different payload")
        if source_seq!=watermark+1:
            raise RuntimeError(
                "JOIN source_seq gap/regression expected=%d got=%d"
                % (watermark+1,source_seq))
        spec=current["spec"]

        affected=set()
        changed=dict(left=set(),right=set())
        for side,row in changes:
            if side not in {"left","right"}:
                raise ValueError("JOIN change side must be left or right")
            if "_sync_op" not in row:
                raise ValueError("JOIN change lacks _sync_op")
            op=int(row["_sync_op"])
            if op not in (0,1):
                raise ValueError(
                    "JOIN _sync_op must be 0(upsert) or 1(delete)")

            source_row=_source_row(spec,side,row)
            pk_blob=_pk_blob(spec,side,source_row)
            changed[side].add(bytes(pk_blob))

            current_row=_stored_row(
                con,state_id,side,pk_blob)
            if current_row is not None:
                old_blob=_join_blob(
                    spec,side,current_row)
                if old_blob is not None:
                    affected.add(bytes(old_blob))
            if op==0:
                new_blob=_join_blob(
                    spec,side,source_row)
                if new_blob is not None:
                    affected.add(bytes(new_blob))

        before_rows=_rows_for_keys_locked(
            con,state_id,affected,changed)

        for side,row in changes:
            if int(row["_sync_op"])==1:
                _delete_row_locked(
                    con,state_id,side,spec,row)
            else:
                _put_row_locked(
                    con,state_id,side,spec,row)

        if fault_after_rows is not None:
            fault_after_rows(source_seq)

        after_rows=_rows_for_keys_locked(
            con,state_id,affected,changed)
        before=_pairs_for_changed_rows(
            spec,before_rows,changed)
        after=_pairs_for_changed_rows(
            spec,after_rows,changed)

        deltas=[]
        for pair in sorted(set(before)|set(after)):
            old=before.get(pair)
            new=after.get(pair)
            if new is None:
                deltas.append(dict(
                    pair_id=pair,op=1,row=old))
            elif old is None or old!=new:
                deltas.append(dict(
                    pair_id=pair,op=0,row=new))

        con.execute("""
            UPDATE join_states
            SET watermark=?,last_digest=?,updated=?
            WHERE state_id=?
        """,(source_seq,digest,time.time(),state_id))
    return dict(applied=True,deltas=deltas)

def iter_pairs(con,state_id):
    """Stream pair projections with indexed equality probes and bounded rows.

    Callers that write from this cursor must hold the existing state transaction
    for the full iteration. This does not invent a fixed-W view of mutable state.
    Pair order is unspecified; outbox digest order comes from its durable PK.
    """
    current=state_info(con,state_id)
    if not current["bootstrap_complete"]:
        raise RuntimeError("cannot read incomplete JOIN bootstrap state")
    cursor=con.execute("""
        SELECT l.pk_blob,l.row_payload,r.pk_blob,r.row_payload
        FROM join_rows AS l
        CROSS JOIN join_rows AS r INDEXED BY join_rows_by_key
        WHERE l.state_id=? AND l.side='left' AND l.join_blob IS NOT NULL
          AND r.state_id=l.state_id AND r.side='right'
          AND r.join_blob=l.join_blob
    """,(str(state_id),))
    try:
        for left_pk,left_payload,right_pk,right_payload in cursor:
            yield dict(
                pair_id=_pair_id(left_pk,right_pk),
                row=_project(current["spec"],pickle.loads(left_payload),
                             pickle.loads(right_payload)))
    finally:
        cursor.close()


def read_pairs(con,state_id):
    current=state_info(con,state_id)
    if not current["bootstrap_complete"]:
        raise RuntimeError(
            "cannot read incomplete JOIN bootstrap state")
    rows=con.execute("""
        SELECT DISTINCT join_blob FROM join_rows
        WHERE state_id=? AND join_blob IS NOT NULL
        ORDER BY join_blob
    """,(str(state_id),)).fetchall()
    pairs=_pairs_for_keys_locked(
        con,state_id,current["spec"],
        [bytes(row[0]) for row in rows])
    return [
        dict(pair_id=pair,row=pairs[pair])
        for pair in sorted(pairs)
    ]


def read_rows(con,state_id):
    return [item["row"] for item in read_pairs(con,state_id)]
