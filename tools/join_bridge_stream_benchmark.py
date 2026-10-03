#!/usr/bin/env python3
"""Isolated cached/streamed JOIN bridge A/B; synthetic journal, no SLO claim."""
import argparse
import hashlib
import json
from pathlib import Path
import pickle
import resource
import sqlite3
import subprocess
import sys
import tempfile
import time

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT),str(ROOT/"tools")]
import j4
import join_job_bridge
import join_job_bridge_test as fixture
import join_outbox
import join_state

# Exact original functions from cb153909; independent cache baseline.
CACHE_REFERENCE = '''def _mutations(con,consumer_id,source_seq,mapping):
    validate_mapping(mapping)
    result=[]
    for item in join_outbox.commit_rows(
        con,consumer_id,source_seq
    ):
        row=dict(item["row"])
        row[PAIR_COLUMN]=join_outbox.target_id_for_pair(
            con,consumer_id,item["pair_id"])
        result.append((int(item["op"]),row))
    return result

def stage_commit(
        con,consumer_id,source_seq,mapping,cfg,engine=None
):
    consumer_id=_text(consumer_id,"consumer_id")
    source_seq=int(source_seq)
    validate_mapping(mapping)
    commit=join_outbox.commit_info(
        con,consumer_id,source_seq)
    if commit["visible"]:
        return dict(
            consumer_id=consumer_id,
            source_seq=source_seq,
            visible=True,job_ids=[])

    existing=_already_staged(
        con,consumer_id,source_seq)
    if existing:
        return dict(
            consumer_id=consumer_id,
            source_seq=source_seq,
            visible=False,job_ids=existing)

    mutations=_mutations(
        con,consumer_id,source_seq,mapping)
    if not mutations:
        join_outbox.mark_visible(
            con,consumer_id,source_seq)
        return dict(
            consumer_id=consumer_id,
            source_seq=source_seq,
            visible=True,job_ids=[])

    own_engine=engine is None
    engine=duckdb.connect(":memory:") if own_engine else engine
    try:
        raw=j4.raw_arrow(
            mapping,mutations)
        routed=j4.route_arrow(
            engine,mapping,raw,
            j4.key_partition_count(cfg))
        with tempfile.SpooledTemporaryFile(
            max_size=1024**2,
            dir=j4.state_temp_dir(cfg),
        ) as spool:
            j4.spool_routed(
                spool,mapping,routed,cfg)
            spool.seek(0)
            records=[]
            while True:
                record=j4.read_spool_record(
                    spool)
                if record is None:
                    break
                records.append(record)
    finally:
        if own_engine:
            engine.close()

    if not records:
        raise RuntimeError(
            "JOIN output rows produced no routed durable jobs")

    stream=join_outbox.stream_info(
        con,consumer_id)
    table=j4.mapping_key(mapping)
    now=time.time()
    job_ids=[]
    logical_total=0
    with j4.state_transaction(con):
        if _already_staged(
            con,consumer_id,source_seq
        ):
            raise RuntimeError(
                "JOIN outbox commit was staged concurrently")
        for record in records:
            (
                record_table,lane,payload,
                nrows,logical_bytes
            )=record
            if record_table!=table:
                raise RuntimeError(
                    "JOIN bridge routed an unexpected table")
            cur=con.execute("""
                INSERT INTO jobs(
                    table_name,lane,kind,payload,nrows,
                    logical_bytes,plan_version,
                    source_file,source_pos,source_seq,
                    source_time,created)
                VALUES(?,?,'cdc',?,?,?,?,NULL,NULL,?,?,?)
            """,(
                table,int(lane),bytes(payload),
                int(nrows),int(logical_bytes),
                stateful_task_plan.writer_plan_version(stream["plan_version"]),
                source_seq,now,now,
            ))
            job_id=int(cur.lastrowid)
            con.execute("""
                INSERT INTO join_job_links(
                    job_id,consumer_id,source_seq)
                VALUES(?,?,?)
            """,(
                job_id,consumer_id,source_seq))
            job_ids.append(job_id)
            logical_total+=int(logical_bytes)
        j4.meta_set(
            con,"pending_bytes",
            j4.meta_get(
                con,"pending_bytes",0
            )+logical_total)
    return dict(
        consumer_id=consumer_id,
        source_seq=source_seq,
        visible=False,job_ids=job_ids)
'''


def measure(mode,rows):
    with tempfile.TemporaryDirectory() as directory:
        path=str(Path(directory)/"state.sqlite3")
        con=j4.init_state(path)
        join_state.install(con)
        join_state.create_state(con,"join-state",fixture.spec(),watermark=0)
        join_outbox.ensure_stream(con,"join-consumer","join-state",11,"generation",0)
        # Stream synthetic initialization so setup cannot mask bridge RSS.
        now=time.time()
        payload=pickle.dumps(dict(customer_name="same",amount=7),protocol=5)
        with j4.state_transaction(con):
            con.execute("INSERT INTO join_output_commits(consumer_id,source_seq,kind,nrows,digest,created,updated) "
                        "VALUES('join-consumer',0,'bootstrap',?,'pending',?,?)",(rows,now,now))
            for index in range(rows):
                pair_id=("pair-%09d"%index).encode()
                join_outbox._register_pair_identity_locked(con,"join-consumer",pair_id)
                con.execute("INSERT INTO join_output_rows(consumer_id,source_seq,pair_id,op,row_payload) "
                            "VALUES('join-consumer',0,?,0,?)",(pair_id,payload))
            digest=join_outbox._digest("bootstrap",con.execute(
                "SELECT pair_id,op,row_payload FROM join_output_rows ORDER BY pair_id"))
            con.execute("UPDATE join_output_commits SET digest=?",(digest,))
        stage=join_job_bridge.stage_commit
        if mode=="cache":
            namespace=dict(vars(join_job_bridge))
            exec(compile(CACHE_REFERENCE,"<cb153909-bridge>","exec"),namespace)
            stage=namespace["stage_commit"]
        mapping=fixture.mapping()
        cfg=fixture.cfg(path)
        cfg["batch_rows"]=4096
        started=time.monotonic()
        cpu=time.process_time()
        result=stage(con,"join-consumer",0,mapping,cfg)
        wall=time.monotonic()-started
        cpu_seconds=time.process_time()-cpu
        peak=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024
        # Independently scan ALL routed rows; same bag digest despite job splits.
        modulus=1<<256
        count=total=0
        for (wire,) in con.execute("SELECT payload FROM jobs ORDER BY id"):
            for row in j4.arrow_job_table(mapping,wire).to_pylist():
                values=[row[fixture.join_job_bridge.PAIR_COLUMN],row["_sync_op"],row["customer_name"],row["amount"]]
                total=(total+int.from_bytes(hashlib.sha256(json.dumps(values,separators=(",",":")).encode()).digest(),"big"))%modulus
                count+=1
        if count!=rows:
            raise RuntimeError("full routed row count differs")
        output=dict(mode=mode,rows=rows,routed_rows=count,bag_digest="%064x"%total,wall_seconds=wall,
                    cpu_seconds=cpu_seconds,process_peak_rss_bytes=peak,jobs=len(result["job_ids"]),
                    logical_bytes=j4.meta_get(con,"pending_bytes",0),atomic_enqueue_lock_bounded=False,
                    source_sha256=hashlib.sha256((ROOT/"join_job_bridge.py").read_bytes()).hexdigest(),
                    cache_reference_sha256=hashlib.sha256(CACHE_REFERENCE.encode()).hexdigest(),
                    software=dict(python=sys.version,sqlite=sqlite3.sqlite_version))
        con.close()
        return output


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows",type=int,default=100000)
    parser.add_argument("--repeats",type=int,default=2)
    parser.add_argument("--mode",choices=["cache","stream"])
    parser.add_argument("--output",type=Path)
    args=parser.parse_args()
    if min(args.rows,args.repeats)<1:
        parser.error("rows/repeats must be positive")
    if args.mode:
        print(json.dumps(measure(args.mode,args.rows)),flush=True)
        return
    runs=[]
    for _ in range(args.repeats):
        for mode in ["cache","stream"]:
            completed=subprocess.run([sys.executable,__file__,"--mode",mode,"--rows",str(args.rows)],
                                     capture_output=True,text=True,check=True)
            runs.append(json.loads(completed.stdout.splitlines()[-1]))
    if len({x["bag_digest"] for x in runs})!=1:
        raise RuntimeError("cached/streamed full output bags differ")
    rendered=json.dumps(dict(kind="m2s_join_bridge_stream_ab_v1",runs=runs,exact_match=True,
                             scope="isolated_synthetic_join_bridge",not_certification=True),indent=2)+"\n"
    if args.output:
        args.output.parent.mkdir(parents=True,exist_ok=True)
        args.output.write_text(rendered)
    print(rendered,flush=True)


if __name__=="__main__":
    main()
