#!/usr/bin/env python3
"""Independent original/bulk seed comparison; synthetic full-output evidence."""
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

sys.path[:0]=[str(Path(__file__).resolve().parents[1]),str(Path(__file__).resolve().parent)]
import join_bootstrap_stream_test as fixture
import join_affected_reads_test
import join_outbox
import join_state

REFERENCE = "def _seed_bootstrap_stream(con,consumer_id,fixed_w,state_id,spec_hash,rows):\n    \"\"\"Atomically seed from an iterator; never cache/sort the complete relation.\n\n    The durable output-row PK provides canonical digest order. Existing commits\n    validate every source pair/payload and count before accepting an exact retry.\n    Total write-lock duration remains proportional to output cardinality.\n    \"\"\"\n    with transaction(con):\n        state=join_state.state_info(con,state_id)\n        if (not state[\"bootstrap_complete\"] or int(state[\"watermark\"])!=int(fixed_w)\n                or state[\"spec_hash\"]!=spec_hash):\n            rows.close()\n            raise RuntimeError(\"JOIN bootstrap state changed before output seed\")\n        existing=con.execute(\"\"\"\n            SELECT kind,nrows,digest,sealed FROM join_output_commits\n            WHERE consumer_id=? AND source_seq=?\n        \"\"\",(consumer_id,int(fixed_w))).fetchone()\n        if existing is not None and str(existing[0])!=\"bootstrap\":\n            raise RuntimeError(\"JOIN output commit retry has different payload\")\n        if existing is not None and not int(existing[3]):\n            raise RuntimeError(\"cannot replace unsealed JOIN output\")\n        now=time.time()\n        if existing is None:\n            con.execute(\"\"\"\n                INSERT INTO join_output_commits(\n                    consumer_id,source_seq,kind,nrows,digest,visible,created,updated)\n                VALUES(?,?,'bootstrap',0,'',0,?,?)\n            \"\"\",(consumer_id,int(fixed_w),now,now))\n        count=0\n        try:\n            for pair_id,op,payload in rows:\n                _register_pair_identity_locked(con,consumer_id,pair_id)\n                if existing is None:\n                    con.execute(\"\"\"\n                        INSERT INTO join_output_rows(\n                            consumer_id,source_seq,pair_id,op,row_payload)\n                        VALUES(?,?,?,?,?)\n                    \"\"\",(consumer_id,int(fixed_w),bytes(pair_id),int(op),bytes(payload)))\n                else:\n                    stored=con.execute(\"\"\"\n                        SELECT op,row_payload FROM join_output_rows\n                        WHERE consumer_id=? AND source_seq=? AND pair_id=?\n                    \"\"\",(consumer_id,int(fixed_w),bytes(pair_id))).fetchone()\n                    if stored is None or int(stored[0])!=int(op) or bytes(stored[1])!=bytes(payload):\n                        raise RuntimeError(\"JOIN output commit retry has different payload\")\n                count+=1\n        finally:\n            rows.close()\n        ordered=con.execute(\"\"\"\n            SELECT pair_id,op,row_payload FROM join_output_rows\n            WHERE consumer_id=? AND source_seq=? ORDER BY pair_id\n        \"\"\",(consumer_id,int(fixed_w)))\n        try:\n            digest=_digest(\"bootstrap\",ordered)\n        finally:\n            ordered.close()\n        if existing is not None:\n            if int(existing[1])!=count or str(existing[2])!=digest:\n                raise RuntimeError(\"JOIN output commit retry has different payload\")\n        else:\n            con.execute(\"\"\"\n                UPDATE join_output_commits SET nrows=?,digest=?,updated=?\n                WHERE consumer_id=? AND source_seq=?\n            \"\"\",(count,digest,time.time(),consumer_id,int(fixed_w)))\n    return commit_info(con,consumer_id,fixed_w)\n"


def bag(rows):
    count=total=0
    for row in rows:
        total=(total+int.from_bytes(hashlib.sha256(
            json.dumps(row,separators=(",",":")).encode()).digest(),"big"))%(1<<256)
        count+=1
    return dict(count=count,sha256_sum="%064x" % total)


def worker(rows,variant):
    if variant=="original":
        exec(REFERENCE,join_outbox.__dict__)
    with tempfile.TemporaryDirectory() as td:
        con=fixture.open_db(str(Path(td)/"state.sqlite3"))
        con.execute("PRAGMA temp_store=FILE")
        join_state.begin_bootstrap(con,"state",join_affected_reads_test.spec(),0)
        for offset in range(0,rows,10000):
            end=min(rows,offset+10000)
            join_state.apply_bootstrap_chunk(con,"state",0,"left",
                [dict(id=i,k=i%1024,v=i) for i in range(offset,end)],str(end).encode(),end==rows)
        join_state.apply_bootstrap_chunk(con,"state",0,"right",
            [dict(id=i,k=i,label="dim-"+str(i)) for i in range(1024)],b"end",True)
        join_outbox.ensure_stream(con,"consumer","state",1,"generation",0)
        counts=dict(sql_statements=0,stream_selects=0)
        def count(sql):
            counts["sql_statements"]+=1
            if sql.lstrip().upper().startswith("SELECT") and "FROM join_output_streams" in sql:
                counts["stream_selects"]+=1
        con.set_trace_callback(count)
        wall=time.monotonic()
        cpu=time.process_time()
        result=join_outbox.seed_bootstrap(con,"consumer","state",1,"generation",0)
        wall=time.monotonic()-wall
        cpu=time.process_time()-cpu
        con.set_trace_callback(None)
        actual=bag([row["v"],row["label"]] for row in (
            pickle.loads(payload) for payload, in con.execute(
                "SELECT row_payload FROM join_output_rows WHERE consumer_id='consumer'")))
        oracle=bag([i,"dim-"+str(i%1024)] for i in range(rows))
        identities=0
        for pair,target in con.execute("SELECT pair_id,target_id FROM join_output_identities"):
            if hashlib.sha256(pair).hexdigest()!=target:
                raise AssertionError("target identity is not exact")
            identities+=1
        if actual!=oracle or identities!=rows or result["nrows"]!=rows:
            raise AssertionError("complete output/identity oracle differs")
        con.close()
        return dict(variant=variant,rows=rows,wall_seconds=wall,cpu_seconds=cpu,
                    peak_process_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,
                    counts=counts,digest=result["digest"],full_bag=actual,
                    full_bag_oracle=oracle,identity_rows=identities,exact=True,
                    scope="synthetic FULL WAL atomic seed; includes SQL trace counter overhead; no remote/SLO",
                    write_transaction_bounded=False)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows",type=int,nargs="+",default=[100000])
    parser.add_argument("--worker",choices=["original","bulk"])
    parser.add_argument("--output",type=Path)
    args=parser.parse_args()
    if min(args.rows)<1:
        parser.error("rows must be positive")
    if args.worker:
        print(json.dumps(worker(args.rows[0],args.worker)),flush=True)
        return
    runs=[]
    for rows in args.rows:
        pair=[]
        for variant in ["original","bulk"]:
            p=subprocess.run([sys.executable,__file__,"--worker",variant,"--rows",str(rows)],
                             capture_output=True,text=True,check=True)
            pair.append(json.loads(p.stdout))
        if len({r["digest"] for r in pair})!=1 or pair[0]["full_bag"]!=pair[1]["full_bag"]:
            raise AssertionError("independent original/bulk full output differs")
        runs.extend(pair)
    output=json.dumps(dict(kind="join_bootstrap_bulk_local_v1",runs=runs,exact=True,
                           not_certification=True,software=dict(python=sys.version,sqlite=sqlite3.sqlite_version)),indent=2)+"\n"
    if args.output:
        args.output.parent.mkdir(parents=True,exist_ok=True)
        args.output.write_text(output)
    print(output,flush=True)


if __name__=="__main__":
    main()
