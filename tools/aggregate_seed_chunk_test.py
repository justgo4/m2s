#!/usr/bin/env python3
"""Bounded aggregate bootstrap seeding resumes and matches one full digest."""
import sqlite3
import sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import aggregate_frozen
import aggregate_outbox
import aggregate_state


def groups(n):
    return [
        dict(category="g%04d" % index,amount=index,_sync_op=0)
        for index in range(n)
    ]


def build(path,n):
    con=sqlite3.connect(path,isolation_level=None)
    con.execute("PRAGMA foreign_keys=ON")
    aggregate_state.install(con)
    spec=aggregate_state.aggregate_spec(
        ["category"],
        [dict(output="n",function="count",input="*")],
    )
    aggregate_state.create_state(con,"agg",spec,watermark=0)
    con.execute("UPDATE aggregate_states SET bootstrap_complete=0 WHERE state_id='agg'")
    aggregate_state.apply_bootstrap_chunk(
        con,"agg",0,groups(n),None,True)
    aggregate_outbox.install(con)
    return con


def main():
    import tempfile
    with tempfile.TemporaryDirectory(prefix="m2s-agg-seed-") as td:
        full=build(str(Path(td)/"full.sqlite3"),5)
        one=aggregate_outbox.seed_bootstrap(full,"c","agg",1,"g",0)
        chunked=build(str(Path(td)/"chunk.sqlite3"),5)
        begins=[]
        chunked.set_trace_callback(
            lambda sql: begins.append(sql)
            if sql.upper().startswith("BEGIN IMMEDIATE") else None)
        partial=aggregate_outbox.seed_chunk(
            chunked,"c","agg",1,"g",0,limit=2)
        assert not partial["done"]
        assert partial["nrows"]==2
        assert aggregate_outbox.pending_commits(chunked,"c")==[]
        try:
            aggregate_outbox.mark_visible(chunked,"c",0)
            raise AssertionError("unsealed aggregate bootstrap became visible")
        except RuntimeError as exc:
            assert "still being seeded" in str(exc)
        done=aggregate_outbox.seed_bootstrap(
            chunked,"c","agg",1,"g",0,limit=2)
        assert done["digest"]==one["digest"]
        assert done["nrows"]==one["nrows"]==5
        assert begins
        assert aggregate_outbox.bootstrap_sealed(chunked,"c",0)
        assert len(aggregate_outbox.pending_commits(chunked,"c"))==1
        assert [
            row["row"]["category"]
            for row in aggregate_outbox.commit_rows(chunked,"c",0)
        ]==[
            row["category"]
            for row in aggregate_state.read_rows(chunked,"agg")
        ]
        full.close()
        chunked.close()

        baseline=build(str(Path(td)/"baseline.sqlite3"),5)
        original=aggregate_outbox.seed_bootstrap(
            baseline,"c","agg",1,"g",0)
        moving=build(str(Path(td)/"moving.sqlite3"),5)
        partial=aggregate_outbox.seed_chunk(
            moving,"c","agg",1,"g",0,limit=2)
        assert not partial["done"] and partial["nrows"]==2
        assert aggregate_state.apply_transaction(moving,"agg",1,[
            dict(category="g0000",_sync_op=0),
            dict(category="g9999",_sync_op=0),
        ])
        assert aggregate_state.state_info(moving,"agg")["watermark"]==1
        assert moving.execute(
            "SELECT COUNT(*) FROM aggregate_group_before_images"
        ).fetchone()[0]==2
        resumed=aggregate_outbox.seed_bootstrap(
            moving,"c","agg",1,"g",0,limit=2)
        assert resumed["digest"]==original["digest"]
        assert resumed["nrows"]==5
        assert all(
            row["row"]["category"]!="g9999"
            for row in aggregate_outbox.commit_rows(moving,"c",0)
        )
        assert moving.execute(
            "SELECT COUNT(*) FROM aggregate_frozen_pins"
        ).fetchone()[0]==0
        while aggregate_frozen.gc_step(moving):
            pass
        assert moving.execute(
            "SELECT COUNT(*) FROM aggregate_group_before_images"
        ).fetchone()[0]==0
        baseline.close()
        moving.close()
    print("aggregate_seed_chunk_test ok bounded_resume digest_match hidden_until_sealed cut_survives_owner_apply",flush=True)


if __name__=="__main__":
    main()
