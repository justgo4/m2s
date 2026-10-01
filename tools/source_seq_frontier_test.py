#!/usr/bin/env python3
from pathlib import Path
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import j4


def add_delivery(con, delivery, source_seq, source_pos, lane=3):
    now = time.time()
    with j4.state_transaction(con):
        cur = con.execute("""
            INSERT INTO jobs(
                table_name,lane,kind,payload,nrows,logical_bytes,plan_version,
                source_file,source_pos,source_seq,source_time,created)
            VALUES('sink',?,'cdc',X'00',1,1,0,'binlog.000001',?,?,?,?)
        """, (lane,source_pos,source_seq,now,now))
        job_id = int(cur.lastrowid)
        con.execute("""
            INSERT INTO deliveries(
                id,table_name,lane,plan_version,prepared)
            VALUES(?, 'sink', ?, 0, 1)
        """, (delivery,lane))
        con.execute("""
            INSERT INTO job_assignments(job_id,delivery_id) VALUES(?,?)
        """, (job_id,delivery))
    return job_id


def main():
    with tempfile.TemporaryDirectory(prefix="m2s-source-seq-") as td:
        path = str(Path(td) / "state.sqlite3")
        con = j4.init_state(path)

        jobs = {row[1] for row in con.execute("PRAGMA table_info(jobs)")}
        applied = {row[1] for row in con.execute("PRAGMA table_info(applied)")}
        assert "source_seq" in jobs
        assert "source_seq" in applied

        add_delivery(con, "d10", 10, 100)
        j4.acknowledge_delivery(con, "d10")
        assert j4.visible_frontiers(con, "sink") == [
            dict(
                lane=3, source_file="binlog.000001",
                source_pos=100, source_seq=10)
        ]

        add_delivery(con, "d11", 11, 120)
        j4.acknowledge_delivery(con, "d11")
        assert j4.visible_frontiers(con, "sink")[0]["source_seq"] == 11

        # A corrupted/out-of-order delivery cannot move a lane's visible
        # source sequence backwards. The acknowledgement transaction rolls back.
        job_id = add_delivery(con, "d09", 9, 90)
        try:
            j4.acknowledge_delivery(con, "d09")
            raise AssertionError("visible source sequence regression was accepted")
        except RuntimeError as exc:
            assert "visible source sequence regression" in str(exc)
        assert j4.visible_frontiers(con, "sink")[0]["source_seq"] == 11
        assert con.execute(
            "SELECT 1 FROM active_jobs WHERE id=?", (job_id,)
        ).fetchone()
        with j4.state_transaction(con):
            con.execute("DELETE FROM deliveries WHERE id='d09'")
            con.execute("DELETE FROM jobs WHERE id=?", (job_id,))

        # Legacy jobs have no source_seq. They may advance file/pos but must not
        # erase a previously established internal sequence frontier.
        add_delivery(con, "legacy", None, 140)
        j4.acknowledge_delivery(con, "legacy")
        frontier = j4.visible_frontiers(con, "sink")[0]
        assert frontier["source_pos"] == 140
        assert frontier["source_seq"] == 11
        con.close()

        # In-place open keeps the added columns/frontier durable.
        con = j4.init_state(path)
        assert j4.visible_frontiers(con, "sink")[0]["source_seq"] == 11
        con.close()

    print("source_seq_frontier_test ok", flush=True)


if __name__ == "__main__":
    main()
