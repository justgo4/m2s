#!/usr/bin/env python3
"""Bounded exact-payload recovery for uncertain Merge Commit responses."""
import hashlib
from pathlib import Path
import sys
import tempfile
import threading
import time
from unittest.mock import patch

import pycurl

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import j4


def mapping(extra_target=False):
    target_schema={
        "id":dict(type="BIGINT"),
        "value":dict(type="BIGINT"),
    }
    if extra_target:
        target_schema["server_default"]=dict(type="DATETIME")
    return dict(
        src_table="events",
        sr_table="events_target",
        primary_key="id",
        _output_columns=["id","value"],
        _size_columns={},
        _target_sequence=False,
        _target_schema=target_schema,
    )


def cfg(mode="idempotent"):
    return dict(
        merge_uncertain_recovery=mode,
        merge_uncertain_replay_max=3,
        merge_uncertain_replay_backoff_seconds=1,
        load_timeout=30,
        merge_commit_interval_ms=10,
        merge_commit_parallel=2,
        compression="",
        sr=dict(
            host="127.0.0.1",
            http_port=8030,
            database="synthetic",
        ),
    )


def runtime(table):
    return dict(
        stop=threading.Event(),
        control_lock=threading.Lock(),
        quarantined_tables={
            table:dict(
                reason="synthetic response loss",
                parts=1,
                replay_disabled=True,
            )
        },
        load_events={table:threading.Event()},
        metrics=dict(
            lock=threading.Lock(),
            tables={
                table:dict(
                    total=j4.metric_bucket(),
                    interval=j4.metric_bucket(),
                )
            },
        ),
    )


def seed(con, item, delivery="delivery-1"):
    table=j4.mapping_key(item)
    payload=b'{"id":1,"value":7,"__op":0}\n'
    con.execute(
        "INSERT INTO deliveries("
        "id,table_name,lane,plan_version,prepared"
        ") VALUES(?,?,?,?,1)",
        (delivery,table,0,0))
    cur=con.execute(
        "INSERT INTO jobs("
        "table_name,lane,kind,payload,nrows,logical_bytes,"
        "plan_version,source_time,created"
        ") VALUES(?,?,?,?,?,?,?,?,?)",
        (
            table,0,"cdc",b"synthetic-arrow",1,15,0,
            time.time(),time.time(),
        ))
    job_id=int(cur.lastrowid)
    con.execute(
        "INSERT INTO job_assignments(job_id,delivery_id) VALUES(?,?)",
        (job_id,delivery))
    con.execute(
        "INSERT INTO load_parts("
        "delivery_id,part,label,payload,nrows,json_bytes"
        ") VALUES(?,?,?,?,?,?)",
        (
            delivery,0,"local-request-id",payload,1,len(payload),
        ))
    j4.begin_merge_request(
        con,item,delivery,0,"local-request-id",payload)
    return dict(
        table=table,
        delivery=delivery,
        payload=payload,
        sha256=hashlib.sha256(payload).hexdigest(),
    )


def candidate_contract():
    with tempfile.TemporaryDirectory(
            prefix="m2s-merge-reconcile-candidate-") as directory:
        path=str(Path(directory)/"state.sqlite3")
        con=j4.init_state(path)
        try:
            item=mapping()
            seeded=seed(con,item)
            state=runtime(seeded["table"])
            with patch.object(
                    j4,"runtime_mapping",return_value=item):
                candidate,reason=j4.merge_uncertain_replay_candidate(
                    con,seeded["table"],cfg(),state)
            assert reason is None,reason
            assert candidate["payload"]==seeded["payload"]
            assert candidate["lanes"]==[0]
            assert candidate["attempts"]==0

            unsafe=mapping(extra_target=True)
            with patch.object(
                    j4,"runtime_mapping",return_value=unsafe):
                candidate,reason=j4.merge_uncertain_replay_candidate(
                    con,seeded["table"],cfg(),state)
            assert candidate is None
            assert "outside the immutable replay payload" in reason

            con.execute(
                "UPDATE merge_uncertain SET payload_sha256=?",
                ("0"*64,))
            with patch.object(
                    j4,"runtime_mapping",return_value=item):
                candidate,reason=j4.merge_uncertain_replay_candidate(
                    con,seeded["table"],cfg(),state)
            assert candidate is None
            assert "SHA-256 mismatch" in reason

            con.execute(
                "UPDATE merge_uncertain "
                "SET payload_sha256=?,replay_attempts=3,last_replay=NULL",
                (seeded["sha256"],))
            with patch.object(
                    j4,"runtime_mapping",return_value=item):
                candidate,reason=j4.merge_uncertain_replay_candidate(
                    con,seeded["table"],cfg(),state)
            assert candidate is None
            assert "budget exhausted" in reason

            con.execute(
                "UPDATE merge_uncertain "
                "SET replay_attempts=0,last_replay=NULL")
            with patch.object(
                    j4,"runtime_mapping",return_value=item):
                candidate,reason=j4.merge_uncertain_replay_candidate(
                    con,seeded["table"],cfg("off"),state)
            assert candidate is None
            assert "disabled" in reason
        finally:
            con.close()


def visible_reconcile_contract():
    with tempfile.TemporaryDirectory(
            prefix="m2s-merge-reconcile-visible-") as directory:
        path=str(Path(directory)/"state.sqlite3")
        con=j4.init_state(path)
        try:
            item=mapping()
            seeded=seed(con,item)
            state=runtime(seeded["table"])
            response=dict(
                Status="Success",
                TxnId=777,
                Label="merge_commit_server_label",
                LeftMergeTimeMs=0,
            )
            with (
                patch.object(
                    j4,"runtime_mapping",return_value=item),
                patch.object(
                    j4,"curl_request",
                    return_value=(200,response)),
                patch.object(
                    j4,"wait_visible",
                    return_value=("visible",dict())),
                patch.object(
                    j4,"metric_add_merge",
                    return_value=None),
            ):
                assert j4.reconcile_merge_quarantine(
                    con,object(),seeded["table"],cfg(),state)
            row=con.execute(
                "SELECT visible,txn_id FROM load_parts "
                "WHERE delivery_id=? AND part=0",
                (seeded["delivery"],)).fetchone()
            assert row==(1,777),row
            assert con.execute(
                "SELECT COUNT(*) FROM merge_uncertain"
            ).fetchone()[0]==0
            assert not j4.merge_table_quarantined(
                state,seeded["table"])
            assert state["load_events"][seeded["table"]].is_set()
            metrics=j4.metric_bucket_summary(
                state["metrics"]["tables"][
                    seeded["table"]]["total"],
                include_quantiles=False)
            assert metrics["merge_uncertain_replays"]==1,metrics
            assert (
                metrics["merge_uncertain_replay_visible"]
                ==1
            ),metrics
            assert (
                metrics["merge_uncertain_replay_aborted"]
                ==0
            ),metrics
            assert (
                metrics["merge_uncertain_replay_blocked"]
                ==0
            ),metrics
        finally:
            con.close()


def pre_send_failure_contract():
    with tempfile.TemporaryDirectory(
            prefix="m2s-merge-reconcile-presend-") as directory:
        path=str(Path(directory)/"state.sqlite3")
        con=j4.init_state(path)
        try:
            item=mapping()
            seeded=seed(con,item)
            state=runtime(seeded["table"])
            error=pycurl.error(
                pycurl.E_COULDNT_CONNECT,
                "synthetic connect failure")
            with (
                patch.object(
                    j4,"runtime_mapping",return_value=item),
                patch.object(
                    j4,"curl_request",side_effect=error),
                patch.object(
                    j4,"metric_add_merge",
                    return_value=None),
            ):
                assert not j4.reconcile_merge_quarantine(
                    con,object(),seeded["table"],cfg(),state)
            row=con.execute(
                "SELECT replay_attempts,last_replay,reason "
                "FROM merge_uncertain"
            ).fetchone()
            assert row[0]==0,row
            assert row[1] is None,row
            assert row[2]=="automatic_replay_pre_send_failure",row
            assert j4.merge_table_quarantined(
                state,seeded["table"])
            part=con.execute(
                "SELECT visible,txn_id FROM load_parts"
            ).fetchone()
            assert part==(0,None),part
            metrics=j4.metric_bucket_summary(
                state["metrics"]["tables"][
                    seeded["table"]]["total"],
                include_quantiles=False)
            assert metrics["merge_uncertain_replays"]==1,metrics
            assert (
                metrics["merge_uncertain_replay_visible"]
                ==0
            ),metrics
        finally:
            con.close()


def main():
    candidate_contract()
    visible_reconcile_contract()
    pre_send_failure_contract()
    print(
        "MERGE RECONCILE PASS closure sha fifo budget "
        "visible_release pre_send_budget_restore",
        flush=True,
    )


if __name__=="__main__":
    main()
