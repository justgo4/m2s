#!/usr/bin/env python3
from pathlib import Path
import os
import tempfile
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import aggregate_ir
import aggregate_target_mapping
import aggregate_task_catalog
import aggregate_task_runner
import j4
import join_ir
import join_target_mapping
import join_task_catalog
import stateful_share_policy


AGG_SIG=[
    ("id","bigint","bigint","NO",None,""),
    ("category","varchar","varchar(32)","NO","utf8mb4_bin",""),
    ("amount","bigint","bigint","YES",None,""),
]
LEFT_SIG=[
    ("id","bigint","bigint","NO",None,""),
    ("customer_id","bigint","bigint","YES",None,""),
    ("amount","bigint","bigint","YES",None,""),
]
RIGHT_SIG=[
    ("id","bigint","bigint","NO",None,""),
    ("name","varchar","varchar(64)","YES","utf8mb4_bin",""),
]


def agg_ir(outputs):
    select=["category"]
    if "n" in outputs:
        select.append("COUNT(*) AS n")
    if "total" in outputs:
        select.append("SUM(amount) AS total")
    return aggregate_ir.compile_sql(
        "db.orders",AGG_SIG,
        "SELECT "+",".join(select)
        +" FROM arrow_batch GROUP BY category")


def agg_schema(ir):
    result=[
        dict(
            name="category",type="VARCHAR(32)",
            nullable=False,key=True),
    ]
    for item in ir["aggregates"]:
        result.append(dict(
            name=item["output"],type="BIGINT",
            nullable=False,key=False))
    return result


def register_agg(con,task_id,sink,version,ir):
    return aggregate_task_catalog.register_task(
        con,task_id,sink,version,ir,
        sink.split(".",1)[-1],
        task_id+"-state",task_id+"-consumer",
        agg_schema(ir))


def join_plan(outputs):
    columns=[]
    if "order_id" in outputs:
        columns.append("l.id AS order_id")
    if "customer_name" in outputs:
        columns.append("r.name AS customer_name")
    if "amount" in outputs:
        columns.append("l.amount AS amount")
    return join_ir.compile_sql(
        "db.orders",LEFT_SIG,"id",
        "db.customers",RIGHT_SIG,"id",
        "SELECT "+",".join(columns)
        +" FROM left_batch l INNER JOIN right_batch r "
        "ON l.customer_id=r.id")


def join_schema(ir):
    result=[
        dict(
            name=join_target_mapping.PAIR_COLUMN,
            type="VARCHAR(1024)",nullable=False,key=True),
    ]
    for item in ir["projections"]:
        result.append(dict(
            name=item["output"],
            type=(
                "VARCHAR(64)"
                if item["column"]=="name"
                else "BIGINT"
            ),
            nullable=True,key=False))
    return result


def register_join(con,task_id,sink,version,ir):
    return join_task_catalog.register_task(
        con,task_id,sink,version,ir,
        sink.split(".",1)[-1],
        task_id+"-state",task_id+"-consumer",
        join_schema(ir))


def main():
    with tempfile.TemporaryDirectory(
        prefix="m2s-share-graph-"
    ) as td:
        con=j4.init_state(
            os.path.join(td,"state.sqlite3"))

        wide=register_agg(
            con,"agg-wide","starrocks.agg_wide",1,
            agg_ir(["n","total"]))
        narrow=register_agg(
            con,"agg-narrow","starrocks.agg_narrow",2,
            agg_ir(["n"]))
        narrow2=register_agg(
            con,"agg-narrow-2","starrocks.agg_narrow_2",3,
            agg_ir(["n"]))

        join_wide=register_join(
            con,"join-wide","starrocks.join_wide",4,
            join_plan(["order_id","customer_name","amount"]))
        join_narrow=register_join(
            con,"join-narrow","starrocks.join_narrow",5,
            join_plan(["customer_name","amount"]))

        compiled=[
            dict(kind="aggregate",task=wide),
            dict(kind="aggregate",task=narrow),
            dict(kind="aggregate",task=narrow2),
            dict(kind="inner_join",task=join_wide),
            dict(kind="inner_join",task=join_narrow),
        ]
        planned=stateful_share_policy.plan_graph(
            con,compiled,
            cfg=dict(stateful_share_mode="compatible"))
        assert len(planned)==3
        assert stateful_share_policy.preference_info(
            con,"agg-narrow"
        )["preferred_leader_task_id"]=="agg-wide"
        assert stateful_share_policy.preference_info(
            con,"agg-narrow-2"
        )["preferred_leader_task_id"]=="agg-wide"
        join_pref=stateful_share_policy.preference_info(
            con,"join-narrow")
        assert join_pref["preferred_leader_task_id"]=="join-wide"
        assert join_pref["reuse_mode"]=="subview"

        # A follower must not allocate private state merely because the chosen
        # owner thread has not finished bootstrap yet.
        mapping=aggregate_target_mapping.mapping_from_descriptor(
            narrow)
        waiting=aggregate_task_runner.step(
            con,narrow["task_id"],
            dict(stateful_share_mode="compatible"),
            mapping=mapping)
        assert waiting["phase"]=="waiting_shared_leader"
        assert waiting["waiting_shared_leader"]
        assert waiting["preferred_leader_task_id"]=="agg-wide"
        try:
            con.execute(
                "SELECT 1 FROM aggregate_states WHERE state_id=?",
                (narrow["state_id"],)
            ).fetchone()
            assert con.execute(
                "SELECT COUNT(*) FROM aggregate_states "
                "WHERE state_id=?",
                (narrow["state_id"],)
            ).fetchone()[0]==0
        except Exception:
            raise

        # Disabling sharing clears candidate preferences on the next graph
        # planning pass and restores independent bootstrap behavior.
        assert stateful_share_policy.plan_graph(
            con,compiled,
            cfg=dict(stateful_share_mode="off"))==[]
        try:
            stateful_share_policy.preference_info(
                con,"agg-narrow")
            raise AssertionError(
                "sharing-off graph retained candidate preference")
        except KeyError:
            pass

        con.close()

    print(
        "stateful_share_graph_test ok aggregate_superset "
        "join_projection whole_graph_no_chain wait_before_private "
        "sharing_off",
        flush=True,
    )


if __name__=="__main__":
    main()
