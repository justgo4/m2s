#!/usr/bin/env python3
"""Fixed-W two-relation bootstrap for the first INNER JOIN candidate.

One source_state pin covers both source relations at the same watermark W.
Each side scans the pinned relation independently with its own durable cursor;
JOIN output becomes readable only after both sides are complete.
"""
import join_ir
import join_state
import source_state


def _text(value,name):
    value=str(value or "").strip()
    if not value:
        raise ValueError(name+" must be non-empty")
    return value


def _relations(ir):
    join_ir.validate_ir(ir)
    return {
        side:str(ir["sources"][side]["relation"])
        for side in ("left","right")
    }


def ensure_build(con,state_id,ir,pin_id):
    join_ir.validate_ir(ir)
    state_id=_text(state_id,"state_id")
    pin_id=_text(pin_id,"pin_id")
    fixed_w=source_state.pin_watermark(con,pin_id)
    relations=_relations(ir)
    for relation in relations.values():
        info=source_state.relation_info(con,relation)
        if (
            info["complete_seq"] is None
            or int(info["complete_seq"])>int(fixed_w)
        ):
            raise RuntimeError(
                "JOIN fixed-W predates complete source relation "+relation)
    state=join_state.begin_bootstrap(
        con,state_id,join_ir.state_spec(ir),fixed_w)
    return state


def _next_side(state):
    if not state["left_complete"]:
        return "left"
    if not state["right_complete"]:
        return "right"
    return None


def process_next_chunk(
        con,state_id,ir,pin_id,limit=1000,
        fault_after_rows=None
):
    join_ir.validate_ir(ir)
    state_id=_text(state_id,"state_id")
    pin_id=_text(pin_id,"pin_id")
    limit=max(1,int(limit))
    current=ensure_build(
        con,state_id,ir,pin_id)
    fixed_w=source_state.pin_watermark(con,pin_id)
    if int(current["watermark"])!=int(fixed_w):
        raise RuntimeError(
            "JOIN bootstrap state and source pin fixed-W differ")
    if current["spec_hash"]!=join_state.semantic_id(
        join_ir.state_spec(ir)
    ):
        raise RuntimeError(
            "JOIN bootstrap IR differs from durable state")
    side=_next_side(current)
    if side is None:
        return dict(
            state_id=state_id,fixed_w=int(fixed_w),
            side=None,done=True,nrows=0,cursor=None)

    relation=ir["sources"][side]["relation"]
    cursor=current[side+"_cursor"]
    table,next_cursor=source_state.read_snapshot_batch(
        con,pin_id,relation,
        after_key=cursor,limit=limit)
    is_last=int(table.num_rows)<limit
    rows=table.to_pylist()
    join_state.apply_bootstrap_chunk(
        con,state_id,fixed_w,side,rows,
        next_cursor,is_last,
        fault_after_rows=fault_after_rows)
    updated=join_state.state_info(
        con,state_id)
    return dict(
        state_id=state_id,fixed_w=int(fixed_w),
        side=side,done=bool(updated["bootstrap_complete"]),
        nrows=int(table.num_rows),
        cursor=updated[side+"_cursor"],
    )
