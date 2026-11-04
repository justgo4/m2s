#!/usr/bin/env python3
"""Pinned aggregate cuts copied with bounded transactions.

Aggregate groups are updated in place. A pin taken at watermark W makes the
next change store the pre-change row, so a follower can keep reading that cut
after the owner has moved on. NULL row_count means the key was absent at W.
"""
import aggregate_state


def seed_owner(consumer_id):
    consumer_id=str(consumer_id or "").strip()
    if not consumer_id:
        raise ValueError("consumer_id must be non-empty")
    return "aggregate-seed:"+consumer_id


def pin(con,owner,state_id,watermark):
    owner=str(owner or "").strip()
    state_id=str(state_id or "").strip()
    watermark=int(watermark)
    if not owner or not state_id:
        raise ValueError("aggregate frozen pin owner and state must be non-empty")
    with aggregate_state.transaction(con):
        state=aggregate_state.state_info(con,state_id)
        existing=con.execute("""
            SELECT state_id,watermark,spec_hash
            FROM aggregate_frozen_pins WHERE owner=?
        """,(owner,)).fetchone()
        expected=(state_id,watermark,state["spec_hash"])
        if existing is not None:
            if (str(existing[0]),int(existing[1]),str(existing[2]))!=expected:
                raise RuntimeError("aggregate frozen pin identity changed")
        else:
            if (
                not state["bootstrap_complete"]
                or int(state["watermark"])!=watermark
            ):
                raise RuntimeError(
                    "aggregate frozen pin requires current complete cut")
            con.execute("""
                INSERT INTO aggregate_frozen_pins(
                    owner,state_id,watermark,spec_hash)
                VALUES(?,?,?,?)
            """,(owner,state_id,watermark,state["spec_hash"]))
    return expected


def info(con,owner):
    row=con.execute("""
        SELECT state_id,watermark,spec_hash
        FROM aggregate_frozen_pins WHERE owner=?
    """,(str(owner),)).fetchone()
    if row is None:
        raise RuntimeError("aggregate frozen pin is missing")
    state=aggregate_state.state_info(con,row[0])
    if (
        not state["bootstrap_complete"]
        or int(state["watermark"])<int(row[1])
        or state["spec_hash"]!=str(row[2])
    ):
        raise RuntimeError("aggregate frozen backing identity changed")
    return (str(row[0]),int(row[1]),str(row[2]))


def release(con,owner):
    with aggregate_state.transaction(con):
        con.execute(
            "DELETE FROM aggregate_frozen_pins WHERE owner=?",
            (str(owner),))


def _cut_group(con,state_id,watermark,key_blob,spec):
    old=con.execute("""
        SELECT key_payload,row_count,accum_payload
        FROM aggregate_group_before_images
        WHERE state_id=? AND key_blob=? AND change_seq>?
        ORDER BY change_seq LIMIT 1
    """,(str(state_id),bytes(key_blob),int(watermark))).fetchone()
    if old is not None:
        if old[1] is None:
            return None
        return aggregate_state._public_group(spec,old[0],old[1],old[2])
    row=con.execute("""
        SELECT key_payload,row_count,accum_payload
        FROM aggregate_groups
        WHERE state_id=? AND key_blob=?
    """,(str(state_id),bytes(key_blob))).fetchone()
    if row is None:
        return None
    return aggregate_state._public_group(spec,row[0],row[1],row[2])


def group_page_at(con,state_id,watermark,after_key=None,limit=1000):
    """Ordered keys at a pinned watermark.

    Absent keys are returned as None so the caller can resume past them.
    The second value is true when this page reached the last key.
    """
    state=aggregate_state.state_info(con,state_id)
    watermark=int(watermark)
    if (
        not state["bootstrap_complete"]
        or int(state["watermark"])<watermark
    ):
        raise RuntimeError("aggregate frozen cut is not readable")
    limit=max(1,int(limit))
    sql=(
        "SELECT key_blob FROM aggregate_groups WHERE state_id=?"
        +(" AND key_blob>?" if after_key is not None else "")
        +" UNION SELECT key_blob FROM aggregate_group_before_images"
        +" WHERE state_id=? AND change_seq>?"
        +(" AND key_blob>?" if after_key is not None else "")
        +" ORDER BY key_blob LIMIT ?"
    )
    params=[str(state_id)]
    if after_key is not None:
        params.append(bytes(after_key))
    params.extend([str(state_id),watermark])
    if after_key is not None:
        params.append(bytes(after_key))
    params.append(limit)
    keys=con.execute(sql,params).fetchall()
    page=[
        (bytes(key),_cut_group(con,state_id,watermark,key,state["spec"]))
        for (key,) in keys
    ]
    return page,len(keys)<limit


def gc_step(con,limit=1000):
    """Drop before-images that are at or below every remaining pin."""
    if con.execute("""
        SELECT 1 FROM sqlite_master
        WHERE type='table' AND name='aggregate_group_before_images'
    """).fetchone() is None:
        return 0
    if not con.execute(
        "SELECT 1 FROM aggregate_group_before_images LIMIT 1"
    ).fetchone():
        return 0
    limit=max(1,min(int(limit),1000))
    with aggregate_state.transaction(con):
        rows=con.execute("""
            SELECT b.state_id,b.key_blob,b.change_seq,
                   coalesce(length(b.key_payload),0)
                   +coalesce(length(b.accum_payload),0)
                   +length(b.key_blob)
            FROM aggregate_group_before_images b
            JOIN aggregate_states s ON s.state_id=b.state_id
            LEFT JOIN (
                SELECT state_id,MIN(watermark) AS floor
                FROM aggregate_frozen_pins GROUP BY state_id
            ) p ON p.state_id=b.state_id
            WHERE b.change_seq<=coalesce(p.floor,s.watermark)
            ORDER BY b.state_id,b.change_seq,b.key_blob
            LIMIT ?
        """,(limit,)).fetchall()
        count=0
        size=0
        for state_id,key_blob,change_seq,nbytes in rows:
            if size and size+int(nbytes)>16*1024**2:
                break
            con.execute("""
                DELETE FROM aggregate_group_before_images
                WHERE state_id=? AND key_blob=? AND change_seq=?
            """,(state_id,key_blob,change_seq))
            size+=int(nbytes)
            count+=1
        return count
