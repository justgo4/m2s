#!/usr/bin/env python3
"""Merge Commit request identity is local-only and never a remote label."""

import hashlib
from pathlib import Path
import sys
import tempfile

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import j4


def main():
    mapping=dict(
        src_table="source_events",
        sr_table="target_events",
        _output_columns=["id","value"],
        _target_sequence=False,
    )
    cfg=dict(
        load_timeout=30,
        merge_commit_interval_ms=100,
        merge_commit_parallel=4,
        compression="",
    )
    headers=j4.merge_commit_headers(mapping,cfg)
    assert "label" not in headers
    assert headers["enable_merge_commit"]=="true"
    assert headers["merge_commit_async"]=="true"

    with tempfile.TemporaryDirectory(prefix="m2s-merge-identity-") as directory:
        path=str(Path(directory)/"state.sqlite3")
        con=j4.init_state(path)
        try:
            delivery="delivery-local"
            local_request_id="local-request-id"
            payload=b'{"id":1,"value":"x","__op":0}\n'
            con.execute(
                "INSERT INTO deliveries(id,table_name,lane) VALUES(?,?,?)",
                (delivery,j4.mapping_key(mapping),0))
            con.execute(
                "INSERT INTO load_parts(delivery_id,part,label,payload,nrows) "
                "VALUES(?,?,?,?,?)",
                (delivery,0,local_request_id,payload,1))
            j4.begin_merge_request(
                con,mapping,delivery,0,local_request_id,payload)
            row=con.execute(
                "SELECT label,payload_sha256,reason FROM merge_uncertain "
                "WHERE delivery_id=? AND part=0",
                (delivery,)).fetchone()
            assert row==(
                local_request_id,
                hashlib.sha256(payload).hexdigest(),
                "request_inflight_no_txn_id",
            )
        finally:
            con.close()

    print(
        "MERGE IDENTITY PASS local request id retained but never sent as "
        "StarRocks Merge Commit label",
        flush=True,
    )


if __name__=="__main__":
    main()
