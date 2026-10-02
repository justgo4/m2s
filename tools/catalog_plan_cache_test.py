#!/usr/bin/env python3
from pathlib import Path
import sys
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import j4


def main():
    assert j4.catalog_plan_hash(
        {},dict(version=7,plan_hash="publish-hash"),
        dict(plan_hash="loaded-hash")
    )=="publish-hash"

    publish=dict(version=9)
    plan=dict(version=9,plan_hash="durable-plan-hash")
    cache_key=j4.catalog_plan_hash({},publish,plan)
    with patch.object(
        j4,"_catalog_plan_payload",
        return_value=plan
    ) as load:
        install_key=j4.catalog_plan_hash({},publish)
    assert cache_key=="durable-plan-hash"
    assert install_key==cache_key
    load.assert_called_once_with({},publish)

    try:
        j4.catalog_plan_hash(
            {},dict(version=11),dict(version=11))
    except RuntimeError as exc:
        assert "version 11" in str(exc)
    else:
        raise AssertionError("missing durable plan hash must fail closed")

    print(
        "catalog_plan_cache_test ok publish_hash durable_requeue fail_closed",
        flush=True,
    )


if __name__=="__main__":
    main()
