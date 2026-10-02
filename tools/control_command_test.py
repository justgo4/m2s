#!/usr/bin/env python3
from pathlib import Path
import sys
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import j4


def main():
    assert (
        j4.canonical_catalog_sink("orders")
        =="starrocks.orders")
    assert (
        j4.canonical_catalog_sink(
            "STARROCKS.Rollup_1")
        =="starrocks.rollup_1")
    for value in (
        "",
        "mysql.orders",
        "starrocks.bad-name",
        "starrocks.a.b",
    ):
        try:
            j4.canonical_catalog_sink(value)
            raise AssertionError(
                "invalid sink accepted: "+repr(value))
        except ValueError:
            pass

    calls=[]
    def fake_paths(
            _root,state_path=None,
            variables=None):
        return dict(
            catalog="/tmp/catalog.sqlite3",
            socket="/tmp/catalog.sock",
            seed=None,
            state="/tmp/state.sqlite3")
    def fake_shell(*args,**kwargs):
        calls.append((args,kwargs))
        return 17

    with patch.object(
        j4.cdc_catalog,
        "catalog_paths",
        side_effect=fake_paths
    ), patch.object(
        j4.cdc_catalog,
        "variables_get",
        return_value={}
    ), patch.object(
        j4.cdc_catalog,
        "shell",
        side_effect=fake_shell
    ), patch.object(
        sys,"argv",
        ["j4.py","cancel","orders"]
    ):
        assert j4.main()==17

    assert len(calls)==1
    args,kwargs=calls[0]
    assert args[:3]==(
        "/tmp/catalog.sqlite3",
        "/tmp/catalog.sock",
        None)
    assert (
        kwargs["command"]
        =="DROP TABLE starrocks.orders")
    assert (
        kwargs["publish_callback"]
        is j4.validate_local_catalog_publish)

    print(
        "control_command_test ok canonical_sink cancel_routes_through_catalog",
        flush=True)


if __name__=="__main__":
    main()
