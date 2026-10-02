#!/usr/bin/env python3
from pathlib import Path
import tempfile
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import cdc_catalog


def main():
    with tempfile.TemporaryDirectory(
        prefix="m2s-catalog-preview-"
    ) as td:
        path=str(Path(td)/"catalog.sqlite3")
        cdc_catalog.execute_batch(
            path,[
                "CREATE TABLE starrocks.base AS "
                "SELECT id,v FROM mysql.events",
                "ALTER TABLE starrocks.base "
                "ADD PRIMARY KEY(id)",
            ],
            auto_publish=True)
        before=cdc_catalog.load_plan(path)
        before_variables=cdc_catalog.variables_get(path)

        preview=cdc_catalog.preview_batch(
            path,[
                "SET VARIABLE CDC_BATCH_MS = 250",
                "CREATE TABLE starrocks.rollup AS "
                "SELECT bucket,COUNT(*) AS n "
                "FROM mysql.events GROUP BY bucket",
                "PUBLISH",
            ])
        assert preview["status"]=="preview"
        assert preview["changed"]
        assert preview["is_new"]
        assert preview["plan_changed"]
        assert preview["publish_requested"]
        assert preview["statements"]==3
        assert preview["configuration_variables"]==[
            "CDC_BATCH_MS"]
        assert [
            item["sink"]
            for item in preview["stateful_tasks"]
        ]==["starrocks.rollup"]
        assert all(
            "source" not in item
            for item in preview["udfs"])

        after=cdc_catalog.load_plan(path)
        after_variables=cdc_catalog.variables_get(path)
        assert after==before
        assert after_variables==before_variables
        assert all(
            item.get("_catalog_sink")
            !="starrocks.rollup"
            for item in after["mappings"])
        assert not after["stateful_tasks"]

    print(
        "catalog_preview_test ok rollback candidate config_redaction",
        flush=True)


if __name__=="__main__":
    main()
