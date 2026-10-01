#!/usr/bin/env python3
from pathlib import Path
import os
import sqlite3
import sys
import tempfile
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import cdc_catalog
import j4


def expect_error(function,error=Exception,contains=None):
    try:
        function()
    except error as exc:
        if contains is not None and contains not in str(exc):
            raise AssertionError(
                "error does not contain %r: %s" % (contains,exc))
        return
    raise AssertionError("expected "+error.__name__)


def create_stateful(path):
    cdc_catalog.execute(
        path,
        "CREATE TABLE starrocks.agg AS "
        "SELECT category, COUNT(*) AS n, SUM(amount) AS total "
        "FROM mysql.orders GROUP BY category")
    cdc_catalog.execute(
        path,
        "CREATE TABLE starrocks.joined AS "
        "SELECT l.id AS left_id, r.name AS right_name "
        "FROM mysql.lefts l INNER JOIN mysql.rights r "
        "ON l.customer_id=r.id")
    cdc_catalog.execute(
        path,
        "CREATE TABLE starrocks.plain AS "
        "SELECT id, amount FROM mysql.orders")
    cdc_catalog.execute(
        path,
        "ALTER TABLE starrocks.plain ADD PRIMARY KEY (id)")


def main():
    with tempfile.TemporaryDirectory(
        prefix="m2s-stateful-catalog-"
    ) as td:
        path=os.path.join(td,"catalog.sqlite3")
        create_stateful(path)

        con=cdc_catalog.catalog_open(path)
        try:
            mappings,tasks,macros,udfs,digest=(
                cdc_catalog.compile_draft(con))
        finally:
            con.close()
        assert len(mappings)==1
        assert mappings[0]["sr_table"]=="plain"
        assert macros==[] and udfs==[]
        assert len(digest)==64
        assert [
            (item["sink"],item["kind"],item["source_relations"])
            for item in tasks
        ]==[
            ("starrocks.agg","aggregate",["orders"]),
            ("starrocks.joined","inner_join",["lefts","rights"]),
        ]
        assert tasks[0]["target_table"]=="agg"
        assert tasks[1]["target_table"]=="joined"
        assert tasks[1]["primary_key"]==[]

        published=cdc_catalog.publish(path)
        assert published["mappings"]==mappings
        assert published["stateful_tasks"]==tasks
        loaded=cdc_catalog.load_plan(path)
        assert loaded["plan_hash"]==published["plan_hash"]
        assert loaded["stateful_tasks"]==tasks
        shown=cdc_catalog.execute(path,"SHOW PLAN")
        assert shown["plan"]["stateful_tasks"]==tasks

        # Stateful plans now validate through the same publish callback, but
        # activation is deliberately fenced to a daemon restart boundary.
        candidate=dict(
            version=2,revision=2,plan_hash="x",
            mappings=[],stateful_tasks=[tasks[0]],
            macros=[],udfs=[],_variables={})
        fake_cfg=dict(
            catalog=path,
            state=os.path.join(td,"state.sqlite3"),
            shared_source_state=True,
            catalog_macros=[],
            catalog_udfs=[],
        )
        with (
            patch.object(j4,"read_config",return_value=fake_cfg),
            patch.object(
                j4,"preflight",
                return_value=([],None,None,None,None,"fingerprint")),
            patch.object(
                j4,"validate_stateful_catalog_plan",
                return_value=[dict(kind="aggregate")])
        ):
            validated=j4.validate_local_catalog_publish(
                candidate,"validate")
            assert validated["status"]=="validated_offline"
            assert validated["stateful_tasks"]==1
            installed=j4.validate_local_catalog_publish(
                candidate,"install")
            assert installed["status"]=="restart_required"
            assert installed["stateful_tasks"]==1

        # A running daemon also creates/binds stateful targets after the
        # catalog commit, but keeps execution fenced until restart.
        runtime=dict(
            plan_lock=__import__("threading").RLock(),
            active_plan_version=1,
            catalog_activation={},
        )
        publish_result=dict(
            version=2,plan_hash="hot-stateful",
            stateful_tasks=[tasks[0]],
            validation=dict(
                status="restart_required",version=2,
                reason="stateful restart fence"),
        )
        with (
            patch.object(
                cdc_catalog,"load_plan_version",
                return_value=dict(stateful_tasks=[])),
            patch.object(
                j4,"validate_local_catalog_publish",
                return_value=dict(
                    status="restart_required",version=2,
                    stateful_tasks=1,
                    reason="created target; restart"))
        ) as mocked:
            activation=j4.catalog_publish_callback(
                fake_cfg,runtime,publish_result,"install")
            assert activation["status"]=="restart_required"
            assert runtime["catalog_activation"]["version"]==2
            mocked.assert_called_once_with(
                publish_result,"install")

        # Restart selection promotes a published stateful-only topology change
        # even when durable active_plan_version still names the prior catalog
        # version; mixed stateless topology changes remain fenced.
        restart_cfg=dict(
            catalog=path,catalog_seed=None,
            state=os.path.join(td,"restart-state.sqlite3"),
        )
        published_plan=dict(
            version=2,plan_hash="new",
            mappings=[],stateful_tasks=[tasks[0]],
            macros=[],udfs=[])
        active_plan=dict(
            version=1,plan_hash="old",
            mappings=[],stateful_tasks=[],
            macros=[],udfs=[])
        with (
            patch.object(
                cdc_catalog,"load_plan",
                return_value=published_plan),
            patch.object(
                cdc_catalog,"load_plan_version",
                return_value=active_plan),
            patch.object(
                j4,"durable_active_plan_version",
                return_value=1)
        ):
            j4.activate_catalog(restart_cfg)
        assert restart_cfg["catalog_version"]==2
        assert restart_cfg["catalog_restart_promote"]
        assert restart_cfg["catalog_stateful_tasks"]==[tasks[0]]

        unsafe_published=dict(
            published_plan,
            mappings=[dict(
                src_table="orders",sr_table="new_plain")])
        with (
            patch.object(
                cdc_catalog,"load_plan",
                return_value=unsafe_published),
            patch.object(
                cdc_catalog,"load_plan_version",
                return_value=active_plan),
            patch.object(
                j4,"durable_active_plan_version",
                return_value=1)
        ):
            expect_error(
                lambda: j4.activate_catalog(dict(restart_cfg)),
                RuntimeError,
                "cannot also change stateless sink topology")

        # Stateful model views remain unsupported; only sink manifests may own
        # stateful operators in the first control-plane phase.
        expect_error(
            lambda: cdc_catalog.execute(
                path,
                "CREATE VIEW model.bad AS "
                "SELECT l.id FROM mysql.lefts l "
                "JOIN mysql.rights r ON l.customer_id=r.id"),
            ValueError)

        # JOIN target identity is internal; a user-declared catalog PK would
        # alias bag members and must fail during draft compilation.
        cdc_catalog.execute(
            path,
            "ALTER TABLE starrocks.joined ADD PRIMARY KEY (left_id)")
        con=cdc_catalog.catalog_open(path)
        try:
            expect_error(
                lambda: cdc_catalog.compile_draft(con),
                ValueError,
                "identity is managed internally")
        finally:
            con.close()

        # Format-2 catalogs migrate in place with an empty stateful manifest.
        old=os.path.join(td,"format2.sqlite3")
        db=sqlite3.connect(old)
        db.execute(
            "CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
        db.execute("INSERT INTO meta VALUES('format','2')")
        db.execute("INSERT INTO meta VALUES('draft_revision','0')")
        db.execute("INSERT INTO meta VALUES('published_version','0')")
        db.execute("INSERT INTO meta VALUES('config_revision','0')")
        db.execute("""
            CREATE TABLE plans(
                version INTEGER PRIMARY KEY,
                revision INTEGER NOT NULL,
                plan_hash TEXT NOT NULL UNIQUE,
                mappings_json TEXT NOT NULL,
                macros_json TEXT NOT NULL,
                created REAL NOT NULL,
                udfs_json TEXT NOT NULL DEFAULT '[]')
        """)
        db.commit()
        db.close()
        migrated=cdc_catalog.catalog_open(old)
        try:
            assert cdc_catalog._meta_get(
                migrated,"format")=="3"
            columns={
                row[1]
                for row in migrated.execute(
                    "PRAGMA table_info(plans)").fetchall()
            }
            assert "stateful_tasks_json" in columns
        finally:
            migrated.close()

    print(
        "catalog_stateful_manifest_test ok format3 "
        "aggregate join coexist hash load show restart_fence hot_install restart_promote migration",
        flush=True,
    )


if __name__=="__main__":
    main()
