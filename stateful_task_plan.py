#!/usr/bin/env python3
"""Pure compiler from catalog stateful manifests to durable task descriptors.

Database I/O is deliberately excluded. Online preflight supplies live MySQL
schema/PK metadata and the already validated StarRocks target contract.
"""
import sqlglot
from sqlglot import exp

import aggregate_ir
import aggregate_target_mapping
import aggregate_task_catalog
import join_ir
import join_target_mapping
import join_task_catalog


def _text(value,name):
    value=str(value or "").strip()
    if not value:
        raise ValueError(name+" must be non-empty")
    return value


def _task_ids(sink,plan_version):
    sink=_text(sink,"sink")
    version=int(plan_version)
    prefix="catalog:"+sink+":plan:"+str(version)
    return dict(
        task_id=prefix,
        state_id=prefix+":state",
        consumer_id=prefix+":consumer",
        sink_key=sink,
    )


def _tree(sql):
    tree=sqlglot.parse_one(str(sql or ""),read="duckdb")
    if not isinstance(tree,exp.Select):
        raise ValueError("stateful task SQL must be one SELECT")
    return tree


def _rewrite_aggregate(sql):
    tree=_tree(sql)
    tables=list(tree.find_all(exp.Table))
    if len(tables)!=1:
        raise ValueError(
            "aggregate manifest must reference exactly one source")
    table=tables[0]
    table.set("this",exp.to_identifier("arrow_batch"))
    table.set("db",None)
    table.set("catalog",None)
    return tree.sql(dialect="duckdb")


def _rewrite_join(sql):
    tree=_tree(sql)
    tables=list(tree.find_all(exp.Table))
    if len(tables)!=2:
        raise ValueError(
            "JOIN manifest must reference exactly two sources")
    for table,name in zip(tables,("left_batch","right_batch")):
        table.set("this",exp.to_identifier(name))
        table.set("db",None)
        table.set("catalog",None)
    return tree.sql(dialect="duckdb")


def compile_ir(
        manifest,mysql_database,source_metadata
):
    if not isinstance(manifest,dict):
        raise ValueError("stateful manifest must be a dict")
    kind=str(manifest.get("kind") or "")
    if kind not in {"aggregate","inner_join"}:
        raise ValueError("unsupported stateful manifest kind: "+kind)
    sources=[
        _text(value,"source relation")
        for value in manifest.get("source_relations",())
    ]
    mysql_database=_text(mysql_database,"mysql_database")
    metadata=dict(source_metadata or {})
    for source in sources:
        if source not in metadata:
            raise ValueError(
                "missing live source metadata for "+source)
        entry=metadata[source]
        if not entry.get("schema_signature"):
            raise ValueError(
                "source metadata lacks schema signature for "+source)
        if not entry.get("primary_key"):
            raise ValueError(
                "source metadata lacks primary key for "+source)

    if kind=="aggregate":
        if len(sources)!=1:
            raise ValueError(
                "aggregate manifest requires one source relation")
        source=sources[0]
        entry=metadata[source]
        ir=aggregate_ir.compile_sql(
            mysql_database+"."+source,
            entry["schema_signature"],
            _rewrite_aggregate(manifest.get("sql")))
    else:
        if len(sources)!=2:
            raise ValueError(
                "INNER JOIN manifest requires two source relations")
        left,right=sources
        left_meta=metadata[left]
        right_meta=metadata[right]
        ir=join_ir.compile_sql(
            mysql_database+"."+left,
            left_meta["schema_signature"],left_meta["primary_key"],
            mysql_database+"."+right,
            right_meta["schema_signature"],right_meta["primary_key"],
            _rewrite_join(manifest.get("sql")))
    return dict(
        kind=kind,
        source_relations=list(sources),
        ir=ir,
    )


def compile_task(
        manifest,plan_version,mysql_database,
        source_metadata,target_schema
):
    compiled=compile_ir(
        manifest,mysql_database,source_metadata)
    kind=compiled["kind"]
    sources=compiled["source_relations"]
    ir=compiled["ir"]
    sink=_text(manifest.get("sink"),"sink")
    target_table=_text(
        manifest.get("target_table"),"target_table")
    task_version=int(manifest.get("task_version",plan_version))
    ids=_task_ids(sink,task_version)

    if kind=="aggregate":
        task=aggregate_task_catalog.descriptor(
            ids["task_id"],ids["sink_key"],task_version,
            ir,target_table,ids["state_id"],ids["consumer_id"],
            target_schema)
        mapping=aggregate_target_mapping.mapping_from_descriptor(
            task)
    else:
        task=join_task_catalog.descriptor(
            ids["task_id"],ids["sink_key"],task_version,
            ir,target_table,ids["state_id"],ids["consumer_id"],
            target_schema)
        mapping=join_target_mapping.mapping_from_descriptor(
            task)

    return dict(
        kind=kind,
        sink=sink,
        catalog_plan_version=int(plan_version),
        task_version=task_version,
        source_relations=list(sources),
        task=task,
        mapping=mapping,
    )
