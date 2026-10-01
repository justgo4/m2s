#!/usr/bin/env python3
"""Startup compiler and source-scope adapter for catalog stateful tasks.

This module deliberately keeps stateful catalog activation restart-boundary
only. It expands shared source capture to every stateful input relation,
compiles live source metadata + the real StarRocks target contract into the
existing durable aggregate/JOIN descriptors, and returns writer mappings for
the ordinary j4 delivery pipeline.
"""
import re

import aggregate_target_mapping
import aggregate_task_catalog
import join_target_mapping
import join_task_catalog
import stateful_task_plan


def _text(value,name):
    value=str(value or "").strip()
    if not value:
        raise ValueError(name+" must be non-empty")
    return value


def required_sources(manifests):
    result=[]
    for manifest in manifests or ():
        for source in manifest.get("source_relations",()):
            source=_text(source,"stateful source relation")
            if source not in result:
                result.append(source)
    return result


def _source_metadata_from_mapping(mapping):
    return dict(
        schema_signature=[
            tuple(item)
            for item in mapping["_schema_signature"]
        ],
        primary_key=list(_pk_columns(mapping)),
    )


def _pk_columns(mapping):
    value=mapping.get("primary_key")
    if isinstance(value,str):
        return [value]
    return list(value or ())


def _probe_source_mapping(cfg,table):
    import j4
    table=_text(table,"stateful source table")
    with j4.mysql_connect(cfg) as source:
        with source.cursor() as cur:
            cur.execute("""
                SELECT ENGINE,TABLE_COMMENT
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
            """,(cfg["mysql"]["database"],table))
            row=cur.fetchone()
            if not row or str(row[0]).upper()!="INNODB":
                raise ValueError(
                    table+": stateful source must be an InnoDB table")
            table_comment=str(row[1] or "")
            cur.execute("""
                SELECT COLUMN_NAME,DATA_TYPE,COLUMN_TYPE,IS_NULLABLE,
                       COLLATION_NAME,EXTRA,COLUMN_COMMENT
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
                ORDER BY ORDINAL_POSITION
            """,(cfg["mysql"]["database"],table))
            column_rows=cur.fetchall()
            if not column_rows:
                raise ValueError(
                    table+": stateful source has no visible columns")
            columns=[tuple(item[:6]) for item in column_rows]
            if any(
                re.search(
                    r"(?:VIRTUAL|STORED) GENERATED",
                    str(item[5]).upper())
                for item in columns
            ):
                raise ValueError(
                    table+": generated columns are unsupported "
                    "for stateful source capture")
            if any(
                str(item[0]).startswith("_sync_")
                or str(item[0])=="__op"
                for item in columns
            ):
                raise ValueError(
                    table+": reserved source column name")
            cur.execute("""
                SELECT COLUMN_NAME
                FROM information_schema.STATISTICS
                WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
                  AND INDEX_NAME='PRIMARY'
                ORDER BY SEQ_IN_INDEX
            """,(cfg["mysql"]["database"],table))
            primary=[str(item[0]) for item in cur.fetchall()]
            if not primary:
                raise ValueError(
                    table+": stateful source requires a PRIMARY KEY")
    schema=[
        (str(item[0]),j4.source_type(str(item[1]),str(item[2])))
        for item in columns
    ]
    return dict(
        src_table=table,
        primary_key=primary[0] if len(primary)==1 else primary,
        _source_only=True,
        _schema=schema,
        _schema_signature=columns,
        _source_name_set=frozenset(name for name,_ in schema),
        _source_index={
            name:index for index,(name,_) in enumerate(schema)
        },
        _json_columns={
            str(item[0]) for item in columns
            if str(item[1]).lower()=="json"
        },
        _source_table_comment=table_comment,
        _source_column_comments={
            str(item[0]):str(item[6] or "")
            for item in column_rows
        },
    )


def source_scope(cfg,manifests,prepared):
    required=required_sources(manifests)
    by_source={}
    for mapping in prepared or ():
        source=str(mapping["src_table"])
        existing=by_source.get(source)
        if existing is None:
            by_source[source]=mapping
        elif (
            list(existing.get("_schema_signature",()))
            !=list(mapping.get("_schema_signature",()))
            or _pk_columns(existing)!=_pk_columns(mapping)
        ):
            raise RuntimeError(
                "stateful source scope found conflicting checked "
                "metadata for "+source)
    for source in required:
        if source not in by_source:
            by_source[source]=_probe_source_mapping(cfg,source)
    capture=[
        by_source[source]
        for source in sorted(by_source)
        if source in set(required)
        or not by_source[source].get("_source_only")
    ]
    metadata={
        source:_source_metadata_from_mapping(by_source[source])
        for source in required
    }
    return dict(
        required_sources=required,
        capture_mappings=capture,
        source_metadata=metadata,
    )


def compile_catalog_tasks(
        cfg,catalog_plan_version,manifests,source_metadata
):
    compiled=[]
    for manifest in manifests or ():
        base=stateful_task_plan.compile_ir(
            manifest,cfg["mysql"]["database"],
            source_metadata)
        if base["kind"]=="aggregate":
            target_schema=(
                aggregate_target_mapping.descriptor_schema_from_target(
                    cfg,base["ir"],manifest["target_table"]))
        else:
            target_schema=(
                join_target_mapping.descriptor_schema_from_target(
                    cfg,base["ir"],manifest["target_table"]))
        item=stateful_task_plan.compile_task(
            manifest,int(catalog_plan_version),
            cfg["mysql"]["database"],
            source_metadata,target_schema)
        compiled.append(item)
    return compiled


def register_compiled(con,compiled):
    result=[]
    for item in compiled or ():
        task=item["task"]
        if item["kind"]=="aggregate":
            durable=aggregate_task_catalog.register_task(
                con,task["task_id"],task["sink_key"],
                task["plan_version"],task["ir"],
                task["target_table"],task["state_id"],
                task["consumer_id"],task["target_schema"])
        else:
            durable=join_task_catalog.register_task(
                con,task["task_id"],task["sink_key"],
                task["plan_version"],task["ir"],
                task["target_table"],task["state_id"],
                task["consumer_id"],task["target_schema"])
        if durable["descriptor_hash"]!=task["descriptor_hash"]:
            raise RuntimeError(
                "stateful catalog descriptor differs from durable task "
                +task["task_id"])
        copy=dict(item)
        copy["task"]=durable
        result.append(copy)
    return result



def _all_durable_tasks(con):
    result=[]
    result.extend(
        ("aggregate",item)
        for item in aggregate_task_catalog.list_tasks(con)
    )
    result.extend(
        ("inner_join",item)
        for item in join_task_catalog.list_tasks(con)
    )
    return result


def ensure_registration_safe(con,cfg,compiled):
    import j4
    current={
        (item["kind"],item["task"]["task_id"])
        for item in compiled or ()
    }
    durable=_all_durable_tasks(con)
    for item in compiled or ():
        task=item["task"]
        for old_kind,old in durable:
            if (
                old["sink_key"]==task["sink_key"]
                and old["task_id"]!=task["task_id"]
                and old["status"] not in {"retired","failed"}
            ):
                raise RuntimeError(
                    "stateful sink semantic replacement requires an explicit "
                    "generation cutover/fence before activation: "
                    +task["sink_key"])
        exists=any(
            old_kind==item["kind"]
            and old["task_id"]==task["task_id"]
            for old_kind,old in durable
        )
        if exists:
            continue
        with j4.mysql_connect(cfg,target=True) as target:
            with target.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM "
                    +j4.sql_name(task["target_table"],True)
                    +" LIMIT 1")
                if cur.fetchone():
                    raise RuntimeError(
                        "new stateful task requires an empty pre-existing "
                        "target table: "+task["target_table"])
    return current



def durable_mappings(con):
    result={}
    for task in aggregate_task_catalog.list_tasks(con):
        mapping=aggregate_target_mapping.mapping_from_descriptor(task)
        result[(int(task["plan_version"]),str(task["sink_key"]))]=mapping
    for task in join_task_catalog.list_tasks(con):
        mapping=join_target_mapping.mapping_from_descriptor(task)
        result[(int(task["plan_version"]),str(task["sink_key"]))]=mapping
    return result


def retire_absent(con,cfg,compiled):
    import aggregate_job_bridge
    import join_job_bridge
    import source_state
    import task_generation

    current={
        (item["kind"],item["task"]["task_id"])
        for item in compiled or ()
    }
    retired=[]
    for kind,task in _all_durable_tasks(con):
        if (kind,task["task_id"]) in current:
            continue
        if task["status"] in {"retired","failed"}:
            continue
        mapping=(
            aggregate_target_mapping.mapping_from_descriptor(task)
            if kind=="aggregate"
            else join_target_mapping.mapping_from_descriptor(task)
        )
        generation=task_generation.maybe_info(
            con,task["sink_key"],task["plan_version"])
        if generation is not None and generation["source_pin_released"]:
            try:
                if kind=="aggregate":
                    aggregate_job_bridge.stage_pending(
                        con,task["consumer_id"],mapping,cfg)
                else:
                    join_job_bridge.stage_pending(
                        con,task["consumer_id"],mapping,cfg)
            except KeyError:
                pass
            try:
                source_state.remove_consumer(
                    con,task["consumer_id"])
            except KeyError:
                pass
        if generation is not None and generation["status"] not in {
            "retired","failed"
        }:
            task_generation.abandon(
                con,task["sink_key"],task["plan_version"],
                status="retired")
        if kind=="aggregate":
            durable=aggregate_task_catalog.set_status(
                con,task["task_id"],"retired")
        else:
            durable=join_task_catalog.set_status(
                con,task["task_id"],"retired")
        retired.append(dict(
            kind=kind,task=durable,mapping=mapping))
    return retired
