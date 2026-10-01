#!/usr/bin/env python3
"""Startup compiler and source-scope adapter for catalog stateful tasks.

This module binds catalog stateful tasks to durable aggregate/JOIN runtimes.
It expands shared source capture, compiles live source/target contracts, and
persists retirement frontiers so compatible hot add/drop survives crashes.
"""
import re
import time

import aggregate_target_mapping
import aggregate_task_catalog
import join_target_mapping
import join_task_catalog
import stateful_task_plan


def install(con):
    con.executescript("""
        CREATE TABLE IF NOT EXISTS stateful_retirements(
            task_id TEXT PRIMARY KEY,
            kind TEXT NOT NULL,
            sink_key TEXT NOT NULL,
            frontier INTEGER NOT NULL,
            created REAL NOT NULL,
            updated REAL NOT NULL);
        CREATE INDEX IF NOT EXISTS stateful_retirements_frontier
            ON stateful_retirements(frontier,task_id);
    """)


def _task_descriptor(con,kind,task_id):
    if str(kind)=="aggregate":
        return aggregate_task_catalog.task_info(
            con,task_id)
    if str(kind)=="inner_join":
        return join_task_catalog.task_info(
            con,task_id)
    raise ValueError(
        "unsupported stateful task kind: "+str(kind))


def _task_mapping(kind,task):
    if str(kind)=="aggregate":
        return aggregate_target_mapping.mapping_from_descriptor(
            task)
    if str(kind)=="inner_join":
        return join_target_mapping.mapping_from_descriptor(
            task)
    raise ValueError(
        "unsupported stateful task kind: "+str(kind))


def stage_retirement(con,kind,task,frontier):
    kind=str(kind)
    task_id=_text(task["task_id"],"task_id")
    sink_key=_text(task["sink_key"],"sink_key")
    frontier=int(frontier)
    if frontier<0:
        raise ValueError(
            "stateful retirement frontier cannot be negative")
    existing=con.execute("""
        SELECT kind,sink_key,frontier
        FROM stateful_retirements
        WHERE task_id=?
    """,(task_id,)).fetchone()
    if existing is not None:
        actual=(str(existing[0]),str(existing[1]),int(existing[2]))
        expected=(kind,sink_key,frontier)
        if actual!=expected:
            raise RuntimeError(
                "stateful retirement intent changed across restart "
                "task=%s actual=%r expected=%r"
                % (task_id,actual,expected))
        return dict(
            task_id=task_id,kind=kind,
            sink_key=sink_key,frontier=frontier)
    now=time.time()
    con.execute("""
        INSERT INTO stateful_retirements(
            task_id,kind,sink_key,frontier,created,updated)
        VALUES(?,?,?,?,?,?)
    """,(task_id,kind,sink_key,frontier,now,now))
    return dict(
        task_id=task_id,kind=kind,
        sink_key=sink_key,frontier=frontier)


def retirement_info(con,task_id):
    row=con.execute("""
        SELECT kind,sink_key,frontier,created,updated
        FROM stateful_retirements
        WHERE task_id=?
    """,(_text(task_id,"task_id"),)).fetchone()
    if row is None:
        raise KeyError(
            "stateful retirement intent does not exist")
    return dict(
        task_id=str(task_id),kind=str(row[0]),
        sink_key=str(row[1]),frontier=int(row[2]),
        created=float(row[3]),updated=float(row[4]))


def clear_retirement(con,task_id):
    con.execute(
        "DELETE FROM stateful_retirements WHERE task_id=?",
        (_text(task_id,"task_id"),))


def pending_retirements(con):
    result=[]
    for task_id,kind,sink_key,frontier,created,updated in con.execute("""
        SELECT task_id,kind,sink_key,frontier,created,updated
        FROM stateful_retirements
        ORDER BY created,task_id
    """).fetchall():
        task=_task_descriptor(
            con,kind,task_id)
        if task["status"] in {"retired","failed"}:
            clear_retirement(
                con,task_id)
            continue
        if str(task["sink_key"])!=str(sink_key):
            raise RuntimeError(
                "stateful retirement sink identity changed task="
                +str(task_id))
        result.append(dict(
            kind=str(kind),task=task,
            mapping=_task_mapping(kind,task),
            frontier=int(frontier)))
    return result


def stage_absent_retirements(con,compiled,frontier):
    current={
        (item["kind"],item["task"]["task_id"])
        for item in compiled or ()
    }
    staged=[]
    for kind,task in _all_durable_tasks(con):
        if (kind,task["task_id"]) in current:
            continue
        if task["status"] in {"retired","failed"}:
            continue
        staged.append(
            stage_retirement(
                con,kind,task,frontier))
    return staged


def prepare_startup_retirements(
        con,cfg,compiled,frontier
):
    """Recover catalog removals without inventing a new retirement frontier.

    Active/ready tasks receive a durable frontier and resume catch-up. A task
    that never reached active publication may be abandoned immediately because
    it has no completed result contract to preserve.
    """
    current={
        (item["kind"],item["task"]["task_id"])
        for item in compiled or ()
    }
    staged=[]
    abandoned=[]
    for kind,task in _all_durable_tasks(con):
        if (kind,task["task_id"]) in current:
            continue
        if task["status"] in {"retired","failed"}:
            continue
        if task["status"]=="candidate":
            abandoned.append(
                retire_task(con,cfg,kind,task))
            continue
        staged.append(
            stage_retirement(
                con,kind,task,frontier))
    return dict(
        staged=staged,abandoned=abandoned)



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


def _signature(ir,side,name=None):
    if ir["kind"]=="group_aggregate":
        column=side
        rows=ir["source"]["schema"]
    else:
        column=name
        rows=ir["sources"][side]["schema"]
    for item in rows:
        if str(item[0])==str(column):
            return item
    raise ValueError(
        "stateful source schema lacks column "+str(column))


def infer_target_schema(kind,ir):
    import j4
    kind=str(kind)
    if kind=="aggregate":
        result=[]
        for name in ir["group_keys"]:
            source=_signature(ir,name)
            if str(source[3]).upper()=="YES":
                raise ValueError(
                    "aggregate GROUP BY key is nullable; stateful runtime v1 "
                    "requires non-null group keys: "+name)
            result.append(dict(
                name=name,
                type=j4.mysql_pk_target_type(source),
                nullable=False,key=True))
        for aggregate in ir["aggregates"]:
            function=aggregate["function"]
            output=aggregate["output"]
            if function=="count":
                type_sql="BIGINT"
                nullable=False
            elif function=="avg":
                type_sql="DOUBLE"
                nullable=True
            elif function=="sum":
                source=_signature(ir,aggregate["input"])
                data_type=str(source[1]).lower()
                column_type=str(source[2]).lower()
                if data_type in {"decimal","numeric"}:
                    match=re.search(
                        r"\((\d+)\s*,\s*(\d+)\)",column_type)
                    scale=int(match.group(2)) if match else 0
                    type_sql="DECIMAL(38,%d)" % scale
                elif data_type in {
                    "tinyint","smallint","mediumint","int",
                    "integer","bigint","year"
                }:
                    type_sql="LARGEINT"
                elif data_type in {"float","double","real"}:
                    type_sql="DOUBLE"
                else:
                    raise ValueError(
                        "SUM input type is unsupported for automatic "
                        "stateful target creation: "+data_type)
                nullable=True
            else:
                raise ValueError(
                    "unsupported aggregate function: "+function)
            result.append(dict(
                name=output,type=type_sql,
                nullable=nullable,key=False))
        return aggregate_target_mapping.validate_semantic_target(
            ir,result)

    if kind!="inner_join":
        raise ValueError("unsupported stateful kind: "+kind)
    result=[dict(
        name=join_target_mapping.PAIR_COLUMN,
        type="VARCHAR(1024)",nullable=False,key=True)]
    for projection in ir["projections"]:
        source=_signature(
            ir,projection["source"],projection["column"])
        dtype=j4.source_type(
            str(source[1]),str(source[2]))
        type_sql,track_size=j4.mysql_output_target_type(
            source,str(dtype))
        if track_size:
            raise ValueError(
                "JOIN projected source column can exceed the automatic "
                "StarRocks field limit; create the target explicitly: "
                +projection["output"])
        result.append(dict(
            name=projection["output"],
            type=type_sql,
            nullable=str(source[3]).upper()=="YES",
            key=False))
    import join_task_catalog
    return join_task_catalog.normalize_target_schema(
        ir,result)


def target_ddl(target_table,target_schema):
    import j4
    target_table=_text(target_table,"target_table")
    schema=list(target_schema or ())
    keys=[item["name"] for item in schema if item["key"]]
    if not keys:
        raise ValueError("stateful target requires a primary key")
    ordered=keys+[
        item["name"] for item in schema
        if item["name"] not in keys]
    by_name={item["name"]:item for item in schema}
    definitions=[]
    for name in ordered:
        item=by_name[name]
        definitions.append(
            "  "+j4.sql_name(name,True)+" "+item["type"]+" "
            +("NOT NULL" if not item["nullable"] else "NULL"))
    return (
        "CREATE TABLE IF NOT EXISTS "
        +j4.sql_name(target_table,True)+" (\n"
        +",\n".join(definitions)
        +"\n) ENGINE=OLAP\nPRIMARY KEY("
        +",".join(j4.sql_name(name,True) for name in keys)
        +")\nDISTRIBUTED BY HASH("
        +",".join(j4.sql_name(name,True) for name in keys[:3])
        +")"
    )


def resolve_target_schema(
        cfg,kind,ir,target_table,
        create_missing=False,allow_missing=False
):
    import j4
    target_table=_text(target_table,"target_table")
    with j4.mysql_connect(cfg,target=True) as target:
        with target.cursor() as cur:
            exists=j4.target_table_exists(
                cur,cfg,target_table)
            if not exists:
                inferred=infer_target_schema(kind,ir)
                if create_missing:
                    ddl=target_ddl(
                        target_table,inferred)
                    try:
                        cur.execute(ddl)
                    except Exception as exc:
                        raise RuntimeError(
                            "automatic stateful target creation failed: "
                            +str(exc)+"; ddl="+ddl) from exc
                    exists=True
                elif allow_missing:
                    return inferred
                else:
                    raise RuntimeError(
                        "stateful target table does not exist: "
                        +target_table)
    if kind=="aggregate":
        return aggregate_target_mapping.descriptor_schema_from_target(
            cfg,ir,target_table)
    return join_target_mapping.descriptor_schema_from_target(
        cfg,ir,target_table)


def compile_catalog_tasks(
        cfg,catalog_plan_version,manifests,source_metadata,
        create_missing=False,allow_missing=False
):
    compiled=[]
    for manifest in manifests or ():
        base=stateful_task_plan.compile_ir(
            manifest,cfg["mysql"]["database"],
            source_metadata)
        target_schema=resolve_target_schema(
            cfg,base["kind"],base["ir"],
            manifest["target_table"],
            create_missing=create_missing,
            allow_missing=allow_missing)
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
        same=[
            old for old_kind,old in durable
            if old_kind==item["kind"]
            and old["task_id"]==task["task_id"]
        ]
        if same:
            old=same[0]
            if old["descriptor_hash"]!=task["descriptor_hash"]:
                raise RuntimeError(
                    "stateful task id was reused with different semantics: "
                    +task["task_id"])
            if old["status"] in {"retired","failed"}:
                raise RuntimeError(
                    "terminal stateful task cannot be revived; create a new "
                    "catalog generation: "+task["task_id"])
            continue
        with j4.mysql_connect(cfg,target=True) as target:
            with target.cursor() as cur:
                if not j4.target_table_exists(
                    cur,cfg,task["target_table"]
                ):
                    continue
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
        result[(stateful_task_plan.writer_plan_version(task["plan_version"]),str(task["sink_key"]))]=mapping
    for task in join_task_catalog.list_tasks(con):
        mapping=join_target_mapping.mapping_from_descriptor(task)
        result[(stateful_task_plan.writer_plan_version(task["plan_version"]),str(task["sink_key"]))]=mapping
    return result


def retire_task(con,cfg,kind,task):
    import aggregate_job_bridge
    import join_job_bridge
    import j4
    import source_state
    import task_generation

    kind=str(kind)
    if kind not in {"aggregate","inner_join"}:
        raise ValueError("unsupported stateful task kind: "+kind)
    mapping=(
        aggregate_target_mapping.mapping_from_descriptor(task)
        if kind=="aggregate"
        else join_target_mapping.mapping_from_descriptor(task)
    )
    if task["status"] in {"retired","failed"}:
        clear_retirement(
            con,task["task_id"])
        return dict(
            kind=kind,task=task,mapping=mapping)

    generation=task_generation.maybe_info(
        con,task["sink_key"],task["plan_version"])

    # Materialize any remaining outbox rows before removing the retention
    # consumer. For an active hot-drop this is normally a no-op because the
    # caller already fenced on target VISIBLE; for an unpublished candidate it
    # preserves any durable jobs that were staged before cancellation.
    if generation is not None and generation["source_pin_released"]:
        if kind=="aggregate":
            aggregate_job_bridge.stage_pending(
                con,task["consumer_id"],mapping,cfg)
        else:
            join_job_bridge.stage_pending(
                con,task["consumer_id"],mapping,cfg)

    # Consumer removal, generation retirement, descriptor retirement and
    # retirement-intent cleanup are one SQLite commit. A crash can therefore
    # never leave a pending intent whose source consumer was already deleted.
    with j4.state_transaction(con):
        if generation is not None and generation["source_pin_released"]:
            source_state.remove_consumer(
                con,task["consumer_id"])
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
        clear_retirement(
            con,task["task_id"])
    return dict(
        kind=kind,task=durable,mapping=mapping)


def retire_absent(con,cfg,compiled):
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
        retired.append(
            retire_task(con,cfg,kind,task))
    return retired



def extend_durable_source_scope(con,cfg,mappings):
    """Keep every previously registered shared relation in live capture scope.

    Removing the last task that references a relation must not silently stop
    logging that relation while its durable base remains marked complete. Until
    an explicit source-scope GC/rebuild protocol exists, shared scope is
    monotonic: once registered, a relation continues to be captured.
    """
    database=_text(
        cfg["mysql"]["database"],"mysql database")
    by_relation={
        database+"."+str(mapping["src_table"]):mapping
        for mapping in mappings or ()
    }
    rows=con.execute("""
        SELECT table_name FROM source_relations
        ORDER BY table_name
    """).fetchall()
    for row in rows:
        relation=str(row[0])
        if relation in by_relation:
            continue
        prefix=database+"."
        if not relation.startswith(prefix):
            raise RuntimeError(
                "durable shared source relation belongs to another "
                "database: "+relation)
        table=relation[len(prefix):]
        if not table or "." in table:
            raise RuntimeError(
                "unsupported durable shared source relation identity: "
                +relation)
        by_relation[relation]=_probe_source_mapping(
            cfg,table)
    return [
        by_relation[name]
        for name in sorted(by_relation)
    ]



def migrate_writer_versions(con):
    """Move pre-namespace stateful durable jobs/deliveries to negative versions.

    Old candidate builds wrote the positive task_version into jobs.plan_version,
    which shares the stateless catalog-version namespace. Migrate every linked
    stateful job atomically. A delivery containing any unrelated/mixed job is
    fail-closed because changing its mapping identity would be unsafe.
    """
    import j4

    specs=(
        (
            "aggregate_job_links",
            "aggregate_task_descriptors",
        ),
        (
            "join_job_links",
            "join_task_descriptors",
        ),
    )
    changes=0
    with j4.state_transaction(con):
        for link_table,descriptor_table in specs:
            rows=con.execute(
                """
                SELECT l.job_id,l.consumer_id,d.plan_version,j.plan_version,
                       a.delivery_id
                FROM %s l
                JOIN %s d ON d.consumer_id=l.consumer_id
                JOIN jobs j ON j.id=l.job_id
                LEFT JOIN job_assignments a ON a.job_id=j.id
                ORDER BY l.job_id
                """ % (link_table,descriptor_table)
            ).fetchall()
            for job_id,consumer_id,task_version,current,delivery in rows:
                expected=stateful_task_plan.writer_plan_version(
                    task_version)
                current=int(current)
                if current!=expected:
                    if current<0:
                        raise RuntimeError(
                            "stateful durable job has an unknown writer "
                            "version namespace job_id=%d current=%d expected=%d"
                            % (int(job_id),current,expected))
                    con.execute(
                        "UPDATE jobs SET plan_version=? WHERE id=?",
                        (expected,int(job_id)))
                    changes+=1
                if delivery is None:
                    continue
                assigned=con.execute(
                    """
                    SELECT j.id,
                           a.consumer_id,
                           q.consumer_id
                    FROM job_assignments x
                    JOIN jobs j ON j.id=x.job_id
                    LEFT JOIN aggregate_job_links a ON a.job_id=j.id
                    LEFT JOIN join_job_links q ON q.job_id=j.id
                    WHERE x.delivery_id=?
                    ORDER BY j.id
                    """,
                    (str(delivery),)
                ).fetchall()
                for other_id,aggregate_consumer,join_consumer in assigned:
                    owners=[
                        value for value in (
                            aggregate_consumer,join_consumer)
                        if value is not None
                    ]
                    if owners!=[str(consumer_id)]:
                        raise RuntimeError(
                            "stateful writer-version migration found a mixed "
                            "delivery delivery=%s job_id=%d owners=%r "
                            "expected_consumer=%s"
                            % (
                                str(delivery),int(other_id),
                                owners,str(consumer_id),
                            ))
                row=con.execute(
                    "SELECT plan_version FROM deliveries WHERE id=?",
                    (str(delivery),)
                ).fetchone()
                if row is None:
                    raise RuntimeError(
                        "stateful linked job assignment lacks delivery row: "
                        +str(delivery))
                delivery_version=int(row[0])
                if delivery_version!=expected:
                    if delivery_version<0:
                        raise RuntimeError(
                            "stateful delivery has an unknown writer version "
                            "delivery=%s current=%d expected=%d"
                            % (
                                str(delivery),
                                delivery_version,expected,
                            ))
                    con.execute(
                        "UPDATE deliveries SET plan_version=? WHERE id=?",
                        (expected,str(delivery)))
                    changes+=1
    return changes



def compile_online_catalog_tasks(
        cfg,catalog_plan_version,manifests,capture_mappings,
        create_missing=False,allow_missing=False
):
    """Compile a hot candidate only from relations already in live capture.

    Online stateful activation must never silently probe/add a new source
    relation: expanding authoritative source history requires an explicit
    source-scope bootstrap/restart boundary.
    """
    manifests=list(manifests or ())
    required=required_sources(manifests)
    by_source={
        str(mapping["src_table"]):mapping
        for mapping in capture_mappings or ()
    }
    missing=[
        source for source in required
        if source not in by_source
    ]
    if missing:
        raise RuntimeError(
            "stateful online activation requires already mirrored source "
            "relations; restart is required to expand capture: "
            +",".join(sorted(missing)))
    metadata={
        source:_source_metadata_from_mapping(by_source[source])
        for source in required
    }
    return compile_catalog_tasks(
        cfg,catalog_plan_version,manifests,metadata,
        create_missing=create_missing,
        allow_missing=allow_missing)


def compiled_by_sink(compiled):
    result={}
    for item in compiled or ():
        sink=str(item["task"]["sink_key"])
        if sink in result:
            raise RuntimeError(
                "duplicate stateful sink in compiled plan: "+sink)
        result[sink]=item
    return result


def transition(current,candidate):
    """Compare durable stateful semantics by sink, not catalog plan version."""
    old=compiled_by_sink(current)
    new=compiled_by_sink(candidate)
    added=sorted(set(new)-set(old))
    dropped=sorted(set(old)-set(new))
    changed=[]
    retained=[]
    for sink in sorted(set(old)&set(new)):
        before=old[sink]["task"]
        after=new[sink]["task"]
        if before["descriptor_hash"]!=after["descriptor_hash"]:
            changed.append(sink)
        else:
            retained.append(sink)
    return dict(
        added=added,
        dropped=dropped,
        changed=changed,
        retained=retained,
        old=old,
        new=new,
    )
