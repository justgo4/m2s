"""Privacy-safe explanation of one durable m2s catalog plan.

This module is deliberately pure: it does not connect to MySQL/StarRocks and
does not mutate catalog/runtime state. j4.py is responsible for loading the
published plan and optional durable runtime snapshot.
"""


def _task_runtime(status,kind,sink):
    key=(
        "aggregate_tasks"
        if kind=="aggregate"
        else "join_tasks"
        if kind=="inner_join"
        else None)
    if key is None:
        return []
    rows=[]
    for item in (status or {}).get(key,()) or ():
        if str(item.get("sink_key",""))!=str(sink):
            continue
        rows.append(dict(
            task_id=str(item.get("task_id","")),
            plan_version=int(item.get("plan_version",0)),
            status=str(item.get("status","")),
            target_table=str(item.get("target_table","")),
        ))
    rows.sort(
        key=lambda item:(
            item["plan_version"],
            item["task_id"]))
    return rows


def explain(plan,status=None):
    plan=dict(plan or {})
    status=dict(status or {})
    stateless=[]
    for mapping in plan.get("mappings",()) or ():
        mapping=dict(mapping)
        primary=mapping.get("primary_key")
        if isinstance(primary,str):
            primary=[primary]
        else:
            primary=list(primary or ())
        stateless.append(dict(
            sink=str(
                mapping.get(
                    "_catalog_sink",
                    "starrocks."+str(
                        mapping.get("sr_table","")))),
            source="mysql."+str(
                mapping.get("src_table","")),
            target_table=str(
                mapping.get("sr_table","")),
            primary_key=primary,
            sql=str(mapping.get("sql","")),
        ))
    stateless.sort(key=lambda item:item["sink"])

    stateful=[]
    for task in plan.get("stateful_tasks",()) or ():
        task=dict(task)
        kind=str(task.get("kind",""))
        sink=str(task.get("sink",""))
        stateful.append(dict(
            sink=sink,
            kind=kind,
            source_relations=[
                "mysql."+str(value)
                for value in task.get(
                    "source_relations",()) or ()
            ],
            target_table=str(
                task.get("target_table","")),
            primary_key=list(
                task.get("primary_key",()) or ()),
            task_version=int(
                task.get("task_version",0)),
            sql=str(task.get("sql","")),
            runtime=_task_runtime(
                status,kind,sink),
        ))
    stateful.sort(
        key=lambda item:(item["sink"],item["kind"]))

    runtime=None
    if status:
        runtime=dict(
            state_exists=bool(
                status.get("state_exists",False)),
            active_plan_version=status.get(
                "active_plan_version"),
            source=status.get("source"),
            sharing=status.get("sharing"),
            admission=status.get("admission"),
            physical=status.get("physical"),
            rebuilds=list(
                status.get("rebuilds",()) or ()),
            retirements=list(
                status.get("retirements",()) or ()),
        )

    return dict(
        format_version=1,
        plan_version=int(plan.get("version",0)),
        plan_revision=int(plan.get("revision",0)),
        plan_hash=str(plan.get("plan_hash","")),
        stateless=stateless,
        stateful=stateful,
        macro_count=len(
            plan.get("macros",()) or ()),
        udf_count=len(
            plan.get("udfs",()) or ()),
        runtime=runtime,
    )
