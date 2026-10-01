#!/usr/bin/env python3
"""Persistent SQL control-plane catalog for J4.

The catalog deliberately supports only the stateless relational subset that the
current CDC runtime can execute safely: one MySQL source, deterministic
projection/filter, reusable macros and chains of model views. Stateful JOIN,
aggregation, windowing and multi-source plans are rejected at publish time.
"""

import contextlib
import fcntl
import hashlib
import json
import os
import re
import socket
import sqlite3
import stat
import sys
import tempfile
import threading
import time
import uuid

import duckdb
import sqlglot
from sqlglot import exp


CATALOG_FORMAT = 3
MAX_COMMAND_BYTES = 1024*1024
MAX_SCRIPT_BYTES = 16*1024*1024
MAX_SESSIONS = 8
SESSION_IDLE_SECONDS = 15*60
SESSION_DUCKDB_MEMORY = "32MB"
BOOTSTRAP_VARIABLES = {"CDC_CATALOG_FILE","CDC_CATALOG_SOCKET"}
SECRET_VARIABLES = {"CDC_MYSQL_PASSWORD","CDC_SR_PASSWORD"}
PUBLIC_VARIABLES = {
    "CDC_MYSQL_HOST","CDC_MYSQL_PORT","CDC_MYSQL_USER","CDC_MYSQL_PASSWORD",
    "CDC_MYSQL_SCHEMA","CDC_SR_FE_HOST","CDC_SR_FE_PORT","CDC_SR_QUERY_PORT",
    "CDC_SR_USER","CDC_SR_PASSWORD","CDC_SR_DB","CDC_STATE_FILE",
    "CDC_CATALOG_SEED","CDC_CATALOG_SOCKET","CDC_KEY_PARTITIONS",
    "CDC_WRITE_WORKERS_MIN","CDC_WRITE_WORKERS_INITIAL","CDC_WRITE_WORKERS_MAX",
    "CDC_WRITE_RAMP_SECONDS","CDC_WRITE_MONITOR_SECONDS","CDC_ROWSET_YELLOW",
    "CDC_ROWSET_RED","CDC_VERSION_RECOVERY_CHECKS","CDC_SNAPSHOT_WORKERS",
    "CDC_SNAPSHOT_ROWS","CDC_SNAPSHOT_CHUNK_BYTES","CDC_SNAPSHOT_READ_AHEAD_GROUPS",
    "CDC_SNAPSHOT_BUNDLE_MAX_LANES","CDC_BATCH_ROWS","CDC_BATCH_BYTES",
    "CDC_BATCH_MS","CDC_TXN_ROWS","CDC_TXN_BYTES","CDC_TXN_SPOOL_MAX_BYTES",
    "CDC_COMMIT_INTERVAL_MS","CDC_PRESSURE_MAX_SECONDS","CDC_LOAD_MODE",
    "CDC_MERGE_COMMIT_INTERVAL_MS","CDC_MERGE_COMMIT_PARALLEL",
    "CDC_MAX_ROW_BYTES","CDC_MAX_BACKLOG_BYTES","CDC_MAX_PREPARED_BYTES",
    "CDC_MAX_INFLIGHT_DELIVERIES","CDC_MIN_FREE_BYTES","CDC_FRESHNESS_SECONDS",
    "CDC_QUERY_TIMEOUT","CDC_LOAD_TIMEOUT","CDC_RETRY_MAX","CDC_SERVER_ID",
    "CDC_COMPRESSION","CDC_DUCKDB_MEMORY","CDC_RESOURCE_MONITOR_SECONDS",
    "CDC_STATUS_SECONDS","CDC_IDLE_STATUS_SECONDS","CDC_DETAIL_LOGS","CDC_SHARED_SOURCE_STATE",
    "CDC_NATIVE_BINLOG_PATH","CDC_RUN_SECONDS","CDC_RESOURCE_CPU_CORES",
    "CDC_NATIVE_EVENT_GROUP_EVENTS",
    "CDC_RESOURCE_MEMORY_MB","CDC_RESOURCE_NICE","CDC_RESOURCE_IONICE",
    "CDC_RESOURCE_SHARED_HOST","CDC_PLAN_RETAIN","CDC_METRICS_MAX_BYTES",
    "CDC_OVERFLOW_VALUE_DAYS","CDC_OVERFLOW_METADATA_DAYS",
}
class CatalogBusyError(RuntimeError):
    pass


RESERVED_MACROS = {
    "bloblen","utf8len","truncate_exceed_byte_limit_str",
    "truncate_exceed_byte_limit_json","format_json",
}


def catalog_paths(root_file, state_path=None, variables=None):
    root = os.path.dirname(os.path.abspath(root_file))
    variables = variables or {}

    def setting(name, default=None):
        return variables[name] if name in variables else os.environ.get(name,default)

    state = os.path.abspath(
        state_path or setting(
            "CDC_STATE_FILE",os.path.join(root,".cdc_v2.sqlite3")))

    explicit_catalog = os.environ.get("CDC_CATALOG_FILE")
    default_catalog = os.path.join(root,".cdc_catalog.sqlite3")
    legacy_catalog = state+".catalog.sqlite3"
    if explicit_catalog:
        catalog = os.path.abspath(explicit_catalog)
    elif os.path.exists(default_catalog) or not os.path.exists(legacy_catalog):
        catalog = os.path.abspath(default_catalog)
    else:
        # One-time compatibility with the first SQL-catalog revision.
        catalog = os.path.abspath(legacy_catalog)

    seed_value = setting("CDC_CATALOG_SEED")
    seed = os.path.abspath(seed_value) if seed_value else None
    socket_default = os.path.join(
        os.path.dirname(catalog) or ".",
        ".cdc_catalog_"+hashlib.sha256(catalog.encode()).hexdigest()[:12]+".sock")
    socket_path = os.path.abspath(setting(
        "CDC_CATALOG_SOCKET",socket_default))
    return dict(state=state,catalog=catalog,seed=seed,socket=socket_path)


def catalog_open(path):
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".",exist_ok=True)
    con = sqlite3.connect(path,timeout=30,isolation_level=None)
    with contextlib.suppress(OSError):
        os.chmod(path,0o600)
    con.execute("PRAGMA busy_timeout=30000")
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=FULL")
    con.executescript("""
        CREATE TABLE IF NOT EXISTS meta(
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS objects(
            name TEXT PRIMARY KEY,
            kind TEXT NOT NULL CHECK(kind IN ('macro','view','sink')),
            sql_text TEXT NOT NULL,
            primary_key_json TEXT,
            revision INTEGER NOT NULL,
            updated REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS plans(
            version INTEGER PRIMARY KEY,
            revision INTEGER NOT NULL,
            plan_hash TEXT NOT NULL UNIQUE,
            mappings_json TEXT NOT NULL,
            macros_json TEXT NOT NULL,
            created REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS variables(
            name TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS arrow_udfs(
            name TEXT PRIMARY KEY,
            function_name TEXT NOT NULL,
            parameters_json TEXT NOT NULL,
            return_type TEXT NOT NULL,
            source TEXT NOT NULL,
            source_sha256 TEXT NOT NULL,
            filename TEXT NOT NULL,
            revision INTEGER NOT NULL,
            updated REAL NOT NULL);
    """)
    plan_columns = {
        row[1] for row in con.execute("PRAGMA table_info(plans)").fetchall()}
    if "udfs_json" not in plan_columns:
        con.execute(
            "ALTER TABLE plans ADD COLUMN udfs_json TEXT NOT NULL DEFAULT '[]'")
    if "stateful_tasks_json" not in plan_columns:
        con.execute(
            "ALTER TABLE plans ADD COLUMN stateful_tasks_json TEXT NOT NULL DEFAULT '[]'")
    current = _meta_get(con,"format")
    if current is None:
        _meta_set(con,"format",str(CATALOG_FORMAT))
        _meta_set(con,"draft_revision","0")
        _meta_set(con,"published_version","0")
        _meta_set(con,"config_revision","0")
    elif int(current) in (1,2):
        # Format 3 persists stateful task manifests in plan identity. Older
        # binaries must fail closed rather than silently drop those tasks.
        _meta_set(con,"format",str(CATALOG_FORMAT))
    elif int(current) != CATALOG_FORMAT:
        con.close()
        raise RuntimeError(
            f"unsupported CDC catalog format {current}; expected {CATALOG_FORMAT}")
    return con


def _meta_get(con, key, default=None):
    row = con.execute("SELECT value FROM meta WHERE key=?",(key,)).fetchone()
    return row[0] if row else default


def _meta_set(con, key, value):
    con.execute("""
        INSERT INTO meta(key,value) VALUES(?,?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
    """,(key,str(value)))


def _revision_next(con):
    value = int(_meta_get(con,"draft_revision","0"))+1
    _meta_set(con,"draft_revision",value)
    return value


def config_revision(path):
    if not os.path.exists(path):
        return 0
    con = catalog_open(path)
    try:
        return int(_meta_get(con,"config_revision","0"))
    finally:
        con.close()


def _variables_from_con(con):
    return {
        str(name):str(value)
        for name,value in con.execute(
            "SELECT name,value FROM variables ORDER BY name")
    }


def variables_get(path):
    if not os.path.exists(path):
        return {}
    con = catalog_open(path)
    try:
        return _variables_from_con(con)
    finally:
        con.close()


CONNECTION_VARIABLES = {
    "CDC_MYSQL_HOST":("mysql","host",str),
    "CDC_MYSQL_PORT":("mysql","port",int),
    "CDC_MYSQL_USER":("mysql","user",str),
    "CDC_MYSQL_PASSWORD":("mysql","password",str),
    "CDC_MYSQL_SCHEMA":("mysql","database",str),
    "CDC_SR_FE_HOST":("starrocks","host",str),
    "CDC_SR_FE_PORT":("starrocks","http_port",int),
    "CDC_SR_QUERY_PORT":("starrocks","port",int),
    "CDC_SR_USER":("starrocks","user",str),
    "CDC_SR_PASSWORD":("starrocks","password",str),
    "CDC_SR_DB":("starrocks","database",str),
    "CDC_SERVER_ID":("runtime","server_id",int),
}


def _connection_variable_value(name, value):
    group,key,cast = CONNECTION_VARIABLES[name]
    try:
        value = cast(value)
    except (TypeError,ValueError) as exc:
        raise ValueError(f"{name} has an invalid value: {value!r}") from exc
    if key in ("port","http_port") and not 1 <= int(value) <= 65535:
        raise ValueError(f"{name} must be between 1 and 65535")
    if key == "server_id" and not 1 <= int(value) <= 2**32-1:
        raise ValueError(f"{name} must be between 1 and {2**32-1}")
    return group,key,value


def connection_settings_values(values, require=True):
    values = values or {}
    missing = [name for name in CONNECTION_VARIABLES
               if name not in values or values[name] is None
               or (not values[name] and name not in SECRET_VARIABLES)]
    if missing:
        if not require:
            return None
        raise ValueError(
            "database connection configuration is incomplete in the persistent "
            "catalog; configure it with 'python j4.py cli' or "
            "'python j4.py sql <file.sql>': "+", ".join(missing))
    result = {"mysql":{},"starrocks":{},"runtime":{}}
    for name in CONNECTION_VARIABLES:
        group,key,value = _connection_variable_value(name,values[name])
        result[group][key] = value
    return result


def connection_settings(path, require=True):
    return connection_settings_values(variables_get(path),require=require)


def connection_configured(path):
    return connection_settings(path,require=False) is not None


def _variable_name(name):
    name = str(name).strip().upper()
    if not re.fullmatch(r"CDC_[A-Z0-9_]+",name):
        raise ValueError("CDC variable name must match CDC_[A-Z0-9_]+")
    if name.startswith("CDC_RELEASE_") or name.startswith("CDC_TEST_"):
        raise ValueError(f"{name} is internal/test-only and cannot be persisted")
    if name in BOOTSTRAP_VARIABLES:
        raise ValueError(
            f"{name} is a bootstrap locator; set it in the process environment")
    if name not in PUBLIC_VARIABLES:
        raise ValueError(
            f"unknown public CDC variable: {name}; refusing a silent typo")
    return name


def _config_revision_next(con):
    value = int(_meta_get(con,"config_revision","0"))+1
    _meta_set(con,"config_revision",value)
    return value


def _duckdb_variable_value(command, name):
    con = duckdb.connect(":memory:")
    try:
        con.execute(command)
        safe_name = name.replace("'","''")
        value = con.execute(
            "SELECT getvariable('"+safe_name+"')").fetchone()[0]
    finally:
        con.close()
    if value is None:
        raise ValueError("SET VARIABLE value cannot be NULL; use RESET VARIABLE")
    if isinstance(value,bool):
        return "true" if value else "false"
    if isinstance(value,(bytes,bytearray,memoryview,list,tuple,dict,set)):
        raise ValueError("CDC variables must be scalar values")
    return str(value)


def _set_variable(con, command):
    match = re.fullmatch(
        r"(?is)SET\s+VARIABLE\s+(CDC_[A-Za-z0-9_]+)\s*=\s*(.+)",
        command)
    if not match:
        return None
    name = _variable_name(match.group(1))
    value = _duckdb_variable_value(command,name)
    if name in CONNECTION_VARIABLES:
        _connection_variable_value(name,value)
    revision = _config_revision_next(con)
    con.execute("""
        INSERT INTO variables(name,value,updated) VALUES(?,?,?)
        ON CONFLICT(name) DO UPDATE SET
            value=excluded.value,updated=excluded.updated
    """,(name,value,time.time()))
    return dict(
        status="configured",name=name,config_revision=revision,
        restart_required=True)


def _reset_variable(con, command):
    match = re.fullmatch(
        r"(?is)RESET\s+VARIABLE\s+(CDC_[A-Za-z0-9_]+)",
        command)
    if not match:
        return None
    name = _variable_name(match.group(1))
    existed = con.execute(
        "SELECT 1 FROM variables WHERE name=?",(name,)).fetchone() is not None
    revision = _config_revision_next(con)
    con.execute("DELETE FROM variables WHERE name=?",(name,))
    return dict(
        status="configured",name=name,deleted=existed,
        config_revision=revision,restart_required=True)


def _select_variable(con, command):
    match = re.fullmatch(
        r"(?is)SELECT\s+getvariable\s*\(\s*'((?:''|[^'])+)'\s*\)"
        r"(?:\s+AS\s+[A-Za-z_][A-Za-z0-9_$]*)?",
        command)
    if not match:
        return None
    name = _variable_name(match.group(1).replace("''","'"))
    row = con.execute(
        "SELECT value FROM variables WHERE name=?",(name,)).fetchone()
    if row:
        value,source = row[0],"catalog"
    elif name in os.environ:
        value,source = os.environ[name],"environment"
    else:
        value,source = None,"default"
    return dict(status="ok",name=name,value=value,source=source)


def _transaction(con):
    con.execute("BEGIN IMMEDIATE")


def _commit(con):
    con.execute("COMMIT")


def _rollback(con):
    con.execute("ROLLBACK")


def split_commands(text):
    commands = []
    current = []
    quote = None
    index = 0
    while index < len(text):
        ch = text[index]
        nxt = text[index+1] if index+1 < len(text) else ""
        if quote:
            current.append(ch)
            if ch == quote:
                if nxt == quote:
                    current.append(nxt)
                    index += 2
                    continue
                quote = None
            elif ch == "\\" and quote in ("'",'"') and nxt:
                current.append(nxt)
                index += 2
                continue
            index += 1
            continue
        if ch in ("'",'"',"`"):
            quote = ch
            current.append(ch)
            index += 1
            continue
        if ch == "-" and nxt == "-":
            index += 2
            while index < len(text) and text[index] not in "\r\n":
                index += 1
            continue
        if ch == "/" and nxt == "*":
            end = text.find("*/",index+2)
            if end < 0:
                raise ValueError("unterminated SQL block comment")
            index = end+2
            continue
        if ch == ";":
            command = "".join(current).strip()
            if command:
                commands.append(command)
            current = []
            index += 1
            continue
        current.append(ch)
        index += 1
    tail = "".join(current).strip()
    if tail:
        commands.append(tail)
    return commands


def _canonical_name(schema, name):
    schema = str(schema).strip().lower()
    name = str(name).strip()
    if schema not in ("model","starrocks"):
        raise ValueError("catalog object schema must be model or starrocks")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*",name):
        raise ValueError(f"unsupported catalog identifier: {name!r}")
    return schema+"."+name.lower()


def _query_tree(sql):
    tree = _select_tree(sql)
    forbidden = (
        exp.Join,exp.Subquery,exp.Union,exp.AggFunc,exp.Window,exp.Group,
        exp.Having,exp.Limit,exp.Offset,exp.Distinct,exp.With,
        exp.Unnest,exp.Explode,
    )
    if any(isinstance(node,forbidden) for node in tree.walk()):
        raise ValueError(
            "catalog phase 1 supports one-source deterministic projection/filter "
            "only; JOIN/subquery/aggregate/window/set operations are stateful or unsupported")
    tables = list(tree.find_all(exp.Table))
    if len(tables) != 1:
        raise ValueError("catalog phase 1 query must read exactly one relation")
    return tree,tables[0]


def _select_tree(sql):
    try:
        tree=sqlglot.parse_one(sql,read="duckdb")
    except Exception as exc:
        raise ValueError(f"invalid DuckDB SQL: {exc}") from exc
    if not isinstance(tree,exp.Select):
        raise ValueError("catalog models must be SELECT queries")
    if re.search(
        r"\b(random|uuid|now|current_timestamp|current_date|current_time|"
        r"read_\w+|query|unnest|explode|generate_series|json_each|"
        r"regexp_split_to_table)\b",
        tree.sql(dialect="duckdb"),re.I):
        raise ValueError(
            "catalog query contains a non-deterministic or external function")
    return tree


def _stateful_sink_manifest(name,sql,primary_key=None):
    tree=_select_tree(sql)
    has_join=any(isinstance(node,exp.Join) for node in tree.walk())
    has_agg=any(
        isinstance(node,(exp.AggFunc,exp.Group))
        for node in tree.walk())
    if not has_join and not has_agg:
        return None
    forbidden=(
        exp.Subquery,exp.Union,exp.Window,exp.Having,exp.Limit,
        exp.Offset,exp.Distinct,exp.With,exp.Unnest,exp.Explode,
    )
    if any(isinstance(node,forbidden) for node in tree.walk()):
        raise ValueError(
            "stateful catalog v1 rejects subquery/window/HAVING/set/"
            "DISTINCT/limit/CTE/table-function operations")
    if has_join and has_agg:
        raise ValueError(
            "stateful catalog v1 does not combine JOIN and aggregation")
    tables=list(tree.find_all(exp.Table))
    sources=[]
    for table in tables:
        db=str(table.db or "").strip().lower()
        relation=str(table.name or "").strip()
        if db!="mysql" or not relation:
            raise ValueError(
                "stateful catalog v1 requires direct mysql.<table> sources")
        sources.append(relation)
    if has_join:
        joins=list(tree.find_all(exp.Join))
        if len(tables)!=2 or len(joins)!=1:
            raise ValueError(
                "stateful INNER JOIN v1 requires exactly two MySQL tables")
        join=joins[0]
        side=str(join.args.get("side") or "").strip().upper()
        kind=str(join.args.get("kind") or "").strip().upper()
        if side or kind not in {"","INNER"}:
            raise ValueError(
                "stateful catalog v1 supports INNER JOIN only")
        if primary_key:
            raise ValueError(
                "stateful INNER JOIN target identity is managed internally; "
                "do not declare a catalog PRIMARY KEY")
        task_kind="inner_join"
    else:
        if len(tables)!=1:
            raise ValueError(
                "stateful aggregate v1 requires exactly one MySQL source")
        task_kind="aggregate"
    return dict(
        kind=task_kind,
        sink=str(name),
        target_table=str(name).split(".",1)[1],
        sql=tree.sql(dialect="duckdb"),
        source_relations=sources,
        primary_key=list(primary_key or ()),
    )


def _relation_name(table):
    db = str(table.db or "").strip().lower()
    name = str(table.name or "").strip()
    if db not in ("mysql","model"):
        raise ValueError(
            "FROM must reference mysql.<table> or model.<view> in catalog phase 1")
    if not name:
        raise ValueError("empty relation name")
    return db,name


def _projection_names(tree):
    names = []
    for item in tree.expressions:
        if isinstance(item,exp.Star):
            return None
        name = str(item.alias_or_name or "")
        if not name:
            raise ValueError(
                "every catalog projection expression must have an output name")
        if name.startswith("_sync_"):
            raise ValueError("_sync_* names are reserved for CDC metadata")
        names.append(name)
    if len(names) != len(set(names)):
        raise ValueError("duplicate output columns in catalog query")
    return names


def _expand_model_star(tree, child_outputs):
    if not any(isinstance(item,exp.Star) for item in tree.expressions):
        return
    if child_outputs is None:
        raise ValueError(
            "SELECT * over a model view whose output is not statically enumerable "
            "is unsupported; list the columns explicitly")
    expanded = []
    for item in tree.expressions:
        if isinstance(item,exp.Star):
            if any(item.args.values()):
                raise ValueError("star modifiers are unsupported")
            expanded.extend(exp.column(name,quoted=True) for name in child_outputs)
        else:
            expanded.append(item)
    tree.set("expressions",expanded)


def _object_get(con, name, kind=None):
    row = con.execute(
        "SELECT name,kind,sql_text,primary_key_json,revision FROM objects WHERE name=?",
        (name,)).fetchone()
    if not row:
        raise ValueError(f"catalog object not found: {name}")
    if kind and row[1] != kind:
        raise ValueError(f"catalog object {name} is {row[1]}, expected {kind}")
    return row


def _resolve_query(con, sql, stack=(), hidden=False):
    tree,table = _query_tree(sql)
    db,name = _relation_name(table)
    if db == "mysql":
        source = name
        table.set("this",exp.to_identifier("arrow_batch"))
        table.set("db",None)
        table.set("catalog",None)
    else:
        object_name = _canonical_name("model",name)
        if object_name in stack:
            raise ValueError(
                "catalog view dependency cycle: "+" -> ".join(stack+(object_name,)))
        child = _object_get(con,object_name,"view")
        child_tree,source,child_outputs = _resolve_query(
            con,child[2],stack+(object_name,),hidden=True)
        _expand_model_star(tree,child_outputs)
        alias = table.args.get("alias")
        if alias is None:
            alias = exp.TableAlias(this=exp.to_identifier(name))
        else:
            alias = alias.copy()
        table.replace(exp.Subquery(this=child_tree,alias=alias))
    outputs = _projection_names(tree)
    if hidden:
        if outputs is not None and (
            "_sync_op" in outputs or "_sync_order" in outputs):
            raise ValueError("_sync_op/_sync_order are reserved")
        tree.select(
            exp.column("_sync_op"),exp.column("_sync_order"),copy=False)
    return tree,source,outputs


def compile_draft(con):
    macros = [
        row[0] for row in con.execute(
            "SELECT sql_text FROM objects WHERE kind='macro' ORDER BY revision,name")]
    udfs = [
        dict(
            name=row[0],function=row[1],parameters=json.loads(row[2]),
            return_type=row[3],source=row[4],source_sha256=row[5],
            filename=row[6])
        for row in con.execute("""
            SELECT name,function_name,parameters_json,return_type,
                   source,source_sha256,filename
            FROM arrow_udfs ORDER BY revision,name
        """)]
    sinks = con.execute("""
        SELECT name,sql_text,primary_key_json
        FROM objects WHERE kind='sink' ORDER BY name
    """).fetchall()
    mappings = []
    stateful_tasks = []
    for name,sql_text,primary_key_json in sinks:
        keys = json.loads(primary_key_json) if primary_key_json else None
        if keys == []:
            raise ValueError(f"{name}: PRIMARY KEY is empty")
        stateful=_stateful_sink_manifest(name,sql_text,keys)
        if stateful is not None:
            stateful_tasks.append(stateful)
            continue
        tree,source,_ = _resolve_query(con,sql_text,hidden=False)
        for key in keys or ():
            if not any(
                isinstance(item,exp.Column) and item.name == key
                for item in tree.expressions
            ):
                raise ValueError(
                    f"{name}: primary key {key!r} must be projected unchanged")
        target = name.split(".",1)[1]
        mappings.append(dict(
            src_table=source,
            sr_table=target,
            primary_key=(keys[0] if len(keys) == 1 else keys) if keys else None,
            full_filter=None,
            sql=tree.sql(dialect="duckdb"),
            _catalog_compiled=True,
            _catalog_sink=name,
        ))
    payload = dict(
        mappings=mappings,stateful_tasks=stateful_tasks,
        macros=macros,udfs=udfs)
    body = json.dumps(
        payload,sort_keys=True,separators=(",",":"),ensure_ascii=False)
    return (
        mappings,stateful_tasks,macros,udfs,
        hashlib.sha256(body.encode()).hexdigest())


def _publish_candidate(con):
    mappings,stateful_tasks,macros,udfs,plan_hash = compile_draft(con)
    existing = con.execute(
        "SELECT version,revision FROM plans WHERE plan_hash=?",
        (plan_hash,)).fetchone()
    current = int(_meta_get(con,"published_version","0"))
    if existing:
        version,revision = int(existing[0]),int(existing[1])
        is_new = False
    else:
        version = int(con.execute(
            "SELECT COALESCE(MAX(version),0)+1 FROM plans").fetchone()[0])
        revision = int(_meta_get(con,"draft_revision","0"))
        is_new = True
    return dict(
        status="candidate",version=version,revision=revision,
        config_revision=int(_meta_get(con,"config_revision","0")),
        plan_hash=plan_hash,changed=current != version,is_new=is_new,
        mappings=mappings,stateful_tasks=stateful_tasks,
        macros=macros,udfs=udfs,
        _variables=_variables_from_con(con))


def _published_config_candidate(con):
    version = int(_meta_get(con,"published_version","0"))
    if version:
        row = con.execute("""
            SELECT revision,plan_hash,mappings_json,macros_json,udfs_json,
                   stateful_tasks_json
            FROM plans WHERE version=?
        """,(version,)).fetchone()
        if not row:
            raise RuntimeError(
                f"published catalog plan version {version} is missing")
        revision,plan_hash,mappings_json,macros_json,udfs_json,stateful_json = row
        mappings = json.loads(mappings_json)
        macros = json.loads(macros_json)
        udfs = json.loads(udfs_json)
        stateful_tasks=json.loads(stateful_json)
    else:
        revision,plan_hash,mappings,macros,udfs,stateful_tasks = (
            0,"",[],[],[],[])
    return dict(
        status="config_candidate",version=version,revision=int(revision),
        config_revision=int(_meta_get(con,"config_revision","0")),
        plan_hash=str(plan_hash),changed=False,is_new=False,
        mappings=mappings,stateful_tasks=stateful_tasks,
        macros=macros,udfs=udfs,
        _variables=_variables_from_con(con))


def _publish_persist(con, candidate):
    if candidate.get("is_new"):
        con.execute("""
            INSERT INTO plans(
                version,revision,plan_hash,mappings_json,macros_json,udfs_json,
                stateful_tasks_json,created)
            VALUES(?,?,?,?,?,?,?,?)
        """,(
            int(candidate["version"]),int(candidate["revision"]),
            candidate["plan_hash"],
            json.dumps(
                candidate["mappings"],separators=(",",":"),ensure_ascii=False),
            json.dumps(
                candidate["macros"],separators=(",",":"),ensure_ascii=False),
            json.dumps(
                candidate["udfs"],separators=(",",":"),ensure_ascii=False),
            json.dumps(
                candidate.get("stateful_tasks",()),
                separators=(",",":"),ensure_ascii=False),
            time.time(),
        ))
    _meta_set(con,"published_version",int(candidate["version"]))
    return dict(
        status="published",version=int(candidate["version"]),
        revision=int(candidate["revision"]),plan_hash=candidate["plan_hash"],
        changed=bool(candidate.get("changed")),
        mappings=candidate["mappings"],
        stateful_tasks=list(candidate.get("stateful_tasks",())),
        macros=candidate["macros"],udfs=candidate["udfs"])


def publish(path, publish_callback=None):
    """Validate a candidate before moving the durable published pointer."""
    con = catalog_open(path)
    validation = None
    result = None
    try:
        _transaction(con)
        candidate = _publish_candidate(con)
        if publish_callback is not None:
            validation = publish_callback(candidate,"validate")
        result = _publish_persist(con,candidate)
        _commit(con)
    except BaseException:
        with contextlib.suppress(sqlite3.Error):
            _rollback(con)
        raise
    finally:
        con.close()
    if validation is not None:
        result["validation"] = validation
    if publish_callback is not None:
        try:
            result["activation"] = publish_callback(result,"install")
        except Exception as exc:
            result["activation"] = dict(
                status="restart_required",
                reason="plan was published but live installation failed: "+str(exc))
    return result


def prune_plans(path, keep_versions=(), keep_recent=32):
    con = catalog_open(path)
    try:
        published = int(_meta_get(con,"published_version","0"))
        keep = {int(v) for v in keep_versions if int(v) > 0}
        if published:
            keep.add(published)
        keep.update(
            int(row[0]) for row in con.execute(
                "SELECT version FROM plans ORDER BY version DESC LIMIT ?",
                (max(1,int(keep_recent)),)).fetchall())
        if not keep:
            return 0
        marks = ",".join("?" for _ in keep)
        with con:
            before = int(con.execute("SELECT COUNT(*) FROM plans").fetchone()[0])
            con.execute(
                f"DELETE FROM plans WHERE version NOT IN ({marks})",
                tuple(sorted(keep)))
            after = int(con.execute("SELECT COUNT(*) FROM plans").fetchone()[0])
        return before-after
    finally:
        con.close()


def load_plan_version(path, version):
    con = catalog_open(path)
    try:
        row = con.execute("""
            SELECT revision,plan_hash,mappings_json,macros_json,udfs_json,
                   stateful_tasks_json
            FROM plans WHERE version=?
        """,(int(version),)).fetchone()
        if not row:
            raise RuntimeError(f"catalog plan version {version} is missing")
        return dict(
            version=int(version),revision=int(row[0]),plan_hash=row[1],
            mappings=json.loads(row[2]),macros=json.loads(row[3]),
            udfs=json.loads(row[4]),stateful_tasks=json.loads(row[5]))
    finally:
        con.close()


def load_plan(path, seed_path=None):
    ensure_seed(path,seed_path)
    con = catalog_open(path)
    try:
        version = int(_meta_get(con,"published_version","0"))
    finally:
        con.close()
    if not version:
        raise RuntimeError(
            "catalog has no published plan; configure it with 'python j4.py cli' "
            "or 'python j4.py sql <file.sql>'")
    return load_plan_version(path,version)


def _put_object(con, name, kind, sql_text, replace=False):
    existing = con.execute(
        "SELECT kind FROM objects WHERE name=?",(name,)).fetchone()
    if existing and not replace:
        raise ValueError(
            f"catalog object already exists: {name}; use CREATE OR REPLACE")
    if existing and existing[0] != kind:
        raise ValueError(
            f"catalog object {name} exists as {existing[0]}, not {kind}")
    revision = _revision_next(con)
    old_pk = con.execute(
        "SELECT primary_key_json FROM objects WHERE name=?",(name,)).fetchone()
    primary = old_pk[0] if old_pk and kind == "sink" else None
    con.execute("""
        INSERT INTO objects(
            name,kind,sql_text,primary_key_json,revision,updated)
        VALUES(?,?,?,?,?,?)
        ON CONFLICT(name) DO UPDATE SET
            sql_text=excluded.sql_text,
            revision=excluded.revision,
            updated=excluded.updated
    """,(name,kind,sql_text.strip(),primary,revision,time.time()))
    return revision


def _create_macro(con, command):
    match = re.match(
        r"(?is)^CREATE\s+(OR\s+REPLACE\s+)?MACRO\s+"
        r"([A-Za-z_][A-Za-z0-9_$]*)\s*\(",
        command)
    if not match:
        return None
    name = match.group(2).lower()
    if name in RESERVED_MACROS:
        raise ValueError(
            f"macro {name} is reserved by the CDC runtime")
    revision = _put_object(
        con,"macro."+name,"macro",command,replace=bool(match.group(1)))
    return dict(status="draft",kind="macro",name=name,revision=revision)


def _create_model_or_sink(con, command):
    match = re.match(
        r"(?is)^CREATE\s+(OR\s+REPLACE\s+)?(VIEW|TABLE)\s+"
        r"(model|starrocks)\.([A-Za-z_][A-Za-z0-9_$]*)\s+AS\s+(.+)$",
        command)
    if not match:
        return None
    replace,object_type,schema,name,query = match.groups()
    schema = schema.lower()
    object_type = object_type.lower()
    if object_type == "view" and schema != "model":
        raise ValueError("CREATE VIEW is supported only in model.*")
    if object_type == "table" and schema != "starrocks":
        raise ValueError("CREATE TABLE AS is supported only in starrocks.*")
    canonical = _canonical_name(schema,name)
    if object_type=="view":
        _query_tree(query)
    else:
        tree=_select_tree(query)
        stateful=_stateful_sink_manifest(canonical,query,None)
        if stateful is None:
            _query_tree(query)
    revision = _put_object(
        con,canonical,"view" if schema == "model" else "sink",
        query,replace=bool(replace))
    return dict(
        status="draft",kind="view" if schema == "model" else "sink",
        name=canonical,revision=revision)


def _alter_primary_key(con, command):
    match = re.match(
        r"(?is)^ALTER\s+TABLE\s+starrocks\."
        r"([A-Za-z_][A-Za-z0-9_$]*)\s+ADD\s+PRIMARY\s+KEY\s*"
        r"\(([^)]+)\)\s*$",
        command)
    if not match:
        return None
    name = _canonical_name("starrocks",match.group(1))
    _object_get(con,name,"sink")
    keys = [part.strip().strip('"`') for part in match.group(2).split(",")]
    if not keys or any(
            not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*",key)
            for key in keys):
        raise ValueError("PRIMARY KEY contains an unsupported identifier")
    revision = _revision_next(con)
    con.execute("""
        UPDATE objects SET primary_key_json=?,revision=?,updated=?
        WHERE name=?
    """,(json.dumps(keys,separators=(",",":")),revision,time.time(),name))
    return dict(
        status="draft",kind="primary_key",name=name,
        primary_key=keys,revision=revision)


def _arrow_udf_call(command):
    match = re.fullmatch(
        r"(?is)CALL\s+cdc_create_arrow_udf\s*\(\s*"
        r"'([^']+)'\s*,\s*'([^']+)'\s*,\s*'([^']+)'\s*,\s*"
        r"'([^']*)'\s*,\s*'([^']+)'"
        r"(?:\s*,\s*'([^']+)')?\s*\)",
        command)
    if not match:
        return None
    name,path,function,parameters,return_type,scope = match.groups()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*",name):
        raise ValueError("Arrow UDF name is invalid")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*",function):
        raise ValueError("Arrow UDF function name is invalid")
    parameters = [
        item.strip() for item in parameters.split(",") if item.strip()]
    return dict(
        name=name,path=os.path.abspath(path),function=function,
        parameters=parameters,return_type=return_type.strip(),
        scope=(scope or "GLOBAL").strip().upper())


def _arrow_udf_source(spec):
    if not os.path.isfile(spec["path"]):
        raise ValueError(f"Arrow UDF source file not found: {spec['path']}")
    if os.path.getsize(spec["path"]) > 1024*1024:
        raise ValueError("Arrow UDF source file exceeds 1MiB")
    source = open(spec["path"],"r",encoding="utf-8").read()
    compile(source,spec["path"],"exec")
    return source,hashlib.sha256(source.encode()).hexdigest()


def _create_arrow_udf(con, command):
    spec = _arrow_udf_call(command)
    if spec is None:
        return None
    if spec["scope"] != "GLOBAL":
        return dict(status="session_udf",spec=spec)
    source,digest = _arrow_udf_source(spec)
    revision = _revision_next(con)
    con.execute("""
        INSERT INTO arrow_udfs(
            name,function_name,parameters_json,return_type,source,
            source_sha256,filename,revision,updated)
        VALUES(?,?,?,?,?,?,?,?,?)
        ON CONFLICT(name) DO UPDATE SET
            function_name=excluded.function_name,
            parameters_json=excluded.parameters_json,
            return_type=excluded.return_type,
            source=excluded.source,
            source_sha256=excluded.source_sha256,
            filename=excluded.filename,
            revision=excluded.revision,
            updated=excluded.updated
    """,(
        spec["name"],spec["function"],
        json.dumps(spec["parameters"],separators=(",",":")),
        spec["return_type"],source,digest,os.path.basename(spec["path"]),
        revision,time.time()))
    return dict(
        status="draft",kind="arrow_udf",name=spec["name"],
        scope="GLOBAL",source_sha256=digest,revision=revision)


def _drop_arrow_udf(con, command):
    match = re.fullmatch(
        r"(?is)CALL\s+cdc_drop_arrow_udf\s*\(\s*'([^']+)'"
        r"(?:\s*,\s*'([^']+)')?\s*\)",
        command)
    if not match:
        return None
    name,scope = match.groups()
    scope = (scope or "GLOBAL").upper()
    if scope != "GLOBAL":
        return dict(status="session_udf_drop",name=name,scope=scope)
    revision = _revision_next(con)
    existed = con.execute(
        "SELECT 1 FROM arrow_udfs WHERE name=?",(name,)).fetchone() is not None
    con.execute("DELETE FROM arrow_udfs WHERE name=?",(name,))
    return dict(
        status="draft",kind="arrow_udf",name=name,scope="GLOBAL",
        dropped=existed,revision=revision)


def _drop_object(con, command):
    match = re.match(
        r"(?is)^DROP\s+(VIEW|TABLE|MACRO)\s+(IF\s+EXISTS\s+)?"
        r"(?:(model|starrocks)\.)?([A-Za-z_][A-Za-z0-9_$]*)\s*$",
        command)
    if not match:
        return None
    kind,if_exists,schema,name = match.groups()
    kind = kind.lower()
    if kind == "macro":
        canonical = "macro."+name.lower()
    else:
        expected = "model" if kind == "view" else "starrocks"
        if (schema or "").lower() != expected:
            raise ValueError(
                f"DROP {kind.upper()} requires {expected}.*")
        canonical = _canonical_name(expected,name)
    row = con.execute(
        "SELECT 1 FROM objects WHERE name=?",(canonical,)).fetchone()
    if not row:
        if if_exists:
            return dict(status="draft",dropped=False,name=canonical)
        raise ValueError(f"catalog object not found: {canonical}")
    revision = _revision_next(con)
    con.execute("DELETE FROM objects WHERE name=?",(canonical,))
    return dict(
        status="draft",kind=kind,dropped=True,name=canonical,revision=revision)


def _show(con, command):
    if re.fullmatch(
            r"(?is)(?:SHOW\s+CDC\s+VARIABLES|CALL\s+cdc_variables\s*\(\s*\))",
            command):
        rows = con.execute(
            "SELECT name,value,updated FROM variables ORDER BY name").fetchall()
        return dict(status="ok",variables=[
            dict(
                name=name,
                value="******" if name in SECRET_VARIABLES else value,
                environment_fallback=name in os.environ,
                updated=float(updated),
            )
            for name,value,updated in rows
        ])
    if re.fullmatch(
            r"(?is)(?:SHOW\s+OBJECTS|CALL\s+cdc_objects\s*\(\s*\))",
            command):
        rows = con.execute("""
            SELECT name,kind,primary_key_json,revision,updated
            FROM objects ORDER BY kind,name
        """).fetchall()
        return dict(status="ok",objects=[
            dict(
                name=row[0],kind=row[1],
                primary_key=json.loads(row[2]) if row[2] else None,
                revision=int(row[3]),updated=float(row[4]))
            for row in rows])
    match = re.fullmatch(
        r"(?is)SHOW\s+CREATE\s+"
        r"((?:model|starrocks)\.[A-Za-z_][A-Za-z0-9_$]*|"
        r"macro\.[A-Za-z_][A-Za-z0-9_$]*)",
        command)
    call_create = re.fullmatch(
        r"(?is)CALL\s+cdc_create\s*\(\s*"
        r"'((?:model|starrocks)\.[A-Za-z_][A-Za-z0-9_$]*|"
        r"macro\.[A-Za-z_][A-Za-z0-9_$]*)'\s*\)",
        command)
    if match or call_create:
        object_name = (match or call_create).group(1).lower()
        row = _object_get(con,object_name)
        return dict(
            status="ok",name=row[0],kind=row[1],sql=row[2],
            primary_key=json.loads(row[3]) if row[3] else None,
            revision=int(row[4]))
    if re.fullmatch(r"(?is)(?:SHOW\s+PLAN|CALL\s+cdc_plan\s*\(\s*\))",command):
        version = int(_meta_get(con,"published_version","0"))
        if not version:
            return dict(
                status="ok",published_version=0,
                draft_revision=int(_meta_get(con,"draft_revision","0")),
                plan=None)
        row = con.execute("""
            SELECT revision,plan_hash,mappings_json,macros_json,udfs_json,
                   stateful_tasks_json,created
            FROM plans WHERE version=?
        """,(version,)).fetchone()
        return dict(
            status="ok",published_version=version,
            draft_revision=int(_meta_get(con,"draft_revision","0")),
            plan=dict(
                revision=int(row[0]),plan_hash=row[1],
                mappings=json.loads(row[2]),macros=json.loads(row[3]),
                udfs=json.loads(row[4]),
                stateful_tasks=json.loads(row[5]),
                created=float(row[6])))
    return None


def help_response():
    return dict(status="ok",commands=[
        "CALL cdc_help(); | CALL cdc_session();",
        "CREATE TEMP MACRO name(...) AS ...;  -- session only, immediate",
        "CREATE [OR REPLACE] MACRO name(...) AS ...;",
        "CREATE [OR REPLACE] VIEW model.name AS SELECT ... FROM mysql.table|model.view;",
        "CREATE [OR REPLACE] TABLE starrocks.name AS SELECT ... FROM mysql.table|model.view;",
        "ALTER TABLE starrocks.name ADD PRIMARY KEY (col[, ...]);  -- optional override; source PK is inferred",
        "DROP VIEW model.name; | DROP TABLE starrocks.name; | DROP MACRO name;",
        "SET VARIABLE CDC_NAME = value; | RESET VARIABLE CDC_NAME;",
        "SELECT getvariable('CDC_NAME');",
        "CALL cdc_variables(); | CALL cdc_objects(); | CALL cdc_create('model.name');",
        "CALL cdc_create_arrow_udf('name','file.py','function','VARCHAR','VARCHAR'[, 'SESSION']);",
        "CALL cdc_drop_arrow_udf('name'[, 'SESSION']);",
        "CALL cdc_plan();",
        "CALL cdc_publish();",
        "PUBLISH;  -- compatibility alias",
        "HELP;",
    ],limits=[
        "phase 1: one MySQL source per sink",
        "projection/filter/macros/view chains only",
        "JOIN/subquery/aggregate/window/set operations are rejected",
        "SET/RESET VARIABLE is persistent; catalog values are the normal runtime source of truth",
        "Arrow UDFs are persistent by default; explicit SESSION is temporary",
        "CDC_CATALOG_FILE is bootstrap-only and cannot be stored inside itself",
        "source PRIMARY KEY is inferred automatically unless explicitly overridden",
        "published plans are eligible for transaction-boundary hot activation by a running daemon",
    ])


def session_new(path=None, memory_limit=SESSION_DUCKDB_MEMORY):
    session = dict(
        id=uuid.uuid4().hex,
        engine=duckdb.connect(
            ":memory:",config={"threads":1,"memory_limit":str(memory_limit)}),
        temp_macros=set(),
        temp_udfs=set(),
        global_udfs=set(),
        updated=time.time())
    if path and os.path.exists(path):
        con = catalog_open(path)
        try:
            for sql_text, in con.execute(
                    "SELECT sql_text FROM objects "
                    "WHERE kind='macro' ORDER BY revision,name"):
                session["engine"].execute(sql_text)
            udf_rows = con.execute("""
                SELECT name,function_name,parameters_json,return_type,
                       source,source_sha256,filename
                FROM arrow_udfs ORDER BY revision,name
            """).fetchall()
        finally:
            con.close()
        for row in udf_rows:
            _register_session_udf(
                session,dict(
                    name=row[0],function=row[1],parameters=json.loads(row[2]),
                    return_type=row[3],source=row[4],
                    source_sha256=row[5],filename=row[6]))
            session["global_udfs"].add(row[0])
    return session


def reap_sessions(sessions, now=None):
    now = time.time() if now is None else float(now)
    stale = [
        session_id for session_id,session in sessions.items()
        if now-float(session.get("updated",now)) >= SESSION_IDLE_SECONDS
    ]
    for session_id in stale:
        session_close(sessions.pop(session_id,None))
    return len(stale)


def session_close(session):
    engine = session.get("engine") if session else None
    if engine is not None:
        with contextlib.suppress(Exception):
            engine.close()
    if session is not None:
        session["engine"] = None


def _session_temp_macro(session, command):
    match = re.match(
        r"(?is)^CREATE\s+(?:OR\s+REPLACE\s+)?"
        r"(?:TEMP|TEMPORARY)\s+MACRO\s+"
        r"([A-Za-z_][A-Za-z0-9_$]*)\s*\(",
        command)
    if not match:
        return None
    name = match.group(1).lower()
    if name in RESERVED_MACROS:
        raise ValueError(f"macro {name} is reserved by the CDC runtime")
    session["engine"].execute(command)
    session["temp_macros"].add(name)
    session["updated"] = time.time()
    return dict(status="session",kind="macro",name=name,scope="SESSION")


def _require_arrow_udf_runtime():
    try:
        import numpy
    except ImportError as exc:
        raise RuntimeError(
            "Arrow UDFs require NumPy; install numpy before registering one") from exc


def _register_session_udf(session, spec):
    _require_arrow_udf_runtime()
    source = spec.get("source")
    digest = spec.get("source_sha256")
    if source is None:
        source,digest = _arrow_udf_source(spec)
    namespace = {"__builtins__":__builtins__}
    exec(compile(
        source,spec.get("filename") or spec.get("path") or "<cdc-arrow-udf>","exec"),
        namespace,namespace)
    function = namespace.get(spec["function"])
    if not callable(function):
        raise ValueError(
            f"Arrow UDF {spec['name']}: function {spec['function']!r} not found")
    with contextlib.suppress(Exception):
        session["engine"].remove_function(spec["name"])
    session["engine"].create_function(
        spec["name"],function,
        [duckdb.sqltype(item) for item in spec["parameters"]],
        duckdb.sqltype(spec["return_type"]),
        type="arrow",side_effects=False)
    session["updated"] = time.time()
    return digest


def _session_arrow_udf(session, command):
    spec = _arrow_udf_call(command)
    if spec is None or spec["scope"] == "GLOBAL":
        return None
    digest = _register_session_udf(session,spec)
    session["temp_udfs"].add(spec["name"])
    return dict(
        status="session",kind="arrow_udf",name=spec["name"],
        scope="SESSION",source_sha256=digest)


def _session_drop(session, command):
    macro = re.fullmatch(
        r"(?is)DROP\s+MACRO\s+(?:IF\s+EXISTS\s+)?"
        r"([A-Za-z_][A-Za-z0-9_$]*)",
        command)
    if macro and macro.group(1).lower() in session["temp_macros"]:
        name = macro.group(1).lower()
        session["engine"].execute(command)
        session["temp_macros"].discard(name)
        session["updated"] = time.time()
        return dict(status="session",kind="macro",name=name,dropped=True)

    udf = re.fullmatch(
        r"(?is)CALL\s+cdc_drop_arrow_udf\s*\(\s*'([^']+)'"
        r"(?:\s*,\s*'([^']+)')?\s*\)",
        command)
    if udf and (udf.group(2) or "GLOBAL").upper() == "SESSION":
        name = udf.group(1)
        existed = name in session["temp_udfs"]
        if existed:
            session["engine"].remove_function(name)
            session["temp_udfs"].discard(name)
        session["updated"] = time.time()
        return dict(
            status="session",kind="arrow_udf",name=name,
            scope="SESSION",dropped=existed)
    return None


def _publish_command(command):
    return re.fullmatch(
        r"(?is)(?:PUBLISH|CALL\s+cdc_publish\s*\(\s*\))",
        command.strip().rstrip(";").strip()) is not None


def _execute_con(con, command):
    selected = _select_variable(con,command)
    if selected is not None:
        return selected
    shown = _show(con,command)
    if shown is not None:
        return shown
    result = (
        _set_variable(con,command)
        or _reset_variable(con,command)
        or _create_macro(con,command)
        or _create_arrow_udf(con,command)
        or _drop_arrow_udf(con,command)
        or _create_model_or_sink(con,command)
        or _alter_primary_key(con,command)
        or _drop_object(con,command)
    )
    if result is None:
        raise ValueError(
            "unsupported catalog command; run HELP; for the phase-1 grammar")
    return result


def _plan_mutation(result):
    return (
        result.get("status") == "draft"
        and result.get("kind") in (
            "macro","view","sink","table","primary_key","arrow_udf"))


def _reject_script_ephemeral(command):
    if re.match(
            r"(?is)^CREATE\s+(?:OR\s+REPLACE\s+)?"
            r"(?:TEMP|TEMPORARY)\s+MACRO\b",command):
        raise ValueError(
            "sql scripts are persistent transactions; TEMP MACRO is allowed only in cli")
    spec = _arrow_udf_call(command)
    if spec is not None and spec["scope"] == "SESSION":
        raise ValueError(
            "sql scripts are persistent transactions; SESSION Arrow UDF is allowed only in cli")
    drop = re.fullmatch(
        r"(?is)CALL\s+cdc_drop_arrow_udf\s*\(\s*'([^']+)'"
        r"(?:\s*,\s*'([^']+)')?\s*\)",command)
    if drop and (drop.group(2) or "GLOBAL").upper() == "SESSION":
        raise ValueError(
            "sql scripts are persistent transactions; SESSION Arrow UDF is allowed only in cli")


def execute_batch(
        path, commands, active_version=None, publish_callback=None,
        auto_publish=True, persistent_only=True, validate_config=False):
    """Execute persistent catalog commands atomically and publish final plan once."""
    normalized = [
        item.strip().rstrip(";").strip()
        for item in commands if item and item.strip().rstrip(";").strip()]
    if not normalized:
        return dict(status="ok",results=[],publish=None)

    con = catalog_open(path)
    results = []
    plan_changed = False
    config_changed = False
    publish_requested = False
    validation = None
    config_validation = None
    config_candidate = None
    config_activation = None
    published = None
    try:
        _transaction(con)
        for index,command in enumerate(normalized,1):
            if persistent_only:
                _reject_script_ephemeral(command)
            if re.fullmatch(r"(?is)(?:HELP|CALL\s+cdc_help\s*\(\s*\))",command):
                result = help_response()
            elif _publish_command(command):
                publish_requested = True
                result = dict(status="publish_deferred",statement=index)
            else:
                result = _execute_con(con,command)
                plan_changed = plan_changed or _plan_mutation(result)
                config_changed = config_changed or result.get("status") == "configured"
            result = dict(result)
            result["statement"] = index
            results.append(result)

        if publish_requested or (auto_publish and plan_changed):
            candidate = _publish_candidate(con)
            if publish_callback is not None:
                validation = publish_callback(candidate,"validate")
            published = _publish_persist(con,candidate)
        elif validate_config and config_changed and publish_callback is not None:
            config_candidate = _published_config_candidate(con)
            config_validation = publish_callback(
                config_candidate,"validate_config")
        _commit(con)
    except BaseException:
        with contextlib.suppress(sqlite3.Error):
            _rollback(con)
        raise
    finally:
        con.close()

    if published is not None:
        if validation is not None:
            published["validation"] = validation
        if publish_callback is not None:
            try:
                published["activation"] = publish_callback(published,"install")
            except Exception as exc:
                published["activation"] = dict(
                    status="restart_required",
                    reason="plan was published but live installation failed: "+str(exc))
    elif config_candidate is not None and publish_callback is not None:
        try:
            config_activation = publish_callback(
                config_candidate,"install_config")
        except Exception as exc:
            config_activation = dict(
                status="restart_required",
                reason="configuration committed but live activation failed: "+str(exc))
    return dict(
        status="ok",results=results,publish=published,
        config_validation=config_validation,
        config_activation=config_activation,
        active_version=int(active_version or 0))


def _persistent_plan_command(command):
    text = command.strip()
    if re.match(
            r"(?is)^CREATE\s+(?:OR\s+REPLACE\s+)?"
            r"(?:MACRO|VIEW\s+model\.|TABLE\s+starrocks\.)",text):
        return True
    if re.match(r"(?is)^ALTER\s+TABLE\s+starrocks\.",text):
        return True
    if re.match(
            r"(?is)^DROP\s+(?:VIEW\s+model\.|TABLE\s+starrocks\.|MACRO\b)",
            text):
        return True
    spec = _arrow_udf_call(text)
    if spec is not None:
        return spec["scope"] == "GLOBAL"
    drop = re.fullmatch(
        r"(?is)CALL\s+cdc_drop_arrow_udf\s*\(\s*'([^']+)'"
        r"(?:\s*,\s*'([^']+)')?\s*\)",text)
    return bool(drop and (drop.group(2) or "GLOBAL").upper() == "GLOBAL")


def _sync_session_persistent(path, session, command, result):
    if result.get("kind") == "macro" and result.get("status") == "draft":
        if result.get("dropped"):
            with contextlib.suppress(Exception):
                session["engine"].execute(command)
        else:
            session["engine"].execute(command)
        return
    if result.get("kind") != "arrow_udf" or result.get("scope") != "GLOBAL":
        return
    name = result.get("name")
    if result.get("dropped"):
        with contextlib.suppress(Exception):
            session["engine"].remove_function(name)
        session["global_udfs"].discard(name)
        return
    con = catalog_open(path)
    try:
        row = con.execute("""
            SELECT name,function_name,parameters_json,return_type,
                   source,source_sha256,filename
            FROM arrow_udfs WHERE name=?
        """,(name,)).fetchone()
    finally:
        con.close()
    if not row:
        raise RuntimeError(f"global Arrow UDF {name} disappeared after update")
    _register_session_udf(
        session,dict(
            name=row[0],function=row[1],parameters=json.loads(row[2]),
            return_type=row[3],source=row[4],
            source_sha256=row[5],filename=row[6]))
    session["global_udfs"].add(name)


def execute_session(
        path, command, session, active_version=None, publish_callback=None):
    command = command.strip().rstrip(";").strip()
    if not command:
        return dict(status="ok")
    if re.fullmatch(r"(?is)CALL\s+cdc_session\s*\(\s*\)",command):
        return dict(
            status="ok",session_id=session["id"],
            temp_macros=sorted(session["temp_macros"]),
            temp_udfs=sorted(session["temp_udfs"]))

    result = (
        _session_temp_macro(session,command)
        or _session_arrow_udf(session,command)
        or _session_drop(session,command)
    )
    if result is not None:
        result["active_version"] = int(active_version or 0)
        return result

    if re.match(r"(?is)^SELECT\b",command) and not re.match(
            r"(?is)^SELECT\s+getvariable\s*\(",command):
        if re.search(r"\b(?:mysql|model|starrocks)\.",command,re.I):
            raise ValueError(
                "session SELECT over virtual CDC namespaces is not implemented yet; "
                "use persistent model definitions or SELECT literals/UDFs")
        cursor = session["engine"].execute(command)
        rows = cursor.fetchmany(1001)
        if len(rows) > 1000:
            raise ValueError("session SELECT result exceeds 1000 rows")
        columns = [item[0] for item in cursor.description]
        session["updated"] = time.time()
        return dict(status="ok",columns=columns,rows=rows)

    try:
        batch = execute_batch(
            path,[command],active_version,publish_callback,
            auto_publish=True,persistent_only=False,validate_config=True)
        item = batch["results"][0] if batch["results"] else dict(status="ok")
    except Exception as exc:
        if isinstance(exc,CatalogBusyError):
            raise
        if not _persistent_plan_command(command):
            raise
        # Interactive CLI is an editor: a syntactically valid persistent DDL
        # survives even when the whole plan cannot yet be published. Active
        # runtime state remains on the last validated plan.
        item = execute(path,command,active_version)
        if not _plan_mutation(item):
            raise
        _sync_session_persistent(path,session,command,item)
        response = dict(item)
        response["status"] = "draft_persisted"
        response["publish_deferred"] = True
        response["publish_error"] = str(exc)
        response["active_version"] = int(active_version or 0)
        response["restart_required"] = False
        return response
    if item.get("status") == "session_udf":
        return _session_arrow_udf(session,command)
    if item.get("status") == "session_udf_drop":
        return _session_drop(session,command)
    _sync_session_persistent(path,session,command,item)
    response = dict(item)
    response["active_version"] = int(active_version or 0)
    if batch.get("publish") is not None:
        response["publish"] = batch["publish"]
        activation = batch["publish"].get("activation")
        if activation is not None:
            response["activation"] = activation
            response["restart_required"] = str(
                activation.get("status","")) in (
                    "rebuild_required","restart_required")
    elif batch.get("config_activation") is not None:
        activation = batch["config_activation"]
        response["activation"] = activation
        response["restart_required"] = str(
            activation.get("status","")) == "restart_required"
    if batch.get("config_validation") is not None:
        response["config_validation"] = batch["config_validation"]
    response.setdefault("restart_required",False)
    return response


def execute(path, command, active_version=None):
    command = command.strip().rstrip(";").strip()
    if not command:
        return dict(status="ok")
    if re.fullmatch(r"(?is)(?:HELP|CALL\s+cdc_help\s*\(\s*\))",command):
        return help_response()
    if _publish_command(command):
        result = publish(path)
        result["active_version"] = int(active_version or 0)
        return result

    con = catalog_open(path)
    try:
        selected = _select_variable(con,command)
        if selected is not None:
            selected["active_version"] = int(active_version or 0)
            return selected
        shown = _show(con,command)
        if shown is not None:
            shown["active_version"] = int(active_version or 0)
            return shown
        _transaction(con)
        try:
            result = _execute_con(con,command)
            _commit(con)
        except BaseException:
            _rollback(con)
            raise
        result["active_version"] = int(active_version or 0)
        result.setdefault("restart_required",False)
        return result
    finally:
        con.close()


def ensure_seed(path, seed_path=None):
    con = catalog_open(path)
    try:
        count = int(con.execute(
            "SELECT COUNT(*) FROM objects").fetchone()[0])
        plan_count = int(con.execute(
            "SELECT COUNT(*) FROM plans").fetchone()[0])
    finally:
        con.close()
    if count or plan_count:
        return
    if not seed_path or not os.path.isfile(seed_path):
        return
    body = open(seed_path,"r",encoding="utf-8").read()
    for command in split_commands(body):
        execute(path,command)
    con = catalog_open(path)
    try:
        version = int(_meta_get(con,"published_version","0"))
    finally:
        con.close()
    if not version:
        publish(path)


def _client_request(
        socket_path, payload, request_limit=MAX_SCRIPT_BYTES*2, timeout=30):
    data = json.dumps(
        payload,ensure_ascii=False,separators=(",",":")).encode("utf-8")
    if len(data) > request_limit:
        raise ValueError("catalog request is too large")
    sock = socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
    try:
        sock.settimeout(timeout)
        sock.connect(socket_path)
        sock.sendall(data)
        sock.shutdown(socket.SHUT_WR)
        chunks = []
        total = 0
        while True:
            block = sock.recv(65536)
            if not block:
                break
            total += len(block)
            if total > MAX_SCRIPT_BYTES*4:
                raise RuntimeError("catalog response is unexpectedly large")
            chunks.append(block)
        if not chunks:
            raise RuntimeError("catalog server closed without a response")
        return json.loads(b"".join(chunks).decode("utf-8"))
    finally:
        sock.close()


def _catalog_lock_acquire(socket_path):
    lock_path = socket_path+".lock"
    os.makedirs(os.path.dirname(lock_path) or ".",exist_ok=True)
    handle = open(lock_path,"a+b")
    with contextlib.suppress(OSError):
        os.chmod(lock_path,0o600)
    try:
        fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        return None
    return handle


def _catalog_lock_release(handle):
    if handle is None:
        return
    with contextlib.suppress(OSError):
        fcntl.flock(handle,fcntl.LOCK_UN)
    handle.close()


def _catalog_socket_state(socket_path, attempts=3):
    if not os.path.exists(socket_path):
        return "missing"
    for attempt in range(max(1,int(attempts))):
        session_id = "probe-"+uuid.uuid4().hex
        try:
            _client_request(
                socket_path,
                dict(session_id=session_id,command="CALL cdc_session()"),
                MAX_COMMAND_BYTES*2,timeout=0.25)
        except FileNotFoundError:
            return "missing"
        except ConnectionRefusedError:
            if attempt+1 < attempts:
                time.sleep(0.05)
                continue
            return "stale"
        except (TimeoutError,OSError,RuntimeError,ValueError):
            # A path that accepts connections but is slow or speaks an
            # unexpected protocol must never be unlinked as "stale".
            return "occupied"
        else:
            with contextlib.suppress(Exception):
                _client_request(
                    socket_path,
                    dict(
                        session_id=session_id,
                        command="CALL cdc_close_session()"),
                    MAX_COMMAND_BYTES*2,timeout=0.25)
            return "live"
    return "stale"


def _clear_stale_catalog_socket(socket_path):
    try:
        mode = os.lstat(socket_path).st_mode
    except FileNotFoundError:
        return True
    if not stat.S_ISSOCK(mode):
        raise RuntimeError(
            f"refusing to remove non-socket catalog path: {socket_path}")
    lock_handle = _catalog_lock_acquire(socket_path)
    if lock_handle is None:
        return False
    try:
        state = _catalog_socket_state(socket_path)
        if state == "missing":
            return True
        if state != "stale":
            return False
        try:
            mode = os.lstat(socket_path).st_mode
        except FileNotFoundError:
            return True
        if not stat.S_ISSOCK(mode):
            raise RuntimeError(
                f"catalog path changed while clearing stale socket: {socket_path}")
        os.unlink(socket_path)
        return True
    finally:
        _catalog_lock_release(lock_handle)


def client(socket_path, command, session_id=None):
    return _client_request(
        socket_path,
        dict(session_id=session_id or uuid.uuid4().hex,command=command),
        MAX_COMMAND_BYTES*2)


def client_script(socket_path, commands, session_id=None):
    return _client_request(
        socket_path,
        dict(
            session_id=session_id or uuid.uuid4().hex,
            script=True,commands=list(commands)),
        MAX_SCRIPT_BYTES*2)


def server_worker(
        path, socket_path, active_version, stop, publish_callback=None,
        active_version_getter=None, listening=None):
    listener = socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
    sessions = {}
    bound = False
    lock_handle = _catalog_lock_acquire(socket_path)
    if lock_handle is None:
        listener.close()
        raise RuntimeError(
            f"another J4 daemon owns the catalog control socket: {socket_path}")
    try:
        os.makedirs(os.path.dirname(socket_path) or ".",exist_ok=True)
        try:
            mode = os.lstat(socket_path).st_mode
            if not stat.S_ISSOCK(mode):
                raise RuntimeError(
                    f"refusing to remove non-socket catalog path: {socket_path}")
            state = _catalog_socket_state(socket_path)
            if state == "stale":
                os.unlink(socket_path)
            elif state != "missing":
                raise RuntimeError(
                    f"catalog socket is already owned by a live or unresponsive "
                    f"process: {socket_path}")
        except FileNotFoundError:
            pass
        listener.bind(socket_path)
        bound = True
        os.chmod(socket_path,0o600)
        listener.listen(8)
        listener.settimeout(0.5)
        if listening is not None:
            listening.set()
        print(
            f"[catalog] shell_socket={socket_path} active_version={active_version}",
            flush=True)
        while not stop.is_set():
            try:
                conn,_ = listener.accept()
            except socket.timeout:
                reap_sessions(sessions)
                continue
            except OSError:
                if stop.is_set():
                    break
                raise
            with conn:
                try:
                    chunks,total = [],0
                    while True:
                        block = conn.recv(65536)
                        if not block:
                            break
                        total += len(block)
                        if total > MAX_SCRIPT_BYTES*2:
                            raise ValueError("catalog request exceeds script size limit")
                        chunks.append(block)
                    request_text = b"".join(chunks).decode("utf-8")
                    try:
                        request = json.loads(request_text)
                    except json.JSONDecodeError:
                        request = dict(
                            session_id="legacy-"+uuid.uuid4().hex,
                            command=request_text)
                    session_id = str(request.get("session_id") or uuid.uuid4().hex)
                    command = str(request.get("command") or "")
                    is_script = bool(request.get("script"))
                    if re.fullmatch(
                            r"(?is)CALL\s+cdc_close_session\s*\(\s*\)\s*;?",
                            command.strip()):
                        session = sessions.pop(session_id,None)
                        session_close(session)
                        result = dict(status="closed",session_id=session_id)
                    else:
                        session = sessions.get(session_id)
                        if session is None:
                            reap_sessions(sessions)
                            if len(sessions) >= MAX_SESSIONS:
                                raise RuntimeError(
                                    f"catalog session limit reached ({MAX_SESSIONS}); "
                                    "close an existing REPL or wait for idle expiry")
                            session = session_new(path)
                            session["id"] = session_id
                            sessions[session_id] = session
                        current_version = (
                            int(active_version_getter())
                            if active_version_getter is not None
                            else int(active_version or 0))
                        if is_script:
                            commands = request.get("commands")
                            if not isinstance(commands,list):
                                raise ValueError("script request requires a commands list")
                            result = execute_batch(
                                path,commands,current_version,publish_callback,
                                auto_publish=True,persistent_only=True,
                                validate_config=True)
                        else:
                            result = execute_session(
                                path,command,session,current_version,publish_callback)
                        session["updated"] = time.time()
                    response = dict(ok=True,result=result)
                except Exception as exc:
                    response = dict(ok=False,error=str(exc))
                conn.sendall(json.dumps(
                    response,ensure_ascii=False,separators=(",",":"),
                    default=str).encode("utf-8"))
    finally:
        for session in sessions.values():
            session_close(session)
        sessions.clear()
        listener.close()
        if bound:
            try:
                mode = os.lstat(socket_path).st_mode
            except FileNotFoundError:
                pass
            else:
                if stat.S_ISSOCK(mode):
                    os.unlink(socket_path)
        _catalog_lock_release(lock_handle)


def _print_response(response):
    print(json.dumps(
        response,ensure_ascii=False,indent=2,sort_keys=True,default=str))


def _batch_activation_error(result):
    activations = [
        (result.get("publish") or {}).get("activation"),
        result.get("config_activation"),
    ]
    for activation in activations:
        if isinstance(activation,dict) and activation.get("status") in ("restart_required","rebuild_required"):
            return str(
                activation.get("reason")
                or "catalog committed but offline installation failed")
    return None


def shell(
        path, socket_path, seed_path=None, command=None, file_path=None,
        auto_publish_file=True,publish_callback=None):
    ensure_seed(path,seed_path)
    session_id = uuid.uuid4().hex
    local_session = session_new(path)
    local_session["id"] = session_id

    def remote(action):
        if not os.path.exists(socket_path):
            return False,None
        try:
            return True,action()
        except FileNotFoundError:
            return False,None
        except ConnectionRefusedError as exc:
            if _clear_stale_catalog_socket(socket_path):
                return False,None
            raise RuntimeError(
                f"catalog daemon owns the socket but refused the connection: "
                f"{exc}") from exc
        except (ConnectionError,OSError,TimeoutError) as exc:
            raise RuntimeError(
                f"catalog socket exists but daemon is unreachable: {exc}") from exc

    def run(text):
        used,response = remote(
            lambda: client(socket_path,text,session_id))
        if used:
            return response
        try:
            return dict(
                ok=True,result=execute_session(
                    path,text,local_session,active_version=None,
                    publish_callback=publish_callback))
        except Exception as exc:
            return dict(ok=False,error=str(exc))

    def run_many(commands):
        for index,item in enumerate(commands,1):
            response = run(item)
            _print_response(dict(statement=index,response=response))
            if not response.get("ok"):
                return 1
        return 0

    if command is not None and file_path is not None:
        raise ValueError("-c/--command and -f/--file are mutually exclusive")
    if command is not None:
        try:
            response = run(command)
            _print_response(response)
            return 0 if response.get("ok") else 1
        finally:
            if os.path.exists(socket_path):
                with contextlib.suppress(Exception):
                    client(socket_path,"CALL cdc_close_session()",session_id)
            session_close(local_session)
    if file_path is not None:
        try:
            size = os.path.getsize(file_path)
            if size > MAX_SCRIPT_BYTES:
                raise ValueError("SQL script exceeds 16MiB")
            with open(file_path,"r",encoding="utf-8") as handle:
                commands = split_commands(handle.read())
            used,response = remote(
                lambda: client_script(socket_path,commands,session_id))
            if not used:
                try:
                    result = execute_batch(
                        path,commands,active_version=None,
                        publish_callback=publish_callback,
                        auto_publish=auto_publish_file,persistent_only=True,
                        validate_config=True)
                    activation_error = _batch_activation_error(result)
                    response = (
                        dict(ok=False,error=activation_error,result=result)
                        if activation_error
                        else dict(ok=True,result=result))
                except Exception as exc:
                    response = dict(ok=False,error=str(exc))
            if used and response.get("ok"):
                activation_error = _batch_activation_error(response.get("result") or {})
                if activation_error:
                    response = dict(ok=False,error=activation_error,result=response.get("result"))
            _print_response(response)
            return 0 if response.get("ok") else 1
        finally:
            if os.path.exists(socket_path):
                with contextlib.suppress(Exception):
                    client(socket_path,"CALL cdc_close_session()",session_id)
            session_close(local_session)

    print(
        "CDC SQL catalog shell. HELP; shows commands. "
        "Persistent DDL auto-validates and publishes; CREATE/DROP/SET survive restart. "
        "TEMP/SESSION objects are explicit cli-only exceptions.",
        flush=True)
    pending = ""
    while True:
        try:
            prompt = "...> " if pending else "cdc> "
            line = input(prompt)
        except EOFError:
            print()
            if os.path.exists(socket_path):
                with contextlib.suppress(Exception):
                    client(socket_path,"CALL cdc_close_session()",session_id)
            session_close(local_session)
            return 0
        if not pending and line.strip().lower() in ("quit","exit","\\q"):
            if os.path.exists(socket_path):
                with contextlib.suppress(Exception):
                    client(socket_path,"CALL cdc_close_session()",session_id)
            session_close(local_session)
            return 0
        pending += ("\n" if pending else "")+line
        if ";" not in pending:
            continue
        last_semicolon = pending.rfind(";")
        complete = pending[:last_semicolon+1]
        pending = pending[last_semicolon+1:].strip()
        run_many(split_commands(complete))


def selftest():
    directory = tempfile.TemporaryDirectory(prefix="cdc_catalog_selftest_")
    try:
        v1_path = os.path.join(directory.name,"catalog_v1.sqlite3")
        v1 = sqlite3.connect(v1_path)
        v1.execute("CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
        v1.execute("INSERT INTO meta VALUES('format','1')")
        v1.execute("INSERT INTO meta VALUES('draft_revision','0')")
        v1.execute("INSERT INTO meta VALUES('published_version','0')")
        v1.execute("INSERT INTO meta VALUES('config_revision','0')")
        v1.commit()
        v1.close()
        migrated = catalog_open(v1_path)
        try:
            assert int(_meta_get(migrated,"format","0")) == CATALOG_FORMAT
        finally:
            migrated.close()

        path = os.path.join(directory.name,"catalog.sqlite3")
        execute(
            path,
            "CREATE MACRO clean_name(x) AS trim(x)")
        execute(
            path,
            "CREATE VIEW model.base AS "
            "SELECT id, clean_name(name) AS name FROM mysql.source_table")
        execute(
            path,
            "CREATE VIEW model.filtered AS "
            "SELECT id, name FROM model.base WHERE id > 0")
        execute(
            path,
            "CREATE TABLE starrocks.target AS "
            "SELECT id, name FROM model.filtered")
        plan = execute(path,"CALL cdc_publish()")
        assert plan["version"] == 1
        assert plan["mappings"][0]["src_table"] == "source_table"
        assert plan["mappings"][0]["sr_table"] == "target"
        assert plan["mappings"][0]["primary_key"] is None
        assert "arrow_batch" in plan["mappings"][0]["sql"].lower()
        assert "_sync_op" in plan["mappings"][0]["sql"].lower()
        loaded = load_plan(path)
        assert loaded["plan_hash"] == plan["plan_hash"]

        fanout_path = os.path.join(directory.name,"fanout.sqlite3")
        execute(
            fanout_path,
            "CREATE TABLE starrocks.fanout_a AS "
            "SELECT id, name FROM mysql.source_table")
        execute(
            fanout_path,
            "CREATE TABLE starrocks.fanout_b AS "
            "SELECT id, upper(name) AS name FROM mysql.source_table")
        fanout = publish(fanout_path)
        assert len(fanout["mappings"]) == 2
        assert {item["src_table"] for item in fanout["mappings"]} == {"source_table"}
        assert {item["_catalog_sink"] for item in fanout["mappings"]} == {
            "starrocks.fanout_a","starrocks.fanout_b"}
        assert {item["sr_table"] for item in fanout["mappings"]} == {
            "fanout_a","fanout_b"}

        empty_path = os.path.join(directory.name,"empty_plan.sqlite3")
        execute(
            empty_path,
            "CREATE TABLE starrocks.only AS SELECT id FROM mysql.source_table")
        publish(empty_path)
        execute(empty_path,"DROP TABLE starrocks.only")
        empty = publish(empty_path)
        assert empty["mappings"] == []
        assert load_plan(empty_path)["mappings"] == []
        same = execute(path,"CALL cdc_publish()")
        assert same["version"] == 1 and not same["changed"]
        stale_socket = os.path.join(directory.name,"stale.sock")
        stale_listener = socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
        stale_listener.bind(stale_socket)
        stale_listener.close()
        assert os.path.exists(stale_socket)
        assert _clear_stale_catalog_socket(stale_socket)
        assert not os.path.exists(stale_socket)

        protected_path = os.path.join(directory.name,"not-a-socket.sock")
        with open(protected_path,"w",encoding="utf-8") as handle:
            handle.write("keep")
        try:
            _clear_stale_catalog_socket(protected_path)
            raise AssertionError("non-socket catalog path was removed")
        except RuntimeError:
            pass
        assert os.path.isfile(protected_path)
        try:
            server_worker(
                path,protected_path,0,threading.Event())
            raise AssertionError("server accepted a non-socket control path")
        except RuntimeError:
            pass
        assert os.path.isfile(protected_path)

        stale = session_new(path)
        stale["updated"] = time.time()-SESSION_IDLE_SECONDS-1
        stale_sessions = {"stale":stale}
        assert reap_sessions(stale_sessions,time.time()) == 1
        assert stale_sessions == {} and stale["engine"] is None

        session = session_new(path)
        try:
            selected = execute_session(
                path,"SELECT clean_name(' demo ') AS value",session)
            assert selected["rows"] == [("demo",)]
            temp = execute_session(
                path,"CREATE TEMP MACRO twice(x) AS x * 2",session)
            assert temp["scope"] == "SESSION"
            selected = execute_session(path,"SELECT twice(21) AS value",session)
            assert selected["rows"] == [(42,)]
            check_con = catalog_open(path)
            try:
                assert check_con.execute(
                    "SELECT COUNT(*) FROM objects WHERE name='macro.twice'"
                ).fetchone()[0] == 0
            finally:
                check_con.close()
        finally:
            session_close(session)

        udf_path = os.path.join(directory.name,"udfs.py")
        with open(udf_path,"w",encoding="utf-8") as handle:
            handle.write(
                "import pyarrow.compute as pc\n"
                "def plus_one(x):\n"
                "    return pc.add(x, 1)\n")
        session = session_new()
        try:
            udf = execute_session(
                path,
                "CALL cdc_create_arrow_udf("
                f"'plus_one','{udf_path}','plus_one','BIGINT','BIGINT','SESSION')",
                session)
            assert udf["scope"] == "SESSION"
            selected = execute_session(path,"SELECT plus_one(41) AS value",session)
            assert selected["rows"] == [(42,)]
        finally:
            session_close(session)

        global_udf = execute(
            path,
            "CALL cdc_create_arrow_udf("
            f"'plus_one','{udf_path}','plus_one','BIGINT','BIGINT')")
        assert global_udf["scope"] == "GLOBAL"
        udf_plan = execute(path,"CALL cdc_publish()")
        assert udf_plan["version"] == 2
        assert udf_plan["udfs"][0]["name"] == "plus_one"
        assert len(udf_plan["udfs"][0]["source_sha256"]) == 64
        session = session_new(path)
        try:
            selected = execute_session(
                path,"SELECT plus_one(9) AS value",session)
            assert selected["rows"] == [(10,)]
        finally:
            session_close(session)
        try:
            execute(path,"SET VARIABLE CDC_MYSQL_PORT = 'not-a-port'")
            raise AssertionError("invalid MySQL port was persisted")
        except ValueError:
            pass
        assert "CDC_MYSQL_PORT" not in variables_get(path)

        configured = execute(
            path,"SET VARIABLE CDC_BATCH_ROWS = 12345")
        assert configured["restart_required"]
        selected = execute(
            path,"SELECT getvariable('CDC_BATCH_ROWS')")
        assert selected["value"] == "12345" and selected["source"] == "catalog"
        execute(path,"RESET VARIABLE CDC_BATCH_ROWS")
        assert execute(
            path,"SELECT getvariable('CDC_BATCH_ROWS')")["value"] is None

        script_path = os.path.join(directory.name,"config.sql")
        with open(script_path,"w",encoding="utf-8") as handle:
            handle.write("""
                -- SQL-file path uses normal DuckDB variable grammar.
                SET VARIABLE CDC_STATUS_SECONDS = 45;
                /* RESET must delete the durable catalog value. */
                RESET VARIABLE CDC_STATUS_SECONDS;
            """)
        assert shell(
            path,os.path.join(directory.name,"missing.sock"),
            file_path=script_path) == 0
        assert "CDC_STATUS_SECONDS" not in variables_get(path)

        failed_install_path = os.path.join(directory.name,"failed-install.sqlite3")
        failed_install_sql = os.path.join(directory.name,"failed-install.sql")
        with open(failed_install_sql,"w",encoding="utf-8") as handle:
            handle.write(
                "CREATE TABLE starrocks.target AS "
                "SELECT id FROM mysql.source_table;\n")
        def fail_after_commit(candidate, phase):
            if phase == "validate":
                return dict(status="validated_offline")
            if phase == "install":
                raise RuntimeError("synthetic target creation failure")
            raise AssertionError(f"unexpected failed-install callback phase: {phase}")
        assert shell(
            failed_install_path,os.path.join(directory.name,"missing-install.sock"),
            file_path=failed_install_sql,publish_callback=fail_after_commit) == 1
        assert load_plan(failed_install_path)["version"] == 1

        pure_config_path = os.path.join(directory.name,"pure-config.sqlite3")
        seen_config_candidate = {}
        def validate_pure_config(candidate, phase):
            if phase == "validate_config":
                seen_config_candidate.update(candidate)
                return dict(status="validated_config")
            raise AssertionError(f"unexpected pure-config callback phase: {phase}")
        result = execute_batch(
            pure_config_path,
            ["SET VARIABLE CDC_STATUS_SECONDS = 46"],
            publish_callback=validate_pure_config,
            auto_publish=True,persistent_only=True,validate_config=True)
        assert result["config_validation"]["status"] == "validated_config"
        assert seen_config_candidate["_variables"]["CDC_STATUS_SECONDS"] == "46"
        assert variables_get(pure_config_path)["CDC_STATUS_SECONDS"] == "46"

        # SQL deployment files are one durable transaction: a late failure
        # must not leave earlier SET/DDL changes behind.
        before_revision = config_revision(path)
        before_target = execute(path,"SHOW CREATE starrocks.target")["sql"]
        try:
            execute_batch(
                path,[
                    "SET VARIABLE CDC_STATUS_SECONDS = 61",
                    "CREATE OR REPLACE TABLE starrocks.target AS "
                    "SELECT id FROM model.missing",
                ],
                auto_publish=True,persistent_only=True)
            raise AssertionError("invalid atomic script unexpectedly succeeded")
        except Exception:
            pass
        assert "CDC_STATUS_SECONDS" not in variables_get(path)
        assert config_revision(path) == before_revision
        assert execute(path,"SHOW CREATE starrocks.target")["sql"] == before_target

        candidate_seen = {}
        def reject_with_candidate(candidate, phase):
            if phase == "validate":
                candidate_seen.update(candidate.get("_variables",{}))
                raise RuntimeError("synthetic candidate validation failure")
            return dict(status="unused")
        try:
            execute_batch(
                path,[
                    "SET VARIABLE CDC_STATUS_SECONDS = 62",
                    "CREATE OR REPLACE VIEW model.filtered AS "
                    "SELECT id, name FROM model.base WHERE id > 1",
                ],
                publish_callback=reject_with_candidate,
                auto_publish=True,persistent_only=True)
            raise AssertionError("candidate validation unexpectedly succeeded")
        except RuntimeError as exc:
            assert "synthetic candidate validation failure" in str(exc)
        assert candidate_seen["CDC_STATUS_SECONDS"] == "62"
        assert "CDC_STATUS_SECONDS" not in variables_get(path)

        old_env = os.environ.get("CDC_BATCH_ROWS")
        os.environ["CDC_BATCH_ROWS"] = "777"
        try:
            execute(path,"SET VARIABLE CDC_BATCH_ROWS = 888")
            effective = execute(
                path,"SELECT getvariable('CDC_BATCH_ROWS')")
            assert effective["value"] == "888"
            assert effective["source"] == "catalog"
        finally:
            if old_env is None:
                os.environ.pop("CDC_BATCH_ROWS",None)
            else:
                os.environ["CDC_BATCH_ROWS"] = old_env
            execute(path,"RESET VARIABLE CDC_BATCH_ROWS")
        # Failed online validation must never move published_version.
        publish_path = os.path.join(directory.name,"publish_atomic.sqlite3")
        execute(
            publish_path,
            "CREATE VIEW model.base AS SELECT id FROM mysql.source_table")
        execute(
            publish_path,
            "CREATE TABLE starrocks.target AS SELECT id FROM model.base")
        first = publish(publish_path)
        assert first["version"] == 1
        execute(
            publish_path,
            "CREATE OR REPLACE VIEW model.base AS "
            "SELECT id FROM mysql.source_table WHERE id > 0")
        def reject_publish(candidate, phase):
            if phase == "validate":
                raise RuntimeError("synthetic preflight failure")
            return dict(status="unused")
        try:
            publish(publish_path,reject_publish)
            raise AssertionError("failed validation unexpectedly published")
        except RuntimeError as exc:
            assert "synthetic preflight failure" in str(exc)
        assert load_plan(publish_path)["version"] == 1

        busy_path = os.path.join(directory.name,"busy_transition.sqlite3")
        busy_session = session_new(busy_path)
        try:
            def reject_busy(candidate, phase):
                if phase == "validate":
                    raise CatalogBusyError("synthetic startup transition")
                return dict(status="unused")
            try:
                execute_session(
                    busy_path,
                    "CREATE TABLE starrocks.busy AS SELECT id FROM mysql.source_table",
                    busy_session,publish_callback=reject_busy)
                raise AssertionError("busy catalog mutation unexpectedly persisted")
            except CatalogBusyError:
                pass
            busy_con = catalog_open(busy_path)
            try:
                assert busy_con.execute(
                    "SELECT COUNT(*) FROM objects WHERE name='starrocks.busy'"
                ).fetchone()[0] == 0
            finally:
                busy_con.close()
        finally:
            session_close(busy_session)

        try:
            execute(
                path,
                "CREATE OR REPLACE VIEW model.filtered AS "
                "SELECT a.id FROM mysql.a a JOIN mysql.b b ON a.id=b.id")
        except ValueError:
            pass
        else:
            raise AssertionError("JOIN must be rejected in catalog phase 1")
        print("CATALOG SELFTEST PASS",flush=True)
        return 0
    finally:
        directory.cleanup()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "selftest":
        raise SystemExit(selftest())
    paths = catalog_paths(__file__)
    raise SystemExit(shell(
        paths["catalog"],paths["socket"],paths["seed"],
        " ".join(sys.argv[1:]) if len(sys.argv) > 1 else None))

