#!/usr/bin/env python3
"""SQL frontend for the stream DAG.

read selects the sqlglot dialect. The default is duckdb, which is the dialect
already used by the validated mapping SQL, so existing statements keep their
shape. read="mysql" accepts MySQL syntax such as backticks and GROUP_CONCAT.
Expressions stored in the plan are DuckDB SQL. The durable COUNT/SUM/AVG and
INNER JOIN engines are not given the wider functions.
"""
import sqlglot
from sqlglot import exp

import stream_ir


READ_DIALECTS = {"duckdb", "mysql"}


class _Fallback(Exception):
    pass


class _Frame:
    def __init__(self):
        self.nodes = []
        self.inputs = []
        self.ctes = {}
        self._seq = 0
        self._col = 0

    def nid(self, prefix):
        self._seq += 1
        return prefix + str(self._seq)

    def col(self):
        self._col += 1
        return "c" + str(self._col)

    def add(self, node):
        self.nodes.append(node)
        return node["id"]

    def touch(self, name):
        if name not in self.inputs:
            self.inputs.append(name)


class _Scope:
    def __init__(self, node, outputs, by_alias, by_name):
        self.node = node
        self.outputs = list(outputs)
        self.by_alias = dict(by_alias)
        self.by_name = dict(by_name)


def _q(name):
    return '"' + str(name).replace('"', '""') + '"'


def _sources(sources):
    if not isinstance(sources, dict) or not sources:
        raise ValueError("SQL sources must be a non-empty dict")
    result = {}
    for name, spec in sources.items():
        key = str(name or "").strip()
        if not key:
            raise ValueError("SQL source name must be non-empty")
        if isinstance(spec, dict):
            columns = spec.get("columns")
        else:
            columns = spec
        columns = [str(item) for item in columns or ()]
        if not columns or len(columns) != len(set(columns)):
            raise ValueError("SQL source columns must be unique: " + key)
        result[key] = columns
    return result


def compile_sql(sql, sources, read="duckdb", nondeterministic="reject"):
    """Compile one statement to a stream plan.

    Returns plan, output column names, and the read dialect. The default
    dialect is duckdb. Nondeterministic functions are rejected unless
    nondeterministic="timed_recompute", which marks the plan explicitly.
    """
    if read not in READ_DIALECTS:
        raise ValueError("sql read dialect must be duckdb or mysql")
    if nondeterministic not in {"reject", "timed_recompute"}:
        raise ValueError("nondeterministic mode must be reject or timed_recompute")
    sources = _sources(sources)
    text = str(sql or "").strip()
    if not text:
        raise ValueError("SQL is empty")
    try:
        tree = sqlglot.parse_one(text, read=read)
    except Exception as exc:
        raise ValueError("invalid SQL: " + str(exc)) from exc
    if _nondeterministic(tree):
        if nondeterministic != "timed_recompute":
            raise ValueError(
                "nondeterministic function is rejected; "
                "pass nondeterministic='timed_recompute' to recompute on a schedule")
        frame = _Frame()
        node, outputs = _compile_recompute(
            text, sources, frame, read, strategy="timed_recompute")
        plan = stream_ir._plan(frame.inputs, frame.nodes, node)
        return dict(plan=plan, outputs=list(outputs), read=read)
    frame = _Frame()
    try:
        node, outputs = _compile_query(tree, sources, frame, read)
    except _Fallback:
        frame = _Frame()
        node, outputs = _compile_recompute(text, sources, frame, read)
    plan = stream_ir._plan(frame.inputs, frame.nodes, node)
    return dict(plan=plan, outputs=list(outputs), read=read)


def _nondeterministic(tree):
    names = {
        "random", "rand", "uuid", "now",
        "current_timestamp", "current_date", "current_time",
    }
    for node in tree.walk():
        if type(node).__name__ in {
            "Rand", "Uuid", "CurrentTimestamp", "CurrentDate", "CurrentTime",
        }:
            return True
        if isinstance(node, exp.Anonymous) and str(node.name).lower() in names:
            return True
    return False


def _compile_query(tree, sources, frame, read):
    if isinstance(tree, (exp.Union, exp.Intersect, exp.Except)):
        return _compile_set(tree, sources, frame, read)
    if isinstance(tree, exp.Subquery):
        return _compile_query(tree.this, sources, frame, read)
    if not isinstance(tree, exp.Select):
        raise ValueError("unsupported SQL statement")
    return _compile_select(tree, sources, frame, read)


def _compile_set(tree, sources, frame, read):
    left_id, left_out = _compile_query(tree.this, sources, frame, read)
    right_id, right_out = _compile_query(
        tree.expression, sources, frame, read)
    if len(left_out) != len(right_out):
        raise ValueError("set operands have different widths")
    if list(right_out) != list(left_out):
        expressions = []
        for src, dst in zip(right_out, left_out):
            expressions.append(dict(
                output=dst,
                expression=_q(src) + " AS " + _q(dst)))
        right_id = frame.add(dict(
            id=frame.nid("p"), op="project", input=right_id,
            expressions=expressions))
    distinct = bool(tree.args.get("distinct"))
    if isinstance(tree, exp.Union):
        mode = "union" if distinct else "union_all"
    elif isinstance(tree, exp.Intersect):
        mode = "intersect" if distinct else "intersect_all"
    else:
        mode = "except" if distinct else "except_all"
    node = frame.add(dict(
        id=frame.nid("set"), op="set_op",
        left=left_id, right=right_id, mode=mode))
    return node, list(left_out)


def _compile_recompute(sql, sources, frame, read, strategy="full_input_recompute"):
    tree = sqlglot.parse_one(sql, read=read)
    if any(isinstance(node, exp.Window) for node in tree.walk()):
        raise ValueError("window functions are not in this SQL subset")
    tables = []
    for table in tree.find_all(exp.Table):
        name = table.name
        if name not in sources or name in tables:
            continue
        tables.append(name)
    if not tables:
        raise ValueError("recompute SQL has no known source")
    ids = []
    columns = {}
    for name in tables:
        frame.touch(name)
        node = frame.add(dict(
            id=frame.nid("in"), op="input", input=name))
        ids.append(node)
        columns[node] = list(sources[name])
    duck_sql = tree.sql(dialect="duckdb").strip()
    node = frame.add(dict(
        id=frame.nid("re"),
        op="recompute",
        sources=ids,
        tables=list(tables),
        input_columns=columns,
        sql=duck_sql,
        strategy=strategy,
    ))
    outputs = _output_names(tree) if isinstance(tree, exp.Select) else []
    if not outputs:
        outputs = ["expr"]
    return node, outputs


def _output_names(tree):
    names = []
    for item in tree.expressions:
        name = str(item.alias_or_name or "").strip()
        if not name or name == "*":
            return []
        names.append(name)
    return names


def _compile_select(tree, sources, frame, read):
    if tree.args.get("offset") is not None:
        raise ValueError("OFFSET is not part of this relational subset")
    if tree.args.get("with_") is not None:
        _bind_ctes(tree.args["with_"], sources, frame, read)
    if any(isinstance(node, exp.Window) for node in tree.walk()):
        scope = _compile_from(tree, sources, frame, read)
        scope = _apply_where(
            scope, tree.args.get("where"), sources, frame, read)
        return _compile_over(scope, tree, frame)
    scope = _compile_from(tree, sources, frame, read)
    scope = _apply_where(scope, tree.args.get("where"), sources, frame, read)
    grouped = _compile_aggregate(scope, tree, frame)
    if grouped is not None:
        scope = grouped
    if tree.args.get("limit") is not None:
        if tree.args.get("order") is None:
            raise ValueError("LIMIT without ORDER BY is rejected")
        scope = _compile_projection(scope, tree, frame)
        return _compile_topn(scope, tree, frame)
    scope = _compile_projection(scope, tree, frame)
    if tree.args.get("distinct") is not None:
        node = frame.add(dict(
            id=frame.nid("d"), op="distinct", input=scope.node))
        scope = _Scope(node, scope.outputs, scope.by_alias, scope.by_name)
    return scope.node, list(scope.outputs)


def _compile_from(tree, sources, frame, read):
    origin = tree.args.get("from_")
    if origin is None:
        raise ValueError("SQL subset requires FROM")
    scope = _compile_from_item(origin.this, sources, frame, read)
    for join in tree.args.get("joins") or ():
        right = _compile_from_item(join.this, sources, frame, read)
        scope = _compile_join(scope, right, join, frame)
    return scope


def _compile_from_item(item, sources, frame, read):
    if isinstance(item, exp.Subquery):
        alias = str(item.alias or "").strip()
        if not alias:
            raise ValueError("subquery in FROM requires an alias")
        node, outputs = _compile_query(item.this, sources, frame, read)
        return _rename_scope(frame, node, outputs, alias, outputs)
    if isinstance(item, exp.Table):
        if item.name in frame.ctes:
            return frame.ctes[item.name]
        return _compile_table(item, sources, frame)
    raise ValueError("unsupported FROM item")


def _compile_table(table, sources, frame):
    name = table.name
    if name not in sources:
        raise ValueError("unknown SQL source: " + name)
    alias = str(table.alias or name)
    frame.touch(name)
    input_id = frame.add(dict(
        id=frame.nid("in"), op="input", input=name))
    expressions = []
    by_alias = {}
    by_name = {}
    outputs = []
    for column in sources[name]:
        internal = frame.col()
        expressions.append(dict(
            output=internal,
            expression=_q(column) + " AS " + _q(internal)))
        by_alias[(alias, column)] = internal
        by_name[column] = internal
        outputs.append(internal)
    node = frame.add(dict(
        id=frame.nid("p"), op="project", input=input_id,
        expressions=expressions))
    return _Scope(node, outputs, by_alias, by_name)


def _rename_scope(frame, node, outputs, alias, public_names):
    expressions = []
    by_alias = {}
    by_name = {}
    renamed = []
    for public in public_names:
        internal = frame.col()
        expressions.append(dict(
            output=internal,
            expression=_q(public) + " AS " + _q(internal)))
        by_alias[(alias, public)] = internal
        if public in by_name:
            by_name[public] = None
        else:
            by_name[public] = internal
        renamed.append(internal)
    node = frame.add(dict(
        id=frame.nid("p"), op="project", input=node,
        expressions=expressions))
    return _Scope(node, renamed, by_alias, by_name)


def _compile_join(left, right, join, frame):
    side = str(join.args.get("side") or "").upper()
    kind = str(join.args.get("kind") or "").upper()
    if kind == "SEMI":
        join_kind = "semi"
    elif kind == "ANTI":
        join_kind = "anti"
    elif side == "LEFT":
        join_kind = "left"
    elif side == "RIGHT":
        join_kind = "right"
    elif side == "FULL":
        join_kind = "full"
    else:
        join_kind = "inner"
    on = join.args.get("on")
    if on is None or kind == "CROSS":
        return _theta(left, right, exp.true(), join_kind, frame)
    pairs, residual = _split_equalities(on, left, right)
    if residual or not pairs:
        predicate = on.this if isinstance(on, exp.Where) else on
        return _theta(left, right, predicate, join_kind, frame)
    if join_kind == "inner" and not residual:
        return _equi(left, right, pairs, frame)
    return _keyed(left, right, pairs, join_kind, frame)


def _and_terms(node):
    if isinstance(node, exp.And):
        return _and_terms(node.this) + _and_terms(node.expression)
    return [node]


def _split_equalities(on, left, right):
    expr = on.this if isinstance(on, exp.Where) else on
    pairs = []
    residual = []
    for term in _and_terms(expr):
        pair = _equality_pair(term, left, right)
        if pair is None:
            residual.append(term)
        else:
            pairs.append(pair)
    return pairs, residual


def _equality_pair(term, left, right):
    if not isinstance(term, exp.EQ):
        return None
    if not isinstance(term.this, exp.Column) or not isinstance(
        term.expression, exp.Column
    ):
        return None
    try:
        first_left = _in_scope(term.this, left)
        first_right = _in_scope(term.this, right)
        second_left = _in_scope(term.expression, left)
        second_right = _in_scope(term.expression, right)
    except ValueError:
        return None
    if first_left and second_right:
        return (
            _lookup(term.this, left),
            _lookup(term.expression, right))
    if first_right and second_left:
        return (
            _lookup(term.expression, left),
            _lookup(term.this, right))
    return None


def _projections_from(scope, source):
    return [
        dict(output=name, source=source, column=name)
        for name in scope.outputs
    ]


def _equi(left, right, pairs, frame):
    projections = (
        _projections_from(left, "left")
        + _projections_from(right, "right"))
    node = frame.add(dict(
        id=frame.nid("j"),
        op="equi_join",
        left=left.node,
        right=right.node,
        join_pairs=[dict(left=item[0], right=item[1]) for item in pairs],
        projections=projections,
    ))
    return _merge_scopes(node, left, right)


def _keyed(left, right, pairs, kind, frame):
    if kind in {"semi", "anti"}:
        projections = _projections_from(left, "left")
    else:
        projections = (
            _projections_from(left, "left")
            + _projections_from(right, "right"))
    node = frame.add(dict(
        id=frame.nid("k"),
        op="keyed_join",
        left=left.node,
        right=right.node,
        kind=kind,
        join_pairs=[dict(left=item[0], right=item[1]) for item in pairs],
        projections=projections,
    ))
    if kind in {"semi", "anti"}:
        return _Scope(node, left.outputs, left.by_alias, left.by_name)
    return _merge_scopes(node, left, right)


def _theta(left, right, predicate, kind, frame):
    pred = _rewrite_join_predicate(predicate, left, right)
    if kind == "semi":
        selected = ", ".join(
            "l." + _q(name) + " AS " + _q(name) for name in left.outputs)
        sql = (
            "SELECT " + selected
            + ", l._m2s_z_weight AS _m2s_z_weight FROM _left AS l "
            "SEMI JOIN _right AS r ON " + pred)
    elif kind == "anti":
        selected = ", ".join(
            "l." + _q(name) + " AS " + _q(name) for name in left.outputs)
        sql = (
            "SELECT " + selected
            + ", l._m2s_z_weight AS _m2s_z_weight FROM _left AS l "
            "ANTI JOIN _right AS r ON " + pred)
    else:
        how = {
            "inner": "INNER JOIN",
            "left": "LEFT JOIN",
            "right": "RIGHT JOIN",
            "full": "FULL OUTER JOIN",
        }[kind]
        pieces = [
            "l." + _q(name) + " AS " + _q(name) for name in left.outputs
        ] + [
            "r." + _q(name) + " AS " + _q(name) for name in right.outputs
        ]
        if kind == "inner":
            weight = "l._m2s_z_weight * r._m2s_z_weight"
        elif kind == "left":
            weight = "l._m2s_z_weight * COALESCE(r._m2s_z_weight, 1)"
        elif kind == "right":
            weight = "COALESCE(l._m2s_z_weight, 1) * r._m2s_z_weight"
        else:
            weight = (
                "COALESCE(l._m2s_z_weight, 1) * COALESCE(r._m2s_z_weight, 1)")
        sql = (
            "SELECT " + ", ".join(pieces) + ", " + weight
            + " AS _m2s_z_weight FROM _left AS l " + how
            + " _right AS r ON " + pred)
    node = frame.add(dict(
        id=frame.nid("t"),
        op="theta_join",
        left=left.node,
        right=right.node,
        kind=kind if kind != "cross" else "inner",
        sql=sql.strip(),
        strategy="full_input_recompute",
        left_columns=list(left.outputs),
        right_columns=list(right.outputs),
    ))
    if kind in {"semi", "anti"}:
        return _Scope(node, left.outputs, left.by_alias, left.by_name)
    return _merge_scopes(node, left, right)


def _merge_scopes(node, left, right):
    by_alias = dict(left.by_alias)
    by_alias.update(right.by_alias)
    by_name = dict(left.by_name)
    for name, internal in right.by_name.items():
        if name in by_name:
            by_name[name] = None
        else:
            by_name[name] = internal
    return _Scope(
        node, list(left.outputs) + list(right.outputs), by_alias, by_name)


def _rewrite_join_predicate(predicate, left, right):
    def transform(node):
        if isinstance(node, exp.Column):
            if _in_scope(node, left):
                table = "l"
                name = _lookup(node, left)
            elif _in_scope(node, right):
                table = "r"
                name = _lookup(node, right)
            else:
                raise ValueError("join predicate column is not in either input")
            return exp.Column(
                this=exp.to_identifier(name, quoted=True),
                table=exp.to_identifier(table))
        return node
    return predicate.copy().transform(transform).sql(dialect="duckdb").strip()


def _apply_where(scope, where, sources, frame, read):
    if where is None:
        return scope
    expr = where.this if isinstance(where, exp.Where) else where
    remaining = []
    for term in _and_terms(expr):
        updated = _peel_term(scope, term, sources, frame, read)
        if updated is None:
            remaining.append(term)
        else:
            scope = updated
    if not remaining:
        return scope
    predicate = remaining[0]
    for extra in remaining[1:]:
        predicate = exp.and_(predicate, extra)
    if any(isinstance(node, (exp.Subquery, exp.Exists))
           for node in predicate.walk()):
        raise _Fallback()
    return _filter_scope(frame, scope, _rewrite(predicate, scope))


def _peel_term(scope, term, sources, frame, read):
    if isinstance(term, exp.Anonymous) and str(term.name).lower() == "m2s_watermark":
        return _insert_watermark(scope, term, frame)
    if isinstance(term, exp.Exists):
        return _exists_join(scope, term, sources, frame, read, anti=False)
    if isinstance(term, exp.Not) and isinstance(term.this, exp.Exists):
        return _exists_join(scope, term.this, sources, frame, read, anti=True)
    if isinstance(term, exp.Not) and isinstance(term.this, exp.In):
        raise _Fallback()
    if isinstance(term, exp.In) and term.args.get("query") is not None:
        return _in_join(scope, term, sources, frame, read)
    if any(isinstance(node, (exp.Subquery, exp.Exists)) for node in term.walk()):
        raise _Fallback()
    return None


def _exists_join(scope, exists, sources, frame, read, anti):
    inner = exists.this
    if not isinstance(inner, exp.Select):
        inner = exists.this.this if isinstance(exists.this, exp.Subquery) else None
    if not isinstance(inner, exp.Select):
        raise _Fallback()
    correlations, local = _split_correlation(inner.args.get("where"), scope)
    if not correlations:
        raise _Fallback()
    inner = inner.copy()
    _set_where(inner, local)
    right = _compile_from(inner, sources, frame, read)
    if local:
        pred = local[0]
        for extra in local[1:]:
            pred = exp.and_(pred, extra)
        right = _filter_scope(frame, right, _rewrite(pred, right))
    pairs = []
    for outer_col, inner_col in correlations:
        pairs.append((
            _lookup(outer_col, scope),
            _lookup(inner_col, right)))
    return _keyed(scope, right, pairs, "anti" if anti else "semi", frame)


def _in_join(scope, term, sources, frame, read):
    query = term.args["query"]
    inner = query.this if isinstance(query, exp.Subquery) else query
    if not isinstance(inner, exp.Select) or len(inner.expressions) != 1:
        raise _Fallback()
    correlations, local = _split_correlation(inner.args.get("where"), scope)
    inner = inner.copy()
    _set_where(inner, local)
    right_id, right_outs = _compile_query(inner, sources, frame, read)
    if len(right_outs) != 1:
        raise _Fallback()
    right = _Scope(
        right_id, right_outs,
        {("", right_outs[0]): right_outs[0]},
        {right_outs[0]: right_outs[0]})
    # The compiled subquery exposes public names. Correlation columns inside
    # it were renamed, so equality on those columns is only available when
    # the subquery select list is the join key. Correlated IN falls back
    # unless the correlation columns are exactly the selected output.
    if correlations:
        raise _Fallback()
    left_expr = term.this
    if not isinstance(left_expr, exp.Column):
        raise _Fallback()
    pairs = [(_lookup(left_expr, scope), right_outs[0])]
    return _keyed(scope, right, pairs, "semi", frame)


def _split_correlation(where, outer):
    if where is None:
        return [], []
    expr = where.this if isinstance(where, exp.Where) else where
    correlations = []
    local = []
    for term in _and_terms(expr):
        if (
            isinstance(term, exp.EQ)
            and isinstance(term.this, exp.Column)
            and isinstance(term.expression, exp.Column)
        ):
            left_outer = _is_outer(term.this, outer)
            right_outer = _is_outer(term.expression, outer)
            if left_outer and not right_outer:
                correlations.append((term.this, term.expression))
                continue
            if right_outer and not left_outer:
                correlations.append((term.expression, term.this))
                continue
        if _references_outer(term, outer):
            raise _Fallback()
        local.append(term)
    return correlations, local


def _set_where(select, terms):
    if not terms:
        select.set("where", None)
        return
    expr = terms[0]
    for extra in terms[1:]:
        expr = exp.and_(expr, extra)
    select.set("where", exp.Where(this=expr))


def _is_outer(column, outer):
    try:
        return _in_scope(column, outer)
    except ValueError:
        return False


def _references_outer(term, outer):
    for node in term.walk():
        if isinstance(node, exp.Column) and _is_outer(node, outer):
            return True
    return False


def _filter_scope(frame, scope, predicate):
    node = frame.add(dict(
        id=frame.nid("f"), op="filter", input=scope.node,
        predicate=predicate.strip(), scope="predicate"))
    return _Scope(node, scope.outputs, scope.by_alias, scope.by_name)


def _compile_aggregate(scope, tree, frame):
    group = tree.args.get("group")
    select_has_agg = any(_is_agg(item) for item in tree.expressions)
    having = tree.args.get("having")
    if group is None and not select_has_agg and having is None:
        return None
    window_call = _find_m2s_window(tree)
    if window_call is not None:
        return _compile_window_agg(scope, tree, frame, window_call)
    sets = _grouping_sets(group, scope)
    if sets is None:
        sets = [[]]
    outputs = _select_slots(tree, scope, frame)
    grouped_cols = {column for item in sets for column in item}
    for kind, _public, internal, _spec in outputs:
        if kind == "key" and internal not in grouped_cols:
            raise ValueError("select column is not grouped")
    branches = []
    for keys in sets:
        aggregates = []
        seen_outputs = set()
        for slot in outputs:
            if slot[0] != "agg" or slot[3]["output"] in seen_outputs:
                continue
            aggregates.append(slot[3])
            seen_outputs.add(slot[3]["output"])
        if not aggregates:
            raise ValueError("GROUP BY without an aggregate is unsupported")
        node = frame.add(dict(
            id=frame.nid("a"),
            op="aggregate",
            input=scope.node,
            group_keys=list(keys),
            aggregates=aggregates,
        ))
        expressions = []
        produced = []
        for kind, public, internal, spec in outputs:
            produced.append(public)
            if kind == "key":
                if internal in keys:
                    expr = _q(internal) + " AS " + _q(public)
                else:
                    expr = "NULL AS " + _q(public)
            else:
                expr = _q(spec["output"]) + " AS " + _q(public)
            expressions.append(dict(output=public, expression=expr))
        node = frame.add(dict(
            id=frame.nid("p"), op="project", input=node,
            expressions=expressions))
        branches.append((node, produced))
    node, produced = branches[0]
    for other, _names in branches[1:]:
        node = frame.add(dict(
            id=frame.nid("set"), op="set_op",
            left=node, right=other, mode="union_all"))
    if having is not None:
        having_expr = having.this if isinstance(having, exp.Having) else having
        post = _Scope(
            node, produced,
            {("", name): name for name in produced},
            {name: name for name in produced})
        if any(isinstance(item, exp.AggFunc) for item in having_expr.walk()):
            raise ValueError("HAVING must use select aliases of aggregates")
        node_id = _filter_scope(
            frame, post, _rewrite(having_expr, post)).node
        node = node_id
    return _Scope(
        node, produced,
        {("", name): name for name in produced},
        {name: name for name in produced})


def _select_slots(tree, scope, frame):
    slots = []
    seen_aggs = {}
    for item in tree.expressions:
        public = str(item.alias_or_name or "").strip()
        target = item.this if isinstance(item, exp.Alias) else item
        if isinstance(target, exp.Star):
            raise ValueError("SELECT * cannot be combined with GROUP BY")
        if _is_agg(target):
            spec = _agg_spec(target, scope, frame)
            key = stream_ir.canonical_bytes(spec)
            if key not in seen_aggs:
                seen_aggs[key] = spec
            spec = seen_aggs[key]
            if not public or public == spec["output"]:
                public = spec["output"]
            slots.append(("agg", public, spec["output"], spec))
            continue
        if isinstance(target, exp.Column):
            internal = _lookup(target, scope)
            if not public:
                public = target.name
            slots.append(("key", public, internal, None))
            continue
        raise ValueError(
            "grouped select list must be grouping columns and aggregates")
    return slots


def _grouping_sets(group, scope):
    if group is None:
        return [[]]
    if group.args.get("rollup"):
        columns = [
            _lookup(item, scope)
            for item in group.args["rollup"][0].expressions
        ]
        return [columns[:index] for index in range(len(columns), -1, -1)]
    if group.args.get("cube"):
        columns = [
            _lookup(item, scope)
            for item in group.args["cube"][0].expressions
        ]
        sets = []
        total = 1 << len(columns)
        for mask in range(total - 1, -1, -1):
            sets.append([
                columns[index] for index in range(len(columns))
                if mask & (1 << index)
            ])
        return sets
    if group.args.get("grouping_sets"):
        sets = []
        for item in group.args["grouping_sets"][0].expressions:
            sets.append(_grouping_columns(item, scope))
        return sets
    return [[_lookup(item, scope) for item in group.expressions]]


def _grouping_columns(item, scope):
    if isinstance(item, exp.Tuple):
        return [_lookup(child, scope) for child in item.expressions]
    if isinstance(item, exp.Paren):
        return _grouping_columns(item.this, scope)
    if isinstance(item, exp.Column):
        return [_lookup(item, scope)]
    if item is None or (isinstance(item, exp.Tuple) and not item.expressions):
        return []
    raise ValueError("unsupported GROUPING SETS entry")


def _is_agg(node):
    target = node.this if isinstance(node, exp.Alias) else node
    return isinstance(target, (exp.AggFunc, exp.GroupConcat))


def _agg_spec(target, scope, frame):
    if isinstance(target, exp.Alias):
        target = target.this
    output = frame.col()
    if isinstance(target, exp.GroupConcat):
        return _string_agg_spec(target, scope, output)
    if isinstance(target, exp.Count) and isinstance(target.this, exp.Distinct):
        column = target.this.expressions[0]
        if not isinstance(column, exp.Column):
            raise ValueError("COUNT DISTINCT requires one column")
        return dict(
            output=output, function="count_distinct",
            input=_lookup(column, scope))
    if isinstance(target, exp.Count):
        if isinstance(target.this, exp.Star):
            source = "*"
        elif isinstance(target.this, exp.Column):
            source = _lookup(target.this, scope)
        else:
            raise ValueError("COUNT input must be * or a column")
        return dict(output=output, function="count", input=source)
    functions = {
        exp.Sum: "sum",
        exp.Avg: "avg",
        exp.Min: "min",
        exp.Max: "max",
    }
    function = functions.get(type(target))
    if function is None:
        raise ValueError("unsupported aggregate")
    if not isinstance(target.this, exp.Column):
        raise ValueError(function + " input must be a column")
    return dict(
        output=output, function=function, input=_lookup(target.this, scope))


def _string_agg_spec(target, scope, output):
    separator = target.args.get("separator")
    if not isinstance(separator, exp.Literal) or not separator.is_string:
        raise ValueError("STRING_AGG requires a string separator")
    order = target.this if isinstance(target.this, exp.Order) else None
    if order is None:
        raise ValueError(
            "STRING_AGG without ORDER BY is rejected as non-deterministic")
    if not isinstance(order.this, exp.Column):
        raise ValueError("STRING_AGG input must be a column")
    order_by = []
    for item in order.expressions:
        if not isinstance(item, exp.Ordered) or not isinstance(
            item.this, exp.Column
        ):
            raise ValueError("STRING_AGG ORDER BY must be columns")
        order_by.append(dict(
            column=_lookup(item.this, scope),
            desc=bool(item.args.get("desc")),
            nulls_first=bool(item.args.get("nulls_first")),
        ))
    if not order_by:
        raise ValueError(
            "STRING_AGG without ORDER BY is rejected as non-deterministic")
    return dict(
        output=output,
        function="string_agg",
        input=_lookup(order.this, scope),
        separator=str(separator.this),
        order_by=order_by,
    )


def _compile_projection(scope, tree, frame):
    if tree.args.get("group") is not None or any(
        _is_agg(item) for item in tree.expressions
    ):
        return scope
    expressions = []
    outputs = []
    by_name = {}
    for item in tree.expressions:
        target = item.this if isinstance(item, exp.Alias) else item
        public = str(item.alias_or_name or "").strip()
        if isinstance(target, exp.Star):
            for internal in _star_columns(scope, target):
                public = _public_of(scope, internal)
                expressions.append(dict(
                    output=public,
                    expression=_q(internal) + " AS " + _q(public)))
                outputs.append(public)
                by_name[public] = public
            continue
        if isinstance(target, (exp.Subquery, exp.AggFunc)):
            raise _Fallback()
        sql = _rewrite(target, scope)
        if not public:
            public = target.name if isinstance(target, exp.Column) else ""
        if not public:
            raise ValueError("select expression requires an alias")
        expressions.append(dict(
            output=public,
            expression=sql + " AS " + _q(public)))
        outputs.append(public)
        by_name[public] = public
    if len(outputs) != len(set(outputs)):
        raise ValueError("select output names must be unique")
    node = frame.add(dict(
        id=frame.nid("p"), op="project", input=scope.node,
        expressions=expressions))
    return _Scope(
        node, outputs,
        {("", name): name for name in outputs},
        by_name)


def _public_of(scope, internal):
    found = [
        column for (_alias, column), name in scope.by_alias.items()
        if name == internal
    ]
    if len(found) != 1:
        raise ValueError("SELECT * cannot name an output column")
    return found[0]


def _star_columns(scope, star):
    table = star.args.get("table")
    if table is not None:
        alias = str(table.name if isinstance(table, exp.Identifier) else table)
        chosen = [
            internal for (name, _column), internal in scope.by_alias.items()
            if name == alias
        ]
        if not chosen:
            raise ValueError("unknown star alias: " + alias)
        return chosen
    if any(value is None for value in scope.by_name.values()):
        raise ValueError("SELECT * is ambiguous; list the columns")
    return list(scope.outputs)


def _rewrite(expression, scope):
    def transform(node):
        if isinstance(node, exp.Subquery):
            raise _Fallback()
        if isinstance(node, exp.Column):
            name = _lookup(node, scope)
            return exp.Column(this=exp.to_identifier(name, quoted=True))
        return node
    return expression.copy().transform(transform).sql(dialect="duckdb").strip()


def _in_scope(column, scope):
    if not isinstance(column, exp.Column):
        return False
    table = str(column.table or "")
    name = column.name
    if table:
        return (table, name) in scope.by_alias
    return name in scope.by_name and scope.by_name[name] is not None


def _lookup(column, scope):
    if not isinstance(column, exp.Column):
        raise ValueError("expected a column")
    table = str(column.table or "")
    name = column.name
    if table:
        key = (table, name)
        if key not in scope.by_alias:
            raise ValueError("unknown column " + table + "." + name)
        return scope.by_alias[key]
    if name not in scope.by_name or scope.by_name[name] is None:
        raise ValueError("ambiguous or unknown column " + name)
    return scope.by_name[name]


def _bind_ctes(with_, sources, frame, read):
    recursive = bool(with_.args.get("recursive"))
    for cte in with_.expressions:
        alias = str(cte.alias)
        body = cte.this
        if recursive and isinstance(body, exp.Union):
            scope = _compile_recursive_cte(body, alias, sources, frame, read)
        else:
            node, outputs = _compile_query(body, sources, frame, read)
            scope = _rename_scope(frame, node, outputs, alias, outputs)
        frame.ctes[alias] = scope


def _compile_recursive_cte(body, alias, sources, frame, read):
    if not isinstance(body, exp.Union):
        raise ValueError("recursive CTE requires UNION")
    distinct = bool(body.args.get("distinct"))
    seed_sql = body.this.sql(dialect="duckdb").strip()
    step = body.expression.copy()
    for table in step.find_all(exp.Table):
        if table.name == alias:
            table.set("this", exp.to_identifier("_delta"))
    step_sql = step.sql(dialect="duckdb").strip()
    tables = []
    for table in body.find_all(exp.Table):
        if table.name in sources and table.name not in tables:
            tables.append(table.name)
    ids = []
    columns = {}
    for name in tables:
        frame.touch(name)
        node = frame.add(dict(
            id=frame.nid("in"), op="input", input=name))
        ids.append(node)
        columns[node] = list(sources[name])
    outputs = _output_names(body.this) or ["expr"]
    node = frame.add(dict(
        id=frame.nid("rec"),
        op="recursive_cte",
        sources=ids,
        tables=list(tables),
        input_columns=columns,
        columns=list(outputs),
        seed_sql=seed_sql,
        step_sql=step_sql,
        max_iterations=32,
        set_mode="union" if distinct else "union_all",
        strategy="semi_naive",
    ))
    by_alias = {(alias, name): name for name in outputs}
    by_name = {name: name for name in outputs}
    return _Scope(node, outputs, by_alias, by_name)


def _insert_watermark(scope, term, frame):
    args = list(term.expressions or ())
    if len(args) < 3:
        raise ValueError(
            "m2s_watermark(time, lateness, policy) is incomplete")
    node = frame.add(dict(
        id=frame.nid("wm"),
        op="watermark",
        input=scope.node,
        time=_lookup(args[0], scope),
        lateness=int(args[1].this),
        policy=str(args[2].this),
    ))
    outputs = list(scope.outputs)
    by_alias = dict(scope.by_alias)
    by_name = dict(scope.by_name)
    if node and str(args[2].this) == "side":
        outputs.append("_late")
        by_name["_late"] = "_late"
    return _Scope(node, outputs, by_alias, by_name)


def _find_m2s_window(tree):
    group = tree.args.get("group")
    if group is None:
        return None
    for item in group.expressions:
        if isinstance(item, exp.Anonymous) and str(item.name).lower().startswith(
            "m2s_"
        ):
            return item
    return None


def _compile_window_agg(scope, tree, frame, call):
    name = str(call.name).lower()
    args = list(call.expressions)
    time_name = _lookup(args[0], scope)
    kind = {
        "m2s_tumble": "tumble",
        "m2s_hop": "hop",
        "m2s_session": "session",
        "m2s_cumulate": "cumulate",
    }.get(name)
    if kind is None:
        raise ValueError("unsupported window function " + name)
    size = slide = step = gap = None
    if kind == "tumble":
        size = int(args[1].this)
    elif kind == "hop":
        size = int(args[1].this)
        slide = int(args[2].this)
    elif kind == "session":
        gap = int(args[1].this)
        size = gap
    else:
        step = int(args[1].this)
        size = int(args[2].this)
    keys = []
    for item in tree.args["group"].expressions:
        if item is call or (
            isinstance(item, exp.Anonymous)
            and str(item.name).lower() == name
        ):
            continue
        keys.append(_lookup(item, scope))
    aggregates = []
    if kind != "session":
        for item in tree.expressions:
            target = item.this if isinstance(item, exp.Alias) else item
            if isinstance(target, exp.Anonymous):
                continue
            if isinstance(target, exp.Column):
                continue
            if not _is_agg(target):
                raise ValueError("window select list is not an aggregate")
            spec = _agg_spec(target, scope, frame)
            public = str(item.alias_or_name or spec["output"])
            spec = dict(spec)
            spec["output"] = public
            aggregates.append(spec)
    else:
        aggregates = [dict(output="n", function="count", input="*")]
    lateness = 0
    late_policy = "recompute"
    ttl = None
    node = frame.add(dict(
        id=frame.nid("win"),
        op="window_agg",
        input=scope.node,
        kind=kind,
        time=time_name,
        size=int(size),
        slide=slide,
        step=step,
        gap=gap,
        lateness=lateness,
        late_policy=late_policy,
        ttl=ttl,
        keys=keys,
        aggregates=aggregates,
    ))
    outputs = []
    expressions = []
    for item in tree.expressions:
        target = item.this if isinstance(item, exp.Alias) else item
        public = str(item.alias_or_name or "").strip()
        if isinstance(target, exp.Column):
            internal = _lookup(target, scope)
            if not public:
                public = target.name
            expressions.append(dict(
                output=public,
                expression=_q(internal) + " AS " + _q(public)))
            outputs.append(public)
            continue
        if isinstance(target, exp.Anonymous):
            expressions.append(dict(
                output="w_start",
                expression='"w_start" AS "w_start"'))
            expressions.append(dict(
                output="w_end",
                expression='"w_end" AS "w_end"'))
            outputs.extend(["w_start", "w_end"])
            continue
        if _is_agg(target):
            if not public:
                public = "n"
            expressions.append(dict(
                output=public,
                expression=_q(public) + " AS " + _q(public)))
            outputs.append(public)
    if kind == "session" and "n" not in outputs:
        expressions.append(dict(
            output="n", expression='"n" AS "n"'))
        outputs.append("n")
    if "w_start" not in outputs:
        expressions.extend([
            dict(output="w_start", expression='"w_start" AS "w_start"'),
            dict(output="w_end", expression='"w_end" AS "w_end"'),
        ])
        outputs.extend(["w_start", "w_end"])
    projected = frame.add(dict(
        id=frame.nid("p"), op="project", input=node, expressions=expressions))
    return _Scope(
        projected, outputs,
        {("", name): name for name in outputs},
        {name: name for name in outputs})


def _compile_over(scope, tree, frame):
    cloned = tree.copy()
    cloned.set("with_", None)
    cloned.set("where", None)
    cloned.set("joins", None)
    cloned.set(
        "from_",
        exp.From(this=exp.Table(this=exp.to_identifier("_input"))))
    aliased = []
    for item in cloned.expressions:
        public = str(item.alias_or_name or "").strip()
        target = item.this if isinstance(item, exp.Alias) else item
        aliased.append(exp.alias_(target, public, quoted=True))
    cloned.set("expressions", aliased)

    def transform(node):
        if isinstance(node, exp.Column):
            return exp.Column(this=exp.to_identifier(
                _lookup(node, scope), quoted=True))
        return node

    cloned = cloned.transform(transform)
    partitions = None
    for window in tree.find_all(exp.Window):
        parts = [
            _lookup(column, scope)
            for column in (window.args.get("partition_by") or [])
        ]
        if partitions is None:
            partitions = parts
        elif partitions != parts:
            partitions = []
            break
    if partitions is None:
        partitions = []
    node = frame.add(dict(
        id=frame.nid("over"),
        op="over",
        input=scope.node,
        partition=list(partitions),
        columns=list(scope.outputs),
        sql=cloned.sql(dialect="duckdb").strip(),
        strategy="affected_partition_recompute",
    ))
    outputs = []
    for item in tree.expressions:
        public = str(item.alias_or_name or "").strip()
        if not public:
            raise ValueError("OVER output requires a name")
        outputs.append(public)
    return node, outputs


def _compile_topn(scope, tree, frame):
    limit = int(tree.args["limit"].expression.this)
    order = []
    for item in tree.args["order"].expressions:
        order.append(dict(
            column=_lookup(item.this, scope),
            desc=bool(item.args.get("desc")),
            nulls_first=bool(item.args.get("nulls_first")),
        ))
    node = frame.add(dict(
        id=frame.nid("top"),
        op="topn",
        input=scope.node,
        partition=[],
        columns=list(scope.outputs),
        order=order,
        limit=limit,
        strategy="affected_partition_recompute",
    ))
    return node, list(scope.outputs)
