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


def compile_sql(sql, sources, read="duckdb", nondeterministic="reject",
               on_unsupported="raise", force_recompute=False):
    """Compile one statement to a stream plan.

    Returns plan, output column names, and the read dialect. The default
    dialect is duckdb. Nondeterministic functions are rejected unless
    nondeterministic="timed_recompute", which marks the plan explicitly.
    """
    if read not in READ_DIALECTS:
        raise ValueError("sql read dialect must be duckdb or mysql")
    if nondeterministic not in {"reject", "timed_recompute"}:
        raise ValueError("nondeterministic mode must be reject or timed_recompute")
    if on_unsupported not in {"raise", "recompute"}:
        raise ValueError("on_unsupported must be raise or recompute")
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
    if force_recompute:
        node, outputs = _compile_recompute(text, sources, frame, read)
        plan = stream_ir._plan(frame.inputs, frame.nodes, node)
        return dict(plan=plan, outputs=list(outputs), read=read)
    try:
        node, outputs = _compile_query(tree, sources, frame, read)
    except (_Fallback, ValueError) as exc:
        if on_unsupported != "recompute":
            raise
        frame = _Frame()
        try:
            node, outputs = _compile_recompute(text, sources, frame, read)
        except ValueError:
            raise exc
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


def _output_name(item):
    if isinstance(item, exp.Alias):
        return str(item.alias).strip()
    target = item
    if isinstance(target, exp.Count) and isinstance(target.this, exp.Star):
        return "count_star()"
    if isinstance(item, exp.Column):
        return item.name
    try:
        text = item.sql(dialect="duckdb").strip().lower()
    except Exception:
        text = ""
    return text


def _output_names(tree):
    names = []
    for item in tree.expressions:
        name = _output_name(item)
        if not name or name == "*":
            return []
        names.append(name)
    return names


def _unordered_limit_is_noop(tree, grouped):
    if grouped is not None and tree.args.get("group") is None:
        return True
    origin = tree.args.get("from_")
    if origin is None:
        return False
    items = [origin.this]
    for join in tree.args.get("joins") or ():
        if str(join.args.get("kind") or "") or str(join.args.get("side") or ""):
            return False
        items.append(join.this)
    if not items:
        return False
    for item in items:
        if not isinstance(item, exp.Subquery):
            return False
        inner = item.this
        if not isinstance(inner, exp.Select):
            return False
        if inner.args.get("group") is not None:
            return False
        if not any(_contains_agg(expr) for expr in inner.expressions):
            return False
    return True


def _detach_expr(scope, expr, frame):
    sources = frame.sources
    read = frame.read
    while True:
        found = [node for node in expr.walk() if isinstance(node, exp.Subquery)]
        if not found:
            return scope, expr
        sql = found[0].sql()
        scope, name = _attach_scalar(scope, found[0], sources, frame, read)
        def replace(node, sql=sql, name=name):
            if isinstance(node, exp.Subquery) and node.sql() == sql:
                return exp.column(name, quoted=True)
            return node
        expr = expr.transform(replace)
    return scope, expr


def _compile_select(tree, sources, frame, read):
    if tree.args.get("offset") is not None:
        raise ValueError("OFFSET is not part of this relational subset")
    if tree.args.get("with_") is not None:
        _bind_ctes(tree.args["with_"], sources, frame, read)
    own_window = False
    for item in tree.expressions:
        if any(isinstance(node, exp.Window) for node in item.walk()):
            own_window = True
            break
    frame.sources = sources
    frame.read = read
    scope = _compile_from(tree, sources, frame, read)
    scope = _apply_where(
        scope, tree.args.get("where"), sources, frame, read)
    replaced = []
    for item in tree.expressions:
        if isinstance(item, exp.Alias):
            scope, new = _detach_expr(scope, item.this, frame)
            item.set("this", new)
            replaced.append(item)
        else:
            scope, new = _detach_expr(scope, item, frame)
            replaced.append(new)
    tree.set("expressions", replaced)
    if own_window:
        return _compile_over(scope, tree, frame)
    grouped = _compile_aggregate(scope, tree, frame, sources, read)
    if grouped is not None:
        scope = grouped
    if tree.args.get("limit") is not None:
        if tree.args.get("order") is not None:
            scope = _compile_projection(scope, tree, frame)
            return _compile_topn(scope, tree, frame)
        # One global aggregate row is unchanged by LIMIT. A FROM list of
        # scalar aggregate subqueries is also one row. Any other unordered
        # LIMIT stays rejected because the kept rows are not stable.
        if not _unordered_limit_is_noop(tree, grouped):
            raise ValueError("LIMIT without ORDER BY is rejected")
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
    bare = [origin.this]
    explicit = []
    for join in tree.args.get("joins") or ():
        if (
            join.args.get("on") is None
            and not str(join.args.get("kind") or "")
            and not str(join.args.get("side") or "")
        ):
            bare.append(join.this)
        else:
            explicit.append(join)
    if len(bare) > 1:
        scope = _compile_comma(bare, tree, sources, frame, read)
    else:
        scope = _compile_from_item(bare[0], sources, frame, read)
    for join in explicit:
        right = _compile_from_item(join.this, sources, frame, read)
        scope = _compile_join(scope, right, join, frame)
    return scope


def _compile_comma(items, tree, sources, frame, read):
    scopes = [
        _compile_from_item(item, sources, frame, read) for item in items
    ]
    where = tree.args.get("where")
    terms = []
    if where is not None:
        expr = where.this if isinstance(where, exp.Where) else where
        terms = _and_terms(expr)
    used = set()
    pending = list(enumerate(scopes[1:], start=1))
    scope = scopes[0]
    while pending:
        chosen = 0
        for index, (_origin, nxt) in enumerate(pending):
            if any(
                i not in used and _equality_pair(term, scope, nxt)
                for i, term in enumerate(terms)
            ):
                chosen = index
                break
        _origin, nxt = pending.pop(chosen)
        for index, term in enumerate(terms):
            if index in used or _has_subquery(term):
                continue
            if _refs_only(term, nxt):
                nxt = _filter_scope(frame, nxt, _rewrite(term, nxt))
                used.add(index)
        pairs = []
        extras = []
        for index, term in enumerate(terms):
            if index in used or _has_subquery(term):
                continue
            pair = _equality_pair(term, scope, nxt)
            if pair:
                pairs.append(pair)
                used.add(index)
                continue
            if _refs_only_two(term, scope, nxt):
                extras.append(term)
                used.add(index)
        if pairs and not extras:
            scope = _equi(scope, nxt, pairs, frame)
        elif pairs:
            scope = _equi(scope, nxt, pairs, frame)
            pred = extras[0]
            for extra in extras[1:]:
                pred = exp.and_(pred, extra)
            scope = _filter_scope(frame, scope, _rewrite(pred, scope))
        elif extras:
            split = None
            if len(extras) == 1:
                split = _split_or_equi(extras[0], scope, nxt)
            if split:
                common, residual = split
                scope = _equi(scope, nxt, common, frame)
                scope = _filter_scope(frame, scope, _rewrite(residual, scope))
            else:
                expr_pairs = []
                left_scope, right_scope = scope, nxt
                rest = []
                for term in extras:
                    built = _expression_equi(term, left_scope, right_scope, frame)
                    if built is not None:
                        extra, left_scope, right_scope = built
                        expr_pairs.extend(extra)
                    else:
                        rest.append(term)
                if expr_pairs:
                    scope = _equi(left_scope, right_scope, expr_pairs, frame)
                    if rest:
                        pred = rest[0]
                        for extra in rest[1:]:
                            pred = exp.and_(pred, extra)
                        scope = _filter_scope(
                            frame, scope, _rewrite(pred, scope))
                else:
                    pred = extras[0]
                    for extra in extras[1:]:
                        pred = exp.and_(pred, extra)
                    scope = _theta(scope, nxt, pred, "inner", frame)
        else:
            rescued = []
            for index, term in enumerate(terms):
                if index in used or _has_subquery(term):
                    continue
                found = _common_equi_pairs(term, scope, nxt)
                if found:
                    rescued.extend(found)
            if rescued:
                scope = _equi(scope, nxt, rescued, frame)
            else:
                scope = _equi_on_one(scope, nxt, frame)
    for index, term in enumerate(terms):
        if index in used or _has_subquery(term):
            continue
        if _refs_only(term, scope):
            scope = _filter_scope(frame, scope, _rewrite(term, scope))
            used.add(index)
    remaining = [term for index, term in enumerate(terms) if index not in used]
    if remaining:
        expr = remaining[0]
        for extra in remaining[1:]:
            expr = exp.and_(expr, extra)
        tree.set("where", exp.Where(this=expr))
    else:
        tree.set("where", None)
    return scope


def _has_subquery(term):
    return any(
        isinstance(node, (exp.Subquery, exp.Exists)) for node in term.walk()
    )


def _or_terms(node):
    if isinstance(node, exp.Paren):
        return _or_terms(node.this)
    if isinstance(node, exp.Or):
        return _or_terms(node.this) + _or_terms(node.expression)
    return [node]


def _common_equi_pairs(term, left, right):
    """Equalities shared by every branch of an OR, limited to these inputs."""
    branches = _or_terms(term)
    if len(branches) < 2:
        return None
    common = None
    for branch in branches:
        found = []
        for piece in _and_terms(branch):
            pair = _equality_pair(piece, left, right)
            if pair and pair not in found:
                found.append(pair)
        if not found:
            return None
        keys = set(found)
        common = keys if common is None else common & keys
        if not common:
            return None
    return list(common)


def _equi_on_one(left, right, frame):
    """Join two inputs that have no equi key by a constant key.

    This is the cross product of the two inputs. It stays an equi join so
    the plan does not drop into a full-input theta recompute.
    """
    left, left_key = _materialize_expr(left, exp.Literal.number(1), frame)
    right, right_key = _materialize_expr(right, exp.Literal.number(1), frame)
    return _equi(left, right, [(left_key, right_key)], frame)


def _expression_equi(term, left, right, frame):
    """Turn `expr(left) = expr(right)` into an equi join on projected keys."""
    if not isinstance(term, exp.EQ):
        return None

    def place(expr):
        columns = [node for node in expr.walk() if isinstance(node, exp.Column)]
        if not columns:
            return None
        if all(_in_scope(column, left) for column in columns):
            return "left"
        if all(_in_scope(column, right) for column in columns):
            return "right"
        return None

    left_side = place(term.this)
    right_side = place(term.expression)
    if left_side is None or right_side is None or left_side == right_side:
        return None
    if left_side == "right":
        term = exp.EQ(this=term.expression, expression=term.this)
    if isinstance(term.this, exp.Column):
        left_name = _lookup(term.this, left)
    else:
        left, left_name = _materialize_expr(left, term.this, frame)
    if isinstance(term.expression, exp.Column):
        right_name = _lookup(term.expression, right)
    else:
        right, right_name = _materialize_expr(right, term.expression, frame)
    return [(left_name, right_name)], left, right


def _split_or_equi(node, left, right):
    branches = _or_terms(node)
    if len(branches) < 2:
        return None
    common = None
    leftovers = []
    for branch in branches:
        pairs = []
        rest = []
        for term in _and_terms(branch):
            pair = _equality_pair(term, left, right)
            if pair:
                pairs.append(pair)
            else:
                rest.append(term)
        key = tuple(sorted(pairs))
        if not key:
            return None
        if common is None:
            common = key
        elif key != common:
            return None
        leftovers.append(rest)
    expr = None
    for rest in leftovers:
        if not rest:
            return None
        branch = rest[0]
        for extra in rest[1:]:
            branch = exp.and_(branch, extra)
        expr = branch if expr is None else exp.or_(expr, branch)
    return list(common), expr


def _refs_only(term, scope):
    columns = [node for node in term.walk() if isinstance(node, exp.Column)]
    if not columns:
        return False
    return all(_in_scope(column, scope) for column in columns)


def _refs_only_two(term, left, right):
    columns = [node for node in term.walk() if isinstance(node, exp.Column)]
    if not columns:
        return False
    return all(
        _in_scope(column, left) or _in_scope(column, right)
        for column in columns
    )


def _compile_from_item(item, sources, frame, read):
    if isinstance(item, exp.Subquery):
        alias_expr = item.args.get("alias")
        alias = str(item.alias or "").strip()
        columns = []
        if alias_expr is not None:
            alias = str(alias_expr.this.name)
            columns = [str(column.name) for column in alias_expr.args.get("columns") or ()]
        if not alias:
            raise ValueError("subquery in FROM requires an alias")
        node, outputs = _compile_query(item.this, sources, frame, read)
        if columns:
            if len(columns) != len(outputs):
                raise ValueError("subquery column list does not match its outputs")
            expressions = [
                dict(output=new, expression=_q(old) + " AS " + _q(new))
                for old, new in zip(outputs, columns)
            ]
            node = frame.add(dict(
                id=frame.nid("p"), op="project", input=node,
                expressions=expressions))
            outputs = columns
        return _rename_scope(frame, node, outputs, alias, outputs)
    if isinstance(item, exp.Table):
        if item.name in frame.ctes:
            return _alias_scope(
                frame.ctes[item.name], str(item.alias or item.name))
        return _compile_table(item, sources, frame)
    raise ValueError("unsupported FROM item")


def _compile_table(table, sources, frame):
    name = table.name
    if name not in sources:
        raise ValueError("unknown SQL source: " + name)
    alias = str(table.alias or name).lower()
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


def _project_back(scope, outputs, by_alias, by_name, frame):
    expressions = [
        dict(output=name, expression=_q(name) + " AS " + _q(name))
        for name in outputs
    ]
    node = frame.add(dict(
        id=frame.nid("p"), op="project", input=scope.node, expressions=expressions))
    return _Scope(node, list(outputs), dict(by_alias), dict(by_name))


def _alias_scope(scope, alias):
    by_alias = {}
    alias = str(alias).lower()
    for (_previous, column), internal in scope.by_alias.items():
        by_alias[(alias, column)] = internal
    return _Scope(scope.node, list(scope.outputs), by_alias, dict(scope.by_name))


def _rename_scope(frame, node, outputs, alias, public_names):
    alias = str(alias).lower()
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
    kept = []
    for term in residual:
        built = _expression_equi(term, left, right, frame)
        if built is not None:
            extra, left, right = built
            pairs.extend(extra)
            continue
        if _refs_only(term, right):
            right = _filter_scope(frame, right, _rewrite(term, right))
        else:
            kept.append(term)
    residual = kept
    if residual or not pairs:
        predicate = on.this if isinstance(on, exp.Where) else on
        return _theta(left, right, predicate, join_kind, frame)
    if join_kind == "inner" and not residual:
        return _equi(left, right, pairs, frame)
    return _keyed(left, right, pairs, join_kind, frame)


def _and_terms(node):
    if isinstance(node, exp.Paren):
        return _and_terms(node.this)
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


def _freshen_scope(frame, scope):
    mapping = {}
    expressions = []
    outputs = []
    for name in scope.outputs:
        fresh = frame.col()
        mapping[name] = fresh
        expressions.append(dict(
            output=fresh, expression=_q(name) + " AS " + _q(fresh)))
        outputs.append(fresh)
    by_alias = {}
    for key, internal in scope.by_alias.items():
        if internal in mapping:
            by_alias[key] = mapping[internal]
    by_name = {}
    for public, internal in scope.by_name.items():
        if internal in mapping and public not in by_name:
            by_name[public] = mapping[internal]
    for name in outputs:
        by_name.setdefault(name, name)
    node = frame.add(dict(
        id=frame.nid("p"), op="project", input=scope.node,
        expressions=expressions))
    return _Scope(node, outputs, by_alias, by_name), mapping


def _equi(left, right, pairs, frame):
    if set(left.outputs) & set(right.outputs):
        right, mapping = _freshen_scope(frame, right)
        pairs = [(left_name, mapping[right_name]) for left_name, right_name in pairs]
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


def _peel_or(scope, branches, sources, frame, read):
    """A row matches an OR when it matches any branch. Union, then distinct."""
    arms = []
    for branch in branches:
        arms.append(_apply_where(scope, branch, sources, frame, read))
    node = arms[0].node
    for arm in arms[1:]:
        node = frame.add(dict(
            id=frame.nid("set"), op="set_op",
            left=node, right=arm.node, mode="union"))
    return _Scope(node, list(scope.outputs), scope.by_alias, scope.by_name)


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
        if term.this.args.get("query") is not None:
            return _in_join(scope, term.this, sources, frame, read, anti=True)
        raise _Fallback()
    if isinstance(term, exp.In) and term.args.get("query") is not None:
        return _in_join(scope, term, sources, frame, read, anti=False)
    branches = _or_terms(term)
    if len(branches) > 1:
        return _peel_or(scope, branches, sources, frame, read)
    compared = _scalar_eq(term)
    if compared is not None:
        return _scalar_eq_join(scope, compared, sources, frame, read)
    nested = [node for node in term.walk() if isinstance(node, exp.Subquery)]
    if len(nested) > 1:
        pred = term
        for _index in range(len(nested)):
            current = [node for node in pred.walk() if isinstance(node, exp.Subquery)]
            if not current:
                break
            target = current[0].sql()
            scope, name = _attach_scalar(scope, current[0], sources, frame, read)
            def replace(node, target=target, name=name):
                if isinstance(node, exp.Subquery) and node.sql() == target:
                    return exp.column(name, quoted=True)
                return node
            pred = pred.copy().transform(replace)
        return _filter_scope(frame, scope, _rewrite(pred, scope))
    if isinstance(term, (exp.EQ, exp.LT, exp.GT, exp.LTE, exp.GTE, exp.Between)):
        nested = [
            node for node in term.walk() if isinstance(node, exp.Subquery)
        ]
        if len(nested) == 1:
            scope, name = _attach_scalar(scope, nested[0], sources, frame, read)
            def replace(node):
                if isinstance(node, exp.Subquery):
                    return exp.column(name, quoted=True)
                return node
            return _filter_scope(
                frame, scope, _rewrite(term.copy().transform(replace), scope))
    if any(isinstance(node, (exp.Subquery, exp.Exists)) for node in term.walk()):
        raise _Fallback()
    return None


def _exists_join(scope, exists, sources, frame, read, anti):
    inner = exists.this
    if not isinstance(inner, exp.Select):
        inner = exists.this.this if isinstance(exists.this, exp.Subquery) else None
    if not isinstance(inner, exp.Select):
        raise _Fallback()
    inner_scope = _schema_scope(inner, sources)
    correlations, local, residuals = _split_correlation(
        inner.args.get("where"), scope, inner_scope)
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
    if residuals:
        return _exists_equi_filter(
            scope, right, correlations, residuals, frame, anti)
    pairs = []
    for outer_col, inner_col in correlations:
        pairs.append((
            _lookup(outer_col, scope),
            _lookup(inner_col, right)))
    return _keyed(scope, right, pairs, "anti" if anti else "semi", frame)


def _exists_equi_filter(scope, right, correlations, residuals, frame, anti):
    pairs = [
        (_lookup(outer_col, scope), _lookup(inner_col, right))
        for outer_col, inner_col in correlations
    ]
    joined = _equi(scope, right, pairs, frame)
    pred = residuals[0]
    for extra in residuals[1:]:
        pred = exp.and_(pred, extra)
    joined = _filter_scope(frame, joined, _rewrite(pred, joined))
    expressions = [
        dict(output=name, expression=_q(name) + " AS " + _q(name))
        for name in scope.outputs
    ]
    projected = frame.add(dict(
        id=frame.nid("p"), op="project", input=joined.node,
        expressions=expressions))
    distinct = frame.add(dict(
        id=frame.nid("d"), op="distinct", input=projected))
    matched = _Scope(
        distinct, list(scope.outputs),
        {("", name): name for name in scope.outputs},
        {name: name for name in scope.outputs})
    identity = [(name, name) for name in scope.outputs]
    return _keyed(scope, matched, identity, "anti" if anti else "semi", frame)


def _in_join(scope, term, sources, frame, read, anti=False):
    query = term.args["query"]
    inner = query.this if isinstance(query, exp.Subquery) else query
    if not isinstance(inner, exp.Select) or len(inner.expressions) != 1:
        raise _Fallback()
    inner_scope = _schema_scope(inner, sources)
    correlations, local, _residuals = _split_correlation(
        inner.args.get("where"), scope, inner_scope)
    if _residuals:
        raise _Fallback()
    inner = inner.copy()
    if correlations:
        inner.set(
            "expressions",
            list(inner.expressions) + [column.copy() for _outer, column in correlations])
    _set_where(inner, local)
    right_id, right_outs = _compile_query(inner, sources, frame, read)
    if len(right_outs) < 1:
        raise _Fallback()
    right = _Scope(
        right_id, right_outs,
        {("", name): name for name in right_outs},
        {name: name for name in right_outs})
    left_expr = term.this
    if isinstance(left_expr, exp.Column):
        left_key = _lookup(left_expr, scope)
    else:
        scope, left_key = _materialize_expr(scope, left_expr, frame)
    pairs = []
    corr_names = right_outs[1:]
    if correlations:
        if len(corr_names) < len(correlations):
            raise _Fallback()
        for (outer_col, _inner), corr_name in zip(correlations, corr_names):
            pairs.append((_lookup(outer_col, scope), corr_name))
    pairs.append((left_key, right_outs[0]))
    return _keyed(scope, right, pairs, "anti" if anti else "semi", frame)


def _scalar_eq(term):
    if not isinstance(term, (exp.EQ, exp.LT, exp.GT, exp.LTE, exp.GTE)):
        return None
    if isinstance(term.expression, exp.Subquery):
        return term, term.this, term.expression
    if isinstance(term.this, exp.Subquery):
        return term, term.expression, term.this
    return None


def _scalar_eq_join(scope, compared, sources, frame, read):
    compare, outer_expr, subquery = compared
    saved = (list(scope.outputs), dict(scope.by_alias), dict(scope.by_name))
    inner = subquery.this
    if not isinstance(inner, exp.Select) or len(inner.expressions) != 1:
        raise _Fallback()
    agg = inner.expressions[0]
    inner_scope = _schema_scope(inner, sources)
    correlations, local, residuals = _split_correlation(
        inner.args.get("where"), scope, inner_scope)
    if not (_is_agg(agg) or _contains_agg(agg)):
        if correlations or residuals:
            raise _Fallback()
        return _uncorrelated_scalar(
            scope, outer_expr, inner, compare, sources, frame, read)
    if residuals:
        raise _Fallback()
    if not correlations:
        return _uncorrelated_scalar(
            scope, outer_expr, inner, compare, sources, frame, read)
    grouped = inner.copy()
    keys = [column.copy() for _outer, column in correlations]
    grouped.set(
        "expressions",
        keys + [exp.alias_(agg.copy(), "_m_agg", quoted=True)])
    grouped.set("group", exp.Group(expressions=[column.copy() for column in keys]))
    _set_where(grouped, local)
    right_id, right_outs = _compile_query(grouped, sources, frame, read)
    if len(right_outs) != len(correlations) + 1:
        raise _Fallback()
    right = _Scope(
        right_id, right_outs,
        {("", name): name for name in right_outs},
        {name: name for name in right_outs})
    right, mapping = _freshen_scope(frame, right)
    right_outs = [mapping[name] for name in right_outs]
    if isinstance(outer_expr, exp.Column):
        left_key = _lookup(outer_expr, scope)
    else:
        scope, left_key = _materialize_expr(scope, outer_expr, frame)
    pairs = []
    for (outer_col, _inner), corr_name in zip(correlations, right_outs[:-1]):
        pairs.append((_lookup(outer_col, scope), corr_name))
    if isinstance(compare, exp.EQ):
        pairs.append((left_key, right_outs[-1]))
        return _project_back(
            _equi(scope, right, pairs, frame), *saved, frame)
    scope = _equi(scope, right, pairs, frame)
    pred = type(compare)(
        this=exp.column(left_key, quoted=True),
        expression=exp.column(right_outs[-1], quoted=True))
    return _project_back(
        _filter_scope(frame, scope, _rewrite(pred, scope)), *saved, frame)


def _attach_scalar(scope, subquery, sources, frame, read):
    """Join one scalar subquery and return the column that holds its value."""
    saved_outputs = list(scope.outputs)
    saved_alias = dict(scope.by_alias)
    saved_names = dict(scope.by_name)
    inner = subquery.this
    if not isinstance(inner, exp.Select) or len(inner.expressions) != 1:
        raise _Fallback()
    inner_scope = _schema_scope(inner, sources)
    correlations, local, residuals = _split_correlation(
        inner.args.get("where"), scope, inner_scope)
    if residuals:
        raise _Fallback()
    copied = inner.copy()
    _set_where(copied, local)
    select = copied.expressions[0]
    if correlations:
        if not (_is_agg(select) or _contains_agg(select)):
            raise _Fallback()
        keys = [column.copy() for _outer, column in correlations]
        copied.set(
            "expressions",
            keys + [exp.alias_(select, "_m_val", quoted=True)])
        copied.set(
            "group", exp.Group(expressions=[column.copy() for column in keys]))
        right_id, right_outs = _compile_query(copied, sources, frame, read)
        if len(right_outs) != len(keys) + 1:
            raise _Fallback()
        right = _Scope(
            right_id, right_outs,
            {("", name): name for name in right_outs},
            {name: name for name in right_outs})
        right, mapping = _freshen_scope(frame, right)
        right_outs = [mapping[name] for name in right_outs]
        pairs = []
        for (outer_col, _inner), corr_name in zip(correlations, right_outs[:-1]):
            pairs.append((_lookup(outer_col, scope), corr_name))
        joined = _equi(scope, right, pairs, frame)
        value = right_outs[-1]
        names = dict(saved_names)
        names[value] = value
        return _project_back(
            joined, saved_outputs + [value], saved_alias, names, frame), value
    if not isinstance(select, exp.Alias):
        copied.set(
            "expressions",
            [exp.alias_(select, "_m_val", quoted=True)])
    right_id, right_outs = _compile_query(copied, sources, frame, read)
    if len(right_outs) != 1:
        raise _Fallback()
    fresh = frame.col()
    right_id = frame.add(dict(
        id=frame.nid("p"), op="project", input=right_id,
        expressions=[dict(
            output=fresh,
            expression=_q(right_outs[0]) + " AS " + _q(fresh))]))
    scope, left_key = _materialize_expr(scope, exp.Literal.number(1), frame)
    right = _Scope(
        right_id, [fresh],
        {("", fresh): fresh},
        {fresh: fresh})
    right, right_key = _materialize_expr(right, exp.Literal.number(1), frame)
    joined = _equi(scope, right, [(left_key, right_key)], frame)
    names = dict(saved_names)
    names[fresh] = fresh
    return _project_back(
        joined, saved_outputs + [fresh], saved_alias, names, frame), fresh


def _uncorrelated_scalar(scope, outer_expr, inner, compare, sources, frame, read):
    saved = (list(scope.outputs), dict(scope.by_alias), dict(scope.by_name))
    copied = inner.copy()
    if not isinstance(copied.expressions[0], exp.Alias):
        copied.set(
            "expressions",
            [exp.alias_(copied.expressions[0], "_m_val", quoted=True)])
    right_id, right_outs = _compile_query(copied, sources, frame, read)
    if len(right_outs) != 1:
        raise _Fallback()
    if isinstance(outer_expr, exp.Column):
        left_name = _lookup(outer_expr, scope)
    else:
        scope, left_name = _materialize_expr(scope, outer_expr, frame)
    fresh = frame.col()
    right_id = frame.add(dict(
        id=frame.nid("p"), op="project", input=right_id,
        expressions=[dict(
            output=fresh,
            expression=_q(right_outs[0]) + " AS " + _q(fresh))]))
    right_outs = [fresh]
    scope, left_key = _materialize_expr(scope, exp.Literal.number(1), frame)
    right = _Scope(
        right_id, right_outs,
        {("", right_outs[0]): right_outs[0]},
        {right_outs[0]: right_outs[0]})
    right, right_key = _materialize_expr(right, exp.Literal.number(1), frame)
    scope = _equi(scope, right, [(left_key, right_key)], frame)
    pred = type(compare)(
        this=exp.column(left_name, quoted=True),
        expression=exp.column(right_outs[0], quoted=True))
    return _project_back(
        _filter_scope(frame, scope, _rewrite(pred, scope)), *saved, frame)


def _split_correlation(where, outer, inner=None):
    if where is None:
        return [], [], []
    expr = where.this if isinstance(where, exp.Where) else where
    correlations = []
    local = []
    residuals = []
    for term in _and_terms(expr):
        if (
            isinstance(term, exp.EQ)
            and isinstance(term.this, exp.Column)
            and isinstance(term.expression, exp.Column)
        ):
            left_outer = _column_is_outer(term.this, outer, inner)
            right_outer = _column_is_outer(term.expression, outer, inner)
            if left_outer and not right_outer:
                correlations.append((term.this, term.expression))
                continue
            if right_outer and not left_outer:
                correlations.append((term.expression, term.this))
                continue
        branches = _or_terms(term)
        if len(branches) > 1:
            common = None
            common_key = None
            pieces = []
            failed = False
            for branch in branches:
                found, locals_, nested = _split_correlation(branch, outer, inner)
                if nested:
                    failed = True
                    break
                key = tuple(pair[0].sql() + "=" + pair[1].sql() for pair in found)
                if common is None:
                    common = found
                    common_key = key
                elif key != common_key:
                    failed = True
                    break
                if locals_:
                    expr = locals_[0]
                    for extra in locals_[1:]:
                        expr = exp.and_(expr, extra)
                    pieces.append(expr)
            if not failed and common:
                correlations.extend(common)
                if pieces:
                    expr = pieces[0]
                    for extra in pieces[1:]:
                        expr = exp.or_(expr, extra)
                    local.append(expr)
                continue
        if _references_outer(term, outer, inner):
            residuals.append(term)
            continue
        local.append(term)
    return correlations, local, residuals


def _schema_scope(select, sources):
    by_alias = {}
    by_name = {}
    tables = []
    origin = select.args.get("from_")
    if origin is not None and isinstance(origin.this, exp.Table):
        tables.append(origin.this)
    for join in select.args.get("joins") or ():
        if isinstance(join.this, exp.Table):
            tables.append(join.this)
    for table in tables:
        alias = str(table.alias or table.name).lower()
        for column in sources.get(table.name) or ():
            by_alias[(alias, column)] = column
            if column in by_name:
                by_name[column] = None
            else:
                by_name[column] = column
    return _Scope(None, [], by_alias, by_name)


def _column_is_outer(column, outer, inner):
    if inner is not None and _in_scope(column, inner):
        return False
    return _is_outer(column, outer)


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


def _references_outer(term, outer, inner=None):
    for node in term.walk():
        if isinstance(node, exp.Column) and _column_is_outer(node, outer, inner):
            return True
    return False


def _filter_scope(frame, scope, predicate):
    node = frame.add(dict(
        id=frame.nid("f"), op="filter", input=scope.node,
        predicate=predicate.strip(), scope="predicate"))
    return _Scope(node, scope.outputs, scope.by_alias, scope.by_name)


def _compile_aggregate(scope, tree, frame, sources=None, read="duckdb"):
    group = tree.args.get("group")
    _resolve_group_aliases(tree)
    select_has_agg = any(_contains_agg(item) for item in tree.expressions)
    having = tree.args.get("having")
    if group is None and not select_has_agg and having is None:
        return None
    window_call = _find_m2s_window(tree)
    if window_call is not None:
        return _compile_window_agg(scope, tree, frame, window_call)
    scope, tree = _bind_computed(scope, tree, frame)
    frame.scope = scope
    group = tree.args.get("group")
    sets = _grouping_sets(group, scope)
    if sets is None:
        sets = [[]]
    outputs = _select_slots(tree, scope, frame)
    scope = getattr(frame, "scope", scope)
    having = tree.args.get("having")
    if having is not None:
        having_expr = having.this if isinstance(having, exp.Having) else having
        having_expr, extra = _pull_aggs(having_expr, scope, frame)
        tree.set("having", exp.Having(this=having_expr))
        having = tree.args.get("having")
        for spec in extra:
            outputs.append(("hidden", spec["output"], spec["output"], spec))
    grouped_cols = {column for item in sets for column in item}
    for kind, _public, internal, spec in outputs:
        if kind != "key" or internal in grouped_cols:
            continue
        if isinstance(spec, tuple) and spec and all(
            dep in grouped_cols for dep in spec
        ):
            for item in sets:
                if internal not in item:
                    item.append(internal)
            grouped_cols.add(internal)
            continue
        raise ValueError("select column is not grouped")
    branches = []
    for keys in sets:
        aggregates = []
        seen_outputs = set()
        for slot in outputs:
            specs = []
            if slot[0] == "agg":
                specs = [slot[3]]
            elif slot[0] in {"formula", "hidden"}:
                specs = slot[3] if slot[0] == "formula" else [slot[3]]
            for spec in specs:
                if spec["output"] in seen_outputs:
                    continue
                aggregates.append(spec)
                seen_outputs.add(spec["output"])
        if not aggregates:
            expressions = []
            produced = []
            for kind, public, internal, _spec in outputs:
                if kind == "hidden":
                    continue
                produced.append(public)
                if kind == "const":
                    expr = internal + " AS " + _q(public)
                elif kind == "grouping":
                    expr = _grouping_literal(internal, keys, scope) + " AS " + _q(public)
                else:
                    expr = _q(internal) + " AS " + _q(public)
                expressions.append(dict(output=public, expression=expr))
            node = frame.add(dict(
                id=frame.nid("p"), op="project", input=scope.node,
                expressions=expressions))
            node = frame.add(dict(
                id=frame.nid("d"), op="distinct", input=node))
            branches.append((node, produced))
            continue
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
            elif kind == "const":
                expr = internal + " AS " + _q(public)
            elif kind == "grouping":
                expr = _grouping_literal(internal, keys, scope) + " AS " + _q(public)
            elif kind == "formula":
                expr = internal + " AS " + _q(public)
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
        if any(isinstance(item, (exp.Subquery, exp.Exists)) for item in having_expr.walk()):
            post = _apply_where(post, having_expr, sources, frame, read)
            node = post.node
            produced = list(post.outputs)
        else:
            if any(isinstance(item, exp.AggFunc) for item in having_expr.walk()):
                raise ValueError("HAVING must use select aliases of aggregates")
            node = _filter_scope(
                frame, post, _rewrite(having_expr, post)).node
    visible = [slot[1] for slot in outputs if slot[0] != "hidden"]
    if any(slot[0] == "hidden" for slot in outputs):
        expressions = [
            dict(output=name, expression=_q(name) + " AS " + _q(name))
            for name in visible
        ]
        node = frame.add(dict(
            id=frame.nid("p"), op="project", input=node, expressions=expressions))
        produced = visible
    return _Scope(
        node, produced,
        {("", name): name for name in produced},
        {name: name for name in produced})


def _select_slots(tree, scope, frame):
    slots = []
    seen_aggs = {}
    for item in tree.expressions:
        public = _output_name(item)
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
        if _contains_agg(target):
            sql, specs = _swap_aggs(target, scope, frame)
            if not public:
                public = frame.col()
            slots.append(("formula", public, sql, specs))
            continue
        if any(isinstance(node, exp.Grouping) for node in target.walk()):
            if not public:
                public = frame.col()
            slots.append(("grouping", public, target, None))
            continue
        if isinstance(target, exp.Column):
            internal = _lookup(target, scope)
            if not public:
                public = target.name
            slots.append(("key", public, internal, None))
            continue
        if not any(isinstance(node, exp.Column) for node in target.walk()):
            if not public:
                public = frame.col()
            slots.append((
                "const", public, target.sql(dialect="duckdb").strip(), None))
            continue
        dependencies = []
        for node in target.walk():
            if isinstance(node, exp.Column):
                dependencies.append(_lookup(node, scope))
        scope, internal = _materialize_expr(scope, target, frame)
        frame.scope = scope
        if not public:
            public = frame.col()
        slots.append(("key", public, internal, tuple(dependencies)))
    return slots
    return slots


def _grouping_literal(expr, keys, scope):
    def transform(node):
        if isinstance(node, exp.Grouping):
            column = node.this if isinstance(node.this, exp.Column) else node.expressions[0]
            internal = _lookup(column, scope)
            return exp.Literal.number(0 if internal in keys else 1)
        return node
    return expr.copy().transform(transform).sql(dialect="duckdb").strip()


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


def _resolve_group_aliases(tree):
    aliases = {}
    for item in tree.expressions:
        if isinstance(item, exp.Alias):
            aliases[str(item.alias)] = item.this
    group = tree.args.get("group")
    if group is None:
        return
    rewritten = []
    for item in group.expressions or []:
        if isinstance(item, exp.Column) and not item.table and item.name in aliases:
            rewritten.append(aliases[item.name].copy())
        else:
            rewritten.append(item)
    group.set("expressions", rewritten)


def _contains_agg(node):
    target = node.this if isinstance(node, exp.Alias) else node
    return any(
        isinstance(item, (exp.AggFunc, exp.GroupConcat)) for item in target.walk()
    )


def _swap_aggs(expr, scope, frame):
    specs = []

    def walk(node):
        if isinstance(node, exp.Subquery):
            return node
        if isinstance(node, (exp.AggFunc, exp.GroupConcat)):
            spec = _agg_spec(node, scope, frame)
            specs.append(spec)
            return exp.column(spec["output"], quoted=True)
        if not isinstance(node, exp.Expression):
            return node
        for arg, value in list(node.args.items()):
            if isinstance(value, list):
                node.set(arg, [
                    walk(item) if isinstance(item, exp.Expression) else item
                    for item in value
                ])
            elif isinstance(value, exp.Expression):
                node.set(arg, walk(value))
        return node

    return walk(expr.copy()).sql(dialect="duckdb"), specs


def _pull_aggs(expr, scope, frame):
    sql, specs = _swap_aggs(expr, scope, frame)
    return sqlglot.parse_one(sql, read="duckdb"), specs


def _is_agg(node):
    target = node.this if isinstance(node, exp.Alias) else node
    return isinstance(target, (exp.AggFunc, exp.GroupConcat))


def _bind_computed(scope, tree, frame):
    """Project arithmetic inside aggregates and GROUP BY to real columns.

    SUM(price * (1 - discount)) then groups by that column. The aggregate
    itself stays incremental.
    """
    tree = tree.copy()
    wanted = []

    def consider(node):
        if node is None or isinstance(node, (exp.Column, exp.Star, exp.Literal)):
            return
        wanted.append(node)

    group = tree.args.get("group")
    if group is not None:
        for item in group.expressions or []:
            consider(item)
        for key in ("rollup", "cube"):
            for roll in group.args.get(key) or ():
                for item in roll.expressions or []:
                    consider(item)
    for item in tree.expressions:
        target = item.this if isinstance(item, exp.Alias) else item
        if isinstance(target, (exp.AggFunc, exp.GroupConcat)):
            arg = target.this
            if isinstance(arg, exp.Order):
                consider(arg.this)
            elif isinstance(arg, exp.Distinct) and arg.expressions:
                consider(arg.expressions[0])
            else:
                consider(arg)
    mapping = {}
    for node in wanted:
        try:
            key = node.sql(dialect="duckdb")
        except Exception:
            continue
        if key in mapping:
            continue
        scope, name = _materialize_expr(scope, node, frame)
        mapping[key] = name
    if not mapping:
        return scope, tree

    def replace(node):
        if isinstance(node, exp.Subquery):
            return node
        if not isinstance(node, exp.Expression):
            return node
        try:
            key = node.sql(dialect="duckdb")
        except Exception:
            key = None
        if key in mapping:
            return exp.column(mapping[key], quoted=True)
        for arg, value in list(node.args.items()):
            if isinstance(value, list):
                node.set(arg, [
                    replace(item) if isinstance(item, exp.Expression) else item
                    for item in value
                ])
            elif isinstance(value, exp.Expression):
                node.set(arg, replace(value))
        return node

    return scope, replace(tree)


def _materialize_expr(scope, expr, frame):
    name = frame.col()
    expressions = [
        dict(output=column, expression=_q(column) + " AS " + _q(column))
        for column in scope.outputs
    ]
    expressions.append(dict(
        output=name,
        expression=_rewrite(expr, scope) + " AS " + _q(name)))
    node = frame.add(dict(
        id=frame.nid("p"), op="project", input=scope.node,
        expressions=expressions))
    by_alias = dict(scope.by_alias)
    by_name = dict(scope.by_name)
    by_name[name] = name
    return _Scope(node, list(scope.outputs) + [name], by_alias, by_name), name


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
        exp.StddevSamp: "stddev_samp",
    }
    function = functions.get(type(target))
    if function is None:
        raise ValueError("unsupported aggregate")
    if isinstance(target.this, exp.Column):
        source = _lookup(target.this, scope)
    else:
        scope, source = _materialize_expr(
            getattr(frame, "scope", scope), target.this, frame)
        frame.scope = scope
    return dict(
        output=output, function=function, input=source)


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
        _contains_agg(item) for item in tree.expressions
    ):
        return scope
    expressions = []
    outputs = []
    by_name = {}
    for item in tree.expressions:
        target = item.this if isinstance(item, exp.Alias) else item
        public = _output_name(item)
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
    found = []
    for (_alias, column), name in scope.by_alias.items():
        if name == internal and column not in found:
            found.append(column)
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
    named = set(scope.by_alias.values())
    return [internal for internal in scope.outputs if internal in named]


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
    table = str(column.table or "").lower()
    name = column.name
    if table:
        return (table, name) in scope.by_alias
    return name in scope.by_name and scope.by_name[name] is not None


def _lookup(column, scope):
    if not isinstance(column, exp.Column):
        raise ValueError("expected a column")
    table = str(column.table or "").lower()
    name = column.name
    if table:
        key = (table, name)
        if key not in scope.by_alias:
            raise ValueError("unknown column " + table + "." + name)
        return scope.by_alias[key]
    if name not in scope.by_name or scope.by_name[name] is None:
        if scope.outputs.count(name) == 1:
            return name
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

    aliases = set()
    for item in tree.expressions:
        if isinstance(item, exp.Alias):
            aliases.add(str(item.alias))

    def transform(node):
        if isinstance(node, exp.Column):
            if not node.table and node.name in aliases:
                return node
            return exp.Column(this=exp.to_identifier(
                _lookup(node, scope), quoted=True))
        return node

    cloned = cloned.transform(transform)
    partitions = None
    for window in tree.find_all(exp.Window):
        parts = []
        for column in window.args.get("partition_by") or []:
            if not isinstance(column, exp.Column):
                parts = []
                break
            parts.append(_lookup(column, scope))
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


def _order_column(item, scope, tree):
    expr = item.this
    if isinstance(expr, exp.Column):
        try:
            return _lookup(expr, scope)
        except ValueError:
            pass
    if isinstance(expr, exp.Literal) and not expr.args.get("is_string"):
        try:
            index = int(expr.this) - 1
        except ValueError:
            index = -1
        if 0 <= index < len(scope.outputs):
            return scope.outputs[index]
    try:
        text = expr.sql(dialect="duckdb").strip().lower()
    except Exception:
        text = ""
    for select_item, output in zip(tree.expressions, scope.outputs):
        if isinstance(select_item, exp.Alias) and str(select_item.alias).lower() == text:
            return output
        target = select_item.this if isinstance(select_item, exp.Alias) else select_item
        try:
            if target.sql(dialect="duckdb").strip().lower() == text:
                return output
        except Exception:
            continue
    if text and text in [name.lower() for name in scope.outputs]:
        return scope.outputs[[name.lower() for name in scope.outputs].index(text)]
    if isinstance(expr, exp.Column) and not expr.table:
        for select_item, output in zip(tree.expressions, scope.outputs):
            target = select_item.this if isinstance(select_item, exp.Alias) else select_item
            columns = [node for node in target.walk() if isinstance(node, exp.Column)]
            if len(columns) == 1 and columns[0].name == expr.name and not columns[0].table:
                return output
    raise ValueError("ORDER BY is not a select output")


def _compile_topn(scope, tree, frame):
    limit = int(tree.args["limit"].expression.this)
    visible = list(scope.outputs)
    order = []
    for item in tree.args["order"].expressions:
        try:
            column = _order_column(item, scope, tree)
        except ValueError:
            scope, column = _materialize_expr(scope, item.this, frame)
        order.append(dict(
            column=column,
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
    if list(scope.outputs) != visible:
        expressions = [
            dict(output=name, expression=_q(name) + " AS " + _q(name))
            for name in visible
        ]
        node = frame.add(dict(
            id=frame.nid("p"), op="project", input=node, expressions=expressions))
        return node, visible
    return node, list(scope.outputs)
