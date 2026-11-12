#!/usr/bin/env python3
"""Synthetic Nexmark-shaped queries against DuckDB.

The generator and the SQL are original. Official Nexmark query text is not
copied into this repository. A tumbling window is checked against the
equivalent full-group SQL, because that function is not a DuckDB builtin.
"""
from pathlib import Path
import random
import sys

import duckdb
import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import stream_eval
import stream_sql
import stream_zset


SOURCES = {
    "person": ["id", "name", "city", "state"],
    "auction": ["id", "seller", "category", "start_ms", "end_ms"],
    "bid": ["id", "auction", "bidder", "price", "channel", "bid_ms"],
}

QUERIES = [
    (
        "q0",
        "SELECT id, auction, bidder, price, channel, bid_ms FROM bid",
        None,
    ),
    (
        "q1",
        "SELECT id, auction, price / 100.0 AS dollars FROM bid",
        None,
    ),
    (
        "q2",
        "SELECT id, auction, price FROM bid "
        "WHERE channel = 'c1' AND price > 1000",
        None,
    ),
    (
        "q3",
        "SELECT person.name AS seller_name, auction.id AS auction_id "
        "FROM person, auction "
        "WHERE person.id = auction.seller AND person.city = 'omaha'",
        None,
    ),
    (
        "q4",
        "SELECT auction.category AS category, AVG(bid.price) AS mean_price "
        "FROM bid, auction "
        "WHERE bid.auction = auction.id "
        "GROUP BY auction.category",
        None,
    ),
    (
        "q5",
        "SELECT auction, COUNT(*) AS n FROM bid GROUP BY auction",
        None,
    ),
    (
        "q6",
        "SELECT auction, MAX(price) AS top_price FROM bid GROUP BY auction",
        None,
    ),
    (
        "q7",
        "SELECT auction, COUNT(*) AS n FROM bid "
        "GROUP BY auction, m2s_tumble(bid_ms, 1000)",
        "SELECT auction, (bid_ms // 1000) * 1000 AS w_start, "
        "(bid_ms // 1000) * 1000 + 1000 AS w_end, COUNT(*) AS n "
        "FROM bid GROUP BY auction, 2, 3",
    ),
    (
        "q8",
        "SELECT id, name FROM person WHERE id IN (SELECT bidder FROM bid)",
        None,
    ),
    (
        "q9",
        "SELECT id FROM auction WHERE id NOT IN "
        "(SELECT auction FROM bid WHERE auction IS NOT NULL)",
        None,
    ),
]


def _generate():
    rng = random.Random(20261112)
    cities = ["omaha", "portland", "austin", "boston"]
    states = ["ne", "or", "tx", "ma"]
    person = []
    for index in range(1, 51):
        person.append(dict(
            id=index,
            name="p%d" % index,
            city=cities[index % 4],
            state=states[index % 4],
        ))
    auction = []
    for index in range(1, 41):
        start = 1000000 + index * 1000
        auction.append(dict(
            id=index,
            seller=(index % 50) + 1,
            category=index % 5,
            start_ms=start,
            end_ms=start + 50000,
        ))
    bid = []
    for index in range(1, 401):
        bid.append(dict(
            id=index,
            auction=rng.randrange(1, 36),
            bidder=rng.randrange(1, 51),
            price=100 + rng.randrange(5000),
            channel="c%d" % rng.randrange(3),
            bid_ms=1000000 + index * 10,
        ))
    return person, auction, bid


def _mode(plan):
    for node in plan["nodes"]:
        if node["op"] in {"recompute", "theta_join"} or node.get(
            "full_input_recompute"
        ):
            return "recompute"
    return "incremental"


def _norm(value):
    if value is None:
        return None
    if hasattr(value, "as_tuple"):
        return format(value, "f").rstrip("0").rstrip(".") or "0"
    if isinstance(value, float):
        return "%.6f" % value
    return str(value)


def _bag(zset):
    rows = []
    for row, weight in zset.items():
        item = tuple(sorted(
            (key, _norm(value)) for key, value in row.items()))
        rows.extend([item] * weight)
    return sorted(rows)


def _oracle(sql, tables):
    con = duckdb.connect(":memory:")
    try:
        for name, rows in tables.items():
            con.register(name, pa.Table.from_pylist(rows))
        cursor = con.execute(sql)
        names = [item[0] for item in cursor.description]
        result = stream_zset.ZSet()
        for record in cursor.fetchall():
            result.add(dict(zip(names, record)), 1)
        return result
    finally:
        con.close()


def _visible(person, auction, bid):
    return {"person": person, "auction": auction, "bid": bid}


def main():
    person, auction, bid = _generate()
    first = bid[:250]
    second = bid[250:]
    retracted = dict(second[-1])
    failures = []
    for name, sql, oracle_sql in QUERIES:
        compiled = stream_sql.compile_sql(sql, SOURCES)
        mode = _mode(compiled["plan"])
        if mode != "incremental":
            failures.append((name, "recompute", "plan fell back to full recompute"))
            print("nexmark %s recompute" % name, flush=True)
            continue
        graph = stream_eval.StreamGraph(compiled["plan"])
        used = {
            node["input"]
            for node in compiled["plan"]["nodes"]
            if node["op"] == "input"
        }
        try:
            seen_bid = []
            for label, people, auctions, bids in (
                ("insert-1", person, auction, first),
                ("insert-2", person, auction, second),
                ("retract", person, auction, [(retracted, -1)]),
            ):
                if label == "retract":
                    batch = {"bid": bids} if "bid" in used else {}
                    if "bid" in used:
                        seen_bid = [row for row in seen_bid if row != retracted]
                else:
                    batch = {
                        "person": [(row, 1) for row in people] if label == "insert-1" else [],
                        "auction": [(row, 1) for row in auctions] if label == "insert-1" else [],
                        "bid": [(row, 1) for row in bids],
                    }
                    batch = {
                        key: value for key, value in batch.items()
                        if value and key in used
                    }
                    if label == "insert-1":
                        seen_bid = list(bids)
                    else:
                        seen_bid.extend(bids)
                graph.push(batch)
                expected = _oracle(
                    oracle_sql or sql, _visible(person, auction, seen_bid))
                if _bag(graph.output) != _bag(expected):
                    raise AssertionError(
                        "%s row mismatch got=%d expected=%d"
                        % (label, sum(graph.output.weights.values()),
                           sum(expected.weights.values())))
            rows = sum(graph.output.weights.values())
            print("nexmark %s incremental rows=%d" % (name, rows), flush=True)
        except Exception as exc:
            line = str(exc).splitlines()
            failures.append((name, type(exc).__name__, (line[0] if line else "")[:200]))
            print("nexmark %s FAIL %s" % (name, failures[-1][1]), flush=True)
        finally:
            graph.close()
    if failures:
        for item in failures:
            print("nexmark_fail", item, flush=True)
        raise SystemExit(1)
    print("nexmark_diff_test ok queries=%d persons=50 auctions=40 bids=400" % len(QUERIES), flush=True)


if __name__ == "__main__":
    main()
