#!/usr/bin/env python3
import incremental_contract as ic


def expect_error(function):
    try:
        function()
    except ValueError:
        return
    raise AssertionError("expected ValueError")


def main():
    base = ic.state_spec(
        "arrangement",
        ["mysql.orders"],
        [7],
        key_exprs=["customer_id"],
        value_exprs=["amount", "status"],
        predicate="status = 1",
        collation="utf8mb4_bin",
    )
    same = {
        "collation": "utf8mb4_bin",
        "predicate": "status = 1",
        "value_exprs": ["amount", "status"],
        "key_exprs": ["customer_id"],
        "schema_epochs": [7],
        "relations": ["mysql.orders"],
        "kind": "arrangement",
        "format_version": 1,
        "semantics_version": 1,
    }
    assert ic.state_identity(base) == ic.state_identity(same)

    changed_epoch = ic.state_spec(
        "arrangement", ["mysql.orders"], [8],
        key_exprs=["customer_id"], value_exprs=["amount", "status"],
        predicate="status = 1", collation="utf8mb4_bin")
    changed_predicate = ic.state_spec(
        "arrangement", ["mysql.orders"], [7],
        key_exprs=["customer_id"], value_exprs=["amount", "status"],
        predicate="status = 2", collation="utf8mb4_bin")
    changed_collation = ic.state_spec(
        "arrangement", ["mysql.orders"], [7],
        key_exprs=["customer_id"], value_exprs=["amount", "status"],
        predicate="status = 1", collation="utf8mb4_general_ci")
    assert len({
        ic.state_identity(base),
        ic.state_identity(changed_epoch),
        ic.state_identity(changed_predicate),
        ic.state_identity(changed_collation),
    }) == 4

    handle = ic.state_handle(base, 120, backend="candidate")
    assert ic.state_compatible(handle, base, minimum_watermark=120)
    assert not ic.state_compatible(handle, base, minimum_watermark=121)
    assert not ic.state_compatible(handle, changed_epoch)

    assert ic.changelog_retention_floor(200, [180, 190], [150]) == 150
    assert ic.changelog_retention_floor(200, [180, 190], []) == 180
    assert ic.changelog_retention_floor(200, [], []) == 200
    expect_error(lambda: ic.changelog_retention_floor(200, [], [201]))

    def candidate(strategy, values):
        return ic.plan_candidate(
            strategy,
            [ic.state_identity(base)] if strategy == "reuse_state" else [],
            dict(zip(ic.COST_FIELDS, values)),
        )

    reuse = candidate("reuse_state", [100, 0, 10, 2, 1, 20])
    dominated_full = candidate("full_recompute", [500, 1000, 20, 4, 8, 80])
    fast_large = candidate("partial_recompute", [50, 100, 200, 8, 10, 10])
    frontier = ic.pareto_frontier([reuse, dominated_full, fast_large])
    assert {row["strategy"] for row in frontier} == {"reuse_state", "partial_recompute"}

    # Restricting the policy to time-to-ready intentionally changes dominance:
    # the fastest candidate becomes the sole frontier member.
    time_frontier = ic.pareto_frontier(
        [reuse, dominated_full, fast_large], metrics=("time_to_ready_ms",))
    assert [row["strategy"] for row in time_frontier] == ["partial_recompute"]

    expect_error(lambda: ic.plan_candidate(
        "incremental_build", [], {name: -1 for name in ic.COST_FIELDS}))
    expect_error(lambda: ic.state_spec("arrangement", ["a"], [], key_exprs=["k"]))

    print("incremental_contract_test ok", flush=True)


if __name__ == "__main__":
    main()
