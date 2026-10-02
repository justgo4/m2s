"""Canonical fixed-resource P11 certification profile.

Keep this dependency-free so the workload driver, gate and offline tests share
one exact profile identity without importing runtime/database modules.
"""

NAME="p11-50m-50rps-72h-v2"

PARAMETERS=dict(
    load_mode="merge_async",
    rows=50_000_000,
    rows_per_second=50,
    duration_seconds=72*3600,
    dynamic_tasks=10,
    fault_every_seconds=6*3600,
    sample_seconds=1.0,
    fault_recovery_timeout_seconds=1800.0,
    seed_chunk=10_000,
    snapshot_rows=16_384,
    memory_mb=8192,
    share_mode="adaptive",
    drain_timeout_seconds=1800.0,
    checkpoint_seconds=300.0,
)


def mismatches(values):
    getter=(
        values.get
        if isinstance(values,dict)
        else lambda name: getattr(values,name)
    )
    result={}
    for name,expected in PARAMETERS.items():
        actual=getter(name)
        if actual!=expected:
            result[name]=dict(
                expected=expected,
                actual=actual)
    return result
