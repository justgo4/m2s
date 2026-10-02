#!/usr/bin/env python3
from pathlib import Path
import sys
from types import SimpleNamespace

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import p11_profile


def main():
    assert p11_profile.NAME=="p11-50m-50rps-72h-v1"
    expected=dict(
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
    assert p11_profile.PARAMETERS==expected
    assert p11_profile.mismatches(expected)=={}
    assert p11_profile.mismatches(
        SimpleNamespace(**expected))=={}

    changed=dict(expected)
    changed["rows_per_second"]=51
    changed["memory_mb"]=4096
    mismatch=p11_profile.mismatches(changed)
    assert set(mismatch)=={
        "rows_per_second","memory_mb"}
    assert mismatch["rows_per_second"]==dict(
        expected=50,actual=51)
    assert mismatch["memory_mb"]==dict(
        expected=8192,actual=4096)

    print(
        "p11_profile_test ok exact_identity exact_parameters "
        "mapping_namespace_validation mismatch_evidence",
        flush=True,
    )


if __name__=="__main__":
    main()
