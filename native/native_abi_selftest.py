#!/usr/bin/env python3
import os
import sys

import pyarrow as pa

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import j4


def main():
    expected_loader_revision = 3
    actual_loader_revision = getattr(j4,"J4_NATIVE_LOADER_REVISION",0)
    if actual_loader_revision != expected_loader_revision:
        raise RuntimeError(
            "stale j4.py detected: "
            f"native_abi_selftest requires loader_revision={expected_loader_revision} "
            f"but imported j4.py reports {actual_loader_revision}; "
            "update j4.py and native/ from the same repository revision")
    os.environ["CDC_NATIVE_PARTITION"] = "required"
    lib = j4.native_partition_library()
    if lib is None:
        raise AssertionError("required bundled native library was not loaded")

    actual_abi = int(lib.j4_native_abi_version())
    actual_features = int(lib.j4_native_feature_bits())
    required = (
        j4.J4_NATIVE_FEATURE_STABLE_PARTITION
        | j4.J4_NATIVE_FEATURE_NO_LIBC
        | j4.J4_NATIVE_FEATURE_JSON_ENCODER
    )
    if actual_abi != j4.J4_NATIVE_ABI_VERSION:
        raise AssertionError(
            f"native ABI mismatch expected={j4.J4_NATIVE_ABI_VERSION} "
            f"actual={actual_abi}")
    if actual_features & required != required:
        raise AssertionError(
            f"native features missing required=0x{required:x} "
            f"actual=0x{actual_features:x}")

    routed = pa.table({
        "_sync_lane": pa.array(
            [2,0,2,1,0,1,2,0], type=pa.uint16()),
        "value": pa.array(range(8), type=pa.int64()),
    })
    result = j4.native_stable_partition_order(routed,3)
    if result is None:
        raise AssertionError("native stable partition unexpectedly fell back")
    order,counts = result
    if order.to_pylist() != [1,4,7,3,5,0,2,6]:
        raise AssertionError(
            f"native stable partition order mismatch {order.to_pylist()!r}")
    if counts != [(0,3),(1,2),(2,3)]:
        raise AssertionError(
            f"native stable partition counts mismatch {counts!r}")

    print(
        "NATIVE_ABI SELFTEST PASS "
        f"abi={actual_abi} features=0x{actual_features:x} "
        f"python={sys.version.split()[0]} pyarrow={pa.__version__} "
        f"loader={j4._NATIVE_PARTITION_SOURCE} "
        "python_minor_abi_dependency=0",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
