#!/usr/bin/env python3
"""Execute Python UDAF/UDTF and Rust/WASM scalars inside a sandbox."""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import wasmtime

import stream_ops


RUST = """#![no_std]
#[panic_handler]
fn panic(_: &core::panic::PanicInfo) -> ! {
    loop {}
}
#[no_mangle]
pub extern "C" fn add_one(x: i32) -> i32 {
    x.wrapping_add(1)
}
"""

WASM_ADD = b"""
(module
  (func (export "add_one") (param i32) (result i32)
    local.get 0
    i32.const 1
    i32.add))
"""

WASM_SPIN = b"""
(module
  (func (export "spin")
    (loop
      br 0)))
"""

WASM_IMPORT = b"""
(module
  (func (import "host" "now") (result i32))
  (func (export "add_one") (param i32) (result i32)
    local.get 0))
"""


def main():
    udaf = stream_ops.register_udf(
        "total", "python", "aggregate", True,
        "def accumulate(state, value):\n"
        "    return state + value\n"
        "def retract(state, value):\n"
        "    return state - value\n"
        "def finish(state):\n"
        "    return state\n")
    first = stream_ops.execute_python_udaf(udaf, [1, 2], [1])
    second = stream_ops.execute_python_udaf(udaf, [1, 2], [1])
    if first != 2 or first != second:
        raise AssertionError("udaf determinism %s %s" % (first, second))
    udtf = stream_ops.register_udf(
        "split", "python", "table", True,
        "def udtf(value):\n    return [value, value + 1]\n")
    rows = stream_ops.execute_python_udtf(udtf, [3])
    if rows != [3, 4] or rows != stream_ops.execute_python_udtf(udtf, [3]):
        raise AssertionError(rows)
    try:
        stream_ops.execute_python_udaf(
            dict(udaf, deterministic=False), [1])
        raise AssertionError("non-deterministic udaf ran")
    except ValueError:
        pass

    wasm_fn = stream_ops.register_udf("add_one", "wasm", "scalar", True, "add_one")
    wasm_module = wasmtime.wat2wasm(WASM_ADD)
    wasm_run = stream_ops.run_wasm(wasm_module, "add_one", [4], fuel=1000)
    again = stream_ops.execute_wasm_udf(wasm_fn, wasm_module, [4], fuel=1000)
    if wasm_run["value"] != 5 or again["value"] != 5:
        raise AssertionError(wasm_run)
    try:
        stream_ops.run_wasm(wasmtime.wat2wasm(WASM_SPIN), "spin", [], fuel=40)
        raise AssertionError("fuel limit did not trap")
    except RuntimeError as exc:
        if "fuel" not in str(exc):
            raise
    try:
        stream_ops.run_wasm(wasmtime.wat2wasm(WASM_IMPORT), "add_one", [1])
        raise AssertionError("imported wasm ran")
    except ValueError:
        pass

    rust_fn = stream_ops.register_udf("add_one", "rust", "scalar", True, RUST)
    rust_run = stream_ops.execute_rust_udf(rust_fn, [41], fuel=10000)
    rust_again = stream_ops.execute_rust_udf(rust_fn, [41], fuel=10000)
    if rust_run["value"] != 42 or rust_again["value"] != 42:
        raise AssertionError(rust_run)
    if rust_run["fuel_used"] != rust_again["fuel_used"]:
        raise AssertionError("rust fuel was not deterministic")
    module = stream_ops.compile_rust_wasm(RUST, "add_one")
    print(
        "udf_exec python_udaf=%s python_udtf=%s wasm=%s wasm_fuel=%d "
        "rust=%s rust_bytes=%d rust_fuel=%d"
        % (
            first, rows, wasm_run["value"], wasm_run["fuel_used"],
            rust_run["value"], len(module), rust_run["fuel_used"],
        ),
        flush=True)
    print("udf_exec_test ok", flush=True)


if __name__ == "__main__":
    main()
