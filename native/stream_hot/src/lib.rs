use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use std::collections::BTreeMap;

/// Sum weights by key, drop zeros, and return keys in order.
#[pyfunction]
fn consolidate(pairs: Vec<(String, i64)>) -> PyResult<Vec<(String, i64)>> {
    let mut totals: BTreeMap<String, i64> = BTreeMap::new();
    for (key, weight) in pairs {
        let slot = totals.entry(key).or_insert(0);
        *slot = slot.checked_add(weight).ok_or_else(|| {
            PyTypeError::new_err("weight overflow")
        })?;
    }
    Ok(totals.into_iter().filter(|(_, weight)| *weight != 0).collect())
}

#[pymodule]
fn stream_hot_rs(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(consolidate, module)?)?;
    Ok(())
}
