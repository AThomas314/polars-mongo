use ::polars_mongo::prelude::{MongoLazyReader, MongoScanOptions};
use polars::{frame::DataFrame, prelude::LazyFrame};
use pyo3::prelude::*;
use pyo3_polars::PyLazyFrame;
use pyo3_stub_gen::define_stub_info_gatherer;
#[pyfunction]
#[pyo3(signature = (connection_str, db, collection, infer_schema_length=100, batch_size=None))]
fn read_mongo(
    connection_str: String,
    db: String,
    collection: String,
    infer_schema_length: Option<usize>,
    batch_size: Option<usize>,
) -> PyResult<PyLazyFrame> {
    let options = MongoScanOptions {
        connection_str,
        db,
        collection,
        infer_schema_length,
        n_rows: None,
        batch_size,
    };
    let df: LazyFrame = LazyFrame::scan_mongo_collection(options)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    // .collect()
    // .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

    Ok(PyLazyFrame(df))
}

#[pymodule]
fn polars_mongo(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_mongo, m)?)?;
    Ok(())
}
define_stub_info_gatherer!(stub_info);
