use ::polars_mongo::prelude::{MongoLazyReader, MongoScan, MongoScanOptions};
use polars::prelude::{AnonymousScan, AnonymousScanArgs, Expr, LazyFrame};
use polars_core::prelude::PlSmallStr;
use polars_core::schema::{Schema, SchemaRef};
use pyo3::prelude::*;
use pyo3_polars::*;
use pyo3_stub_gen::define_stub_info_gatherer;
use std::sync::Arc;
#[pyclass]
pub struct PyMongoScanner {
    inner: MongoScan,
    schema: SchemaRef,
}

#[pymethods]
impl PyMongoScanner {
    #[new]
    fn new(
        connection_str: String,
        db: String,
        collection: String,
        infer_schema_length: Option<usize>,
    ) -> PyResult<Self> {
        let scanner = MongoScan::new(connection_str, db, collection)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        let raw_schema = scanner
            .schema(infer_schema_length)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        let mut top_level_schema = Schema::default();
        for (name, dtype) in raw_schema.iter() {
            if !name.as_str().contains('.') {
                top_level_schema.with_column(name.clone(), dtype.clone());
            }
        }
        Ok(Self {
            inner: scanner,
            // schema: Arc::new(top_level_schema),
            schema: raw_schema,
        })
    }
    #[getter]
    fn schema(&self) -> PySchema {
        PySchema(self.schema.clone())
    }

    #[pyo3(signature = (with_columns=None, _predicate=None, n_rows=None, _batch_size=None))]
    fn __call__(
        &self,
        with_columns: Option<Vec<String>>,
        _predicate: Option<PyExpr>,
        n_rows: Option<usize>,
        _batch_size: Option<usize>,
    ) -> PyResult<PyDataFrame> {
        let output_schema = if let Some(cols) = with_columns.clone() {
            let mut proj_schema = Schema::default();
            for col in cols {
                if let Some((_, _, dtype)) = self.schema.get_full(&col) {
                    proj_schema.with_column(col.into(), dtype.clone());
                }
            }
            Some(Arc::new(proj_schema))
        } else {
            let mut top_level_schema = Schema::default();
            for (name, dtype) in self.schema.iter() {
                if !name.as_str().contains('.') {
                    top_level_schema.with_column(name.clone(), dtype.clone());
                }
            }
            Some(Arc::new(top_level_schema))
        };
        let predicate: Option<Expr> = if let Some(_predicate) = _predicate {
            Some(_predicate.0)
        } else {
            None
        };
        let with_columns = with_columns.map(|cols| {
            cols.into_iter()
                .map(PlSmallStr::from) // or |s| s.into()
                .collect::<Arc<[PlSmallStr]>>()
        });
        let args = AnonymousScanArgs {
            n_rows,
            with_columns,
            schema: self.schema.clone(),
            output_schema,
            predicate: predicate,
        };

        let df = self
            .inner
            .scan(args)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        Ok(PyDataFrame(df))
    }
}
#[pymodule]
fn polars_mongo(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyMongoScanner>()?;
    Ok(())
}

define_stub_info_gatherer!(stub_info);
