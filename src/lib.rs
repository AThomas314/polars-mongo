//! Polars mongo is a connector to read from a mongodb collection into a Polars dataframe.
//! Usage:
//! ```rust
//! use polars::prelude::*;
//! use polars_mongo::prelude::*;
//!
//! pub fn main() -> PolarsResult<()> {
//!     let connection_str = std::env::var("POLARS_MONGO_CONNECTION_URI").unwrap();
//!     let db = std::env::var("POLARS_MONGO_DB").unwrap();
//!     let collection = std::env::var("POLARS_MONGO_COLLECTION").unwrap();
//!
//!     let df = LazyFrame::scan_mongo_collection(MongoScanOptions {
//!         batch_size: None,
//!         connection_str,
//!         db,
//!         collection,
//!         infer_schema_length: Some(1000),
//!         n_rows: None,
//!     })?
//!     .collect()?;
//!
//!     dbg!(df);
//!     Ok(())
//! }
//!
#![deny(clippy::all)]
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
mod buffer;
mod conversion;
pub mod prelude;

use crate::buffer::*;

use conversion::Wrap;
use polars::{frame::row::*, prelude::*};
use polars_core::POOL;
use rayon::prelude::*;

use mongodb::{
    bson::{Bson, Document},
    options::{ClientOptions, FindOptions},
    sync::{Client, Collection, Cursor},
};
use polars_core::utils::accumulate_dataframes_vertical;

pub struct MongoScan {
    client_options: ClientOptions,
    db: String,
    collection_name: String,
    pub collection: Option<Collection<Document>>,
    pub n_threads: Option<usize>,
    pub batch_size: Option<usize>,
    pub rechunk: bool,
}

impl MongoScan {
    pub fn with_rechunk(mut self, rechunk: bool) -> Self {
        self.rechunk = rechunk;
        self
    }
    pub fn with_batch_size(mut self, batch_size: Option<usize>) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn new(connection_str: String, db: String, collection: String) -> PolarsResult<Self> {
        let client_options = ClientOptions::parse(connection_str).run().map_err(|e| {
            PolarsError::InvalidOperation(format!("unable to connect to mongodb: {}", e).into())
        })?;

        Ok(MongoScan {
            client_options,
            db,
            collection_name: collection,
            collection: None,
            n_threads: None,
            rechunk: false,
            batch_size: None,
        })
    }

    fn get_collection(&self) -> Collection<Document> {
        let client = Client::with_options(self.client_options.clone()).unwrap();

        let database = client.database(&self.db);
        database.collection::<Document>(&self.collection_name)
    }

    fn parse_lines<'a>(
        &self,
        mut cursor: Cursor<Document>,
        buffers: &mut PlIndexMap<PlSmallStr, Buffer<'a>>,
    ) -> mongodb::error::Result<()> {
        while let Some(Ok(doc)) = cursor.next() {
            for (field_path, inner_buffer) in buffers.iter_mut() {
                match get_nested_bson(&doc, field_path.as_str()) {
                    Some(v) => {
                        inner_buffer.add(v).expect("Failed to add value to buffer");
                    }
                    None => {
                        inner_buffer.add_null();
                    }
                }
            }
        }
        Ok(())
    }
}

impl AnonymousScan for MongoScan {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn scan(&self, scan_opts: AnonymousScanArgs) -> PolarsResult<DataFrame> {
        let collection = &self.get_collection();
        let filter = expr_to_document(scan_opts.predicate);
        let projection = scan_opts.output_schema.clone().map(|schema| {
            let prj = schema
                .iter_names()
                .map(|name| (name.to_string(), Bson::Int64(1)));

            Document::from_iter(prj)
        });
        let mut find_options = FindOptions::default();

        find_options.projection = projection;
        find_options.batch_size = self.batch_size.map(|b| b as u32);

        let schema = scan_opts.output_schema.unwrap_or_else(|| {
            let filtered: Schema = scan_opts
                .schema
                .iter()
                .filter(|(name, _)| !name.contains('.'))
                .map(|(name, dtype)| (name.clone(), dtype.clone()))
                .collect();
            Arc::new(filtered)
        });
        let n_rows = match scan_opts.n_rows {
            Some(n) => n,
            None => collection.estimated_document_count().run().unwrap_or(0) as usize,
        };

        let mut n_threads = self.n_threads.unwrap_or_else(|| POOL.current_num_threads());
        if n_rows < 128 {
            n_threads = 1
        }

        let rows_per_thread = n_rows / n_threads;
        let projection = find_options.projection.unwrap_or(Document::new());

        let dfs = POOL
            .install(|| {
                (0..n_threads).into_par_iter().map(|idx| {
                    let start = idx * rows_per_thread;

                    let cursor = collection
                        .find(Document::new())
                        .skip(start as u64)
                        .limit(rows_per_thread as i64)
                        .projection(projection.clone())
                        .batch_size(self.batch_size.unwrap_or(1) as u32)
                        .run()
                        .map_err(|err| {
                            PolarsError::ComputeError(format!("Mongo find failed: {}", err).into())
                        })?;

                    let mut buffers = init_buffers(schema.as_ref(), rows_per_thread)?;
                    self.parse_lines(cursor, &mut buffers)
                        .map_err(|err| PolarsError::ComputeError(format!("{:#?}", err).into()))?;
                    let series = buffers
                        .into_values()
                        .map(|buf| Column::from(buf.into_series().unwrap()))
                        .collect::<Vec<Column>>();
                    DataFrame::new(rows_per_thread, series)
                })
            })
            .collect::<PolarsResult<Vec<_>>>()?;
        let mut df = accumulate_dataframes_vertical(dfs)?;

        if self.rechunk {
            df.rechunk_mut();
        }
        Ok(df)
    }

    fn schema(&self, infer_schema_length: Option<usize>) -> PolarsResult<Arc<Schema>> {
        let collection: Collection<Document> = self.get_collection();
        let limit = infer_schema_length.unwrap_or(100);

        let cursor = collection
            .find(Document::new())
            .limit(limit as i64)
            .run()
            .map_err(|err| {
                PolarsError::ComputeError(format!("MongoDB find failed: {:#?}", err).into())
            })?;

        let docs_iter = cursor.take(limit).map(|doc_res| {
            let doc = doc_res.expect("Failed to read document from MongoDB cursor");
            let mut row_fields = Vec::new();
            collect_fields(&doc, None, &mut row_fields);
            row_fields
        });

        // infer_schema will now merge all these paths across the 'limit' documents
        let schema = infer_schema(docs_iter, limit);
        Ok(Arc::new(schema))
    }
    fn allows_predicate_pushdown(&self) -> bool {
        true
    }
    fn allows_projection_pushdown(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct MongoScanOptions {
    /// mongodb style connection string. `mongodb://<user>:<password>@host.domain`
    pub connection_str: String,
    /// the name of the mongodb database
    pub db: String,
    /// the name of the mongodb collection
    pub collection: String,
    // Number of rows used to infer the schema. Defaults to `100` if not provided.
    pub infer_schema_length: Option<usize>,
    /// Number of rows to return from mongodb collection. If not provided, it will fetch all rows from collection.
    pub n_rows: Option<usize>,
    /// determines the number of records to return from a single request to mongodb
    pub batch_size: Option<usize>,
}

pub trait MongoLazyReader {
    fn scan_mongo_collection(options: MongoScanOptions) -> PolarsResult<LazyFrame> {
        let f = MongoScan::new(options.connection_str, options.db, options.collection)?;
        let args = ScanArgsAnonymous {
            name: "MONGO SCAN",
            infer_schema_length: options.infer_schema_length,
            n_rows: options.n_rows,
            ..ScanArgsAnonymous::default()
        };
        LazyFrame::anonymous_scan(Arc::new(f), args)
    }
}
fn expr_to_document(expr: Option<Expr>) -> Result<Document, Box<dyn std::error::Error>> {
    match expr {
        Some(expr) => {
            println!("expr {:#?}", expr);
            Ok(Document::new())
        }
        None => Ok(Document::new()),
    }
}
fn collect_fields(doc: &Document, prefix: Option<&str>, fields: &mut Vec<(PlSmallStr, DataType)>) {
    for (key, value) in doc {
        let name = match prefix {
            Some(p) => format!("{}.{}", p, key),
            None => key.clone(),
        };

        let dtype: Wrap<DataType> = value.into();
        fields.push((PlSmallStr::from_str(&name), dtype.0.clone()));

        // If it's a nested document, recurse to expose sub-fields
        if let Bson::Document(inner_doc) = value {
            collect_fields(inner_doc, Some(&name), fields);
        }
    }
}
fn get_nested_bson<'a>(doc: &'a Document, path: &str) -> Option<&'a Bson> {
    let mut current_value: Option<&Bson> = None;
    let mut current_doc = doc;
    let parts: Vec<&str> = path.split('.').collect();

    for (i, part) in parts.iter().enumerate() {
        if i == parts.len() - 1 {
            // We've reached the final key in the path
            current_value = current_doc.get(part);
        } else {
            // Move deeper into the nested document
            match current_doc.get(part) {
                Some(Bson::Document(next_doc)) => current_doc = next_doc,
                _ => return None, // Path doesn't exist or isn't a document
            }
        }
    }
    current_value
}

impl MongoLazyReader for LazyFrame {}
