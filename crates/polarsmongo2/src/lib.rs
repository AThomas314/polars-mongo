//! Polars mongo is a connector to read from a mongodb collection into a Polars dataframe.
//! Usage:
//! ```no_run
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
    options::FindOptions,
    sync::{Client, Collection, Cursor},
};
use polars_core::utils::accumulate_dataframes_vertical;
use serde::{Deserialize, Serialize};

// #[derive(Serialize, Deserialize)]
pub struct MongoScan {
    connection_str: String,
    db: String,
    collection_name: String,
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
        Ok(MongoScan {
            connection_str,
            db,
            collection_name: collection,
            // collection: None,
            n_threads: None,
            rechunk: false,
            batch_size: None,
        })
    }

    fn get_collection(&self) -> Collection<Document> {
        let client = Client::with_uri_str(self.connection_str.clone()).unwrap();
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
        let projection = scan_opts.output_schema.clone().map(|schema| {
            let mut root_keys = std::collections::HashSet::new();
            for name in schema.iter_names() {
                //Handle path collisions
                let root_key = name.split('.').next().unwrap_or(name.as_str());
                root_keys.insert(root_key.to_string());
            }
            let prj = root_keys
                .into_iter()
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

        let schema = infer_schema(docs_iter, limit);
        Ok(Arc::new(schema))
    }
    fn allows_predicate_pushdown(&self) -> bool {
        false
    }
    fn allows_projection_pushdown(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

fn collect_fields(doc: &Document, prefix: Option<&str>, fields: &mut Vec<(PlSmallStr, DataType)>) {
    for (key, value) in doc {
        let name = match prefix {
            Some(p) => format!("{}.{}", p, key),
            None => key.clone(),
        };

        let dtype: Wrap<DataType> = value.into();
        fields.push((PlSmallStr::from_str(&name), dtype.0.clone()));

        if let Bson::Document(inner_doc) = value {
            collect_fields(inner_doc, Some(&name), fields);
        }
    }
}
fn get_nested_bson<'a>(doc: &'a Document, path: &str) -> Option<&'a Bson> {
    let mut current_doc = doc;
    let mut iter = path.split('.').peekable();
    while let Some(part) = iter.next() {
        if iter.peek().is_none() {
            return current_doc.get(part);
        } else {
            match current_doc.get(part) {
                Some(Bson::Document(next_doc)) => current_doc = next_doc,
                _ => return None,
            }
        }
    }
    None
}

impl MongoLazyReader for LazyFrame {}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::doc;

    // -------------------------------------------------------------------------
    // 1. Tests for `get_nested_bson`
    // -------------------------------------------------------------------------

    #[test]
    fn test_get_nested_bson_flat() {
        let document = doc! {
            "name": "Alice",
            "age": 30i32
        };

        // Test existing top-level fields
        assert_eq!(
            get_nested_bson(&document, "name"),
            Some(&Bson::String("Alice".to_string()))
        );
        assert_eq!(get_nested_bson(&document, "age"), Some(&Bson::Int32(30)));

        // Test missing field
        assert_eq!(get_nested_bson(&document, "missing"), None);
    }

    #[test]
    fn test_get_nested_bson_deep() {
        let document = doc! {
            "user": {
                "profile": {
                    "email": "alice@example.com",
                    "id": 100i32
                }
            }
        };

        assert_eq!(
            get_nested_bson(&document, "user.profile.email"),
            Some(&Bson::String("alice@example.com".to_string()))
        );
        assert_eq!(
            get_nested_bson(&document, "user.profile.id"),
            Some(&Bson::Int32(100))
        );

        let profile_doc = get_nested_bson(&document, "user.profile").unwrap();
        assert!(matches!(profile_doc, Bson::Document(_)));

        assert_eq!(get_nested_bson(&document, "user.profile.id.invalid"), None);
    }

    #[test]
    fn test_collect_fields_flat() {
        let document = doc! {
            "name": "Bob",
            "active": true
        };

        let mut fields = Vec::new();
        collect_fields(&document, None, &mut fields);

        fields.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].0.as_str(), "active");
        assert_eq!(fields[0].1, DataType::Boolean);
        assert_eq!(fields[1].0.as_str(), "name");
        assert_eq!(fields[1].1, DataType::String);
    }

    #[test]
    fn test_collect_fields_nested() {
        let document = doc! {
            "config": {
                "port": 8080i32
            }
        };

        let mut fields = Vec::new();
        collect_fields(&document, None, &mut fields);

        assert_eq!(fields.len(), 2);

        let config_field = fields
            .iter()
            .find(|(name, _)| name.as_str() == "config")
            .unwrap();
        let port_field = fields
            .iter()
            .find(|(name, _)| name.as_str() == "config.port")
            .unwrap();

        assert_eq!(port_field.1, DataType::Int32);

        match &config_field.1 {
            DataType::Struct(struct_fields) => {
                assert_eq!(struct_fields.len(), 1);
                assert_eq!(struct_fields[0].name().as_str(), "port");
                assert_eq!(struct_fields[0].dtype(), &DataType::Int32);
            }
            _ => panic!("Expected config to be a Struct datatype"),
        }
    }
}
