use polars::prelude::*;
use polars_mongo::prelude::*;
pub fn main() -> PolarsResult<()> {
    dotenv::dotenv().unwrap();
    let connection_str = std::env::var("POLARS_MONGO_CONNECTION_URI").unwrap();
    let db = std::env::var("POLARS_MONGO_DB").unwrap();
    let collection = std::env::var("POLARS_MONGO_COLLECTION").unwrap();
    let df = LazyFrame::scan_mongo_collection(MongoScanOptions {
        batch_size: None,
        connection_str,
        db,
        collection,
        infer_schema_length: Some(1000),
        n_rows: None,
    })?
    .filter(col(PlSmallStr::from_str("startTimeLocal")).eq(lit("Thu Mar 12 00:18:42.288")))
    .collect()?;
    dbg!(df);
    Ok(())
}
