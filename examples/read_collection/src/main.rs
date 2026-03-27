use polars::prelude::*;
use polarsmongo2::prelude::*;
pub fn main() -> PolarsResult<()> {
    dotenv::dotenv().unwrap();
    let connection_str = std::env::var("POLARS_MONGO_CONNECTION_URI").unwrap();
    let db = std::env::var("POLARS_MONGO_DB").unwrap();
    let collection = std::env::var("POLARS_MONGO_COLLECTION").unwrap();
    let df = LazyFrame::scan_mongo_collection(MongoScanOptions {
        batch_size: Some(5000),
        connection_str,
        db,
        collection,
        infer_schema_length: Some(100),
        n_rows: None,
    })?
    .select([
        col("_id"),
        col("title"),
        col("year"),
        col("directors"),
        col("imdb.rating"),
    ])
    .rename(["imdb.rating"], ["Rating"], true)
    // .filter(col("Rating").gt(lit(5.0)))
    // .filter(col("year").gt(lit(2000)))
    .collect()?;
    dbg!(df);
    Ok(())
}
