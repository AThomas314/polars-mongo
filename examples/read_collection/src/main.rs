use polars::prelude::*;
use polars_mongo::prelude::*;
pub fn main() -> PolarsResult<()> {
    dotenv::dotenv().unwrap();
    // let _ = LazyFrame::scan_parquet(path, args);
    // let x = ScanArgsAnonymous;
    // let _ = ParquetOptions;
    // let _ = IpcScanOptions;
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
    // .select([col("cmdLine.config"), col("cmdLine")])
    // .filter(
    //     col("hostname")
    //         .eq(lit("ashish-LOQ-15IRX9"))
    //         .and(col("startTimeLocal").eq(lit("Thu Mar 12 01:58:45.668")))
    //         .and(col("startTimeLocal").eq(lit("Thu Mar 12 01:58:45.668")))
    //         .and(col("cmdLine").is_not_null())
    //         .or(col("pid").is_in(
    //             lit(Series::new(
    //                 PlSmallStr::from_str("name"),
    //                 vec![225949i64, 1588i64],
    //             )),
    //             true,
    //         )),
    // )
    .collect()?;
    dbg!(df);
    Ok(())
}
