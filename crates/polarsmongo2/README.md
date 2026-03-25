A high-performance, vectorized MongoDB connector for Polars, written in 100% Rust.

This crate provides a native bridge between MongoDB BSON and Arrow memory buffers, enabling lightning-fast data ingestion for Polars LazyFrame workflows.
Complete Projection Pushdown: Automatically translates Polars selections—including nested dot-notation paths (e.g., user.profile.email)— stages to minimize network I/O.
Parallel Ingestion: Leverages rayon and the AnonymousScan trait to parallelize the MongoDB cursor workload across all available CPU cores.

Schema Inference: Dynamically samples collections to build accurate Polars schemas, with support for complex nested Struct and List types.

Memory Efficiency: Designed for "near-zero copy" throughput by allocating contiguous memory buffers before ingestion.
Quick Example (Rust)
Rust

use polars::prelude::*;
use polarsmongo2::prelude::*;

fn main() -> PolarsResult<()> {
    // Define your connection and collection
    let options = MongoScanOptions {
        connection_str: "mongodb://localhost:27017".to_string(),
        db: "fintech_audit".to_string(),
        collection: "transactions".to_string(),
        infer_schema_length: Some(1000),
        n_rows: None,
        batch_size: Some(1000),
    };

    // Initialize the LazyFrame
    let lf = LazyFrame::scan_mongo_collection(options)?;

    // Only "date" and "metadata.amount" will be queried from MongoDB
    let df = lf
        .select([
            col("date"),
            col("metadata.amount")
        ])
        .collect()?;

    println!("{:?}", df);
    Ok(())
}

Roadmap
    [x] V0.1.x: Core vectorized engine, Projection Pushdown, Nested field support.
    [x] V0.2.0: Predicate Pushdown (filtering at the database level).
