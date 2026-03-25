# polars-mongo

polars-mongo is a high-performance Rust-backed connector that extends Polars to read from MongoDB collections via the LazyFrame API.

Key Features (v0.2.0)

    Binary ObjectId Mapping: Instead of  hex-encoding IDs into 24-character strings, we store _id as raw 12-byte Binary. This reduces memory footprint by 50% and accelerates joins.

    Hybrid Predicate Pushdown: Automatically pushes supported filters (e.g., $gt, $eq, $and) to MongoDB while safely falling back to Polars for complex logic, ensuring 100% correctness.

    OS-Aware Memory Allocation: The python library dynamically links jemalloc on Linux/macOS and mimalloc on Windows to minimize fragmentation and maximize throughput.

    Nested Path Support: Efficiently flattens BSON documents into Polars Structs or individual columns using dot-notation (e.g., imdb.rating).
