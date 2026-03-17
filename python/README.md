# polars-mongo
A high-performance, native MongoDB connector for Polars, written in Rust.
Why polars-mongo?
1. Filter columns directly at the database, avoiding the python overhead
2. Keep polars syntax
Key Features

    Native Speed: Built with Rust and pyo3-polars for maximum BSON-to-Arrow throughput.

    Projection Pushdown: Fully optimized to query only the fields you select (including subfields).

    Lazy Integration: Works seamlessly with the pl.LazyFrame API.

Installation    
    uv add polars-mongo
    # or
    pip install polars-mongo
    
Roadmap
    [ ] V0.2.0: Predicate Pushdown (Filtering at the database level).
