import polars as pl
from polars.io.plugins import register_io_source
from polars_mongo import PyMongoScanner


def scan_mongo(
    connection_str: str,
    db: str,
    collection: str,
    infer_schema_length: int | None = 100,
) -> pl.LazyFrame:
    scanner = PyMongoScanner(connection_str, db, collection, infer_schema_length)

    def source_generator(with_columns, predicate, n_rows, batch_size):
        # Call the Rust backend and YIELD the resulting DataFrame
        df = scanner(with_columns, predicate, n_rows, batch_size)
        yield df

    return register_io_source(
        io_source=source_generator,
        schema=scanner.schema,
    )


def main():
    lf: pl.LazyFrame = scan_mongo(
        "mongodb://127.0.0.1:27017",
        "sample_mflix",
        "movies",
        infer_schema_length=1,
    )

    print(lf.head(10).collect())


if __name__ == "__main__":
    main()
