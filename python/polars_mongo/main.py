import polars as pl
from polars.io.plugins import register_io_source

from ._polars_mongo import PyMongoScanner  # type: ignore


def scan_mongo(
    connection_str: str,
    db: str,
    collection: str,
    infer_schema_length: int | None = 100,
) -> pl.LazyFrame:
    scanner = PyMongoScanner(connection_str, db, collection, infer_schema_length)

    def source_generator(with_columns, predicate, n_rows, batch_size):
        df = scanner(with_columns, predicate, n_rows, batch_size)
        yield df

    return register_io_source(
        io_source=source_generator,
        schema=scanner.schema,
    )
