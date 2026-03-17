import polars as pl
import polars_mongo


def main():
    lf: pl.LazyFrame = polars_mongo.read_mongo(
        "mongodb://127.0.0.1:27017", "local", "startup_log"
    )
    df = lf.collect()
    print(df)


if __name__ == "__main__":
    main()
