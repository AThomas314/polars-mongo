import time

import polars as pl

from polars_mongo import scan_mongo

# from . import funcs


def main():
    start = time.time()
    print(
        scan_mongo("mongodb://127.0.0.1:27017", "sample_mflix", "movies", 100)
        # .select(["title", "year", "directors", "imdb.rating"])
        .collect()
    )
    print(time.time() - start)


if __name__ == "__main__":
    main()
