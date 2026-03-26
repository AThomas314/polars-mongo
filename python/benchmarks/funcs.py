import polars as pl
from pymongo import collection
from pymongoarrow.api import find_polars_all

from . import polars_mongo


def blah():
    print("Blah")


def test_pymongoarrow(collection: collection.Collection, query) -> pl.DataFrame:
    df = find_polars_all(collection, query)
    return df
