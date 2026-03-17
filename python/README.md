# polars-mongo
This package extends polars to read from mongodb collections via the LazyFrame apis. 

simply pip install polars_mongo (or rather, uv add polars_mongo)
from polars_mongo import scan_mongo

and pass connection_str,db, and collection to scan_mongo to get a lazyframe.
Toplevel fields are returned by default, subfields can be returned as seperate columns using dot-notation (i.e select(["parent.child1.child2"])) 
This library supports complete projection pushdown (Only the fields / subfields you required are queried from MongoDB). Predicate pushdown is planned for V0.2.0
