# Superset Github Example

This project turns deeply nested Github events from Githubarchive.org into Parquet tables in preparation for being 
filtered down to just Apache projects and then loaded into a SQL database.

## Data Collection

See: [`download.sh`](download.sh)

Requires `wget`.

## PySpark Processing into Parquet

Processing the data is very slow so the first step is to encode the data as column compressed Parquet tables to improve 
load times. This is done in [`build_parquet_tables.spark.py`](build_parquet_tables.spark.py), which should run on an EMR
cluster with Spark and a bootstrap script on S3 in the form of [`emr_bootstrap.sh`](emr_bootstrap.sh).