# Justfile
default:
    @just --list

# Query a CSV file from S3
queryc S3_URI PROFILE="grid2-prod":
    python duckdb_s3_query.py csv {{S3_URI}} {{PROFILE}}

# Query a Parquet file from S3
queryp S3_URI PROFILE="grid2-prod":
    python duckdb_s3_query.py parquet {{S3_URI}} {{PROFILE}}
