# Default command to show available commands
default:
    @echo "just what?"
    @echo "----------"
    @just --list

# Set default values
default_aws_profile := "grid2-prod"

# Query a file (auto-detect type based on extension)
query FILE_PATH PROFILE=default_aws_profile:
    #!/usr/bin/env bash
    set -euo pipefail
    file_type=$(echo "{{FILE_PATH}}" | awk -F. '{print tolower($NF)}')
    if [[ "$file_type" != "csv" && "$file_type" != "parquet" ]]; then
        echo "Error: Unsupported file type. Use 'queryc' or 'queryp' for explicit CSV or Parquet querying."
        exit 1
    fi
    python duckdb_query.py "{{FILE_PATH}}" --type $file_type --profile "{{PROFILE}}"

# Query a CSV file
queryc FILE_PATH PROFILE=default_aws_profile:
    python duckdb_query.py} "{{FILE_PATH}}" --type csv --profile "{{PROFILE}}"

# Query a Parquet file
queryp FILE_PATH PROFILE=default_aws_profile:
    python duckdb_query.py "{{FILE_PATH}}" --type parquet --profile "{{PROFILE}}"

# npq = native (duckdb cli) parquet query
npq FILE_PATH PROFILE=default_aws_profile:
    python duckdb_query.py "{{FILE_PATH}}" --type parquet --profile "{{PROFILE}}" --ui cli

# hpq = harlequin parquet query
hpq FILE_PATH PROFILE=default_aws_profile:
    python duckdb_query.py "{{FILE_PATH}}" --type parquet --profile "{{PROFILE}}" --ui harlequin
