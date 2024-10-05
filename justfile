# Default command to show available commands
default:
    @just --list

# Set default values
default_profile := "grid2-prod"
script_name := "duckdb_query.py"

# Query a file (auto-detect type based on extension)
query FILE_PATH PROFILE=default_profile:
    #!/usr/bin/env bash
    set -euo pipefail
    file_type=$(echo "{{FILE_PATH}}" | awk -F. '{print tolower($NF)}')
    if [[ "$file_type" != "csv" && "$file_type" != "parquet" ]]; then
        echo "Error: Unsupported file type. Use 'queryc' or 'queryp' for explicit CSV or Parquet querying."
        exit 1
    fi
    python {{script_name}} "{{FILE_PATH}}" --type $file_type --profile "{{PROFILE}}"

# Query a CSV file
queryc FILE_PATH PROFILE=default_profile:
    python {{script_name}} "{{FILE_PATH}}" --type csv --profile "{{PROFILE}}"

# Query a Parquet file
queryp FILE_PATH PROFILE=default_profile:
    python {{script_name}} "{{FILE_PATH}}" --type parquet --profile "{{PROFILE}}"

# Query a local file (auto-detect type based on extension)
querylocal FILE_PATH:
    #!/usr/bin/env bash
    set -euo pipefail
    file_type=$(echo "{{FILE_PATH}}" | awk -F. '{print tolower($NF)}')
    if [[ "$file_type" != "csv" && "$file_type" != "parquet" ]]; then
        echo "Error: Unsupported file type. Use 'querylocalc' or 'querylocalp' for explicit CSV or Parquet querying."
        exit 1
    fi
    python {{script_name}} "{{FILE_PATH}}" --type $file_type

# Query a local CSV file
querylocalc FILE_PATH:
    python {{script_name}} "{{FILE_PATH}}" --type csv

# Query a local Parquet file
querylocalp FILE_PATH:
    python {{script_name}} "{{FILE_PATH}}" --type parquet

# List available AWS profiles
list-profiles:
    aws configure list-profiles

# Show script help
help:
    python {{script_name}} --help
