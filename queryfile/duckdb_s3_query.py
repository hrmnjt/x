import sys
import subprocess
import tempfile
import os

def create_duckdb_init_script(file_type, s3_uri, profile_name):
    read_function = "read_csv" if file_type == "csv" else "read_parquet"
    return f"""
INSTALL aws;
LOAD aws;
CALL load_aws_credentials('{profile_name}');
CREATE OR REPLACE TABLE data AS SELECT * FROM {read_function}('{s3_uri}');
.mode box
.echo on
PRAGMA table_info('data');
"""

def main():
    if len(sys.argv) < 3:
        print("Usage: python duckdb_s3_query.py <file_type> <s3_uri> [profile_name]")
        print("file_type should be 'csv' or 'parquet'")
        sys.exit(1)

    file_type = sys.argv[1].lower()
    if file_type not in ['csv', 'parquet']:
        print("Error: file_type must be 'csv' or 'parquet'")
        sys.exit(1)

    s3_uri = sys.argv[2]
    profile_name = sys.argv[3] if len(sys.argv) > 3 else 'default'

    init_commands = create_duckdb_init_script(file_type, s3_uri, profile_name)

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.sql') as temp_file:
        temp_file.write(init_commands)
        temp_file_path = temp_file.name

    try:
        print(f"Loading {file_type.upper()} file from {s3_uri}")
        print("Initializing DuckDB...")

        subprocess.run(['duckdb', '-init', temp_file_path], check=True)

    except subprocess.CalledProcessError as e:
        print(f"Error: DuckDB process failed with exit code {e.returncode}")
        if e.output:
            print(f"DuckDB output: {e.output.decode()}")
    except FileNotFoundError:
        print("Error: DuckDB executable not found. Please ensure DuckDB is installed and in your PATH.")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
    finally:
        try:
            os.unlink(temp_file_path)
        except Exception as e:
            print(f"Warning: Could not delete temporary file {temp_file_path}: {str(e)}")

    print("\nExited DuckDB CLI. To query again, rerun the command.")

if __name__ == "__main__":
    main()
