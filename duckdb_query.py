import argparse
import subprocess
import tempfile
import os
import logging
from urllib.parse import urlparse
from typing import List, Optional

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def is_s3_uri(uri: str) -> bool:
    parsed = urlparse(uri)
    return parsed.scheme == 's3'

def create_duckdb_init_script(file_path: str, file_type: str, profile_name: Optional[str] = None) -> str:
    read_function: str = "read_csv" if file_type == "csv" else "read_parquet"

    script: List[str] = []

    if is_s3_uri(file_path):
        logger.info("S3 URI detected. Adding AWS extension commands.")
        script.extend([
            "INSTALL aws;",
            "LOAD aws;",
            f"CALL load_aws_credentials('{profile_name}');" if profile_name else ""
        ])

    script.extend([
        f"CREATE OR REPLACE TABLE data AS SELECT * FROM {read_function}('{file_path}');",
        ".mode box",
        ".echo on",
        "PRAGMA table_info('data');"
    ])

    return "\n".join(script)

def main() -> None:
    parser: argparse.ArgumentParser = argparse.ArgumentParser(description="Query CSV or Parquet files using DuckDB")
    parser.add_argument("file_path", type=str, help="Path to the file (local) or S3 URI")
    parser.add_argument("--type", type=str, choices=["csv", "parquet"], default="csv", help="File type (default: csv)")
    parser.add_argument("--profile", type=str, help="AWS profile name for S3 access")

    args: argparse.Namespace = parser.parse_args()

    file_type: str = args.type.lower()
    file_path: str = args.file_path
    profile_name: Optional[str] = args.profile

    logger.info(f"Initializing query for {file_type.upper()} file: {file_path}")

    if is_s3_uri(file_path) and not profile_name:
        logger.warning("S3 URI detected but no AWS profile specified. Using default credentials.")

    init_commands: str = create_duckdb_init_script(file_path, file_type, profile_name)

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.sql') as temp_file:
        temp_file.write(init_commands)
        temp_file_path: str = temp_file.name
        logger.debug(f"Created temporary SQL file: {temp_file_path}")

    try:
        source: str = "S3" if is_s3_uri(file_path) else "local file"
        logger.info(f"Loading {file_type.upper()} from {source}: {file_path}")
        logger.info("Initializing DuckDB...")

        subprocess.run(['duckdb', '-init', temp_file_path], check=True)
        logger.info("DuckDB query completed successfully.")

    except subprocess.CalledProcessError as e:
        logger.error(f"DuckDB process failed with exit code {e.returncode}")
        if e.output:
            logger.error(f"DuckDB output: {e.output.decode()}")
    except FileNotFoundError:
        logger.error("DuckDB executable not found. Please ensure DuckDB is installed and in your PATH.")
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {str(e)}")
    finally:
        try:
            os.unlink(temp_file_path)
            logger.debug(f"Deleted temporary file: {temp_file_path}")
        except Exception as e:
            logger.warning(f"Could not delete temporary file {temp_file_path}: {str(e)}")

    logger.info("Exited DuckDB CLI. To query again, rerun the command.")

if __name__ == "__main__":
    main()
