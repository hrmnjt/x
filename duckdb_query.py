"""
This script provides a command-line interface to query CSV or Parquet files using DuckDB.
It supports both local files and S3 URIs, and can handle AWS credentials for S3 access.
You can choose between the native DuckDB CLI or Harlequin UI.

Usage:
    python script_name.py <file_path> [--type {csv,parquet}] [--profile AWS_PROFILE_NAME] [--ui {cli,harlequin}]

Arguments:
    file_path: Path to the file (local) or S3 URI
    --type: File type (csv or parquet, default: csv)
    --profile: AWS profile name for S3 access (optional)
    --ui: Choose between native DuckDB CLI or Harlequin UI (default: cli)

Requirements:
- DuckDB installed and accessible in the system PATH
- Harlequin installed (pip install harlequin) if using --ui harlequin
- AWS CLI configured (if accessing S3 files)
- Required Python packages: argparse, subprocess, tempfile, os, logging, urllib, typing
"""
import argparse
import subprocess
import tempfile
import os
import logging
from urllib.parse import urlparse
from typing import List, Optional
import shutil

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def is_s3_uri(uri: str) -> bool:
    """
    Check if the given URI is an S3 URI.

    Args:
        uri (str): The URI to check.

    Returns:
        bool: True if the URI is an S3 URI, False otherwise.
    """
    parsed = urlparse(uri)
    return parsed.scheme == 's3'

def check_harlequin_installed() -> bool:
    """
    Check if Harlequin is installed in the system.

    Returns:
        bool: True if Harlequin is installed, False otherwise.
    """
    return shutil.which('harlequin') is not None

def create_duckdb_init_script(file_path: str, file_type: str, is_cli: bool, profile_name: Optional[str] = None) -> str:
    """
    Create a DuckDB initialization script based on the file type and location.

    Args:
        file_path (str): Path to the file (local) or S3 URI.
        file_type (str): Type of the file ('csv' or 'parquet').
        is_cli (bool): Whether using native DuckDB CLI (True) or Harlequin (False).
        profile_name (Optional[str]): AWS profile name for S3 access.

    Returns:
        str: The DuckDB initialization script as a string.
    """
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
        f"CREATE OR REPLACE TABLE data AS SELECT * FROM {read_function}('{file_path}');"
    ])

    # Add CLI-specific settings when using native DuckDB CLI
    if is_cli:
        script.extend([
            ".mode duckbox",
            ".echo on"
        ])

    script.append("PRAGMA table_info('data');")

    return "\n".join(script)

def main() -> None:
    """
    Main function to handle command-line arguments and execute the query process.
    """
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Query CSV or Parquet files using DuckDB with choice of UI"
    )
    parser.add_argument("file_path", type=str, help="Path to the file (local) or S3 URI")
    parser.add_argument("--type", type=str, choices=["csv", "parquet"], default="csv", help="File type (default: csv)")
    parser.add_argument("--profile", type=str, help="AWS profile name for S3 access")
    parser.add_argument("--ui", type=str, choices=["cli", "harlequin"], default="cli",
                       help="Choose between native DuckDB CLI or Harlequin UI (default: cli)")

    args: argparse.Namespace = parser.parse_args()

    file_type: str = args.type.lower()
    file_path: str = args.file_path
    profile_name: Optional[str] = args.profile
    use_cli: bool = args.ui == "cli"

    if not use_cli and not check_harlequin_installed():
        logger.error("Harlequin is not installed. Please install it using: pip install harlequin")
        logger.info("Falling back to native DuckDB CLI...")
        use_cli = True

    logger.info(f"Initializing query for {file_type.upper()} file: {file_path}")
    logger.info(f"Using {'DuckDB CLI' if use_cli else 'Harlequin'} interface")

    if is_s3_uri(file_path) and not profile_name:
        logger.warning("S3 URI detected but no AWS profile specified. Using default credentials.")

    init_commands: str = create_duckdb_init_script(file_path, file_type, use_cli, profile_name)

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.sql') as temp_file:
        temp_file.write(init_commands)
        temp_file_path: str = temp_file.name
        logger.debug(f"Created temporary SQL file: {temp_file_path}")

    try:
        source: str = "S3" if is_s3_uri(file_path) else "local file"
        logger.info(f"Loading {file_type.upper()} from {source}: {file_path}")

        if use_cli:
            logger.info("Initializing DuckDB CLI...")
            subprocess.run(['duckdb', '-init', temp_file_path], check=True)
            logger.info("DuckDB CLI session completed successfully.")
        else:
            logger.info("Initializing Harlequin...")
            subprocess.run(['harlequin', 'duckdb', '-init', temp_file_path], check=True)
            logger.info("Harlequin session completed successfully.")

    except subprocess.CalledProcessError as e:
        logger.error(f"Process failed with exit code {e.returncode}")
        if e.output:
            logger.error(f"Output: {e.output.decode()}")
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

    logger.info("Session ended. To query again, rerun the command.")

if __name__ == "__main__":
    main()
