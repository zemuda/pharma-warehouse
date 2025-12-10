#!/usr/bin/env python3
"""
Script to run Excel to Parquet Pipeline with configuration from .env file
"""

import os
import argparse
from pathlib import Path
from dotenv import load_dotenv


def load_environment():
    """Load environment variables from .env file"""
    env_path = Path(".") / ".env"
    if env_path.exists():
        load_dotenv(env_path)
        print("✓ Loaded configuration from .env file")
    else:
        print("⚠ .env file not found, using default values")


def run_pipeline():
    """Run the Excel to Parquet pipeline"""

    # Load environment variables
    load_environment()

    # Get configuration from environment with fallbacks
    config = {
        "input_dir": os.getenv("INPUT_DIR", "./raw_data/input"),
        "output_dir": os.getenv("OUTPUT_DIR", "./raw_data/output"),
        "state_file": os.getenv("STATE_FILE", "./raw_data/state/processing_state.json"),
        "log_level": os.getenv("LOG_LEVEL", "INFO"),
        "clean_orphaned": os.getenv("CLEAN_ORPHANED", "true").lower() == "true",
        "show_logs": os.getenv("SHOW_LOGS", "true").lower() == "true",
        "log_limit": int(os.getenv("LOG_LIMIT", "10")),
        "file_pattern": os.getenv("FILE_PATTERN", "*.xls"),
        "db_host": os.getenv("DB_HOST", "localhost"),
        "db_port": os.getenv("DB_PORT", "5432"),
        "db_name": os.getenv("DB_NAME", "pharma_warehouse"),
        "db_user": os.getenv("DB_USER", "postgres"),
        "db_password": os.getenv("DB_PASSWORD", "1234"),
    }

    # Create directories if they don't exist
    Path(config["input_dir"]).mkdir(parents=True, exist_ok=True)
    Path(config["output_dir"]).mkdir(parents=True, exist_ok=True)
    Path(config["state_file"]).parent.mkdir(parents=True, exist_ok=True)

    # Build command
    cmd = [
        "python",
        "excel_parquet_pipeline.py",
        "--input-dir",
        config["input_dir"],
        "--output-dir",
        config["output_dir"],
        "--state-file",
        config["state_file"],
        "--log-level",
        config["log_level"],
        "--pattern",
        config["file_pattern"],
        "--db-host",
        config["db_host"],
        "--db-port",
        config["db_port"],
        "--db-name",
        config["db_name"],
        "--db-user",
        config["db_user"],
        "--db-password",
        config["db_password"],
    ]

    # Add optional flags
    if config["clean_orphaned"]:
        cmd.append("--clean-orphaned")

    if config["show_logs"]:
        cmd.append("--show-logs")
        cmd.extend(["--log-limit", str(config["log_limit"])])

    # Print configuration
    print("\n=== Pipeline Configuration ===")
    print(f"Input Directory: {config['input_dir']}")
    print(f"Output Directory: {config['output_dir']}")
    print(f"State File: {config['state_file']}")
    print(
        f"Database: {config['db_user']}@{config['db_host']}:{config['db_port']}/{config['db_name']}"
    )
    print(f"Clean Orphaned: {config['clean_orphaned']}")
    print(f"Show Logs: {config['show_logs']}")
    print(f"Log Limit: {config['log_limit']}")
    print("=" * 30)

    # Execute the pipeline
    try:
        # Import and run the pipeline directly
        from raw_layer.src.excel_parquet_pipeline import main as pipeline_main
        import sys

        # Set up command line arguments for the pipeline
        sys.argv = cmd[1:]  # Remove 'python' from the command

        print("\n🚀 Starting Excel to Parquet Pipeline...")
        pipeline_main()

    except ImportError as e:
        print(f"❌ Error: Could not import pipeline module: {e}")
        print("Make sure excel_parquet_pipeline.py is in the same directory")
        return 1
    except Exception as e:
        print(f"❌ Error running pipeline: {e}")
        return 1

    return 0


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="Run Excel to Parquet Pipeline with .env configuration"
    )
    parser.add_argument(
        "--env-file", default=".env", help="Path to .env file (default: .env)"
    )

    args = parser.parse_args()

    # Set custom env file if provided
    if args.env_file != ".env":
        os.environ["DOTENV_PATH"] = args.env_file

    # Run the pipeline
    return run_pipeline()


if __name__ == "__main__":
    exit(main())
