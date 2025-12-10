#!/usr/bin/env python3
"""
Excel to Parquet Pipeline Script
Incrementally processes nested Excel files and converts them to Parquet format.
Logs are saved in PostgreSQL database.
Uses YAML configuration file.
"""

import os
import json
import hashlib
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union, Any
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dataclasses import dataclass, asdict
import psycopg2
from psycopg2.extras import RealDictCursor
import uuid
import yaml

# Add scripts directory to Python path
scripts_dir = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

# Try to import config manager
USE_CONFIG_MANAGER = False
config_manager = None

try:
    from config_manager import ConfigManager, get_config_manager

    USE_CONFIG_MANAGER = True
    print("[INFO] Config manager module found")
except ImportError as e:
    print(f"[WARN] Config manager not found: {e}")
    USE_CONFIG_MANAGER = False


def load_yaml_config(config_path: str = None) -> Dict[str, Any]:
    """
    Load configuration from YAML file with proper path handling

    Args:
        config_path: Path to YAML configuration file

    Returns:
        Dictionary containing configuration
    """
    if config_path is None:
        # Try to find config file in common locations with correct paths
        possible_paths = [
            # Current directory
            Path.cwd() / "config.yaml",
            Path.cwd() / "config.yml",
            # Scripts directory (two levels up from src)
            Path(__file__).parent.parent / "scripts" / "config.yaml",
            # Parent directory
            Path(__file__).parent.parent / "config.yaml",
            # Project root
            Path(__file__).parent.parent.parent / "config.yaml",
            # Same directory as script
            Path(__file__).parent / "config.yaml",
        ]

        print("\n[INFO] Looking for configuration file:")
        for path in possible_paths:
            exists = path.exists()
            print(f"  [{'FOUND' if exists else 'MISS'}] {path.absolute()}")
            if exists:
                config_path = str(path.absolute())
                print(f"\n[INFO] Found configuration at: {config_path}")
                break
        else:
            error_msg = (
                "\n[ERROR] Could not find config.yaml file.\n\n"
                "Please create one in one of these locations:\n"
                f"1. {Path(__file__).parent.parent / 'scripts' / 'config.yaml'}\n"
                f"2. {Path(__file__).parent.parent / 'config.yaml'}\n"
                f"3. {Path.cwd() / 'config.yaml'}\n\n"
                "Or specify the path using --config argument:\n"
                "  python excel_parquet_pipeline.py --config /path/to/config.yaml"
            )
            raise FileNotFoundError(error_msg)
    else:
        # Ensure config_path is absolute
        config_path_obj = Path(config_path)
        if not config_path_obj.is_absolute():
            config_path_obj = Path.cwd() / config_path_obj

        if not config_path_obj.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path_obj}")

        config_path = str(config_path_obj.absolute())
        print(f"[INFO] Using specified configuration: {config_path}")

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        if config is None:
            raise RuntimeError("Configuration file is empty or invalid")

        # Normalize paths in configuration
        def normalize_paths(obj):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    if isinstance(value, str) and ("/" in value or "\\" in value):
                        # Convert to Path object and back to string for normalization
                        try:
                            obj[key] = str(Path(value))
                        except:
                            pass  # Keep original if path conversion fails
                    else:
                        normalize_paths(value)
            elif isinstance(obj, list):
                for item in obj:
                    normalize_paths(item)

        normalize_paths(config)

        # Set default values for required sections
        if "database" not in config:
            config["database"] = {}

        db_defaults = {
            "host": "localhost",
            "port": 5432,
            "name": "pharma_warehouse",
            "user": "postgres",
            "password": "1234",
        }

        for key, default in db_defaults.items():
            if key not in config["database"]:
                config["database"][key] = default

        if "raw_layer" not in config:
            config["raw_layer"] = {}

        raw_defaults = {
            "input_dir": "./raw_data/input",
            "output_dir": "./raw_data/output",
            "state_file": "./raw_data/state/processing_state.json",
        }

        for key, default in raw_defaults.items():
            if key not in config["raw_layer"]:
                config["raw_layer"][key] = default

        if "pipeline" not in config:
            config["pipeline"] = {}

        pipeline_defaults = {
            "log_level": "INFO",
            "clean_orphaned": False,
            "show_logs": False,
            "log_limit": 10,
            "file_pattern": "*.xls",
        }

        for key, default in pipeline_defaults.items():
            if key not in config["pipeline"]:
                config["pipeline"][key] = default

        return config

    except FileNotFoundError:
        raise
    except yaml.YAMLError as e:
        raise RuntimeError(f"Failed to parse YAML configuration: {e}")
    except Exception as e:
        raise RuntimeError(f"Failed to load YAML configuration: {e}")


@dataclass
class ProcessingState:
    """Track processing state for incremental updates"""

    file_path: str
    file_hash: str
    last_modified: float
    processed_at: str
    sheets_processed: List[str]
    output_files: List[str]
    excel_row_count: int = 0  # Total rows in Excel file
    parquet_row_count: int = 0  # Total rows in all Parquet files


class ExcelParquetPipeline:
    """Pipeline for converting Excel files to Parquet format incrementally"""

    def __init__(
        self,
        input_dir: Optional[str] = None,
        output_dir: Optional[str] = None,
        state_file: Optional[str] = None,
        log_level: Optional[str] = None,
        db_config: Optional[Dict[str, str]] = None,
        config_file: Optional[str] = None,
    ):
        """
        Initialize the pipeline

        Args:
            input_dir: Directory containing Excel files (overrides config)
            output_dir: Directory to save Parquet files (overrides config)
            state_file: File to track processing state (overrides config)
            log_level: Logging level (overrides config)
            db_config: PostgreSQL database configuration (overrides config)
            config_file: Path to YAML configuration file
        """
        print("\n" + "=" * 60)
        print("Excel to Parquet Pipeline - Initialization")
        print("=" * 60)

        # Load configuration
        config = None
        config_manager_instance = None

        if USE_CONFIG_MANAGER and config_file:
            print("[INFO] Using config manager with specified config file...")
            try:
                config_manager_instance = get_config_manager(config_file)
                config = config_manager_instance.get_all()
                print("[INFO] Configuration loaded via config manager")
            except Exception as e:
                print(f"[WARN] Config manager failed: {e}")
                print("[INFO] Falling back to direct YAML loading...")
                config = load_yaml_config(config_file)
        elif USE_CONFIG_MANAGER:
            print("[INFO] Using config manager (automatic config discovery)...")
            try:
                config_manager_instance = get_config_manager()
                config = config_manager_instance.get_all()
                print("[INFO] Configuration loaded via config manager")
            except Exception as e:
                print(f"[WARN] Config manager failed: {e}")
                print("[INFO] Falling back to direct YAML loading...")
                config = load_yaml_config(config_file)
        else:
            print("[INFO] Loading configuration directly from YAML...")
            config = load_yaml_config(config_file)

        # Use provided arguments or fall back to configuration
        self.input_dir = Path(input_dir or config["raw_layer"]["input_dir"]).absolute()
        self.output_dir = Path(
            output_dir or config["raw_layer"]["output_dir"]
        ).absolute()
        self.state_file = Path(
            state_file or config["raw_layer"]["state_file"]
        ).absolute()
        log_level = log_level or config["pipeline"]["log_level"]

        print(f"\n[INFO] Configuration Summary:")
        print(f"  Input directory:  {self.input_dir}")
        print(f"  Output directory: {self.output_dir}")
        print(f"  State file:       {self.state_file}")
        print(f"  Log level:        {log_level}")

        # Database configuration
        if db_config:
            self.db_config = db_config
            print(f"  Database config:   Provided via arguments")
        else:
            db_config_from_yaml = config["database"]
            self.db_config = {
                "host": db_config_from_yaml["host"],
                "port": str(db_config_from_yaml["port"]),
                "database": db_config_from_yaml["name"],
                "user": db_config_from_yaml["user"],
                "password": db_config_from_yaml["password"],
            }
            print(
                f"  Database:          {self.db_config['user']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            )

        # Create directories if they don't exist
        print(f"\n[INFO] Creating directories...")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        print(f"  [OK] Output directory: {self.output_dir}")
        print(f"  [OK] State directory:  {self.state_file.parent}")

        # Setup logging with proper encoding for Windows
        self._setup_logging(log_level)
        self._setup_postgresql()

        # Load processing state
        self.state = self._load_state()

        # Store configuration for reference
        self.config = config
        self.config_manager = config_manager_instance
        print(f"\n[INFO] Pipeline initialization complete!")

    def _setup_logging(self, log_level: str):
        """Setup logging configuration with proper encoding for Windows"""
        # Create logs directory if it doesn't exist
        logs_dir = Path("logs")
        logs_dir.mkdir(exist_ok=True)

        # Remove all existing handlers
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        # Create formatter with ASCII-only characters
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # File handler
        file_handler = logging.FileHandler(
            logs_dir / "excel_parquet_pipeline.log", encoding="utf-8"
        )
        file_handler.setFormatter(formatter)

        # Stream handler with proper encoding for Windows console
        try:
            # Try to set UTF-8 encoding for Windows console
            import io

            stream = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
            stream_handler = logging.StreamHandler(stream)
        except:
            # Fallback to regular stream handler
            stream_handler = logging.StreamHandler()

        stream_handler.setFormatter(formatter)

        # Configure root logger
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            handlers=[file_handler, stream_handler],
        )

        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Logging initialized at level: {log_level}")

    def _setup_postgresql(self):
        """Setup PostgreSQL database for logging"""
        try:
            self.logger.info(
                f"Connecting to PostgreSQL: {self.db_config['host']}:{self.db_config['port']}"
            )
            self.conn = psycopg2.connect(**self.db_config)
            self.conn.autocommit = True

            # Create logs table if it doesn't exist
            with self.conn.cursor() as cur:
                # Create raw_layer schema
                cur.execute("CREATE SCHEMA IF NOT EXISTS raw_layer")
                self.logger.info("Created/verified raw_layer schema")

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS raw_layer.pipeline_logs (
                        log_id UUID PRIMARY KEY,
                        timestamp TIMESTAMP,
                        level VARCHAR(10),
                        logger_name VARCHAR(100),
                        message TEXT,
                        file_path VARCHAR(500),
                        sheet_name VARCHAR(200),
                        operation_type VARCHAR(50),
                        duration_ms INTEGER,
                        rows_processed INTEGER,
                        success BOOLEAN,
                        error_message TEXT
                    )
                """
                )
                self.logger.info("Created/verified pipeline_logs table")

                # Create processing_stats table
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS raw_layer.processing_stats (
                        run_id UUID PRIMARY KEY,
                        start_time TIMESTAMP,
                        end_time TIMESTAMP,
                        total_files INTEGER,
                        total_sheets INTEGER,
                        total_excel_rows INTEGER,
                        total_parquet_rows INTEGER,
                        total_duration_ms INTEGER,
                        success_count INTEGER,
                        failure_count INTEGER,
                        files_with_row_mismatch INTEGER,
                        total_row_difference INTEGER
                    )
                """
                )
                self.logger.info("Created/verified processing_stats table")

            self.current_run_id = str(uuid.uuid4())
            self.run_start_time = datetime.now()
            self.logger.info(f"Run ID: {self.current_run_id}")

        except psycopg2.OperationalError as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {e}")
            self.logger.warning("Continuing without database logging...")
            self.conn = None
        except Exception as e:
            self.logger.error(f"Error setting up PostgreSQL: {e}")
            self.conn = None

    def _log_to_postgresql(self, level: str, message: str, **kwargs):
        """Log message to PostgreSQL"""
        if self.conn is None:
            return

        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO raw_layer.pipeline_logs VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """,
                    (
                        str(uuid.uuid4()),
                        datetime.now(),
                        level,
                        self.logger.name,
                        message,
                        kwargs.get("file_path"),
                        kwargs.get("sheet_name"),
                        kwargs.get("operation_type"),
                        kwargs.get("duration_ms"),
                        kwargs.get("rows_processed"),
                        kwargs.get("success"),
                        kwargs.get("error_message"),
                    ),
                )
        except Exception as e:
            self.logger.error(f"Failed to log to PostgreSQL: {e}")

    def _count_excel_rows(self, file_path: Path) -> int:
        """
        Count total rows in Excel file across all sheets efficiently

        Args:
            file_path: Path to Excel file

        Returns:
            Total number of non-empty rows across all sheets
        """
        total_rows = 0
        try:
            excel_file = pd.ExcelFile(file_path)

            for sheet_name in excel_file.sheet_names:
                try:
                    # Read only first column to count rows (much faster)
                    df = pd.read_excel(file_path, sheet_name=sheet_name, usecols=[0])
                    # Count non-empty rows
                    sheet_rows = len(df.dropna(how="all"))
                    total_rows += sheet_rows

                    self._log_to_postgresql(
                        "DEBUG",
                        f"Counted {sheet_rows} rows in sheet '{sheet_name}'",
                        file_path=str(file_path),
                        sheet_name=sheet_name,
                        operation_type="row_counting",
                        rows_processed=sheet_rows,
                    )

                except Exception as e:
                    self.logger.warning(
                        f"Error counting rows in sheet '{sheet_name}': {e}"
                    )
                    continue

        except Exception as e:
            self.logger.error(f"Error counting rows in {file_path}: {e}")

        return total_rows

    def _count_parquet_rows(self, parquet_path: Path) -> int:
        """
        Count rows in Parquet file efficiently

        Args:
            parquet_path: Path to Parquet file

        Returns:
            Number of rows in Parquet file
        """
        try:
            # Use PyArrow to get row count without loading full file
            parquet_file = pq.ParquetFile(parquet_path)
            return parquet_file.metadata.num_rows

        except Exception as e:
            self.logger.warning(f"Error counting rows in {parquet_path}: {e}")
            return 0

    def _save_run_stats(self, stats: Dict[str, Any]):
        """Save run statistics to PostgreSQL"""
        if self.conn is None:
            return

        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO raw_layer.processing_stats VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """,
                    (
                        self.current_run_id,
                        self.run_start_time,
                        datetime.now(),
                        stats.get("total_files", 0),
                        stats.get("total_sheets", 0),
                        stats.get("total_excel_rows", 0),
                        stats.get("total_parquet_rows", 0),
                        stats.get("total_duration_ms", 0),
                        stats.get("success_count", 0),
                        stats.get("failure_count", 0),
                        stats.get("files_with_row_mismatch", 0),
                        stats.get("total_row_difference", 0),
                    ),
                )
            self.logger.info("Run statistics saved to PostgreSQL")
        except Exception as e:
            self.logger.error(f"Failed to save run stats: {e}")

    def _load_state(self) -> Dict[str, ProcessingState]:
        """Load processing state from file"""
        if not self.state_file.exists():
            self.logger.info(
                f"State file does not exist, starting fresh: {self.state_file}"
            )
            return {}

        try:
            with open(self.state_file, "r") as f:
                data = json.load(f)
                state = {
                    path: ProcessingState(**state_data)
                    for path, state_data in data.items()
                }
            self.logger.info(
                f"Loaded state from {self.state_file}: {len(state)} files tracked"
            )
            return state
        except Exception as e:
            self.logger.warning(f"Could not load state file: {e}")
            self._log_to_postgresql(
                "WARNING", "Could not load state file", error_message=str(e)
            )
            return {}

    def _save_state(self):
        """Save processing state to file"""
        try:
            state_data = {path: asdict(state) for path, state in self.state.items()}
            with open(self.state_file, "w") as f:
                json.dump(state_data, f, indent=2)
            self.logger.info(
                f"Saved state to {self.state_file}: {len(state_data)} files"
            )
        except Exception as e:
            self.logger.error(f"Could not save state file: {e}")
            self._log_to_postgresql(
                "ERROR", "Could not save state file", error_message=str(e)
            )

    def _get_file_hash(self, file_path: Path) -> str:
        """Calculate MD5 hash of file"""
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            self.logger.error(f"Error calculating hash for {file_path}: {e}")
            return ""

    def _needs_processing(self, file_path: Path) -> bool:
        """Check if file needs processing based on state"""
        file_str = str(file_path)

        if file_str not in self.state:
            self.logger.debug(f"File not in state, needs processing: {file_path}")
            return True

        state = self.state[file_str]
        current_hash = self._get_file_hash(file_path)
        current_modified = file_path.stat().st_mtime

        needs_processing = (
            current_hash != state.file_hash or current_modified != state.last_modified
        )

        if needs_processing:
            self.logger.debug(f"File changed, needs processing: {file_path}")
        else:
            self.logger.debug(f"File unchanged, skipping: {file_path}")

        return needs_processing

    def _flatten_nested_data(self, df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
        """
        Flatten nested data structures in DataFrame

        Args:
            df: DataFrame with potentially nested data
            sheet_name: Name of the sheet for context

        Returns:
            Flattened DataFrame
        """
        result_df = df.copy()

        # Handle nested dictionaries and lists in cells
        for col in result_df.columns:
            if result_df[col].dtype == "object":
                # Check if column contains nested structures
                sample_values = result_df[col].dropna().head()

                for idx, value in sample_values.items():
                    if isinstance(value, (dict, list)):
                        # Convert nested structures to JSON strings
                        result_df[col] = result_df[col].apply(
                            lambda x: (
                                json.dumps(x, ensure_ascii=False)
                                if isinstance(x, (dict, list))
                                else x
                            )
                        )
                        self.logger.info(
                            f"Flattened nested data in column '{col}' of sheet '{sheet_name}'"
                        )
                        self._log_to_postgresql(
                            "INFO",
                            f"Flattened nested data in column '{col}'",
                            file_path=sheet_name,
                            sheet_name=sheet_name,
                            operation_type="data_flattening",
                        )
                        break

        # Add metadata columns
        result_df["_raw_source_sheet"] = sheet_name
        result_df["_raw_processed_at"] = datetime.now().isoformat()

        return result_df

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and prepare DataFrame for Parquet conversion"""
        original_shape = df.shape

        # Remove completely empty rows
        df = df.dropna(how="all")

        # Remove completely empty columns
        df = df.dropna(axis=1, how="all")

        cleaned_shape = df.shape

        if original_shape != cleaned_shape:
            self.logger.debug(f"Cleaned DataFrame: {original_shape} -> {cleaned_shape}")

        # Clean column names
        df.columns = [
            str(col)
            .strip()
            .replace(" ", "_")
            .replace(".", "_")
            .replace("(", "")
            .replace(")", "")
            for col in df.columns
        ]

        # Handle duplicate column names
        cols = pd.Series(df.columns)
        for dup in cols[cols.duplicated()].unique():
            cols[cols == dup] = [f"{dup}_{i}" for i in range(sum(cols == dup))]
        df.columns = cols

        return df

    def _process_excel_file(self, file_path: Path) -> List[str]:
        """
        Process a single Excel file and convert to Parquet
        """
        output_files = []
        processed_sheets = []
        excel_row_count = 0
        parquet_row_count = 0

        try:
            self.logger.info(f"Processing Excel file: {file_path}")

            # Count rows in Excel file first
            excel_row_count = self._count_excel_rows(file_path)

            self.logger.info(f"Excel file has {excel_row_count} total rows")

            # Read all sheets from Excel file
            excel_file = pd.ExcelFile(file_path)
            self.logger.info(
                f"Found {len(excel_file.sheet_names)} sheets in Excel file"
            )

            # Get relative path from input directory to preserve structure
            try:
                relative_path = file_path.relative_to(self.input_dir)
                output_subdir = self.output_dir / relative_path.parent
            except ValueError:
                # If file is not relative to input_dir, use just the filename
                output_subdir = self.output_dir

            output_subdir.mkdir(parents=True, exist_ok=True)
            self.logger.debug(f"Output subdirectory: {output_subdir}")

            for sheet_name in excel_file.sheet_names:
                sheet_start_time = datetime.now()
                try:
                    self.logger.info(f"Processing sheet: '{sheet_name}'")

                    # Read sheet
                    df = pd.read_excel(
                        file_path, sheet_name=sheet_name, header=0, index_col=None
                    )

                    if df.empty:
                        self.logger.warning(f"Sheet '{sheet_name}' is empty, skipping")
                        continue

                    self.logger.debug(f"Sheet shape before cleaning: {df.shape}")

                    # Clean and flatten data
                    df = self._clean_dataframe(df)
                    df = self._flatten_nested_data(df, sheet_name)

                    # Generate output filename
                    base_name = file_path.stem
                    safe_sheet_name = (
                        "".join(
                            c for c in sheet_name if c.isalnum() or c in (" ", "-", "_")
                        )
                        .strip()
                        .replace(" ", "_")
                    )

                    output_filename = f"{base_name}_{safe_sheet_name}.parquet"
                    output_path = output_subdir / output_filename

                    self.logger.info(f"Converting to Parquet: {output_path}")

                    # Convert to Parquet
                    table = pa.Table.from_pandas(df)
                    pq.write_table(table, output_path, compression="snappy")

                    # Count rows in the generated Parquet file
                    sheet_parquet_rows = self._count_parquet_rows(output_path)
                    parquet_row_count += sheet_parquet_rows

                    output_files.append(str(output_path))
                    processed_sheets.append(sheet_name)

                    duration_ms = (
                        datetime.now() - sheet_start_time
                    ).total_seconds() * 1000

                    # Use ASCII-only message for logging
                    self.logger.info(
                        f"Converted sheet '{sheet_name}' to {output_path} "
                        f"({sheet_parquet_rows} rows, {duration_ms:.0f}ms)"
                    )
                    self._log_to_postgresql(
                        "INFO",
                        f"Converted sheet to Parquet",
                        file_path=str(file_path),
                        sheet_name=sheet_name,
                        operation_type="sheet_conversion",
                        duration_ms=int(duration_ms),
                        rows_processed=sheet_parquet_rows,
                        success=True,
                    )

                except Exception as e:
                    duration_ms = (
                        datetime.now() - sheet_start_time
                    ).total_seconds() * 1000
                    self.logger.error(
                        f"Error processing sheet '{sheet_name}' in {file_path}: {e}"
                    )
                    self._log_to_postgresql(
                        "ERROR",
                        f"Error processing sheet",
                        file_path=str(file_path),
                        sheet_name=sheet_name,
                        operation_type="sheet_conversion",
                        duration_ms=int(duration_ms),
                        success=False,
                        error_message=str(e),
                    )
                    continue

            # Log row count comparison
            row_difference = excel_row_count - parquet_row_count
            if row_difference != 0:
                self.logger.warning(
                    f"Row count mismatch: Excel={excel_row_count}, "
                    f"Parquet={parquet_row_count}, Diff={row_difference}"
                )
                self._log_to_postgresql(
                    "WARNING",
                    f"Row count difference detected",
                    file_path=str(file_path),
                    operation_type="row_count_validation",
                    rows_processed=excel_row_count,
                    success=True,
                    error_message=f"Excel: {excel_row_count}, Parquet: {parquet_row_count}, Difference: {row_difference}",
                )
            else:
                self.logger.info(f"Row counts match: {excel_row_count} rows")

            # Update processing state with row counts
            file_hash = self._get_file_hash(file_path)
            file_modified = file_path.stat().st_mtime

            self.state[str(file_path)] = ProcessingState(
                file_path=str(file_path),
                file_hash=file_hash,
                last_modified=file_modified,
                processed_at=datetime.now().isoformat(),
                sheets_processed=processed_sheets,
                output_files=output_files,
                excel_row_count=excel_row_count,
                parquet_row_count=parquet_row_count,
            )

            self._log_to_postgresql(
                "INFO",
                f"Successfully processed Excel file",
                file_path=str(file_path),
                operation_type="file_processing_complete",
                rows_processed=excel_row_count,
                success=True,
            )

            self.logger.info(f"Successfully processed: {file_path}")

        except Exception as e:
            self.logger.error(f"Error processing Excel file {file_path}: {e}")
            self._log_to_postgresql(
                "ERROR",
                f"Error processing Excel file",
                file_path=str(file_path),
                operation_type="file_processing",
                success=False,
                error_message=str(e),
            )

        return output_files

    def process_directory(self, file_pattern: str = None) -> Dict[str, List[str]]:
        """
        Process all Excel files and track row count statistics

        Args:
            file_pattern: File pattern to match (overrides config if provided)

        Returns:
            Dictionary mapping input files to output files
        """
        # Use provided pattern or configuration
        if file_pattern is None:
            file_pattern = self.config["pipeline"]["file_pattern"]

        results = {}
        success_count = 0
        failure_count = 0
        total_excel_rows = 0
        total_parquet_rows = 0
        total_files_with_mismatch = 0

        self.logger.info(f"Processing directory: {self.input_dir}")
        self.logger.info(f"File pattern: {file_pattern}")
        print(f"[INFO] Processing directory: {self.input_dir}")

        # Find Excel files
        excel_files = []
        excel_files.extend(list(self.input_dir.rglob("*.xls")))
        excel_files.extend(list(self.input_dir.rglob("*.xlsx")))
        excel_files.extend(list(self.input_dir.rglob("*.xlsm")))
        excel_files.extend(list(self.input_dir.rglob("*.xlsb")))

        if not excel_files:
            self.logger.warning(f"No Excel files found in {self.input_dir}")
            print(f"[WARN] No Excel files found in: {self.input_dir}")
            return results

        self.logger.info(f"Found {len(excel_files)} Excel files to process")
        print(f"[INFO] Found {len(excel_files)} Excel files to process")

        for file_path in excel_files:
            file_start_time = datetime.now()
            try:
                if self._needs_processing(file_path):
                    print(f"[PROC] Processing: {file_path.name}")
                    output_files = self._process_excel_file(file_path)
                    results[str(file_path)] = output_files

                    # Update totals from state
                    if str(file_path) in self.state:
                        state = self.state[str(file_path)]
                        total_excel_rows += state.excel_row_count
                        total_parquet_rows += state.parquet_row_count

                        # Check for mismatches
                        if state.excel_row_count != state.parquet_row_count:
                            total_files_with_mismatch += 1

                    success_count += 1
                    print(f"[OK] Processed: {file_path.name}")
                else:
                    # For already processed files, use existing state
                    results[str(file_path)] = self.state[str(file_path)].output_files
                    state = self.state[str(file_path)]
                    total_excel_rows += state.excel_row_count
                    total_parquet_rows += state.parquet_row_count

                    if state.excel_row_count != state.parquet_row_count:
                        total_files_with_mismatch += 1

                    success_count += 1
                    print(f"[SKIP] Already processed: {file_path.name}")

                self.logger.debug(f"Completed {file_path}")

            except Exception as e:
                self.logger.error(f"Error processing file {file_path}: {e}")
                print(f"[ERROR] Processing {file_path.name}: {e}")
                failure_count += 1

        # Save state and statistics
        self._save_state()

        # Log overall row count comparison
        total_row_difference = total_excel_rows - total_parquet_rows
        if total_row_difference != 0:
            self.logger.warning(
                f"Overall row count mismatch: Excel={total_excel_rows}, "
                f"Parquet={total_parquet_rows}, Diff={total_row_difference}"
            )
            self._log_to_postgresql(
                "WARNING",
                f"Overall row count difference",
                operation_type="summary_validation",
                rows_processed=total_excel_rows,
                success=True,
                error_message=f"Total Excel: {total_excel_rows}, Total Parquet: {total_parquet_rows}, "
                f"Difference: {total_row_difference}, Files with mismatch: {total_files_with_mismatch}",
            )
        else:
            self.logger.info(f"All row counts match: {total_excel_rows} rows")

        # Save run statistics
        run_stats = {
            "total_files": len(excel_files),
            "total_sheets": sum(
                len(state.sheets_processed) for state in self.state.values()
            ),
            "total_excel_rows": total_excel_rows,
            "total_parquet_rows": total_parquet_rows,
            "total_duration_ms": int(
                (datetime.now() - self.run_start_time).total_seconds() * 1000
            ),
            "success_count": success_count,
            "failure_count": failure_count,
            "files_with_row_mismatch": total_files_with_mismatch,
            "total_row_difference": total_row_difference,
        }
        self._save_run_stats(run_stats)

        self.logger.info(
            f"Processing complete: {success_count} successful, {failure_count} failed"
        )
        return results

    def get_processing_summary(self) -> Dict[str, Any]:
        """Get summary of processing state"""
        total_files = len(self.state)
        total_sheets = sum(len(state.sheets_processed) for state in self.state.values())
        total_outputs = sum(len(state.output_files) for state in self.state.values())

        return {
            "total_excel_files": total_files,
            "total_sheets_processed": total_sheets,
            "total_parquet_files": total_outputs,
            "last_run": datetime.now().isoformat(),
        }

    def clean_orphaned_outputs(self):
        """Remove Parquet files for Excel files that no longer exist"""
        orphaned_files = []
        self.logger.info("Cleaning orphaned output files...")
        print("[CLEAN] Cleaning orphaned output files...")

        for file_path_str, state in list(self.state.items()):
            if not Path(file_path_str).exists():
                self.logger.info(f"Source file no longer exists: {file_path_str}")
                # Excel file no longer exists, remove corresponding Parquet files
                for output_file in state.output_files:
                    output_path = Path(output_file)
                    if output_path.exists():
                        output_path.unlink()
                        orphaned_files.append(str(output_path))
                        self.logger.info(f"Removed orphaned output file: {output_path}")
                        print(f"  [DEL] Removed: {output_path.name}")
                        self._log_to_postgresql(
                            "INFO",
                            f"Removed orphaned output file",
                            file_path=output_file,
                            operation_type="cleanup",
                        )

                # Remove from state
                del self.state[file_path_str]

        if orphaned_files:
            self.logger.info(f"Cleaned up {len(orphaned_files)} orphaned output files")
            self._log_to_postgresql(
                "INFO",
                f"Cleaned up {len(orphaned_files)} orphaned output files",
                operation_type="cleanup",
                rows_processed=len(orphaned_files),
            )
            self._save_state()
            print(f"[OK] Cleaned up {len(orphaned_files)} orphaned files")
        else:
            self.logger.info("No orphaned files found")
            print("[OK] No orphaned files found")

        return orphaned_files

    def get_logs(self, limit: int = 100, level: str = None):
        """Retrieve logs from PostgreSQL"""
        if self.conn is None:
            return pd.DataFrame()

        query = "SELECT * FROM raw_layer.pipeline_logs"
        if level:
            query += f" WHERE level = '{level}'"
        query += f" ORDER BY timestamp DESC LIMIT {limit}"

        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query)
                return pd.DataFrame(cur.fetchall())
        except Exception as e:
            self.logger.error(f"Failed to retrieve logs: {e}")
            return pd.DataFrame()

    def close(self):
        """Close database connection"""
        if hasattr(self, "conn") and self.conn:
            self.conn.close()
            self.logger.info("Database connection closed")


def main():
    """Main function to run the pipeline with YAML configuration"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Excel to Parquet Pipeline (YAML Config)"
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Path to YAML configuration file (default: looks in common locations)",
    )
    parser.add_argument("--input-dir", help="Override input directory from config")
    parser.add_argument("--output-dir", help="Override output directory from config")
    parser.add_argument("--state-file", help="Override state file from config")
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Override log level from config",
    )
    parser.add_argument(
        "--clean-orphaned", action="store_true", help="Clean up orphaned output files"
    )
    parser.add_argument("--pattern", help="Override file pattern from config")
    parser.add_argument("--db-host", help="Override database host from config")
    parser.add_argument("--db-port", help="Override database port from config")
    parser.add_argument("--db-name", help="Override database name from config")
    parser.add_argument("--db-user", help="Override database user from config")
    parser.add_argument("--db-password", help="Override database password from config")
    parser.add_argument(
        "--show-logs", action="store_true", help="Show recent logs after processing"
    )
    parser.add_argument(
        "--log-limit", type=int, default=None, help="Number of log entries to show"
    )

    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("EXCEL TO PARQUET PIPELINE")
    print("=" * 60)

    # Build database configuration (command line args have priority)
    db_config = None

    if any([args.db_host, args.db_port, args.db_name, args.db_user, args.db_password]):
        # Use command line arguments if any are provided
        db_config = {
            "host": args.db_host or "localhost",
            "port": args.db_port or "5432",
            "database": args.db_name or "pharma_warehouse",
            "user": args.db_user or "postgres",
            "password": args.db_password or "1234",
        }
        print(f"[INFO] Using database configuration from command line")

    # Initialize pipeline
    try:
        pipeline = ExcelParquetPipeline(
            input_dir=args.input_dir,
            output_dir=args.output_dir,
            state_file=args.state_file,
            log_level=args.log_level,
            db_config=db_config,
            config_file=args.config,
        )
    except FileNotFoundError as e:
        print(f"\n[ERROR] {e}")
        return 1
    except Exception as e:
        print(f"\n[ERROR] Failed to initialize pipeline: {e}")
        print("\n[TIPS] Troubleshooting:")
        print("1. Check that config.yaml exists in the scripts directory")
        print("2. Verify the configuration file format is correct")
        print("3. Ensure all required directories exist")
        print(f"4. Current working directory: {Path.cwd()}")
        print(f"5. Script location: {Path(__file__).parent}")
        return 1

    # Clean orphaned files if requested
    if args.clean_orphaned:
        print("\n" + "-" * 40)
        pipeline.clean_orphaned_outputs()

    # Process files
    print("\n" + "-" * 40)
    print("PROCESSING FILES")
    print("-" * 40)

    pattern = args.pattern
    results = pipeline.process_directory(pattern)

    # Print summary
    summary = pipeline.get_processing_summary()
    print("\n" + "=" * 60)
    print("PROCESSING SUMMARY")
    print("=" * 60)
    print(f"[INFO] Total Excel files processed: {summary['total_excel_files']}")
    print(f"[INFO] Total sheets processed: {summary['total_sheets_processed']}")
    print(f"[INFO] Total Parquet files created: {summary['total_parquet_files']}")
    print(f"[INFO] Last run: {summary['last_run']}")

    if results:
        print(f"\n[INFO] Processed files:")
        for excel_file, parquet_files in results.items():
            excel_name = Path(excel_file).name
            print(f"  [FILE] {excel_name} -> {len(parquet_files)} Parquet files")
    else:
        print("\n[INFO] No files processed.")

    # Show logs if requested
    show_logs = args.show_logs
    if not show_logs and pipeline.config["pipeline"].get("show_logs", False):
        show_logs = True

    log_limit = args.log_limit or pipeline.config["pipeline"].get("log_limit", 10)

    if show_logs:
        print(f"\n[INFO] Recent Logs (last {log_limit}):")
        logs = pipeline.get_logs(limit=log_limit)
        if not logs.empty:
            for _, log in logs.iterrows():
                timestamp = (
                    log["timestamp"].strftime("%H:%M:%S")
                    if hasattr(log["timestamp"], "strftime")
                    else str(log["timestamp"])
                )
                print(f"  [{timestamp}] [{log['level']:7}] {log['message']}")
        else:
            print("  [INFO] No logs found.")

    pipeline.close()
    print("\n" + "=" * 60)
    print("[SUCCESS] PIPELINE COMPLETED!")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    # Set UTF-8 encoding for Windows console
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
            sys.stderr.reconfigure(encoding="utf-8")
        except:
            pass  # Python 3.6 doesn't have reconfigure

    exit(main())
