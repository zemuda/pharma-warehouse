#!/usr/bin/env python
"""
COMPLETE BRONZE LAYER - FIXED VERSION
Compatible with deltalake package
"""

import yaml
import sys
import logging
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Tuple, Union
import hashlib
import json
import pickle
import warnings
from dataclasses import dataclass, asdict, field
from enum import Enum
from collections import defaultdict
import uuid

import pandas as pd
import numpy as np
from deltalake import DeltaTable, write_deltalake
import pyarrow as pa

# Suppress warnings
warnings.filterwarnings("ignore")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("bronze_complete_fixed.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


class LoadStrategy(Enum):
    FULL = "full"
    INCREMENTAL = "incremental"
    MERGE = "merge"


class SCD2Type(Enum):
    TYPE1 = "type1"
    TYPE2 = "type2"
    TYPE3 = "type3"


@dataclass
class Checkpoint:
    """Checkpoint for incremental loading"""

    table_name: str
    last_processed_file: str
    last_processed_time: datetime
    last_watermark_value: Any
    last_watermark_column: Optional[str]
    records_processed: int
    load_strategy: str
    metadata: Dict = field(default_factory=dict)

    def to_dict(self):
        data = asdict(self)
        if isinstance(data["last_processed_time"], datetime):
            data["last_processed_time"] = data["last_processed_time"].isoformat()
        if isinstance(data["last_watermark_value"], (datetime, date)):
            data["last_watermark_value"] = data["last_watermark_value"].isoformat()
        return data

    @classmethod
    def from_dict(cls, data: Dict):
        if isinstance(data.get("last_processed_time"), str):
            data["last_processed_time"] = datetime.fromisoformat(
                data["last_processed_time"]
            )
        if isinstance(data.get("last_watermark_value"), str):
            try:
                data["last_watermark_value"] = datetime.fromisoformat(
                    data["last_watermark_value"]
                )
            except:
                pass
        return cls(**data)


@dataclass
class LoadMetrics:
    """Metrics for load operations"""

    start_time: datetime
    end_time: Optional[datetime] = None
    rows_processed: int = 0
    files_processed: int = 0
    insert_count: int = 0
    update_count: int = 0
    delete_count: int = 0
    expire_count: int = 0
    errors: List[str] = field(default_factory=list)

    @property
    def duration(self) -> float:
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0

    def to_dict(self):
        return {
            "duration_seconds": round(self.duration, 2),
            "rows_processed": self.rows_processed,
            "files_processed": self.files_processed,
            "insert_count": self.insert_count,
            "update_count": self.update_count,
            "delete_count": self.delete_count,
            "expire_count": self.expire_count,
            "errors": self.errors,
        }


class CompleteBronzeLayer:
    """
    Complete Bronze Layer - Fixed for deltalake compatibility
    """

    def __init__(self, config_path: str = "bronze_config_complete.yaml"):
        self.config_path = config_path
        self.load_id = str(uuid.uuid4())[:8]
        self.load_config()
        self.setup_paths()
        self.setup_checkpoints()
        self.setup_delta_features()

        logger.info("=" * 80)
        logger.info("🏥 COMPLETE BRONZE LAYER - FIXED VERSION")
        logger.info(f"📊 Load ID: {self.load_id}")
        logger.info("=" * 80)

        self.log_feature_status()

    def load_config(self):
        """Load configuration from YAML"""
        with open(self.config_path, "r") as f:
            self.config = yaml.safe_load(f)

        self.delta_config = self.config.get("delta_features", {})
        self.data_sources = self.config.get("data_sources", {})
        self.paths = self.config.get("paths", {})
        self.incremental_config = self.config.get("incremental", {})
        self.scd2_config = self.config.get("scd2", {})

    def setup_paths(self):
        """Setup all directories"""
        base_path = Path(self.paths.get("bronze_base_path", "./bronze_data"))
        source_path = Path(self.paths.get("source_data_path", "./source_data"))
        log_path = Path(self.paths.get("log_directory", "./logs"))
        checkpoint_path = Path(self.paths.get("checkpoint_directory", "./checkpoints"))
        temp_path = Path(self.paths.get("temp_directory", "./temp"))

        for path in [base_path, source_path, log_path, checkpoint_path, temp_path]:
            path.mkdir(parents=True, exist_ok=True)

        self.bronze_base_path = base_path
        self.source_data_path = source_path
        self.log_dir = log_path
        self.checkpoint_dir = checkpoint_path
        self.temp_dir = temp_path
        self.tables_path = self.bronze_base_path / "tables"
        self.tables_path.mkdir(exist_ok=True)

    def setup_checkpoints(self):
        """Setup checkpoint system"""
        self.checkpoint_dir.mkdir(exist_ok=True)
        self.checkpoints: Dict[str, Checkpoint] = {}
        self.load_checkpoints()

    def setup_delta_features(self):
        """Initialize Delta feature configurations"""
        self.enabled_features = {}
        for feature, config in self.delta_config.items():
            if isinstance(config, dict):
                self.enabled_features[feature] = config.get("enabled", False)
            else:
                self.enabled_features[feature] = bool(config)

    def log_feature_status(self):
        """Log which Delta features are enabled"""
        enabled = [f for f, enabled in self.enabled_features.items() if enabled]
        disabled = [f for f, enabled in self.enabled_features.items() if not enabled]

        logger.info("⚙️  DELTA FEATURES STATUS:")
        logger.info(f"   ✅ ENABLED ({len(enabled)}): {', '.join(enabled)}")
        logger.info(f"   ⚠️  DISABLED ({len(disabled)}): {', '.join(disabled)}")

    def is_feature_enabled(self, feature_name: str) -> bool:
        """Check if a Delta feature is enabled"""
        return self.enabled_features.get(feature_name, False)

    def load_checkpoints(self):
        """Load existing checkpoints"""
        checkpoint_files = list(self.checkpoint_dir.glob("*.json"))
        for file in checkpoint_files:
            try:
                with open(file, "r") as f:
                    checkpoint_data = json.load(f)
                    checkpoint = Checkpoint.from_dict(checkpoint_data)
                    self.checkpoints[checkpoint.table_name] = checkpoint
            except Exception as e:
                logger.warning(f"⚠️ Failed to load checkpoint {file}: {e}")

    def save_checkpoint(self, checkpoint: Checkpoint):
        """Save checkpoint as JSON"""
        checkpoint_file = self.checkpoint_dir / f"{checkpoint.table_name}.json"
        with open(checkpoint_file, "w") as f:
            json.dump(checkpoint.to_dict(), f, indent=2)
        self.checkpoints[checkpoint.table_name] = checkpoint

    def discover_files_incremental(
        self, relative_path: str, table_name: str
    ) -> Tuple[List[Path], Optional[Checkpoint]]:
        """
        Discover files for incremental loading - FIXED to handle multiple file types
        """
        search_path = self.source_data_path / relative_path

        if not search_path.exists():
            logger.warning(f"⚠️ Source path does not exist: {search_path}")
            return [], None

        # Look for both Parquet and CSV files (since your raw data might be in CSV)
        all_files = []
        for ext in ["*.parquet", "*.csv"]:
            all_files.extend(sorted(search_path.rglob(ext)))

        # Get or create checkpoint
        checkpoint = self.checkpoints.get(table_name)

        if not self.is_feature_enabled("incremental_loading"):
            logger.info(f"📁 Full load: Processing all {len(all_files)} files")
            return all_files, checkpoint

        if checkpoint is None:
            # First run - process all files
            logger.info(f"📁 First run: Processing all {len(all_files)} files")
            return all_files, checkpoint

        # Discover by modification time
        new_files = []
        for file_path in all_files:
            file_mod_time = datetime.fromtimestamp(file_path.stat().st_mtime)
            if (
                file_mod_time > checkpoint.last_processed_time
                or str(file_path) > checkpoint.last_processed_file
            ):
                new_files.append(file_path)

        logger.info(
            f"📁 Incremental: {len(new_files)} new files since {checkpoint.last_processed_time}"
        )
        return new_files, checkpoint

    def read_files_with_features(
        self, files: List[Path], data_type_config: Dict
    ) -> Optional[pd.DataFrame]:
        """
        Read files with features - FIXED to handle CSV files
        """
        if not files:
            return None

        dataframes = []
        metrics = LoadMetrics(start_time=datetime.now())

        for file_path in files:
            try:
                # Read file based on extension
                if file_path.suffix.lower() == ".parquet":
                    df = pd.read_parquet(file_path)
                elif file_path.suffix.lower() == ".csv":
                    df = pd.read_csv(file_path, low_memory=False)

                    # Try to parse date columns
                    date_columns = [
                        col
                        for col in df.columns
                        if "DATE" in col.upper() or "TIME" in col.upper()
                    ]
                    for col in date_columns:
                        try:
                            df[col] = pd.to_datetime(df[col], errors="coerce")
                        except:
                            pass
                else:
                    logger.warning(f"   ⚠️ Unsupported file type: {file_path.suffix}")
                    continue

                if df is None or len(df) == 0:
                    continue

                # Apply watermark filtering if enabled
                if self.is_feature_enabled("incremental_loading"):
                    df = self._apply_watermark_filter(df, data_type_config)

                # Add metadata columns
                df = self._add_metadata_columns(df, file_path, data_type_config)

                dataframes.append(df)
                metrics.files_processed += 1
                metrics.rows_processed += len(df)

                logger.debug(f"   ✅ Read {file_path.name}: {len(df):,} rows")

            except Exception as e:
                error_msg = f"Failed to read {file_path.name}: {str(e)}"
                logger.warning(f"   ⚠️ {error_msg}")
                metrics.errors.append(error_msg)
                continue

        if not dataframes:
            return None

        # Combine dataframes
        if len(dataframes) == 1:
            combined_df = dataframes[0]
        else:
            combined_df = pd.concat(dataframes, ignore_index=True)

        metrics.end_time = datetime.now()
        logger.info(
            f"📊 Read {metrics.rows_processed:,} rows from {metrics.files_processed} files "
            f"({metrics.duration:.1f}s)"
        )

        return combined_df

    def _apply_watermark_filter(self, df: pd.DataFrame, config: Dict) -> pd.DataFrame:
        """Apply watermark filtering for incremental loading"""
        watermark_column = config.get("watermark_column")
        checkpoint = self.checkpoints.get(config["table_name"])

        if (
            not watermark_column
            or not checkpoint
            or checkpoint.last_watermark_value is None
        ):
            return df

        if watermark_column in df.columns:
            try:
                # Ensure the column is datetime
                if not pd.api.types.is_datetime64_any_dtype(df[watermark_column]):
                    df[watermark_column] = pd.to_datetime(
                        df[watermark_column], errors="coerce"
                    )

                # Filter rows
                filtered = df[
                    df[watermark_column] > pd.Timestamp(checkpoint.last_watermark_value)
                ]
                logger.debug(
                    f"   🔍 Watermark filter: {len(filtered):,}/{len(df):,} rows after {checkpoint.last_watermark_value}"
                )
                return filtered
            except Exception as e:
                logger.warning(f"   ⚠️ Watermark filtering failed: {e}")

        return df

    def _add_metadata_columns(
        self, df: pd.DataFrame, file_path: Path, config: Dict
    ) -> pd.DataFrame:
        """Add bronze layer metadata columns"""
        df_meta = df.copy()
        current_time = datetime.now()

        # Source metadata
        df_meta["_bronze_source_file_path"] = str(file_path)
        df_meta["_bronze_source_file_name"] = file_path.name
        df_meta["_bronze_source_file_mtime"] = datetime.fromtimestamp(
            file_path.stat().st_mtime
        )

        # Load metadata
        df_meta["_bronze_load_id"] = self.load_id
        df_meta["_bronze_load_timestamp"] = current_time
        df_meta["_bronze_ingestion_date"] = current_time.date()
        df_meta["_bronze_data_granularity"] = config.get("data_type", "unknown")

        # Ensure partition columns exist
        partition_cols = config.get("partition_columns", [])
        for col in partition_cols:
            if col not in df_meta.columns:
                if col == "_bronze_ingestion_date":
                    df_meta[col] = current_time.date()
                elif col == "TRN_YEAR" and "TRN_DATE" in df_meta.columns:
                    df_meta[col] = pd.to_datetime(df_meta["TRN_DATE"]).dt.year
                elif col == "TRN_MONTH" and "TRN_DATE" in df_meta.columns:
                    df_meta[col] = pd.to_datetime(df_meta["TRN_DATE"]).dt.month

        return df_meta

    def implement_scd2(
        self, new_df: pd.DataFrame, table_name: str, config: Dict
    ) -> LoadMetrics:
        """
        Implement SCD2 - FIXED partitioning issue
        """
        metrics = LoadMetrics(start_time=datetime.now())

        if not self.is_feature_enabled("scd2"):
            # Simple append
            return self._simple_append(new_df, table_name, metrics, config)

        scd2_type = config.get("scd2_type", "type2")
        primary_keys = config.get("primary_keys", [])

        if not primary_keys:
            logger.warning(
                f"⚠️ No primary keys for SCD2 on {table_name}, using simple append"
            )
            return self._simple_append(new_df, table_name, metrics, config)

        try:
            if scd2_type == "type1":
                return self._implement_scd1(
                    new_df, table_name, primary_keys, metrics, config
                )
            elif scd2_type == "type2":
                return self._implement_scd2(
                    new_df, table_name, primary_keys, config, metrics
                )
            else:
                return self._simple_append(new_df, table_name, metrics, config)

        except Exception as e:
            logger.error(f"❌ SCD2 implementation failed: {e}")
            metrics.errors.append(f"SCD2 failed: {str(e)}")
            return self._simple_append(new_df, table_name, metrics, config)

    def _implement_scd2(
        self,
        new_df: pd.DataFrame,
        table_name: str,
        primary_keys: List[str],
        config: Dict,
        metrics: LoadMetrics,
    ) -> LoadMetrics:
        """
        SCD2 Type 2 implementation - FIXED partitioning issue
        """
        delta_path = self.tables_path / table_name

        # Prepare new data with SCD2 metadata
        new_scd2 = self._prepare_scd2_data(new_df, primary_keys, "type2", config)

        # Calculate business hash for change detection
        business_cols = self._get_business_columns(new_df, config)
        new_scd2["_bronze_data_hash"] = self._calculate_business_hash(
            new_df, business_cols
        )

        if not self.table_exists(table_name):
            # First load - create table with partitioning
            self._write_delta_with_features(new_scd2, table_name, "overwrite", config)
            metrics.insert_count = len(new_scd2)
            metrics.rows_processed = len(new_scd2)
            metrics.end_time = datetime.now()
            return metrics

        # For existing table, we need to check current partitioning
        try:
            dt = DeltaTable(str(delta_path))

            # Get existing table's partition columns
            existing_partitions = dt.metadata().partition_columns

            # If partitions don't match, we need to handle this
            config_partitions = config.get("partition_columns", [])
            if set(existing_partitions) != set(config_partitions):
                logger.warning(
                    f"⚠️ Partition mismatch for {table_name}: "
                    f"Existing: {existing_partitions}, Config: {config_partitions}"
                )
                # For now, disable partitioning for this write
                config_copy = config.copy()
                config_copy["partition_columns"] = (
                    existing_partitions if existing_partitions else []
                )
                return self._simple_append(new_scd2, table_name, metrics, config_copy)

            # Use Delta MERGE for SCD2 (if partition columns match)
            # Build merge condition
            merge_condition = " AND ".join(
                [f"target.{pk} = source.{pk}" for pk in primary_keys]
            )

            # Execute SCD2 MERGE
            (
                dt.merge(
                    source=pa.Table.from_pandas(new_scd2),
                    predicate=merge_condition,
                    source_alias="source",
                    target_alias="target",
                )
                .when_matched_update(
                    condition="""
                    source._bronze_data_hash != target._bronze_data_hash 
                    AND target._bronze_is_current = true
                """,
                    set={
                        "_bronze_valid_to": "source._bronze_valid_from",
                        "_bronze_is_current": "false",
                        "_bronze_record_version": "target._bronze_record_version + 1",
                    },
                )
                .when_not_matched_insert_all()
                .execute()
            )

            # Get simplified metrics
            existing_df = dt.to_pandas()
            existing_current = existing_df[existing_df["_bronze_is_current"] == True]

            new_keys = set(tuple(row) for row in new_scd2[primary_keys].values)
            existing_keys = set(
                tuple(row) for row in existing_current[primary_keys].values
            )

            metrics.insert_count = len(new_keys - existing_keys)
            metrics.update_count = len(new_keys.intersection(existing_keys))
            metrics.expire_count = metrics.update_count
            metrics.rows_processed = len(new_scd2)

            logger.info(
                f"🔀 SCD2 Type 2: {table_name} - "
                f"New: {metrics.insert_count:,}, "
                f"Changed: {metrics.update_count:,}"
            )

        except Exception as e:
            logger.error(f"❌ Delta MERGE failed: {e}")
            metrics.errors.append(f"Merge failed: {str(e)}")
            # Fallback to simple append
            return self._simple_append(new_scd2, table_name, metrics, config)

        metrics.end_time = datetime.now()
        return metrics

    def _prepare_scd2_data(
        self, df: pd.DataFrame, primary_keys: List[str], scd_type: str, config: Dict
    ) -> pd.DataFrame:
        """Prepare data with SCD2 metadata columns"""
        df_scd = df.copy()
        current_time = datetime.now()

        # Common metadata
        df_scd["_bronze_valid_from"] = current_time
        df_scd["_bronze_is_current"] = True
        df_scd["_bronze_load_timestamp"] = current_time

        if scd_type == "type2":
            df_scd["_bronze_valid_to"] = pd.NaT
            df_scd["_bronze_record_version"] = 1

        return df_scd

    def _calculate_business_hash(
        self, df: pd.DataFrame, business_cols: List[str]
    ) -> pd.Series:
        """Calculate hash of business columns"""
        if not business_cols:
            return pd.Series([""] * len(df))

        def row_hash(row):
            try:
                values = []
                for col in sorted(business_cols):
                    val = row[col]
                    if pd.isna(val):
                        values.append("NULL")
                    elif isinstance(val, float):
                        values.append(f"{val:.10f}")
                    elif isinstance(val, datetime):
                        values.append(val.isoformat())
                    else:
                        values.append(str(val).strip())

                hash_str = "|".join(values)
                return hashlib.sha256(hash_str.encode()).hexdigest()
            except Exception:
                return ""

        return df.apply(row_hash, axis=1)

    def _get_business_columns(self, df: pd.DataFrame, config: Dict) -> List[str]:
        """Get business columns for hash calculation"""
        exclude_prefixes = ["_bronze_", "_source_", "_raw_"]
        business_cols = [
            col
            for col in df.columns
            if not any(col.startswith(prefix) for prefix in exclude_prefixes)
        ]

        configured_cols = config.get("hash_columns", [])
        if configured_cols:
            business_cols = [col for col in configured_cols if col in df.columns]

        return business_cols

    def _simple_append(
        self, df: pd.DataFrame, table_name: str, metrics: LoadMetrics, config: Dict
    ) -> LoadMetrics:
        """Simple append without SCD2"""
        mode = "append" if self.table_exists(table_name) else "overwrite"
        self._write_delta_with_features(df, table_name, mode, config)

        metrics.insert_count = len(df)
        metrics.rows_processed = len(df)
        metrics.end_time = datetime.now()

        return metrics

    def _write_delta_with_features(
        self, df: pd.DataFrame, table_name: str, mode: str, config: Dict
    ):
        """
        Write to Delta with compatible features - FIXED partitioning issue
        """
        delta_path = self.tables_path / table_name
        delta_path.mkdir(parents=True, exist_ok=True)

        # Prepare write options
        write_options = {
            "mode": mode,
        }

        # Schema Evolution
        if self.is_feature_enabled("schema_evolution"):
            write_options["schema_mode"] = "merge"

        # Partitioning
        if self.is_feature_enabled("partitioning"):
            partition_cols = config.get("partition_columns", [])
            if not partition_cols:
                partition_cols = self.delta_config.get("partitioning", {}).get(
                    "default_columns", []
                )

            # Check if table exists and has different partitions
            if self.table_exists(table_name) and partition_cols:
                try:
                    dt = DeltaTable(str(delta_path))
                    existing_partitions = dt.metadata().partition_columns
                    if set(existing_partitions) != set(partition_cols):
                        logger.warning(
                            f"⚠️ Partition mismatch for {table_name}: "
                            f"Existing: {existing_partitions}, New: {partition_cols}. "
                            f"Skipping partitioning for this write."
                        )
                        partition_cols = []  # Skip partitioning to avoid error
                except Exception as e:
                    logger.warning(f"⚠️ Could not check existing partitions: {e}")

            if partition_cols:
                # Ensure all partition columns exist in the dataframe
                missing_cols = [col for col in partition_cols if col not in df.columns]
                if missing_cols:
                    logger.warning(
                        f"⚠️ Missing partition columns {missing_cols} in data for {table_name}. "
                        f"Using existing partitions only."
                    )
                    partition_cols = [
                        col for col in partition_cols if col in df.columns
                    ]

                if partition_cols:
                    write_options["partition_by"] = partition_cols
                    logger.debug(f"📁 Partitioning by: {partition_cols}")

        # Write to Delta (FIXED: removed 'engine' parameter)
        try:
            write_deltalake(str(delta_path), df, **write_options)
            logger.info(f"💾 Wrote {len(df):,} rows to {table_name}")

            # Apply post-write features
            self._apply_post_write_features(table_name, config)

        except Exception as e:
            logger.error(f"❌ Failed to write Delta table: {e}")
            # Try without partitioning as fallback
            if "partition_by" in write_options:
                logger.info("🔄 Retrying without partitioning...")
                write_options.pop("partition_by")
                try:
                    write_deltalake(str(delta_path), df, **write_options)
                    logger.info(
                        f"💾 Wrote {len(df):,} rows to {table_name} (no partitioning)"
                    )
                except Exception as e2:
                    logger.error(f"❌ Failed retry: {e2}")
                    raise
            else:
                raise

    def _apply_post_write_features(self, table_name: str, config: Dict):
        """Apply post-write Delta features - FIXED DeltaTable method issue"""
        delta_path = self.tables_path / table_name

        # Check if table exists using try-except (for older deltalake versions)
        try:
            # Try the new method first
            if hasattr(DeltaTable, "is_delta_table"):
                table_exists = DeltaTable.is_delta_table(str(delta_path))
            else:
                # Fallback for older versions - try to load the table
                try:
                    dt = DeltaTable(str(delta_path))
                    table_exists = True
                except:
                    table_exists = False

            if not table_exists:
                return

            dt = DeltaTable(str(delta_path))

            # Set table properties
            self._set_table_properties(dt, table_name, config)

            # Optimize table
            if self.should_optimize(table_name):
                self._optimize_table(dt, table_name, config)

        except Exception as e:
            logger.warning(f"⚠️ Failed to apply post-write features: {e}")

    def _set_table_properties(self, dt: DeltaTable, table_name: str, config: Dict):
        """Set Delta table properties"""
        properties = {}

        # Time Travel retention
        if self.is_feature_enabled("time_travel"):
            retention = self.delta_config.get("time_travel", {}).get(
                "retention_days", 90
            )
            properties["delta.logRetentionDuration"] = f"interval {retention} days"

        # Vacuum retention
        if self.is_feature_enabled("vacuum"):
            retention = self.delta_config.get("vacuum", {}).get("retention_hours", 168)
            properties["delta.deletedFileRetentionDuration"] = (
                f"interval {retention} hours"
            )

        # Set properties
        if properties:
            try:
                dt.alter.set_properties(properties)
                logger.debug(f"⚙️ Set properties for {table_name}")
            except Exception as e:
                logger.warning(f"⚠️ Failed to set properties: {e}")

    def should_optimize(self, table_name: str) -> bool:
        """Check if table should be optimized"""
        if not self.is_feature_enabled("optimize"):
            return False

        delta_path = self.tables_path / table_name

        # Check if table exists
        try:
            if hasattr(DeltaTable, "is_delta_table"):
                table_exists = DeltaTable.is_delta_table(str(delta_path))
            else:
                try:
                    dt = DeltaTable(str(delta_path))
                    table_exists = True
                except:
                    table_exists = False

            if not table_exists:
                return True

            dt = DeltaTable(str(delta_path))
            history = dt.history()

            # Find last optimization
            last_optimized = None
            for entry in reversed(history):
                if entry.get("operation") == "OPTIMIZE":
                    last_optimized = datetime.fromisoformat(
                        entry["timestamp"].replace("Z", "+00:00")
                    )
                    break

            if not last_optimized:
                return True

            # Check frequency
            frequency = self.delta_config.get("optimize", {}).get("frequency", "weekly")
            days_since = (datetime.now() - last_optimized).days

            if frequency == "daily":
                return days_since >= 1
            elif frequency == "weekly":
                return days_since >= 7
            elif frequency == "monthly":
                return days_since >= 30

            return False

        except Exception:
            return True

    def _optimize_table(self, dt: DeltaTable, table_name: str, config: Dict):
        """Optimize Delta table"""
        try:
            dt.optimize()
            logger.info(f"🔧 Optimized {table_name}")

        except Exception as e:
            logger.warning(f"⚠️ Optimization failed for {table_name}: {e}")

    def table_exists(self, table_name: str) -> bool:
        """Check if Delta table exists - FIXED for older deltalake versions"""
        try:
            delta_path = self.tables_path / table_name

            # Try the new method first
            if hasattr(DeltaTable, "is_delta_table"):
                return DeltaTable.is_delta_table(str(delta_path))
            else:
                # Fallback for older versions - try to load the table
                try:
                    dt = DeltaTable(str(delta_path))
                    return True
                except:
                    return False
        except Exception:
            return False

    def process_data_type(self, data_type_config: Dict) -> Dict:
        """Process a data type"""
        start_time = datetime.now()
        table_name = data_type_config["table_name"]

        logger.info(f"\n🎯 PROCESSING: {table_name}")
        logger.info(f"   📁 Source: {data_type_config.get('path')}")

        try:
            # Discover files
            files, checkpoint = self.discover_files_incremental(
                data_type_config["path"], table_name
            )

            if not files:
                return self._create_result(
                    data_type_config, "NO_NEW_FILES", 0, start_time
                )

            # Read files
            combined_df = self.read_files_with_features(files, data_type_config)

            if combined_df is None or len(combined_df) == 0:
                return self._create_result(data_type_config, "NO_DATA", 0, start_time)

            # Ensure partition columns exist
            partition_cols = data_type_config.get("partition_columns", [])
            for col in partition_cols:
                if col not in combined_df.columns:
                    logger.warning(
                        f"⚠️ Partition column {col} not found in data for {table_name}"
                    )

            # Implement SCD2
            metrics = self.implement_scd2(combined_df, table_name, data_type_config)

            # Update checkpoint
            if (
                self.is_feature_enabled("incremental_loading")
                and data_type_config.get("load_strategy") == "incremental"
                and files
            ):

                self._update_checkpoint(
                    table_name, files, combined_df, data_type_config, metrics
                )

            # Create result
            result = self._create_result(
                data_type_config, "COMPLETED", len(combined_df), start_time, metrics
            )

            logger.info(f"✅ Completed: {table_name} - {len(combined_df):,} rows")
            return result

        except Exception as e:
            logger.error(f"❌ Failed to process {table_name}: {e}")
            return self._create_result(
                data_type_config, "FAILED", 0, start_time, error=str(e)
            )

    def _update_checkpoint(
        self,
        table_name: str,
        files: List[Path],
        df: pd.DataFrame,
        config: Dict,
        metrics: LoadMetrics,
    ):
        """Update checkpoint"""
        if not files:
            return

        latest_file = max(files, key=lambda f: f.stat().st_mtime)
        latest_time = datetime.fromtimestamp(latest_file.stat().st_mtime)

        watermark_value = None
        watermark_column = config.get("watermark_column")
        if watermark_column and watermark_column in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[watermark_column]):
                watermark_value = df[watermark_column].max()
            else:
                watermark_value = df[watermark_column].max()

        checkpoint = Checkpoint(
            table_name=table_name,
            last_processed_file=str(latest_file),
            last_processed_time=latest_time,
            last_watermark_value=watermark_value,
            last_watermark_column=watermark_column,
            records_processed=metrics.rows_processed,
            load_strategy=config.get("load_strategy", "incremental"),
            metadata={"load_id": self.load_id, "metrics": metrics.to_dict()},
        )

        self.save_checkpoint(checkpoint)
        logger.info(f"📌 Updated checkpoint for {table_name}")

    def _create_result(
        self,
        config: Dict,
        status: str,
        rows: int,
        start_time: datetime,
        metrics: LoadMetrics = None,
        error: str = None,
    ) -> Dict:
        """Create result dictionary"""
        duration = (datetime.now() - start_time).total_seconds()

        result = {
            "feature": config["feature"],
            "subfeature": config["subfeature"],
            "data_type": config["data_type"],
            "table_name": config["table_name"],
            "status": status,
            "rows_loaded": rows,
            "duration_seconds": round(duration, 2),
            "timestamp": datetime.now().isoformat(),
            "load_id": self.load_id,
            "load_strategy": config.get("load_strategy", "incremental"),
        }

        if metrics:
            result["metrics"] = metrics.to_dict()

        if error:
            result["error"] = error

        return result

    def get_data_types_to_process(self) -> List[Dict]:
        """Get all enabled data types"""
        data_types = []

        for feature in self.data_sources.get("features", []):
            if not feature.get("enabled", True):
                continue

            for subfeature in feature.get("subfeatures", []):
                if not subfeature.get("enabled", True):
                    continue

                for data_type in subfeature.get("data_types", []):
                    if data_type.get("enabled", True):
                        full_config = {
                            "feature": feature["name"],
                            "subfeature": subfeature["name"],
                            "data_type": data_type["name"],
                            "path": data_type["path"],
                            "table_name": f"bronze_{feature['name']}_{subfeature['name']}_{data_type['name']}",
                            "primary_keys": data_type.get("primary_keys", []),
                            "scd2_type": data_type.get("scd2_type", "type2"),
                            "enable_scd2": data_type.get("enable_scd2", True),
                            "hash_columns": data_type.get("hash_columns", []),
                            "load_strategy": data_type.get(
                                "load_strategy", "incremental"
                            ),
                            "watermark_column": data_type.get("watermark_column"),
                            "partition_columns": data_type.get("partition_columns", []),
                        }

                        data_types.append(full_config)

        return data_types

    def load_all_data_types(self) -> List[Dict]:
        """Load all enabled data types"""
        data_types = self.get_data_types_to_process()
        results = []

        logger.info(f"\n🚀 Starting load of {len(data_types)} data types")

        for data_type_config in data_types:
            result = self.process_data_type(data_type_config)
            results.append(result)

            if result["status"] == "COMPLETED":
                metrics = result.get("metrics", {})
                logger.info(
                    f"   ✅ {result['table_name']}: "
                    f"{result['rows_loaded']:,} rows "
                    f"({metrics.get('duration_seconds', 0):.1f}s)"
                )
            else:
                logger.warning(f"   ⚠️ {result['table_name']}: {result['status']}")

        return results

    def generate_summary_report(self, results: List[Dict]):
        """Generate summary report"""
        successful = [r for r in results if r["status"] == "COMPLETED"]
        failed = [r for r in results if r["status"] == "FAILED"]
        skipped = [r for r in results if r["status"] in ["NO_NEW_FILES", "NO_DATA"]]

        total_rows = sum(r.get("rows_loaded", 0) for r in results)
        total_duration = sum(r.get("duration_seconds", 0) for r in results)

        total_insert = sum(
            r.get("metrics", {}).get("insert_count", 0) for r in successful
        )
        total_update = sum(
            r.get("metrics", {}).get("update_count", 0) for r in successful
        )

        print("\n" + "=" * 80)
        print("📈 COMPLETE BRONZE LAYER - SUMMARY REPORT")
        print("=" * 80)

        print(f"\n📊 OVERALL STATISTICS:")
        print(f"   ✅ Successful: {len(successful)} tables")
        print(f"   ❌ Failed: {len(failed)} tables")
        print(f"   ⚠️  Skipped: {len(skipped)} tables")
        print(f"   📥 Total Rows: {total_rows:,}")
        print(f"   ⏱️  Total Duration: {total_duration:.1f} seconds")

        print(f"\n🔄 CHANGE STATISTICS:")
        print(f"   📥 Inserted: {total_insert:,} records")
        print(f"   🔄 Updated: {total_update:,} records")

        if successful:
            print(f"\n✅ SUCCESSFUL LOADS:")
            for result in successful:
                metrics = result.get("metrics", {})
                print(f"   📋 {result['table_name']}:")
                print(f"      📊 Rows: {result['rows_loaded']:,}")
                print(f"      ⏱️  Time: {metrics.get('duration_seconds', 0):.1f}s")
                print(f"      📥 New: {metrics.get('insert_count', 0):,}")
                print(f"      🔄 Changed: {metrics.get('update_count', 0):,}")

        if failed:
            print(f"\n❌ FAILED LOADS:")
            for result in failed:
                print(
                    f"   📋 {result['table_name']}: {result.get('error', 'Unknown error')}"
                )

        print("\n" + "=" * 80)

    def run(self):
        """Main run method"""
        try:
            # Load all data types
            results = self.load_all_data_types()

            # Generate summary
            self.generate_summary_report(results)

            # Return success status
            failed = [r for r in results if r["status"] == "FAILED"]
            return len(failed) == 0

        except Exception as e:
            logger.error(f"❌ Fatal error in bronze layer: {e}", exc_info=True)
            return False


def main():
    """Main execution"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Complete Bronze Layer - Fixed Version"
    )
    parser.add_argument(
        "--config",
        default="bronze_config_complete.yaml",
        help="Configuration file path",
    )

    args = parser.parse_args()

    loader = None
    try:
        loader = CompleteBronzeLayer(config_path=args.config)
        success = loader.run()
        return success

    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
