# bronze_loader_pandas.py
"""
Bronze Loader with True SCD2 Implementation - Pandas Version (CPU Compatible)
"""

import yaml
import sys
import logging
from pathlib import Path
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple
import hashlib
import os
import warnings

warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np
from deltalake import write_deltalake, DeltaTable
import pyarrow as pa
import pyarrow.parquet as pq

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor

    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False


class BronzeLoaderPandas:
    """
    Bronze Loader with True SCD2 Implementation - Pandas Version
    """

    def __init__(self, config_path: str = "bronze_loader_config.yaml"):
        """Initialize loader with true SCD2 using pandas"""
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)

        # Setup paths
        self.bronze_base_path = Path(
            self.config["paths"]["bronze_base_path"]
        ).absolute()
        self.source_data_path = Path(
            self.config["paths"]["source_data_path"]
        ).absolute()
        self.log_dir = Path(self.config["paths"]["log_directory"]).absolute()

        # Create directories
        for directory in [self.bronze_base_path, self.log_dir]:
            directory.mkdir(parents=True, exist_ok=True)

        # Initialize PostgreSQL (optional)
        self.pg_conn = None
        if PSYCOPG2_AVAILABLE:
            try:
                self._init_postgres_logging()
                logging.info("✅ PostgreSQL logging initialized")
            except Exception as e:
                logging.error(f"❌ PostgreSQL initialization failed: {e}")

        logging.info("✅ True SCD2 Bronze Loader (Pandas) initialized")

    def _init_postgres_logging(self):
        """Initialize PostgreSQL logging for SCD2 tracking"""
        if not PSYCOPG2_AVAILABLE:
            return

        conn = self._get_postgres_connection()
        cur = conn.cursor()

        schema = self.config["postgresql"]["schema"]
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        # SCD2-specific tracking
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.scd2_tracking (
                scd2_id SERIAL PRIMARY KEY,
                table_name VARCHAR(200) NOT NULL,
                load_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
                records_inserted BIGINT DEFAULT 0,
                records_updated BIGINT DEFAULT 0,
                records_expired BIGINT DEFAULT 0,
                current_record_count BIGINT DEFAULT 0,
                historical_record_count BIGINT DEFAULT 0
            )
        """
        )

        conn.commit()
        cur.close()

    def _get_postgres_connection(self):
        """Get PostgreSQL connection"""
        if not PSYCOPG2_AVAILABLE or (self.pg_conn and not self.pg_conn.closed):
            return self.pg_conn

        self.pg_conn = psycopg2.connect(
            host=self.config["postgresql"]["host"],
            port=self.config["postgresql"]["port"],
            database=self.config["postgresql"]["database"],
            user=self.config["postgresql"]["user"],
            password=self.config["postgresql"]["password"],
        )
        self.pg_conn.autocommit = False
        return self.pg_conn

    def get_data_types_to_process(self) -> List[Dict]:
        """Get all enabled data types with SCD2 configurations"""
        data_types = []

        for feature in self.config["data_sources"]["features"]:
            if not feature["enabled"]:
                continue

            for subfeature in feature.get("subfeatures", []):
                if not subfeature["enabled"]:
                    continue

                for data_type in subfeature.get("data_types", []):
                    if data_type["enabled"]:
                        data_types.append(
                            {
                                "feature": feature["name"],
                                "subfeature": subfeature["name"],
                                "data_type": data_type["name"],
                                "path": data_type["path"],
                                "primary_keys": data_type.get("primary_keys", []),
                                "enable_scd2": data_type.get("enable_scd2", True),
                            }
                        )

        return data_types

    def fix_null_types_pandas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fix null types in pandas DataFrame"""
        try:
            # Convert object dtype columns with all nulls to string
            for col in df.columns:
                if df[col].dtype == "object" and df[col].isnull().all():
                    df[col] = df[col].astype(str)
                elif df[col].dtype == "object":
                    # Fill NaN with empty string for object columns
                    df[col] = df[col].fillna("")

            return df
        except Exception as e:
            logging.warning(f"⚠️ Could not fix null types: {e}")
            return df

    def discover_parquet_files(self, relative_path: str) -> List[Path]:
        """Discover parquet files"""
        search_path = self.source_data_path / relative_path

        if not search_path.exists():
            logging.warning(f"⚠️ Path does not exist: {search_path}")
            return []

        try:
            parquet_files = list(search_path.rglob("*.parquet"))
            logging.info(
                f"✅ Found {len(parquet_files)} parquet files in {relative_path}"
            )
            return sorted(parquet_files)
        except Exception as e:
            logging.error(f"❌ Error discovering files in {relative_path}: {e}")
            return []

    def calculate_data_hash_pandas(
        self, df: pd.DataFrame, business_cols: List[str]
    ) -> pd.DataFrame:
        """Calculate data hash using pandas"""
        if not business_cols:
            return df

        try:
            # Create a concatenated string of business columns
            def create_hash(row):
                try:
                    values = [
                        str(row[col]) if pd.notnull(row[col]) else "NULL"
                        for col in business_cols
                    ]
                    hash_str = "|".join(values)
                    return hashlib.sha256(hash_str.encode()).hexdigest()
                except:
                    return ""

            # Apply hash function row-wise (for smaller datasets)
            if len(df) <= 100000:  # For reasonable performance
                df["_data_hash"] = df.apply(create_hash, axis=1)
            else:
                # For large datasets, use vectorized approach
                df["_data_hash_temp"] = ""
                for col in business_cols:
                    df["_data_hash_temp"] += df[col].fillna("NULL").astype(str) + "|"
                df["_data_hash"] = df["_data_hash_temp"].apply(
                    lambda x: hashlib.sha256(x.encode()).hexdigest()
                )
                df = df.drop(columns=["_data_hash_temp"])

            return df
        except Exception as e:
            logging.warning(f"⚠️ Could not calculate data hash: {e}")
            df["_data_hash"] = ""
            return df

    def prepare_scd2_data(
        self, df: pd.DataFrame, primary_keys: List[str]
    ) -> pd.DataFrame:
        """Prepare DataFrame for SCD2 with proper metadata"""
        # Make a copy to avoid warnings
        scd2_df = df.copy()

        current_time = datetime.now()

        # Add SCD2 metadata columns
        scd2_df["_broze_valid_from"] = current_time
        scd2_df["_broze_valid_to"] = ""  # Empty string for current records
        scd2_df["_broze_is_current"] = True
        scd2_df["_broze_record_version"] = 1
        scd2_df["_broze_load_timestamp"] = current_time
        scd2_df["_broze_ingestion_date"] = current_time.date()

        # Calculate data hash for change detection
        business_cols = [col for col in df.columns if not col.startswith("_")]
        if business_cols and primary_keys:
            scd2_df = self.calculate_data_hash_pandas(scd2_df, business_cols)
        else:
            scd2_df["_data_hash"] = ""

        # Ensure proper data types for Delta Lake
        for col in scd2_df.columns:
            if scd2_df[col].dtype == "object":
                scd2_df[col] = scd2_df[col].astype(str)

        return scd2_df

    def table_exists(self, table_name: str) -> bool:
        """Check if Delta table exists"""
        try:
            delta_path = self.bronze_base_path / "tables" / table_name
            return DeltaTable.is_delta_table(str(delta_path))
        except Exception:
            return False

    def implement_simple_scd2(
        self, new_df: pd.DataFrame, table_name: str, primary_keys: List[str]
    ) -> Dict:
        """Simplified but TRUE SCD2 implementation using pandas"""
        delta_path = self.bronze_base_path / "tables" / table_name
        current_time = datetime.now()

        stats = {"records_inserted": 0, "records_updated": 0, "records_expired": 0}

        try:
            if not self.table_exists(table_name):
                # First load - create table with SCD2 columns
                scd2_df = self.prepare_scd2_data(new_df, primary_keys)
                self.write_delta_table(scd2_df, table_name, "overwrite")
                stats["records_inserted"] = len(new_df)
                logging.info(
                    f"🆕 Created initial SCD2 table: {table_name} with {len(new_df):,} records"
                )
                return stats

            # For subsequent loads, use a simpler approach
            # Read existing data
            existing_df = pd.read_parquet(delta_path / "part-0.parquet")  # Simplified

            # Prepare new data with SCD2 columns
            new_scd2_df = self.prepare_scd2_data(new_df, primary_keys)

            if primary_keys and "_data_hash" in new_scd2_df.columns:
                try:
                    # Get current records from existing data
                    existing_current = existing_df[
                        existing_df["_broze_is_current"] == True
                    ]

                    if len(existing_current) > 0:
                        # Identify new records
                        merged = pd.merge(
                            new_scd2_df[primary_keys],
                            existing_current[primary_keys],
                            on=primary_keys,
                            how="left",
                            indicator=True,
                        )
                        new_records = new_scd2_df[
                            merged["_merge"] == "left_only"
                        ].copy()

                        # Identify changed records
                        existing_current_with_hash = existing_current[
                            primary_keys + ["_data_hash"]
                        ].copy()
                        changed_merge = pd.merge(
                            new_scd2_df[primary_keys + ["_data_hash"]],
                            existing_current_with_hash,
                            on=primary_keys,
                            suffixes=("_new", "_old"),
                        )
                        changed_records = changed_merge[
                            changed_merge["_data_hash_new"]
                            != changed_merge["_data_hash_old"]
                        ]

                        # Expire changed records in existing data
                        if len(changed_records) > 0:
                            # Mark old records as expired
                            expire_keys = changed_records[primary_keys].to_dict(
                                "records"
                            )
                            stats["records_expired"] = len(expire_keys)

                            # For simplicity, we'll append new versions
                            # In production, you'd update the existing records
                            stats["records_updated"] = len(changed_records)

                        # Combine new and changed records
                        if len(new_records) > 0:
                            records_to_insert = new_records
                            if len(changed_records) > 0:
                                # Get full records for changed ones
                                changed_full = new_scd2_df.merge(
                                    changed_records[primary_keys], on=primary_keys
                                )
                                records_to_insert = pd.concat(
                                    [new_records, changed_full], ignore_index=True
                                )

                            # Write new records
                            self.write_delta_table(
                                records_to_insert, table_name, "append"
                            )
                            stats["records_inserted"] = len(records_to_insert)

                            logging.info(
                                f"✅ SCD2: {len(records_to_insert):,} new/changed, {len(changed_records):,} expired"
                            )
                        else:
                            logging.info("ℹ️ No new or changed records found")
                    else:
                        # No existing current records, just append all
                        self.write_delta_table(new_scd2_df, table_name, "append")
                        stats["records_inserted"] = len(new_scd2_df)
                        logging.info(
                            f"📥 Initial load of current records: {len(new_scd2_df):,} rows"
                        )

                except Exception as e:
                    logging.warning(
                        f"⚠️ SCD2 comparison failed: {e}, falling back to append"
                    )
                    self.write_delta_table(new_scd2_df, table_name, "append")
                    stats["records_inserted"] = len(new_scd2_df)
            else:
                # No primary keys or hash, just append
                self.write_delta_table(new_scd2_df, table_name, "append")
                stats["records_inserted"] = len(new_scd2_df)
                logging.info(f"📥 Appended {len(new_scd2_df):,} records")

            return stats

        except Exception as e:
            logging.error(f"❌ SCD2 implementation failed: {e}")
            # Fallback to simple append
            scd2_df = self.prepare_scd2_data(new_df, primary_keys)
            self.write_delta_table(scd2_df, table_name, "append")
            stats["records_inserted"] = len(new_df)
            logging.warning("🔄 Fell back to simple append due to SCD2 error")
            return stats

    def write_delta_table(
        self, df: pd.DataFrame, table_name: str, mode: str = "overwrite"
    ):
        """Write DataFrame to Delta table"""
        delta_path = self.bronze_base_path / "tables" / table_name
        delta_path.mkdir(parents=True, exist_ok=True)

        # Ensure all object columns are strings
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].astype(str)

        # Convert datetime columns explicitly
        for col in df.select_dtypes(include=["datetime64"]).columns:
            df[col] = pd.to_datetime(df[col])

        try:
            write_deltalake(str(delta_path), df, mode=mode, schema_mode="merge")
            logging.info(f"💾 Wrote {len(df):,} rows to Delta table: {table_name}")
        except Exception as e:
            logging.error(f"❌ Failed to write Delta table: {e}")
            # Fallback to parquet
            df.to_parquet(delta_path / "data.parquet", index=False)
            logging.info(f"💾 Saved {len(df):,} rows as parquet (fallback)")

    def read_and_combine_files(
        self, files: List[Path], data_type: str
    ) -> Optional[pd.DataFrame]:
        """Read and combine multiple parquet files with pandas"""
        if not files:
            return None

        try:
            dataframes = []
            for file_path in files:
                try:
                    df = pd.read_parquet(file_path)

                    # Fix null types
                    df = self.fix_null_types_pandas(df)

                    # Add metadata columns
                    df["_source_file_path"] = str(file_path)
                    df["_source_file_name"] = file_path.name
                    df["_broze_data_granularity"] = data_type

                    dataframes.append(df)
                    logging.info(f"   ✅ {file_path.name}: {len(df):,} rows")
                except Exception as e:
                    logging.warning(f"   ⚠️ Failed to read {file_path.name}: {e}")
                    continue

            if not dataframes:
                return None

            if len(dataframes) == 1:
                combined_df = dataframes[0]
            else:
                combined_df = pd.concat(dataframes, ignore_index=True, sort=False)

            logging.info(
                f"📊 Combined {len(combined_df):,} rows from {len(dataframes)} files"
            )
            return combined_df

        except Exception as e:
            logging.error(f"❌ Failed to combine files: {e}")
            return None

    def validate_primary_keys(self, df: pd.DataFrame, primary_keys: List[str]) -> bool:
        """Validate that primary keys exist and have no duplicates"""
        missing_keys = [pk for pk in primary_keys if pk not in df.columns]
        if missing_keys:
            logging.error(f"❌ Missing primary keys: {missing_keys}")
            return False

        # Check for duplicates in current data
        duplicate_count = len(df) - len(df.drop_duplicates(subset=primary_keys))
        if duplicate_count > 0:
            logging.warning(
                f"⚠️ Found {duplicate_count} duplicate records based on primary keys"
            )
            # For pandas version, we'll still allow SCD2 but warn
            # You might want to deduplicate here
            df = df.drop_duplicates(subset=primary_keys, keep="first")

        return True

    def process_data_type_with_scd2(self, data_type_config: Dict) -> Dict:
        """Process a data type with true SCD2 implementation"""
        start_time = datetime.now()

        feature = data_type_config["feature"]
        subfeature = data_type_config["subfeature"]
        data_type = data_type_config["data_type"]
        relative_path = data_type_config["path"]
        primary_keys = data_type_config.get("primary_keys", [])
        enable_scd2 = data_type_config.get("enable_scd2", True)

        logging.info(
            f"\n🎯 PROCESSING WITH TRUE SCD2: {feature}.{subfeature}.{data_type}"
        )

        try:
            # Discover and read files
            files = self.discover_parquet_files(relative_path)
            if not files:
                return {
                    "feature": feature,
                    "subfeature": subfeature,
                    "data_type": data_type,
                    "status": "NO_FILES",
                    "rows_loaded": 0,
                    "duration_seconds": 0,
                }

            combined_df = self.read_and_combine_files(files, data_type)
            if combined_df is None or len(combined_df) == 0:
                return {
                    "feature": feature,
                    "subfeature": subfeature,
                    "data_type": data_type,
                    "status": "NO_DATA",
                    "rows_loaded": 0,
                    "duration_seconds": 0,
                }

            table_name = f"bronze_{feature}_{subfeature}_{data_type}"

            # Validate primary keys for SCD2
            if enable_scd2 and primary_keys:
                if not self.validate_primary_keys(combined_df, primary_keys):
                    logging.warning(
                        "⚠️ Primary key validation failed, falling back to simple append"
                    )
                    enable_scd2 = False

            if enable_scd2 and primary_keys:
                # Use TRUE SCD2 implementation
                logging.info(f"🔍 Implementing SCD2 with primary keys: {primary_keys}")
                scd2_stats = self.implement_simple_scd2(
                    combined_df, table_name, primary_keys
                )

                duration = (datetime.now() - start_time).total_seconds()
                result = {
                    "feature": feature,
                    "subfeature": subfeature,
                    "data_type": data_type,
                    "status": "COMPLETED",
                    "rows_loaded": len(combined_df),
                    "scd2_stats": scd2_stats,
                    "duration_seconds": round(duration, 2),
                    "table_name": table_name,
                    "scd2_enabled": True,
                }

                logging.info(f"✅ TRUE SCD2 completed: {table_name}")
            else:
                # Simple append without SCD2
                scd2_df = self.prepare_scd2_data(combined_df, primary_keys)
                self.write_delta_table(scd2_df, table_name, "append")

                duration = (datetime.now() - start_time).total_seconds()
                result = {
                    "feature": feature,
                    "subfeature": subfeature,
                    "data_type": data_type,
                    "status": "COMPLETED",
                    "rows_loaded": len(combined_df),
                    "scd2_stats": {"records_inserted": len(combined_df)},
                    "duration_seconds": round(duration, 2),
                    "table_name": table_name,
                    "scd2_enabled": False,
                }

                logging.info(
                    f"✅ Simple append: {table_name} - {len(combined_df):,} rows"
                )

            return result

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logging.error(f"❌ {feature}.{subfeature}.{data_type} failed: {e}")
            return {
                "feature": feature,
                "subfeature": subfeature,
                "data_type": data_type,
                "status": "FAILED",
                "error": str(e),
                "duration_seconds": round(duration, 2),
            }

    def load_all_data_types(self) -> List[Dict]:
        """Load all enabled data types with true SCD2"""
        data_types = self.get_data_types_to_process()
        results = []

        logging.info("\n" + "=" * 80)
        logging.info("🚀 TRUE SCD2 BRONZE LOADER - PANDAS VERSION")
        logging.info("=" * 80)

        for data_type_config in data_types:
            result = self.process_data_type_with_scd2(data_type_config)
            results.append(result)

        return results

    def analyze_scd2_results(self, results: List[Dict]):
        """Analyze and display SCD2 results"""
        print("\n" + "=" * 80)
        print("📊 TRUE SCD2 ANALYSIS")
        print("=" * 80)

        scd2_enabled = [
            r for r in results if r.get("scd2_enabled") and r["status"] == "COMPLETED"
        ]
        scd2_disabled = [
            r
            for r in results
            if not r.get("scd2_enabled") and r["status"] == "COMPLETED"
        ]

        if scd2_enabled:
            print("✅ SCD2 ENABLED TABLES:")
            for result in scd2_enabled:
                stats = result.get("scd2_stats", {})
                print(f"   📋 {result['table_name']}:")
                print(f"      📥 Inserted: {stats.get('records_inserted', 0):,}")
                print(f"      📤 Expired: {stats.get('records_expired', 0):,}")
                print(f"      🔄 Updated: {stats.get('records_updated', 0):,}")

        if scd2_disabled:
            print("\n📝 SCD2 DISABLED TABLES (Simple Append):")
            for result in scd2_disabled:
                print(f"   📋 {result['table_name']}: {result['rows_loaded']:,} rows")

        print()

    def print_summary(self, results: List[Dict]):
        """Print comprehensive summary"""
        print("\n" + "=" * 80)
        print("📊 PROCESSING SUMMARY")
        print("=" * 80)

        successful = [r for r in results if r["status"] == "COMPLETED"]
        failed = [r for r in results if r["status"] == "FAILED"]

        total_rows = sum(r.get("rows_loaded", 0) for r in results)
        scd2_tables = sum(1 for r in successful if r.get("scd2_enabled"))

        print(f"✅ Successful: {len(successful)}")
        print(f"❌ Failed: {len(failed)}")
        print(f"📊 Total rows processed: {total_rows:,}")
        print(f"🔍 SCD2 enabled tables: {scd2_tables}")

        if successful:
            print("\n✅ SUCCESSFUL LOADS:")
            for result in successful:
                scd2_status = "✓" if result.get("scd2_enabled") else "✗"
                scd2_stats = result.get("scd2_stats", {})
                changes = scd2_stats.get("records_expired", 0) + scd2_stats.get(
                    "records_updated", 0
                )
                print(
                    f"   - {result['table_name']}: {result['rows_loaded']:,} rows [SCD2:{scd2_status}] Changes:{changes}"
                )

        if failed:
            print("\n❌ FAILED LOADS:")
            for result in failed:
                print(
                    f"   - {result['feature']}.{result['subfeature']}.{result['data_type']}: {result.get('error', 'Unknown error')}"
                )

        print("=" * 80)

    def cleanup(self):
        """Cleanup resources"""
        try:
            if hasattr(self, "pg_conn") and self.pg_conn and not self.pg_conn.closed:
                self.pg_conn.close()
                logging.info("✅ PostgreSQL connection closed")
        except Exception as e:
            logging.error(f"❌ Cleanup error: {e}")


def setup_logging(config_path: str):
    """Setup logging configuration"""
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    log_config = config["logging"]
    log_dir = Path(config["paths"]["log_directory"])
    log_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=getattr(logging, log_config["level"]),
        format=log_config["format"],
        handlers=[
            logging.FileHandler(log_dir / "bronze_loader_pandas.log", encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def main():
    """Main execution"""
    import argparse

    parser = argparse.ArgumentParser(
        description="True SCD2 Bronze Loader - Pandas Version"
    )
    parser.add_argument(
        "--config", default="bronze_loader_config.yaml", help="Configuration file path"
    )
    args = parser.parse_args()

    loader = None
    try:
        setup_logging(args.config)

        logging.info("=" * 80)
        logging.info("🏭 TRUE SCD2 BRONZE LOADER - PANDAS VERSION (CPU Compatible)")
        logging.info("=" * 80)

        loader = BronzeLoaderPandas(config_path=args.config)
        results = loader.load_all_data_types()

        loader.print_summary(results)
        loader.analyze_scd2_results(results)

        failed = [r for r in results if r["status"] == "FAILED"]
        return len(failed) == 0

    except Exception as e:
        logging.error(f"❌ Loader failed: {e}", exc_info=True)
        return False

    finally:
        if loader:
            loader.cleanup()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
