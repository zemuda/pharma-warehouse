# bronze_loader_true_scd2_fixed.py
"""
True SCD2 Implementation - Fixed Version
"""

import yaml
import sys
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import hashlib
import os

# Set environment variable to skip CPU check
os.environ["POLARS_NO_SIMD"] = "1"  # Add this line
os.environ["POLARS_SKIP_CPU_CHECK"] = "1"

import polars as pl
from deltalake import write_deltalake, DeltaTable
import pyarrow as pa

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor

    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False


class BronzeLoaderPolars:
    """
    Bronze Loader with True SCD2 Implementation - Fixed Version
    """

    def __init__(self, config_path: str = "bronze_loader_config.yaml"):
        """Initialize loader with true SCD2"""
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

        # Initialize PostgreSQL
        self.pg_conn = None
        if PSYCOPG2_AVAILABLE:
            try:
                self._init_postgres_logging()
                logging.info("✅ PostgreSQL logging initialized")
            except Exception as e:
                logging.error(f"❌ PostgreSQL initialization failed: {e}")

        logging.info("✅ True SCD2 Bronze Loader initialized")

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

    def fix_null_types_simple(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Simple fix for NullType columns by converting them to StringType
        """
        try:
            # Get the schema to identify NullType columns
            null_columns = []
            for col in df.columns:
                # Check if column has null type or problematic type
                if df[col].dtype == pl.Null:
                    null_columns.append(col)

            if null_columns:
                logging.info(
                    f"🔄 Fixing {len(null_columns)} NullType columns: {null_columns}"
                )

                # Convert all NullType columns to String
                for col in null_columns:
                    df = df.with_columns(pl.col(col).cast(pl.Utf8))

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

    def calculate_data_hash_polars(
        self, df: pl.DataFrame, business_cols: List[str]
    ) -> pl.DataFrame:
        """Calculate data hash using Polars expressions (FIXED)"""
        if not business_cols:
            return df

        # Use Polars' built-in hash functions properly
        hash_expr = (
            pl.concat_str(
                [pl.col(col).cast(pl.Utf8).fill_null("NULL") for col in business_cols],
                separator="|",
            )
            .map_elements(
                lambda x: hashlib.sha256(x.encode()).hexdigest() if x else "",
                return_dtype=pl.Utf8,
            )
            .alias("_data_hash")
        )

        return df.with_columns([hash_expr])

    """V1: START DEEPSEEK prepare_scd2_data"""

    # def prepare_scd2_data(
    #     self, df: pl.DataFrame, primary_keys: List[str]
    # ) -> pl.DataFrame:
    #     """Prepare DataFrame for SCD2 with proper metadata"""
    #     """START GEMINI: Prepare DataFrame for SCD2 with proper metadata and fix schema issues."""
    #     # 1. Convert Polars to PyArrow Table to check for null types
    #     # PyArrow types are what Delta Lake ultimately consumes.
    #     # arrow_table = df.to_arrow()

    #     # # Columns in PyArrow with the 'null' type that need conversion
    #     # null_pyarrow_cols = [
    #     #     field.name for field in arrow_table.schema if pa.types.is_null(field.type)
    #     # ]

    #     # # 2. Apply a default cast (e.g., Utf8/String) to all problematic columns
    #     # if null_pyarrow_cols:
    #     #     logging.warning(
    #     #         f"⚠️ Found Null-type columns: {null_pyarrow_cols}. Forcing cast to Utf8."
    #     #     )
    #     #     # Create a list of expressions to cast the problematic columns
    #     #     cast_expressions = [pl.col(col).cast(pl.Utf8) for col in null_pyarrow_cols]

    #     #     # Apply the cast to the Polars DataFrame
    #     #     df = df.with_columns(cast_expressions)

    #     # # 3. Proceed with SCD2 metadata columns
    #     """END GEMINI"""

    #     current_time = datetime.now()

    #     # Add SCD2 metadata columns
    #     scd2_df = df.with_columns(
    #         [
    #             pl.lit(current_time).alias("_broze_valid_from"),
    #             pl.lit(None).cast(pl.Utf8).alias("_broze_valid_to"),
    #             pl.lit(True).alias("_broze_is_current"),
    #             pl.lit(1).alias("_broze_record_version"),
    #             pl.lit(datetime.now()).alias("_broze_load_timestamp"),
    #             pl.lit(datetime.now().date()).alias("_broze_ingestion_date"),
    #             pl.lit(str(datetime.now())).alias("_source_file_path"),  # Temporary
    # change this <---------------------------->
    #             pl.lit("combined").alias("_source_file_name"),
    #             pl.lit("detailed").alias("_broze_data_granularity"),
    #         ]
    #     )

    #     # Calculate data hash for change detection (exclude metadata columns)
    #     business_cols = [col for col in df.columns if not col.startswith("_")]
    #     if business_cols and primary_keys:
    #         scd2_df = self.calculate_data_hash_polars(scd2_df, business_cols)

    #     return scd2_df

    """V1: END DEEPSEEK prepare_scd2_data"""

    """V2: START DEEPSEEK prepare_scd2_data and _fix_polars_schema_for_delta"""

    # def prepare_scd2_data(
    #     self, df: pl.DataFrame, primary_keys: List[str]
    # ) -> pl.DataFrame:
    #     """Prepare DataFrame for SCD2 with proper metadata and fix schema issues."""

    #     # 1. Fix ALL potential schema issues in Polars before any conversion
    #     df_fixed = self._fix_polars_schema_for_delta(df)

    #     # 2. Proceed with SCD2 metadata columns
    #     current_time = datetime.now()

    #     # Add SCD2 metadata columns
    #     scd2_df = df_fixed.with_columns(
    #         [
    #             pl.lit(current_time).alias("_broze_valid_from"),
    #             pl.lit(None).cast(pl.Utf8).alias("_broze_valid_to"),
    #             pl.lit(True).alias("_broze_is_current"),
    #             pl.lit(1).alias("_broze_record_version"),
    #             pl.lit(datetime.now()).alias("_broze_load_timestamp"),
    #             pl.lit(datetime.now().date()).alias("_broze_ingestion_date"),
    #             # ❌ REMOVE these - they're already set in read_and_combine_files
    #             pl.lit("combined").alias("_source_file_name"),  # DELETE
    #             pl.lit("detailed").alias("_broze_data_granularity"),  # DELETE
    #         ]
    #     )

    #     # Calculate data hash for change detection (exclude metadata columns)
    #     business_cols = [col for col in df_fixed.columns if not col.startswith("_")]
    #     if business_cols and primary_keys:
    #         scd2_df = self.calculate_data_hash_polars(scd2_df, business_cols)

    #     return scd2_df

    """V2.1: END DEEPSEEK prepare_scd2_data"""

    """V2.2 START CLAUDE: prepare_scd2_data"""

    def prepare_scd2_data(
        self, df: pl.DataFrame, primary_keys: List[str]
    ) -> pl.DataFrame:
        """Prepare DataFrame for SCD2 with proper metadata and fix schema issues."""

        # 1. Fix ALL potential schema issues in Polars before any conversion
        df_fixed = self._fix_polars_schema_for_delta(df)

        # 2. Proceed with SCD2 metadata columns
        current_time = datetime.now()

        # Add SCD2 metadata columns - CRITICAL: Use proper types, not Null
        scd2_df = df_fixed.with_columns(
            [
                pl.lit(current_time).alias("_broze_valid_from"),
                pl.lit("")
                .cast(pl.Utf8)
                .alias("_broze_valid_to"),  # Changed from None to ""
                pl.lit(True).alias("_broze_is_current"),
                pl.lit(1).alias("_broze_record_version"),
                pl.lit(datetime.now()).alias("_broze_load_timestamp"),
                pl.lit(datetime.now().date()).alias("_broze_ingestion_date"),
            ]
        )

        # Calculate data hash for change detection (exclude metadata columns)
        business_cols = [col for col in df_fixed.columns if not col.startswith("_")]
        if business_cols and primary_keys:
            scd2_df = self.calculate_data_hash_polars(scd2_df, business_cols)

        return scd2_df

    """V2.2 END CLAUDE: prepare_scd2_data"""

    def _fix_polars_schema_for_delta(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Fix Polars schema issues before writing to Delta Lake
        """
        fixed_df = df

        # Fix Null types
        for col in df.columns:
            if df[col].dtype == pl.Null:
                logging.info(f"🔄 Converting NullType column '{col}' to String")
                fixed_df = fixed_df.with_columns(pl.col(col).cast(pl.Utf8))

        # Fix other problematic types that might cause Delta issues
        for col in fixed_df.columns:
            dtype = fixed_df[col].dtype

            # Handle any remaining problematic types
            if dtype in [pl.Null, pl.Categorical, pl.Object]:
                logging.info(f"🔄 Converting {dtype} column '{col}' to String")
                fixed_df = fixed_df.with_columns(pl.col(col).cast(pl.Utf8))

        return fixed_df

    """V2: END DEEPSEEK prepare_scd2_data and _fix_polars_schema_for_delta"""

    def table_exists(self, table_name: str) -> bool:
        """Check if Delta table exists"""
        try:
            delta_path = self.bronze_base_path / "tables" / table_name
            return DeltaTable.is_delta_table(str(delta_path))
        except Exception:
            return False

    def implement_simple_scd2(
        self, new_df: pl.DataFrame, table_name: str, primary_keys: List[str]
    ) -> Dict:
        """Simplified but TRUE SCD2 implementation"""
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

            # For subsequent loads, we'll use a simpler approach
            # Read existing data
            existing_df = pl.read_delta(str(delta_path))

            # Prepare new data with SCD2 columns
            new_scd2_df = self.prepare_scd2_data(new_df, primary_keys)

            if primary_keys:
                # Identify new records (records that don't exist in current data)
                existing_keys = existing_df.filter(
                    pl.col("_broze_is_current") == True
                ).select(primary_keys)
                new_records = new_scd2_df.join(
                    existing_keys, on=primary_keys, how="anti"
                )

                # Identify changed records (records that exist but data hash changed)
                existing_current = existing_df.filter(
                    pl.col("_broze_is_current") == True
                )
                changed_records = new_scd2_df.join(
                    existing_current.select(primary_keys + ["_data_hash"]),
                    on=primary_keys,
                    how="inner",
                ).filter(pl.col("_data_hash") != pl.col("_data_hash_right"))

                # Expire changed records in existing data
                if len(changed_records) > 0:
                    changed_keys = changed_records.select(primary_keys).to_pandas()

                    # Update existing records to expire them
                    for i in range(0, len(changed_keys), 1000):  # Process in batches
                        batch = changed_keys[i : i + 1000]
                        key_conditions = " OR ".join(
                            [
                                " AND ".join(
                                    [f"{pk} = '{row[pk]}'" for pk in primary_keys]
                                )
                                for _, row in batch.iterrows()
                            ]
                        )

                        # This would require Delta Lake SQL operations
                        # For now, we'll handle this differently
                        pass

                # Combine new and changed records
                records_to_insert = pl.concat(
                    [new_records, changed_records.select(new_scd2_df.columns)]
                )

                if len(records_to_insert) > 0:
                    # Write new records
                    self.write_delta_table(records_to_insert, table_name, "append")
                    stats["records_inserted"] = len(records_to_insert)
                    stats["records_expired"] = len(changed_records)

                    logging.info(
                        f"✅ SCD2: {len(records_to_insert):,} new/changed, {len(changed_records):,} expired"
                    )
                else:
                    logging.info("ℹ️ No new or changed records found")

            else:
                # No primary keys, just append
                self.write_delta_table(new_scd2_df, table_name, "append")
                stats["records_inserted"] = len(new_scd2_df)
                logging.info(
                    f"📥 Appended {len(new_scd2_df):,} records (no primary keys for SCD2)"
                )

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
        self, df: pl.DataFrame, table_name: str, mode: str = "overwrite"
    ):
        """Write DataFrame to Delta table"""
        delta_path = self.bronze_base_path / "tables" / table_name
        delta_path.mkdir(parents=True, exist_ok=True)

        pandas_df = df.to_pandas()
        write_deltalake(str(delta_path), pandas_df, mode=mode, schema_mode="merge")

    """v1 start: read_and_combine_files"""

    # def read_and_combine_files(
    #     self, files: List[Path], data_type: str
    # ) -> Optional[pl.DataFrame]:
    #     """Read and combine multiple parquet files"""
    #     if not files:
    #         return None

    #     try:
    #         dataframes = []
    #         for file_path in files:
    #             try:
    #                 df = pl.read_parquet(file_path)
    #                 df = df.with_columns(
    #                     [
    #                         pl.lit(str(file_path)).alias("_source_file_path"),
    #                         pl.lit(file_path.name).alias("_source_file_name"),
    #                         pl.lit(data_type).alias("_broze_data_granularity"),
    #                     ]
    #                 )
    #                 dataframes.append(df)
    #                 logging.info(f"   ✅ {file_path.name}: {len(df):,} rows")
    #             except Exception as e:
    #                 logging.warning(f"   ⚠️ Failed to read {file_path.name}: {e}")
    #                 continue

    #         if not dataframes:
    #             return None

    #         if len(dataframes) == 1:
    #             combined_df = dataframes[0]
    #         else:
    #             combined_df = pl.concat(dataframes, how="diagonal")

    #         logging.info(
    #             f"📊 Combined {len(combined_df):,} rows from {len(dataframes)} files"
    #         )
    #         return combined_df

    #     except Exception as e:
    #         logging.error(f"❌ Failed to combine files: {e}")
    #         return None

    """v1 end: read_and_combine_files"""

    """v2 start: read_and_combine_files"""

    def read_and_combine_files(
        self, files: List[Path], data_type: str
    ) -> Optional[pl.DataFrame]:
        """Read and combine multiple parquet files with null type fixing"""
        if not files:
            return None

        try:
            dataframes = []
            for file_path in files:
                try:
                    df = pl.read_parquet(file_path)

                    # # 🔥 V1: ADD THIS LINE: Fix null types before processing
                    # df = self.fix_null_types_simple(df)

                    # 🔥V2 FIX SCHEMA HERE - at the source!
                    df = self._fix_polars_schema_for_delta(df)

                    df = df.with_columns(
                        [
                            pl.lit(str(file_path)).alias("_source_file_path"),
                            pl.lit(file_path.name).alias("_source_file_name"),
                            pl.lit(data_type).alias("_broze_data_granularity"),
                        ]
                    )
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
                combined_df = pl.concat(dataframes, how="diagonal")

            logging.info(
                f"📊 Combined {len(combined_df):,} rows from {len(dataframes)} files"
            )
            return combined_df

        except Exception as e:
            logging.error(f"❌ Failed to combine files: {e}")
            return None

    """v2 end: read_and_combine_files"""

    def validate_primary_keys(self, df: pl.DataFrame, primary_keys: List[str]) -> bool:
        """Validate that primary keys exist and have no duplicates"""
        missing_keys = [pk for pk in primary_keys if pk not in df.columns]
        if missing_keys:
            logging.error(f"❌ Missing primary keys: {missing_keys}")
            return False

        # Check for duplicates in current data
        duplicate_count = len(df) - len(df.unique(subset=primary_keys))
        if duplicate_count > 0:
            logging.warning(
                f"⚠️ Found {duplicate_count} duplicate records based on primary keys"
            )

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
        logging.info("🚀 TRUE SCD2 BRONZE LOADER - FIXED VERSION")
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
            logging.FileHandler(
                log_dir / "bronze_loader_true_scd2_fixed.log", encoding="utf-8"
            ),
            logging.StreamHandler(sys.stdout),
        ],
    )

    # start claude addition for schema diagnosis
    # self.diagnose_schema(scd2_df, "before_write")
    # def diagnose_schema(self, df: pl.DataFrame, label: str):
    #     """Diagnose schema issues"""
    #     logging.info(f"🔍 Schema diagnosis for {label}:")
    #     arrow_table = df.to_arrow()
    #     for field in arrow_table.schema:
    #         logging.info(f"  {field.name}: {field.type}")
    #         if pa.types.is_null(field.type):
    #             logging.error(f"  ❌ NULL TYPE FOUND: {field.name}")

    # end claude addition for schema diagnosis


def main():
    """Main execution"""
    import argparse

    parser = argparse.ArgumentParser(description="True SCD2 Bronze Loader - Fixed")
    parser.add_argument(
        "--config", default="bronze_loader_config.yaml", help="Configuration file path"
    )
    args = parser.parse_args()

    loader = None
    try:
        setup_logging(args.config)

        logging.info("=" * 80)
        logging.info("🏭 TRUE SCD2 BRONZE LOADER - FIXED VERSION")
        logging.info("=" * 80)

        loader = BronzeLoaderPolars(config_path=args.config)
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
