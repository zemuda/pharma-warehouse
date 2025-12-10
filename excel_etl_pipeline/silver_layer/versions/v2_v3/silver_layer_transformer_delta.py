# silver_layer_transformer_delta_complete.py
import yaml
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import gc
import sys

# Set environment variable to skip CPU check
import os

os.environ["POLARS_SKIP_CPU_CHECK"] = "1"

import polars as pl
from deltalake import DeltaTable
import psycopg2
from psycopg2.extras import RealDictCursor


class SilverLayerTransformer:
    """
    Complete Silver Layer Transformer for Pharma Warehouse
    Processes Bronze Delta tables and loads cleaned data to PostgreSQL
    """

    def __init__(self, config_path: str = "silver_config_complete.yaml"):
        self.config = self._load_config(config_path)
        self._setup_paths()
        self._setup_logging()
        self.db_conn = None

        logging.info("✅ Silver Layer Transformer initialized")

    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file"""
        try:
            with open(config_path, "r") as f:
                config = yaml.safe_load(f)
            logging.info(f"✅ Loaded config from {config_path}")
            return config
        except Exception as e:
            logging.error(f"❌ Failed to load config: {e}")
            raise

    def _setup_paths(self):
        """Setup directory paths"""
        self.bronze_base_path = Path(self.config["paths"]["bronze_base_path"])
        self.silver_base_path = Path(self.config["paths"]["silver_base_path"])
        self.log_dir = Path(self.config["paths"]["log_directory"])

        # Create directories
        for directory in [self.silver_base_path, self.log_dir]:
            directory.mkdir(parents=True, exist_ok=True)

    def _setup_logging(self):
        """Setup logging configuration"""
        log_config = self.config["logging"]

        logging.basicConfig(
            level=getattr(logging, log_config["level"]),
            format=log_config["format"],
            handlers=[
                logging.FileHandler(self.log_dir / "silver_transformer.log"),
                logging.StreamHandler(sys.stdout),
            ],
        )

    def get_postgres_connection(self):
        """Get PostgreSQL connection"""
        if self.db_conn is None:
            pg_config = self.config["postgresql"]
            self.db_conn = psycopg2.connect(
                host=pg_config["host"],
                port=pg_config["port"],
                database=pg_config["database"],
                user=pg_config["user"],
                password=pg_config["password"],
            )
        return self.db_conn

    def execute_sql(self, query: str, params: Tuple = None):
        """Execute SQL query"""
        conn = self.get_postgres_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(query, params or ())
                if query.strip().upper().startswith("SELECT"):
                    return cur.fetchall()
                conn.commit()
        except Exception as e:
            conn.rollback()
            logging.error(f"❌ SQL execution failed: {e}\nQuery: {query}")
            raise

    def init_silver_schema(self):
        """Initialize Silver layer schema in PostgreSQL"""
        schema = self.config["postgresql"]["schema"]

        # Create schema
        self.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        # Create process logging table
        self.execute_sql(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.silver_process_log (
                process_id SERIAL PRIMARY KEY,
                table_name VARCHAR(200),
                records_processed INTEGER,
                records_loaded INTEGER,
                process_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(50),
                error_message TEXT
            )
        """
        )

        logging.info("✅ Silver layer schema initialized")

    def get_bronze_tables_to_process(self) -> List[Dict]:
        """Get list of Bronze tables to process"""
        tables = []

        for feature in self.config["data_sources"]["features"]:
            if not feature.get("enabled", True):
                continue

            for subfeature in feature.get("subfeatures", []):
                if not subfeature.get("enabled", True):
                    continue

                for data_type in subfeature.get("data_types", []):
                    if data_type.get("enabled", True):
                        table_name = f"bronze_{feature['name']}_{subfeature['name']}_{data_type['name']}"
                        tables.append(
                            {
                                "bronze_table": table_name,
                                "silver_table": f"silver_{feature['name']}_{subfeature['name']}_{data_type['name']}",
                                "feature": feature["name"],
                                "subfeature": subfeature["name"],
                                "data_type": data_type["name"],
                                "primary_keys": data_type.get("primary_keys", []),
                                "cleaning_rules": data_type.get("cleaning_rules", {}),
                            }
                        )

        return tables

    def read_bronze_delta_table(self, table_name: str) -> Optional[pl.DataFrame]:
        """Read data from Bronze Delta table"""
        try:
            delta_path = self.bronze_base_path / "tables" / table_name
            if not DeltaTable.is_delta_table(str(delta_path)):
                logging.warning(f"⚠️ Delta table not found: {table_name}")
                return None

            dt = DeltaTable(str(delta_path))
            df = pl.from_arrow(dt.to_pyarrow_table())
            logging.info(f"✅ Read {len(df):,} rows from {table_name}")
            return df

        except Exception as e:
            logging.error(f"❌ Failed to read Delta table {table_name}: {e}")
            return None

    def apply_cleaning_rules(
        self, df: pl.DataFrame, cleaning_rules: Dict
    ) -> pl.DataFrame:
        """Apply cleaning rules to DataFrame"""
        if df.is_empty():
            return df

        cleaned_df = df

        # String cleaning - trim whitespace
        if cleaning_rules.get("trim_whitespace", True):
            string_columns = [col for col in df.columns if df[col].dtype == pl.Utf8]
            for col in string_columns:
                cleaned_df = cleaned_df.with_columns(
                    [pl.col(col).str.strip_chars().alias(f"{col}_cleaned")]
                )

        # Handle null values
        null_rules = cleaning_rules.get("null_handling", {})
        for col, default_value in null_rules.items():
            if col in df.columns:
                if default_value == "drop":
                    cleaned_df = cleaned_df.filter(pl.col(col).is_not_null())
                else:
                    cleaned_df = cleaned_df.with_columns(
                        [pl.col(col).fill_null(default_value)]
                    )

        # Data type conversions
        type_conversions = cleaning_rules.get("type_conversions", {})
        for col, target_type in type_conversions.items():
            if col in df.columns:
                if target_type == "numeric":
                    cleaned_df = cleaned_df.with_columns(
                        [pl.col(col).cast(pl.Float64).alias(f"{col}_numeric")]
                    )
                elif target_type == "date":
                    cleaned_df = cleaned_df.with_columns(
                        [
                            pl.col(col)
                            .str.strptime(pl.Date, "%Y-%m-%d")
                            .alias(f"{col}_date")
                        ]
                    )

        return cleaned_df

    def validate_data_quality(self, df: pl.DataFrame, table_config: Dict) -> Dict:
        """Validate data quality and return metrics"""
        if df.is_empty():
            return {"total_rows": 0, "valid_rows": 0, "quality_score": 100.0}

        total_rows = len(df)
        primary_keys = table_config.get("primary_keys", [])

        # Check for duplicates
        duplicate_count = 0
        if primary_keys:
            duplicate_count = total_rows - len(df.unique(subset=primary_keys))

        # Check for nulls in critical columns
        null_checks = {}
        critical_columns = table_config.get("critical_columns", [])
        for col in critical_columns:
            if col in df.columns:
                null_count = df[col].is_null().sum()
                null_checks[col] = null_count

        # Calculate quality score
        valid_rows = total_rows - duplicate_count
        quality_score = (valid_rows / total_rows * 100) if total_rows > 0 else 100.0

        return {
            "total_rows": total_rows,
            "valid_rows": valid_rows,
            "duplicate_count": duplicate_count,
            "null_checks": null_checks,
            "quality_score": round(quality_score, 2),
        }

    def create_silver_table(self, table_name: str, df: pl.DataFrame):
        """Create Silver table in PostgreSQL"""
        schema = self.config["postgresql"]["schema"]

        if df.is_empty():
            logging.warning(f"⚠️ No data to create table {table_name}")
            return

        # Generate CREATE TABLE statement
        columns = []
        for col_name, col_type in zip(df.columns, df.dtypes):
            pg_type = self._polars_to_postgres_type(col_type)
            columns.append(f'"{col_name}" {pg_type}')

        create_sql = f'CREATE TABLE IF NOT EXISTS {schema}."{table_name}" (\n'
        create_sql += ",\n".join(columns)
        create_sql += "\n)"

        self.execute_sql(create_sql)
        logging.info(f"✅ Created/verified table: {table_name}")

    def _polars_to_postgres_type(self, polars_type):
        """Convert Polars data type to PostgreSQL type"""
        type_mapping = {
            pl.Int8: "SMALLINT",
            pl.Int16: "SMALLINT",
            pl.Int32: "INTEGER",
            pl.Int64: "BIGINT",
            pl.UInt8: "SMALLINT",
            pl.UInt16: "INTEGER",
            pl.UInt32: "BIGINT",
            pl.UInt64: "BIGINT",
            pl.Float32: "REAL",
            pl.Float64: "DOUBLE PRECISION",
            pl.Utf8: "TEXT",
            pl.Boolean: "BOOLEAN",
            pl.Date: "DATE",
            pl.Datetime: "TIMESTAMP",
        }

        return type_mapping.get(polars_type, "TEXT")

    def load_to_postgres(self, table_name: str, df: pl.DataFrame):
        """Load DataFrame to PostgreSQL"""
        if df.is_empty():
            logging.warning(f"⚠️ No data to load for {table_name}")
            return 0

        schema = self.config["postgresql"]["schema"]

        # Convert to pandas for easier loading
        pandas_df = df.to_pandas()

        conn = self.get_postgres_connection()
        try:
            with conn.cursor() as cur:
                # Create temporary table for upsert
                temp_table = (
                    f"temp_{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                )

                # Create temp table
                columns = ", ".join([f'"{col}"' for col in pandas_df.columns])
                cur.execute(
                    f'CREATE TEMP TABLE {temp_table} (LIKE {schema}."{table_name}")'
                )

                # Load data to temp table
                from io import StringIO

                output = StringIO()
                pandas_df.to_csv(output, sep="\t", header=False, index=False)
                output.seek(0)
                cur.copy_from(output, temp_table, null="")

                # Upsert from temp to main table
                conflict_cols = [
                    "_source_file_path",
                    "_source_file_name",
                ]  # Adjust based on your needs
                update_cols = [
                    col for col in pandas_df.columns if col not in conflict_cols
                ]

                set_clause = ", ".join(
                    [f'"{col}" = EXCLUDED."{col}"' for col in update_cols]
                )
                conflict_clause = ", ".join([f'"{col}"' for col in conflict_cols])

                upsert_sql = f"""
                    INSERT INTO {schema}."{table_name}" ({columns})
                    SELECT {columns} FROM {temp_table}
                    ON CONFLICT ({conflict_clause}) 
                    DO UPDATE SET {set_clause}
                """

                cur.execute(upsert_sql)
                conn.commit()

                row_count = len(pandas_df)
                logging.info(f"✅ Loaded {row_count:,} rows to {table_name}")
                return row_count

        except Exception as e:
            conn.rollback()
            logging.error(f"❌ Failed to load data to {table_name}: {e}")
            return 0

    def process_table(self, table_config: Dict) -> Dict:
        """Process a single Bronze table"""
        start_time = datetime.now()

        try:
            bronze_table = table_config["bronze_table"]
            silver_table = table_config["silver_table"]

            logging.info(f"\n🔄 Processing: {bronze_table} -> {silver_table}")

            # Read from Bronze Delta
            df = self.read_bronze_delta_table(bronze_table)
            if df is None or df.is_empty():
                return {
                    "table_name": silver_table,
                    "status": "NO_DATA",
                    "records_processed": 0,
                    "records_loaded": 0,
                    "duration_seconds": 0,
                }

            # Apply cleaning rules
            cleaned_df = self.apply_cleaning_rules(
                df, table_config.get("cleaning_rules", {})
            )

            # Validate data quality
            quality_metrics = self.validate_data_quality(cleaned_df, table_config)

            # Create Silver table
            self.create_silver_table(silver_table, cleaned_df)

            # Load to PostgreSQL
            records_loaded = self.load_to_postgres(silver_table, cleaned_df)

            duration = (datetime.now() - start_time).total_seconds()

            result = {
                "table_name": silver_table,
                "status": "COMPLETED",
                "records_processed": len(cleaned_df),
                "records_loaded": records_loaded,
                "quality_score": quality_metrics["quality_score"],
                "duplicate_count": quality_metrics["duplicate_count"],
                "duration_seconds": round(duration, 2),
            }

            logging.info(
                f"✅ Completed: {silver_table} - {records_loaded:,} rows, Quality: {quality_metrics['quality_score']}%"
            )
            return result

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logging.error(f"❌ Failed to process {table_config['silver_table']}: {e}")
            return {
                "table_name": table_config["silver_table"],
                "status": "FAILED",
                "error": str(e),
                "duration_seconds": round(duration, 2),
            }

    def run_silver_pipeline(self):
        """Run complete Silver layer pipeline"""
        logging.info("\n" + "=" * 80)
        logging.info("🏭 SILVER LAYER PIPELINE - STARTED")
        logging.info("=" * 80)

        # Initialize schema
        self.init_silver_schema()

        # Get tables to process
        tables_to_process = self.get_bronze_tables_to_process()
        logging.info(f"📊 Found {len(tables_to_process)} tables to process")

        results = []
        for table_config in tables_to_process:
            result = self.process_table(table_config)
            results.append(result)

            # Log to database
            self.log_processing_result(result)

        # Print summary
        self.print_summary(results)

        logging.info("\n" + "=" * 80)
        logging.info("🏭 SILVER LAYER PIPELINE - COMPLETED")
        logging.info("=" * 80)

        return results

    def log_processing_result(self, result: Dict):
        """Log processing result to database"""
        schema = self.config["postgresql"]["schema"]

        self.execute_sql(
            f"""
            INSERT INTO {schema}.silver_process_log 
            (table_name, records_processed, records_loaded, status, error_message)
            VALUES (%s, %s, %s, %s, %s)
        """,
            (
                result["table_name"],
                result.get("records_processed", 0),
                result.get("records_loaded", 0),
                result["status"],
                result.get("error", ""),
            ),
        )

    def print_summary(self, results: List[Dict]):
        """Print processing summary"""
        successful = [r for r in results if r["status"] == "COMPLETED"]
        failed = [r for r in results if r["status"] == "FAILED"]
        no_data = [r for r in results if r["status"] == "NO_DATA"]

        total_processed = sum(r.get("records_processed", 0) for r in results)
        total_loaded = sum(r.get("records_loaded", 0) for r in results)

        print("\n" + "=" * 80)
        print("📊 SILVER LAYER PROCESSING SUMMARY")
        print("=" * 80)
        print(f"✅ Successful: {len(successful)} tables")
        print(f"❌ Failed: {len(failed)} tables")
        print(f"ℹ️  No Data: {len(no_data)} tables")
        print(f"📥 Total Processed: {total_processed:,} records")
        print(f"📤 Total Loaded: {total_loaded:,} records")

        if successful:
            print("\n✅ SUCCESSFUL TABLES:")
            for result in successful:
                print(
                    f"   - {result['table_name']}: {result['records_loaded']:,} rows, Quality: {result.get('quality_score', 'N/A')}%"
                )

        if failed:
            print("\n❌ FAILED TABLES:")
            for result in failed:
                print(
                    f"   - {result['table_name']}: {result.get('error', 'Unknown error')}"
                )

    def cleanup(self):
        """Cleanup resources"""
        if self.db_conn:
            self.db_conn.close()
            logging.info("✅ Database connection closed")


def main():
    """Main execution function"""
    import argparse

    parser = argparse.ArgumentParser(description="Silver Layer Transformer")
    parser.add_argument(
        "--config",
        default="silver_config_complete.yaml",
        help="Configuration file path",
    )
    args = parser.parse_args()

    transformer = None
    try:
        transformer = SilverLayerTransformer(config_path=args.config)
        results = transformer.run_silver_pipeline()

        failed = [r for r in results if r["status"] == "FAILED"]
        return len(failed) == 0

    except Exception as e:
        logging.error(f"❌ Silver pipeline failed: {e}")
        return False
    finally:
        if transformer:
            transformer.cleanup()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
