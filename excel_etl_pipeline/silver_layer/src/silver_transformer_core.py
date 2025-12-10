# src/silver_transformer_core.py
"""
Enhanced Silver Layer Transformer
Main orchestrator for Bronze → Silver transformation using Polars, Delta Lake, and PostgreSQL
"""
import polars as pl
from deltalake import DeltaTable, write_deltalake
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging
import yaml
import json
from uuid import uuid4

from incremental_loader import IncrementalLoader
from data_quality_validator import DataQualityValidator
from metadata_handler import MetadataHandler
from column_standardizer import ColumnStandardizer, DataTransformer
from reconciliation_engine import ReconciliationEngine

logger = logging.getLogger(__name__)


class SilverTransformerCore:
    """Enhanced Silver Layer Transformer"""

    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.bronze_base = Path(self.config["paths"]["bronze_base"])
        self.silver_delta_base = Path(self.config["paths"]["silver_delta_base"])
        self.checkpoint_base = Path(self.config["paths"]["checkpoint_base"])
        self.reports_base = Path(self.config["paths"]["reports_base"])

        # Initialize components
        self.incremental_loader = IncrementalLoader(self.checkpoint_base)
        self.data_validator = DataQualityValidator(
            "config/data_quality_rules.yaml",
            strict_mode=self.config["data_quality"]["strict_mode"],
        )
        self.metadata_handler = MetadataHandler(self.config)
        self.column_standardizer = ColumnStandardizer("config/column_mapping.yaml")
        self.data_transformer = DataTransformer()
        self.reconciliation_engine = ReconciliationEngine()

        # PostgreSQL connection
        self.pg_conn = self._connect_postgresql()
        self.pg_schema = self.config["database"]["postgresql"]["schema"]

        # Create directories
        self.silver_delta_base.mkdir(parents=True, exist_ok=True)
        self.reports_base.mkdir(parents=True, exist_ok=True)

        # Process tracking
        self.process_id = (
            f"process_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"
        )
        self.processing_stats = {
            "start_time": datetime.now().isoformat(),
            "tables_processed": [],
            "summary": {"successful": 0, "failed": 0, "skipped": 0},
        }

        logger.info(
            f"🚀 Silver Transformer initialized (process_id: {self.process_id})"
        )

    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML"""
        with open(config_path, "r") as f:
            return yaml.safe_load(f)

    def _connect_postgresql(self) -> psycopg2.extensions.connection:
        """Connect to PostgreSQL"""
        db_config = self.config["database"]["postgresql"]
        try:
            conn = psycopg2.connect(
                host=db_config["host"],
                port=db_config["port"],
                database=db_config["database"],
                user=db_config["user"],
                password=db_config["password"],
            )
            logger.info("✅ Connected to PostgreSQL")
            return conn
        except Exception as e:
            logger.error(f"❌ Failed to connect to PostgreSQL: {e}")
            raise

    def run_complete_pipeline(self) -> bool:
        """Run complete silver layer pipeline"""

        logger.info("=" * 80)
        logger.info("🚀 STARTING ENHANCED SILVER LAYER PIPELINE")
        logger.info("=" * 80)

        try:
            # Step 1: Process Invoice Headers
            logger.info("\n📄 STEP 1: Processing Invoice Headers...")
            invoice_headers = self._process_invoice_headers()

            # Step 2: Process Invoice Details
            logger.info("\n📊 STEP 2: Processing Invoice Details...")
            invoice_details = self._process_invoice_details()

            # Step 3: Reconcile Invoices
            logger.info("\n🔄 STEP 3: Reconciling Invoice Headers with Details...")
            if invoice_headers is not None and invoice_details is not None:
                self._reconcile_invoices(invoice_headers, invoice_details)

            # Step 4: Process Credit Note Headers (if exist)
            logger.info("\n📝 STEP 4: Processing Credit Note Headers...")
            credit_headers = self._process_credit_note_headers()

            # Step 5: Process Credit Note Details (if exist)
            logger.info("\n📋 STEP 5: Processing Credit Note Details...")
            credit_details = self._process_credit_note_details()

            # Step 6: Reconcile Credit Notes with Invoices
            logger.info("\n🔗 STEP 6: Reconciling Credit Notes with Invoices...")
            if credit_headers is not None and invoice_headers is not None:
                self._reconcile_credit_notes(credit_headers, invoice_headers)

            # Step 7: Update Customer Dimension
            logger.info("\n👥 STEP 7: Updating Customer Dimension...")
            self._update_customer_dimension(invoice_headers, credit_headers)

            # Step 8: Generate Final Report
            logger.info("\n📊 STEP 8: Generating Final Report...")
            self._generate_final_report()

            logger.info("\n" + "=" * 80)
            logger.info("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
            logger.info("=" * 80)

            return True

        except Exception as e:
            logger.error(f"\n❌ PIPELINE FAILED: {e}")
            import traceback

            logger.error(traceback.format_exc())
            return False
        finally:
            if self.pg_conn:
                self.pg_conn.close()
                logger.info("🔌 PostgreSQL connection closed")

    # ==================== INVOICE HEADER PROCESSING ====================

    def _process_invoice_headers(self) -> Optional[pl.DataFrame]:
        """Process invoice summary/header data"""

        processing_start = datetime.now()
        table_name = "summarized_cash_invoice"

        try:
            # Find bronze table
            bronze_tables = self._discover_bronze_tables("*summarized*cash*invoice*")
            if not bronze_tables:
                logger.warning("⚠️  No summarized invoice table found")
                return None

            bronze_path = bronze_tables[0]
            logger.info(f"📥 Reading from: {bronze_path}")

            # Read incrementally
            df = self.incremental_loader.read_incremental(
                bronze_path, table_name, timestamp_column="_raw_processed_at"
            )

            if df.is_empty():
                logger.info("⏭️  No new records to process")
                return None

            logger.info(f"📊 Processing {len(df):,} records...")

            # Clean and transform
            df = self._clean_strings(df)
            df = self._parse_dates(df, "TRN_DATE", "transaction_date")
            df = self._parse_amounts(df)

            # Separate positive invoices from credit notes/negatives
            df_invoices = df.filter(
                (pl.col("AMOUNT") > 0)
                & (~pl.col("DOCUMENT_NUMBER").str.starts_with("CN"))
            )

            df_credits = df.filter(
                (pl.col("AMOUNT") <= 0)
                | (pl.col("DOCUMENT_NUMBER").str.starts_with("CN"))
            )

            logger.info(f"   ✓ Invoices: {len(df_invoices):,}")
            logger.info(f"   ✓ Credits/Negatives: {len(df_credits):,}")

            # Process invoices
            if not df_invoices.is_empty():
                df_invoices = self._transform_invoice_headers(
                    df_invoices, processing_start
                )

                # Validate
                passed, validation_report = self.data_validator.validate(
                    df_invoices, "invoice_header"
                )

                self._log_validation_results(validation_report, "invoice_header")

                # Write to destinations
                self._write_to_delta(df_invoices, "sales/cash_invoices/summarized")
                self._write_to_postgresql(df_invoices, "cash_invoice_summary")

                # Update checkpoint
                max_timestamp = df.select(pl.col("_raw_processed_at").max()).item()
                self.incremental_loader.update_checkpoint(
                    table_name, max_timestamp, len(df)
                )

            # Process credit notes separately (if any)
            if not df_credits.is_empty():
                logger.info(
                    f"📝 Processing {len(df_credits):,} credit notes/negatives..."
                )
                self._process_generated_credit_notes_summary(
                    df_credits, processing_start
                )

            self.processing_stats["summary"]["successful"] += 1
            return df_invoices

        except Exception as e:
            logger.error(f"❌ Failed to process invoice headers: {e}")
            self.processing_stats["summary"]["failed"] += 1
            return None

    def _transform_invoice_headers(
        self, df: pl.DataFrame, processing_start: datetime
    ) -> pl.DataFrame:
        """Apply transformations to invoice headers"""

        # Optimize bronze metadata
        df = self.metadata_handler.optimize_bronze_metadata(df)

        # Standardize column names
        df = self.column_standardizer.standardize_columns(df, "invoice_header")

        # Add calculated fields
        df = self.data_transformer.add_calculated_fields_invoice_header(df)

        # Add quality flags
        df = self.data_validator.add_quality_flags(df)

        # Add silver metadata
        df = self.metadata_handler.add_silver_metadata(
            df, self.process_id, processing_start
        )

        # Calculate quality score
        df = self.metadata_handler.calculate_quality_score(df)

        return df

    # ==================== INVOICE DETAIL PROCESSING ====================

    def _process_invoice_details(self) -> Optional[pl.DataFrame]:
        """Process invoice line item details"""

        processing_start = datetime.now()
        table_name = "detailed_cash_invoice"

        try:
            # Find bronze table
            bronze_tables = self._discover_bronze_tables("*detailed*cash*invoice*")
            if not bronze_tables:
                logger.warning("⚠️  No detailed invoice table found")
                return None

            bronze_path = bronze_tables[0]
            logger.info(f"📥 Reading from: {bronze_path}")

            # Read incrementally
            df = self.incremental_loader.read_incremental(
                bronze_path, table_name, timestamp_column="_raw_processed_at"
            )

            if df.is_empty():
                logger.info("⏭️  No new records to process")
                return None

            logger.info(f"📊 Processing {len(df):,} records...")

            # Clean and transform
            df = self._clean_strings(df)
            df = self._parse_dates(df, "TRN_DATE", "transaction_date")
            df = self._parse_amounts(df)

            # Separate positive invoices from credit notes
            df_invoices = df.filter(
                (pl.col("AMOUNT") > 0)
                & (~pl.col("DOCUMENT_NUMBER").str.starts_with("CN"))
            )

            df_credits = df.filter(
                (pl.col("AMOUNT") <= 0)
                | (pl.col("DOCUMENT_NUMBER").str.starts_with("CN"))
            )

            logger.info(f"   ✓ Invoice lines: {len(df_invoices):,}")
            logger.info(f"   ✓ Credit lines: {len(df_credits):,}")

            # Process invoice details
            if not df_invoices.is_empty():
                df_invoices = self._transform_invoice_details(
                    df_invoices, processing_start
                )

                # Validate
                passed, validation_report = self.data_validator.validate(
                    df_invoices, "invoice_detail"
                )

                self._log_validation_results(validation_report, "invoice_detail")

                # Write to destinations
                self._write_to_delta(df_invoices, "sales/cash_invoices/detailed")
                self._write_to_postgresql(df_invoices, "cash_invoice_detail")

                # Update checkpoint
                max_timestamp = df.select(pl.col("_raw_processed_at").max()).item()
                self.incremental_loader.update_checkpoint(
                    table_name, max_timestamp, len(df)
                )

            # Process credit note details
            if not df_credits.is_empty():
                logger.info(f"📝 Processing {len(df_credits):,} credit note lines...")
                self._process_generated_credit_notes_detail(
                    df_credits, processing_start
                )

            self.processing_stats["summary"]["successful"] += 1
            return df_invoices

        except Exception as e:
            logger.error(f"❌ Failed to process invoice details: {e}")
            self.processing_stats["summary"]["failed"] += 1
            return None

    def _transform_invoice_details(
        self, df: pl.DataFrame, processing_start: datetime
    ) -> pl.DataFrame:
        """Apply transformations to invoice details"""

        # Optimize bronze metadata
        df = self.metadata_handler.optimize_bronze_metadata(df)

        # Standardize column names
        df = self.column_standardizer.standardize_columns(df, "invoice_detail")

        # Add calculated fields
        df = self.data_transformer.add_calculated_fields_invoice_detail(df)

        # Add quality flags
        df = self.data_validator.add_quality_flags(df)

        # Add silver metadata
        df = self.metadata_handler.add_silver_metadata(
            df, self.process_id, processing_start
        )

        # Calculate quality score
        df = self.metadata_handler.calculate_quality_score(df)

        return df

    # ==================== CREDIT NOTE PROCESSING ====================

    def _process_credit_note_headers(self) -> Optional[pl.DataFrame]:
        """Process credit note headers"""

        processing_start = datetime.now()
        table_name = "credit_note_summary"

        try:
            # Find bronze table
            bronze_tables = self._discover_bronze_tables("*credit*note*summary*")
            if not bronze_tables:
                logger.warning("⚠️  No credit note summary table found (may not exist)")
                return None

            bronze_path = bronze_tables[0]
            logger.info(f"📥 Reading from: {bronze_path}")

            # Read incrementally
            df = self.incremental_loader.read_incremental(
                bronze_path, table_name, timestamp_column="_raw_processed_at"
            )

            if df.is_empty():
                logger.info("⏭️  No new credit notes to process")
                return None

            logger.info(f"📊 Processing {len(df):,} credit notes...")

            # Transform
            df = self._transform_credit_note_headers(df, processing_start)

            # Validate
            passed, validation_report = self.data_validator.validate(
                df, "credit_note_header"
            )

            self._log_validation_results(validation_report, "credit_note_header")

            # Write to destinations
            self._write_to_delta(df, "sales/credit_notes/summarized")
            self._write_to_postgresql(df, "credit_note_summary")

            # Update checkpoint
            max_timestamp = df.select(pl.col("_raw_processed_at").max()).item()
            self.incremental_loader.update_checkpoint(
                table_name, max_timestamp, len(df)
            )

            self.processing_stats["summary"]["successful"] += 1
            return df

        except Exception as e:
            logger.warning(f"⚠️  Credit note processing: {e}")
            return None

    def _process_credit_note_details(self) -> Optional[pl.DataFrame]:
        """Process credit note details"""

        processing_start = datetime.now()
        table_name = "credit_note_detail"

        try:
            # Find bronze table
            bronze_tables = self._discover_bronze_tables("*credit*note*detail*")
            if not bronze_tables:
                logger.warning("⚠️  No credit note detail table found (may not exist)")
                return None

            bronze_path = bronze_tables[0]
            logger.info(f"📥 Reading from: {bronze_path}")

            # Read incrementally
            df = self.incremental_loader.read_incremental(
                bronze_path, table_name, timestamp_column="_raw_processed_at"
            )

            if df.is_empty():
                logger.info("⏭️  No new credit note details to process")
                return None

            logger.info(f"📊 Processing {len(df):,} credit note lines...")

            # Transform
            df = self._transform_credit_note_details(df, processing_start)

            # Write to destinations
            self._write_to_delta(df, "sales/credit_notes/detailed")
            self._write_to_postgresql(df, "credit_note_detail")

            # Update checkpoint
            max_timestamp = df.select(pl.col("_raw_processed_at").max()).item()
            self.incremental_loader.update_checkpoint(
                table_name, max_timestamp, len(df)
            )

            return df

        except Exception as e:
            logger.warning(f"⚠️  Credit note detail processing: {e}")
            return None

    def _transform_credit_note_headers(
        self, df: pl.DataFrame, processing_start: datetime
    ) -> pl.DataFrame:
        """Transform credit note headers"""

        # Clean and parse
        df = self._clean_strings(df)
        df = self._parse_dates(df, "TRN_DATE", "transaction_date")
        df = self._parse_amounts(df)

        # Ensure negative amounts
        if "AMOUNT" in df.columns:
            df = df.with_columns([(pl.col("AMOUNT").abs() * -1).alias("AMOUNT")])

        # Optimize bronze metadata
        df = self.metadata_handler.optimize_bronze_metadata(df)

        # Standardize column names
        df = self.column_standardizer.standardize_columns(df, "credit_note_header")

        # Add calculated fields
        df = self.data_transformer.add_calculated_fields_credit_note_header(df)

        # Add silver metadata
        df = self.metadata_handler.add_silver_metadata(
            df, self.process_id, processing_start
        )

        return df

    def _transform_credit_note_details(
        self, df: pl.DataFrame, processing_start: datetime
    ) -> pl.DataFrame:
        """Transform credit note details"""

        # Clean and parse
        df = self._clean_strings(df)
        df = self._parse_dates(df, "TRN_DATE", "transaction_date")
        df = self._parse_amounts(df)

        # Ensure negative amounts
        if "AMOUNT_VALUE" in df.columns:
            df = df.with_columns(
                [(pl.col("AMOUNT_VALUE").abs() * -1).alias("AMOUNT_VALUE")]
            )

        # Optimize bronze metadata
        df = self.metadata_handler.optimize_bronze_metadata(df)

        # Standardize column names
        df = self.column_standardizer.standardize_columns(df, "credit_note_detail")

        # Add silver metadata
        df = self.metadata_handler.add_silver_metadata(
            df, self.process_id, processing_start
        )

        return df

    def _process_generated_credit_notes_summary(
        self, df: pl.DataFrame, processing_start: datetime
    ):
        """Process credit notes generated from negative/zero invoice amounts"""

        logger.info(f"🔄 Processing generated credit notes (summary)...")

        # Transform to credit note format
        df = df.with_columns([(pl.col("AMOUNT").abs() * -1).alias("AMOUNT")])

        df = self._transform_credit_note_headers(df, processing_start)

        # Write to a separate table for tracking
        self._write_to_postgresql(df, "generated_credit_note_summary")

        logger.info(
            f"✅ Generated {len(df):,} credit note records from negative amounts"
        )

    def _process_generated_credit_notes_detail(
        self, df: pl.DataFrame, processing_start: datetime
    ):
        """Process credit note details generated from negative line items"""

        logger.info(f"🔄 Processing generated credit notes (detail)...")

        # Transform to credit note format
        df = df.with_columns([(pl.col("AMOUNT").abs() * -1).alias("AMOUNT")])

        df = self._transform_credit_note_details(df, processing_start)

        # Write to a separate table for tracking
        self._write_to_postgresql(df, "generated_credit_note_detail")

        logger.info(
            f"✅ Generated {len(df):,} credit note line records from negative amounts"
        )

    # ==================== RECONCILIATION ====================

    def _reconcile_invoices(self, headers: pl.DataFrame, details: pl.DataFrame):
        """Reconcile invoice headers with details"""

        try:
            reconciliation_df, summary = (
                self.reconciliation_engine.reconcile_invoice_header_detail(
                    headers, details
                )
            )

            # Generate report for PostgreSQL
            report_df = self.reconciliation_engine.generate_reconciliation_report(
                reconciliation_df, "INVOICE"
            )

            # Write to PostgreSQL
            self._write_to_postgresql(report_df, "reconciliation_report")

            # Save detailed report to file
            report_path = (
                self.reports_base / f"invoice_reconciliation_{self.process_id}.json"
            )
            with open(report_path, "w") as f:
                json.dump(summary, f, indent=2)

            logger.info(f"📄 Reconciliation report saved: {report_path}")

        except Exception as e:
            logger.error(f"❌ Invoice reconciliation failed: {e}")

    def _reconcile_credit_notes(
        self, credit_notes: pl.DataFrame, invoices: pl.DataFrame
    ):
        """Reconcile credit notes with invoices"""

        try:
            reconciliation_df, summary = (
                self.reconciliation_engine.reconcile_credit_notes_with_invoices(
                    credit_notes, invoices
                )
            )

            # Generate report for PostgreSQL
            report_df = self.reconciliation_engine.generate_reconciliation_report(
                reconciliation_df, "CREDIT_NOTE"
            )

            # Write to PostgreSQL
            self._write_to_postgresql(report_df, "reconciliation_report")

            # Save detailed report to file
            report_path = (
                self.reports_base / f"credit_note_reconciliation_{self.process_id}.json"
            )
            with open(report_path, "w") as f:
                json.dump(summary, f, indent=2)

            logger.info(f"📄 Credit note reconciliation report saved: {report_path}")

        except Exception as e:
            logger.error(f"❌ Credit note reconciliation failed: {e}")

    # ==================== CUSTOMER DIMENSION (SCD Type 2) ====================

    def _update_customer_dimension(
        self,
        invoice_headers: Optional[pl.DataFrame],
        credit_headers: Optional[pl.DataFrame],
    ):
        """Update customer dimension with SCD Type 2 logic"""

        try:
            logger.info("👥 Updating customer dimension...")

            # Collect unique customers from all sources
            customers = []

            if invoice_headers is not None:
                inv_customers = invoice_headers.select(
                    [
                        "customer_code",
                        "customer_name",
                        "contact_telephone",
                        "contact_email",
                    ]
                ).unique()
                customers.append(inv_customers)

            if credit_headers is not None:
                cred_customers = credit_headers.select(
                    [
                        "customer_code",
                        "customer_name",
                        "contact_telephone",
                        "contact_email",
                    ]
                ).unique()
                customers.append(cred_customers)

            if not customers:
                logger.warning("⚠️  No customer data to process")
                return

            # Combine and deduplicate
            all_customers = pl.concat(customers).unique(subset=["customer_code"])

            logger.info(f"📊 Processing {len(all_customers):,} unique customers...")

            # Read existing dimension
            existing_query = f"""
                SELECT customer_code, customer_name, contact_telephone, contact_email
                FROM {self.pg_schema}.customer_dimension
                WHERE _is_current = TRUE
            """

            cursor = self.pg_conn.cursor()
            cursor.execute(existing_query)
            existing_rows = cursor.fetchall()

            if existing_rows:
                existing_df = pl.DataFrame(
                    {
                        "customer_code": [r[0] for r in existing_rows],
                        "customer_name": [r[1] for r in existing_rows],
                        "contact_telephone": [r[2] for r in existing_rows],
                        "contact_email": [r[3] for r in existing_rows],
                    }
                )

                # Detect changes
                changes = all_customers.join(
                    existing_df, on="customer_code", how="left", suffix="_existing"
                )

                # Find new and changed customers
                new_customers = changes.filter(
                    pl.col("customer_name_existing").is_null()
                )
                changed_customers = changes.filter(
                    pl.col("customer_name_existing").is_not_null()
                    & (
                        (pl.col("customer_name") != pl.col("customer_name_existing"))
                        | (
                            pl.col("contact_telephone")
                            != pl.col("contact_telephone_existing")
                        )
                        | (pl.col("contact_email") != pl.col("contact_email_existing"))
                    )
                )

                # Close existing records for changed customers
                if not changed_customers.is_empty():
                    changed_codes = (
                        changed_customers.select("customer_code").to_series().to_list()
                    )
                    update_query = f"""
                        UPDATE {self.pg_schema}.customer_dimension
                        SET _valid_to = CURRENT_TIMESTAMP, _is_current = FALSE
                        WHERE customer_code = ANY(%s) AND _is_current = TRUE
                    """
                    cursor.execute(update_query, (changed_codes,))
                    logger.info(
                        f"   ✓ Closed {len(changed_codes)} changed customer records"
                    )

                # Insert new versions
                to_insert = pl.concat([new_customers, changed_customers])
            else:
                # First load - all are new
                to_insert = all_customers

            if not to_insert.is_empty():
                # Prepare insert data
                insert_data = [
                    (
                        row["customer_code"],
                        row["customer_name"],
                        row["contact_telephone"],
                        row["contact_email"],
                        datetime.now(),
                        None,
                        True,
                        1,
                    )
                    for row in to_insert.to_dicts()
                ]

                insert_query = f"""
                    INSERT INTO {self.pg_schema}.customer_dimension
                    (customer_code, customer_name, contact_telephone, contact_email,
                     _valid_from, _valid_to, _is_current, _record_version)
                    VALUES %s
                """

                execute_values(cursor, insert_query, insert_data)
                self.pg_conn.commit()

                logger.info(
                    f"✅ Inserted/Updated {len(to_insert):,} customer dimension records"
                )
            else:
                logger.info("✅ Customer dimension is up to date")

        except Exception as e:
            logger.error(f"❌ Failed to update customer dimension: {e}")
            self.pg_conn.rollback()

    # ==================== DATA WRITING ====================

    def _write_to_delta(self, df: pl.DataFrame, delta_path: str):
        """Write DataFrame to Delta Lake"""

        full_path = self.silver_delta_base / delta_path
        full_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            # Convert to PyArrow
            arrow_table = df.to_arrow()

            # Write to Delta
            if full_path.exists():
                write_deltalake(str(full_path), arrow_table, mode="append")
                logger.info(f"✅ Appended {len(df):,} records to Delta: {delta_path}")
            else:
                write_deltalake(str(full_path), arrow_table, mode="overwrite")
                logger.info(
                    f"✅ Created Delta table with {len(df):,} records: {delta_path}"
                )

        except Exception as e:
            logger.error(f"❌ Failed to write to Delta {delta_path}: {e}")
            raise

    def _write_to_postgresql(self, df: pl.DataFrame, table_name: str):
        """Write DataFrame to PostgreSQL"""

        full_table = f"{self.pg_schema}.{table_name}"

        try:
            # Convert to list of tuples
            columns = df.columns
            data = [tuple(row) for row in df.iter_rows()]

            # Generate INSERT query
            placeholders = ", ".join(["%s"] * len(columns))
            insert_query = f"""
                INSERT INTO {full_table} ({', '.join(columns)})
                VALUES ({placeholders})
                ON CONFLICT DO NOTHING
            """

            cursor = self.pg_conn.cursor()
            execute_values(cursor, insert_query, data)
            self.pg_conn.commit()

            logger.info(f"✅ Wrote {len(df):,} records to PostgreSQL: {table_name}")

        except Exception as e:
            logger.error(f"❌ Failed to write to PostgreSQL {table_name}: {e}")
            self.pg_conn.rollback()
            raise
