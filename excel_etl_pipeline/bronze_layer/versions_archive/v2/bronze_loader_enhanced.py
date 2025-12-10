# bronze_loader_enhanced.py
"""
Enhanced Production Bronze Loader with:
- Configuration file support
- Data validation rules
- Anomaly detection
- Auto-healing
- Notifications (Email/Slack)
- Retry logic
- Quarantine/DLQ
"""

import yaml
import sys
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import re
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import requests
import json

# Import the base production loader
from bronze_loader_production import ProductionBronzeLoader
from pyspark.sql import DataFrame
from pyspark.sql.functions import *


class EnhancedBronzeLoader(ProductionBronzeLoader):
    def __init__(self, config_path: str = "bronze_loader_config.yaml"):
        """Initialize enhanced loader with configuration file"""
        # Load configuration
        with open(config_path, "r") as f:
            self.full_config = yaml.safe_load(f)

        # Initialize base loader
        super().__init__(
            bronze_base_path=self.full_config["paths"]["bronze_base_path"],
            source_data_path=self.full_config["paths"]["source_data_path"],
            postgres_config=self.full_config["postgresql"],
            spark_configs=self._build_spark_configs(),
            enable_scd2=self.full_config["features"]["enable_scd2"],
            enable_quality_columns=self.full_config["features"][
                "enable_quality_columns"
            ],
            enable_zorder=self.full_config["features"]["enable_zorder"],
        )

        # Additional paths
        self.checkpoint_dir = Path(self.full_config["paths"]["checkpoint_directory"])
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

        if self.full_config["error_handling"]["quarantine_enabled"]:
            self.quarantine_dir = Path(
                self.full_config["error_handling"]["quarantine_path"]
            )
            self.quarantine_dir.mkdir(parents=True, exist_ok=True)

        if self.full_config["error_handling"]["dlq_enabled"]:
            self.dlq_dir = Path(self.full_config["error_handling"]["dlq_path"])
            self.dlq_dir.mkdir(parents=True, exist_ok=True)

        # Enabled features from data_sources
        self.features = [
            ds["name"]
            for ds in self.full_config["data_sources"]["features"]
            if ds["enabled"]
        ]

        # Validation and anomaly detection flags
        self.enable_validation = self.full_config["features"]["enable_validation"]
        self.enable_anomaly_detection = self.full_config["features"][
            "enable_anomaly_detection"
        ]
        self.enable_auto_healing = self.full_config["features"]["enable_auto_healing"]

        # Retry settings
        self.retry_config = self.full_config["error_handling"]

        logging.info("✅ Enhanced Bronze Loader initialized with configuration")

    def _build_spark_configs(self) -> Dict:
        """Build Spark configurations from config file"""
        spark_cfg = self.full_config["spark"]

        configs = {
            "spark.driver.memory": spark_cfg["driver_memory"],
            "spark.executor.memory": spark_cfg["executor_memory"],
            "spark.sql.shuffle.partitions": str(spark_cfg["shuffle_partitions"]),
            "spark.default.parallelism": str(spark_cfg["default_parallelism"]),
            "spark.sql.files.maxPartitionBytes": spark_cfg["max_partition_bytes"],
        }

        # Add additional configs
        if "additional_configs" in spark_cfg:
            for key, value in spark_cfg["additional_configs"].items():
                configs[key] = str(value)

        return configs

    def validate_dataframe(
        self, df: DataFrame, table_name: str
    ) -> Tuple[bool, List[str], DataFrame]:
        """
        Validate DataFrame against configured rules
        Returns: (is_valid, violations, cleaned_df)
        """
        if not self.enable_validation:
            return True, [], df

        violations = []
        cleaned_df = df

        # Get validation rules
        global_rules = self.full_config["validation_rules"].get("global", [])
        table_rules = (
            self.full_config["validation_rules"]
            .get("tables", {})
            .get(table_name.replace("bronze_", ""), [])
        )

        all_rules = global_rules + table_rules

        for rule in all_rules:
            rule_type = rule["rule_type"]
            severity = rule.get("severity", "warning")

            try:
                if rule_type == "null_check":
                    # Check null percentage per column
                    max_null_pct = rule.get("max_null_percentage", 10.0)
                    total_rows = df.count()

                    for col_name in df.columns:
                        if not col_name.startswith("_"):
                            null_count = df.filter(col(col_name).isNull()).count()
                            null_pct = (
                                (null_count / total_rows * 100) if total_rows > 0 else 0
                            )

                            if null_pct > max_null_pct:
                                violation = f"Column '{col_name}' has {null_pct:.2f}% nulls (threshold: {max_null_pct}%)"
                                violations.append(f"[{severity.upper()}] {violation}")

                                # Auto-heal if enabled
                                if self.enable_auto_healing and severity != "error":
                                    logging.info(
                                        f"Auto-healing: Filling nulls in {col_name}"
                                    )
                                    # Simple strategy: fill with default values
                                    if df.schema[col_name].dataType.simpleString() in [
                                        "string"
                                    ]:
                                        cleaned_df = cleaned_df.fillna(
                                            {col_name: "UNKNOWN"}
                                        )
                                    elif df.schema[
                                        col_name
                                    ].dataType.simpleString() in [
                                        "int",
                                        "bigint",
                                        "double",
                                    ]:
                                        cleaned_df = cleaned_df.fillna({col_name: 0})

                elif rule_type == "range_check":
                    # Check if values are within range
                    columns = rule.get("columns", [])
                    min_val = rule.get("min_value")
                    max_val = rule.get("max_value")

                    for col_name in columns:
                        if col_name in df.columns:
                            if min_val is not None:
                                out_of_range = df.filter(
                                    col(col_name) < min_val
                                ).count()
                                if out_of_range > 0:
                                    violation = f"Column '{col_name}' has {out_of_range} values below {min_val}"
                                    violations.append(
                                        f"[{severity.upper()}] {violation}"
                                    )

                            if max_val is not None:
                                out_of_range = df.filter(
                                    col(col_name) > max_val
                                ).count()
                                if out_of_range > 0:
                                    violation = f"Column '{col_name}' has {out_of_range} values above {max_val}"
                                    violations.append(
                                        f"[{severity.upper()}] {violation}"
                                    )

                elif rule_type == "format_check":
                    # Check if values match pattern
                    columns = rule.get("columns", [])
                    pattern = rule.get("pattern", "")

                    for col_name in columns:
                        if col_name in df.columns:
                            invalid_count = df.filter(
                                ~col(col_name).rlike(pattern)
                                & col(col_name).isNotNull()
                            ).count()

                            if invalid_count > 0:
                                violation = f"Column '{col_name}' has {invalid_count} values not matching pattern"
                                violations.append(f"[{severity.upper()}] {violation}")

                elif rule_type == "duplicate_check":
                    # Check for duplicates
                    business_cols = [c for c in df.columns if not c.startswith("_")]
                    if business_cols:
                        dup_count = (
                            df.groupBy(business_cols)
                            .count()
                            .filter(col("count") > 1)
                            .count()
                        )
                        if dup_count > 0:
                            violation = f"Found {dup_count} duplicate records"
                            violations.append(f"[{severity.upper()}] {violation}")

            except Exception as e:
                logging.error(f"Error applying validation rule {rule_type}: {e}")

        # Determine if validation passed
        error_violations = [v for v in violations if v.startswith("[ERROR]")]
        is_valid = len(error_violations) == 0

        return is_valid, violations, cleaned_df

    def detect_anomalies(
        self, df: DataFrame, table_name: str, operation_id: int
    ) -> List[str]:
        """Detect anomalies in the data"""
        if not self.enable_anomaly_detection:
            return []

        anomalies = []
        anomaly_config = self.full_config["anomaly_detection"]

        try:
            total_records = df.count()

            # Volume anomaly detection
            if anomaly_config["volume"]["enabled"]:
                min_records = anomaly_config["volume"]["expected_records_min"]
                max_records = anomaly_config["volume"]["expected_records_max"]

                if total_records < min_records:
                    anomalies.append(
                        f"Low volume: {total_records} records (expected >= {min_records})"
                    )
                elif total_records > max_records:
                    anomalies.append(
                        f"High volume: {total_records} records (expected <= {max_records})"
                    )

            # Statistical anomaly detection
            if anomaly_config["statistical"]["enabled"]:
                method = anomaly_config["statistical"]["method"]
                threshold = anomaly_config["statistical"]["threshold"]
                columns_to_check = anomaly_config["statistical"]["columns_to_check"]

                for col_name in columns_to_check:
                    if col_name in df.columns:
                        # Calculate mean and stddev
                        stats = df.select(
                            mean(col(col_name)).alias("mean"),
                            stddev(col(col_name)).alias("stddev"),
                        ).collect()[0]

                        if stats["stddev"] and stats["stddev"] > 0:
                            # Find outliers using Z-score
                            outlier_count = df.filter(
                                abs((col(col_name) - stats["mean"]) / stats["stddev"])
                                > threshold
                            ).count()

                            if outlier_count > 0:
                                outlier_pct = outlier_count / total_records * 100
                                anomalies.append(
                                    f"Column '{col_name}': {outlier_count} outliers ({outlier_pct:.2f}%)"
                                )

            # Log anomalies to PostgreSQL
            if anomalies:
                for anomaly in anomalies:
                    log_data = {
                        "table_name": table_name,
                        "operation_id": operation_id,
                        "anomaly_type": "statistical",
                        "anomaly_description": anomaly,
                        "detected_at": datetime.now(),
                    }
                    # Would log to a new anomaly_detection table
                    logging.warning(f"⚠️  Anomaly detected: {anomaly}")

        except Exception as e:
            logging.error(f"Error detecting anomalies: {e}")

        return anomalies

    def send_email_notification(self, subject: str, body: str):
        """Send email notification"""
        email_config = self.full_config["notifications"]["email"]

        if not email_config["enabled"]:
            return

        try:
            msg = MIMEMultipart()
            msg["From"] = email_config["from_address"]
            msg["To"] = ", ".join(email_config["to_addresses"])
            msg["Subject"] = subject

            msg.attach(MIMEText(body, "plain"))

            server = smtplib.SMTP(email_config["smtp_host"], email_config["smtp_port"])
            server.starttls()
            server.login(email_config["smtp_user"], email_config["smtp_password"])
            server.send_message(msg)
            server.quit()

            logging.info(f"✅ Email notification sent: {subject}")
        except Exception as e:
            logging.error(f"Failed to send email notification: {e}")

    def send_slack_notification(self, message: str, level: str = "info"):
        """Send Slack notification"""
        slack_config = self.full_config["notifications"]["slack"]

        if not slack_config["enabled"]:
            return

        try:
            color_map = {"info": "#36a64f", "warning": "#ff9800", "error": "#f44336"}

            payload = {
                "channel": slack_config["channel"],
                "attachments": [
                    {
                        "color": color_map.get(level, "#36a64f"),
                        "title": f"Bronze Layer - {level.upper()}",
                        "text": message,
                        "footer": "Bronze Layer Loader",
                        "ts": int(datetime.now().timestamp()),
                    }
                ],
            }

            response = requests.post(
                slack_config["webhook_url"],
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"},
            )

            if response.status_code == 200:
                logging.info("✅ Slack notification sent")
            else:
                logging.error(f"Slack notification failed: {response.status_code}")

        except Exception as e:
            logging.error(f"Failed to send Slack notification: {e}")

    def notify(self, event_type: str, message: str, level: str = "info"):
        """Send notifications based on configuration"""
        notifications_config = self.full_config["notifications"]

        if not notifications_config["enabled"]:
            return

        # Email notifications
        email_config = notifications_config["email"]
        if email_config["enabled"] and event_type in email_config["notify_on"]:
            self.send_email_notification(
                subject=f"Bronze Layer: {event_type.replace('_', ' ').title()}",
                body=message,
            )

        # Slack notifications
        slack_config = notifications_config["slack"]
        if slack_config["enabled"] and event_type in slack_config["notify_on"]:
            self.send_slack_notification(message, level)

    def quarantine_file(self, file_path: Path, reason: str):
        """Move failed file to quarantine"""
        if not self.retry_config["quarantine_enabled"]:
            return

        try:
            import shutil

            quarantine_path = self.quarantine_dir / file_path.name
            shutil.copy2(file_path, quarantine_path)

            # Write reason file
            reason_file = quarantine_path.with_suffix(".reason.txt")
            with open(reason_file, "w") as f:
                f.write(f"Quarantined at: {datetime.now()}\n")
                f.write(f"Reason: {reason}\n")

            logging.info(f"📦 File quarantined: {file_path.name}")
        except Exception as e:
            logging.error(f"Failed to quarantine file: {e}")

    def move_to_dlq(self, file_path: Path, error: str):
        """Move file to Dead Letter Queue"""
        if not self.retry_config["dlq_enabled"]:
            return

        try:
            import shutil

            dlq_path = self.dlq_dir / file_path.name
            shutil.copy2(file_path, dlq_path)

            # Write error file
            error_file = dlq_path.with_suffix(".error.txt")
            with open(error_file, "w") as f:
                f.write(f"Failed at: {datetime.now()}\n")
                f.write(f"Error: {error}\n")

            logging.error(f"💀 File moved to DLQ: {file_path.name}")
        except Exception as e:
            logging.error(f"Failed to move file to DLQ: {e}")

    def load_file_with_retry(
        self, file_path: Path, operation_id: int
    ) -> Tuple[bool, int]:
        """Load file with retry logic"""
        max_retries = (
            self.retry_config["max_retries"]
            if self.retry_config["retry_enabled"]
            else 1
        )
        retry_delay = self.retry_config["retry_delay_seconds"]
        exponential_backoff = self.retry_config["exponential_backoff"]

        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    delay = (
                        retry_delay * (2**attempt)
                        if exponential_backoff
                        else retry_delay
                    )
                    logging.info(
                        f"⏳ Retry {attempt}/{max_retries} after {delay}s: {file_path.name}"
                    )
                    import time

                    time.sleep(delay)

                # Calculate file hash
                file_hash = self._calculate_file_hash(file_path)

                # Check if already processed
                if self._is_file_already_processed(file_path, file_hash):
                    logging.info(f"⏭️  Skipping already processed: {file_path.name}")
                    return True, 0

                table_name = self.get_table_name_from_path(file_path)
                logging.info(f"📄 Processing: {file_path.name} -> {table_name}")

                # Read parquet file
                df = self.spark.read.parquet(str(file_path))
                initial_count = df.count()

                # Validate data
                is_valid, violations, cleaned_df = self.validate_dataframe(
                    df, table_name
                )

                if violations:
                    for violation in violations:
                        logging.warning(f"  {violation}")

                if not is_valid:
                    error_msg = f"Validation failed: {'; '.join(violations)}"
                    if attempt == max_retries - 1:
                        self.quarantine_file(file_path, error_msg)
                        self.notify(
                            "quality_breach",
                            f"File {file_path.name} failed validation:\n"
                            + "\n".join(violations),
                            "warning",
                        )
                    raise Exception(error_msg)

                # Use cleaned dataframe if auto-healing was applied
                df = cleaned_df

                # Enhance with metadata
                enhanced_df = self._enhance_with_metadata(df, file_path, operation_id)

                # Apply quality checks
                quality_df = self._apply_data_quality_checks(enhanced_df, table_name)

                # Detect anomalies
                anomalies = self.detect_anomalies(quality_df, table_name, operation_id)
                if anomalies:
                    logging.warning(f"⚠️  Anomalies detected in {file_path.name}:")
                    for anomaly in anomalies:
                        logging.warning(f"  - {anomaly}")

                # Calculate quality score
                if self.enable_quality_columns:
                    quality_score = quality_df.agg(avg("_dq_quality_score")).collect()[
                        0
                    ][0]
                    quality_score = float(quality_score) if quality_score else 0.0
                else:
                    quality_score = 1.0

                # Check quality threshold
                min_quality = self.full_config["quality_thresholds"][
                    "min_quality_score"
                ]
                if quality_score < min_quality:
                    warning_msg = f"Quality score {quality_score:.2f} below threshold {min_quality}"
                    logging.warning(f"⚠️  {warning_msg}")

                    if self.full_config["quality_thresholds"]["fail_on_quality_breach"]:
                        raise Exception(warning_msg)

                    if self.full_config["quality_thresholds"][
                        "alert_on_quality_breach"
                    ]:
                        self.notify(
                            "quality_breach",
                            f"File {file_path.name}: {warning_msg}",
                            "warning",
                        )

                # Write to Delta
                success, write_stats = self._write_to_delta_table(
                    quality_df, table_name, operation_id
                )

                if success:
                    # Log success
                    file_stats = {
                        "operation_id": operation_id,
                        "file_path": str(file_path),
                        "file_hash": file_hash,
                        "file_size": file_path.stat().st_size,
                        "file_modified_time": datetime.fromtimestamp(
                            file_path.stat().st_mtime
                        ),
                        "records_count": initial_count,
                        "processed_at": datetime.now(),
                        "status": "SUCCESS",
                        "data_quality_score": quality_score,
                        "processing_time_seconds": 0,  # Calculate properly
                    }
                    self._log_to_postgres("file_processing", file_stats)

                    # Log table stats
                    table_path = self.bronze_base_path / "tables" / table_name
                    self._log_delta_table_stats(table_name, table_path)

                    # Log batch distribution
                    if self.enable_quality_columns:
                        self._log_batch_distribution(table_name, operation_id)

                    logging.info(
                        f"  ✓ {initial_count:,} rows, quality: {quality_score:.2f}"
                    )
                    return True, initial_count
                else:
                    raise Exception(write_stats.get("error", "Unknown error"))

            except Exception as e:
                logging.error(f"  ✗ Attempt {attempt + 1} failed: {e}")

                if attempt == max_retries - 1:
                    # Final attempt failed
                    self.move_to_dlq(file_path, str(e))
                    self.notify(
                        "failure",
                        f"File {file_path.name} failed after {max_retries} attempts:\n{str(e)}",
                        "error",
                    )
                    return False, 0

        return False, 0

    def load_feature_incrementally(self, feature: str) -> Dict:
        """Override to use retry logic"""
        start_time = datetime.now()

        operation_data = {
            "feature_name": feature,
            "load_path": str(self.source_data_path / feature),
            "started_at": start_time,
            "status": "STARTED",
            "spark_application_id": self.spark.sparkContext.applicationId,
            "scd2_enabled": self.enable_scd2,
            "quality_checks_enabled": self.enable_quality_columns,
        }

        operation_id = self._log_to_postgres("load_operations", operation_data)

        if operation_id is None:
            logging.error(f"Failed to log operation start for {feature}")
            return {
                "feature": feature,
                "status": "FAILED",
                "error": "Failed to log operation",
            }

        try:
            all_files = self.discover_parquet_files(feature)

            if not all_files:
                logging.warning(f"No files found for feature: {feature}")
                self._update_postgres_record(
                    "load_operations",
                    operation_id,
                    {
                        "status": "COMPLETED",
                        "files_processed": 0,
                        "files_skipped": 0,
                        "rows_loaded": 0,
                        "completed_at": datetime.now(),
                        "total_duration_seconds": 0,
                    },
                )
                return {"feature": feature, "status": "NO_FILES", "files_processed": 0}

            files_processed = 0
            files_skipped = 0
            total_rows_loaded = 0
            failed_files = []

            # Process each file with retry
            for file_path in all_files:
                success, rows_loaded = self.load_file_with_retry(
                    file_path, operation_id
                )
                if success:
                    if rows_loaded > 0:
                        files_processed += 1
                        total_rows_loaded += rows_loaded
                    else:
                        files_skipped += 1
                else:
                    failed_files.append(file_path.name)

                    if not self.retry_config["continue_on_error"]:
                        break

            duration = (datetime.now() - start_time).total_seconds()
            status = "COMPLETED" if not failed_files else "PARTIAL_SUCCESS"

            self._update_postgres_record(
                "load_operations",
                operation_id,
                {
                    "status": status,
                    "files_processed": files_processed,
                    "files_skipped": files_skipped,
                    "rows_loaded": total_rows_loaded,
                    "completed_at": datetime.now(),
                    "total_duration_seconds": int(duration),
                    "error_message": (
                        f"Failed files: {', '.join(failed_files)}"
                        if failed_files
                        else None
                    ),
                },
            )

            # Send completion notification
            if status == "COMPLETED":
                self.notify(
                    "completion",
                    f"Feature {feature} completed successfully:\n"
                    f"- Files processed: {files_processed}\n"
                    f"- Rows loaded: {total_rows_loaded:,}\n"
                    f"- Duration: {duration:.2f}s",
                    "info",
                )

            return {
                "feature": feature,
                "files_processed": files_processed,
                "files_skipped": files_skipped,
                "total_files": len(all_files),
                "failed_files": len(failed_files),
                "total_rows_loaded": total_rows_loaded,
                "status": status,
                "duration_seconds": round(duration, 2),
            }

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            self._update_postgres_record(
                "load_operations",
                operation_id,
                {
                    "status": "FAILED",
                    "error_message": str(e)[:500],
                    "completed_at": datetime.now(),
                    "total_duration_seconds": int(duration),
                },
            )

            self.notify("failure", f"Feature {feature} failed: {str(e)}", "error")

            return {"feature": feature, "error": str(e), "status": "FAILED"}


def main():
    """Main execution with configuration file"""
    import argparse

    parser = argparse.ArgumentParser(description="Enhanced Production Bronze Loader")
    parser.add_argument(
        "--config", default="bronze_loader_config.yaml", help="Configuration file path"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Dry run mode (no writes)"
    )

    args = parser.parse_args()

    # Setup logging
    with open(args.config, "r") as f:
        config = yaml.safe_load(f)

    log_config = config["logging"]
    log_dir = Path(config["paths"]["log_directory"])
    log_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=getattr(logging, log_config["level"]),
        format=log_config["format"],
        handlers=[
            logging.FileHandler(log_dir / log_config["file"]["filename"]),
            logging.StreamHandler(sys.stdout),
        ],
    )

    logger = logging.getLogger(__name__)
    loader = None

    try:
        logger.info("=" * 80)
        logger.info("🚀 ENHANCED PRODUCTION BRONZE LOADER")
        logger.info("=" * 80)

        # Initialize loader
        loader = EnhancedBronzeLoader(config_path=args.config)

        # Print enabled features
        loader.print_feature_flags()

        logger.info(f"\n📋 Additional Features:")
        logger.info(f"  {'✅' if loader.enable_validation else '❌'} Data Validation")
        logger.info(
            f"  {'✅' if loader.enable_anomaly_detection else '❌'} Anomaly Detection"
        )
        logger.info(f"  {'✅' if loader.enable_auto_healing else '❌'} Auto-Healing")
        logger.info(
            f"  {'✅' if loader.retry_config['retry_enabled'] else '❌'} Retry Logic"
        )
        logger.info(
            f"  {'✅' if config['notifications']['enabled'] else '❌'} Notifications"
        )

        if args.dry_run:
            logger.info("\n🧪 DRY RUN MODE - No actual writes will be performed")
            return True

        # Load all features
        results = loader.load_all_features()

        # Optimize tables
        loader.optimize_all_tables()

        # Create catalog
        loader.create_bronze_catalog()

        # Verify data
        loader.verify_loaded_data()

        # Print summary
        loader.print_summary_report()

        # Check results
        failed = [r for r in results if r["status"] == "FAILED"]
        if failed:
            logger.error(f"❌ {len(failed)} feature(s) failed")
            return False
        else:
            logger.info("✅ ALL FEATURES LOADED SUCCESSFULLY!")
            return True

    except Exception as e:
        logger.error(f"❌ Load failed: {e}", exc_info=True)
        return False

    finally:
        if loader:
            loader.cleanup()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
