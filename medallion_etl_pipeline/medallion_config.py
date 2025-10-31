# config.py
"""
Configuration management for the Medallion Architecture Pipeline
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pathlib import Path
import os


@dataclass
class DatabaseConfig:
    """PostgreSQL database configuration"""
    host: str = os.getenv("POSTGRES_HOST", "localhost")
    port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    database: str = os.getenv("POSTGRES_DB", "etl_metadata")
    user: str = os.getenv("POSTGRES_USER", "etl_user")
    password: str = os.getenv("POSTGRES_PASSWORD", "etl_password")
    schema: str = "etl_logs"
    
    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
    
    @property
    def sqlalchemy_url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class SparkConfig:
    """Spark configuration"""
    app_name: str = "MedallionArchitecturePipeline"
    master: str = "local[*]"
    driver_memory: str = "4g"
    executor_memory: str = "4g"
    shuffle_partitions: int = 200
    delta_log_retention: str = "interval 30 days"
    delta_deleted_file_retention: str = "interval 7 days"
    
    @property
    def spark_configs(self) -> Dict[str, str]:
        return {
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.driver.memory": self.driver_memory,
            "spark.executor.memory": self.executor_memory,
            "spark.sql.shuffle.partitions": str(self.shuffle_partitions),
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.databricks.delta.retentionDurationCheck.enabled": "false",
            "spark.databricks.delta.properties.defaults.logRetentionDuration": self.delta_log_retention,
            "spark.databricks.delta.properties.defaults.deletedFileRetentionDuration": self.delta_deleted_file_retention,
        }


@dataclass
class PathConfig:
    """File system path configuration"""
    base_input_path: str = r"C:\etl\data\input"
    base_output_path: str = r"C:\etl\data\lakehouse"
    checkpoint_path: str = r"C:\etl\checkpoints"
    temp_path: str = r"C:\etl\temp"
    
    @property
    def bronze_path(self) -> str:
        return str(Path(self.base_output_path) / "bronze")
    
    @property
    def silver_path(self) -> str:
        return str(Path(self.base_output_path) / "silver")
    
    @property
    def gold_path(self) -> str:
        return str(Path(self.base_output_path) / "gold")
    
    def get_bronze_table_path(self, table_name: str) -> str:
        return str(Path(self.bronze_path) / table_name)
    
    def get_silver_table_path(self, table_name: str) -> str:
        return str(Path(self.silver_path) / table_name)
    
    def get_gold_table_path(self, table_name: str) -> str:
        return str(Path(self.gold_path) / table_name)


@dataclass
class FeatureConfig:
    """Feature-specific configuration"""
    name: str
    source_patterns: List[str]
    partition_columns: List[str] = field(default_factory=lambda: ["year", "month"])
    scd2_enabled: bool = True
    data_quality_checks: List[str] = field(default_factory=list)


@dataclass
class PipelineConfig:
    """Main pipeline configuration"""
    # Sub-configurations
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)
    paths: PathConfig = field(default_factory=PathConfig)
    
    # Features to process
    features: Dict[str, FeatureConfig] = field(default_factory=lambda: {
        "sales_cash_invoices_detailed": FeatureConfig(
            name="sales_cash_invoices_detailed",
            source_patterns=["sales/cash_invoices/detailed/**/*.xls*"],
            partition_columns=["transaction_year", "transaction_month"],
            scd2_enabled=True,
            data_quality_checks=["null_check", "duplicate_check", "amount_check"]
        ),
        "sales_cash_invoices_summarized": FeatureConfig(
            name="sales_cash_invoices_summarized",
            source_patterns=["sales/cash_invoices/summarized/**/*.xls*"],
            partition_columns=["transaction_year", "transaction_month"],
            scd2_enabled=True,
            data_quality_checks=["null_check", "duplicate_check", "amount_check"]
        ),
        "sales_cash_sales_detailed": FeatureConfig(
            name="sales_cash_sales_detailed",
            source_patterns=["sales/cash_sales/detailed/**/*.xls*"],
            partition_columns=["transaction_year", "transaction_month"],
            scd2_enabled=True,
            data_quality_checks=["null_check", "duplicate_check", "amount_check"]
        ),
        "sales_cash_sales_summarized": FeatureConfig(
            name="sales_cash_sales_summarized",
            source_patterns=["sales/cash_sales/summarized/**/*.xls*"],
            partition_columns=["transaction_year", "transaction_month"],
            scd2_enabled=True,
            data_quality_checks=["null_check", "duplicate_check", "amount_check"]
        ),
        "sales_credit_notes_detailed": FeatureConfig(
            name="sales_credit_notes_detailed",
            source_patterns=["sales/credit_notes/detailed/**/*.xls*"],
            partition_columns=["transaction_year", "transaction_month"],
            scd2_enabled=True,
            data_quality_checks=["null_check", "duplicate_check", "credit_amount_check"]
        ),
        "sales_credit_notes_summarized": FeatureConfig(
            name="sales_credit_notes_summarized",
            source_patterns=["sales/credit_notes/summarized/**/*.xls*"],
            partition_columns=["transaction_year", "transaction_month"],
            scd2_enabled=True,
            data_quality_checks=["null_check", "duplicate_check", "credit_amount_check"]
        ),
    })
    
    # Processing settings
    batch_size: int = 1000
    max_files_per_batch: int = 100
    enable_schema_evolution: bool = True
    enable_data_quality_checks: bool = True
    enable_scd2: bool = True
    
    # Retry settings
    max_retries: int = 3
    retry_delay_seconds: int = 60


# Singleton instance
config = PipelineConfig()
