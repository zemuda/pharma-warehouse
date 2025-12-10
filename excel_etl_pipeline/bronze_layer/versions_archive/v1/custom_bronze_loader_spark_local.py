from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import os
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Any
import logging
import sys
import psycopg2
from psycopg2.extras import RealDictCursor


class BronzeLayerSparkLoader:
    def __init__(
        self,
        bronze_base_path: str = "/opt/etl/bronze",
        source_data_path: str = "/opt/etl/output",
        postgres_config: Optional[Dict[str, Any]] = None,
        spark_configs: Optional[Dict[str, Any]] = None,
    ):
        self.bronze_base_path = Path(bronze_base_path).absolute()
        self.source_data_path = Path(source_data_path).absolute()

        # Ensure directories exist
        self.bronze_base_path.mkdir(parents=True, exist_ok=True)

        self.postgres_config = postgres_config or {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": os.getenv("DB_PORT", "5432"),
            "database": os.getenv("DB_NAME", "pharma_warehouse"),
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD", "1234"),
            "schema": os.getenv("DB_BRONZE_LAYER_SCHEMA", "bronze_layer"),
        }

        self.features = ["sales", "purchases", "inventory", "master_data"]
        self.spark_configs = spark_configs or {}

        # Initialize Spark session with Delta Lake for local storage
        self.spark = self._create_spark_session()

        # Initialize PostgreSQL logging
        self._init_postgres_logging()

        # PostgreSQL connection pool
        self.pg_conn = None

    # def _create_spark_session(self) -> SparkSession:
    #  def _get_postgres_connection(self):
