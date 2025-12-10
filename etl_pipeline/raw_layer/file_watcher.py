#!/usr/bin/env python3
"""
File watcher service to trigger pipeline on new Excel files
Logs are saved in PostgreSQL database.
"""

import os
import time
import logging
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import docker
import psycopg2
import uuid
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class PostgreSQLLogger:
    """Logger that saves to PostgreSQL"""

    def __init__(self, db_config=None):
        self.db_config = db_config or {
            "host": "localhost",
            "port": "5432",
            "database": "pharma_warehouse",
            "user": "postgres",
            "password": "1234",
        }
        self.conn = psycopg2.connect(**self.db_config)
        self.conn.autocommit = True
        self._setup_database()

    def _setup_database(self):
        """Setup PostgreSQL database for file watcher logs"""
        with self.conn.cursor() as cur:
            # Create raw_layer schema
            cur.execute("CREATE SCHEMA IF NOT EXISTS raw_layer")

            # Create raw_layer.file_watcher_logs table in raw_layer schema
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_layer.file_watcher_logs (
                    log_id UUID PRIMARY KEY,
                    timestamp TIMESTAMP,
                    level VARCHAR(10),
                    message TEXT,
                    file_path VARCHAR(500),
                    event_type VARCHAR(20),
                    container_name VARCHAR(100),
                    success BOOLEAN,
                    error_message TEXT
                )
            """
            )

    def log_event(self, level: str, message: str, **kwargs):
        """Log event to PostgreSQL"""
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO raw_layer.file_watcher_logs VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """,
                    (
                        str(uuid.uuid4()),
                        datetime.now(),
                        level,
                        message,
                        kwargs.get("file_path"),
                        kwargs.get("event_type"),
                        kwargs.get("container_name"),
                        kwargs.get("success"),
                        kwargs.get("error_message"),
                    ),
                )
        except Exception as e:
            logger.error(f"Failed to log to PostgreSQL: {e}")

    def close(self):
        """Close database connection"""
        if hasattr(self, "conn"):
            self.conn.close()


class ExcelFileHandler(FileSystemEventHandler):
    """Handle Excel file events"""

    def __init__(self, pipeline_container, db_logger):
        self.pipeline_container = pipeline_container
        self.docker_client = docker.from_env()
        self.processing_delay = 30  # Wait 30 seconds before processing
        self.db_logger = db_logger

    def on_created(self, event):
        if not event.is_directory:
            self.handle_file_event(event.src_path, "created")

    def on_modified(self, event):
        if not event.is_directory:
            self.handle_file_event(event.src_path, "modified")

    def handle_file_event(self, file_path, event_type):
        """Handle file events for Excel files"""
        path = Path(file_path)

        # Check if it's an Excel file
        if path.suffix.lower() in [".xlsx", ".xls", ".xlsm", ".xlsb"]:
            logger.info(f"Excel file {event_type}: {file_path}")
            self.db_logger.log_event(
                "INFO",
                f"Excel file {event_type}",
                file_path=file_path,
                event_type=event_type,
                container_name=self.pipeline_container,
            )

            # Wait a bit to ensure file is completely written
            time.sleep(self.processing_delay)

            try:
                # Trigger pipeline processing
                container = self.docker_client.containers.get(self.pipeline_container)

                # Execute pipeline command in the container
                result = container.exec_run(
                    [
                        "python",
                        "excel_parquet_pipeline.py",
                        "--input-dir",
                        "/etl/data/input",
                        "--output-dir",
                        "/etl/data/output",
                        "--state-file",
                        "/etl/state/processing_state.json",
                        "--log-level",
                        "INFO",
                        "--clean-orphaned",
                        "--db-host",
                        "host.docker.internal",
                        "--db-port",
                        "5432",
                        "--db-name",
                        "pharma_warehouse",
                        "--db-user",
                        "postgres",
                        "--db-password",
                        "1234",
                    ]
                )

                if result.exit_code == 0:
                    logger.info(f"Pipeline triggered successfully for {file_path}")
                    self.db_logger.log_event(
                        "INFO",
                        "Pipeline triggered successfully",
                        file_path=file_path,
                        event_type=event_type,
                        container_name=self.pipeline_container,
                        success=True,
                    )
                else:
                    error_msg = result.output.decode()
                    logger.error(f"Pipeline execution failed: {error_msg}")
                    self.db_logger.log_event(
                        "ERROR",
                        "Pipeline execution failed",
                        file_path=file_path,
                        event_type=event_type,
                        container_name=self.pipeline_container,
                        success=False,
                        error_message=error_msg,
                    )

            except Exception as e:
                logger.error(f"Error triggering pipeline: {e}")
                self.db_logger.log_event(
                    "ERROR",
                    "Error triggering pipeline",
                    file_path=file_path,
                    event_type=event_type,
                    container_name=self.pipeline_container,
                    success=False,
                    error_message=str(e),
                )


def main():
    watch_dir = os.getenv("WATCH_DIR", "/etl/data/input")
    pipeline_container = os.getenv("PIPELINE_CONTAINER", "excel-parquet-pipeline")

    # Database configuration from environment variables
    db_config = {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
        "database": os.getenv("DB_NAME", "pharma_warehouse"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "1234"),
    }

    # Create watch directory if it doesn't exist
    Path(watch_dir).mkdir(parents=True, exist_ok=True)

    # Setup PostgreSQL logger
    db_logger = PostgreSQLLogger(db_config)

    logger.info(f"Starting file watcher for directory: {watch_dir}")
    logger.info(f"Pipeline container: {pipeline_container}")
    logger.info(f"Logging to PostgreSQL database: {db_config['database']}")

    # Setup file watcher
    event_handler = ExcelFileHandler(pipeline_container, db_logger)
    observer = Observer()
    observer.schedule(event_handler, watch_dir, recursive=True)

    # Start watching
    observer.start()
    logger.info("File watcher started")
    db_logger.log_event("INFO", "File watcher started")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping file watcher...")
        db_logger.log_event("INFO", "File watcher stopping")
        observer.stop()

    observer.join()
    logger.info("File watcher stopped")
    db_logger.log_event("INFO", "File watcher stopped")
    db_logger.close()


if __name__ == "__main__":
    main()
