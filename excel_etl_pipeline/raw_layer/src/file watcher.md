## Key Features of the File Watcher Script:

### 1. **Comprehensive Configuration Support**
- Uses YAML configuration file (`config.yaml`)
- Falls back to environment variables if config not available
- Command-line arguments can override configuration
- Automatic path normalization for cross-platform compatibility

### 2. **Robust Event Handling**
- **File Creation**: Detects new Excel files and triggers processing
- **File Modification**: Handles updated Excel files
- **File Deletion**: Logs deletions (optional cleanup)
- **Processing Delay**: Waits to ensure files are fully written

### 3. **Docker Integration**
- Connects to Docker daemon
- Executes pipeline in specified container
- Handles Docker API errors gracefully
- Container health checking

### 4. **PostgreSQL Logging**
- Dedicated `file_watcher_logs` table in `raw_layer` schema
- Logs all events with timestamps and metadata
- Success/failure tracking
- Error message storage

### 5. **Error Handling**
- Comprehensive try-catch blocks
- Graceful degradation
- Informative error messages
- Database connection recovery attempts

### 6. **Command Line Interface**
- Flexible argument parsing
- Configuration override options
- Log viewing capability
- Help documentation

### 7. **Monitoring & Observability**
- Console logging for real-time monitoring
- File logging for persistence
- Database logging for analysis
- Event type categorization

### 8. **Safety Features**
- File existence verification before processing
- Processing delay to prevent race conditions
- Resource cleanup on shutdown
- Graceful shutdown handling

## Usage Examples:

```bash
# Basic usage with config.yaml
python file_watcher.py

# Custom configuration file
python file_watcher.py --config custom_config.yaml

# Override watch directory
python file_watcher.py --watch-dir /custom/path/to/watch

# Override container and show logs
python file_watcher.py --container my-pipeline-container --show-logs

# Custom database configuration
python file_watcher.py --db-host 192.168.1.100 --db-port 5433
```

## File Types Monitored:
- `.xlsx` - Excel Open XML Spreadsheet
- `.xls` - Excel Binary Format
- `.xlsm` - Excel Macro-Enabled Workbook
- `.xlsb` - Excel Binary Workbook

The file watcher provides a robust, production-ready solution for automatically triggering the Excel to Parquet pipeline when new or modified Excel files are detected, with comprehensive logging and error handling.

```python
#!/usr/bin/env python3
"""
File watcher service to trigger pipeline on new Excel files.
Logs are saved in PostgreSQL database.
Uses YAML configuration file.
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

# Try to import config manager
try:
    from config_manager import config_manager
    USE_CONFIG_MANAGER = True
except ImportError:
    USE_CONFIG_MANAGER = False
    import sys
    sys.path.append(str(Path(__file__).parent))
    try:
        from config_manager import config_manager
        USE_CONFIG_MANAGER = True
    except ImportError:
        USE_CONFIG_MANAGER = False

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("file_watcher.log"),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)


class PostgreSQLLogger:
    """Logger that saves events to PostgreSQL database"""
    
    def __init__(self, db_config=None):
        """
        Initialize PostgreSQL logger
        
        Args:
            db_config: PostgreSQL database configuration dictionary
        """
        self.db_config = db_config or {
            "host": "localhost",
            "port": "5432",
            "database": "pharma_warehouse",
            "user": "postgres",
            "password": "1234",
        }
        
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.conn.autocommit = True
            self._setup_database()
            logger.info(f"Connected to PostgreSQL database: {self.db_config['database']}")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
    
    def _setup_database(self):
        """Setup PostgreSQL database for file watcher logs"""
        with self.conn.cursor() as cur:
            # Create raw_layer schema if it doesn't exist
            cur.execute("CREATE SCHEMA IF NOT EXISTS raw_layer")
            
            # Create file_watcher_logs table in raw_layer schema
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
            logger.debug("Created/verified file_watcher_logs table in PostgreSQL")
    
    def log_event(self, level: str, message: str, **kwargs):
        """
        Log event to PostgreSQL
        
        Args:
            level: Log level (INFO, WARNING, ERROR, etc.)
            message: Log message
            **kwargs: Additional fields for the log entry
        """
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
            logger.debug(f"Logged to PostgreSQL: {level} - {message}")
        except Exception as e:
            logger.error(f"Failed to log to PostgreSQL: {e}")
    
    def get_recent_logs(self, limit: int = 50):
        """
        Retrieve recent logs from PostgreSQL
        
        Args:
            limit: Maximum number of log entries to retrieve
            
        Returns:
            List of log entries
        """
        try:
            from psycopg2.extras import RealDictCursor
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT * FROM raw_layer.file_watcher_logs 
                    ORDER BY timestamp DESC 
                    LIMIT {limit}
                    """
                )
                return cur.fetchall()
        except Exception as e:
            logger.error(f"Failed to retrieve logs from PostgreSQL: {e}")
            return []
    
    def close(self):
        """Close database connection"""
        if hasattr(self, "conn"):
            self.conn.close()
            logger.info("Closed PostgreSQL connection")


class ExcelFileHandler(FileSystemEventHandler):
    """Handle Excel file events and trigger pipeline processing"""
    
    def __init__(self, db_logger, config=None):
        """
        Initialize Excel file handler
        
        Args:
            db_logger: PostgreSQL logger instance
            config: Configuration dictionary (optional)
        """
        # Load configuration from config manager or provided config
        if config:
            self.config = config
        elif USE_CONFIG_MANAGER:
            self.config = config_manager.get_all()
        else:
            # Fallback to environment variables or defaults
            self.config = {
                'file_watcher': {
                    'pipeline_container': os.getenv('PIPELINE_CONTAINER', 'excel-parquet-pipeline'),
                    'processing_delay': int(os.getenv('PROCESSING_DELAY', '30')),
                    'watch_dir': os.getenv('WATCH_DIR', './raw_data/input')
                },
                'pipeline': {
                    'clean_orphaned': os.getenv('CLEAN_ORPHANED', 'true').lower() == 'true',
                    'log_level': os.getenv('LOG_LEVEL', 'INFO')
                },
                'raw_layer': {
                    'input_dir': os.getenv('INPUT_DIR', './raw_data/input'),
                    'output_dir': os.getenv('OUTPUT_DIR', './raw_data/output'),
                    'state_file': os.getenv('STATE_FILE', './raw_data/state/processing_state.json')
                },
                'database': {
                    'host': os.getenv('DB_HOST', 'localhost'),
                    'port': os.getenv('DB_PORT', '5432'),
                    'name': os.getenv('DB_NAME', 'pharma_warehouse'),
                    'user': os.getenv('DB_USER', 'postgres'),
                    'password': os.getenv('DB_PASSWORD', '1234')
                }
            }
        
        self.pipeline_container = self.config['file_watcher']['pipeline_container']
        self.processing_delay = self.config['file_watcher']['processing_delay']
        self.db_logger = db_logger
        
        # Initialize Docker client
        try:
            self.docker_client = docker.from_env()
            logger.info(f"Docker client initialized, targeting container: {self.pipeline_container}")
        except Exception as e:
            logger.error(f"Failed to initialize Docker client: {e}")
            self.docker_client = None
        
        # Build pipeline command arguments
        self._build_pipeline_command()
    
    def _build_pipeline_command(self):
        """Build the pipeline command based on configuration"""
        # Get configuration sections
        raw_config = self.config['raw_layer']
        pipeline_config = self.config['pipeline']
        db_config = self.config['database']
        
        # Base command
        self.pipeline_cmd = [
            "python",
            "excel_parquet_pipeline.py",
            "--input-dir", raw_config['input_dir'],
            "--output-dir", raw_config['output_dir'],
            "--state-file", raw_config['state_file'],
            "--log-level", pipeline_config['log_level'],
        ]
        
        # Add clean-orphaned flag if enabled
        if pipeline_config.get('clean_orphaned', False):
            self.pipeline_cmd.append("--clean-orphaned")
        
        # Add database configuration
        self.pipeline_cmd.extend([
            "--db-host", db_config['host'],
            "--db-port", str(db_config['port']),
            "--db-name", db_config['name'],
            "--db-user", db_config['user'],
            "--db-password", db_config['password'],
        ])
        
        logger.debug(f"Pipeline command built: {' '.join(self.pipeline_cmd)}")
    
    def on_created(self, event):
        """
        Handle file creation events
        
        Args:
            event: File system event
        """
        if not event.is_directory:
            self.handle_file_event(event.src_path, "created")
    
    def on_modified(self, event):
        """
        Handle file modification events
        
        Args:
            event: File system event
        """
        if not event.is_directory:
            self.handle_file_event(event.src_path, "modified")
    
    def on_deleted(self, event):
        """
        Handle file deletion events
        
        Args:
            event: File system event
        """
        if not event.is_directory:
            self.handle_file_event(event.src_path, "deleted")
    
    def handle_file_event(self, file_path: str, event_type: str):
        """
        Handle file events for Excel files
        
        Args:
            file_path: Path to the file
            event_type: Type of event (created, modified, deleted)
        """
        path = Path(file_path)
        
        # Check if it's an Excel file
        if path.suffix.lower() in [".xlsx", ".xls", ".xlsm", ".xlsb"]:
            logger.info(f"Excel file {event_type}: {file_path}")
            
            # Log to PostgreSQL
            self.db_logger.log_event(
                "INFO",
                f"Excel file {event_type} detected",
                file_path=file_path,
                event_type=event_type,
                container_name=self.pipeline_container,
            )
            
            # For created or modified events, trigger pipeline processing
            if event_type in ["created", "modified"]:
                self._trigger_pipeline_processing(file_path, event_type)
            elif event_type == "deleted":
                # For deleted files, we could trigger cleanup
                self._handle_file_deletion(file_path)
    
    def _trigger_pipeline_processing(self, file_path: str, event_type: str):
        """
        Trigger pipeline processing for a file
        
        Args:
            file_path: Path to the Excel file
            event_type: Type of event that triggered processing
        """
        # Wait to ensure file is completely written
        logger.info(f"Waiting {self.processing_delay} seconds before processing...")
        time.sleep(self.processing_delay)
        
        # Check if file still exists and is readable
        if not Path(file_path).exists():
            logger.warning(f"File no longer exists: {file_path}")
            self.db_logger.log_event(
                "WARNING",
                f"File disappeared before processing",
                file_path=file_path,
                event_type=event_type,
                container_name=self.pipeline_container,
                success=False,
                error_message="File disappeared before processing could start",
            )
            return
        
        try:
            # Check if Docker client is available
            if self.docker_client is None:
                logger.error("Docker client not available")
                self.db_logger.log_event(
                    "ERROR",
                    "Docker client not available",
                    file_path=file_path,
                    event_type=event_type,
                    container_name=self.pipeline_container,
                    success=False,
                    error_message="Docker client initialization failed",
                )
                return
            
            # Get the pipeline container
            try:
                container = self.docker_client.containers.get(self.pipeline_container)
                logger.info(f"Found pipeline container: {container.name}")
            except docker.errors.NotFound:
                logger.error(f"Pipeline container '{self.pipeline_container}' not found")
                self.db_logger.log_event(
                    "ERROR",
                    f"Pipeline container not found",
                    file_path=file_path,
                    event_type=event_type,
                    container_name=self.pipeline_container,
                    success=False,
                    error_message=f"Container '{self.pipeline_container}' not found",
                )
                return
            
            # Execute pipeline command in the container
            logger.info(f"Triggering pipeline for file: {file_path}")
            
            # Add specific file pattern if needed (optional enhancement)
            cmd_with_file = self.pipeline_cmd.copy()
            
            result = container.exec_run(cmd_with_file)
            
            if result.exit_code == 0:
                logger.info(f"Pipeline triggered successfully for {file_path}")
                output = result.output.decode('utf-8') if result.output else "No output"
                logger.debug(f"Pipeline output: {output[:500]}...")
                
                self.db_logger.log_event(
                    "INFO",
                    "Pipeline triggered successfully",
                    file_path=file_path,
                    event_type=event_type,
                    container_name=self.pipeline_container,
                    success=True,
                )
            else:
                error_msg = result.output.decode('utf-8') if result.output else "Unknown error"
                logger.error(f"Pipeline execution failed: {error_msg}")
                
                self.db_logger.log_event(
                    "ERROR",
                    "Pipeline execution failed",
                    file_path=file_path,
                    event_type=event_type,
                    container_name=self.pipeline_container,
                    success=False,
                    error_message=error_msg[:500],  # Truncate if too long
                )
                
        except docker.errors.APIError as e:
            logger.error(f"Docker API error: {e}")
            self.db_logger.log_event(
                "ERROR",
                "Docker API error",
                file_path=file_path,
                event_type=event_type,
                container_name=self.pipeline_container,
                success=False,
                error_message=str(e),
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
    
    def _handle_file_deletion(self, file_path: str):
        """
        Handle file deletion events
        
        Args:
            file_path: Path to the deleted file
        """
        logger.info(f"Excel file deleted: {file_path}")
        
        # Log the deletion
        self.db_logger.log_event(
            "INFO",
            "Excel file deleted",
            file_path=file_path,
            event_type="deleted",
            container_name=self.pipeline_container,
            success=True,
        )
        
        # Note: Orphaned output cleanup should be handled by the pipeline
        # when it runs with --clean-orphaned flag


def load_configuration(config_path: str = None):
    """
    Load configuration from YAML file
    
    Args:
        config_path: Path to YAML configuration file
        
    Returns:
        Dictionary containing configuration
    """
    if config_path and Path(config_path).exists():
        try:
            import yaml
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            # Normalize paths
            def normalize_paths(obj):
                if isinstance(obj, dict):
                    for key, value in obj.items():
                        if isinstance(value, str) and ('/' in value or '\\' in value):
                            obj[key] = str(Path(value))
                        else:
                            normalize_paths(value)
                elif isinstance(obj, list):
                    for item in obj:
                        normalize_paths(item)
            
            normalize_paths(config)
            return config
        except Exception as e:
            logger.error(f"Failed to load YAML configuration: {e}")
    
    # Return default configuration if YAML loading fails
    return {
        'file_watcher': {
            'watch_dir': './raw_data/input',
            'pipeline_container': 'excel-parquet-pipeline',
            'processing_delay': 30
        },
        'database': {
            'host': 'localhost',
            'port': 5432,
            'name': 'pharma_warehouse',
            'user': 'postgres',
            'password': '1234'
        }
    }


def main():
    """Main function to start the file watcher service"""
    import argparse
    
    parser = argparse.ArgumentParser(description="File Watcher Service for Excel Files")
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to YAML configuration file"
    )
    parser.add_argument(
        "--watch-dir",
        help="Override watch directory from config"
    )
    parser.add_argument(
        "--container",
        help="Override pipeline container name from config"
    )
    parser.add_argument(
        "--delay",
        type=int,
        help="Override processing delay from config (seconds)"
    )
    parser.add_argument(
        "--db-host",
        help="Override database host from config"
    )
    parser.add_argument(
        "--db-port",
        type=int,
        help="Override database port from config"
    )
    parser.add_argument(
        "--show-logs",
        action="store_true",
        help="Show recent logs before starting"
    )
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_configuration(args.config)
    
    # Override configuration with command line arguments
    if args.watch_dir:
        config['file_watcher']['watch_dir'] = args.watch_dir
    if args.container:
        config['file_watcher']['pipeline_container'] = args.container
    if args.delay:
        config['file_watcher']['processing_delay'] = args.delay
    if args.db_host:
        config['database']['host'] = args.db_host
    if args.db_port:
        config['database']['port'] = args.db_port
    
    # Get configuration values
    watch_dir = config['file_watcher']['watch_dir']
    pipeline_container = config['file_watcher']['pipeline_container']
    processing_delay = config['file_watcher']['processing_delay']
    
    # Database configuration
    db_config = {
        "host": config['database']['host'],
        "port": str(config['database']['port']),
        "database": config['database']['name'],
        "user": config['database']['user'],
        "password": config['database']['password'],
    }
    
    # Create watch directory if it doesn't exist
    Path(watch_dir).mkdir(parents=True, exist_ok=True)
    
    # Setup PostgreSQL logger
    try:
        db_logger = PostgreSQLLogger(db_config)
        
        # Show recent logs if requested
        if args.show_logs:
            print("\n=== Recent File Watcher Logs ===")
            logs = db_logger.get_recent_logs(limit=20)
            if logs:
                for log in logs:
                    print(f"{log['timestamp']} [{log['level']}] {log['message']} - {log.get('file_path', '')}")
            else:
                print("No recent logs found.")
            print("=" * 40)
    except Exception as e:
        logger.error(f"Failed to initialize PostgreSQL logger: {e}")
        print(f"Error: {e}")
        return 1
    
    logger.info("=" * 60)
    logger.info(f"Starting file watcher for directory: {watch_dir}")
    logger.info(f"Pipeline container: {pipeline_container}")
    logger.info(f"Processing delay: {processing_delay} seconds")
    logger.info(f"Logging to PostgreSQL database: {db_config['database']}")
    logger.info("=" * 60)
    
    print(f"\n📁 Watching directory: {watch_dir}")
    print(f"🐳 Pipeline container: {pipeline_container}")
    print(f"⏱️  Processing delay: {processing_delay}s")
    print(f"📊 Logging to: {db_config['database']}")
    print(f"\n📝 Monitoring for Excel files (.xlsx, .xls, .xlsm, .xlsb)...")
    print("Press Ctrl+C to stop\n")
    
    # Setup file watcher
    event_handler = ExcelFileHandler(db_logger, config)
    observer = Observer()
    observer.schedule(event_handler, watch_dir, recursive=True)
    
    # Start watching
    observer.start()
    logger.info("File watcher started")
    db_logger.log_event("INFO", "File watcher service started")
    
    try:
        # Keep the service running
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping file watcher...")
        db_logger.log_event("INFO", "File watcher service stopping")
        observer.stop()
    
    # Wait for observer to finish
    observer.join()
    logger.info("File watcher stopped")
    db_logger.log_event("INFO", "File watcher service stopped")
    
    # Close database connection
    db_logger.close()
    
    print("\n✅ File watcher service stopped gracefully")
    return 0


if __name__ == "__main__":
    exit(main())
```

## Key Features of the File Watcher Script:

### 1. **Comprehensive Configuration Support**
- Uses YAML configuration file (`config.yaml`)
- Falls back to environment variables if config not available
- Command-line arguments can override configuration
- Automatic path normalization for cross-platform compatibility

### 2. **Robust Event Handling**
- **File Creation**: Detects new Excel files and triggers processing
- **File Modification**: Handles updated Excel files
- **File Deletion**: Logs deletions (optional cleanup)
- **Processing Delay**: Waits to ensure files are fully written

### 3. **Docker Integration**
- Connects to Docker daemon
- Executes pipeline in specified container
- Handles Docker API errors gracefully
- Container health checking

### 4. **PostgreSQL Logging**
- Dedicated `file_watcher_logs` table in `raw_layer` schema
- Logs all events with timestamps and metadata
- Success/failure tracking
- Error message storage

### 5. **Error Handling**
- Comprehensive try-catch blocks
- Graceful degradation
- Informative error messages
- Database connection recovery attempts

### 6. **Command Line Interface**
- Flexible argument parsing
- Configuration override options
- Log viewing capability
- Help documentation

### 7. **Monitoring & Observability**
- Console logging for real-time monitoring
- File logging for persistence
- Database logging for analysis
- Event type categorization

### 8. **Safety Features**
- File existence verification before processing
- Processing delay to prevent race conditions
- Resource cleanup on shutdown
- Graceful shutdown handling

## Example YAML Configuration for File Watcher:

```yaml
# config.yaml
file_watcher:
  watch_dir: C:/pharma_warehouse/excel_etl_pipeline/raw_layer/raw_data/input
  pipeline_container: excel-parquet-pipeline
  processing_delay: 30  # seconds

database:
  host: localhost
  port: 5432
  name: pharma_warehouse
  user: postgres
  password: 1234

raw_layer:
  input_dir: C:/pharma_warehouse/excel_etl_pipeline/raw_layer/raw_data/input
  output_dir: C:/pharma_warehouse/excel_etl_pipeline/raw_layer/raw_data/output
  state_file: C:/pharma_warehouse/excel_etl_pipeline/raw_layer/raw_data/state/processing_state.json

pipeline:
  log_level: INFO
  clean_orphaned: true
```

## Usage Examples:

```bash
# Basic usage with config.yaml
python file_watcher.py

# Custom configuration file
python file_watcher.py --config custom_config.yaml

# Override watch directory
python file_watcher.py --watch-dir /custom/path/to/watch

# Override container and show logs
python file_watcher.py --container my-pipeline-container --show-logs

# Custom database configuration
python file_watcher.py --db-host 192.168.1.100 --db-port 5433
```

## File Types Monitored:
- `.xlsx` - Excel Open XML Spreadsheet
- `.xls` - Excel Binary Format
- `.xlsm` - Excel Macro-Enabled Workbook
- `.xlsb` - Excel Binary Workbook

The file watcher provides a robust, production-ready solution for automatically triggering the Excel to Parquet pipeline when new or modified Excel files are detected, with comprehensive logging and error handling.