I'll create a `.env` file and a script to run the pipeline with database configuration loaded from the environment file.

## 1. Create the `.env` file

Create a file named `.env` in your project root:

```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=excel_pipeline
DB_USER=postgres
DB_PASSWORD=password

# Pipeline Directories
INPUT_DIR=./raw_data/input
OUTPUT_DIR=./raw_data/output
STATE_FILE=./raw_data/state/processing_state.json

# Pipeline Options
LOG_LEVEL=INFO
CLEAN_ORPHANED=true
SHOW_LOGS=true
LOG_LIMIT=10
FILE_PATTERN=*.xls
```

## 2. Create the run script

Create a file named `run_pipeline.py`:

```python
#!/usr/bin/env python3
"""
Script to run Excel to Parquet Pipeline with configuration from .env file
"""

import os
import argparse
from pathlib import Path
from dotenv import load_dotenv

def load_environment():
    """Load environment variables from .env file"""
    env_path = Path('.') / '.env'
    if env_path.exists():
        load_dotenv(env_path)
        print("✓ Loaded configuration from .env file")
    else:
        print("⚠ .env file not found, using default values")

def run_pipeline():
    """Run the Excel to Parquet pipeline"""
    
    # Load environment variables
    load_environment()
    
    # Get configuration from environment with fallbacks
    config = {
        'input_dir': os.getenv('INPUT_DIR', './raw_data/input'),
        'output_dir': os.getenv('OUTPUT_DIR', './raw_data/output'),
        'state_file': os.getenv('STATE_FILE', './raw_data/state/processing_state.json'),
        'log_level': os.getenv('LOG_LEVEL', 'INFO'),
        'clean_orphaned': os.getenv('CLEAN_ORPHANED', 'true').lower() == 'true',
        'show_logs': os.getenv('SHOW_LOGS', 'true').lower() == 'true',
        'log_limit': int(os.getenv('LOG_LIMIT', '10')),
        'file_pattern': os.getenv('FILE_PATTERN', '*.xls'),
        'db_host': os.getenv('DB_HOST', 'localhost'),
        'db_port': os.getenv('DB_PORT', '5432'),
        'db_name': os.getenv('DB_NAME', 'excel_pipeline'),
        'db_user': os.getenv('DB_USER', 'postgres'),
        'db_password': os.getenv('DB_PASSWORD', 'password'),
    }
    
    # Create directories if they don't exist
    Path(config['input_dir']).mkdir(parents=True, exist_ok=True)
    Path(config['output_dir']).mkdir(parents=True, exist_ok=True)
    Path(config['state_file']).parent.mkdir(parents=True, exist_ok=True)
    
    # Build command
    cmd = [
        'python', 'excel_parquet_pipeline.py',
        '--input-dir', config['input_dir'],
        '--output-dir', config['output_dir'],
        '--state-file', config['state_file'],
        '--log-level', config['log_level'],
        '--pattern', config['file_pattern'],
        '--db-host', config['db_host'],
        '--db-port', config['db_port'],
        '--db-name', config['db_name'],
        '--db-user', config['db_user'],
        '--db-password', config['db_password'],
    ]
    
    # Add optional flags
    if config['clean_orphaned']:
        cmd.append('--clean-orphaned')
    
    if config['show_logs']:
        cmd.append('--show-logs')
        cmd.extend(['--log-limit', str(config['log_limit'])])
    
    # Print configuration
    print("\n=== Pipeline Configuration ===")
    print(f"Input Directory: {config['input_dir']}")
    print(f"Output Directory: {config['output_dir']}")
    print(f"State File: {config['state_file']}")
    print(f"Database: {config['db_user']}@{config['db_host']}:{config['db_port']}/{config['db_name']}")
    print(f"Clean Orphaned: {config['clean_orphaned']}")
    print(f"Show Logs: {config['show_logs']}")
    print(f"Log Limit: {config['log_limit']}")
    print("=" * 30)
    
    # Execute the pipeline
    try:
        # Import and run the pipeline directly
        from excel_parquet_pipeline import main as pipeline_main
        import sys
        
        # Set up command line arguments for the pipeline
        sys.argv = cmd[1:]  # Remove 'python' from the command
        
        print("\n🚀 Starting Excel to Parquet Pipeline...")
        pipeline_main()
        
    except ImportError as e:
        print(f"❌ Error: Could not import pipeline module: {e}")
        print("Make sure excel_parquet_pipeline.py is in the same directory")
        return 1
    except Exception as e:
        print(f"❌ Error running pipeline: {e}")
        return 1
    
    return 0

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Run Excel to Parquet Pipeline with .env configuration')
    parser.add_argument('--env-file', default='.env', help='Path to .env file (default: .env)')
    
    args = parser.parse_args()
    
    # Set custom env file if provided
    if args.env_file != '.env':
        os.environ['DOTENV_PATH'] = args.env_file
    
    # Run the pipeline
    return run_pipeline()

if __name__ == '__main__':
    exit(main())
```

## 3. Create a bash script (alternative)

Create a file named `run_pipeline.sh`:

```bash
#!/bin/bash

# Load environment variables
set -a
source .env
set +a

# Create directories if they don't exist
mkdir -p $INPUT_DIR $OUTPUT_DIR $(dirname $STATE_FILE)

echo "=== Running Excel to Parquet Pipeline ==="
echo "Input Directory: $INPUT_DIR"
echo "Output Directory: $OUTPUT_DIR"
echo "Database: $DB_USER@$DB_HOST:$DB_PORT/$DB_NAME"

# Build command
CMD="python excel_parquet_pipeline.py \
  --input-dir \"$INPUT_DIR\" \
  --output-dir \"$OUTPUT_DIR\" \
  --state-file \"$STATE_FILE\" \
  --log-level \"$LOG_LEVEL\" \
  --pattern \"$FILE_PATTERN\" \
  --db-host \"$DB_HOST\" \
  --db-port \"$DB_PORT\" \
  --db-name \"$DB_NAME\" \
  --db-user \"$DB_USER\" \
  --db-password \"$DB_PASSWORD\""

# Add optional flags
if [ "$CLEAN_ORPHANED" = "true" ]; then
  CMD="$CMD --clean-orphaned"
fi

if [ "$SHOW_LOGS" = "true" ]; then
  CMD="$CMD --show-logs --log-limit $LOG_LIMIT"
fi

echo "Command: $CMD"
echo "========================================="

# Execute the command
eval $CMD
```

Make it executable:
```bash
chmod +x run_pipeline.sh
```

## 4. Create a Windows batch file

Create a file named `run_pipeline.bat`:

```batch
@echo off
setlocal enabledelayedexpansion

echo Loading environment from .env file...

:: Load environment variables from .env file
for /f "usebackq delims== tokens=1,2" %%i in (".env") do (
  set %%i=%%j
)

:: Create directories if they don't exist
if not exist "%INPUT_DIR%" mkdir "%INPUT_DIR%"
if not exist "%OUTPUT_DIR%" mkdir "%OUTPUT_DIR%"
mkdir "%STATE_FILE%\.." 2>nul

echo.
echo === Running Excel to Parquet Pipeline ===
echo Input Directory: %INPUT_DIR%
echo Output Directory: %OUTPUT_DIR%
echo Database: %DB_USER%@%DB_HOST%:%DB_PORT%/%DB_NAME%
echo.

:: Build command
set CMD=python excel_parquet_pipeline.py --input-dir "%INPUT_DIR%" --output-dir "%OUTPUT_DIR%" --state-file "%STATE_FILE%" --log-level "%LOG_LEVEL%" --pattern "%FILE_PATTERN%" --db-host "%DB_HOST%" --db-port "%DB_PORT%" --db-name "%DB_NAME%" --db-user "%DB_USER%" --db-password "%DB_PASSWORD%"

:: Add optional flags
if "%CLEAN_ORPHANED%"=="true" (
    set CMD=!CMD! --clean-orphaned
)

if "%SHOW_LOGS%"=="true" (
    set CMD=!CMD! --show-logs --log-limit %LOG_LIMIT%
)

echo Command: !CMD!
echo =========================================
echo.

:: Execute the command
!CMD!

pause
```

## 5. Updated Requirements

Make sure to install `python-dotenv`:

```bash
pip install python-dotenv
```

## Usage

### Using the Python script (recommended):
```bash
python run_pipeline.py
```

### Using the bash script:
```bash
./run_pipeline.sh
```

### Using the batch file (Windows):
```cmd
run_pipeline.bat
```

### With custom .env file:
```bash
python run_pipeline.py --env-file production.env
```

## Project Structure

Your project should now look like this:
```
project/
├── .env
├── run_pipeline.py
├── run_pipeline.sh
├── run_pipeline.bat
├── excel_parquet_pipeline.py
├── file_watcher.py
├── requirements.txt
└── excel_pipeline/
    ├── data/
    │   ├── input/
    │   └── output/
    ├── state/
    └── logs/
```

## Additional Features

The run script also includes:

1. **Automatic directory creation** - Creates input/output directories if they don't exist
2. **Configuration validation** - Shows the loaded configuration before running
3. **Error handling** - Proper error messages if modules are missing
4. **Flexible .env file location** - Can specify custom .env file path
5. **Cross-platform compatibility** - Works on Windows, Linux, and macOS

This setup makes it easy to manage your database credentials and pipeline configuration in one place while providing a simple way to run the pipeline.