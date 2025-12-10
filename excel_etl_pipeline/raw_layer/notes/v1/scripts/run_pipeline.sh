#!/bin/bash
set -e

# Resolve project root (one level up from this script)
# SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Resolve project root (TWO levels up from this script)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Load environment variables from project root .env
set -a
if [ -f "$PROJECT_ROOT/.env" ]; then
  # shellcheck disable=SC1090
  source "$PROJECT_ROOT/.env"
else
  echo "Warning: .env not found at $PROJECT_ROOT/.env"
fi
set +a

# Create directories if they don't exist
mkdir -p "$INPUT_DIR" "$OUTPUT_DIR" "$(dirname "$STATE_FILE")"

echo "=== Running Excel to Parquet Pipeline ==="
echo "Project root: $PROJECT_ROOT"
echo "Input Directory: $INPUT_DIR"
echo "Output Directory: $OUTPUT_DIR"
echo "Database: $DB_USER@$DB_HOST:$DB_PORT/$DB_NAME"

# Verify pipeline entrypoint exists
PIPELINE_SCRIPT="$PROJECT_ROOT/raw_layer/src/excel_parquet_pipeline.py"
if [ ! -f "$PIPELINE_SCRIPT" ]; then
  echo "Error: pipeline script not found at $PIPELINE_SCRIPT"
  echo "If the pipeline is a package module, adjust this script to call 'python -m <module>'"
  exit 1
fi

# Build command using absolute path to the pipeline script
CMD="python \"$PIPELINE_SCRIPT\" \
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