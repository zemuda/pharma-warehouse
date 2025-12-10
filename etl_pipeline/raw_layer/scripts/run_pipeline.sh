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