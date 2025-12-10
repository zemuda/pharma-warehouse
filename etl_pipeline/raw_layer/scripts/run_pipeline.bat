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