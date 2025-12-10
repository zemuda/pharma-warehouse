@echo off
setlocal enabledelayedexpansion

echo ========================================
echo Excel to Parquet Pipeline (YAML Config)
echo ========================================
echo.

:: Set script directory
set "SCRIPT_DIR=%~dp0"
echo Script directory: %SCRIPT_DIR%

:: Change to project directory
cd /d "C:\pharma_warehouse\excel_etl_pipeline\raw_layer\src"
echo Working directory: %CD%
echo.

:: Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python is not installed or not in PATH
    echo Please install Python 3.7 or higher
    pause
    exit /b 1
)

:: Check if configuration file exists
if not exist "%SCRIPT_DIR%\config.yaml" (
    echo ❌ Configuration file not found: %SCRIPT_DIR%\config.yaml
    echo.
    echo Please create a config.yaml file in the scripts directory.
    echo You can use the example configuration provided.
    pause
    exit /b 1
)

echo ✅ Configuration file found: %SCRIPT_DIR%\config.yaml
echo.

:: Check if required Python packages are installed
echo Checking required Python packages...
python -c "import pandas, pyarrow, psycopg2, yaml" >nul 2>&1
if errorlevel 1 (
    echo ⚠ Some required packages are not installed
    echo Installing required packages...
    pip install pandas pyarrow psycopg2-binary pyyaml watchdog docker
    if errorlevel 1 (
        echo ❌ Failed to install required packages
        echo Please install them manually: pip install pandas pyarrow psycopg2-binary pyyaml
        pause
        exit /b 1
    )
    echo ✅ Packages installed successfully
) else (
    echo ✅ All required packages are installed
)

echo.
echo ========================================
echo Starting Pipeline with YAML Configuration
echo ========================================
echo.

:: Get configuration values for display
for /f "tokens=2 delims=:" %%i in ('findstr /C:"input_dir:" "%SCRIPT_DIR%\config.yaml"') do set "INPUT_DIR=%%i"
for /f "tokens=2 delims=:" %%i in ('findstr /C:"output_dir:" "%SCRIPT_DIR%\config.yaml"') do set "OUTPUT_DIR=%%i"
for /f "tokens=2 delims=:" %%i in ('findstr /C:"db-host:" "%SCRIPT_DIR%\config.yaml"') do set "DB_HOST=%%i"
for /f "tokens=2 delims=:" %%i in ('findstr /C:"db-user:" "%SCRIPT_DIR%\config.yaml"') do set "DB_USER=%%i"

:: Clean up strings
set "INPUT_DIR=!INPUT_DIR: =!"
set "OUTPUT_DIR=!OUTPUT_DIR: =!"
set "DB_HOST=!DB_HOST: =!"
set "DB_USER=!DB_USER: =!"

echo Configuration Summary:
echo   Input Directory:  %INPUT_DIR%
echo   Output Directory: %OUTPUT_DIR%
echo   Database:         %DB_USER%@%DB_HOST%
echo.

:: Create directories if they don't exist
if not exist "%INPUT_DIR%" (
    echo Creating input directory: %INPUT_DIR%
    mkdir "%INPUT_DIR%"
)

if not exist "%OUTPUT_DIR%" (
    echo Creating output directory: %OUTPUT_DIR%
    mkdir "%OUTPUT_DIR%"
)

echo.
echo ========================================
echo Running Pipeline...
echo ========================================
echo.

:: Run the pipeline with YAML configuration
python excel_parquet_pipeline.py --config "%SCRIPT_DIR%\config.yaml"

:: Check exit code
if errorlevel 1 (
    echo.
    echo ❌ Pipeline execution failed with error code: %errorlevel%
    echo.
    echo Troubleshooting steps:
    echo 1. Check that PostgreSQL is running
    echo 2. Verify database credentials in config.yaml
    echo 3. Ensure input directory contains Excel files
    echo 4. Check the log file: excel_parquet_pipeline.log
    echo.
    pause
    exit /b %errorlevel%
) else (
    echo.
    echo ✅ Pipeline completed successfully!
    echo.
    pause
)

endlocal