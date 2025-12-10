:: run_pipeline_yaml.bat
@echo off
setlocal enabledelayedexpansion

echo Configure an Excel to Parquet ETL Pipeline using YAML Configuration
echo.

:: Switch to the project directory
cd /d C:\pharma_warehouse\excel_etl_pipeline\raw_layer\src

:: Operate the ETL pipeline with the specified YAML configuration file
python excel_parquet_pipeline.py --config ..\..\config.yaml

echo.
echo Pipeline execution completed.
pause