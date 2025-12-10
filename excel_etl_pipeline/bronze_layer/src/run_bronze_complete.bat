@echo off
echo ========================================
echo 🏥 COMPLETE BRONZE LAYER - ALL DELTA FEATURES
echo ========================================

REM Activate virtual environment
call venv\Scripts\activate.bat

REM Run with default config
python bronze_layer_complete.py --config bronze_config_complete.yaml

REM Alternative commands:
REM python bronze_layer_complete.py --config bronze_config_complete.yaml --vacuum
REM python bronze_layer_complete.py --info bronze_sales_detailed
REM python bronze_layer_complete.py --clone bronze_sales_detailed bronze_sales_detailed_test

pause