Based on your file structure, here are the complete scripts with the correct paths:

https://chat.deepseek.com/a/chat/s/70c3047f-cd76-48e2-903c-58be3a828690

## Usage Instructions:

### 1. **First-Time Setup:**
```bash
# Activate venv
source C:/pharma_warehouse/excel_etl_pipeline/venv/Scripts/activate
venv\Scripts\activate

# Navigate to scripts directory
cd C:\pharma_warehouse\excel_etl_pipeline\raw_layer\scripts

# Validate configuration
python validate_config.py

# Install required packages if needed
pip list

pip install pandas pyarrow psycopg2-binary pyyaml watchdog docker

pip freeze > requirements.txt
```

### 2. **Run the Pipeline:**
```bash
# Using the batch file (Windows)
run_pipeline_yaml.bat

# Or using Python directly
cd C:\pharma_warehouse\excel_etl_pipeline\raw_layer\src
python excel_parquet_pipeline.py --config ..\scripts\config.yaml
```

### 3. **Start File Watcher:**
```bash
cd C:\pharma_warehouse\excel_etl_pipeline\raw_layer\src
python file_watcher.py --config ..\scripts\config.yaml
```

## Key Features:

1. **Centralized Configuration**: All settings in one YAML file
2. **Path Management**: Automatic path normalization for Windows/Linux
3. **Validation**: Configuration validation before pipeline runs
4. **Error Handling**: Comprehensive error messages and troubleshooting
5. **Batch Script**: Easy-to-use Windows batch file
6. **Extensible**: Easy to add new configuration sections
7. **Logging**: Configurable logging levels and destinations

The scripts are now properly organized with:
- Configuration files in `scripts/` directory
- Main Python scripts in `src/` directory
- Data in `raw_data/` directory
- Clear separation of concerns
- Easy maintenance and updates