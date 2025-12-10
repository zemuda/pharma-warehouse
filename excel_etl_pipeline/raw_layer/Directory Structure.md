C:\pharma_warehouse\excel_etl_pipeline\
├── raw_layer\
│   ├── src\
│   │   ├── excel_parquet_pipeline.py     # Main pipeline script
│   │   └── file_watcher.py              # File watcher script
│   ├── raw_data\
│   │   ├── input\                        # Input Excel files
│   │   ├── output\                       # Output Parquet files
│   │   └── state\                        # Processing state
│   └── scripts\
│       ├── config.yaml                   # YAML configuration
│       ├── config_manager.py            # Configuration manager
│       ├── run_pipeline_yaml.bat        # Windows batch file
│       └── validate_config.py           # Config validation tool