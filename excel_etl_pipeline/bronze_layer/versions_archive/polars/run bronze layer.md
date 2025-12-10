cd /c/pharma_warehouse/excel_etl_pipeline
source .venv/Scripts/activate
python bronze_layer/bronze_loader_polars.py --config bronze_layer/bronze_loader_config.yaml

Or without config (uses default):
python bronze_layer/bronze_loader_polars.py