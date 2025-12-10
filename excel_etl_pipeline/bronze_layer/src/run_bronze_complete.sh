#!/bin/bash

echo "========================================"
echo "🏥 COMPLETE BRONZE LAYER - ALL DELTA FEATURES"
echo "========================================"

# Activate virtual environment
source venv/bin/activate

# Run with default config
python bronze_layer_complete.py --config bronze_config_complete.yaml

# Alternative commands:
# python bronze_layer_complete.py --config bronze_config_complete.yaml --vacuum
# python bronze_layer_complete.py --info bronze_sales_detailed
# python bronze_layer_complete.py --clone bronze_sales_detailed bronze_sales_detailed_test