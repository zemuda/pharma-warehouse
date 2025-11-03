#!/bin/bash

echo "Setting up Medallion Architecture Pipeline environment..."

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install dependencies
pip install -r requirements.txt

# Create directory structure
mkdir -p C:/pharma_warehouse/medallion_etl_pipeline/data/input
mkdir -p C:/pharma_warehouse/medallion_etl_pipeline/data/lakehouse/{bronze,silver,gold}
mkdir -p C:/pharma_warehouse/medallion_etl_pipeline/checkpoints
mkdir -p C:/pharma_warehouse/medallion_etl_pipeline/temp
mkdir -p C:/pharma_warehouse/medallion_etl_pipeline/logs

# Set environment variables
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=etl_metadata
export POSTGRES_USER=etl_user
export POSTGRES_PASSWORD=etl_password

echo "Environment setup complete!"
echo "To activate: source venv/bin/activate"