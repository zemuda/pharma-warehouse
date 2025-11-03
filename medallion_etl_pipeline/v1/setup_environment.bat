@echo off
echo Setting up Medallion Architecture Pipeline environment...

REM Create virtual environment
python -m venv venv
call venv\Scripts\activate.bat

REM Upgrade pip
python -m pip install --upgrade pip

REM Install dependencies
pip install -r requirements.txt

REM Create directory structure
mkdir C:\pharma_warehouse\medallion_etl_pipeline\data\input 2>nul
mkdir C:\pharma_warehouse\medallion_etl_pipeline\data\lakehouse\bronze 2>nul
mkdir C:\pharma_warehouse\medallion_etl_pipeline\data\lakehouse\silver 2>nul
mkdir C:\pharma_warehouse\medallion_etl_pipeline\data\lakehouse\gold 2>nul
mkdir C:\pharma_warehouse\medallion_etl_pipeline\checkpoints 2>nul
mkdir C:\pharma_warehouse\medallion_etl_pipeline\temp 2>nul
mkdir C:\pharma_warehouse\medallion_etl_pipeline\logs 2>nul

REM Set environment variables
setx POSTGRES_HOST localhost
setx POSTGRES_PORT 5432
setx POSTGRES_DB etl_metadata
setx POSTGRES_USER etl_user
setx POSTGRES_PASSWORD etl_password

echo Environment setup complete!
echo To activate: venv\Scripts\activate.bat