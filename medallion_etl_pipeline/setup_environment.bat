@echo off
echo ========================================
echo Medallion Architecture Pipeline Setup
echo ========================================
echo.

REM Check if venv already exists
if exist venv (
    echo Warning: venv folder already exists
    choice /C YN /M "Do you want to delete and recreate it?"
    if errorlevel 2 goto skip_venv_creation
    echo Removing old venv...
    rmdir /s /q venv
)

:skip_venv_creation
REM Create virtual environment
echo Creating virtual environment...
python -m venv venv

REM Activate virtual environment
echo Activating virtual environment...
call venv\Scripts\activate.bat

REM Verify activation
python -c "import sys; print('Python location:', sys.executable)"
if not "%VIRTUAL_ENV%"=="" (
    echo [OK] Virtual environment activated
    echo Location: %VIRTUAL_ENV%
) else (
    echo [ERROR] Failed to activate virtual environment
    pause
    exit /b 1
)

echo.
echo Upgrading pip...
python -m pip install --upgrade pip setuptools wheel

echo.
echo Installing dependencies...
pip install -r requirements.txt

REM Create directory structure
echo.
echo Creating directory structure...
mkdir C:\pharma_warehouse\medallion_etl_pipeline\data\input 2>nul
mkdir C:\pharma_warehouse\medallion_etl_pipeline\data\lakehouse\bronze 2>nul
mkdir C:\pharma_warehouse\medallion_etl_pipeline\data\lakehouse\silver 2>nul
mkdir C:\pharma_warehouse\medallion_etl_pipeline\data\lakehouse\gold 2>nul
mkdir C:\pharma_warehouse\medallion_etl_pipeline\checkpoints 2>nul
mkdir C:\pharma_warehouse\medallion_etl_pipeline\temp 2>nul
mkdir C:\pharma_warehouse\medallion_etl_pipeline\logs 2>nul

REM Set environment variables
echo.
echo Setting environment variables...
setx POSTGRES_HOST localhost
setx POSTGRES_PORT 5432
setx POSTGRES_DB etl_metadata
setx POSTGRES_USER etl_user
setx POSTGRES_PASSWORD etl_password

echo.
echo ========================================
echo Setup complete!
echo ========================================
echo.
echo Virtual environment is ACTIVE
echo To activate in future: venv\Scripts\activate.bat
echo To deactivate: deactivate
echo.
pause