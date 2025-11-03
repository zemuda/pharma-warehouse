@echo off
echo ========================================
echo CLEAN INSTALLATION
echo ========================================
echo.
echo This will:
echo 1. Remove existing venv
echo 2. Create fresh venv
echo 3. Install all dependencies in venv
echo.
pause

REM Remove old venv
if exist venv (
    echo Removing old venv...
    rmdir /s /q venv
)

REM Create new venv
echo Creating new virtual environment...
python -m venv venv

REM Activate venv
echo Activating venv...
call venv\Scripts\activate.bat

REM Verify activation
echo.
echo Checking virtual environment...
python -c "import sys; print('Python:', sys.executable)"
echo Virtual environment: %VIRTUAL_ENV%
echo.

if "%VIRTUAL_ENV%"=="" (
    echo [ERROR] Virtual environment NOT activated!
    pause
    exit /b 1
)

echo [OK] Virtual environment is active
echo.

REM Upgrade pip
echo Upgrading pip...
python -m pip install --upgrade pip setuptools wheel

REM Install dependencies one by one to see progress
echo.
echo Installing dependencies...
echo.

echo [1/10] Installing numpy...
pip install numpy>=1.26.0

echo [2/10] Installing pyarrow...
pip install pyarrow>=15.0.0

echo [3/10] Installing pandas...
pip install pandas==2.2.0

echo [4/10] Installing SQLAlchemy...
pip install sqlalchemy==2.0.36

echo [5/10] Installing psycopg...
pip install "psycopg[binary]">=3.2

echo [6/10] Installing Excel support...
pip install openpyxl xlrd

echo [7/10] Installing utilities...
pip install python-dotenv pyyaml

echo [8/10] Installing PySpark...
pip install pyspark==3.5.0

echo [9/10] Installing Delta Lake...
pip install delta-spark==3.0.0

echo [10/10] Installing Prefect...
pip install prefect==2.14.21

echo.
echo ========================================
echo Installation complete!
echo ========================================
echo.
echo Testing imports...
python test_imports.py

echo.
echo ========================================
echo SUMMARY
echo ========================================
python -c "import sys; print('Python:', sys.executable)"
pip list
echo.
echo To activate in future: venv\Scripts\activate.bat
echo.
pause