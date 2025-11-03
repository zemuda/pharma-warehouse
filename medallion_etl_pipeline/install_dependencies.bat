@echo off
echo Installing dependencies for Python 3.13...

REM Upgrade pip
python -m pip install --upgrade pip

REM Install core dependencies first
echo Installing PySpark and Delta Lake...
pip install pyspark==3.5.0
pip install delta-spark==3.0.0

REM Install Prefect
echo Installing Prefect...
pip install prefect==2.14.21

REM Install database driver (Python 3.13 compatible)
echo Installing PostgreSQL driver...
pip install "psycopg[binary]"==3.1.18
pip install sqlalchemy==2.0.25

REM Install pandas and numpy
echo Installing pandas...
pip install numpy>=1.26.0
pip install pandas==2.2.0

REM Install Excel support
echo Installing Excel libraries...
pip install openpyxl xlrd

REM Install utilities
echo Installing utilities...
pip install python-dotenv pyyaml

echo.
echo ========================================
echo Installation complete!
echo ========================================
echo.
echo To verify installation:
echo python -c "import pyspark; import pandas; import psycopg; print('All imports successful!')"
echo.
pause