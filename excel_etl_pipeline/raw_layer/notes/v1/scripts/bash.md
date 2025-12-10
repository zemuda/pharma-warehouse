cd /c/pharma_warehouse/excel_etl_pipeline

# create venv if missing
python -m venv .venv

# activate venv (Git Bash reads the POSIX activate script in Scripts)
source .venv/Scripts/activate

# verify
python -V
which python

# run pipeline script
bash raw_layer/scripts/run_pipeline.sh