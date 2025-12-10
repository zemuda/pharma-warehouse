# Quick test script - save as test_files.py
from pathlib import Path

source_path = Path("C:/pharma_warehouse/etl_pipeline/excel_pipeline/data/output")

for feature in ["sales", "purchases", "inventory"]:
    feature_path = source_path / feature
    print(f"\nChecking: {feature_path}")

    if feature_path.exists():
        parquet_files = list(feature_path.rglob("*.parquet"))
        print(f"  Found {len(parquet_files)} parquet files")
        for f in parquet_files[:5]:  # Show first 5 files
            print(f"    {f.name}")
    else:
        print("  Path does not exist")
