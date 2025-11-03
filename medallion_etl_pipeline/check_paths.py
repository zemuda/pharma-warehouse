# check_paths.py
from pathlib import Path
import os

def check_directories():
    base_path = Path("C:/pharma_warehouse/medallion_etl_pipeline")
    
    required_dirs = [
        base_path / "data/input/sales/cash_invoices/detailed/2025/january",
        base_path / "data/lakehouse/bronze",
        base_path / "data/lakehouse/silver", 
        base_path / "data/lakehouse/gold",
        base_path / "checkpoints",
        base_path / "temp",
        base_path / "logs"
    ]
    
    print("🔍 Checking directory structure...")
    for dir_path in required_dirs:
        if dir_path.exists():
            print(f"✅ {dir_path}")
        else:
            print(f"❌ {dir_path} - Creating...")
            dir_path.mkdir(parents=True, exist_ok=True)
    
    # Check for Excel files
    excel_path = base_path / "data/input/sales/cash_invoices/detailed/2025/january"
    excel_files = list(excel_path.glob("*.xls*"))
    print(f"\n📊 Excel files found: {len(excel_files)}")
    for file in excel_files:
        print(f"   📄 {file.name}")

if __name__ == "__main__":
    check_directories()