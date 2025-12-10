# setup_directories.py
"""
Create the complete directory structure for the pipeline
"""
from pathlib import Path
import sys

# Base paths
BASE_PATH = Path(r"C:\pharma_warehouse\etl_pipeline")

# Create all required directories
directories = [
    # Input directories
    "data/input/sales/cash_invoices/detailed/2025/january",
    "data/input/sales/cash_invoices/summarized/2025/january",
    "data/input/sales/cash_sales/detailed/2025/january",
    "data/input/sales/cash_sales/summarized/2025/january",
    "data/input/sales/credit_notes/detailed/2025/january",
    "data/input/sales/credit_notes/summarized/2025/january",
    # Output directories
    "data/lakehouse/bronze",
    "data/lakehouse/silver",
    "data/lakehouse/gold",
    # Working directories
    "checkpoints",
    "temp",
    "logs",
]

print("Creating directory structure...")
print(f"Base path: {BASE_PATH}\n")

created = 0
existed = 0

for dir_path in directories:
    full_path = BASE_PATH / dir_path
    if not full_path.exists():
        full_path.mkdir(parents=True, exist_ok=True)
        print(f"✓ Created: {dir_path}")
        created += 1
    else:
        print(f"  Exists:  {dir_path}")
        existed += 1

print(f"\n{'='*60}")
print(f"Summary:")
print(f"  Created: {created} directories")
print(f"  Already existed: {existed} directories")
print(f"{'='*60}")

# Check for Excel files
input_path = BASE_PATH / "data/input/sales"
if input_path.exists():
    excel_files = list(input_path.rglob("*.xls*"))
    print(f"\n📊 Found {len(excel_files)} Excel files")

    if excel_files:
        print("\nFiles by location:")
        for file in excel_files:
            rel_path = file.relative_to(input_path)
            print(f"  - {rel_path}")
    else:
        print("\n⚠ No Excel files found yet")
        print("Copy your Excel files to the appropriate folders:")
        print(
            f"  {BASE_PATH / 'data/input/sales/cash_invoices/detailed/2025/january/'}"
        )
else:
    print(f"\n⚠ Input directory not found: {input_path}")

print(f"\n{'='*60}")
print("✅ Directory structure ready!")
print(f"{'='*60}\n")
