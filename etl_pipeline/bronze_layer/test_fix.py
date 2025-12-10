import polars as pl
from pathlib import Path
from bronze_loader_polars import BronzeLoaderPolars


def test_fix():
    try:
        print("\n=== Starting Test Fix ===")
        loader = BronzeLoaderPolars()

        # Get base path from config
        base_path = Path("C:/pharma_warehouse/etl_pipeline/excel_pipeline/data/output")
        print(f"Looking for files in: {base_path}")

        # Test with one problematic file
        files = loader.discover_parquet_files("sales/cash_invoices/detailed")

        if not files:
            print("❌ No files found!")
            return

        print(f"Found {len(files)} files")
        print(f"Testing with: {files[0]}")

        # Read and analyze first file
        df = pl.read_parquet(files[0])
        print("\nOriginal schema:")
        print(df.schema)

        # Apply fix
        print("\nApplying schema fix...")
        df_fixed = loader.fix_null_types_simple(df)
        print("\nFixed schema:")
        print(df_fixed.schema)

        # Try to write
        print("\nAttempting to write fixed data...")
        loader.write_delta_table(df_fixed, "test_fix")
        print("\n✅ Test successful!")

    except Exception as e:
        print(f"\n❌ Test failed: {str(e)}")
        raise


if __name__ == "__main__":
    test_fix()
