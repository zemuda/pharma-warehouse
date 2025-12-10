# simple_robust_version.py
import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime


def read_parquet_files_safely(directory):
    """Read parquet files safely handling duplicates"""
    files = glob.glob(os.path.join(directory, "*.parquet"))
    if not files:
        return None

    dfs = []
    for file in files:
        try:
            df = pd.read_parquet(file)
            # Handle duplicate column names
            if len(df.columns) != len(set(df.columns)):
                # Rename duplicates
                seen = {}
                new_columns = []
                for col in df.columns:
                    if col in seen:
                        seen[col] += 1
                        new_columns.append(f"{col}_{seen[col]}")
                    else:
                        seen[col] = 0
                        new_columns.append(col)
                df.columns = new_columns
            dfs.append(df)
        except Exception as e:
            print(f"Warning: Could not read {os.path.basename(file)}: {e}")

    if not dfs:
        return None

    return pd.concat(dfs, ignore_index=True)


def extract_columns(df):
    """Extract required columns from dataframe"""
    result = {}

    # Map columns
    col_map = {
        "cus_code": ["CUS_CODE", "CUSTOMER_CODE", "CUSCODE", "CODE"],
        "customer_name": ["CUSTOMER_NAME", "CUS_NAME", "NAME", "CUSTOMER"],
        "amount": ["AMOUNT", "TOTAL", "SUM", "VALUE"],
        "date": ["TRN_DATE", "DATE", "TRANSACTION_DATE", "TIMESTAMP"],
        "branch": ["BRANCH_NAME", "BRANCH", "LOCATION"],
    }

    df_cols = [str(col).upper() for col in df.columns]

    for key, patterns in col_map.items():
        found = False
        for pattern in patterns:
            for df_col in df_cols:
                if pattern in df_col:
                    original_col = df.columns[df_cols.index(df_col)]
                    result[key] = original_col
                    found = True
                    break
            if found:
                break

    return result


def generate_report():
    """Main function to generate the report"""
    # Configuration
    input_dir = r"C:\pharma_warehouse\excel_etl_pipeline\bronze_layer\bronze_data\bronze\tables\bronze_sales_cash_invoices_summarized"
    output_dir = r"C:\pharma_warehouse\excel_etl_pipeline\reports"

    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(
        output_dir, f"monthly_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    )

    print("Reading data...")
    df = read_parquet_files_safely(input_dir)

    if df is None:
        print("No data found!")
        return

    print(f"Loaded {len(df):,} records with {len(df.columns)} columns")

    # Extract columns
    col_map = extract_columns(df)
    print(f"\nFound columns:")
    for key, col in col_map.items():
        print(f"  {key}: {col}")

    # Prepare data
    data = {}
    for key, col_name in col_map.items():
        if col_name in df.columns:
            data[key] = df[col_name]
        else:
            # Create default values
            if key == "cus_code":
                data[key] = [f"CUST_{i}" for i in range(len(df))]
            elif key == "customer_name":
                data[key] = ["Unknown Customer"] * len(df)
            elif key == "amount":
                data[key] = [0] * len(df)
            elif key == "date":
                data[key] = [datetime.now()] * len(df)
            elif key == "branch":
                data[key] = ["HQ"] * len(df)

    # Create processed dataframe
    processed_df = pd.DataFrame(
        {
            "CUS_CODE": data["cus_code"],
            "CUSTOMER_NAME": data["customer_name"],
            "AMOUNT": pd.to_numeric(data["amount"], errors="coerce").fillna(0),
            "TRN_DATE": pd.to_datetime(data["date"], errors="coerce"),
            "BRANCH_NAME": data["branch"],
        }
    )

    # Extract year and month
    processed_df["YEAR"] = processed_df["TRN_DATE"].dt.year.fillna(datetime.now().year)
    processed_df["MONTH"] = processed_df["TRN_DATE"].dt.month.fillna(1)
    processed_df["MONTH_YEAR"] = processed_df.apply(
        lambda x: f"{int(x['MONTH']):02d}/{int(x['YEAR'])%100:02d}", axis=1
    )

    print(f"\nData ready:")
    print(f"  - {len(processed_df):,} records")
    print(f"  - {processed_df['BRANCH_NAME'].nunique()} branches")
    print(f"  - {processed_df['CUS_CODE'].nunique()} customers")

    # Generate report
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        for branch in processed_df["BRANCH_NAME"].unique():
            branch_df = processed_df[processed_df["BRANCH_NAME"] == branch]

            # Pivot table
            pivot = branch_df.pivot_table(
                index=["CUS_CODE", "CUSTOMER_NAME"],
                columns="MONTH_YEAR",
                values="AMOUNT",
                aggfunc="sum",
                fill_value=0,
            ).reset_index()

            # Calculate total
            pivot["Total"] = pivot.select_dtypes(include=[np.number]).sum(axis=1)

            # Get top 20
            pivot = pivot.nlargest(20, "Total")

            # Sort monthly columns
            month_cols = sorted([col for col in pivot.columns if "/" in col])
            cols_order = ["CUS_CODE", "CUSTOMER_NAME"] + month_cols + ["Total"]
            pivot = pivot[[col for col in cols_order if col in pivot.columns]]

            # Create sheet name
            sheet_name = (
                str(branch)[:31]
                .replace("/", "_")
                .replace("\\", "_")
                .replace("*", "_")
                .replace("?", "_")
                .replace(":", "_")
                .replace("[", "_")
                .replace("]", "_")
            )

            pivot.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"  Created sheet: {sheet_name}")

    print(f"\nReport saved to: {output_file}")


if __name__ == "__main__":
    generate_report()
