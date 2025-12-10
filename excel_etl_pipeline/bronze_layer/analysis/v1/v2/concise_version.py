# concise_version.py
import pandas as pd
import glob
import os
from datetime import datetime
import numpy as np


def generate_monthly_report():
    """
    Generate monthly sales report for top 20 customers per branch
    """
    # Configuration
    source_dir = r"C:\pharma_warehouse\excel_etl_pipeline\bronze_layer\bronze_data\bronze\tables\bronze_sales_cash_invoices_summarized"
    output_file = r"C:\pharma_warehouse\excel_etl_pipeline\bronze_layer\bronze_data\reports\monthly_sales_report.xlsx"

    # Create output directory
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    print("Reading parquet files...")

    # Read all parquet files
    files = glob.glob(os.path.join(source_dir, "*.parquet"))
    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)

    print(f"Loaded {len(df):,} records")

    # Standardize column names
    df = df.rename(columns=lambda x: str(x).upper().replace(" ", "_"))

    # Map columns
    col_map = {}
    for col in df.columns:
        if "CUS_CODE" in col or "CUSTOMER_CODE" in col:
            col_map[col] = "CUS_CODE"
        elif "CUSTOMER_NAME" in col or "CUS_NAME" in col:
            col_map[col] = "CUSTOMER_NAME"
        elif "AMOUNT" in col:
            col_map[col] = "AMOUNT"
        elif "DATE" in col or "TRN_DATE" in col:
            col_map[col] = "TRN_DATE"
        elif "BRANCH" in col and "NAME" in col:
            col_map[col] = "BRANCH_NAME"

    df = df.rename(columns=col_map)

    # Process data
    df["TRN_DATE"] = pd.to_datetime(df["TRN_DATE"], errors="coerce")
    df["YEAR"] = df["TRN_DATE"].dt.year.fillna(datetime.now().year)
    df["MONTH"] = df["TRN_DATE"].dt.month.fillna(1)
    df["AMOUNT"] = pd.to_numeric(df["AMOUNT"], errors="coerce").fillna(0)
    df["CUS_CODE"] = df["CUS_CODE"].fillna("UNKNOWN")
    df["CUSTOMER_NAME"] = df["CUSTOMER_NAME"].fillna("Unknown")
    df["BRANCH_NAME"] = df["BRANCH_NAME"].fillna("HQ")

    # Create Year-Month column in MM/YY format
    df["MONTH_YY"] = (
        df["MONTH"].astype(str).str.zfill(2) + "/" + df["YEAR"].astype(str).str[-2:]
    )

    print("Generating report...")

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        # Process each branch
        for branch in df["BRANCH_NAME"].unique():
            branch_df = df[df["BRANCH_NAME"] == branch]

            # Get top 20 customers by total sales
            top_customers = (
                branch_df.groupby(["CUS_CODE", "CUSTOMER_NAME"])["AMOUNT"]
                .sum()
                .nlargest(20)
                .reset_index()
            )

            # Create pivot table
            pivot = branch_df.pivot_table(
                index=["CUS_CODE", "CUSTOMER_NAME"],
                columns="MONTH_YY",
                values="AMOUNT",
                aggfunc="sum",
                fill_value=0,
            ).reset_index()

            # Filter to top 20 customers
            pivot = pivot.merge(top_customers[["CUS_CODE"]], on="CUS_CODE")

            # Calculate total
            numeric_cols = pivot.select_dtypes(include=[np.number]).columns
            pivot["Total"] = pivot[numeric_cols].sum(axis=1)

            # Sort by total descending
            pivot = pivot.sort_values("Total", ascending=False)

            # Reorder columns
            month_cols = sorted([col for col in pivot.columns if "/" in col])
            cols_order = ["CUS_CODE", "CUSTOMER_NAME"] + month_cols + ["Total"]
            pivot = pivot[[col for col in cols_order if col in pivot.columns]]

            # Write to sheet
            sheet_name = str(branch)[:31]
            pivot.to_excel(writer, sheet_name=sheet_name, index=False)

            print(f"  Created sheet: {sheet_name}")

    print(f"\nReport saved to: {output_file}")
    return output_file


if __name__ == "__main__":
    generate_monthly_report()
