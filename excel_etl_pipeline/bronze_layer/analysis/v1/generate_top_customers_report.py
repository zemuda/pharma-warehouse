import pandas as pd
import numpy as np
from datetime import datetime
import os
from pathlib import Path
import glob


def read_parquet_data(source_directory):
    """
    Read all parquet files from the specified directory
    """
    parquet_files = glob.glob(os.path.join(source_directory, "*.parquet"))

    if not parquet_files:
        print(f"No parquet files found in {source_directory}")
        return None

    print(f"Found {len(parquet_files)} parquet files")

    # Read and concatenate all parquet files
    data_frames = []
    for file_path in parquet_files:
        try:
            df = pd.read_parquet(file_path)
            data_frames.append(df)
            print(f"Successfully read: {os.path.basename(file_path)}")
        except Exception as e:
            print(f"Error reading {file_path}: {e}")

    if not data_frames:
        print("No data could be read from parquet files")
        return None

    # Combine all dataframes
    combined_df = pd.concat(data_frames, ignore_index=True)
    print(f"Total records loaded: {len(combined_df):,}")

    return combined_df


def process_sales_data(df):
    """
    Process the sales data to extract required information
    """
    # Display column names for debugging
    print("\nAvailable columns in the dataset:")
    for col in df.columns:
        print(f"  - {col}")

    # Standardize column names (handle different naming conventions)
    column_mapping = {
        # CUS_CODE variations
        "CUS_CODE": "CUS_CODE",
        "cus_code": "CUS_CODE",
        "customer_code": "CUS_CODE",
        "CustomerCode": "CUS_CODE",
        # CUSTOMER_NAME variations
        "CUSTOMER_NAME": "CUSTOMER_NAME",
        "customer_name": "CUSTOMER_NAME",
        "CustomerName": "CUSTOMER_NAME",
        "CUS_NAME": "CUSTOMER_NAME",
        # AMOUNT variations
        "AMOUNT": "AMOUNT",
        "amount": "AMOUNT",
        "TotalAmount": "AMOUNT",
        "AMOUNT_INCLUSIVE": "AMOUNT",
        # TRN_DATE variations
        "TRN_DATE": "TRN_DATE",
        "trn_date": "TRN_DATE",
        "transaction_date": "TRN_DATE",
        "TransactionDate": "TRN_DATE",
        "Date": "TRN_DATE",
        # BCODE variations
        "BCODE": "BCODE",
        "bcode": "BCODE",
        "branch_code": "BCODE",
        "BranchCode": "BCODE",
        # BRANCH_NAME variations
        "BRANCH_NAME": "BRANCH_NAME",
        "branch_name": "BRANCH_NAME",
        "BranchName": "BRANCH_NAME",
        # Year and Month variations
        "TRN_YEAR": "TRN_YEAR",
        "trn_year": "TRN_YEAR",
        "year": "TRN_YEAR",
        "Year": "TRN_YEAR",
        "TRN_MONTH": "TRN_MONTH",
        "trn_month": "TRN_MONTH",
        "month": "TRN_MONTH",
        "Month": "TRN_MONTH",
    }

    # Rename columns based on mapping (only if they exist)
    for old_name, new_name in column_mapping.items():
        if old_name in df.columns and new_name not in df.columns:
            df[new_name] = df[old_name]
            print(f"Mapped column: {old_name} -> {new_name}")

    # Ensure required columns exist
    required_columns = ["CUS_CODE", "CUSTOMER_NAME", "AMOUNT"]
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        print(f"\nWarning: Missing required columns: {missing_columns}")
        print("Attempting to find alternative columns...")

        # Try to find date column if TRN_DATE is missing
        if "TRN_DATE" not in df.columns:
            date_columns = [
                col for col in df.columns if "date" in col.lower() or "Date" in col
            ]
            if date_columns:
                df["TRN_DATE"] = df[date_columns[0]]
                print(f"Using '{date_columns[0]}' as TRN_DATE")

    # Parse dates
    if "TRN_DATE" in df.columns:
        try:
            df["TRN_DATE"] = pd.to_datetime(df["TRN_DATE"], errors="coerce")
            # Extract year and month if not already present
            if "TRN_YEAR" not in df.columns:
                df["TRN_YEAR"] = df["TRN_DATE"].dt.year
            if "TRN_MONTH" not in df.columns:
                df["TRN_MONTH"] = df["TRN_DATE"].dt.month
            print(f"Date range: {df['TRN_DATE'].min()} to {df['TRN_DATE'].max()}")
        except Exception as e:
            print(f"Error parsing dates: {e}")
    else:
        print("No date column found. Using placeholders for year and month.")
        df["TRN_YEAR"] = datetime.now().year
        df["TRN_MONTH"] = datetime.now().month

    # Handle missing branch information
    if "BRANCH_NAME" not in df.columns or df["BRANCH_NAME"].isnull().all():
        print("No branch information found. Using default branch.")
        df["BRANCH_NAME"] = "HQ"
        df["BCODE"] = 1

    # Fill missing values
    df["CUS_CODE"] = df["CUS_CODE"].fillna("UNKNOWN")
    df["CUSTOMER_NAME"] = df["CUSTOMER_NAME"].fillna("Unknown Customer")
    df["AMOUNT"] = pd.to_numeric(df["AMOUNT"], errors="coerce").fillna(0)

    print(f"\nData processing completed:")
    print(f"- Unique customers: {df['CUS_CODE'].nunique():,}")
    print(f"- Unique branches: {df['BRANCH_NAME'].nunique():,}")
    print(f"- Total transactions: {len(df):,}")
    print(f"- Total amount: {df['AMOUNT'].sum():,.2f}")

    return df


def generate_top_customers_report(df, output_file_path):
    """
    Generate Excel report with top 20 customers per branch per month
    """
    # Group by branch, month, and customer to get total amount per customer per month per branch
    customer_summary = (
        df.groupby(
            [
                "BCODE",
                "BRANCH_NAME",
                "TRN_YEAR",
                "TRN_MONTH",
                "CUS_CODE",
                "CUSTOMER_NAME",
            ]
        )["AMOUNT"]
        .sum()
        .reset_index()
    )

    # Get top 20 customers per branch per month
    top_customers_per_branch_month = (
        customer_summary.groupby(["BCODE", "BRANCH_NAME", "TRN_YEAR", "TRN_MONTH"])
        .apply(lambda x: x.nlargest(20, "AMOUNT"))
        .reset_index(drop=True)
    )

    # Add ranking column
    top_customers_per_branch_month["Rank"] = top_customers_per_branch_month.groupby(
        ["BCODE", "BRANCH_NAME", "TRN_YEAR", "TRN_MONTH"]
    )["AMOUNT"].rank(ascending=False, method="dense")

    # Create Excel writer
    with pd.ExcelWriter(output_file_path, engine="openpyxl") as writer:
        # Get unique branches
        branches = top_customers_per_branch_month["BRANCH_NAME"].unique()

        print(f"\nGenerating report for {len(branches)} branch(es)...")

        for branch in branches:
            branch_data = top_customers_per_branch_month[
                top_customers_per_branch_month["BRANCH_NAME"] == branch
            ]

            # Get unique months for this branch
            months = (
                branch_data[["TRN_YEAR", "TRN_MONTH"]]
                .drop_duplicates()
                .sort_values(["TRN_YEAR", "TRN_MONTH"])
            )

            # Create summary sheet for this branch
            summary_data = []

            print(f"Processing branch: {branch} ({len(months)} months)")

            for _, month_row in months.iterrows():
                year = int(month_row["TRN_YEAR"])
                month = int(month_row["TRN_MONTH"])
                month_name = datetime(year, month, 1).strftime("%B %Y")

                month_customers = branch_data[
                    (branch_data["TRN_YEAR"] == year)
                    & (branch_data["TRN_MONTH"] == month)
                ].sort_values("Rank")

                for _, customer_row in month_customers.iterrows():
                    summary_data.append(
                        {
                            "Branch": branch,
                            "Year": year,
                            "Month": month_name,
                            "Rank": int(customer_row["Rank"]),
                            "Customer Code": customer_row["CUS_CODE"],
                            "Customer Name": customer_row["CUSTOMER_NAME"],
                            "Total Amount": float(customer_row["AMOUNT"]),
                        }
                    )

            summary_df = pd.DataFrame(summary_data)

            # Write to Excel
            sheet_name = str(branch)[:31]  # Excel sheet names max 31 chars

            if not summary_df.empty:
                summary_df.to_excel(writer, sheet_name=sheet_name, index=False)

                # Auto-adjust column widths
                worksheet = writer.sheets[sheet_name]
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if (
                                cell.value is not None
                                and len(str(cell.value)) > max_length
                            ):
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width

                print(
                    f"  - Created sheet '{sheet_name}' with {len(summary_df)} records"
                )
            else:
                print(f"  - No data for branch '{branch}'")

        # Create summary statistics sheet
        create_summary_statistics(df, writer)

        # Create customer frequency analysis sheet
        create_frequency_analysis(df, writer)

    print(f"\nReport saved to: {output_file_path}")
    return output_file_path


def create_summary_statistics(df, writer):
    """
    Create a summary statistics sheet
    """
    stats_data = []

    # Overall statistics
    stats_data.append({"Metric": "Total Transactions", "Value": len(df)})

    stats_data.append({"Metric": "Total Amount", "Value": f"{df['AMOUNT'].sum():,.2f}"})

    stats_data.append({"Metric": "Unique Customers", "Value": df["CUS_CODE"].nunique()})

    stats_data.append(
        {"Metric": "Unique Branches", "Value": df["BRANCH_NAME"].nunique()}
    )

    stats_data.append(
        {
            "Metric": "Date Range",
            "Value": (
                f"{df['TRN_DATE'].min().date()} to {df['TRN_DATE'].max().date()}"
                if "TRN_DATE" in df.columns
                else "N/A"
            ),
        }
    )

    # Branch-wise statistics
    branch_stats = (
        df.groupby("BRANCH_NAME")
        .agg({"AMOUNT": ["sum", "count"], "CUS_CODE": "nunique"})
        .round(2)
    )

    branch_stats.columns = ["Total Amount", "Transaction Count", "Unique Customers"]
    branch_stats = branch_stats.reset_index()

    # Write statistics sheet
    stats_df = pd.DataFrame(stats_data)
    stats_df.to_excel(writer, sheet_name="Summary Statistics", index=False)

    # Write branch statistics
    branch_stats.to_excel(writer, sheet_name="Branch Statistics", index=False)

    # Auto-adjust column widths
    for sheet_name in ["Summary Statistics", "Branch Statistics"]:
        if sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if cell.value is not None and len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 30)
                worksheet.column_dimensions[column_letter].width = adjusted_width


def create_frequency_analysis(df, writer):
    """
    Create customer frequency analysis sheet
    """
    # Calculate customer frequency
    customer_analysis = (
        df.groupby(["CUS_CODE", "CUSTOMER_NAME", "BRANCH_NAME"])
        .agg({"AMOUNT": "sum", "TRN_DATE": "count"})
        .reset_index()
    )

    customer_analysis = customer_analysis.rename(
        columns={"AMOUNT": "Total Amount", "TRN_DATE": "Transaction Count"}
    )

    # Calculate frequency category
    def categorize_frequency(count):
        if count >= 20:
            return "Daily"
        elif count >= 8:
            return "Weekly"
        elif count >= 4:
            return "Bi-Weekly"
        else:
            return "Monthly"

    customer_analysis["Frequency"] = customer_analysis["Transaction Count"].apply(
        categorize_frequency
    )

    # Sort by total amount
    customer_analysis = customer_analysis.sort_values("Total Amount", ascending=False)

    # Add rank
    customer_analysis.insert(0, "Rank", range(1, len(customer_analysis) + 1))

    # Write to Excel
    customer_analysis.to_excel(writer, sheet_name="Customer Frequency", index=False)

    # Auto-adjust column widths
    if "Customer Frequency" in writer.sheets:
        worksheet = writer.sheets["Customer Frequency"]
        for column in worksheet.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if cell.value is not None and len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 25)
            worksheet.column_dimensions[column_letter].width = adjusted_width


def main():
    """
    Main function to orchestrate the data processing and report generation
    """
    # Configuration
    source_directory = r"C:\pharma_warehouse\excel_etl_pipeline\bronze_layer\bronze_data\bronze\tables\bronze_sales_cash_invoices_summarized"
    output_directory = (
        r"C:\pharma_warehouse\excel_etl_pipeline\bronze_layer\bronze_data\reports"
    )

    # Create output directory if it doesn't exist
    os.makedirs(output_directory, exist_ok=True)

    # Generate output file name with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(
        output_directory, f"top_customers_report_{timestamp}.xlsx"
    )

    print("=" * 60)
    print("PHARMA WAREHOUSE - TOP CUSTOMERS REPORT GENERATOR")
    print("=" * 60)

    # Step 1: Read data from parquet files
    print("\nStep 1: Reading data from parquet files...")
    df = read_parquet_data(source_directory)

    if df is None or df.empty:
        print("No data available. Exiting.")
        return

    # Step 2: Process the data
    print("\nStep 2: Processing data...")
    processed_df = process_sales_data(df)

    # Step 3: Generate report
    print("\nStep 3: Generating Excel report...")
    report_path = generate_top_customers_report(processed_df, output_file)

    print("\n" + "=" * 60)
    print("REPORT GENERATION COMPLETE!")
    print(f"Report saved to: {report_path}")
    print("=" * 60)

    # Optional: Display sample of the data
    print("\nSample of top customers:")
    top_customers = (
        processed_df.groupby(["CUSTOMER_NAME", "BRANCH_NAME"])["AMOUNT"]
        .sum()
        .nlargest(10)
        .reset_index()
    )
    for i, row in top_customers.iterrows():
        print(
            f"  {i+1:2d}. {row['CUSTOMER_NAME'][:30]:30} | {row['BRANCH_NAME']:10} | {row['AMOUNT']:12,.2f}"
        )


if __name__ == "__main__":
    main()
