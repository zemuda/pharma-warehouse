# query_top_customers.py
import pandas as pd
from deltalake import DeltaTable
from datetime import datetime
import json


def get_top_customers_per_branch_month():
    """Get top 20 customers per branch per month"""

    # Path to your bronze table
    delta_path = "C:/pharma_warehouse/excel_etl_pipeline/bronze_layer/bronze_data/bronze/tables/bronze_sales_cash_invoices_summarized"

    try:
        # Load Delta table
        dt = DeltaTable(delta_path)
        df = dt.to_pandas()

        # Ensure proper data types
        df["TRN_DATE"] = pd.to_datetime(df["TRN_DATE"])
        df["AMOUNT"] = pd.to_numeric(df["AMOUNT"], errors="coerce")

        # Create month column
        df["year_month"] = df["TRN_DATE"].dt.to_period("M")

        # Group by branch, month, customer
        grouped = (
            df.groupby(["BRANCH_NAME", "year_month", "CUS_CODE", "CUSTOMER_NAME"])
            .agg(
                total_amount=("AMOUNT", "sum"),
                invoice_count=("DOCUMENT_NUMBER", "nunique"),
                first_purchase=("TRN_DATE", "min"),
                last_purchase=("TRN_DATE", "max"),
            )
            .reset_index()
        )

        # Rank customers within each branch-month
        grouped["rank_by_amount"] = grouped.groupby(["BRANCH_NAME", "year_month"])[
            "total_amount"
        ].rank(method="dense", ascending=False)
        grouped["rank_by_invoices"] = grouped.groupby(["BRANCH_NAME", "year_month"])[
            "invoice_count"
        ].rank(method="dense", ascending=False)

        # Get top 20 per branch per month
        top_customers = grouped[grouped["rank_by_amount"] <= 20].copy()

        # Calculate additional metrics
        top_customers["avg_invoice_amount"] = (
            top_customers["total_amount"] / top_customers["invoice_count"]
        )

        # Sort for output
        top_customers = top_customers.sort_values(
            ["BRANCH_NAME", "year_month", "rank_by_amount"]
        )

        return top_customers

    except Exception as e:
        print(f"❌ Error querying data: {e}")
        return pd.DataFrame()


def analyze_customer_trends(df):
    """Analyze customer trends from the data"""
    if df.empty:
        return

    print("\n" + "=" * 80)
    print("📊 TOP CUSTOMER ANALYSIS")
    print("=" * 80)

    # Overall top customers across all branches
    overall_top = (
        df.groupby(["CUS_CODE", "CUSTOMER_NAME"])
        .agg(
            total_amount=("total_amount", "sum"),
            months_active=("year_month", "nunique"),
        )
        .reset_index()
    )

    overall_top = overall_top.sort_values("total_amount", ascending=False).head(20)

    print(f"\n🏆 OVERALL TOP 20 CUSTOMERS (All Time):")
    print("-" * 80)
    for idx, row in overall_top.iterrows():
        print(
            f"{idx+1:2}. {row['CUSTOMER_NAME'][:30]:30} "
            f"{row['total_amount']:12,.0f} KES "
            f"({row['months_active']} months)"
        )

    # Recent month analysis
    recent_month = df["year_month"].max()
    recent_data = df[df["year_month"] == recent_month]

    if not recent_data.empty:
        print(f"\n📅 RECENT MONTH ({recent_month}):")
        print("-" * 80)

        # Top branches by sales
        branch_sales = (
            recent_data.groupby("BRANCH_NAME")["total_amount"].sum().reset_index()
        )
        branch_sales = branch_sales.sort_values("total_amount", ascending=False)

        for branch in branch_sales["BRANCH_NAME"].unique():
            branch_data = recent_data[recent_data["BRANCH_NAME"] == branch]
            print(f"\n🏢 BRANCH: {branch}")
            print(f"   Total Sales: {branch_data['total_amount'].sum():,.0f} KES")
            print(f"   Unique Customers: {branch_data['CUS_CODE'].nunique()}")

            top_5 = branch_data.head(5)
            for _, customer in top_5.iterrows():
                print(
                    f"   {customer['rank_by_amount']:2}. {customer['CUSTOMER_NAME'][:25]:25} "
                    f"{customer['total_amount']:10,.0f} KES "
                    f"({customer['invoice_count']} invoices)"
                )


def export_to_excel(df, filename="top_customers_analysis.xlsx"):
    """Export results to Excel with formatting"""
    if df.empty:
        return

    with pd.ExcelWriter(filename, engine="openpyxl") as writer:
        # Summary sheet
        summary = df.pivot_table(
            index=["BRANCH_NAME", "CUSTOMER_NAME"],
            columns="year_month",
            values="total_amount",
            aggfunc="sum",
            fill_value=0,
        )
        summary["Total"] = summary.sum(axis=1)
        summary = summary.sort_values("Total", ascending=False)
        summary.to_excel(writer, sheet_name="Summary")

        # Monthly details
        for (branch, month), group in df.groupby(["BRANCH_NAME", "year_month"]):
            sheet_name = f"{branch}_{str(month)}"[:31]  # Excel sheet name limit
            group_sorted = group.sort_values("rank_by_amount")
            group_sorted.to_excel(writer, sheet_name=sheet_name, index=False)

        # Customer trends
        trends = (
            df.groupby(["CUSTOMER_NAME", "year_month"])["total_amount"].sum().unstack()
        )
        trends.to_excel(writer, sheet_name="Customer_Trends")

    print(f"\n💾 Results exported to: {filename}")


def main():
    """Main execution"""
    print("🔍 Analyzing top customers per branch per month...")

    # Get top customers
    top_customers_df = get_top_customers_per_branch_month()

    if top_customers_df.empty:
        print("❌ No data found or error loading data")
        return

    print(f"\n✅ Found {len(top_customers_df):,} customer-month records")
    print(f"   Branches: {top_customers_df['BRANCH_NAME'].nunique()}")
    print(f"   Months: {top_customers_df['year_month'].nunique()}")
    print(f"   Unique Customers: {top_customers_df['CUS_CODE'].nunique()}")

    # Display sample
    print(f"\n📋 SAMPLE DATA (first 5 records):")
    print(
        top_customers_df[
            [
                "BRANCH_NAME",
                "year_month",
                "CUSTOMER_NAME",
                "total_amount",
                "rank_by_amount",
            ]
        ]
        .head()
        .to_string()
    )

    # Analyze trends
    analyze_customer_trends(top_customers_df)

    # Export to Excel
    export_to_excel(top_customers_df)

    # Save as JSON for dashboards
    top_customers_df.to_json("top_customers.json", orient="records", date_format="iso")
    print(f"\n💾 JSON data saved to: top_customers.json")

    # Generate SQL for dashboard
    generate_dashboard_sql()


def generate_dashboard_sql():
    """Generate SQL for Power BI/Tableau dashboard"""
    sql = """
-- DASHBOARD QUERIES FOR TOP CUSTOMER ANALYSIS

-- 1. Monthly Branch Performance
SELECT 
    BRANCH_NAME,
    DATE_TRUNC('month', TRN_DATE) as month,
    COUNT(DISTINCT CUS_CODE) as unique_customers,
    COUNT(DISTINCT DOCUMENT_NUMBER) as total_invoices,
    SUM(AMOUNT) as total_sales,
    AVG(AMOUNT) as avg_invoice_value
FROM bronze_sales_cash_invoices_summarized
WHERE BRANCH_NAME IS NOT NULL
GROUP BY BRANCH_NAME, DATE_TRUNC('month', TRN_DATE)
ORDER BY month DESC, total_sales DESC;

-- 2. Top 20 Customers per Branch (Current Month)
WITH current_month AS (
    SELECT DATE_TRUNC('month', MAX(TRN_DATE)) as max_month
    FROM bronze_sales_cash_invoices_summarized
),
branch_customers AS (
    SELECT 
        b.BRANCH_NAME,
        b.CUS_CODE,
        b.CUSTOMER_NAME,
        SUM(b.AMOUNT) as monthly_sales,
        COUNT(DISTINCT b.DOCUMENT_NUMBER) as invoice_count
    FROM bronze_sales_cash_invoices_summarized b
    CROSS JOIN current_month cm
    WHERE DATE_TRUNC('month', b.TRN_DATE) = cm.max_month
      AND b.BRANCH_NAME IS NOT NULL
    GROUP BY b.BRANCH_NAME, b.CUS_CODE, b.CUSTOMER_NAME
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY BRANCH_NAME ORDER BY monthly_sales DESC) as rank
    FROM branch_customers
)
SELECT *
FROM ranked
WHERE rank <= 20
ORDER BY BRANCH_NAME, rank;

-- 3. Customer Loyalty Analysis
SELECT 
    CUS_CODE,
    CUSTOMER_NAME,
    MIN(TRN_DATE) as first_purchase,
    MAX(TRN_DATE) as last_purchase,
    COUNT(DISTINCT DOCUMENT_NUMBER) as total_invoices_lifetime,
    SUM(AMOUNT) as lifetime_value,
    COUNT(DISTINCT DATE_TRUNC('month', TRN_DATE)) as active_months,
    COUNT(DISTINCT BRANCH_NAME) as branches_purchased_from
FROM bronze_sales_cash_invoices_summarized
WHERE CUS_CODE IS NOT NULL
GROUP BY CUS_CODE, CUSTOMER_NAME
HAVING COUNT(DISTINCT DOCUMENT_NUMBER) >= 5  -- At least 5 invoices
ORDER BY lifetime_value DESC
LIMIT 50;

-- 4. Monthly Customer Retention
WITH monthly_customers AS (
    SELECT 
        DATE_TRUNC('month', TRN_DATE) as month,
        CUS_CODE,
        BRANCH_NAME
    FROM bronze_sales_cash_invoices_summarized
    WHERE CUS_CODE IS NOT NULL
    GROUP BY DATE_TRUNC('month', TRN_DATE), CUS_CODE, BRANCH_NAME
),
retention AS (
    SELECT 
        mc1.month as current_month,
        mc2.month as previous_month,
        mc1.BRANCH_NAME,
        COUNT(DISTINCT mc1.CUS_CODE) as current_customers,
        COUNT(DISTINCT CASE WHEN mc2.CUS_CODE IS NOT NULL THEN mc1.CUS_CODE END) as retained_customers,
        COUNT(DISTINCT CASE WHEN mc2.CUS_CODE IS NULL THEN mc1.CUS_CODE END) as new_customers
    FROM monthly_customers mc1
    LEFT JOIN monthly_customers mc2 
        ON mc1.CUS_CODE = mc2.CUS_CODE 
        AND mc1.BRANCH_NAME = mc2.BRANCH_NAME
        AND mc2.month = DATE_TRUNC('month', mc1.month - INTERVAL '1 month')
    GROUP BY mc1.month, mc1.BRANCH_NAME
)
SELECT *,
    ROUND(retained_customers * 100.0 / NULLIF(LAG(current_customers) OVER (
        PARTITION BY BRANCH_NAME ORDER BY current_month
    ), 0), 2) as retention_rate_percent
FROM retention
ORDER BY BRANCH_NAME, current_month DESC;
"""

    with open("dashboard_queries.sql", "w") as f:
        f.write(sql)

    print(f"\n📊 Dashboard SQL queries saved to: dashboard_queries.sql")


if __name__ == "__main__":
    main()
