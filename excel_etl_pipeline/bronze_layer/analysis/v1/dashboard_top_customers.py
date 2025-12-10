# dashboard_top_customers.py
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from deltalake import DeltaTable

# Page config
st.set_page_config(
    page_title="Pharma Customer Analytics", page_icon="🏥", layout="wide"
)

# Title
st.title("🏥 Pharma Warehouse - Top Customer Analysis")
st.markdown("### Top 20 Customers per Branch per Month")


@st.cache_data
def load_data():
    """Load data from Delta table"""
    delta_path = "C:/pharma_warehouse/excel_etl_pipeline/bronze_layer/bronze_data/bronze/tables/bronze_sales_cash_invoices_summarized"
    dt = DeltaTable(delta_path)
    df = dt.to_pandas()

    # Clean and prepare data
    df["TRN_DATE"] = pd.to_datetime(df["TRN_DATE"])
    df["year_month"] = df["TRN_DATE"].dt.to_period("M").astype(str)
    df["month_name"] = df["TRN_DATE"].dt.strftime("%B %Y")

    return df


def main():
    # Load data
    df = load_data()

    # Sidebar filters
    st.sidebar.header("Filters")

    # Branch filter
    branches = sorted(df["BRANCH_NAME"].dropna().unique())
    selected_branches = st.sidebar.multiselect(
        "Select Branches",
        branches,
        default=branches[:3] if len(branches) > 3 else branches,
    )

    # Date range filter
    min_date = df["TRN_DATE"].min().date()
    max_date = df["TRN_DATE"].max().date()

    date_range = st.sidebar.date_input(
        "Date Range", value=(min_date, max_date), min_value=min_date, max_value=max_date
    )

    # Apply filters
    if len(date_range) == 2:
        mask = (
            (df["BRANCH_NAME"].isin(selected_branches))
            & (df["TRN_DATE"].dt.date >= date_range[0])
            & (df["TRN_DATE"].dt.date <= date_range[1])
        )
        filtered_df = df[mask].copy()
    else:
        filtered_df = df[df["BRANCH_NAME"].isin(selected_branches)].copy()

    # Main content
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Total Sales", f"KES {filtered_df['AMOUNT'].sum():,.0f}")

    with col2:
        st.metric("Total Invoices", filtered_df["DOCUMENT_NUMBER"].nunique())

    with col3:
        st.metric("Unique Customers", filtered_df["CUS_CODE"].nunique())

    # Tab layout
    tab1, tab2, tab3, tab4 = st.tabs(
        ["📊 Monthly Rankings", "📈 Trends", "🏆 Top Performers", "🔍 Customer Details"]
    )

    with tab1:
        st.subheader("Monthly Customer Rankings")

        # Calculate monthly rankings
        monthly_group = (
            filtered_df.groupby(
                ["BRANCH_NAME", "year_month", "month_name", "CUS_CODE", "CUSTOMER_NAME"]
            )
            .agg(
                total_amount=("AMOUNT", "sum"),
                invoice_count=("DOCUMENT_NUMBER", "nunique"),
            )
            .reset_index()
        )

        # Rank within each branch-month
        monthly_group["rank"] = monthly_group.groupby(["BRANCH_NAME", "year_month"])[
            "total_amount"
        ].rank(method="dense", ascending=False)

        # Filter top 20
        top_monthly = monthly_group[monthly_group["rank"] <= 20].copy()

        # Display as pivot table
        pivot_table = top_monthly.pivot_table(
            index=["BRANCH_NAME", "CUSTOMER_NAME"],
            columns="month_name",
            values="rank",
            aggfunc="first",
        )

        st.dataframe(pivot_table, use_container_width=True, height=600)

    with tab2:
        st.subheader("Sales Trends")

        col1, col2 = st.columns(2)

        with col1:
            # Monthly sales trend
            monthly_sales = (
                filtered_df.groupby(["year_month", "BRANCH_NAME"])["AMOUNT"]
                .sum()
                .reset_index()
            )

            fig = px.line(
                monthly_sales,
                x="year_month",
                y="AMOUNT",
                color="BRANCH_NAME",
                title="Monthly Sales by Branch",
                labels={"AMOUNT": "Total Sales (KES)", "year_month": "Month"},
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Top customers trend
            top_customers_all_time = (
                filtered_df.groupby("CUSTOMER_NAME")["AMOUNT"]
                .sum()
                .nlargest(10)
                .index.tolist()
            )

            customer_trend = filtered_df[
                filtered_df["CUSTOMER_NAME"].isin(top_customers_all_time)
            ]
            customer_monthly = (
                customer_trend.groupby(["year_month", "CUSTOMER_NAME"])["AMOUNT"]
                .sum()
                .reset_index()
            )

            fig = px.line(
                customer_monthly,
                x="year_month",
                y="AMOUNT",
                color="CUSTOMER_NAME",
                title="Top 10 Customers - Monthly Trend",
                labels={"AMOUNT": "Sales (KES)", "year_month": "Month"},
            )
            st.plotly_chart(fig, use_container_width=True)

    with tab3:
        st.subheader("Top Performers Analysis")

        # Calculate overall rankings
        customer_summary = (
            filtered_df.groupby(["CUS_CODE", "CUSTOMER_NAME", "BRANCH_NAME"])
            .agg(
                total_amount=("AMOUNT", "sum"),
                invoice_count=("DOCUMENT_NUMBER", "nunique"),
                first_purchase=("TRN_DATE", "min"),
                last_purchase=("TRN_DATE", "max"),
                active_months=("year_month", "nunique"),
            )
            .reset_index()
        )

        customer_summary["avg_invoice"] = (
            customer_summary["total_amount"] / customer_summary["invoice_count"]
        )

        # Top 20 overall
        top_overall = customer_summary.sort_values(
            "total_amount", ascending=False
        ).head(20)

        # Display as table
        st.dataframe(
            top_overall[
                [
                    "CUSTOMER_NAME",
                    "BRANCH_NAME",
                    "total_amount",
                    "invoice_count",
                    "avg_invoice",
                    "active_months",
                ]
            ],
            column_config={
                "total_amount": st.column_config.NumberColumn(
                    "Total Sales", format="KES %,.0f"
                ),
                "avg_invoice": st.column_config.NumberColumn(
                    "Avg Invoice", format="KES %,.0f"
                ),
            },
            use_container_width=True,
            height=400,
        )

        # Bar chart of top customers
        fig = px.bar(
            top_overall.head(10),
            x="CUSTOMER_NAME",
            y="total_amount",
            color="BRANCH_NAME",
            title="Top 10 Customers by Total Sales",
            labels={"total_amount": "Total Sales (KES)", "CUSTOMER_NAME": "Customer"},
        )
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

    with tab4:
        st.subheader("Customer Detail View")

        # Customer selector
        customers = sorted(filtered_df["CUSTOMER_NAME"].unique())
        selected_customer = st.selectbox("Select Customer", customers)

        if selected_customer:
            customer_data = filtered_df[
                filtered_df["CUSTOMER_NAME"] == selected_customer
            ].copy()

            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric("Total Spent", f"KES {customer_data['AMOUNT'].sum():,.0f}")

            with col2:
                st.metric("Total Invoices", customer_data["DOCUMENT_NUMBER"].nunique())

            with col3:
                st.metric("First Purchase", customer_data["TRN_DATE"].min().date())

            with col4:
                st.metric("Last Purchase", customer_data["TRN_DATE"].max().date())

            # Monthly trend for selected customer
            customer_monthly = (
                customer_data.groupby("year_month")
                .agg(
                    monthly_sales=("AMOUNT", "sum"),
                    invoice_count=("DOCUMENT_NUMBER", "nunique"),
                )
                .reset_index()
            )

            fig = go.Figure()
            fig.add_trace(
                go.Bar(
                    x=customer_monthly["year_month"],
                    y=customer_monthly["monthly_sales"],
                    name="Monthly Sales",
                    marker_color="royalblue",
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=customer_monthly["year_month"],
                    y=customer_monthly["invoice_count"],
                    name="Invoice Count",
                    yaxis="y2",
                    mode="lines+markers",
                    line=dict(color="firebrick"),
                )
            )

            fig.update_layout(
                title=f"Monthly Activity for {selected_customer}",
                xaxis_title="Month",
                yaxis_title="Sales (KES)",
                yaxis2=dict(title="Invoice Count", overlaying="y", side="right"),
                hovermode="x unified",
            )

            st.plotly_chart(fig, use_container_width=True)

            # Recent transactions
            st.subheader("Recent Transactions")
            recent_tx = customer_data.sort_values("TRN_DATE", ascending=False).head(20)
            st.dataframe(
                recent_tx[
                    ["TRN_DATE", "DOCUMENT_NUMBER", "AMOUNT", "BRANCH_NAME", "COMMENTS"]
                ],
                use_container_width=True,
            )


if __name__ == "__main__":
    main()
