import duckdb
import plotly.graph_objects as go
import streamlit as st


DUCKDB_FILE = "dbt/airbnb/receipts.duckdb"

st.markdown("### Questions")
st.markdown("1. Are there periods of the year where some businesses are more profitable?")
st.markdown("2. How much has inflation impacted the profit margin of each business?")
st.markdown("3. Which customers were most loyal for each business?")

st.markdown("### Raw Data")
with duckdb.connect(DUCKDB_FILE) as conn:
    raw_data = conn.sql("SELECT * FROM receipts.main.stg_receipts").df()
st.dataframe(raw_data)

st.markdown("#### Strategy")
st.markdown("1. First the table needs to be normalised.")
st.markdown("2. Then different visualisations will be used to gain understanding of the data.")
st.markdown("3. Finally, meanings and conclusion will be drawn to answer the above three questions.")

st.markdown("#### Normalised Tables")
with duckdb.connect(DUCKDB_FILE) as conn:
    receipts_df = conn.sql("SELECT * FROM receipts.main.receipts").df()
    receipt_businesses_df = conn.sql("SELECT * FROM receipts.main.receipt_businesses").df()
    receipt_cashiers_df = conn.sql("SELECT * FROM receipts.main.receipt_cashiers").df()
    receipt_customers_df = conn.sql("SELECT * FROM receipts.main.receipt_customers").df()
    receipt_payments_df = conn.sql("SELECT * FROM receipts.main.receipt_payments").df()
    receipt_products_df = conn.sql("SELECT * FROM receipts.main.receipt_products").df()
    receipt_promotions_df = conn.sql("SELECT * FROM receipts.main.receipt_promotions").df()

st.markdown("##### receipts")
st.dataframe(receipts_df)
row1_col1, row1_col2 = st.columns(2)
with row1_col1:
    st.markdown("##### receipt_businesses")
    st.dataframe(receipt_businesses_df)
with row1_col2:
    st.markdown("##### receipt_cashiers")
    st.dataframe(receipt_cashiers_df)
row2_col1, row2_col2 = st.columns(2)
with row2_col1:
    st.markdown("##### receipt_customers")
    st.dataframe(receipt_customers_df)
with row2_col2:
    st.markdown("##### receipt_payments")
    st.dataframe(receipt_payments_df)
row3_col1, row3_col2 = st.columns(2)
with row3_col1:
    st.markdown("##### receipt_products")
    st.dataframe(receipt_products_df)
with row3_col2:
    st.markdown("##### receipt_promotions")
    st.dataframe(receipt_promotions_df)

st.markdown("## Q1. Are there periods of the year where some businesses are more profitable?")
st.markdown("### 1. Sales by Businesses")
with duckdb.connect(DUCKDB_FILE) as conn:
    business_sales_df = conn.sql("SELECT * FROM receipts.main.business_sales").df()
    sale_profits_df = conn.sql("SELECT * FROM receipts.main.sale_profits").df()
    business_sales_profits_df = conn.sql("SELECT * FROM receipts.main.business_sales_profits").df()
st.dataframe(business_sales_df)
st.markdown("### 2. Sale Profits")
st.dataframe(sale_profits_df)
st.markdown("### 3. Business Sales and Profits")
st.dataframe(business_sales_profits_df)

st.markdown("### 4. Business Monthly Profits")
with duckdb.connect(DUCKDB_FILE) as conn:
    business_monthly_profits_df = conn.sql("SELECT * FROM receipts.main.business_monthly_profits").df()

business_monthly_profits_df["month-year"] = business_monthly_profits_df["month"].astype(str) + "-" + business_monthly_profits_df["year"].astype(str)
st.dataframe(business_monthly_profits_df)

businesses = business_monthly_profits_df["business_name"].unique()
business_colours = ["#70d6ff", "#ff70a6", "#ff9770", "#ffd670"]
fig = go.Figure()
for business, colour in zip(businesses, business_colours):
    business_profit_data = business_monthly_profits_df[
        business_monthly_profits_df["business_name"] == business]
    fig.add_trace(go.Scatter(x=business_profit_data["month-year"],
                             y=business_profit_data["total_monthly_profit"],
                             mode='lines+markers',
                             name=business,
                             line=dict(color=colour)
                             ))
fig.update_layout(
    title="Monthly Profits Across Businesses",
    xaxis_title="Months",
    yaxis_title="Total Profit ($)",
    xaxis_tickangle=270,
    legend_title="Businesses",
)
st.plotly_chart(fig, use_container_width=True)
st.markdown("Observation")
st.markdown("1. Ed's Barber Supplies is doing well year by year, and it is performing especially well during"
            "the first five months of the year.")
st.markdown("2. Please Bring Pizza Pronto is performing really well across the year and it is growing"
            "and doubling its profit every two years.")
st.markdown("3. Penguin Swim School performs worse in the middle of the year, which happens to be "
            "winter in Australia.")
st.markdown("4. Wake Up with Coffee doesn't grow as much as Ed's Barber Supplies or Please Bring Pizza Pronto,"
            "however, they it also doesn't fluctuate as much as Penguin Swim School either.")
