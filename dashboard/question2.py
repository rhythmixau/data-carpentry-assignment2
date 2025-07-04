import streamlit as st
import duckdb
from constants import DUCKDB_FILE
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from plotly import data
import math


st.markdown("## Q2 - Which customers were most loyal for each business?")
st.markdown("Let's start by defining what do we mean by loyal customer.  Is loyal customer someone who bring in the "
            "most business?  Or is it someone who visit the business the most")


def show_bar_graph(receipts_df, header="Some Title"):
    company_titles = [
        "Ed's Barber Supplies",
        'Penguin Swim School',
        'Wake Up with Coffee',
        'Please Bring Pizza Pronto'
    ]
    colours = ["#fb5607", "#ff006e", "#8338ec", "#3a86ff", ]
    fig = make_subplots(rows=2, cols=2, shared_yaxes=True, subplot_titles=company_titles,
                        vertical_spacing=0.35, row_heights=[0.8, 0.8,])

    business_name = company_titles[0]
    df = receipts_df[receipts_df['business_name'] == business_name]
    fig.add_trace(
        go.Bar(x=df.customer_name, y=df.amount_spent, text=df.amount_spent, textposition='outside', texttemplate='%{text:.2s}',
               name=business_name, marker=dict(cornerradius=30, color=colours[0])), row=1, col=1
    )

    business_name = company_titles[1]
    df = receipts_df[receipts_df['business_name'] == business_name]
    fig.add_trace(
        go.Bar(x=df.customer_name, y=df.amount_spent, text=df.amount_spent, textposition='outside', texttemplate='%{text:.2s}',
               name=business_name, marker=dict(cornerradius=30, color=colours[1])), row=1, col=2
    )

    business_name = company_titles[2]
    df = receipts_df[receipts_df['business_name'] == business_name]
    fig.add_trace(
        go.Bar(x=df.customer_name, y=df.amount_spent, text=df.amount_spent, textposition='outside', texttemplate='%{text:.2s}',
               name=business_name, marker=dict(cornerradius=30, color=colours[2])), row=2, col=1
    )

    business_name = company_titles[3]
    df = receipts_df[receipts_df['business_name'] == business_name]
    fig.add_trace(
        go.Bar(x=df.customer_name,
               y=df.amount_spent,
               text=df.amount_spent, textposition='outside', texttemplate='%{text:.2s}',
               name=business_name,
               marker=dict(cornerradius=30, color=colours[3]),
               ),
               row=2, col=2
    )
    fig.update_layout(title_text=header, height=600, showlegend=False)
    st.plotly_chart(fig, use_container_width=True)



st.markdown("#### Top 10 Customers by Amount Spent")

with duckdb.connect(DUCKDB_FILE) as conn:
    top_ten_by_amount_spent_df = conn.sql("SELECT * FROM receipts.main.top_ten_customers_by_business_value").df()

st.dataframe(top_ten_by_amount_spent_df)
show_bar_graph(top_ten_by_amount_spent_df, header="Top Ten Customers by the Amount Spent")

st.markdown("#### Top 10 Customers by Number of Purchases")
with duckdb.connect(DUCKDB_FILE) as conn:
    top_ten_by_num_purchases_df = conn.sql("SELECT * FROM receipts.main.top_ten_customers_by_num_purchases").df()

st.dataframe(top_ten_by_num_purchases_df)
show_bar_graph(top_ten_by_num_purchases_df, header="Top 10 Customers by Number of Purchases")

