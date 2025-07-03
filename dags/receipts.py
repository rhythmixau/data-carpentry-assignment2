from airflow.sdk import asset, Asset, Context
from airflow.decorators import dag, task
import duckdb
import pandas as pd
import os
import csv
import json
from constants import CREATE_RECEIPTS_QUERY, CREATE_CASHIER_QUERY, CREATE_BUSINESS_QUERY, CREATE_CUSTOMER_QUERY, \
    CREATE_RECEIPT_PAYMENTS_QUERY, CREATE_RECEIPT_PRODUCTS_QUERY, CREATE_RECEIPT_PROMOTIONS_QUERY


@asset(
    schedule="@daily",
    uri="data/receipts/"
)
def receipts(self) -> list[dict]:
    files = os.listdir(self.uri)
    data = []
    for file in files:
        if file.endswith('.csv'):
            with open(os.path.join(self.uri, file), 'r') as f:
                csv_data = csv.reader(f)
                for row in csv_data:
                    data.append(row)
        elif file.endswith('.json'):
            with open(os.path.join(self.uri, file), 'r') as f:
                json_data = json.load(f)
                data.append(json_data)

    return data


@asset(
    schedule=receipts,
)
def stg_receipts(receipts, context: Context):
    """
    CREATE STAGING TABLE stg_receipts
    """
    receipts_data = context['ti'].xcom_pull(
        dag_id=receipts.name,
        task_ids=receipts.name,
        include_prior_dates=True,
    )
    receipts_df = pd.DataFrame(receipts_data)
    conn = duckdb.connect("dbt/airbnb/receipts.duckdb")
    conn.sql(
        f"""CREATE OR REPLACE TABLE receipts.main.stg_receipts AS 
                SELECT * FROM receipts_df;"""
    )

    num_rows = conn.sql("SELECT COUNT(*) AS NUM_ROWS FROM receipts.main.stg_receipts;").df()
    print(f"Number of rows: {num_rows["NUM_ROWS"]}")

#
# @dag(schedule=receipts)
# def cleaning_up_receipts_db():
#     @task
#     def drop_tables():
#         with duckdb.connect("data/receipts.duckdb") as conn:
#             conn.sql("""
#             DROP TABLE IF EXISTS receipt_businesses;
#             DROP TABLE IF EXISTS receipt_cashiers;
#             DROP TABLE IF EXISTS receipt_customers;
#             DROP TABLE IF EXISTS receipt_payments;
#             DROP TABLE IF EXISTS receipt_products;
#             DROP TABLE IF EXISTS receipt_promotions;
#             DROP TABLE IF EXISTS receipts;
#             DROP TABLE IF EXISTS businesses_dim;
#             DROP TABLE IF EXISTS cashiers_dim;
#             DROP TABLE IF EXISTS sales;
#             DROP TABLE IF EXISTS sales_details;
#             """)
#
#     @task
#     def create_tables():
#         with duckdb.connect("data/receipts.duckdb") as conn:
#             conn.sql(CREATE_RECEIPTS_QUERY)
#             conn.sql(CREATE_CASHIER_QUERY)
#             conn.sql(CREATE_BUSINESS_QUERY)
#             conn.sql(CREATE_CUSTOMER_QUERY)
#             conn.sql(CREATE_RECEIPT_PAYMENTS_QUERY)
#             conn.sql(CREATE_RECEIPT_PRODUCTS_QUERY)
#             conn.sql(CREATE_RECEIPT_PROMOTIONS_QUERY)
#
#
#     drop_tables() >> create_tables()

# cleaning_up_receipts_db()

