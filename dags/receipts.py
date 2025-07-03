from airflow.sdk import asset, Asset, Context
import duckdb
import pandas as pd
import os
import csv
import json


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
    conn = duckdb.connect("data/receipts.duckdb")
    conn.sql(
        f"""CREATE OR REPLACE TABLE stg_receipts AS 
                SELECT * FROM receipts_df;"""
    )

    num_rows = conn.sql("SELECT COUNT(*) AS NUM_ROWS FROM stg_receipts;").df()
    print(f"Number of rows: {num_rows["NUM_ROWS"]}")