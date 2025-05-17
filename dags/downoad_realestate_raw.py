from airflow.decorators import dag, task, task_group
from pendulum import datetime, now
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
import logging
from include.utils import download_large_csv, normalise_file_name, _get_duckdb_connection
from include.constants import suburb_url, duckdb_file
import logging
from airflow.sdk import Variable
from include.operators.realestate_operator import RealestateOperator
from include.operators.persist_file_operator import PersistFileOperator
from include.operators.raw_listings_processing_operator import RawListingsProcessingOperator
from airflow.utils.db import provide_session
from airflow.models import XCom
from airflow.operators.python import PythonOperator
# from airflow.operators.python import get_current_context
from airflow.models import XCom
import pandas as pd
from pandas import DataFrame
from include.constants import sql_create_agencies_table, sql_create_listings_table, sql_create_agents_table, sql_create_addresses_table
# from astro import sql as aql
# from astro.files import File
# from astro.sql.table import Table, Metadata

logger = logging.getLogger(__name__)

@dag(
    dag_id='downoad_realestate_raw',
    start_date=datetime(2025, 5, 13),
    schedule="@once",
    catchup=False,
    max_active_tasks=4)
def downoad_realestate_raw():
    # @task
    # def get_suburb_data():
    #     print("Getting suburb data")
    api_key = Variable.get("rapid_api_key", default=None)
    channel = "sold"
    state = "VIC"
    suburbs_df = pd.read_csv("./data/australia/suburbs.csv")
    victoria_suburbs = suburbs_df[suburbs_df["state"] == state]
    vic_suburb_names = ['Tawonga South'] # list(victoria_suburbs["suburb"].unique())

    task_groups = []
    for name in vic_suburb_names:
        tg_id = f"group_{normalise_file_name(name)}"

        @task_group(group_id=tg_id)
        def victoria_task_group(group_id:str, suburb:str):
            norm_suburb = normalise_file_name(suburb)
            state_name = normalise_file_name(state)

            download_suburb_listing_task = RealestateOperator(
                task_id=f"download_suburb_listing_{norm_suburb}",
                searchLocation = suburb,
                channel = channel,
                rapidApiKey = api_key,
                url = "https://realty-in-au.p.rapidapi.com/properties/list"
            )

            # persist_listing_data_task = PersistFileOperator(
            #     task_id=f"persist_listing_data_{norm_suburb}",
            #     file_name=f"/data/listing/{channel}/{state_name}/{norm_suburb}.json",
            #     content_task_id=f"{group_id}.download_suburb_listing_{norm_suburb}",
            #     content_key=norm_suburb
            # )

            raw_listings_processing_task = RawListingsProcessingOperator(
                task_id=f"raw_listings_processing_{norm_suburb}",
                target_task_id=f"{group_id}.download_suburb_listing_{norm_suburb}",
                target_key=norm_suburb,
                bucket_name="realestate-raw",
                file_path=f"/{channel}/{state_name}/{norm_suburb}",
            )

            @task
            def create_tables():
                db_connection = _get_duckdb_connection()
                db_connection.execute(sql_create_addresses_table)
                db_connection.execute(sql_create_agencies_table)
                db_connection.execute(sql_create_agents_table)
                db_connection.execute(sql_create_listings_table)

            load_addresses_to_dw_task = aql.load_file(
                task_id="load_addresses_to_dw",
                input_file=File(
                    path=f"s3://{{{{ ti.xcom_pull(task_ids='{group_id}.raw_listings_processing_{norm_suburb}') }}}}",
                    conn_id="minio_s3_conn"
                ),
                output_table=Table(
                    conn_id="mother-duck-conn",
                    name="raw_addresses",
                    metadata=MetaData(schema="public"),
                ),
                load_options={
                    "aws_access_key_id": BaseHook.get_connection("minio_s3_conn").login,
                    "aws_secret_access_key": BaseHook.get_connection("minio_s3_conn").password,
                    "endpoint_url": BaseHook.get_connection("minio_s3_conn").host,
                }
            )
            #
            # load_agencies_to_dw_task = aql.load_file(
            #     task_id="load_agencies_to_dw",
            #     input_file=File(
            #         path=f"s3://{{{{ ti.xcom_pull(task_ids='{group_id}.raw_listings_processing_{norm_suburb}') }}}}",
            #         conn_id="minio_s3_conn"
            #     ),
            #     output_table=Table(
            #         conn_id="mother-duck-conn",
            #         name="raw_agencies",
            #         metadata=MetaData(schema="public"),
            #     ),
            #     load_options={
            #         "aws_access_key_id": BaseHook.get_connection("minio_s3_conn").login,
            #         "aws_secret_access_key": BaseHook.get_connection("minio_s3_conn").password,
            #         "endpoint_url": BaseHook.get_connection("minio_s3_conn").host,
            #     }
            # )
            #
            # load_agents_to_dw_task = aql.load_file(
            #     task_id="load_agents_to_dw",
            #     input_file=File(
            #         path=f"s3://{{{{ ti.xcom_pull(task_ids='{group_id}.raw_listings_processing_{norm_suburb}') }}}}",
            #         conn_id="minio_s3_conn"
            #     ),
            #     output_table=Table(
            #         conn_id="mother-duck-conn",
            #         name="raw_agents",
            #         metadata=MetaData(schema="public"),
            #     ),
            #     load_options={
            #         "aws_access_key_id": BaseHook.get_connection("minio_s3_conn").login,
            #         "aws_secret_access_key": BaseHook.get_connection("minio_s3_conn").password,
            #         "endpoint_url": BaseHook.get_connection("minio_s3_conn").host,
            #     }
            # )
            #
            # load_listings_to_dw_task = aql.load_file(
            #     task_id="load_listings_to_dw",
            #     input_file=File(
            #         path=f"s3://{{{{ ti.xcom_pull(task_ids='{group_id}.raw_listings_processing_{norm_suburb}') }}}}",
            #         conn_id="minio_s3_conn"
            #     ),
            #     output_table=Table(
            #         conn_id="mother-duck-conn",
            #         name="raw_listings",
            #         metadata=MetaData(schema="public"),
            #     ),
            #     load_options={
            #         "aws_access_key_id": BaseHook.get_connection("minio_s3_conn").login,
            #         "aws_secret_access_key": BaseHook.get_connection("minio_s3_conn").password,
            #         "endpoint_url": BaseHook.get_connection("minio_s3_conn").host,
            #     }
            # )

            create_tables_task = create_tables()

            download_suburb_listing_task >> raw_listings_processing_task >> create_tables_task #>> [load_addresses_to_dw_task, load_agencies_to_dw_task, load_agents_to_dw_task, load_listings_to_dw_task]

        task_groups.append(victoria_task_group(tg_id, name))

    @task
    def final_task():
        logger.info("All tasks completed.")

    task_groups >> final_task()

downoad_realestate_raw()
