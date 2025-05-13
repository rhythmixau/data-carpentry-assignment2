from airflow.decorators import dag, task, task_group
from pendulum import datetime
import duckdb
from include.utils import download_large_csv, normalise_file_name
from include.constants import suburb_url, duckdb_file
import logging
from airflow.sdk import Variable
from include.operators.realestate_operator import RealestateOperator
from include.operators.persist_file_operator import PersistFileOperator
import pandas as pd

@dag(start_date=datetime(2025, 5, 13), schedule="@once", catchup=False, max_active_tasks=4)
def downoad_realestate_raw():
    # @task
    # def get_suburb_data():
    #     print("Getting suburb data")
    api_key = Variable.get("rapid_api_key", default=None)
    channel = "sold"
    state = "VIC"
    suburbs_df = pd.read_csv("./data/australia/suburbs.csv")
    victoria_suburbs = suburbs_df[suburbs_df["state"] == state]
    vic_suburb_names = ['Tawonga', 'Bright', 'Tawonga South'] #list(victoria_suburbs["suburb"].unique())

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

            persist_listing_data_task = PersistFileOperator(
                task_id=f"persist_listing_data_{norm_suburb}",
                file_name=f"/data/listing/{channel}/{state_name}/{norm_suburb}.json",
                content_task_id=f"{group_id}.download_suburb_listing_{norm_suburb}",
                content_key=norm_suburb
            )

            download_suburb_listing_task >> persist_listing_data_task
        task_groups.append(victoria_task_group(tg_id, name))

    @task
    def final_task():
        print("All tasks completed.")

    task_groups >> final_task()

downoad_realestate_raw()
