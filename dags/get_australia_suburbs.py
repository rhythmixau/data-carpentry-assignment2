from airflow.decorators import dag, task
from pendulum import datetime
# import duckdb
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from include.utils import download_large_csv
from include.constants import suburb_url, duckdb_file
import logging

@dag(start_date=datetime(2025, 5, 10), schedule="@once", catchup=False)
def get_australia_suburbs():
    suburb_filename = "/data/australia/suburbs.csv"
    logger = logging.getLogger(__name__)

    @task
    def download_australia_suburbs_csv():
        logger.info("Downloading australia suburbs csv")
        download_large_csv(suburb_url, suburb_filename)
        logger.info("Downloaded australia suburbs csv")

    @task
    def save_suburb_to_db():
        logger.info("Downloading suburbs data")
        # conn = duckdb.connect(duckdb_file)
        hook = DuckDBHook.get_hook("mother-duck-conn")
        conn = hook.get_conn()
        conn.sql('DROP TABLE IF EXISTS raw_suburbs;')
        conn.sql(f"""
        CREATE TABLE IF NOT EXISTS raw_suburbs AS
        SELECT * FROM read_csv('{suburb_filename}');
        """)
        conn.close()
        logger.info("Done saving suburbs data")

    @task
    def check_suburbs():
        logger.info("Checking suburbs data")
        # conn = duckdb.connect(duckdb_file)
        hook = DuckDBHook.get_hook("mother-duck-conn")
        conn = hook.get_conn()
        num_rows = conn.sql('SELECT COUNT(*) FROM raw_suburbs;').fetchone()[0]
        logger.info(f"Suburb count: {num_rows}")
        if num_rows == 0:
            raise Exception("Failed to load suburbs")
        conn.close()

        logger.info("Done checking suburbs data")

    download_australia_suburbs_csv_task = download_australia_suburbs_csv()
    save_suburb_to_db_task = save_suburb_to_db()
    check_suburbs_task = check_suburbs()

    download_australia_suburbs_csv_task >> save_suburb_to_db_task >> check_suburbs_task

get_australia_suburbs()