"Contains constants used in the DAGs"

from pathlib import Path
from cosmos import ExecutionConfig

jaffle_shop_path = Path("/usr/local/airflow/dbt/jaffle_shop")
dbt_executable = Path("/usr/local/airflow/dbt_venv/bin/dbt")

venv_execution_config = ExecutionConfig(
    dbt_executable_path=str(dbt_executable),
)

suburb_url = 'https://raw.githubusercontent.com/michalsn/australian-suburbs/refs/heads/master/data/suburbs.csv'
duckdb_file = "/data/dbt.duckdb"

sql_create_addresses_table = """
CREATE TABLE IF NOT EXISTS raw_addresses (
address_id STRING,
street_address STRING,
suburb STRING,
state VARCHAR(3),
postcode CHAR(4),
locality STRING,
subdivision_code STRING,
latitude FLOAT,
longitude FLOAT
);
"""

sql_create_agencies_table = """
CREATE TABLE IF NOT EXISTS raw_agencies(
agency_id CHAR(6),
name STRING,
email STRING,
address_id STRING,
website STRING,
phone_number STRING
);
"""

sql_create_agents_table = """
CREATE TABLE IF NOT EXISTS raw_agents(
agent_id INTEGER,
full_name STRING,
job_title STRING,
email STRING,
website STRING,
phone_number STRING,
mobile_number STRING
);
"""

sql_create_listings_table = """
CREATE TABLE IF NOT EXISTS raw_listings(
listing_id LONG,
title STRING,
property_type STRING,
listing_type STRING,
construction_status STRING,
price STRING,
advertised_price STRING,
bedrooms INTEGER,
bathrooms INTEGER,
parking_spaces INTEGER,
land_size STRING,
description STRING,
features STRING,
status STRING,
date_sold STRING,
classic_project BOOLEAN,
agency_id CHAR(6),
agent_id INTEGER,
address_id STRING
);
"""