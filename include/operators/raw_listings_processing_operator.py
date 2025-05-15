from airflow.sdk import BaseOperator
from include.utils import load_data
from include.models.listing import Listing
from include.models.agency import Agency
from include.models.agent import Agent
from include.models.address import Address
import pandas as pd
from pathlib import Path
from include.utils import ensure_directory_exists, _store_data_as_csv
from pendulum import now

class RawListingsProcessingOperator(BaseOperator):
    def __init__(
            self,
            target_task_id: str,
            target_key: str,
            bucket_name: str,
            file_path: str,
            **kwargs):
        super().__init__(**kwargs)
        self.target_task_id = target_task_id
        self.target_key = target_key
        self.bucket_name = bucket_name
        self.file_path = file_path

    def execute(self, context):
        self.log.info(f"Start RawListingsProcessingOperator {self.target_task_id}")
        file_name = context["ti"].xcom_pull(task_ids=self.target_task_id, key=self.target_key)
        self.log.info(f"Start processing file {file_name}")

        addresses: list[Address] = []
        agents: list[Agent] = []
        agencies: list[Agency] = []
        listings: list[Listing] = []

        load_data(file_name, addresses, agents, agencies, listings)

        address_objects = [object.__dict__ for object in addresses]
        agencies_objects = [object.__dict__ for object in agencies]
        agents_objects = [object.__dict__ for object in agents]
        listings_objects = [object.__dict__ for object in listings]

        # addresses_df = pd.DataFrame.from_dict(address_objects)
        # agencies_df = pd.DataFrame.from_dict(agencies_objects)
        # agents_df = pd.DataFrame.from_dict(agents_objects)
        # listings_df = pd.DataFrame.from_dict(listings_objects)
        #
        # ensure_directory_exists(self.file_path)
        # address_filename = f"{self.file_path}_addresses.csv"
        # agencies_filename = f"{self.file_path}_agencies.csv"
        # agents_filename = f"{self.file_path}_agents.csv"
        # listings_filename = f"{self.file_path}_listings.csv"
        #
        # addresses_df.to_csv(address_filename, index=False)
        # agencies_df.to_csv(agencies_filename, index=False)
        # agents_df.to_csv(agents_filename, index=False)
        # listings_df.to_csv(listings_filename, index=False)
        s3_address_uri = _store_data_as_csv(
            data=address_objects,
            bucket_name=self.bucket_name,
            file_name=f"{self.file_path}/{now().to_date_string().replace("-", "_")}-addresses.csv",
        )
        s3_agencies_uri = _store_data_as_csv(
            data=agencies_objects,
            bucket_name=self.bucket_name,
            file_name=f"{self.file_path}/{now().to_date_string().replace("-", "_")}-agents.csv",
        )
        s3_agents_uri = _store_data_as_csv(
            data=agents_objects,
            bucket_name=self.bucket_name,
            file_name=f"{self.file_path}/{now().to_date_string().replace("-", "_")}-agents.csv",
        )
        s3_listings_uri = _store_data_as_csv(
            data=listings_objects,
            bucket_name=self.bucket_name,
            file_name=f"{self.file_path}/{now().to_date_string().replace("-", "_")}-listings.csv",
        )

        combined_filenames = [s3_address_uri, s3_agencies_uri, s3_agents_uri, s3_listings_uri]
        context["ti"].xcom_push(key=self.target_key, value=combined_filenames)


