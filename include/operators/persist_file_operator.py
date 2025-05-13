from airflow.sdk import BaseOperator
from include.utils import ensure_directory_exists
import json

class PersistFileOperator(BaseOperator):
    def __init__(
            self,
            file_name: str,
            content_task_id: str = "download_suburb_listing",
            content_key: str = "include.triggers.realestate_trigger.RealestateTrigger",
            **kwargs):
        super().__init__(**kwargs)
        self.file_name = file_name
        self.content_task_id = content_task_id
        self.content_key = content_key

    def execute(self, context):
        ensure_directory_exists(self.file_name)
        content = context["ti"].xcom_pull(task_ids=self.content_task_id, key=self.content_key)
        self.log.info(f"Persisting data: {len(content["realestate_data_out"])} pages")
        with open(self.file_name, 'w') as f:
            json.dump(content["realestate_data_out"], f, indent=4)
        message = f"Successfully persisted file {self.file_name}"
        return message