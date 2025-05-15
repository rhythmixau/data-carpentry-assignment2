from airflow.sdk.bases.operator import BaseOperator
from include.triggers.realestate_trigger import RealestateTrigger
from airflow.triggers.base import StartTriggerArgs
import asyncio
from airflow.sdk import Context
from typing import Any
from include.utils import normalise_file_name
from include.utils import _store_data_as_json

class RealestateOperator(BaseOperator):
    """
    Deferrable operator that download and waits for realestate data to become available.
    Args:
        searchLocation (str): The location to search for realestate data.
        channel (str): The realestate channel to use.
        rapidApiKey (str): The realestate api key to use.
        url (str): The realestate url to use.
        **kwargs: Additional keyword arguments.
    Returns:
        [dict]: An array or list of dictionaries containing realestate data.
    """
    ui_color = "#f9f9fa"

    start_from_trigger = True

    def __init__(
            self,
            searchLocation: str = "",
            channel: str = "sold",
            rapidApiKey: str = "",
            url: str ="https://realty-in-au.p.rapidapi.com/properties/list",
            **kwargs,
    ):
        super().__init__(**kwargs)
        self.searchLocation = searchLocation
        self.rapidApiKey = rapidApiKey
        self.params = {
                "searchLocation": searchLocation,
                "page": 1,
                "pageSize": 30,
                "sortType": "relevance",
                "channel": channel,
                "surroundingSuburbs": "false",
                "searchLocationSubtext": "Region",
                "type": "region",
                "ex-under-contract": "false",
            }
        self.headers = {
            'Accept': 'application/json',
            'x-rapidapi-host': 'realty-in-au.p.rapidapi.com',
            'x-rapidapi-key': rapidApiKey,
            }
        # self.requests_per_second = 5
        self.url = url

    def execute(self, context: Context):
        self.log.info(f"Starting downloading realestate data from: {self.url}")
        self.defer(
            trigger=RealestateTrigger(
                url = self.url,
                params = self.params,
                headers = self.headers,
                realestate_data_out = None
            ),
            method_name="execute_complete",
            kwargs={ "kwarg_passed_to_execute_complete": "realestate_operator.completed" }
        )
        self.log.info("Operator completed successfully")

    def execute_complete(
            self,
            context: Context,
            event: tuple[str, dict[str, Any]],
            kwarg_passed_to_execute_complete: str,
    ) -> str:
        """Execute when the tirgger is complete."""
        self.log.info("Trigger is complete")
        s3_path = _store_data_as_json(event[1], channel=self.params["channel"], suburb=self.searchLocation)
        context["ti"].xcom_push(key=normalise_file_name(self.searchLocation), value=s3_path)
        return kwarg_passed_to_execute_complete