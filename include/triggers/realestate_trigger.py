from typing import Any, AsyncIterator
from airflow.configuration import conf
from airflow.triggers.base import BaseTrigger, TriggerEvent
from aiohttp import ClientSession
from math import ceil

class RealestateTrigger(BaseTrigger):
    """
    This custom trigger request for realestate data from a Rapid Api endpoint and awaits for the response.
    When the data is received, it is returned.
    Args:
        url (str): The url of the Rapid Api endpoint.
        params (dict): A dictionary of parameters to pass to the Rapid Api endpoint.
            params = {s
                "page": page,
                "pageSize": page_size,
                "sortType": "relevance",
                "channel": listing_channel,
                "surroundingSuburbs": surrounding_suburbs,
                "searchLocation": suburb,
                "searchLocationSubtext": "Region",
                "type": "region",
                "ex-under-contract": exclude_under_contract,
            }
        headers (dict): A dictionary of headers to pass to the Rapid Api endpoint.
            headers = {
                'Accept': 'application/json',
                'x-rapidapi-host': 'realty-in-au.p.rapidapi.com',
                'x-rapidapi-key': '775d4867bdmshb6b368cdeed11f2p191677jsn716b8bdb976d',
            }
        requests_per_second (int): Number of requests per second.
        realestate_data_out ([Any]): An array of data that will be sent back to the Rapid Api endpoint.
    Returns:
        [dict]: A dictionary containing the response data.
    """
    def __init__(self,
                 url: str,
                 params: dict = {
                    "searchLocation": "",
                    "page": 1,
                    "pageSize": 30,
                    "sortType": "relevance",
                    "channel": "sold",
                    "surroundingSuburbs": "false",
                    "searchLocationSubtext": "Region",
                    "type": "region",
                    "ex-under-contract": "false",
                },
                 headers: dict = {
                    'Accept': 'application/json',
                    'x-rapidapi-host': 'realty-in-au.p.rapidapi.com',
                    'x-rapidapi-key': "",
                },
                 realestate_data_out: [Any] = []
                 ):
        super().__init__()
        self.url = url
        self.params = params
        self.headers = headers
        # self.logger = logging.getLogger(__name__)
        self.realestate_data_out = realestate_data_out


    def serialize(self) -> tuple[str, dict[str, Any]]:
        """
        This trigger serializes the response data and returns it.
        """
        return (
            "include.triggers.realestate_trigger.RealestateTrigger",
            {
                "url": self.url,
                "params": self.params,
                "headers": self.headers,
                "realestate_data_out": self.realestate_data_out
            })

    async def run(self) -> AsyncIterator[TriggerEvent]:
        self.log.info("Running Realestate Trigger")
        from math import ceil

        while True:
            async with ClientSession() as session:
                try:
                    self.log.info(f"Sending request to Rapid Api endpoint {self.url}")
                    has_next_page = True
                    while has_next_page:
                        async with session.get(self.url, params=self.params, headers=self.headers) as resp:
                            resp.raise_for_status()
                            payload = await resp.json()
                            if self.realestate_data_out:
                                self.realestate_data_out.append(payload)
                            else:
                                self.realestate_data_out = [payload]

                            num_pages = ceil(payload["totalResultsCount"]/self.params["pageSize"])
                            if self.params["page"] < num_pages:
                                self.params["page"] = self.params["page"] + 1
                                self.log.info(f"Loading next page {self.params['page']}/{num_pages}")
                            else:
                                has_next_page = False
                                self.log.info(f"Completed downloading data for {self.params["searchLocation"]}")
                    yield TriggerEvent(self.serialize())
                    return
                except Exception as e:
                    raise e
                finally:
                    await session.close()




