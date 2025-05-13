import pandas as pd
from typing import Literal
def ensure_directory_exists(file_path):
    """
    Ensures the directory for the given file path exists.
    Returns the directory path.

    Args:
        file_path (str): Path to the file

    Returns:
        Path: Path object representing the directory
    """
    from pathlib import Path
    # Convert to Path object if string is provided
    path = Path(file_path)

    # Get the directory path
    directory = path.parent

    # Create directory if it doesn't exist
    directory.mkdir(parents=True, exist_ok=True)

    return directory

def download_large_csv(url, filename, chunksize=10000):
    """
    Downloads a large CSV file from given URL and saves it in the given directory.

    Args:
        url (str): URL of the CSV file
        filename (str): Name of the CSV file
        chunksize (int): Chunksize in bytes (default: 10000)

    """
    import pandas as pd
    try:
        chunks = pd.read_csv(url, chunksize=chunksize)
        # Making sure that the parent directory of the diven file exists.
        ensure_directory_exists(filename)

        first_chunk = True
        for chunk in chunks:
            if first_chunk:
                chunk.to_csv(filename, index=False)
                first_chunk = False
            else:
                chunk.to_csv(filename, mode='a', header=False, index=False)

        print(f"Successfully downloaded large CSV to {filename}")

    except Exception as e:
        print(f"An error occurred: {e}")


async def download_realestate_raw_json(
        suburbs: pd.DataFrame,
        listing_channel : Literal["buy", "rental", "sold"] ="buy",
        page_size: int = 30,
        requests_per_second: int = 5,
        api_key: str = "",
        surrounding_suburbs: Literal["true", "false"] = "false",
        exclude_under_contract: Literal["true", "false"] = "false",
):
    import aiohttp
    import asyncio

    headers = {
        'Accept': 'application/json',
        'x-rapidapi-host': 'realty-in-au.p.rapidapi.com',
        'x-rapidapi-key': api_key,
    }

    # session = create_session_with_retry()
    page = 1
    has_next_page = True
    params = {
        "page": page,
        "pageSize": page_size,
        "sortType": "relevance",
        "channel": listing_channel,
        "surroundingSuburbs": surrounding_suburbs,
        # "searchLocation": suburb,
        "searchLocationSubtext": "Region",
        "type": "region",
        "ex-under-contract": exclude_under_contract,
    }

    async with aiohttp.ClientSession() as session:
        try:
            for index in suburbs.index:
                # suburb = suburbs.loc[index, "suburb"]
                params["searchLocation"] = suburbs.loc[index, "suburb"]
                while has_next_page:
                    url = f"https://realty-in-au.p.rapidapi.com/properties/list"
                    # ?page={page}&pageSize={page_size}&sortType=relevance&channel={listing_channel}&surroundingSuburbs={surrounding_suburbs}&searchLocation={suburb}&searchLocationSubtext=Region&type=region&ex-under-contract={exclude_under_contract}

                    async with session.get(url, params=params, headers=headers) as response:
                        # await asyncio.sleep(1/requests_per_second)
                        response.raise_for_status()
                        payload = await response.json()

                        state_name = str(suburbs.loc[index, "state_name"]).strip().lower().replace(" ", "_")
                        suburb_name = str(suburbs.loc[index, "suburb"]).strip().lower().replace(" ", "_")

                        file_name = f"./data/listings/{listing_channel}/{state_name}/{suburb_name}/{suburb_name}_{page}.json"
                        ensure_directory_exists(file_name)

                        with open(file_name, 'w') as f:
                            json.dump(payload, f, indent=4)

                        # Update the page number or status for the next request
                        num_pages = ceil(payload["totalResultsCount"]/page_size)
                        if page < num_pages:
                            page += 1
                        else:
                            has_next_page = False

        except requests.RequestException as e:
            return f"Error: {e}"
        finally:
            await session.close()

def normalise_file_name(file_name: str) -> str:

    return file_name.strip().lower().replace(" ", "_")
