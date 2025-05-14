import pandas as pd
from typing import Literal
from include.models.address import Address
from include.models.agency import Agency
from include.models.agent import Agent
from include.models.listing import Listing

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


def find_address(addresses_list: list[Address], address_id: str):
    for address in addresses_list:
        if address.address_id == address_id:
            return address
    return None

def find_agent(agents_list: list[Agent], email: str):
    for agent in agents_list:
        if agent.email == email:
            return agent
    return None

def find_agency(agencies_list: list[Agency], agency_id: int):
    for agency in agencies_list:
        if agency.agency_id == agency_id:
            return agency
    return None

def load_data(filename: str, addresses: list[Address], agents: list[Agent], agencies: list[Agency], listings: list[Listing]):
    import json
    with open(file=filename, mode="r") as f:
        data = json.load(f)

        for page in data:
            for tier in page["tieredResults"]:
                for listing_data in tier["results"]:
                    # extract features
                    features_list = []
                    if listing_data.keys().__contains__("propertyFeatures"):
                        for feature_type in listing_data["propertyFeatures"]:
                            for feature in feature_type["features"]:
                                features_list.append(feature)
                    listing_agent_ids = []
                    for lister in listing_data["listers"]:
                        if len(list(lister.keys())) == 0:
                            print(f"Missing listers: {listing_data["listingId"]}")
                            continue
                        email = lister["email"] if lister.keys().__contains__("email") else ""
                        agent = find_agent(agents, email)
                        if agent is None:
                            agent = Agent(
                                agent_id=lister["id"] if lister.keys().__contains__("id") else "",
                                full_name=lister["name"] if lister.keys().__contains__("name") else "",
                                job_title=lister["jobTitle"] if lister.keys().__contains__("jobTitle") else "",
                                email=lister["email"] if lister.keys().__contains__("email") else "",
                                website=lister["website"] if lister.keys().__contains__("website") else "",
                                phone_number=lister["phoneNumber"] if lister.keys().__contains__("phoneNumber") else "",
                                mobile_number=lister["mobilePhoneNumber"] if lister.keys().__contains__("mobilePhoneNumber") else ""
                            )
                            agents.append(agent)
                        listing_agent_ids.append(agent.agent_id)

                    agency_address_id = f"{listing_data['agency']['address']['streetAddress']}-{listing_data['agency']['address']['suburb']}-{listing_data['agency']['address']['state']}-{listing_data['agency']['address']['postcode']}".replace(" ", "-") if list(listing_data.keys()).__contains__("agency") else ""

                    agency_address = find_address(addresses, agency_address_id)
                    if agency_address is None and agency_address_id != "":
                        agency_address = Address(
                            address_id=agency_address_id,
                            street_address=listing_data["agency"]["address"]["streetAddress"],
                            suburb=listing_data["agency"]["address"]["suburb"],
                            state=listing_data["agency"]["address"]["state"],
                            postcode=listing_data["agency"]["address"]["postcode"],
                            locality="",
                            subdivision_code="",
                            latitude=0.0,
                            longitude=0.0
                        )
                        addresses.append(agency_address)
                    agency = None
                    if listing_data.keys().__contains__("agency"):
                        agency = find_agency(agencies, listing_data["agency"]["agencyId"])
                        if agency is None:
                            agency = Agency(
                                agency_id=listing_data["agency"]["agencyId"],
                                name=listing_data["agency"]["name"],
                                email=listing_data["agency"]["email"],
                                address_id=agency_address_id,
                                website=listing_data["agency"]["website"] if list(listing_data["agency"].keys()).__contains__("website") else "",
                                phone_number=listing_data["agency"]["phoneNumber"]
                            )
                            agencies.append(agency)

                    listing_address_id = f"{listing_data['address']['streetAddress']}-{listing_data['address']['suburb']}-{listing_data['address']['state']}-{listing_data['address']['postcode']}".replace(" ", "-")

                    list_address = find_address(addresses, listing_address_id)

                    if list_address is None:
                        list_address = Address(
                            address_id=listing_address_id,
                            street_address=listing_data["address"]["streetAddress"],
                            suburb=listing_data["address"]["suburb"],
                            state=listing_data["address"]["state"],
                            postcode=listing_data["address"]["postcode"],
                            locality=listing_data["address"]["locality"],
                            subdivision_code=listing_data["address"]["subdivisionCode"],
                            latitude=listing_data["address"]["location"]["latitude"] if list(listing_data["address"].keys()).__contains__("location") else 0.0,
                            longitude=listing_data["address"]["location"]["longitude"] if list(listing_data["address"].keys()).__contains__("location") else 0.0
                        )
                        addresses.append(list_address)

                    advertised_price = ""
                    if list(listing_data.keys()).__contains__("advertising") and list(listing_data["advertising"].keys()).__contains__("priceRange"):
                        advertised_price = listing_data["advertising"]["priceRange"]

                    listing = Listing(
                        listing_id=listing_data["listingId"],
                        title=listing_data["title"],
                        property_type=listing_data["propertyType"],
                        listing_type=listing_data["channel"],
                        construction_status=listing_data["constructionStatus"],
                        price=listing_data["price"]["display"],
                        advertised_price= advertised_price,
                        bedrooms=listing_data["features"]["general"]["bedrooms"],
                        bathrooms=listing_data["features"]["general"]["bathrooms"],
                        parking_spaces=listing_data["features"]["general"]["parkingSpaces"],
                        land_size= f"{listing_data["landSize"]["value"]} {listing_data["landSize"]["unit"]}" if list(listing_data.keys()).__contains__("landSize") else "",
                        description= listing_data["description"],
                        features=features_list,
                        status=listing_data["status"]["type"],
                        date_sold=listing_data["dateSold"]["value"] if list(listing_data.keys()).__contains__("dateSold") else "",
                        classic_project=listing_data["classicProject"],
                        agency_id=agency.agency_id if agency is not None else "",
                        agent_id=listing_agent_ids,
                        address_id=listing_address_id
                    )

                    listings.append(listing)