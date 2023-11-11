import requests
import os
import polars as pl

URL = 'https://api.schiphol.nl/public-flights/'
ENDPOINT = 'flights'

APP_ID = os.getenv('APP_ID_AMS')
APP_KEY = os.getenv('APP_KEY_AMS')

def extract(URL, ENDPOINT, page, APP_ID, APP_KEY):
    """
    This function extracts the data from the API
    """

    header = {
        'Accept': 'application/json',
        'app_id': APP_ID,
        'app_key': APP_KEY,
        'ResourceVersion': 'v4'
    }

    params = {
        'scheduleDate': '2023-11-08',
        'page': page
    }

    response = requests.get(url=URL + ENDPOINT, headers=header, params=params).json()
    return response['flights']
    
def transform(raw_data):
    """
    This function transforms the data
    """

    transformed_data = pl.DataFrame(raw_data)

    KEEP_COLUMNS = ["id",
                    "flightDirection", # Arrival or Departure
                    "serviceType",
                    "flightName",
                    "scheduleDate", # The date on which the scheduled commercial flight will be operated. Example: 2015-06-17.
                    "scheduleTime", # The time the commercial flight is scheduled to depart or arrive. Example: 15:25:00
                    # "publicFlightState",
                    "aircraftType",
                    "actualLandingTime", # Actual landing time of an arrival
                    "actualOffBlockTime", # The actual time of departure of a flight from Amsterdam Airport Schiphol.
                    "prefixIATA"]
    
    transformed_data = transformed_data.select(KEEP_COLUMNS)
    transformed_data = transformed_data.with_columns(scheduleDateTime = pl.col("scheduleDate") +  " " + pl.col("scheduleTime"))
    transformed_data = transformed_data.with_columns(
        pl.when(pl.col("flightDirection") == "A")
        .then(pl.col("actualLandingTime"))
        .otherwise(pl.col("actualOffBlockTime"))
        .alias("actualDateTime")
    )
    transformed_data = transformed_data.drop(["scheduleDate", "scheduleTime", "actualLandingTime", "actualOffBlockTime"])

    # select aircraftType

    return transformed_data

def transform_and_validate(data):
    """
    This functions checks the data
    """

def load(data):
    """
    This function loads the transformed data into a database
    """