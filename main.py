import requests
import os

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
    
def transform(data):
    """
    This function transforms the data
    """

    transformed_data = data

    return transformed_data

def load(data):
    """
    This function loads the transformed data into a database
    """