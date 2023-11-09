import requests
import os

APP_ID = os.getenv('APP_ID_AMS')
APP_KEY = os.getenv('APP_KEY_AMS')

URL = 'https://api.schiphol.nl/public-flights/'
ENDPOINT = 'flights'

header = {
    'Accept': 'application/json',
    'app_id': APP_ID,
    'app_key': APP_KEY,
    'ResourceVersion': 'v4'
}

params = {
    'scheduleDate': '2023-11-08',
    'page': 1
}

response = requests.get(url=URL + ENDPOINT, headers=header, params=params).json()
len(response['flights'])
