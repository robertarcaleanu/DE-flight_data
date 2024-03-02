import requests

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