import requests
import time
import polars as pl


from pipeline.transform import transform

def extract_page(URL: str, ENDPOINT: str, page: int, header: dict, fl_dir: str) -> dict:
    """
    This function extracts the data from the API
    """

    params = {
        'scheduleDate': '2023-11-08',
        'page': page,
        'flightDirection': fl_dir
    }

    response = requests.get(url=URL + ENDPOINT, headers=header, params=params).json()
    return response['flights']


def extract_all(URL: str, ENDPOINT: str, APP_ID: str, APP_KEY: str) -> pl.DataFrame:

    header = {
        'Accept': 'application/json',
        'app_id': APP_ID,
        'app_key': APP_KEY,
        'ResourceVersion': 'v4'
        }

    data_flights = pl.DataFrame()
    for dir in ['D', 'A']:
        page = 0
        cond = True
        while cond:
            try:
                data_page = extract_page(URL, ENDPOINT, page, header, fl_dir=dir)
                data_page = pl.DataFrame(data_page)
                data_page = transform(data_page, fl_dir=dir)
                data_flights = pl.concat([data_flights, data_page])
                page = page + 1
                print(page)
                time.sleep(1)

            except:
                print('Finished at page ' + str(page))
                break
            

    return data_flights