import os
from dotenv import load_dotenv

import conf
import pipeline

def main():
    load_dotenv()
    APP_ID = os.getenv('APP_ID_AMS')
    APP_KEY = os.getenv('APP_KEY_AMS')
    URL = conf.URL
    ENDPOINT = conf.ENDPOINT

    data_flights = pipeline.extract_all(URL, ENDPOINT, APP_ID, APP_KEY)
    ... # Further Transformations
    pipeline.load_parquet(data_flights, path = 'data/data_flights.parquet')


if __name__ == '__main__':
    main()