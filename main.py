import os
from dotenv import load_dotenv

import conf
import pipeline

def main():
    load_dotenv()
    APP_ID = os.getenv('APP_ID_AMS')
    APP_KEY = os.getenv('APP_KEY_AMS')
    URL = os.getenv('URL')
    ENDPOINT = os.getenv('ENDPOINT')

    data_flights = extract_all(URL, ENDPOINT, APP_ID, APP_KEY)
    ... # Further Transformations
    load_parquet(data_flights, path = 'data/data_flights.parquet')


if __name__ == '__main__':
    main()