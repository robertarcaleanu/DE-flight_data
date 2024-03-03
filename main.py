import os
from dotenv import load_dotenv

import conf
import pipeline

def main():
    load_dotenv()
    # API Keys
    APP_ID = os.getenv('APP_ID_AMS')
    APP_KEY = os.getenv('APP_KEY_AMS')
    URL = conf.URL
    ENDPOINT = conf.ENDPOINT

    # AWS Keys
    AWS_S3_BUCKET_NAME = os.getenv('AWS_S3_BUCKET_NAME')
    AWS_REGION = os.getenv('AWS_REGION')
    AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
    AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')

    # Extract
    data_flights = pipeline.extract_all(URL, ENDPOINT, APP_ID, APP_KEY)

    # Transform
    ... # Further Transformations

    # Load
    LOCAL_FILE = 'data/data_flights.parquet'
    pipeline.load_parquet(data_flights, path = LOCAL_FILE)
    pipeline.load_s3(AWS_REGION, AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_S3_BUCKET_NAME, LOCAL_FILE, LOCAL_FILE)


if __name__ == '__main__':
    main()