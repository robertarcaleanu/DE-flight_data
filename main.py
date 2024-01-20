import os
from dotenv import load_dotenv

import conf
import pipeline

def main():
    load_dotenv()
    APP_ID = os.getenv('APP_ID_AMS')
    APP_KEY = os.getenv('APP_KEY_AMS')

if __name__ == '__main__':
    main()