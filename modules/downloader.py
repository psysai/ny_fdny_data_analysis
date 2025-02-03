# modules/downloader.py
import os
import pandas as pd
import requests
from tenacity import retry, wait_fixed, stop_after_attempt, retry_if_exception_type
import json
from datetime import datetime

import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class DataDownloader:
    @staticmethod
    @retry(
        wait=wait_fixed(5),             # wait 5 seconds between retries
        stop=stop_after_attempt(3),     # try up to 3 times
        retry=retry_if_exception_type(requests.exceptions.HTTPError)
    )
    def download_from_ny_open_data(url: str, path: str):
        if not os.path.exists(path):
            os.makedirs(os.path.dirname(path), exist_ok=True)
            logger.info(f"Downloading data from {url} to {path} ...")
            offset = 0
            all_data = []
            while True:
                data = None
                paginated_url = f"{url} limit 1000 offset {offset}"
                response = requests.get(paginated_url)
                response.raise_for_status()  # failing occasionally with gateway timeout, website is down
                data = response.json()
                if not data:
                    break
                all_data.extend(data)
                offset += 1000
                logger.info(f"Downloaded {len(all_data)} records so far...")
            with open(path, 'a+') as f:
                json.dump(all_data, f)
                
            logger.info(f"Downloaded and saved to {path}")
        else:
            logger.info(f"File {path} already exists. Skipping download.")

    
    
    @staticmethod
    def download_from_github(url: str, path: str):
        if not os.path.exists(path):
            os.makedirs(os.path.dirname(path), exist_ok=True)
            logger.info(f"Downloading data from {url} to {path} ...")
            response = requests.get(url)
            with open(path, 'wb') as f:
                f.write(response.content)
            logger.info(f"Downloaded and saved to {path}")
        else:
            logger.info(f"File {path} already exists. Skipping download.")

