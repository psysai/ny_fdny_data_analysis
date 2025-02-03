import pandas as pd
import os
from modules.downloader import DataDownloader

import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def read_file(filepath: str) -> str:
    with open(filepath, 'r') as file:
        return file.read()

# get the base directory of the current script
def base_dir():
    return os.path.dirname(os.path.abspath(__file__))

# FDNY Fire Incident Dispatch dataset URL
FDNY_FIRE_INCIDENT_QUERY = read_file(os.path.join(base_dir(), '..', 'queries', 'fire_incidents_nyc.sql'))
FDNY_FIRE_INCIDENT_URL = "https://data.cityofnewyork.us/resource/8m42-w767.json?$query=" 
FDNY_FIRE_INCIDENT_DATA_SAVE_PATH = os.path.join(base_dir(), '..', "data/fdny_fire_incidents_{yyyy}.json")

# NYC Property DETAILS dataset URL
NY_PROPERTY_DETAILS_QUERY = read_file(os.path.join(base_dir(), '..', 'queries', 'ny_property_details.sql'))
NY_PROPERTY_DETAILS_URL = "https://data.cityofnewyork.us/resource/8y4t-faws.json?$query="
NY_PROPERTY_DETAILS_DATA_PATH = os.path.join(base_dir(), '..', "data/ny_property_details_{yyyy}.json")

# NYC GeoJSON dataset URL
NYC_GEOJSON_URL = "https://github.com/gdobler/nycep/raw/refs/heads/master/d3/data/nyc-zip-code.json"
NYC_GEOJSON_DATA_PATH = os.path.join(base_dir(), '..', "data/nyc-zip-code.json")

class DataRepository:
    @staticmethod
    def load_fdny_data(year:str = "2023") -> pd.DataFrame:
        fire_incident_url = FDNY_FIRE_INCIDENT_URL+ FDNY_FIRE_INCIDENT_QUERY.format(yyyy=year, yy=year[2:4])
        fire_incident_data_path = FDNY_FIRE_INCIDENT_DATA_SAVE_PATH.format(yyyy=year)
        DataDownloader.download_from_ny_open_data(fire_incident_url, fire_incident_data_path)
        return pd.read_json(fire_incident_data_path)
    
    @staticmethod
    def load_population_data(year:str = "2023") -> pd.DataFrame:
        population_data_url = NY_PROPERTY_DETAILS_URL + NY_PROPERTY_DETAILS_QUERY.format(yyyy=year)
        population_data_path = NY_PROPERTY_DETAILS_DATA_PATH.format(yyyy=year)
        DataDownloader.download_from_ny_open_data(population_data_url, population_data_path)
        return pd.read_json(population_data_path)
    
    @staticmethod
    def load_nyc_geojson() -> pd.DataFrame:
        DataDownloader.download_from_github(NYC_GEOJSON_URL, NYC_GEOJSON_DATA_PATH)

    
