# pipeline.py
import pandas as pd
from prefect import task, flow
from modules.repository import DataRepository
from modules.transformer import DataTransformer
from modules.loader import DataLoaderSQLite
from modules.validator import validate

import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

@task
def extract_fdny():
    """Extract FDNY fire incident records as a DataFrame."""
    df = DataRepository.load_fdny_data()
    logger.info(f"Extracted {len(df)} rows from FDNY dataset.")
    return df

@task
def extract_prop():
    """Load population data as a DataFrame."""
    prop_df = DataRepository.load_population_data()
    logger.info(f"Extracted {len(prop_df)} rows from population dataset.")
    return prop_df

@task
def extract_geojson():
    """Load NYC geojson data as a DataFrame."""
    DataRepository.load_nyc_geojson()
    logger.info(f"Extracted NYC geojson dataset.")

@task
def transform_fdny_dataframe(fdny_df):
    """
    Transform and enrich the FDNY DataFrame using vectorized pandas operations.
    """
    return DataTransformer.clean_fdny_fire_incident_data(fdny_df)

@task
def transform_prop_dataframe(prop_df):
    """
    Transform and enrich the population DataFrame using vectorized pandas operations.
    """
    return DataTransformer.clean_property_data(prop_df)

@task
def merge_data(fdny_df, property_df):
    """Merge FDNY and property DataFrames."""
    return DataTransformer.merge_fdny_property_data(fdny_df, property_df)

@task
def load_data(df, table_name):
    """Load the DataFrame into SQLite."""
    DataLoaderSQLite.load_dataframe(df, table_name)

@task
def display_table(table_name):
    """Display the contents of a table."""
    DataLoaderSQLite.display_table(table_name)

@flow
def etl_flow():
    """Main Prefect flow orchestrating the ETL pipeline."""
    
    fdny_df = extract_fdny()
    property_df = extract_prop()
    extract_geojson()
    
    cleaned_fdny_df = transform_fdny_dataframe(fdny_df)
    cleaned_prop_df = transform_prop_dataframe(property_df)
    merged_df = merge_data(cleaned_fdny_df, cleaned_prop_df)
    
    load_data(cleaned_fdny_df, 'fdny_fire_incidents')
    load_data(cleaned_prop_df, 'ny_property_details')
    load_data(merged_df, 'ny_fire_incidents_enriched')

    display_table('fdny_fire_incidents')
    display_table('ny_property_details')
    display_table('ny_fire_incidents_enriched')

    # Validate data
    #validation_result = validate()
    # logger.info("ETL pipeline complete. Validation result:")
    # logger.info(validation_result)

if __name__ == '__main__':
    etl_flow()