# modules/loader.py
import os
import pandas as pd
from sqlalchemy import MetaData, Table, create_engine

import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class DataLoaderSQLite:
    base_file_path = os.path.dirname(os.path.abspath(__file__))
    DB_PATH = os.path.join(base_file_path, '..', 'data', 'fire_incidents.db')

    @staticmethod
    def load_dataframe(df: pd.DataFrame, table_name: str):
        engine = create_engine(f'sqlite:///{DataLoaderSQLite.DB_PATH}')
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logger.info(f"Loaded {len(df)} rows into SQLite table '{table_name}'.")
    
    def drop_table(table_name: str):
        engine = create_engine(f'sqlite:///{DataLoaderSQLite.DB_PATH}')
        # drop the table if it exists
        try:
            table = Table(table_name, MetaData(), autoload_with=engine)
            table.drop(engine)
            logger.info(f"Dropped table '{table_name}'.")
        except Exception as e:
            logger.info(f"Table '{table_name}' does not exist or could not be dropped. Error: {e}")


    @staticmethod
    def display_table(table_name: str):
        engine = create_engine(f'sqlite:///{DataLoaderSQLite.DB_PATH}')
        df = pd.read_sql_table(table_name, con=engine)
        logger.info(df)

