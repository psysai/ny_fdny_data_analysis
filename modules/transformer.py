# modules/transformer.py
import pandas as pd

import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class DataTransformer:
    @staticmethod
    def clean_fdny_fire_incident_data(df: pd.DataFrame) -> pd.DataFrame:
        # Remove any rows with missing values
        fdny_df = df.dropna()
        # Convert zip code to string
        fdny_df.loc[:, 'zipcode'] = fdny_df['zipcode'].astype(str)

        fdny_zipcode_length = fdny_df[(fdny_df['zipcode'].str.len() == 1) \
                             | (fdny_df['zipcode'].str.len() == 6)]['zipcode']
        # remove the zipcodes with lenght 1 or 6
        fdny_df = fdny_df[~fdny_df['zipcode'].isin(fdny_zipcode_length)]
        # truncate the zipcodes to 5 digits
        fdny_df['zipcode'] = fdny_df['zipcode'].str[:5]

        fdny_df.fillna(0, inplace=True)

        fdny_df.loc[:, 'incident_borough'] = fdny_df['incident_borough'].str.replace("RICHMOND / STATEN ISLAND", "STATEN ISLAND")
        return fdny_df
    
    @staticmethod
    def clean_property_data(df: pd.DataFrame) -> pd.DataFrame:
        # Remove any rows with missing values
        property_df = df.dropna()
        # Convert zip code to string
        property_df.loc[:, 'zipcode'] = property_df['zipcode'].astype(str)
        # length of the zipcode in the property table
        prop_zipcode_length = property_df[(property_df['zipcode'].str.len() == 1) 
                                    | (property_df['zipcode'].str.len() == 6)]['zipcode']
        # remove the zipcodes with lenght 1 or 6
        property_df = property_df[~property_df['zipcode'].isin(prop_zipcode_length)]
        # truncate the zipcodes to 5 digits
        property_df['zipcode'] = property_df['zipcode'].str[:5]

        # regroup the property table by zipcode, year, borough and 
        property_df = property_df.groupby(['zipcode', 'year', 'borough'])\
            .agg({'avg_prop_total_value': 'median', 'avg_prop_gross_sqft': 'median'}).reset_index()
        
        # Set decimal point to 0
        property_df = property_df.round(0)

        return property_df
    
    @staticmethod
    def merge_fdny_property_data(fdny_df: pd.DataFrame, property_df: pd.DataFrame) -> pd.DataFrame:
        #rename the incident_borough to borough, and incident_year to year, # and drop the incident_borough and incident_year columns
        fdny_renamed_df = fdny_df.rename(columns={'incident_borough': 'borough', 'incident_year': 'year'})

        # Merge the two DataFrames on the zipcode, borough, and year columns
        merged_df = pd.merge(fdny_renamed_df, property_df, 
                        on=['zipcode', 'borough', 'year'], how='inner')

        return merged_df
    

