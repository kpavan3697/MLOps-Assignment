##############################################################################
# Import necessary modules and files
# #############################################################################


import pandas as pd
import os
import sqlite3
from sqlite3 import Error
from Lead_scoring_data_pipeline.mapping.city_tier_mapping import city_tier_mapping
from Lead_scoring_data_pipeline.mapping.significant_categorical_level import *

from Lead_scoring_data_pipeline.constants import (
    DB_PATH,
    DB_FILE_NAME,
    DATA_DIRECTORY,
    DATA_FILE_NAME,
    DB_DATA_TABLE_NAME,
    DB_MAPPING_TABLE_NAME,
    DB_CAT_MAP_TABLE_NAME,
    DB_INTER_MAP_TABLE_NAME,
    INTERACTION_MAPPING,
    INDEX_COLUMNS_TRAINING,
    INDEX_COLUMNS_INFERENCE,
    NOT_FEATURES
)
###############################################################################
# Define the function to build database
# ##############################################################################

def build_dbs():
    '''
    This function checks if the db file with specified name is present 
    in the /Assignment/01_data_pipeline/scripts folder. If it is not present it creates 
    the db file with the given name at the given path. 


    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should exist  


    OUTPUT
    The function returns the following under the given conditions:
        1. If the file exists at the specified path
                prints 'DB Already Exists' and returns 'DB Exists'

        2. If the db file is not present at the specified loction
                prints 'Creating Database' and creates the sqlite db 
                file at the specified path with the specified name and 
                once the db file is created prints 'New DB Created' and 
                returns 'DB created'


    SAMPLE USAGE
        build_dbs()
    '''

    db_full_path = os.path.join(DB_PATH, DB_FILE_NAME)
    print("db_full_path ::",db_full_path)
    # Check if the database file exists at the specified path
    if os.path.isfile(db_full_path):
        print('DB Already Exists')
        return 'DB Exists'
    else:
        print('Creating Database')
        # Create the database file
        conn = sqlite3.connect(db_full_path)
        conn.close()
        print('New DB Created')
        return 'DB created'
    
    

###############################################################################
# Define function to load the csv file to the database
# ############################################################################## 

def load_data_into_db():
    '''
    Thie function loads the data present in data directory into the db
    which was created previously.
    It also replaces any null values present in 'toal_leads_dropped' and
    'referred_lead' columns with 0.


    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be
        DATA_DIRECTORY : path of the directory where 'leadscoring.csv' 
                        file is present
        

    OUTPUT
        Saves the processed dataframe in the db in a table named 'loaded_data'.
        If the table with the same name already exsists then the function 
        replaces it.


    SAMPLE USAGE
        load_data_into_db()
    '''
    
    
    # Full paths
    db_full_path = os.path.join(DB_PATH, DB_FILE_NAME)
    csv_file_path = os.path.join(DATA_DIRECTORY, DATA_FILE_NAME)
    
    build_dbs()
    
    # Check if the database file exists
    if not os.path.isfile(db_full_path):
        raise FileNotFoundError(f"Database file '{db_full_path}' does not exist. Please create it first.")

    # Load data from CSV
    df = pd.read_csv(csv_file_path)

    # Replace null values with 0 in specified columns
    df['total_leads_droppped'].fillna(0, inplace=True)
    df['referred_lead'].fillna(0, inplace=True)

    # Connect to the database
    conn = sqlite3.connect(db_full_path)
    
    # Load data into the database table 'loaded_data'
    df.to_sql(DB_DATA_TABLE_NAME, conn, if_exists='replace', index=False)
    
    # Close the connection
    conn.close()
    
    print("Data successfully loaded into the database.", DB_DATA_TABLE_NAME)


###############################################################################
# Define function to map cities to their respective tiers
# ##############################################################################

    
def map_city_tier():
    '''
    This function maps all the cities to their respective tier as per the
    mappings provided in the city_tier_mapping.py file. If a
    particular city's tier isn't mapped(present) in the city_tier_mapping.py 
    file then the function maps that particular city to 3.0 which represents
    tier-3.


    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be
        city_tier_mapping : a dictionary that maps the cities to their tier

    
    OUTPUT
        Saves the processed dataframe in the db in a table named
        'city_tier_mapped'. If the table with the same name already 
        exsists then the function replaces it.

    
    SAMPLE USAGE
        map_city_tier()

    '''
    
    # Define paths and file names
    db_full_path = os.path.join(DB_PATH, DB_FILE_NAME)
    
    # Check if the database file exists
    if not os.path.isfile(db_full_path):
        raise FileNotFoundError(f"Database file '{db_full_path}' does not exist. Please create it first.")

    
    # Connect to the database
    conn = sqlite3.connect(db_full_path)
    
    # Load data from the 'loaded_data' table
    df = pd.read_sql(f'SELECT * FROM {DB_DATA_TABLE_NAME}', conn)
    
    # Map cities to their respective tiers
    df['city_tier'] = df['city_mapped'].map(city_tier_mapping).fillna(3.0)
    
    # Save the processed dataframe into the database table 'city_tier_mapped'
    df.to_sql(DB_MAPPING_TABLE_NAME, conn, if_exists='replace', index=False)
    
    # Close the connection
    conn.close()
    
    print(f"City tier mapping completed and data saved to {DB_MAPPING_TABLE_NAME} table.")

###############################################################################
# Define function to map insignificant categorial variables to "others"
# ##############################################################################


def map_categorical_vars():
    '''
    This function maps all the insignificant variables present in 'first_platform_c'
    'first_utm_medium_c' and 'first_utm_source_c'. The list of significant variables
    should be stored in a python file in the 'significant_categorical_level.py' 
    so that it can be imported as a variable in utils file.
    

    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be present
        list_platform : list of all the significant platform.
        list_medium : list of all the significat medium
        list_source : list of all rhe significant source

        **NOTE : list_platform, list_medium & list_source are all constants and
                 must be stored in 'significant_categorical_level.py'
                 file. The significant levels are calculated by taking top 90
                 percentils of all the levels. For more information refer
                 'data_cleaning.ipynb' notebook.
  

    OUTPUT
        Saves the processed dataframe in the db in a table named
        'categorical_variables_mapped'. If the table with the same name already 
        exsists then the function replaces it.

    
    SAMPLE USAGE
        map_categorical_vars()
    '''
    
    
    # Define paths and file names
    db_full_path = os.path.join(DB_PATH, DB_FILE_NAME)
    
    # Check if the database file exists
    if not os.path.isfile(db_full_path):
        raise FileNotFoundError(f"Database file '{db_full_path}' does not exist. Please create it first.")
    
    # Connect to the database
    conn = sqlite3.connect(db_full_path)
    
    # Load data
    df_lead_scoring = pd.read_sql(f'SELECT * FROM {DB_MAPPING_TABLE_NAME}', conn)
    
    
    # all the levels below 90 percentage are assgined to a single level called others
    new_df = df_lead_scoring[~df_lead_scoring['first_platform_c'].isin(list_platform)] # get rows for levels which are not present in list_platform
    new_df['first_platform_c'] = "others" # replace the value of these levels to others
    old_df = df_lead_scoring[df_lead_scoring['first_platform_c'].isin(list_platform)] # get rows for levels which are present in list_platform
    df = pd.concat([new_df, old_df]) # concatenate new_df and old_df to get the final dataframe

    
    # all the levels below 90 percentage are assgined to a single level called others
    new_df = df[~df['first_utm_medium_c'].isin(list_medium)] # get rows for levels which are not present in list_medium
    new_df['first_utm_medium_c'] = "others" # replace the value of these levels to others
    old_df = df[df['first_utm_medium_c'].isin(list_medium)] # get rows for levels which are present in list_medium
    df = pd.concat([new_df, old_df]) # concatenate new_df and old_df to get the final dataframe

    # all the levels below 90 percentage are assgined to a single level called others
    new_df = df[~df['first_utm_source_c'].isin(list_source)] # get rows for levels which are not present in list_source
    new_df['first_utm_source_c'] = "others" # replace the value of these levels to others
    old_df = df[df['first_utm_source_c'].isin(list_source)] # get rows for levels which are present in list_source
    df = pd.concat([new_df, old_df]) # concatenate new_df and old_df to get the final dataframe

    
    df['total_leads_droppped'] = df['total_leads_droppped'].fillna(0)
    df['referred_lead'] = df['referred_lead'].fillna(0)
    df = df.drop(['city_mapped'], axis = 1)
    df = df.drop_duplicates()
    
    
    # Save the processed dataframe into the database table 'categorical_variables_mapped'
    df.to_sql(DB_CAT_MAP_TABLE_NAME, conn, if_exists='replace', index=False)
    
    # Close the connection
    conn.close()
    
    print(f"Categorical variable mapping completed and data saved to {DB_CAT_MAP_TABLE_NAME} table.")


##############################################################################
# Define function that maps interaction columns into 4 types of interactions
# #############################################################################
def interactions_mapping():
    '''
    This function maps the interaction columns into 4 unique interaction columns
    These mappings are present in 'interaction_mapping.csv' file. 


    INPUTS
        DB_FILE_NAME: Name of the database file
        DB_PATH : path where the db file should be present
        INTERACTION_MAPPING : path to the csv file containing interaction's
                                   mappings
        INDEX_COLUMNS_TRAINING : list of columns to be used as index while pivoting and
                                 unpivoting during training
        INDEX_COLUMNS_INFERENCE: list of columns to be used as index while pivoting and
                                 unpivoting during inference
        NOT_FEATURES: Features which have less significance and needs to be dropped
                                 
        NOTE : Since while inference we will not have 'app_complete_flag' which is
        our label, we will have to exculde it from our features list. It is recommended 
        that you use an if loop and check if 'app_complete_flag' is present in 
        'categorical_variables_mapped' table and if it is present pass a list with 
        'app_complete_flag' column, or else pass a list without 'app_complete_flag'
        column.

    
    OUTPUT
        Saves the processed dataframe in the db in a table named 
        'interactions_mapped'. If the table with the same name already exsists then 
        the function replaces it.
        
        It also drops all the features that are not requried for training model and 
        writes it in a table named 'model_input'

    
    SAMPLE USAGE
        interactions_mapping()
    '''
    
    # Define full database path
    db_full_path = os.path.join(DB_PATH, DB_FILE_NAME)
    
    # Check if the database file exists
    if not os.path.isfile(db_full_path):
        raise FileNotFoundError(f"Database file '{db_full_path}' does not exist. Please create it first.")

    # Load interaction mappings
    interaction_mapping_df = pd.read_csv(INTERACTION_MAPPING)

    # Connect to the database
    conn = sqlite3.connect(db_full_path)
    
    # Load data from the 'categorical_variables_mapped' table
    df = pd.read_sql(f'SELECT * FROM {DB_CAT_MAP_TABLE_NAME}', conn)
    
    
    df_unpivot = pd.melt(df, id_vars=['created_date', 'first_platform_c',
       'first_utm_medium_c', 'first_utm_source_c', 'total_leads_droppped', 'city_tier',
       'referred_lead', 'app_complete_flag'], var_name='interaction_type', value_name='interaction_value')
    df_unpivot['interaction_value'] = df_unpivot['interaction_value'].fillna(0)
    df = pd.merge(df_unpivot, interaction_mapping_df, on='interaction_type', how='left')
    df = df.drop(['interaction_type'], axis=1)
    
    # Save the processed dataframe into the database table 'interactions_mapped'
    df.to_sql(DB_INTER_MAP_TABLE_NAME, conn, if_exists='replace', index=False)
    
    # pivoting the interaction mapping column values to individual columns in the dataset
    df_pivot = df.pivot_table(
            values='interaction_value', index=['created_date', 'city_tier', 'first_platform_c',
           'first_utm_medium_c', 'first_utm_source_c', 'total_leads_droppped',
           'referred_lead', 'app_complete_flag'], columns='interaction_mapping', aggfunc='sum')
    df_pivot = df_pivot.reset_index()
    
    # Save the cleaned dataframe into the database table 'model_input'
    df_pivot.to_sql('model_input', conn, if_exists='replace', index=False)
    
    # Close the connection
    conn.close()
    
    print(f"Interactions mapped and data saved to {DB_INTER_MAP_TABLE_NAME} and 'model_input' tables.")
    
