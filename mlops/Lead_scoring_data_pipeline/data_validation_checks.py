"""
Import necessary modules
############################################################################## 
"""

import pandas as pd
# Import the schema from schema.py
from Lead_scoring_data_pipeline.schema import *
from Lead_scoring_data_pipeline.constants import *
import sqlite3
###############################################################################
# Define function to validate raw data's schema
# ############################################################################## 

def raw_data_schema_check():
    '''
    This function check if all the columns mentioned in schema.py are present in
    leadscoring.csv file or not.

   
    INPUTS
        DATA_DIRECTORY : path of the directory where 'leadscoring.csv' 
                        file is present
        raw_data_schema : schema of raw data in the form oa list/tuple as present 
                          in 'schema.py'

    OUTPUT
        If the schema is in line then prints 
        'Raw datas schema is in line with the schema present in schema.py' 
        else prints
        'Raw datas schema is NOT in line with the schema present in schema.py'

    
    SAMPLE USAGE
        raw_data_schema_check
    '''
    

    # Define the path to the CSV file
    csv_file_path = f"{DATA_DIRECTORY}/{DATA_FILE_NAME}"

    # Load the CSV file
    try:
        df = pd.read_csv(csv_file_path)
    except FileNotFoundError:
        print(f"File not found: {csv_file_path}")
        return

    # Get the columns present in the CSV file
    csv_columns = set(df.columns)
    
    # Check if all schema columns are in the CSV columns
    schema_columns = set(raw_data_schema)
    
    if schema_columns.issubset(csv_columns):
        print('Raw data schema is in line with the schema present in schema.py')
    else:
        print('Raw data schema is NOT in line with the schema present in schema.py')


"""
###############################################################################
# Define function to validate model's input schema
############################################################################### 
"""


def model_input_schema_check():
    '''
    This function check if all the columns mentioned in model_input_schema in 
    schema.py are present in table named in 'model_input' in db file.

   
    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be present
        model_input_schema : schema of models input data in the form oa list/tuple
                          present as in 'schema.py'

    OUTPUT
        If the schema is in line then prints 
        'Models input schema is in line with the schema present in schema.py'
        else prints
        'Models input schema is NOT in line with the schema present in schema.py'
    
    SAMPLE USAGE
        raw_data_schema_check
    '''
    
    # Define the path to the database file
    db_file_path = f"{DB_PATH}/{DB_FILE_NAME}"

    # Connect to the SQLite database
    try:
        conn = sqlite3.connect(db_file_path)
        cursor = conn.cursor()

        # Query to get the columns of the 'model_input' table
        cursor.execute("PRAGMA table_info(model_input);")
        columns_info = cursor.fetchall()

        # Extract column names from the query result
        db_columns = {col[1] for col in columns_info}  # col[1] is the column name

        # Close the database connection
        conn.close()

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        return
    except FileNotFoundError:
        print(f"Database file not found: {db_file_path}")
        return

    # Get the columns from the schema
    schema_columns = set(model_input_schema)

    # Check if all schema columns are in the DB columns
    if schema_columns.issubset(db_columns):
        print('Model input schema is in line with the schema present in schema.py')
    else:
        print('Model input schema is NOT in line with the schema present in schema.py')

    

    
    
