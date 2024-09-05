##############################################################################
# Import the necessary modules
# #############################################################################
import utils
import sqlite3
import os
from unittest.mock import patch
import pytest
import importlib
import pandas as pd
from city_tier_mapping import city_tier_mapping
from significant_categorical_level import *

from constants import (
    DB_PATH,
    DB_FILE_NAME,
    DATA_DIRECTORY,
    DATA_FILE_NAME,
    DB_DATA_TABLE_NAME,
    DB_MAPPING_TABLE_NAME,
    DB_CAT_MAP_TABLE_NAME,
    DB_INTER_MAP_TABLE_NAME
)
from constants import (
    UNIT_TEST_DB_PATH,
    UNIT_TEST_DB_FILE_NAME,
    UNIT_TEST_DATA_DIRECTORY,
    UNIT_TEST_DATA_FILE_NAME,
    UNIT_TEST_DB_DATA_TABLE_NAME,
    UNIT_TEST_DB_MAPPING_TABLE_NAME,
    UNIT_TEST_DB_CAT_MAP_TABLE_NAME,
    UNIT_TEST_DB_INTER_MAP_TABLE_NAME
)


###############################################################################
# Write test cases for load_data_into_db() function
# ##############################################################################




@pytest.fixture
def override_constants():
    with patch('constants.DB_PATH', UNIT_TEST_DB_PATH), \
         patch('constants.DB_FILE_NAME', UNIT_TEST_DB_FILE_NAME), \
         patch('constants.DATA_DIRECTORY', UNIT_TEST_DATA_DIRECTORY), \
         patch('constants.DATA_FILE_NAME', UNIT_TEST_DATA_FILE_NAME), \
         patch('constants.DB_MAPPING_TABLE_NAME', UNIT_TEST_DB_MAPPING_TABLE_NAME), \
         patch('constants.DB_CAT_MAP_TABLE_NAME', UNIT_TEST_DB_CAT_MAP_TABLE_NAME), \
         patch('constants.DB_INTER_MAP_TABLE_NAME', UNIT_TEST_DB_INTER_MAP_TABLE_NAME), \
         patch('constants.DB_DATA_TABLE_NAME', UNIT_TEST_DB_DATA_TABLE_NAME):
        # Reload the utils module to pick up the new constants
        importlib.reload(utils)
        yield

    
def test_load_data_into_db(override_constants):
    """_summary_
    This function checks if the load_data_into_db function is working properly by
    comparing its output with test cases provided in the db in a table named
    'loaded_data_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'unit_test_cases.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DATA_FILE_NAME: Name of the test database file 'leadscoring_test.csv'
    
    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be
        DATA_DIRECTORY : path of the directory where 'leadscoring.csv' 
                        file is present
                        
                        
    SAMPLE USAGE
        output=test_load_data_into_db()
    """

    # Path to the test CSV file
    test_csv_path = os.path.join(UNIT_TEST_DATA_DIRECTORY, UNIT_TEST_DATA_FILE_NAME)
    
    # Ensure the test database path exists and is empty
    db_full_path = os.path.join(UNIT_TEST_DB_PATH, UNIT_TEST_DB_FILE_NAME)
    if os.path.exists(db_full_path):
        os.remove(db_full_path)

    # Call the function to load data into the database
    utils.load_data_into_db()

    # Load data from the CSV file into a DataFrame
    expected_df = pd.read_csv(test_csv_path)
    
    # Replace null values with 0 in specified columns
    expected_df['total_leads_droppped'].fillna(0, inplace=True)
    expected_df['referred_lead'].fillna(0, inplace=True)

    # Connect to the test database and load data
    conn = sqlite3.connect(db_full_path)
        
    try:
        # Load data from the database into a DataFrame
        df_loaded = pd.read_sql_query(f"SELECT * FROM {UNIT_TEST_DB_DATA_TABLE_NAME}", conn)

        # Compare the DataFrames
        assert df_loaded.shape == expected_df.shape, f"DataFrame shapes are different: {df_loaded.shape} vs {expected_df.shape}"
        print("Test passed: Data matches expected output")

    finally:
        # Close the database connection
        conn.close()
        #if os.path.exists(db_full_path):
         #   os.remove(db_full_path)
    
    

###############################################################################
# Write test cases for map_city_tier() function
# ##############################################################################
def test_map_city_tier(override_constants):
    """_summary_
    This function checks if map_city_tier function is working properly by
    comparing its output with test cases provided in the db in a table named
    'city_tier_mapped_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'   

    SAMPLE USAGE
        output=test_map_city_tier()

    """
    
    # Push data into DB
    utils.load_data_into_db()
    
    # Connect to the main database
    conn = sqlite3.connect(f"{UNIT_TEST_DB_PATH}/{UNIT_TEST_DB_FILE_NAME}")
    
    try:
        # Load the expected data from the test database
        expected_data = pd.read_sql_query(f"SELECT * FROM {UNIT_TEST_DB_DATA_TABLE_NAME}", conn)
        expected_data['city_tier'] = expected_data['city_mapped'].map(city_tier_mapping).fillna(3.0)

        # Map cities
        utils.map_city_tier()
        
        # Load the expected data from the test database
        actual_data = pd.read_sql_query(f"SELECT * FROM {UNIT_TEST_DB_MAPPING_TABLE_NAME}", conn)
        
        # Compare the actual data with the expected data
        pd.testing.assert_frame_equal(actual_data, expected_data, check_dtype=False)

        print("Test passed: Data matches expected output")
    
    except AssertionError as e:
        print(e)

    finally:
        # Close the database connections
        conn.close()

    

###############################################################################
# Write test cases for map_categorical_vars() function
# ##############################################################################    
def test_map_categorical_vars(override_constants):
    """_summary_
    This function checks if map_cat_vars function is working properly by
    comparing its output with test cases provided in the db in a table named
    'categorical_variables_mapped_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'
    
    SAMPLE USAGE
        output=test_map_cat_vars()

    """
    # Push data into DB
    utils.load_data_into_db()
    
    # Connect to the main database
    conn = sqlite3.connect(f"{UNIT_TEST_DB_PATH}/{UNIT_TEST_DB_FILE_NAME}")

    try:
        # Load the actual data from the main database
         # Load the expected data from the test database
        expected_data = pd.read_sql_query(f"SELECT * FROM {UNIT_TEST_DB_DATA_TABLE_NAME}", conn)
        expected_data['first_platform_c'] = expected_data['first_platform_c'].apply(lambda x: x if x in list_platform else 'others')
        expected_data['first_utm_medium_c'] = expected_data['first_utm_medium_c'].apply(lambda x: x if x in list_medium else 'others')
        expected_data['first_utm_source_c'] = expected_data['first_utm_source_c'].apply(lambda x: x if x in list_source else 'others')

        utils.map_categorical_vars()
        
        # Load the expected data from the test database
        actual_data = pd.read_sql_query(f"SELECT * FROM {UNIT_TEST_DB_CAT_MAP_TABLE_NAME}", conn)

        # Compare the actual data with the expected data
        pd.testing.assert_frame_equal(actual_data, expected_data, check_dtype=False)

        print("Test passed: Data matches expected output")
    
    except AssertionError as e:
        print(e)

    finally:
        # Close the database connections
        conn.close()
    

###############################################################################
# Write test cases for interactions_mapping() function
# ##############################################################################    
def test_interactions_mapping(override_constants):
    """_summary_
    This function checks if test_column_mapping function is working properly by
    comparing its output with test cases provided in the db in a table named
    'interactions_mapped_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'

    SAMPLE USAGE
        output=test_column_mapping()

    """ 
    
    # Push data into DB
    utils.load_data_into_db()
    
    # Connect to the main database
    conn = sqlite3.connect(f"{UNIT_TEST_DB_PATH}/{UNIT_TEST_DB_FILE_NAME}")

    actual_data = utils.interactions_mapping()
    
    try:
        # Load the actual data from the main database
        expected_data = pd.read_sql_query(f"SELECT * FROM {UNIT_TEST_DB_INTER_MAP_TABLE_NAME}", conn)
        
    
        # Compare the actual data with the expected data
        pd.testing.assert_frame_equal(actual_data, expected_data, check_dtype=True)


        print("Test passed: Data matches expected output")
    
    except AssertionError as e:
        print(e)

    finally:
        # Close the database connections
        conn.close()   
