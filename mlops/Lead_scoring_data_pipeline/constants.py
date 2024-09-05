# You can create more variables according to your project. The following are the basic variables that have been provided to you
DB_PATH = '/home/database'
DB_FILE_NAME = 'lead_scoring_data_cleaning.db'
DATA_DIRECTORY = '/home/Assignment/01_data_pipeline/scripts/data'
DATA_FILE_NAME = 'leadscoring.csv'
INTERACTION_MAPPING = '/home/Assignment/01_data_pipeline/scripts/mapping/interaction_mapping.csv'
INDEX_COLUMNS_TRAINING = ["created_date",
    "city_mapped",
    "first_platform_c",
    "first_utm_medium_c",
    "first_utm_source_c",
    "total_leads_droppped",
    "referred_lead"]
INDEX_COLUMNS_INFERENCE = ['app_complete_flag']
NOT_FEATURES = [
    '1_on_1_industry_mentorship', 
    'call_us_button_clicked', 'career_coach', 'career_impact', 
    'careers', 'chat_clicked', 'companies', 'download_button_clicked', 
    'download_syllabus', 'emi_partner_click', 'emi_plans_clicked', 'fee_component_click', 
    'hiring_partners', 'homepage_upgrad_support_number_clicked', 
    'industry_projects_case_studies', 'live_chat_button_clicked', 
    'payment_amount_toggle_mover', 'placement_support', 
    'placement_support_banner_tab_clicked', 'program_structure', 
    'programme_curriculum', 'programme_faculty', 
    'request_callback_on_instant_customer_support_cta_clicked', 
    'shorts_entry_click', 'social_referral_click', 
    'specialisation_tab_clicked', 'specializations', 'specilization_click', 
    'syllabus', 'syllabus_expand', 'syllabus_submodule_expand', 
    'tab_career_assistance', 'tab_job_opportunities', 'tab_student_support', 
    'view_programs_page', 'whatsapp_chat_click']

DB_DATA_TABLE_NAME = 'loaded_data'
DB_MAPPING_TABLE_NAME = 'city_tier_mapped'
DB_CAT_MAP_TABLE_NAME = 'categorical_variables_mapped'
DB_INTER_MAP_TABLE_NAME = 'interactions_mapped'

#Test Properties
UNIT_TEST_DB_PATH = '/home/Assignment/01_data_pipeline/scripts'
UNIT_TEST_DB_FILE_NAME = 'unit_test_cases.db'
UNIT_TEST_DATA_DIRECTORY = '/home/Assignment/01_data_pipeline/scripts'
UNIT_TEST_DATA_FILE_NAME = 'leadscoring_test.csv'
UNIT_TEST_DB_DATA_TABLE_NAME = 'loaded_data_test_case'
UNIT_TEST_DB_MAPPING_TABLE_NAME = 'city_tier_mapped_test_case'
UNIT_TEST_DB_CAT_MAP_TABLE_NAME = 'categorical_variables_mapped_test_case'
UNIT_TEST_DB_INTER_MAP_TABLE_NAME = 'interactions_mapped_test_case'
