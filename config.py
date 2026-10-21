# config.py
from pymongo import MongoClient
# --- MongoDB URI ---

MONGO_URI = "mongodb://admin:admin123@10.100.192.85:27017/"
client = MongoClient(MONGO_URI)
SENDER_EMAIL    = 'bb9054407@gmail.com'
SENDER_PASSWORD = 'pwrs wvax hkfo ffiz'
# production
# TEAMS_WEBHOOK_URL = 'https://prod-19.centralindia.logic.azure.com:443/workflows/bee492ae48074bc89b0f2caa2b593d6c/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=LZ9grNE1LLfAgtKl1TgVI4W-P6O7jtFexYcMXqrkfPI'
TEAMS_WEBHOOK_URL =  'https://defaultf792002939f941b79c94ec73395d18.4d.environment.api.powerplatform.com:443/powerautomate/automations/direct/cu/07/workflows/a4711ce7bfd24f23b52e1adb6c8e2e65/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=l_e36RS6GBliBAVai6jXyeW5BX3aX6gpEbR6up0tnwM'
# TEAMS_WEBHOOK_URL = 'https://defaultf792002939f941b79c94ec73395d18.4d.environment.api.powerplatform.com:443/powerautomate/automations/direct/cu/19/workflows/c3aff12f860d404f805203d8a7f2c886/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=mZTHREr04jDTqTv5kWd4Y1DcFPWsSjtQToEoTxlexoU'
# #test
# TEAMS_WEBHOOK_URL = 'https://defaultf792002939f941b79c94ec73395d18.4d.environment.api.powerplatform.com:443/powerautomate/automations/direct/cu/18/workflows/51aff480a3c44b36a7ebaac49aef0218/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=aBTgjjCnAPwMblqB8bMKBOp_iFQUCh1-Xr7-kFz5A78'

FILE_TRACKING_DB = "file_tracking"
FILE_TRACKING_COLLECTION = "file_metadata"

TUB_DB = "TUB"
TUB_DB_new = "TUB_HOME"
TUB_DB_history = "TUB_HISTORICAL"
CIMCORP_DB = "CIMCORP"
CIMCORP_DB_new = "CIMCORP_HOME"
CIMCORP_DB_history = "CIMCORP_HISTORICAL_DATA"
Data_Clean_CIMCORP = "Data_Clean_CIMCORP"
Data_Clean_CIMCORP_HOME = "Data_Clean_CIMCORP_HOME"
Data_Clean_CIMCORP_HISTORICAL_DATA = "Data_Clean_CIMCORP_HISTORICAL_DATA"
Data_Clean_TUB = "Data_Clean_TUB"
Data_Clean_TUB_HOME = "Data_Clean_TUB_HOME"
Data_Clean_TUB_HISTORICAL_DATA = "Data_Clean_TUB_HISTORICAL_DATA"

TUB_CLEANED_PREFIX = "data_Cleaned_TUB_"
TUB_CLEANED_PREFIX_new = "data_Cleaned_TUB_HOME_"
TUB_CLEANED_PREFIX_history = "data_Cleaned_TUB_HISTORICAL_DATA_"
CIMCORP_CLEANED_PREFIX = "data_Cleaned_CIMCORP_"
CIMCORP_CLEANED_PREFIX_new = "data_Cleaned_CIMCORP_HOME_"
CIMCORP_CLEANED_PREFIX_history = "data_Cleaned_CIMCORP_HISTORICAL_DATA_"

TUB_INVALID_PREFIX = "invalid_rows_TUB_"
CIMCORP_INVALID_PREFIX = "invalid_rows_CIMCORP_"

COMBINED_Database = "all_combined_data"
COMBINED_Database_new = "all_combined_data_home"
COMBINED_Database_history = "all_combined_data_historical_data"
ALL_COMBINED_TUB_COLLECTION = "all_combined_tub_data"
ALL_COMBINED_TUB_COLLECTION_new = "all_combined_tub_data_home"
ALL_COMBINED_TUB_COLLECTION_history = "all_combined_tub_data_historical_data"
ALL_COMBINED_CIMCORP_COLLECTION = "all_combined_cimcorp_data"
ALL_COMBINED_CIMCORP_COLLECTION_new = "all_combined_cimcorp_data_home"
ALL_COMBINED_CIMCORP_COLLECTION_history = "all_combined_cimcorp_data_historical_data"

INVALID_ROWS_DB = "INVALID_ROWS"
TUB_CIMCORP_DB = "TUB_CIMCORP_merged_cleaned_data"
TUB_CIMCORP_DB_new = "TUB_CIMCORP_merged_cleaned_data_home"
TUB_CIMCORP_DB_history = "TUB_CIMCORP_merged_cleaned_data_historical_data"
all_combined_tub_data = "all_combined_tub_data"
all_combined_cimcorp_data = "all_combined_cimcorp_data"
all_combined_tub_data_home = "all_combined_tub_data_home"
all_combined_tub_data_historical_data = "all_combined_tub_data_historical_data"
all_combined_cimcorp_data_home = "all_combined_cimcorp_data_home"
all_combined_cimcorp_data_historical_data = "all_combined_cimcorp_data_historical_data"

# COMBINED_COLLECTION = "TUB_CIMCORP_cleaned_merge_data"
inner_join_collection_name = "data_mereged_dashboard"
inner_join_collection_name_new = "data_mereged_dashboard_home"
inner_join_collection_name_history = "data_mereged_dashboard_historical_data"
tub_collection_name = "data_mereged_tub"
tub_collection_name_new = "data_mereged_tub_home"
tub_collection_name_history = "data_mereged_tub_historical_data"
cimcorp_collection_name = "data_mereged_dashboard"
cimcorp_collection_name_new = "data_mereged_dashboard_home"
cimcorp_collection_name_history = "data_mereged_dashboard_historical_data"
new_tub_collection_collection="new_data_mereged_tub"
new_inner_join_collection_collection="new_data_mereged_dashboard"

config_databse = "production_db"
channel_no_collect = "channel_no_ufm"
channel_no_collect_tbm = 'channel_no_tbm'
channel_no_collect_curing = 'channel_no_curing'

lot_db = "lot_db"
alert_db = "alert_db"
alert_db_dashboard = "alert_db_dashboard"
alert_db_virgin_yield = "alert_db_virgin_yield"
alert_db_final_yield = "alert_db_final_yield"
###### Add 18-07 ######
model_db = "model_workcenter_db"
curing_collection = "curing_modelno"
tbm_collection = "tbm_gtcodes"
ufm_collection = "ufm_modelno"
tbm_collection_machine = "tbm_machine"
curing_collection_machine = "curing_machine"
ufm_collection_machine = "ufm_machine"
###### Add 18-07 ######
alert_table = "alert_table"
summary_db = "summary_db"
alert_deviated = "alert_deviated"

machine_no_collect = "machine_no"
ub_machine_no_collect= "ub_machine_no"
curing_press_dict_collect = "curing_press_dict"

average_db = "average_db"
lot_tbm_collect_name = "lot_tbm_table"
lot_tbm_collect_name_new= "lot_tbm_table_new"
alert_tbm_collect_name = "alert_tbm_table_with_lot_size"
alert_tbm_collect_name_new = "alert_tbm_table_without_lot_size"
averages_tbm_table = "averages_tbm_table"
averages_tbm_table_new = "averages_tbm_table_new"


lot_table_collect = "lot_ubno_table"
lot_ufm_collect_name_new = "lot_ubno_table_new"
alert_ufm_collect_name = "alert_ufm_table_with_lot_size"
alert_ufm_collect_name_new = "alert_ufm_table_without_lot_size"
alert_triggering_ubno_collect_name ="alert_triggering_ufm_with_lot_size"
alert_triggering_ubno_collect_name_new = "alert_triggering_ufm_without_lot_size"

averages_ufm_table = "averages_ufm_table"
averages_ufm_table_new = "averages_ufm_table_new"
alert_triggering_deviated_ufm = "alert_ufm_with_lot_size"
alert_triggering_deviated_ufm_new = "alert_ufm_without_lot_size"


alert_triggering_collect_name = "alert_triggering_tbm_with_lot_size"

alert_triggering_collect_name_new = "alert_triggering_tbm_without_lot_size"
summary_triggering_collect_name_tbm = "alert1_summary_triggering_tbm"
alert_triggering_deviated_tbm = "alert_tbm_with_lot_size"
alert_triggering_deviated_tbm_new = "alert_tbm_without_lot_size"  
deviation_occurred_collect_name = "deviation_occurred"
deviation_occurred_param_collect_name = "deviation_occurred_param"

lot_curing_collect_name = "lot_curing_table"
lot_curing_collect_name_new= "lot_curing_table_new"
alert_curing_collect_name = "alert_curing_table_with_lot_size"
alert_curing_collect_name_new = "alert_curing_table_without_lot_size"
averages_curing_table_name = "averages_curing_table"
averages_curing_table_name_new = "averages_curing_table_new"
alert_triggering_curing_collect_name = "alert_triggering_curing_with_lot_size"
alert_triggering_curing_collect_name_new = "alert_triggering_curing_without_lot_size"
alert_triggering_deviated_curing = "alert_curing_with_lot_size"
alert_triggering_deviated_curing_new = "alert_curing_without_lot_size"



lot_tbm_balancing_collect_name = "lot_tbm_table_balancing"
alert_tbm_balancing_collect_name = "alert_tbm_table_balancing"
averages_tbm_table_balancing = "averages_tbm_table_balancing"
alert_triggering_collect_name_balancing = "alert_triggering_balancing_tbm"
lot_curing_balancing_collect_name = "lot_curing_balancing_table"
alert_curing_balancing_collect_name = "alert_curing_balancing_table"
averages_curing_balancing_table_name = "averages_curing_balancing_table"
alert_triggering_curing_balancing_collect_name = "alert_triggering_balancing_curing"
lot_table_collect_bal = "lot_ubno_table_balancing"
alert_table_collect_bal = "alert_triggering_balancing_ubno"



alert_triggering_collect_yield_drop = "alert_triggering_curing_yield_drop"
yield_collect_yield_drop = "yield_curing_table_yield_drop"
alert_collect_yield_drop = "alert_curing_table_yield_drop"
lot_curing_collect_yield_drop = "lot_curing_table_yield_drop"
alert_triggering_tbm_collect_yield_drop = "alert_triggering_tbm_yield_drop"
yield_tbm_collect_yield_drop = "yield_tbm_table_yield_drop"
alert_tbm_collect_yield_drop = "alert_tbm_table_yield_drop"
lot_tbm_collect_yield_drop = "lot_tbm_table_yield_drop"
lot_table_collect_yield = "lot_ubno_table_yield_drop"
alert_table_collect_yield = "alert_triggering_ubno_yield_drop"
yield_db = "yield_db"


lot_tbm_load_collect_name = "lot_tbm_table_load"
alert_tbm_load_collect_name = "alert_tbm_table_load"
averages_tbm_table_load = "averages_tbm_table_load"
alert_triggering_collect_name_load = "alert_triggering_load_tbm"
lot_curing_load_collect_name = "lot_curing_load_table"
alert_curing_load_collect_name = "alert_curing_load_table"
averages_curing_load_table_name = "averages_curing_load_table"
alert_triggering_curing_load_collect_name = "alert_triggering_load_curing"
lot_table_collect_load = "lot_ubno_table_load"
alert_table_collect_load = "alert_triggering_load_ubno"

alert_mail_db = "alert_mail_db"  # Add 05-07
admin_table = "admin_table"    # Add 05-07
alert_users = "alert_users"    # Add 05-08



DAILY_YIELD_HISTORY = "daily_yield_historical_data"
LATEST_YIELD = "latest_yield"

CIMCORP_DB_visual = "CIMCORP_VISUAL"
Data_Clean_CIMCORP_VISUAL = "Data_Clean_CIMCORP_VISUAL"
CIMCORP_CLEANED_PREFIX_visual = "data_Cleaned_CIMCORP_VISUAL"

visual_data = "visual_data"
inspection_data = "inspection_data"

All_SLOBS_DATA = "All_SLOBS_DATA"
active_sku_list = "active_sku_list"