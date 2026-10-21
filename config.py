# config.py
import os
from pymongo import MongoClient
# --- MongoDB URI ---

MONGO_URI = os.environ.get("MONGO_URI", "")
client = MongoClient(MONGO_URI)
SENDER_EMAIL    = os.environ.get("SENDER_EMAIL", "")
SENDER_PASSWORD = os.environ.get("SENDER_PASSWORD", "")

TEAMS_WEBHOOK_URL = os.environ.get("TEAMS_WEBHOOK_URL", "")
# --- Database and Collection Names ---

FILE_TRACKING_DB = "file_tracking"
FILE_TRACKING_COLLECTION = "file_metadata"

UNIFORMITY_DB = "UNIFORMITY"
UNIFORMITY_DB_new = "UNIFORMITY_HOME"
UNIFORMITY_DB_history = "UNIFORMITY_HISTORICAL"
BALANCING_DB = "BALANCING"
BALANCING_DB_new = "BALANCING_HOME"
BALANCING_DB_history = "BALANCING_HISTORICAL"
GEOMETRY_DB = "GEOMETRY"
GEOMETRY_DB_new = "GEOMETRY_HOME"
GEOMETRY_DB_history = "GEOMETRY_HISTORICAL"
Data_Clean_BALANCING = "Data_Clean_BALANCING"
Data_Clean_BALANCING_HOME = "Data_Clean_BALANCING_HOME"
Data_Clean_BALANCING_HISTORICAL_DATA = "Data_Clean_BALANCING_HISTORICAL_DATA"
Data_Clean_UNIFORMITY = "Data_Clean_UNIFORMITY"
Data_Clean_UNIFORMITY_HOME = "Data_Clean_UNIFORMITY_HOME"
Data_Clean_UNIFORMITY_HISTORICAL_DATA = "Data_Clean_UNIFORMITY_HISTORICAL_DATA"
Data_Clean_GEOMETRY = "Data_Clean_GEOMETRY"
Data_Clean_GEOMETRY_HOME = "Data_Clean_GEOMETRY_HOME"
Data_Clean_GEOMETRY_HISTORICAL_DATA = "Data_Clean_GEOMETRY_HISTORICAL_DATA"

UNIFORMITY_CLEANED_PREFIX = "data_Cleaned_UNIFORMITY_"
UNIFORMITY_CLEANED_PREFIX_new = "data_Cleaned_UNIFORMITY_HOME_"
UNIFORMITY_CLEANED_PREFIX_history = "data_Cleaned_UNIFORMITY_HISTORICAL_DATA_"
BALANCING_CLEANED_PREFIX = "data_Cleaned_BALANCING_"
BALANCING_CLEANED_PREFIX_new = "data_Cleaned_BALANCING_HOME_"
BALANCING_CLEANED_PREFIX_history = "data_Cleaned_BALANCING_HISTORICAL_DATA_"
GEOMETRY_CLEANED_PREFIX = "data_Cleaned_GEOMETRY_"
GEOMETRY_CLEANED_PREFIX_new = "data_Cleaned_GEOMETRY_HOME_"
GEOMETRY_CLEANED_PREFIX_history = "data_Cleaned_GEOMETRY_HISTORICAL_DATA_"

UNIFORMITY_INVALID_PREFIX = "invalid_rows_UNIFORMITY_"
BALANCING_INVALID_PREFIX = "invalid_rows_BALANCING_"
GEOMETRY_INVALID_PREFIX = "invalid_rows_GEOMETRY_"

COMBINED_Database = "all_combined_data"
COMBINED_Database_new = "all_combined_data_home"
COMBINED_Database_history = "all_combined_data_historical_data"
ALL_COMBINED_UNIFORMITY_COLLECTION = "all_combined_uniformity_data"
ALL_COMBINED_UNIFORMITY_COLLECTION_new = "all_combined_uniformity_data_home"
ALL_COMBINED_UNIFORMITY_COLLECTION_history = "all_combined_uniformity_data_historical_data"
ALL_COMBINED_BALANCING_COLLECTION = "all_combined_balancing_data"
ALL_COMBINED_BALANCING_COLLECTION_new = "all_combined_balancing_data_home"
ALL_COMBINED_BALANCING_COLLECTION_history = "all_combined_balancing_data_historical_data"
ALL_COMBINED_GEOMETRY_COLLECTION = "all_combined_geometry_data"
ALL_COMBINED_GEOMETRY_COLLECTION_new = "all_combined_geometry_data_home"
ALL_COMBINED_GEOMETRY_COLLECTION_history = "all_combined_geometry_data_historical_data"

INVALID_ROWS_DB = "INVALID_ROWS"
UNIFORMITY_BALANCING_GEOMETRY_DB = "UNIFORMITY_BALANCING_GEOMETRY_merged_cleaned_data"
UNIFORMITY_BALANCING_GEOMETRY_DB_new = "UNIFORMITY_BALANCING_GEOMETRY_merged_cleaned_data_home"
UNIFORMITY_BALANCING_GEOMETRY_DB_history = "UNIFORMITY_BALANCING_GEOMETRY_merged_cleaned_data_historical"
all_combined_uniformity_data = "all_combined_uniformity_data"
all_combined_uniformity_data_home = "all_combined_uniformity_data_home"
all_combined_uniformity_data_historical_data = "all_combined_uniformity_data_historical_data"
all_combined_balancing_data = "all_combined_balancing_data"
all_combined_balancing_data_home = "all_combined_balancing_data_home"
all_combined_balancing_data_historical_data = "all_combined_balancing_data_historical_data"
all_combined_geometry_data = "all_combined_geometry_data"
all_combined_geometry_data_home = "all_combined_geometry_data_home"
all_combined_geometry_data_historical_data = "all_combined_geometry_data_historical_data"

# COMBINED_COLLECTION = "TUB_CIMCORP_cleaned_merge_data"
# inner_join_collection_name = "data_mereged_dashboard"
uniformity_collection_name = "data_mereged_uniformity"
uniformity_collection_name_new = "data_mereged_uniformity_home"
uniformity_collection_name_history = "data_mereged_uniformity_historical_data"
balancing_collection_name = "data_mereged_balancing"
balancing_collection_name_new = "data_mereged_balancing_home"
balancing_collection_name_history = "data_mereged_balancing_historical_data"
geometry_collection_name = "data_mereged_geometry"
geometry_collection_name_new = "data_mereged_geometry_home"
geometry_collection_name_history = "data_mereged_geometry_historical_data"
new_uniformity_collection_name = "new_data_mereged_uniformity"
new_balancing_collection_name = "new_data_mereged_balancing"
new_geometry_collection_name = "new_data_mereged_geometry"
COMBINED_COLLECTION_NAME = "data_merged_combined_uniform_balan_geo"
new_COMBINED_COLLECTION_NAME = "new_data_merged_combined_uniform_balan_geo"
COMBINED_COLLECTION_NAME_new = "data_merged_combined_uniform_balan_geo_home"
COMBINED_COLLECTION_NAME_history = "data_merged_combined_uniform_balan_geo_historical_data"

config_databse = "production_db"
channel_no_collect = "channel_no_ufm"
channel_no_collect_tbm = 'channel_no_tbm'
channel_no_collect_curing = 'channel_no_curing'

lot_db = "lot_db"
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
alert_table_final_yield = "alert_table_final_yield"
summary_db = "summary_db"
alert_deviated_final_yield = "alert_deviated_final_yield"

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
alert_users = "alert_users"


DAILY_YIELD_HISTORY = "daily_yield_historical_data"
LATEST_YIELD = "latest_yield"