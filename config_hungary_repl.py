# config.py
# # //////////////////////////////////////////////////////////////////////////////////////////
# # //////////////////////////////////////////////////////////////////////////////////////////
# from flask import Flask
# from sshtunnel import SSHTunnelForwarder
# from pymongo import MongoClient
# import paramiko
# import socket
 
# paramiko.DSSKey = None  # fix for recent paramiko versions
 
# app = Flask(__name__)
 
# # -----------------------
# # SSH & MongoDB Settings
# SSH_HOST = "10.100.192.123"
# SSH_PORT = 22
# SSH_USER = "ec2-user"
# SSH_PKEY = "C:/Users/Ravikumar Koduru/Downloads/key/OE-YIELD-PEM-KEY.pem"
 
# MONGO_HOST = "localhost"
# MONGO_PORT = 27017
# MONGO_USER = "admin"
# MONGO_PASS = "admin123"
# AUTH_DB = "admin"
# # -----------------------
 
# # Find a free local port to avoid conflicts
# def get_free_port():
#     s = socket.socket()
#     s.bind(('', 0))
#     port = s.getsockname()[1]
#     s.close()
#     return port
 
# LOCAL_PORT = get_free_port()
 
# # -----------------------
# # Start SSH tunnel
# tunnel = SSHTunnelForwarder(
#     (SSH_HOST, SSH_PORT),
#     ssh_username=SSH_USER,
#     ssh_pkey=SSH_PKEY,
#     remote_bind_address=(MONGO_HOST, MONGO_PORT),
#     local_bind_address=('localhost', LOCAL_PORT)
# )
# tunnel.start()
# print(f"SSH tunnel established on local port {LOCAL_PORT}")
 
# # -----------------------
# # Connect PyMongo through the tunnel
# MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASS}@localhost:{LOCAL_PORT}/?authSource={AUTH_DB}"
# client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
 
# # //////////////////////////////////////////////////////////////////////////////////////////
# # //////////////////////////////////////////////////////////////////////////////////////////


from pymongo import MongoClient
# --- MongoDB URI ---

# connect to your local instance
# MONGO_URI = "mongodb://admin:admin123@10.100.192.123:27017/"
MONGO_URI = "mongodb://10.100.192.93:27017/"
client = MongoClient(MONGO_URI,serverSelectionTimeoutMS=5000)
SENDER_EMAIL    = 'testcode498@gmail.com'
SENDER_PASSWORD = 'aejh uukg ebge vauj'

# # ################## Personal Teams Alert ID start #######################################################
# TEAMS_WEBHOOK_URL = 'https://defaultf792002939f941b79c94ec73395d18.4d.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/67fc14590d7f465e95e10d0d4d516677/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=RZrtfWzs6Dvy19O1W7uAJ1i-5JFRxzBrXX3PKH--6Jk'
# # ################## Personal Teams Alert ID end #######################################################

# # ################## Prod Teams Alert ID start #######################################################
TEAMS_WEBHOOK_URL = 'https://defaultf792002939f941b79c94ec73395d18.4d.environment.api.powerplatform.com:443/powerautomate/automations/direct/cu/26/workflows/56e7b47cde2048909bb479c4c4f47e91/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=5GdkmlmyiUXBnHSqBQk8JZV83RZucLdZ1Id-5qNxlWs'
# # ################## Prod Teams Alert ID end #######################################################



# --- Database and Collection Names ---
FILE_TRACKING_DB = "file_tracking"
FILE_TRACKING_COLLECTION = "file_metadata"

TUB_DB = "TUB"
CIMCORP_DB = "CIMCORP"
Data_Clean_CIMCORP = "Data_Clean_CIMCORP"
Data_Clean_TUB = "Data_Clean_TUB"

TUB_CLEANED_PREFIX = "data_Cleaned_TUB_"
CIMCORP_CLEANED_PREFIX = "data_Cleaned_CIMCORP_"

COMBINED_Database = "all_combined_data"
ALL_COMBINED_TUB_COLLECTION = "all_combined_tub_data"
ALL_COMBINED_CIMCORP_COLLECTION = "all_combined_cimcorp_data"

INVALID_ROWS_DB = "INVALID_ROWS"
TUB_CIMCORP_DB = "TUB_CIMCORP_merged_cleaned_data"
all_combined_tub_data = "all_combined_tub_data"
all_combined_cimcorp_data = "all_combined_cimcorp_data"
all_combined_tub_data_home = "all_combined_tub_data_home"
all_combined_tub_data_historical_data = "all_combined_tub_data_historical_data"
all_combined_cimcorp_data_historical_data = "all_combined_cimcorp_data_historical_data"

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

config_databse = "production_db_hungary"
channel_no_collect = "channel_no_ufm"
channel_no_collect_tbm = 'channel_no_tbm'
channel_no_collect_curing = 'channel_no_curing'
gyo_qr_status = 'gyo_qr_status'

lot_db = "lot_db"
alert_db = "alert_db"
alert_db_dashboard = "alert_db_dashboard"
alert_db_virgin_yield = "alert_db_virgin_yield"
alert_db_final_yield = "alert_db_final_yield"
###### Add 18-07 ######
model_db = "model_workcenter_db_hungary"
curing_collection = "curing_modelno"
tbm_collection = "tbm_gtcodes"
ufm_collection = "ufm_modelno"
tbm_collection_machine = "tbm_machine"
curing_collection_machine = "curing_machine"
ufm_collection_machine = "ufm_machine"
model_no_status_collection = "model_no_status"
###### Add 18-07 ######
alert_table = "alert_table"
summary_db = "summary_db"
alert_deviated = "alert_deviated"

machine_no_collect = "machine_no"
ub_machine_no_collect= "ub_machine_no"
curing_press_dict_collect = "curing_press_dict"
re_codes = "re_codes"

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

# OE Yield home merged data
TUB_CIMCORP_VISUAL_MERGED_DB = "TUB_CIMCORP_VISUAL_merged_cleaned_data_home"
DATA_MERGED_SUMMARY_COLLECTION = "data_merged_summary"
DATA_MERGED_SCRAP_COLLECTION = "data_merged_scrap"

# OE Yield historical merged data
TUB_CIMCORP_VISUAL_MERGED_DB_hisrory = "TUB_CIMCORP_VISUAL_merged_cleaned_data_historical_data"
DATA_MERGED_SUMMARY_COLLECTION_history = "data_merged_summary_historical_data"
DATA_MERGED_SCRAP_COLLECTION_history = "data_merged_scrap_historical_data"
DATA_MERGED_TRACKING_COLLECTION_history = "data_merged_tracking_historical_data"
DATA_MERGED_TRACKING_TBM_COLLECTION_history = "data_merged_tracking_tbm"

# OE Yield alerts merged data
TUB_CIMCORP_VISUAL_MERGED_DB_alert = "TUB_CIMCORP_VISUAL_merged_cleaned_data_alerts_data"
DATA_MERGED_SUMMARY_COLLECTION_alert = "data_merged_summary_alerts_data"
DATA_MERGED_SCRAP_COLLECTION_alert = "data_merged_scrap_alerts_data"
DATA_MERGED_SUMMARY_COLLECTION_alert_new = "new_data_merged_summary_alerts_data"
DATA_MERGED_SCRAP_COLLECTION_alert_new = "new_data_merged_scrap_alerts_data"

DAILY_YIELD_HISTORY = "daily_yield_historical_data"
LATEST_YIELD = "latest_yield"


CIMCORP_DB_visual = "CIMCORP_VISUAL"
Data_Clean_CIMCORP_VISUAL = "Data_Clean_CIMCORP_VISUAL"
CIMCORP_CLEANED_PREFIX_visual = "data_Cleaned_CIMCORP_VISUAL"

visual_data = "visual_data"
inspection_data = "inspection_data"

All_SLOBS_DATA = "All_SLOBS_DATA"
active_sku_list = "active_sku_list"