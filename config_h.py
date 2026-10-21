"""
@author: Automaton AI Infosystem Pvt. Ltd.(https://automatonai.com/)
"""

# # # # # //////////////////////////////////////////////////////////////////////////////////////////
# # # # # //////////////////////////////////////////////////////////////////////////////////////////
# from flask import Flask
# from sshtunnel import SSHTunnelForwarder
# from pymongo import MongoClient
# import paramiko
# import socket

# paramiko.DSSKey = None  # fix for recent paramiko versions

# app = Flask(__name__)

# # -----------------------
# # SSH & MongoDB Settings

# # SSH_HOST = "10.100.192.123"
# SSH_HOST = "10.100.192.93"

# SSH_PORT = 22
# SSH_USER = "ec2-user"

# # SSH_PKEY = "C:\\Users\\Rutwik Sonar\\.ssh\\OE-YIELD-PEM-KEY.pem"
# SSH_PKEY = "/home/avantikalambe/Desktop/Apolo/OE-YIELD-PEM-KEY.pem"

# MONGO_HOST = "localhost"
# MONGO_PORT = 27017

# # -----------------------
# # Find a free local port to avoid conflicts

# def get_free_port():
#     s = socket.socket()
#     s.bind(("", 0))
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
#     local_bind_address=("localhost", LOCAL_PORT),
# )

# tunnel.start()
# print(f"SSH tunnel established on local port {LOCAL_PORT}")

# # -----------------------
# # Connect PyMongo through the tunnel (Authentication Disabled)

# MONGO_URI = f"mongodb://localhost:{LOCAL_PORT}/"
# client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

from pymongo import MongoClient
# --- MongoDB URI ---

# connect to your local instance
MONGO_URI = "mongodb://10.100.192.93:27017/"
client = MongoClient(MONGO_URI,serverSelectionTimeoutMS=5000)


TUB_CIMCORP_VISUAL_merged_cleaned_data_home=client["TUB_CIMCORP_VISUAL_merged_cleaned_data_home"]
data_merged_summary = TUB_CIMCORP_VISUAL_merged_cleaned_data_home["data_merged_summary"]
data_merged_scrap   = TUB_CIMCORP_VISUAL_merged_cleaned_data_home["data_merged_scrap"]

historical_db = client["TUB_CIMCORP_VISUAL_merged_cleaned_data_historical_data"]
data_merged_summary_historical = historical_db["data_merged_summary_historical_data"]

production_db_hungary = client["production_db_hungary"]
channel_no_curing = production_db_hungary["channel_no_curing"]
channel_no_tbm    = production_db_hungary["channel_no_tbm"]
channel_no_ufm    = production_db_hungary["channel_no_ufm"]
re_codes          = production_db_hungary["re_codes"]

workcenter_db = client["model_workcenter_db_hungary"]
model_no_status = workcenter_db["model_no_status"]
sku_audit_log   = workcenter_db["sku_audit_log"]

home_historical_hungary = client["home_historical_hungary"]
home_data = home_historical_hungary["home_data"]
oe_yield_daily   = home_historical_hungary["oe_yield_daily"]
oe_yield_monthly = home_historical_hungary["oe_yield_monthly"]

TUB_CIMCORP_VISUAL_merged_cleaned_data_historical_data = client["TUB_CIMCORP_VISUAL_merged_cleaned_data_historical_data"]
data_merged_summary_historical_data = TUB_CIMCORP_VISUAL_merged_cleaned_data_historical_data["data_merged_summary_historical_data"]
data_merged_scrap_historical_data   = TUB_CIMCORP_VISUAL_merged_cleaned_data_historical_data["data_merged_scrap_historical_data"]
data_merged_tracking_historical_data = TUB_CIMCORP_VISUAL_merged_cleaned_data_historical_data["data_merged_tracking_historical_data"]   

# ── Master DB (user management) ───────────────────────────────────────────────
master_db = client["Master"]
users_col = master_db["Users"]

# ── Machine DB ────────────────────────────────────────────────────────────────
machine_db          = client["model_workcenter_db_hungary"]
curing_machine      = machine_db["curing_machine"]
tbm_machine         = machine_db["tbm_machine"]
ufm_machine         = machine_db["ufm_machine"]
machine_audit_log   = machine_db["machine_audit_log"]
model_no_status     = machine_db["model_no_status"]
tbm_gtcodes         = machine_db['tbm_gtcodes']
ufm_modelno         = machine_db['ufm_modelno']
curing_modelno      = machine_db['curing_modelno']

DOE = client["DOE"]
balance_spec_hungary = DOE["balance_spec_hungary"]

config_date_hungary               = client['config_date_hungary']
deployment_collection   = config_date_hungary['deployment_date']

alert_db_final_yield  = client['alert_db_final_yield']
tbm_col             = alert_db_final_yield["alert_triggering_tbm_with_lot_size"]
ufm_col             = alert_db_final_yield["alert_triggering_ufm_with_lot_size"]
curing_col          = alert_db_final_yield["alert_triggering_curing_with_lot_size"]

alert_deviated_final_yield_db           = client["alert_deviated_final_yield"]
alert_ufm_with_lot_size     = alert_deviated_final_yield_db["alert_ufm_with_lot_size"]
alert_tbm_with_lot_size     = alert_deviated_final_yield_db["alert_tbm_with_lot_size"]
alert_curing_with_lot_size  = alert_deviated_final_yield_db["alert_curing_with_lot_size"]

dashboard_cdc_db            = client['dashboard_cdc_hungary']
dashboard_status_logs       = dashboard_cdc_db['dashboard_status_logs']
dashboard_model             = dashboard_cdc_db['dashboard_model']

login_db = client["alert_mail_db"]
alert_users = login_db["alert_users"]

user_db_hungary                 = client["user_db_hungary"]  # New database for user assignments
alert_assign_user_col   = user_db_hungary["alert_assign_user"]  # New collection for user assignments
