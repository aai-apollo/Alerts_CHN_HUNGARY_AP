from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from datetime import datetime, timezone,time as datetime_time
from datetime import datetime, date  # Ensure both datetime and date are imported
import traceback
import numpy as np
import pymongo
import time
import pytz
import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta
from pytz import timezone
import re
import logging
from datetime import datetime
import os
from passlib.context import CryptContext
import csv
import warnings
import bcrypt
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
import redshift_connector
from config import (
    client,
    FILE_TRACKING_DB,
    FILE_TRACKING_COLLECTION,
    TUB_DB,
    CIMCORP_DB,
    Data_Clean_CIMCORP,
    Data_Clean_TUB,
    TUB_CLEANED_PREFIX,
    CIMCORP_CLEANED_PREFIX,
    TUB_INVALID_PREFIX,
    CIMCORP_INVALID_PREFIX,
    COMBINED_Database,
    ALL_COMBINED_TUB_COLLECTION,
    ALL_COMBINED_CIMCORP_COLLECTION,
    INVALID_ROWS_DB,
    TUB_CIMCORP_DB,
    all_combined_tub_data,
    all_combined_cimcorp_data,
    # COMBINED_COLLECTION,
    inner_join_collection_name,
    tub_collection_name,
    cimcorp_collection_name,
    config_databse,
    channel_no_collect,
    ub_machine_no_collect,
    lot_db,
    alert_db,
    alert_db_dashboard,
    alert_db_virgin_yield,
    alert_db_final_yield,
    alert_deviated,
    alert_table,
    summary_db,
    lot_table_collect,
    machine_no_collect,
    curing_press_dict_collect,
    average_db,
    lot_tbm_collect_name,
    alert_tbm_collect_name,
    alert_ufm_collect_name,
    alert_triggering_deviated_ufm,
    alert_curing_collect_name,
    deviation_occurred_collect_name,
    deviation_occurred_param_collect_name,

    averages_tbm_table,
    alert_triggering_deviated_tbm,
    averages_ufm_table,
    alert_triggering_collect_name,
    summary_triggering_collect_name_tbm,
    lot_curing_collect_name,
    alert_curing_collect_name,
    averages_curing_table_name,
    alert_triggering_curing_collect_name,
    channel_no_collect_tbm,
    channel_no_collect_curing,
    alert_triggering_deviated_curing,
    alert_triggering_ubno_collect_name,
       
    # tbm new without lot size
    new_inner_join_collection_collection,
    lot_tbm_collect_name_new,
    averages_tbm_table_new,
    alert_tbm_collect_name_new,
    alert_triggering_collect_name_new,
    alert_triggering_deviated_tbm_new,

    # curing new without lot size
    new_inner_join_collection_collection,
    lot_curing_collect_name_new,
    alert_curing_collect_name_new,
    averages_curing_table_name_new,
    alert_triggering_curing_collect_name_new,
    alert_triggering_deviated_curing_new,

   # ufm new without lot size
    lot_ufm_collect_name_new,
    alert_ufm_collect_name_new,
    averages_ufm_table_new,
    alert_ufm_collect_name_new,
    alert_triggering_ubno_collect_name_new,
    alert_triggering_deviated_ufm_new,
    new_tub_collection_collection,
    alert_mail_db, # Add 05-07
    admin_table,   # Add 05-07
    alert_users,   # Add 05-08
######### Add 18-07 ###########
    model_db,
    curing_collection,
    tbm_collection,
    ufm_collection,
    tbm_collection_machine,
    curing_collection_machine,
    ufm_collection_machine
######### Add 18-07 ###########

)

# # =============================================================================
# # NO-SAVE PATCH — writes to these DBs are silently suppressed.
# # Reads return empty results so all logic continues unchanged.
# # =============================================================================
# _BLOCKED_DBS = {alert_db, alert_db_dashboard, alert_deviated, alert_table, lot_db, average_db, CIMCORP_DB, TUB_DB, Data_Clean_CIMCORP, Data_Clean_TUB}

# class _NoOpCollection:
#     """Drop-in replacement for a PyMongo collection that discards all writes."""
#     def insert_one(self, *a, **kw):        return type('R', (), {'inserted_id': None})()
#     def insert_many(self, *a, **kw):       return type('R', (), {'inserted_ids': []})()
#     def bulk_write(self, *a, **kw):        return type('R', (), {'inserted_count': 0, 'modified_count': 0, 'upserted_count': 0})()
#     def update_one(self, *a, **kw):        return type('R', (), {'modified_count': 0})()
#     def update_many(self, *a, **kw):       return type('R', (), {'modified_count': 0})()
#     def delete_one(self, *a, **kw):        return type('R', (), {'deleted_count': 0})()
#     def delete_many(self, *a, **kw):       return type('R', (), {'deleted_count': 0})()
#     def find_one(self, *a, **kw):          return None
#     def find(self, *a, **kw):              return iter([])
#     def aggregate(self, *a, **kw):         return iter([])
#     def distinct(self, *a, **kw):          return []
#     def count_documents(self, *a, **kw):   return 0
#     def create_index(self, *a, **kw):      return None
#     def __getattr__(self, name):           return lambda *a, **kw: None

# class _NoOpDB:
#     """Drop-in replacement for a PyMongo database that returns NoOpCollections."""
#     def __getitem__(self, name):           return _NoOpCollection()
#     def __getattr__(self, name):           return _NoOpCollection()

# _original_getitem = client.__class__.__getitem__

# def _patched_getitem(self, db_name):
#     if db_name in _BLOCKED_DBS:
#         return _NoOpDB()
#     return _original_getitem(self, db_name)

# client.__class__.__getitem__ = _patched_getitem
# # =============================================================================

app = Flask(__name__)
app.secret_key = "supersecretkey"


def configure_logging():
    # Create logs directory if it doesn't exist
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    # Create a log filename with current date
    current_date = datetime.now().strftime('%Y-%m-%d')
    log_filename = f'logs/log_alert_{current_date}.txt'
    
    # Clear existing handlers (to prevent duplicates or console fallback)
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    # Configure logging to file ONLY
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, mode='a', encoding='utf-8')
        ]
    )
     # Redirect warnings to the log file as well
    warnings.simplefilter("always")
    logging.captureWarnings(True)

# Call this at the beginning of your script           
# Define yield calculation functions
def yield_percentage(numerator, denominator):
    return (numerator / denominator * 100) if denominator > 0 else 0

################################ Change all the yield logic format ### Add 05-07 start #####################

def total_rank_yield(batch_data):   # Add 05-07   
    try:
        if 'total_rank' not in batch_data.columns or 'barcode' not in batch_data.columns:
            return 0.0
        denominator_data = batch_data[(batch_data['total_rank'] > 0) & (batch_data['total_rank'] <= 5)]
        denominator = len(denominator_data['barcode'])
        if denominator == 0:
            return 0.0
        numerator_data = denominator_data[denominator_data['total_rank'].isin([1, 2])]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

# def rfv_yield(batch_data):   # Add 05-07
#     try:
#         denominator_data = batch_data[((batch_data['cwrfv_oa_rank'] > 0) & (batch_data['cwrfv_oa_rank'] <= 5)) &
#                                       ((batch_data['ccwrfv_oa_rank'] > 0) & (batch_data['ccwrfv_oa_rank'] <= 5)) &
#                                         batch_data['barcode'].notnull()]
#         denominator = len(denominator_data['barcode'])
#         numerator_data = denominator_data[
#             (denominator_data['cwrfv_oa_rank'].isin([1, 2])) &
#             (denominator_data['ccwrfv_oa_rank'].isin([1, 2])) &
#             denominator_data['barcode'].notnull()
#         ]
#         numerator = len(numerator_data['barcode'])
#         return yield_percentage(numerator, denominator)
#     except:
#         return 0.0

def rfv_yield(batch_data):
    try:
        # -----------------------------
        # 1. Denominator (same as before)
        # -----------------------------
        denominator_data = batch_data[
            ((batch_data['cwrfv_oa_rank'] > 0) & (batch_data['cwrfv_oa_rank'] <= 5)) &
            ((batch_data['ccwrfv_oa_rank'] > 0) & (batch_data['ccwrfv_oa_rank'] <= 5)) &
            batch_data['barcode'].notnull()
        ]
        denominator = len(denominator_data)
 
        # ---------------------------------------
        # 2. Numerator for cwrfv (rank 1 or 2)
        # ---------------------------------------
        numerator_cwrfv = len(
            denominator_data[
                (denominator_data['cwrfv_oa_rank'].isin([1, 2])) &
                denominator_data['barcode'].notnull()
            ]
        )
 
        # ---------------------------------------
        # 3. Numerator for ccwrfv (rank 1 or 2)
        # ---------------------------------------
        numerator_ccwrfv = len(
            denominator_data[
                (denominator_data['ccwrfv_oa_rank'].isin([1, 2])) &
                denominator_data['barcode'].notnull()
            ]
        )
 
        # ---------------------------------------
        # 4. Pick MAX numerator between the two
        # ---------------------------------------
        numerator = max(numerator_cwrfv, numerator_ccwrfv)
 
        # ---------------------------------------
        # 5. Final yield percentage
        # ---------------------------------------
        return yield_percentage(numerator, denominator)
 
    except:
        return 0.0

# def rfh1_yield(batch_data):  # Add 05-07
#     try:
#         denominator_data = batch_data[((batch_data['cwrfv_oa1h_rank'] > 0) & (batch_data['cwrfv_oa1h_rank'] <= 5)) &
#                                       ((batch_data['ccwrfv_oa1h_rank'] > 0) & (batch_data['ccwrfv_oa1h_rank'] <= 5)) &
#                                         batch_data['barcode'].notnull()]
#         denominator = len(denominator_data['barcode'])
#         numerator_data = denominator_data[
#             (denominator_data['cwrfv_oa1h_rank'].isin([1, 2])) &
#             (denominator_data['ccwrfv_oa1h_rank'].isin([1, 2])) &
#             denominator_data['barcode'].notnull()
#         ]
#         numerator = len(numerator_data['barcode'])
#         return yield_percentage(numerator, denominator)
#     except:
#         return 0.0

def rfh1_yield(batch_data):
    try:
        # -----------------------------
        # 1. Denominator (same as before)
        # -----------------------------
        denominator_data = batch_data[
            ((batch_data['cwrfv_oa1h_rank'] > 0) & (batch_data['cwrfv_oa1h_rank'] <= 5)) &
            ((batch_data['ccwrfv_oa1h_rank'] > 0) & (batch_data['ccwrfv_oa1h_rank'] <= 5)) &
            batch_data['barcode'].notnull()
        ]
        denominator = len(denominator_data)
 
        # ---------------------------------------
        # 2. Numerator for cwrfv (rank 1 or 2)
        # ---------------------------------------
        numerator_cwrfv = len(
            denominator_data[
                (denominator_data['cwrfv_oa1h_rank'].isin([1, 2])) &
                denominator_data['barcode'].notnull()
            ]
        )
 
        # ---------------------------------------
        # 3. Numerator for ccwrfv (rank 1 or 2)
        # ---------------------------------------
        numerator_ccwrfv = len(
            denominator_data[
                (denominator_data['ccwrfv_oa1h_rank'].isin([1, 2])) &
                denominator_data['barcode'].notnull()
            ]
        )
 
        # ---------------------------------------
        # 4. Pick MAX numerator between the two
        # ---------------------------------------
        numerator = max(numerator_cwrfv, numerator_ccwrfv)
 
        # ---------------------------------------
        # 5. Final yield percentage
        # ---------------------------------------
        return yield_percentage(numerator, denominator)
 
    except:
        return 0.0

# def lfv_yield(batch_data):    # Add 05-07
#     try:
#         denominator_data = batch_data[((batch_data['lfv_oa_rank'] > 0) & (batch_data['lfv_oa_rank'] <= 5)) &
#                                       ((batch_data['lfv_oa_1_rank'] > 0) & (batch_data['lfv_oa_1_rank'] <= 5)) &
#                                         batch_data['barcode'].notnull()]
#         denominator = len(denominator_data['barcode'])
#         numerator_data = denominator_data[
#             (denominator_data['lfv_oa_rank'].isin([1, 2])) &
#             (denominator_data['lfv_oa_1_rank'].isin([1, 2])) &
#             denominator_data['barcode'].notnull()
#         ]
#         numerator = len(numerator_data['barcode'])
#         return yield_percentage(numerator, denominator)
#     except:
#         return 0.0

def lfv_yield(batch_data):
    try:
        # -----------------------------
        # 1. Denominator (same as before)
        # -----------------------------
        denominator_data = batch_data[
            ((batch_data['lfv_oa_rank'] > 0) & (batch_data['lfv_oa_rank'] <= 5)) &
            ((batch_data['lfv_oa_1_rank'] > 0) & (batch_data['lfv_oa_1_rank'] <= 5)) &
            batch_data['barcode'].notnull()
        ]
        denominator = len(denominator_data)
 
        # ---------------------------------------
        # 2. Numerator for cwrfv (rank 1 or 2)
        # ---------------------------------------
        numerator_cwrfv = len(
            denominator_data[
                (denominator_data['lfv_oa_rank'].isin([1, 2])) &
                denominator_data['barcode'].notnull()
            ]
        )
 
        # ---------------------------------------
        # 3. Numerator for ccwrfv (rank 1 or 2)
        # ---------------------------------------
        numerator_ccwrfv = len(
            denominator_data[
                (denominator_data['lfv_oa_1_rank'].isin([1, 2])) &
                denominator_data['barcode'].notnull()
            ]
        )
 
        # ---------------------------------------
        # 4. Pick MAX numerator between the two
        # ---------------------------------------
        numerator = max(numerator_cwrfv, numerator_ccwrfv)
 
        # ---------------------------------------
        # 5. Final yield percentage
        # ---------------------------------------
        return yield_percentage(numerator, denominator)
 
    except:
        return 0.0

def conicity_yield(batch_data):        # Add 05-07
    try:
        denominator_data = batch_data[(batch_data['con_rank'] > 0) & (batch_data['con_rank'] <= 5) & 
                                      batch_data['barcode'].notnull()]
        denominator = len(denominator_data['barcode'])
        numerator_data = denominator_data[
            (denominator_data['con_rank'].isin([1, 2])) &
            denominator_data['barcode'].notnull()
        ]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def ufm_yield(batch_data):         # Add 05-07
    try:
        denominator_data = batch_data[(batch_data['ufm_rank'] > 0) & (batch_data['ufm_rank'] <= 5) & 
                                      batch_data['barcode'].notnull()]
        denominator = len(denominator_data['barcode'])
        numerator_data = denominator_data[
            (denominator_data['ufm_rank'].isin([1, 2])) &
            denominator_data['barcode'].notnull()
        ]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def radial_runout_yield(batch_data):       # Add 05-07
    try:
        denominator_data = batch_data[(batch_data['rc_oa_rank'] > 0) & (batch_data['rc_oa_rank'] <= 5) & 
                                      batch_data['barcode'].notnull()]
        denominator = len(denominator_data['barcode'])
        numerator_data = denominator_data[denominator_data['rc_oa_rank'].isin([1, 2])]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def radial_runout_first_harmonic_yield(batch_data):      # Add 05-07
    try:
        denominator_data = batch_data[(batch_data['rc_oa1h_rank'] > 0) & (batch_data['rc_oa1h_rank'] <= 5) & 
                                      batch_data['barcode'].notnull()]
        denominator = len(denominator_data['barcode'])
        numerator_data = denominator_data[denominator_data['rc_oa1h_rank'].isin([1, 2])]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def rorank_yield(batch_data):        # Add 05-07
    try:
        denominator_data = batch_data[(batch_data['rorank'] > 0) & (batch_data['rorank'] <= 5) & 
                                      batch_data['barcode'].notnull()]
        denominator = len(denominator_data['barcode'])
        numerator_data = denominator_data[
            (denominator_data['rorank'].isin([1, 2])) &
            denominator_data['barcode'].notnull()
        ]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def lateral_run_out_top_over(batch_data):        # Add 05-07
    try:
        denominator_data = batch_data[(batch_data['lt_oa_rank'] > 0) & (batch_data['lt_oa_rank'] <= 5) & 
                                      batch_data['barcode'].notnull()]
        denominator = len(denominator_data['barcode'])
        numerator_data = denominator_data[denominator_data['lt_oa_rank'].isin([1, 2])]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def lateral_run_out_bottom_over(batch_data):      # Add 05-07
    try:
        denominator_data = batch_data[(batch_data['lb_oa_rank'] > 0) & (batch_data['lb_oa_rank'] <= 5) & 
                                      batch_data['barcode'].notnull()]
        denominator = len(denominator_data['barcode'])
        numerator_data = denominator_data[denominator_data['lb_oa_rank'].isin([1, 2])]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def lateral_run_out_yield(batch_data):     # Add 05-07
    try:
        denominator_data = batch_data[(batch_data['lateral_run_out_rank'] > 0) & (batch_data['lateral_run_out_rank'] <= 5) & 
                                      batch_data['barcode'].notnull()]
        denominator = len(denominator_data['barcode'])
        numerator_data = denominator_data[denominator_data['lateral_run_out_rank'].isin([1, 2])]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def lateral_run_out_bottom_first_harmonic(batch_data):  # Add 05-07
    try:
        denominator_data = batch_data[(batch_data['lb_oa1h_rank'] > 0) & (batch_data['lb_oa1h_rank'] <= 5) & 
                                      batch_data['barcode'].notnull()]
        denominator = len(denominator_data['barcode'])
        numerator_data = denominator_data[denominator_data['lb_oa1h_rank'].isin([1, 2])]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def lateral_run_out_top_first_harmonic(batch_data):   # Add 05-07
    try:
        denominator_data = batch_data[(batch_data['lt_oa1h_rank'] > 0) & (batch_data['lt_oa1h_rank'] <= 5) & 
                                      batch_data['barcode'].notnull()]
        denominator = len(denominator_data['barcode'])
        numerator_data = denominator_data[denominator_data['lt_oa1h_rank'].isin([1, 2])]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def lateral_run_out_harmonic_yield(batch_data):    # Add 05-07
    try:
        denominator_data = batch_data[(batch_data['lateral_run_out_harmonic_rank'] > 0) & (batch_data['lateral_run_out_harmonic_rank'] <= 5) & 
                                      batch_data['barcode'].notnull()]
        denominator = len(denominator_data['barcode'])
        numerator_data = denominator_data[denominator_data['lateral_run_out_harmonic_rank'].isin([1, 2])]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def bulge_top_over(batch_data):      # Add 05-07
    try:
        denominator_data = batch_data[(batch_data['ltbulg_rank'] > 0) & (batch_data['ltbulg_rank'] <= 5) & 
                                      batch_data['barcode'].notnull()]
        denominator = len(denominator_data['barcode'])
        numerator_data = denominator_data[denominator_data['ltbulg_rank'].isin([1, 2])]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def bulge_bottom_over(batch_data):        # Add 05-07
    try:
        denominator_data = batch_data[(batch_data['lbbulg_rank'] > 0) & (batch_data['lbbulg_rank'] <= 5) & 
                                      batch_data['barcode'].notnull()]
        denominator = len(denominator_data['barcode'])
        numerator_data = denominator_data[denominator_data['lbbulg_rank'].isin([1, 2])]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def bulge_yield(batch_data):      # Add 05-07
    try:
        denominator_data = batch_data[(batch_data['bulge_rank'] > 0) & (batch_data['bulge_rank'] <= 5) & 
                                      batch_data['barcode'].notnull()]
        denominator = len(denominator_data['barcode'])
        numerator_data = denominator_data[denominator_data['bulge_rank'].isin([1, 2])]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def dent_top_over(batch_data):     # Add 05-07
    try:
        denominator_data = batch_data[(batch_data['dent_rank'] > 0) & (batch_data['dent_rank'] <= 5) & 
                                      batch_data['barcode'].notnull()]
        denominator = len(denominator_data['barcode'])
        numerator_data = denominator_data[denominator_data['dent_rank'].isin([1, 2])]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def dent_bottom_over(batch_data):        # Add 05-07
    try:
        denominator_data = batch_data[(batch_data['dent1_rank'] > 0) & (batch_data['dent1_rank'] <= 5) & 
                                      batch_data['barcode'].notnull()]
        denominator = len(denominator_data['barcode'])
        numerator_data = denominator_data[denominator_data['dent1_rank'].isin([1, 2])]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def dent_yield(batch_data):   # Add 05-07
    try:
        denominator_data = batch_data[(batch_data['dent_overall_rank'] > 0) & (batch_data['dent_overall_rank'] <= 5) & 
                                      batch_data['barcode'].notnull()]
        denominator = len(denominator_data['barcode'])
        numerator_data = denominator_data[denominator_data['dent_overall_rank'].isin([1, 2])]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def balancing_yield(batch_data):     # Add 05-07
    try:
        denominator_data = batch_data[(batch_data['bal_rank'] > 0) & (batch_data['bal_rank'] <= 5) & 
                                      batch_data['barcode'].notnull()]
        denominator = len(denominator_data['barcode'])
        numerator_data = denominator_data[denominator_data['bal_rank'].isin([1, 2])]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def upper_yield(batch_data):    # Add 05-07
    try:
        denominator_data = batch_data[(batch_data['upper_rank'] > 0) & (batch_data['upper_rank'] <= 5) & 
                                      batch_data['barcode'].notnull()]
        denominator = len(denominator_data['barcode'])
        numerator_data = denominator_data[denominator_data['upper_rank'].isin([1, 2])]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def lower_yield(batch_data):    # Add 05-07
    try:
        denominator_data = batch_data[(batch_data['lower_rank'] > 0) & (batch_data['lower_rank'] <= 5) & 
                                      batch_data['barcode'].notnull()]
        denominator = len(denominator_data['barcode'])
        numerator_data = denominator_data[denominator_data['lower_rank'].isin([1, 2])]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def upper_lower_yield(batch_data):    # Add 05-07
    try:
        denominator_data = batch_data[(batch_data['up_low_rank'] > 0) & (batch_data['up_low_rank'] <= 5) & 
                                      batch_data['barcode'].notnull()]
        denominator = len(denominator_data['barcode'])
        numerator_data = denominator_data[denominator_data['up_low_rank'].isin([1, 2])]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def static_yield(batch_data):    # Add 05-07
    try:
        denominator_data = batch_data[(batch_data['static_rank'] > 0) & (batch_data['static_rank'] <= 5) & 
                                      batch_data['barcode'].notnull()]
        denominator = len(denominator_data['barcode'])
        numerator_data = denominator_data[denominator_data['static_rank'].isin([1, 2])]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

# def warehouse_yield(batch_data):    # Add 05-07
#     try:
#         # Initialize variables
#         numerator = 0
#         denominator = 0
 
#         # Filter for denominator: Cimcorp[Total Tyres]
#         try:
#             # Filter for total_rank between 1 and 5
#             denominator_data = batch_data[(batch_data['total_rank'] > 0) & (batch_data['total_rank'] < 4)]
#             # denominator = denominator_data['barcode'].nunique()
#             denominator = len(denominator_data['barcode'])
#         except Exception as e:
#             raise ValueError(f"Error calculating denominator (total tyres): {e}")
 
#         # Check for uftesting_result column first
#         if 'uftesting_result' in batch_data.columns:
#             try:
#                 oe_data = batch_data[batch_data['uftesting_result'] == 'OE1']
#                 # numerator = oe_data['barcode'].nunique()
#                 numerator = len(oe_data['barcode'])
#             except Exception as e:
#                 raise ValueError(f"Error processing uftesting_result column: {e}")
#         # Fallback to uftesting_result_tub if uftesting_result is not available
#         elif 'uftesting_result_tub' in batch_data.columns:
#             try:
#                 oe_data = batch_data[batch_data['uftesting_result_tub'] == 'OE1']
#                 # numerator = oe_data['barcode'].nunique()
#                 numerator = len(oe_data['barcode'])
#             except Exception as e:
#                 raise ValueError(f"Error processing uftesting_result_tub column: {e}")
#         else:
#             raise ValueError("Neither 'uftesting_result' nor 'uftesting_result_tub' columns are available in the data.")
 
#         # Call the yield_percentage function to get the yield percentage
#         try:
#             return yield_percentage(numerator, denominator)
#         except Exception as e:
#             raise ValueError(f"Error calculating yield percentage: {e}")
 
#     except Exception as e:
#         # Log the error and return None or a default value
#         logging.info("Error in warehouse_yield: %s", e)
#         return None

def warehouse_yield(batch_data):    # Add 05-07
    try:
        # Initialize variables
        numerator = 0
        denominator = 0
 
        # Check for uftesting_result column first
        if 'uftesting_result' in batch_data.columns:
            try:
                oe_data = batch_data[batch_data['uftesting_result'].isin(['OE1', 'Repl'])]
                denominator = len(oe_data['barcode'])
            except Exception as e:
                raise ValueError(f"Error processing uftesting_result column: {e}")
        
        # Fallback to uftesting_result_tub if uftesting_result is not available
        elif 'uftesting_result_tub' in batch_data.columns:
            try:
                oe_data = batch_data[batch_data['uftesting_result_tub'].isin(['OE1', 'Repl'])]
                denominator = len(oe_data['barcode'])
            except Exception as e:
                raise ValueError(f"Error processing uftesting_result column: {e}")

        else:
            raise ValueError("Neither 'uftesting_result' nor 'uftesting_result_tub' columns are available in the data.")
 
        # Check for uftesting_result column first
        if 'uftesting_result' in batch_data.columns:
            try:
                oe_data = batch_data[batch_data['uftesting_result'] == 'OE1']
                # numerator = oe_data['barcode'].nunique()
                numerator = len(oe_data['barcode'])
            except Exception as e:
                raise ValueError(f"Error processing uftesting_result column: {e}")
        # Fallback to uftesting_result_tub if uftesting_result is not available
        elif 'uftesting_result_tub' in batch_data.columns:
            try:
                oe_data = batch_data[batch_data['uftesting_result_tub'] == 'OE1']
                # numerator = oe_data['barcode'].nunique()
                numerator = len(oe_data['barcode'])
            except Exception as e:
                raise ValueError(f"Error processing uftesting_result_tub column: {e}")
        else:
            raise ValueError("Neither 'uftesting_result' nor 'uftesting_result_tub' columns are available in the data.")
 
        # Call the yield_percentage function to get the yield percentage
        try:
            return yield_percentage(numerator, denominator)
        except Exception as e:
            raise ValueError(f"Error calculating yield percentage: {e}")
 
    except Exception as e:
        # Log the error and return None or a default value
        logging.info("Error in warehouse_yield: %s", e)
        return None

# def visual_yield(batch_data):
#     try:
#         # Filter for total_rank between 1 and 5
#         denominator_data = batch_data[(batch_data['total_rank'] > 0) & (batch_data['total_rank'] <= 5)]
#         denominator = len(denominator_data['barcode'])

#         # Check for available column: operation or operation_tub
#         if 'operation' in batch_data.columns:
#             numerator_data = denominator_data[
#                 (denominator_data['operation'].notna()) &
#                 (~denominator_data['operation'].isin(['Unknown', 'None']))
#             ]
#         elif 'operation_tub' in batch_data.columns:
#             numerator_data = denominator_data[
#                 (denominator_data['operation_tub'].notna()) &
#                 (~denominator_data['operation_tub'].isin(['Unknown', 'None']))
#             ]
#         else:
#             return 0.0  # Skip if neither column exists

#         numerator = len(numerator_data['barcode'])

#         # Apply yield formula
#         return (1 - (numerator / denominator)) * 100 if denominator > 0 else 0.0

#     except Exception as e:
#         logging.info("Error in visual_yield: %s", e)
#         return 0.0

def visual_yield(batch_data):
    try:
        # Filter for total_rank between 1 and 5
        if 'operation' in batch_data.columns:
            denominator_data = batch_data[
                (batch_data['operation'].notna()) |
                (batch_data['operation'].isna()) |
                (~batch_data['operation'].isin(['Unknown', 'None']))
            ]

            denominator = len(denominator_data['barcode'])
        elif 'operation_tub' in batch_data.columns:
            denominator_data = batch_data[
                (batch_data['operation_tub'].notna()) |
                (batch_data['operation_tub'].isna()) |
                (~batch_data['operation_tub'].isin(['Unknown', 'None']))
            ]

            denominator = len(denominator_data['barcode'])

        # Check for available column: operation or operation_tub
        if 'operation' in batch_data.columns:
            numerator_data = denominator_data[
                (denominator_data['operation'].notna()) &
                (~denominator_data['operation'].isin(['Unknown', 'None']))
            ]
        elif 'operation_tub' in batch_data.columns:
            numerator_data = denominator_data[
                (denominator_data['operation_tub'].notna()) &
                (~denominator_data['operation_tub'].isin(['Unknown', 'None']))
            ]
        else:
            return 0.0  # Skip if neither column exists

        numerator = len(numerator_data['barcode'])

        # Apply yield formula
        return (1 - (numerator / denominator)) * 100 if denominator > 0 else 0.0

    except Exception as e:
        logging.info("Error in visual_yield: %s", e)
        return 0.0

################################ Change all the yield logic format ### Add 05-07 end #####################

# Initialize MongoDB client
# client = MongoClient(MONGO_URI)
db_tracking = client[FILE_TRACKING_DB]
tracking_collection = db_tracking[FILE_TRACKING_COLLECTION]
 

################ IST Time Zone ######################
# def some_function():
#     india_tz = pytz.timezone('Asia/Kolkata')
#     return datetime.now(india_tz).replace(tzinfo=None)
################ IST Time Zone ######################


################ UTC Time Zone ######################
def some_function():
    india_time = datetime.now(pytz.timezone('Asia/Kolkata'))
    return india_time

logging.info(some_function())
################ UTC Time Zone ######################

# Define your default datetime
DEFAULT_DATETIME = datetime(1900, 1, 1, 12, 30, 0)

DEFAULT_DATE = datetime(1900, 1, 1)

# Access databases
file_tracking_db = client['file_tracking']
cimcorp_db = client['CIMCORP']
tub_db = client['TUB']

# Function to insert into MongoDB with the CIMCORP/TUB_{timestamp} format
# def insert_with_timestamp(df, collection_name_prefix, db_name):
#     # Create a timestamp
#     timestamp = some_function().strftime("%Y%m%d_%H%M%S")

#     # Determine the correct database
#     if db_name == 'CIMCORP':
#         db = cimcorp_db
#     elif db_name == 'TUB':
#         db = tub_db
#     else:
#         raise ValueError("Invalid database name")

#     # Create the collection name using the prefix and timestamp
#     collection_name = f"{collection_name_prefix}_{timestamp}"
#     collection = db[collection_name]

#     # Insert dataframe into MongoDB (assuming df is a pandas DataFrame)
#     collection.insert_many(df.to_dict('records'))

#     # Track the file in the file_tracking collection
#     track_file_in_file_tracking(collection_name_prefix, timestamp, len(df), db_name)

def insert_with_timestamp(df, collection_name_prefix, db_name):
    if not isinstance(df, pd.DataFrame):
        # logging.error(f"Input 'df' is not a pandas DataFrame, got type: {type(df)}")
        raise TypeError("Expected a pandas DataFrame as input")
    
    df = df.copy()
    # logging.info(f"Inserting DataFrame with columns: {list(df.columns)}")
    # logging.info(f"DataFrame dtypes:\n{df.dtypes}")

    # Convert any datetime.date objects to strings
    for col in df.select_dtypes(include=['object']).columns:
        if any(isinstance(x, date) and not isinstance(x, datetime) for x in df[col] if x is not None):
            # logging.info(f"Converting datetime.date to string in column: {col}")
            df[col] = df[col].apply(
                lambda x: x.strftime('%Y-%m-%d') if isinstance(x, date) and not isinstance(x, datetime) and x is not None else x
            )

    timestamp = some_function().strftime("%Y%m%d_%H%M%S")
    if db_name == 'CIMCORP':
        db = cimcorp_db
    elif db_name == 'TUB':
        db = tub_db
    else:
        raise ValueError("Invalid database name")
    
    collection_name = f"{collection_name_prefix}_{timestamp}"
    collection = db[collection_name]
    collection.insert_many(df.to_dict('records'))
    
    track_file_in_file_tracking(collection_name_prefix, timestamp, len(df), db_name)

# Function to insert tracking information into the file_tracking collection
def track_file_in_file_tracking(file_prefix, timestamp, num_rows, db_name):
    # Access file_tracking collection
    file_tracking = file_tracking_db['file_metadata']

    # Create the tracking entry
    tracking_entry = {
        "filename": f"{file_prefix}_{timestamp}",
        "inserted_timestamp": some_function(),
        "num_rows": num_rows,
        "database_name": db_name,
        "processed": False
    }

    # Insert the tracking entry
    file_tracking.insert_one(tracking_entry)

class RedshiftDataConnectorr:
    def __init__(self):
        self.conn = redshift_connector.connect(
            host='atl-dl-prd-01.cuvxusm4qify.ap-south-1.redshift.amazonaws.com',
            database='db_prd_plant_che',
            port=5439,
            user='usr_thangadurai_a',
            password='QtHJO%1^m^dQEVFc', sslmode='disable'
        )
    def execute_query(self, q):
        data = pd.read_sql(q, self.conn)
        return data
    
inst = RedshiftDataConnectorr()

local_tz = pytz.timezone('Asia/Kolkata')
now = datetime.now(pytz.utc).astimezone(local_tz)

def get_prev_hour_range(run_dt):
    # Get start of previous hour in IST
    prev_hour = (run_dt - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    next_hour = prev_hour + timedelta(hours=1)

    query_date = prev_hour.strftime("%Y-%m-%d")
    start_time = prev_hour.strftime("%H:%M:%S")
    end_time = next_hour.strftime("%H:%M:%S")

    return query_date, start_time, end_time

# Get actual query date/time range for the cron run
query_date, start_time, end_time = get_prev_hour_range(now)

############## Manual trigger Multiple dates range start ################
# Define timezone
# local_tz = pytz.timezone('Asia/Kolkata')

# # Get current IST time
# now = datetime.now(pytz.utc).astimezone(local_tz)

# # Example: set custom date range
# start_date = '2026-04-27'
# end_date = '2026-05-01'
# start_time = '07:00:00'
# end_time = '15:35:00'

# # ------------------------------------------------------------------
# # MongoDB: Fetch modelno and gt_code from production_db.channel_no_ufm
# # ------------------------------------------------------------------

# # MONGO_URI = "mongodb://<host>:<port>/"   # <-- replace with actual URI

# # mongo_client = MongoClient(MONGO_URI)
# mongo_db = client["production_db"]
# channel_coll = mongo_db["channel_no_ufm"]

# # -------- Fetch model numbers --------
# modelno_list = channel_coll.distinct("modelno")

# # modelno stored as string → convert to int
# modelno_list = [int(m) for m in modelno_list if str(m).isdigit()]

# if not modelno_list:
#     raise ValueError("No model numbers found in MongoDB")

# modelno_sql = ",".join(map(str, modelno_list))

# print("Model numbers from MongoDB:", modelno_list)

# # # -------- Fetch GT codes --------
# # gt_code_nested = channel_coll.distinct("gt_code")

# # # Flatten + deduplicate
# # gt_code_list = sorted({gt for sub in gt_code_nested for gt in sub})

# # if not gt_code_list:
# #     raise ValueError("No GT codes found in MongoDB")

# # gt_code_sql = ",".join(f"'{gt}'" for gt in gt_code_list)

# # print("GT codes from MongoDB:", gt_code_list)

# # -------- Fetch GT codes --------
# gt_code_raw = channel_coll.distinct("gt_code")

# gt_code_set = set()

# for item in gt_code_raw:
#     if isinstance(item, list):
#         gt_code_set.update(item)
#     elif isinstance(item, str):
#         gt_code_set.add(item)

# gt_code_list = sorted(gt_code_set)

# gt_code_sql = ",".join(f"'{gt}'" for gt in gt_code_list)

# print("GT codes from MongoDB:", gt_code_list)

# # ------------------------------------------------------------------
# # Query 1: ubtable (modelno filter from MongoDB)
# # ------------------------------------------------------------------

# q1 = f"""
# SELECT *
# FROM sc_tub_pcr.ubtable
# WHERE ("date" > '{start_date}' 
#        OR ("date" = '{start_date}' AND "time" >= '{start_time}'))
# AND ("date" < '{end_date}' 
#      OR ("date" = '{end_date}' AND "time" < '{end_time}'))
# AND modelno IN ({modelno_sql});
# """

# print("######################### QUERY 1 #########################")
# # print(q1)

# df_tub = inst.execute_query(q1)
# # df_tub.to_csv('df_tub.csv', escapechar='\\', quoting=csv.QUOTE_NONE,index=False)

# # df_tub = pd.read_csv(f"df_tub.csv")

# # ------------------------------------------------------------------
# # Query 2: TBM + Curing + UFT + Repair (gt_code filter from MongoDB)
# # ------------------------------------------------------------------

# q2 = f"""
# WITH barcode_data AS (
#     SELECT barcode
#     FROM sc_tub_pcr.ubtable
#     WHERE ("date" > '{start_date}' 
#            OR ("date" = '{start_date}' AND "time" >= '{start_time}'))
#     AND ("date" < '{end_date}' 
#          OR ("date" = '{end_date}' AND "time" < '{end_time}'))
# ),
# transport_units AS (
#     SELECT 
#         b.barcode, 
#         t.transportationunitid
#     FROM barcode_data b
#     JOIN sc_cimcorp_pcr.transportationunit t 
#       ON b.barcode = t.barcode
#     ORDER BY b.barcode, t.transportationunitid
# )
# SELECT 
#     tbm.timestamp AS timestamp_x,
#     tbm.deviceid AS deviceid_x,
#     tu.barcode,
#     curing.timestamp AS timestamp_y,
#     curing.deviceid AS deviceid_y,
#     uft.result AS uftesting_result,
#     tbm.*,
#     curing.*,
#     repair.operation AS operation
# FROM sc_cimcorp_pcr.tbm tbm
# JOIN transport_units tu 
#   ON tbm.transportationunitid = tu.transportationunitid
# JOIN sc_cimcorp_pcr.curing curing 
#   ON tbm.transportationunitid = curing.transportationunitid
# LEFT JOIN sc_cimcorp_pcr.uftesting uft 
#   ON tbm.transportationunitid = uft.transportationunitid
# LEFT JOIN sc_cimcorp_pcr.repair repair 
#   ON tbm.transportationunitid = repair.transportationunitid
# WHERE tbm.gttypeid IN ({gt_code_sql});
# """

# print("######################### QUERY 2 #########################")
# # print(q2)

# df_cimcorp = inst.execute_query(q2)

# ############## Manual triggering end #################

# query_date = '2026-04-16'
# start_time = '07:00:00'
# end_time = '19:00:00'


# query_date = '2025-10-10'
# start_time = '00:00:00'
# end_time = '07:00:00'


# ------------------------------------------------------------------
# MongoDB: Fetch modelno and gt_code from production_db.channel_no_ufm


# MONGO_URI = "mongodb://<host>:<port>/"   # <-- replace with actual URI

# mongo_client = MongoClient(MONGO_URI)
# ------------------------------------------------------------------

mongo_db = client["production_db"]
channel_coll = mongo_db["channel_no_ufm"]

# -------- Fetch model numbers --------
modelno_list = channel_coll.distinct("modelno")

# modelno stored as string → convert to int
modelno_list = [int(m) for m in modelno_list if str(m).isdigit()]

if not modelno_list:
    raise ValueError("No model numbers found in MongoDB")

modelno_sql = ",".join(map(str, modelno_list))

print("Model numbers from MongoDB:", modelno_list)


# -------- Fetch GT codes --------
gt_code_raw = channel_coll.distinct("gt_code")

gt_code_set = set()

for item in gt_code_raw:
    if isinstance(item, list):
        gt_code_set.update(item)
    elif isinstance(item, str):
        gt_code_set.add(item)

gt_code_list = sorted(gt_code_set)

gt_code_sql = ",".join(f"'{gt}'" for gt in gt_code_list)

print("GT codes from MongoDB:", gt_code_list)

# ------------------------------------------------------------------
# Query 1: ubtable (modelno filter from MongoDB)
# ------------------------------------------------------------------

q1 = f"""
SELECT *
FROM sc_tub_pcr.ubtable
WHERE date = '{query_date}'
AND time >= '{start_time}'
AND time < '{end_time}'
AND modelno IN ({modelno_sql});
"""

print("######################### QUERY 1 #########################")
# print(q1)

df_tub = inst.execute_query(q1)

# ------------------------------------------------------------------
# Query 2: TBM + Curing + UFT + Repair (gt_code filter from MongoDB)
# ------------------------------------------------------------------

q2 = f"""
WITH barcode_data AS (
    SELECT barcode
    FROM sc_tub_pcr.ubtable
    WHERE date = '{query_date}'
    AND time >= '{start_time}'
    AND time < '{end_time}'
),
transport_units AS (
    SELECT 
        b.barcode, 
        t.transportationunitid
    FROM barcode_data b
    JOIN sc_cimcorp_pcr.transportationunit t 
      ON b.barcode = t.barcode
    ORDER BY b.barcode, t.transportationunitid
)
SELECT 
    tbm.timestamp AS timestamp_x,
    tbm.deviceid AS deviceid_x,
    tu.barcode,
    curing.timestamp AS timestamp_y,
    curing.deviceid AS deviceid_y,
    uft.result AS uftesting_result,
    tbm.*,
    curing.*,
    repair.operation AS operation
FROM sc_cimcorp_pcr.tbm tbm
JOIN transport_units tu 
  ON tbm.transportationunitid = tu.transportationunitid
JOIN sc_cimcorp_pcr.curing curing 
  ON tbm.transportationunitid = curing.transportationunitid
LEFT JOIN sc_cimcorp_pcr.uftesting uft 
  ON tbm.transportationunitid = uft.transportationunitid
LEFT JOIN sc_cimcorp_pcr.repair repair 
  ON tbm.transportationunitid = repair.transportationunitid
WHERE tbm.gttypeid IN ({gt_code_sql});
"""

print("######################### QUERY 2 #########################")
# print(q2)

df_cimcorp = inst.execute_query(q2)


def sanitize_collection_name(filename):
    name = filename.rsplit(".", 1)[0]
    name = re.sub(r'[\/\.\$]', '_', name)
    return name[:120]

def excel_to_csv(excel_file, filename):
    try:
        df = pd.read_excel(excel_file)
        csv_filepath = os.path.join(app.config['TEMP_CSV_FOLDER'], f"{os.path.splitext(filename)[0]}.csv")
        df.to_csv(csv_filepath, index=False, encoding='utf-8')
        return csv_filepath
    except Exception as e:
        logging.info("Error converting Excel to CSV: %s",e)
        return None

def has_special_chars(value):
    if isinstance(value, str):
        return bool(re.search(r'[^0-9]', value))
    return False

def convert_string_to_date(date_string):
    """Convert a date string to a datetime object."""
    for fmt in date_formats:
        try:
            return datetime.strptime(date_string, fmt)
        except ValueError:
            continue
    return None
 
def get_shift(dt):
    # Check if the input is invalid (string, None, or "No Date")
    if dt is None or dt == "No Date":
        return "No Date"
 
    if isinstance(dt, str):
        try:
            # Use the correct format to handle the timezone information
            dt = pd.to_datetime(dt, errors='coerce')
            if pd.isna(dt):
                return "No Date"
        except Exception as e:
            logging.info("Error parsing date: %s", e)
            return "No Date"
 
    if not isinstance(dt, (datetime, pd.Timestamp)):
        return "No Date"
 
    # Special check for the datetime '1900-01-01 12:30:00'
    if dt == DEFAULT_DATETIME:
        return "No Date"
 
    # Convert dt to naive datetime if it's timezone-aware
    if dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
 
    # ←— APPENDED: treat anything before 07:00 as “next day” so it falls into C_start–C_end
    if dt.hour < 7:
        dt = dt + pd.Timedelta(days=1)
 
    # Define the datetime ranges for shifts A, B, and C based on the format used
    A_start = datetime.strptime(f"{dt.date()} 07:00:00", "%Y-%m-%d %H:%M:%S")
    A_end   = datetime.strptime(f"{dt.date()} 14:59:59", "%Y-%m-%d %H:%M:%S")
    B_start = datetime.strptime(f"{dt.date()} 15:00:00", "%Y-%m-%d %H:%M:%S")
    B_end   = datetime.strptime(f"{dt.date()} 22:59:59", "%Y-%m-%d %H:%M:%S")
    C_start = datetime.strptime(f"{dt.date()} 23:00:00", "%Y-%m-%d %H:%M:%S")
    C_end   = datetime.strptime(f"{(dt + pd.Timedelta(days=1)).date()} 06:59:59", "%Y-%m-%d %H:%M:%S")
 
    # Determine the shift based on the datetime ranges
    if A_start <= dt <= A_end:
        return "A"
    elif B_start <= dt <= B_end:
        return "B"
    elif C_start <= dt <= C_end:
        return "C"
    elif A_start >= dt <= C_end:
        return "C"
    else:
        return "No Shift"

def get_rank_columns(batch_data):
    rank_columns_list = [
        'total_rank', 'cwrfv_oa_rank', 'ccwrfv_oa_rank', 'cwrfv_oa1h_rank',
        'ccwrfv_oa1h_rank', 'lfv_oa_rank', 'lfv_oa_1_rank', 'con_rank',
        'ufm_rank', 'rc_oa_rank', 'rc_oa1h_rank', 'rorank', 'lt_oa_rank',
        'lb_oa_rank', 'lateral_run_out_rank', 'lb_oa1h_rank', 'lt_oa1h_rank',
        'lateral_run_out_harmonic_rank', 'ltbulg_rank', 'lbbulg_rank',
        'bulge_rank', 'dent_rank', 'dent1_rank', 'dent_overall_rank',
        'bal_rank', 'upper_rank', 'lower_rank', 'up_low_rank', 'static_rank'
    ]
    
    # Create a dictionary to store rank values
    rank_values = {}
    for column in rank_columns_list:
        # Assuming batch_data is a DataFrame; adjust if it's a different structure
        if column in batch_data.columns:
            # Get the mean value if it's a DataFrame column, or adjust as needed
            rank_values[column] = float(batch_data[column].mean()) if not batch_data[column].isna().all() else None
        else:
            rank_values[column] = None
    
    return rank_values

def clean_tub_data(df):
    output_messages = []
    invalid_rows = pd.DataFrame()
    logging.info("clean_tub_data function is just started")

    mask = df['barcode'].isnull() | df['barcode'].apply(has_special_chars)
    invalid_rows = df[mask]
    df_clean = df[~mask].drop_duplicates()

    ####################added 04-06################################

    df_clean.drop(columns=['rn', 'Unnamed: 226', 'Unnamed: 225'], inplace=True, errors='ignore')

    df_clean.columns = df_clean.columns.to_series().str.strip().str.lower().str.replace(' ', '_')
    df_clean = df_clean.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Remove trailing zeros and decimal point from 'barcode' and 'modelno'
    # if 'barcode' in df_clean.columns:
    #     df_clean['barcode'] = df_clean['barcode'].astype(str).apply(lambda x: x[:-2] if x.endswith('.0') else x)

    if 'barcode' in df_clean.columns:
        df_clean['barcode'] = (df_clean['barcode'].astype(str).apply(lambda x: x[:-2] if x.endswith('.0') else x).apply(lambda x: x.lstrip('0')))  # Remove leading zeros)


    if 'modelno' in df_clean.columns:
        df_clean['modelno'] = df_clean['modelno'].astype(str).apply(lambda x: x.rstrip('0').rstrip('.'))

    column_types = {
        'primary_ubtable': 'str', 'no': 'float64', 'ubno': 'str', 'shift': 'str', 'cycletime': 'float64',
        'modelno': 'str', 'sizecode': 'str', 'barcode': 'str', 'low_speed1': 'float64', 'spindlerpm1': 'float64',
        'airpress1': 'float64', 'toptemp1': 'float64', 'centertemp1': 'float64', 'bottomtemp1': 'float64',
        'aroundtemp1': 'float64', 'load1': 'float64', 'cwrfv_oa_kgf': 'float64', 'cwrfv_oa_deg': 'float64',
        'cwrfv_oa_rank': 'float64', 'cwrfv_oa1h_kgf': 'float64', 'cwrfv_oa1h_deg': 'float64', 'cwrfv_oa1h_rank': 'float64',
        'cwrfv_oa2h_kgf': 'float64', 'cwrfv_oa2h_deg': 'float64', 'cwrfv_oa2h_rank': 'float64', 'cwrfv_oa3h_kgf': 'float64',
        'cwrfv_oa3h_deg': 'float64', 'cwrfv_oa3h_rank': 'float64', 'cwrfv_oa4h_kgf': 'float64', 'cwrfv_oa4h_deg': 'float64',
        'cwrfv_oa4h_rank': 'float64', 'cwrfv_oa5h_kgf': 'float64', 'cwrfv_oa5h_deg': 'float64', 'cwrfv_oa5h_rank': 'float64',
        'cwrfv_oa6h_kgf': 'float64', 'cwrfv_oa6h_deg': 'float64', 'cwrfv_oa6h_rank': 'float64', 'cwrfv_oa7h_kgf': 'float64',
        'cwrfv_oa7h_deg': 'float64', 'cwrfv_oa7h_rank': 'float64', 'cwrfv_oa8h_kgf': 'float64', 'cwrfv_oa8h_deg': 'float64',
        'cwrfv_oa8h_rank': 'float64', 'cwrfv_oa9h_kgf': 'float64', 'cwrfv_oa9h_deg': 'float64', 'cwrfv_oa9h_rank': 'float64',
        'cwrfv_oa10h_kgf': 'float64', 'cwrfv_oa10h_deg': 'float64', 'cwrfv_oa10h_rank': 'float64', 'lfv_oa_kgf': 'float64',
        'lfv_oa_deg': 'float64', 'lfv_oa_rank': 'float64', 'lfv_oa1h_kgf': 'float64', 'lfv_oa1h_deg': 'float64',
        'lfv_oa1h_rank': 'float64', 'lfv_oa2h_kgf': 'float64', 'lfv_oa2h_deg': 'float64', 'lfv_oa2h_rank': 'float64',
        'lfv_oa3h_kgf': 'float64', 'lfv_oa3h_deg': 'float64', 'lfv_oa3h_rank': 'float64', 'lfv_oa4h_kgf': 'float64',
        'lfv_oa4h_deg': 'float64', 'lfv_oa4h_rank': 'float64', 'lfv_oa5h_kgf': 'float64', 'lfv_oa5h_deg': 'float64',
        'lfv_oa5h_rank': 'float64', 'lfv_oa6h_kgf': 'float64', 'lfv_oa6h_deg': 'float64', 'lfv_oa6h_rank': 'float64',
        'lfv_oa7h_kgf': 'float64', 'lfv_oa7h_deg': 'float64', 'lfv_oa7h_rank': 'float64', 'lfv_oa8h_kgf': 'float64',
        'lfv_oa8h_deg': 'float64', 'lfv_oa8h_rank': 'float64', 'lfv_oa9h_kgf': 'float64', 'lfv_oa9h_deg': 'float64',
        'lfv_oa9h_rank': 'float64', 'lfv_oa10h_kgf': 'float64', 'lfv_oa10h_deg': 'float64', 'lfv_oa10h_rank': 'float64',
        'lfd_kgf': 'float64', 'lfd_rank': 'float64', 'ccwrfv_oa_kgf': 'float64', 'ccwrfv_oa_deg': 'float64',
        'ccwrfv_oa_rank': 'float64', 'ccwrfv_oa1h_kgf': 'float64', 'ccwrfv_oa1h_deg': 'float64', 'ccwrfv_oa1h_rank': 'float64',
        'ccwrfv_oa2h_kgf': 'float64', 'ccwrfv_oa2h_deg': 'float64', 'ccwrfv_oa2h_rank': 'float64', 'ccwrfv_oa3h_kgf': 'float64',
        'ccwrfv_oa3h_deg': 'float64', 'ccwrfv_oa3h_rank': 'float64', 'ccwrfv_oa4h_kgf': 'float64', 'ccwrfv_oa4h_deg': 'float64',
        'ccwrfv_oa4h_rank': 'float64', 'ccwrfv_oa5h_kgf': 'float64', 'ccwrfv_oa5h_deg': 'float64', 'ccwrfv_oa5h_rank': 'float64',
        'bal_speed': 'float64', 'spindlerpm': 'float64', 'airpress': 'float64', 'toptemp': 'float64', 'centertemp': 'float64',
        'bottomtemp': 'float64', 'aroundtemp': 'float64', 'upper_g': 'float64', 'upper_deg': 'float64', 'upper_rank': 'float64',
        'lower_g': 'float64', 'lower_deg': 'float64', 'lower_rank': 'float64', 'static_g': 'float64', 'static_deg': 'float64',
        'static_rank': 'float64', 'couple_g': 'float64', 'couple_deg': 'float64', 'couple_rank': 'float64', 'up_low_g': 'float64',
        'up_low_rank': 'float64', 'bal_rank': 'float64', 'total_rank': 'float64'
    }
    # Add new columns based on the maximum of specified pairs
    df_clean['lateral_run_out_rank'] = df_clean[['lt_oa_rank', 'lb_oa_rank']].max(axis=1)
    df_clean['lateral_run_out_harmonic_rank'] = df_clean[['lt_oa1h_rank', 'lb_oa1h_rank']].max(axis=1)
    df_clean['bulge_rank'] = df_clean[['ltbulg_rank', 'lbbulg_rank']].max(axis=1)
    df_clean['dent_overall_rank'] = df_clean[['dent_rank', 'dent1_rank']].max(axis=1)


    # Helper function to convert string to datetime
    def convert_string_to_date_TUB(date_string, date_formats=["%d-%m-%Y", "%Y-%m-%d"]):
        """Convert a date string to a datetime object."""
        for fmt in date_formats:
            try:
                return datetime.strptime(date_string, fmt)
            except ValueError:
                continue
        return DEFAULT_DATE  # Return default datetime if conversion fails
    if 'date' in df_clean.columns:
            df_clean['ub_date'] = df_clean['date'].apply(lambda x: convert_string_to_date_TUB(x))
            df_clean['ub_date'] = pd.to_datetime(df_clean['ub_date'], errors='coerce')

    if 'date' in df_clean.columns:
        df_clean['date'] = pd.to_datetime(df_clean['date'], errors='coerce').dt.strftime('%Y-%m-%d')


    # Ensure the new columns are included in the column types
    column_types.update({
        'lateral_run_out_rank': 'float64',
        'lateral_run_out_harmonic_rank': 'float64',
        'bulge_rank': 'float64',
        'dent_overall_rank': 'float64'
    })
    column_types = {col: dtype for col, dtype in column_types.items() if col in df_clean.columns}
    df_clean = df_clean.astype(column_types, errors='ignore')

    if 'modelno' in df_clean.columns:
        df_clean['lot_id'] = df_clean['modelno'].astype(str) + '_' + df_clean.groupby('modelno').cumcount().astype(str)
        df_clean['previous_lot_id'] = df_clean['modelno'].astype(str) + '_' + (df_clean.groupby('modelno').cumcount() - 1).astype(str)
        df_clean.loc[df_clean['previous_lot_id'].str.contains('_-1',na=False), 'previous_lot_id'] = None

        # Initialize date_and_time column as None
        df['date_and_time'] = None

        try:
            # Function to parse various date formats for a single value
            def parse_date(date_str):
                if pd.isnull(date_str):
                    return None
                try:
                    return datetime.strptime(str(date_str), '%Y-%m-%d').strftime('%Y-%m-%d')
                except ValueError:
                    try:
                        return datetime.strptime(str(date_str), '%m/%d/%Y').strftime('%Y-%m-%d')
                    except ValueError:
                        try:
                            return datetime.strptime(str(date_str), '%d-%m-%Y').strftime('%Y-%m-%d')
                        except ValueError:
                            return None

            # Function to parse various time formats for a single value
            def parse_time(time_str):
                if pd.isnull(time_str):
                    return None
                try:
                    return datetime.strptime(str(time_str), '%H:%M:%S').strftime('%H:%M:%S')
                except ValueError:
                    try:
                        return datetime.strptime(str(time_str), '%I:%M:%S %p').strftime('%H:%M:%S')
                    except ValueError:
                        try:
                            return datetime.strptime(str(time_str), '%H:%M:%s').strftime('%H:%M:%S')
                        except ValueError:
                            return None

            # Vectorized parsing simulation without apply
            parsed_dates = []
            parsed_times = []
            
            # Loop through each row manually to parse dates and times
            for date_str, time_str in zip(df_clean['date'], df_clean['time']):
                parsed_dates.append(parse_date(date_str))
                parsed_times.append(parse_time(time_str))
            
            # Assign parsed values back to DataFrame
            df_clean['parsed_date'] = parsed_dates
            df_clean['parsed_time'] = parsed_times

            # Define masks for vectorized operations
            mask_both = (df_clean['parsed_date'].notnull()) & (df_clean['parsed_time'].notnull())
            mask_date_only = (df_clean['parsed_date'].notnull()) & (df_clean['parsed_time'].isnull())
            mask_time_only = (df_clean['parsed_date'].isnull()) & (df_clean['parsed_time'].notnull())
            
            # Combine where both are non-null, format as 'YYYY-mm-dd H:M:S'
            df_clean.loc[mask_both, 'date_and_time'] = df_clean.loc[mask_both, 'parsed_date'] + ' ' + df_clean.loc[mask_both, 'parsed_time']
            # Use date where only date is non-null
            df_clean.loc[mask_date_only, 'date_and_time'] = df_clean.loc[mask_date_only, 'parsed_date']
            # Use time where only time is non-null
            df_clean.loc[mask_time_only, 'date_and_time'] = df_clean.loc[mask_time_only, 'parsed_time']
            # Null remains where both are null (already set to None)

            # Drop temporary parsed columns
            df_clean = df_clean.drop(columns=['parsed_date', 'parsed_time'])

        except Exception as e:
            # Handle any errors (e.g., parsing issues, type mismatch)
            print(f"Error occurred while combining or formatting date and time: {e}")
            df_clean['date_and_time'] = None  # Fallback to None in case of error

        print(df_clean['date_and_time'])

        # Add new columns with None (null) values
        df_clean["tbm_machine_model_lot_id"] = None
        df_clean["tbm_previous_machine_model_lot_id"] = None

        df_clean["curing_machine_model_lot_id"] = None
        df_clean["curing_previous_machine_model_lot_id"] = None

        df_clean["ufm_machine_model_lot_id"] = None
        df_clean["ufm_previous_machine_model_lot_id"] = None


        # Add new columns with None (null) values for balancing changes 04-03
        df_clean["tbm_machine_model_balancing_lot_id"] = None
        df_clean["tbm_previous_machine_balancing_model_lot_id"] = None

        df_clean["curing_machine_model_balancing_lot_id"] = None
        df_clean["curing_previous_machine_balancing_model_lot_id"] = None

        df_clean["ufm_machine_model_balancing_lot_id"] = None
        df_clean["ufm_previous_machine_balancing_model_lot_id"] = None

        # Add new columns with None (null) values for yiel drop changes 05-03
        df_clean["tbm_machine_model_lot_id_yield_drop"] = None
        df_clean["tbm_previous_machine_model_lot_id_yield_drop"] = None

        df_clean["curing_machine_model_lot_id_yield_drop"] = None
        df_clean["curing_previous_machine_model_lot_id_yield_drop"] = None

        df_clean["ufm_model_lot_id_yield_drop"] = None
        df_clean["ufm_previous_model_lot_id_yield_drop"] = None

        # Add new columns with None (null) values for load and pressure changes 05-05
        df_clean["tbm_machine_model_lot_id_load_pressure"] = None
        df_clean["tbm_previous_machine_model_lot_id_load_pressure"] = None

        df_clean["curing_machine_model_lot_id_load_pressure"] = None
        df_clean["curing_previous_machine_model_lot_id_load_pressure"] = None

        df_clean["ufm_model_lot_id_load_pressure"] = None
        df_clean["ufm_previous_model_lot_id_load_pressure"] = None

    logging.info("clean_tub_data function is completed")
    return df_clean, invalid_rows, output_messages

def clean_cimcorp_data(df):
    output_messages = []
    invalid_barcode_rows = pd.DataFrame()
    duplicates_df = pd.DataFrame()
    logging.info("clean_cimcorp_data function is just started")

    try:
        df = df.applymap(str)
        df.columns = [re.sub(r'[.\s]+', '_', col.strip()).lower() for col in df.columns]
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        for col in df.select_dtypes(include=['object']):
            df[col] = df[col].astype(str).str.strip()

        # Step 2: Rename columns based on the provided mapping
        rename_columns_mapping = {
            'deviceid_x': 'tbm',
            'timestamp_x': 'tbm_buildtime',
            'skutypeid': 'sku',
            'mouldid': 'curing_mouldid',
            'machinenumber': 'curing_press',
            'timestamp_y': 'curing_infeedtime',
            'gttypeid': 'tbm_gtcode'
        }
        for old_name, new_name in rename_columns_mapping.items():
            if old_name in df.columns:
                df = df.rename(columns={old_name: new_name}, inplace=False)  # inplace=False to reassign df

        # # Convert tbm_buildtime and curing_infeedtime to IST
        # if 'tbm_buildtime' in df.columns:
        #     df['tbm_buildtime'] = (pd.to_datetime(df['tbm_buildtime']) + pd.Timedelta(hours=5, minutes=30)).dt.strftime("%Y-%m-%d %H:%M:%S")

        # if 'curing_infeedtime' in df.columns:
        #     df['curing_infeedtime'] = (pd.to_datetime(df['curing_infeedtime']) + pd.Timedelta(hours=5, minutes=30)).dt.strftime("%Y-%m-%d %H:%M:%S")

        # After renaming, modify the 'tbm' column (add 'TBM' prefix before the existing values)
        if 'tbm' in df.columns:
            df['tbm'] = 'TBM' + df['tbm'].astype(str)

        # After renaming, modify the 'curing_press' column (add 'M' prefix before the existing values)
        if 'curing_press' in df.columns:
            df['curing_press'] = 'M' + df['curing_press'].astype(str)

        # Step 3: Clean Cell Values (remove spaces from string values)
        for col in df.select_dtypes(include=['object']):  # Iterate over string columns only
            df[col] = df[col].astype(str).str.strip()  # Remove leading/trailing spaces from strings

        special_char_regex = re.compile(r'[^0-9]')

        rows_with_null_barcode = df[df['barcode'].isnull()]
        rows_with_special_char_barcode = df[df['barcode'].astype(str).apply(lambda x: bool(special_char_regex.search(x)))]

        invalid_barcode_rows = pd.concat([rows_with_null_barcode, rows_with_special_char_barcode]).drop_duplicates()
        df = df[~df.index.isin(invalid_barcode_rows.index)]

        df = df.dropna(axis=1, how='all')
        df = df.loc[:, ~df.apply(lambda x: x.apply(lambda val: isinstance(val, str) and val.strip() == '').all())]

        duplicates_df = df[df.duplicated(keep=False)]
        df = df.drop_duplicates()

        if 'barcode' in df.columns:
            df['barcode'] = (df['barcode'].astype(str).apply(lambda x: x[:-2] if x.endswith('.0') else x).apply(lambda x: x.lstrip('0')))  # Remove leading zeros)

        ##############added 04-06####################
        # # Define datetime columns
        # datetime_columns = ['tbm_buildtime', 'curing_infeedtime', 'timestamp', 'timestamp_1']

        # # Debugging: Print initial state
        # print("Initial missing values:")
        # for col in datetime_columns:
        #     if col in df.columns:
        #         print(f"{col}: {df[col].isna().sum()} missing, sample values: {df[col].head().tolist()}")

        # # Process each datetime column
        # for col in datetime_columns:
        #     if col in df.columns:
        #         # Step 1: If column is already datetime, skip parsing; otherwise, try multiple formats
        #         if not pd.api.types.is_datetime64_any_dtype(df[col]):
        #             try:
        #                 # Try common datetime formats
        #                 df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
        #             except Exception as e:
        #                 print(f"Error parsing {col}: {e}")
        #                 # Fallback: treat as string and fill first
        #                 df[col] = df[col].ffill().bfill()

        #         # Step 2: Apply ffill and bfill to handle missing values
        #         df[col] = df[col].ffill().bfill()

        #         # Step 3: If still missing (e.g., all values were NaT), use a default datetime
        #         if df[col].isna().any():
        #             print(f"Warning: {col} still has missing values after ffill/bfill, using default")
        #             df[col] = df[col].fillna(pd.Timestamp('2025-01-01 00:00:00'))

        #         # Step 4: Format as MongoDB-compatible string (YYYY-MM-DD HH:MM:SS)
        #         df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')

        # # Debugging: Verify no missing values and check formats
        # # print("\nAfter processing:")
        # for col in datetime_columns:
        #     if col in df.columns:
        #         print(f"{col}: {df[col].isna().sum()} missing, sample values: {df[col].head().tolist()}")

        # Step 5: Prepare data for MongoDB
        # data_for_mongodb = df[datetime_columns].to_dict(orient="records")
        # print("\nData for MongoDB:", data_for_mongodb)
        # # Define datetime columns
        # datetime_columns = ['tbm_buildtime', 'curing_infeedtime', 'timestamp', 'timestamp_1']
        # # Process each datetime column
        # for col in datetime_columns:
        #     if col in df.columns:
        #         # Step 1: If the column contains strings, convert to datetime
        #         try:
        #             df[col] = pd.to_datetime(df[col], errors='coerce')
        #         except:
        #             # If already datetime or mixed, try to standardize
        #             df[col] = pd.to_datetime(df[col], errors='coerce')

        #         # Step 2: Apply forward fill and backward fill to handle missing values
        #         df[col] = df[col].ffill().bfill()

        #         # Step 3: Format as string for MongoDB compatibility (YYYY-MM-DD HH:MM:SS)
        #         df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')

        # # Step 4: Verify no missing values
        # for col in datetime_columns:
        #     if col in df.columns:
        #         print(f"Missing values in {col}:", df[col].isna().sum())

        # # Step 5: Prepare data for MongoDB
        # data_for_mongodb = df[datetime_columns].to_dict(orient="records")
        # print("Data for MongoDB:", data_for_mongodb)


        ##############added 04-06##################################added 04-06##################################added 04-06##################################added 04-06##################################added 04-06##################################added 04-06##################################added 04-06##################################added 04-06####################

        # Step 4: Calculate tbm_shift, curing_shift, and build_date
        df['tbm_shift'] = df['tbm_buildtime'].apply(get_shift)
        df['curing_shift'] = df['curing_infeedtime'].apply(get_shift)

        # Extract and convert build_date to ISO format
        def extract_and_convert_build_date(row):
            build_date = DEFAULT_DATETIME
            tbm_buildtime = row.get('tbm_buildtime', '')
            if isinstance(tbm_buildtime, str):
                try:
                    build_date = tbm_buildtime[:10]  # Extract YYYY-MM-DD
                    build_date = convert_string_to_date(build_date)  # Convert to datetime
                    if build_date:
                        return build_date.isoformat()  # Return as ISO string
                except Exception as e:
                    logging.info("Error extracting build_date: %s", e)
            return "No Date"

        df['build_date'] = df.apply(extract_and_convert_build_date, axis=1)

        df['build_date'] = pd.to_datetime(df['build_date'], errors='coerce')


        output_messages.append("CIMCORP Cleaning successful")

        logging.info("clean_cimcorp_data function is completed")
        return df, invalid_barcode_rows, duplicates_df, output_messages

    except Exception as e:
        error_message = f"Error during cleaning: {e}"
        output_messages.append(error_message)
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), output_messages

def insert_cleaned_data(db_client, cleaned_df, database_name, filename):
    output_messages = []
    logging.info("insert_cleaned_data function is just started")
    try:
        if cleaned_df.empty:
            output_messages.append("No valid data to save after cleaning.")
            return output_messages

        if database_name == "TUB":
            cleaned_collection_name = TUB_CLEANED_PREFIX + filename.replace(".csv", "").replace(".xlsx", "")
            target_db_name = Data_Clean_TUB
        elif database_name == "CIMCORP":
            cleaned_collection_name = CIMCORP_CLEANED_PREFIX + filename.replace(".csv", "").replace(".xlsx", "")
            target_db_name = Data_Clean_CIMCORP
        else:
            error_message = f"Unknown database: {database_name}.  Cannot insert cleaned data."
            output_messages.append(error_message)
            return output_messages

        data_to_insert = cleaned_df.to_dict(orient="records")
        db_client[target_db_name][cleaned_collection_name].insert_many(data_to_insert)
        success_message = f"Successfully copied {len(data_to_insert)} to {target_db_name}.{cleaned_collection_name}"
        output_messages.append(success_message)

    except Exception as e:
        error_message = f"An error occurred during insert cleaned data: {e}"
        output_messages.append(error_message)
    
    logging.info("insert_cleaned_data function is completed")
    return output_messages

# def remove_duplicates_collection(collection):
#     output_messages = []
#     logging.info("remove_duplicates_collection function is just started")
#     records = list(collection.find({}, {"_id": 0}))

#     if records:
#         df = pd.DataFrame(records)
#         df_unique = df.drop_duplicates()

#         if len(df) > len(df_unique):
#             message = f"🔍 Found and removing {len(df) - len(df_unique)} duplicate records from {collection.name}."
#             output_messages.append(message)

#             collection.delete_many({})
#             collection.insert_many(df_unique.to_dict(orient="records"))
#             success_message = f"✅ Cleaned and reinserted {len(df_unique)} unique records in {collection.name}."
#             output_messages.append(success_message)
#         else:
#             output_messages.append(f"✅ No duplicates found in {collection.name}.")
#     else:
#         output_messages.append(f"⚠️ No records found in {collection.name}.")
    
#     logging.info("remove_duplicates_collection function is completed")
#     return output_messages

def remove_duplicates_collection(collection):
    output_messages = []
    logging.info(f"remove_duplicates_collection function started for {collection.name}")
   
    try:
        # Create compound index for better duplicate detection
        # Using the fields you mentioned: barcode, ubno, date, time
        index_name = "duplicate_detection_idx"
        existing_indexes = collection.index_information()
       
        if index_name not in existing_indexes:
            collection.create_index([
                ("barcode", 1),
                ("modelno",1),
                ("sku",1),
                ("sizecode",1),
                ("gt_code",1),
                ("ubno", 1),
                ("date", 1),
                ("time", 1),
                ("tbm", 1),
                ("curing_press",1),
                ("curing_mouldid",1),
                # ("tbm_buildtime", 1),
                # ("curing_infeedtime", 1),
                ("con_kgf",1),
                ("cwrfv_oa_kgf",1),
                ("ccwrfv_oa_kgf",1),
                ("cwrfv_oa1h_kgf",1),
                ("ccwrfv_oa1h_kgf",1),
                ("lfv_oa_kgf",1),
                ("lfv_oa_1_kgf",1),
                ("upper_g",1),
                ("lower_g",1),
                ("static_g",1),
                ("up_low_g",1),
                ("total_rank",1)
            ], name=index_name, background=True)
            output_messages.append(f"Created compound index '{index_name}' on {collection.name}")
       
        # Use aggregation to find duplicates based on all relevant fields
        pipeline = [
            {"$group": {
                "_id": {
                    "barcode": "$barcode",
                    "modelno": "$modelno",
                    "sku": "$sku",
                    "sizecode": "$sizecode",
                    "gt_code": "$gt_code",
                    "ubno": "$ubno",
                    "date": "$date",
                    "time": "$time",
                    "tbm": "$tbm",
                    "curing_press": "$curing_press",
                    "curing_mouldid": "$curing_mouldid",
                    # "tbm_buildtime": "$tbm_buildtime",
                    # "curing_infeedtime": "$curing_infeedtime",
                    "con_kgf": "$con_kgf",
                    "cwrfv_oa_kgf": "$cwrfv_oa_kgf",
                    "ccwrfv_oa_kgf": "$ccwrfv_oa_kgf",
                    "cwrfv_oa1h_kgf": "$cwrfv_oa1h_kgf",
                    "ccwrfv_oa1h_kgf": "$ccwrfv_oa1h_kgf",
                    "lfv_oa_kgf": "$lfv_oa_kgf",
                    "lfv_oa_1_kgf": "$lfv_oa_1_kgf",
                    "upper_g": "$upper_g",
                    "lower_g": "$lower_g",
                    "static_g": "$static_g",
                    "up_low_g": "$up_low_g",
                    "total_rank": "$total_rank"
                },
                "count": {"$sum": 1},
                "ids": {"$push": "$_id"},
                "first_occurrence": {"$first": "$_id"}
            }},
            {"$match": {
                "count": {"$gt": 1}
            }}
        ]
       
        duplicates = list(collection.aggregate(pipeline))
        duplicate_count = sum(dup['count'] - 1 for dup in duplicates)
       
        if duplicate_count > 0:
            message = f"Found {duplicate_count} duplicate records in {collection.name}"
            output_messages.append(message)
            logging.info(message)
           
            # Remove duplicates - keep only the first occurrence
            removed_count = 0
            for dup in duplicates:
                # Keep the first document, remove others
                ids_to_remove = [id for id in dup['ids'] if id != dup['first_occurrence']]
                if ids_to_remove:
                    result = collection.delete_many({"_id": {"$in": ids_to_remove}})
                    removed_count += result.deleted_count
           
            success_message = f"Removed {removed_count} duplicates from {collection.name}"
            output_messages.append(success_message)
            logging.info(success_message)
        else:
            no_dupes_message = f"No duplicates found in {collection.name}"
            output_messages.append(no_dupes_message)
            logging.info(no_dupes_message)
           
    except Exception as e:
        error_message = f"Error removing duplicates from {collection.name}: {str(e)}"
        output_messages.append(error_message)
        logging.error(error_message)
        logging.exception("Detailed error traceback:")
   
    logging.info(f"remove_duplicates_collection function completed for {collection.name}")
    return output_messages

# def process_data_for_api(db_client, source_data, database_name, filename):
#     output_messages = []
#     cleaned_collection_name = None
#     invalid_collection_name = None
#     logging.info("process_data_for_api function is just started")
#     try:
#         df = pd.DataFrame(list(source_data))

#         if database_name == "TUB":
#             cleaned_df, invalid_df, tub_messages = clean_tub_data(df.copy())
#             output_messages.extend(tub_messages)

#             cleaned_collection_name = TUB_CLEANED_PREFIX + filename.replace(".csv", "").replace(".xlsx", "")
#             invalid_collection_name = "invalid_data_TUB_" + filename.replace(".csv", "").replace(".xlsx", "")

#             if not invalid_df.empty:
#                 invalid_df = invalid_df.drop('_id', axis=1, errors='ignore')
#                 invalid_collect = db_client[INVALID_ROWS_DB][invalid_collection_name]
#                 output_messages.extend(remove_duplicates_collection(invalid_collect))
#                 db_client[INVALID_ROWS_DB][invalid_collection_name].insert_many(invalid_df.to_dict("records"))
#                 output_messages.append(f"Invalid TUB data saved to invalid_data_TUB.{invalid_collection_name}")
#             else:
#                 output_messages.append("No invalid TUB data to save.")

#             output_messages.extend(insert_cleaned_data(db_client, cleaned_df, database_name, filename))

#             if not cleaned_df.empty:
#                 cleaned_df = cleaned_df.drop('_id', axis=1, errors='ignore')
#                 db_client[COMBINED_Database][ALL_COMBINED_TUB_COLLECTION].insert_many(cleaned_df.to_dict("records"))
#                 output_messages.append(f"Cleaned TUB data pushed to {COMBINED_Database}.{ALL_COMBINED_TUB_COLLECTION}")
#             else:
#                 output_messages.append("No cleaned TUB data to push to combined collection")

#         elif database_name == "CIMCORP":
#             cleaned_df, invalid_barcode_rows, duplicates_df, cimcorp_messages = clean_cimcorp_data(df.copy())
#             output_messages.extend(cimcorp_messages)

#             cleaned_collection_name = CIMCORP_CLEANED_PREFIX + filename.replace(".csv", "").replace(".xlsx", "")
#             invalid_collection_name = "invalid_data_CIMCORP_" + filename.replace(".csv", "").replace(".xlsx", "")

#             if not invalid_barcode_rows.empty:
#                 invalid_barcode_rows = invalid_barcode_rows.drop('_id', axis=1, errors='ignore')
#                 invalid_collect = db_client[INVALID_ROWS_DB][invalid_collection_name]
#                 output_messages.extend(remove_duplicates_collection(invalid_collect))
#                 db_client[INVALID_ROWS_DB][invalid_collection_name].insert_many(invalid_barcode_rows.to_dict("records"))
#                 output_messages.append(f"Invalid CIMCORP data saved to invalid_data_CIMCORP.{invalid_collection_name}")
#             else:
#                 output_messages.append("No invalid barcode CIMCORP data to save.")

#             if not duplicates_df.empty:
#                 duplicate_collection_name = f"duplicate_CIMCORP_{filename.replace('.csv', '').replace('.xlsx', '')}"
#                 duplicates_df = duplicates_df.drop('_id', axis=1, errors='ignore')
#                 db_client[INVALID_ROWS_DB][duplicate_collection_name].insert_many(duplicates_df.to_dict("records"))
#                 output_messages.append(f"Duplicate CIMCORP data saved to {INVALID_ROWS_DB}.{duplicate_collection_name}")
#             else:
#                 output_messages.append("No duplicate CIMCORP data to save.")

#             output_messages.extend(insert_cleaned_data(db_client, cleaned_df, database_name, filename))

#             if not cleaned_df.empty:
#                 cleaned_df = cleaned_df.drop('_id', axis=1, errors='ignore')
#                 db_client[COMBINED_Database][ALL_COMBINED_CIMCORP_COLLECTION].insert_many(cleaned_df.to_dict("records"))
#                 output_messages.append(f"Cleaned CIMCORP data pushed to {COMBINED_Database}.{ALL_COMBINED_CIMCORP_COLLECTION}")
#             else:
#                 output_messages.append("No cleaned CIMCORP data to push to combined collection")
#         else:
#             error_message = f"Unknown database: {database_name}"
#             output_messages.append(error_message)
            
#             logging.info("process_data_for_api function is completed inside the loop")
#             return output_messages, None, None

#     except Exception as e:
#         error_message = f"Error processing data: {e}"
#         output_messages.append(error_message)
#         return output_messages, None, None

#     logging.info("process_data_for_api function is completed")
#     return output_messages, cleaned_collection_name, invalid_collection_name

def process_data_for_api(db_client, source_data, database_name, filename):
    output_messages = []
    cleaned_collection_name = None
    invalid_collection_name = None
    logging.info("process_data_for_api function is just started")
    try:
        df = pd.DataFrame(list(source_data))
        if database_name == "TUB":
            cleaned_df, invalid_df, tub_messages = clean_tub_data(df.copy())
            output_messages.extend(tub_messages)
            cleaned_collection_name = TUB_CLEANED_PREFIX + filename.replace(".csv", "").replace(".xlsx", "")
            invalid_collection_name = "invalid_data_TUB_" + filename.replace(".csv", "").replace(".xlsx", "")
            
            if not invalid_df.empty:
                invalid_df = invalid_df.drop('_id', axis=1, errors='ignore')
                invalid_collect = db_client[INVALID_ROWS_DB][invalid_collection_name]
                db_client[INVALID_ROWS_DB][invalid_collection_name].insert_many(invalid_df.to_dict("records"))
                output_messages.append(f"Invalid TUB data saved to invalid_data_TUB.{invalid_collection_name}")
            else:
                output_messages.append("No invalid TUB data to save.")
            
            output_messages.extend(insert_cleaned_data(db_client, cleaned_df, database_name, filename))
            
            if not cleaned_df.empty:
                cleaned_df = cleaned_df.drop('_id', axis=1, errors='ignore')
                
                # CHANGE 1: Delete all existing data in combined TUB collection before insert
                combined_tub_collection = db_client[COMBINED_Database][ALL_COMBINED_TUB_COLLECTION]
                delete_result = combined_tub_collection.delete_many({})
                output_messages.append(f"Deleted {delete_result.deleted_count} existing documents from {COMBINED_Database}.{ALL_COMBINED_TUB_COLLECTION}")
                
                combined_tub_collection.insert_many(cleaned_df.to_dict("records"))
                output_messages.append(f"Cleaned TUB data pushed to {COMBINED_Database}.{ALL_COMBINED_TUB_COLLECTION}")
            else:
                output_messages.append("No cleaned TUB data to push to combined collection")

        elif database_name == "CIMCORP":
            cleaned_df, invalid_barcode_rows, duplicates_df, cimcorp_messages = clean_cimcorp_data(df.copy())
            output_messages.extend(cimcorp_messages)
            cleaned_collection_name = CIMCORP_CLEANED_PREFIX + filename.replace(".csv", "").replace(".xlsx", "")
            invalid_collection_name = "invalid_data_CIMCORP_" + filename.replace(".csv", "").replace(".xlsx", "")
            
            if not invalid_barcode_rows.empty:
                invalid_barcode_rows = invalid_barcode_rows.drop('_id', axis=1, errors='ignore')
                db_client[INVALID_ROWS_DB][invalid_collection_name].insert_many(invalid_barcode_rows.to_dict("records"))
                output_messages.append(f"Invalid CIMCORP data saved to invalid_data_CIMCORP.{invalid_collection_name}")
            else:
                output_messages.append("No invalid barcode CIMCORP data to save.")
            
            if not duplicates_df.empty:
                duplicate_collection_name = f"duplicate_CIMCORP_{filename.replace('.csv', '').replace('.xlsx', '')}"
                duplicates_df = duplicates_df.drop('_id', axis=1, errors='ignore')
                db_client[INVALID_ROWS_DB][duplicate_collection_name].insert_many(duplicates_df.to_dict("records"))
                output_messages.append(f"Duplicate CIMCORP data saved to {INVALID_ROWS_DB}.{duplicate_collection_name}")
            else:
                output_messages.append("No duplicate CIMCORP data to save.")
            
            output_messages.extend(insert_cleaned_data(db_client, cleaned_df, database_name, filename))
            
            if not cleaned_df.empty:
                cleaned_df = cleaned_df.drop('_id', axis=1, errors='ignore')
                
                # CHANGE 2: Delete all existing data in combined CIMCORP collection before insert
                combined_cimcorp_collection = db_client[COMBINED_Database][ALL_COMBINED_CIMCORP_COLLECTION]
                delete_result = combined_cimcorp_collection.delete_many({})
                output_messages.append(f"Deleted {delete_result.deleted_count} existing documents from {COMBINED_Database}.{ALL_COMBINED_CIMCORP_COLLECTION}")
                
                combined_cimcorp_collection.insert_many(cleaned_df.to_dict("records"))
                output_messages.append(f"Cleaned CIMCORP data pushed to {COMBINED_Database}.{ALL_COMBINED_CIMCORP_COLLECTION}")
            else:
                output_messages.append("No cleaned CIMCORP data to push to combined collection")
        else:
            error_message = f"Unknown database: {database_name}"
            output_messages.append(error_message)
            return output_messages, None, None
    except Exception as e:
        error_message = f"Error processing data: {e}"
        output_messages.append(error_message)
        return output_messages, None, None
    
    logging.info("process_data_for_api function is completed")
    return output_messages, cleaned_collection_name, invalid_collection_name

def get_unprocessed_files_api():
    logging.info("get_unprocessed_files_api function is just started")
    db = client[FILE_TRACKING_DB]
    files_to_process = list(db[FILE_TRACKING_COLLECTION].find({"processed": False}).sort([("inserted_timestamp", -1)]))
    logging.info("get_unprocessed_files_api function is completed")
    return files_to_process

def copy_to_single_collection_api():
    output_messages = []
    logging.info("copy_to_single_collection_api function is just started")
   
    try:
        source_db = client[COMBINED_Database]
        target_db = client[TUB_CIMCORP_DB]
 
        # Retrieve data from source collections
        tub_data = list(source_db[ALL_COMBINED_TUB_COLLECTION].find())
        output_messages.append(f"Retrieved {len(tub_data)} records from {ALL_COMBINED_TUB_COLLECTION}")
        tub_df = pd.DataFrame(tub_data)
 
        cimcorp_data = list(source_db[ALL_COMBINED_CIMCORP_COLLECTION].find())
        output_messages.append(f"Retrieved {len(cimcorp_data)} records from {ALL_COMBINED_CIMCORP_COLLECTION}")
        cimcorp_df = pd.DataFrame(cimcorp_data)
 
        if tub_df.empty:
            output_messages.append("TUB data is empty. No data to process.")
            return output_messages  # Exit if TUB is empty, as no data to process
 
        ##################Added 20-07-------------------------#############################
 
        db = client[COMBINED_Database]
        all_combined_cimcorp_collection = db[ALL_COMBINED_CIMCORP_COLLECTION]
 
        # Fetch all required fields from MongoDB
        try:
            cursor = all_combined_cimcorp_collection.find({}, {"barcode": 1, "uftesting_result": 1, "operation": 1, "tbm_gtcode": 1, "tbm": 1, "curing_press": 1, "curing_mouldid": 1, "curing_infeedtime": 1, "tbm_buildtime": 1, "build_date": 1, "_id": 0})
            cimcorp_records = list(cursor)
            if not cimcorp_records:
                output_messages.append("Warning: all_combined_cimcorp_data collection is empty.")
        except Exception as e:
            output_messages.append(f"Error fetching data from MongoDB: {e}")
            cimcorp_records = []
 
        # Create lookup dictionaries from MongoDB records
        uftesting_lookup = {}
        operation_lookup = {}
        tbm_gtcode_lookup = {}
        cimcorp_tbm_lookup = {}
        curing_press_lookup = {}
        curing_mouldid_lookup = {}
        curing_infeed_lookup = {}
        tbm_build_lookup = {}
        build_date_lookup = {}
 
        for rec in cimcorp_records:
            barcode = rec.get('barcode')
            if barcode is None:
                continue
 
            # uftesting_result lookup
            uftesting_lookup[barcode] = rec.get('uftesting_result')
 
            # operation lookup
            operation_lookup[barcode] = rec.get('operation')
 
            # tbm_gtcode lookup
            tbm_gtcode_lookup[barcode] = rec.get('tbm_gtcode')

            # cimcorp_tbm lookup
            cimcorp_tbm_lookup[barcode] = rec.get('tbm')

            # curing_press lookup
            curing_press_lookup[barcode] = rec.get('curing_press')

            # curing_mouldid lookup
            curing_mouldid_lookup[barcode] = rec.get('curing_mouldid')

            # curing_infeed lookup
            curing_infeed_lookup[barcode] = rec.get('curing_infeedtime')
            
            # tbm_build lookup
            tbm_build_lookup[barcode] = rec.get('tbm_buildtime')

            # build_date lookuo
            build_date_lookup[barcode] = rec.get('build_date')
 
        # Map values to tub_df
        try:
            tub_df['uftesting_result_tub'] = tub_df['barcode'].map(uftesting_lookup)
        except Exception as e:
            output_messages.append(f"Error mapping uftesting_result to tub_df: {e}")
            tub_df['uftesting_result_tub'] = None
 
        try:
            tub_df['operation_tub'] = tub_df['barcode'].map(operation_lookup)
        except Exception as e:
            output_messages.append(f"Error mapping operation to tub_df: {e}")
            tub_df['operation_tub'] = None
 
        try:
            tub_df['tbm_gtcode_tub'] = tub_df['barcode'].map(tbm_gtcode_lookup)
        except Exception as e:
            output_messages.append(f"Error mapping tbm_gtcode to tub_df: {e}")
            tub_df['tbm_gtcode_tub'] = None

        try:
            tub_df['cimcorp_tbm_tub'] = tub_df['barcode'].map(cimcorp_tbm_lookup)
        except Exception as e:
            output_messages.append(f"Error mapping tbm to tub_df: {e}")
            tub_df['cimcorp_tbm_tub'] = None

        try:
            tub_df['curing_press_tub'] = tub_df['barcode'].map(curing_press_lookup)
        except Exception as e:
            output_messages.append(f"Error mapping curing_press to tub_df: {e}")
            tub_df['curing_press_tub'] = None
        
        try:
            tub_df['curing_mouldid_tub'] = tub_df['barcode'].map(curing_mouldid_lookup)
        except Exception as e:
            output_messages.append(f"Error mapping curing_mouldid to tub_df: {e}")
            tub_df['curing_mouldid_tub'] = None

        try:
            tub_df['curing_infeedtime_tub'] = tub_df['barcode'].map(curing_infeed_lookup)
        except Exception as e:
            output_messages.append(f"Error mapping curing_infeedtime to tub_df: {e}")
            tub_df['curing_infeedtime_tub'] = None
        
        try:
            tub_df['tbm_buildtime_tub'] = tub_df['barcode'].map(tbm_build_lookup)
        except Exception as e:
            output_messages.append(f"Error mapping tbm_buildtime to tub_df: {e}")
            tub_df['tbm_buildtime_tub'] = None

        try:
            tub_df['build_date_tub'] = tub_df['barcode'].map(build_date_lookup)
        except Exception as e:
            output_messages.append(f"Error mapping tbm_buildtime to tub_df: {e}")
            tub_df['build_date_tub'] = None
 
        tub_df.replace('nan', pd.NA, inplace=True)
 
        ##############added 05-july -2025##################################
        cimcorp_df.replace('nan', pd.NA, inplace=True)
 
        tub_df['uftesting_result_tub'] = tub_df['uftesting_result_tub'].fillna('Unknown')
        cimcorp_df['uftesting_result'] = cimcorp_df['uftesting_result'].fillna('Unknown')
        tub_df['operation_tub'] = tub_df['operation_tub'].fillna('Unknown')
        cimcorp_df['operation'] = cimcorp_df['operation'].fillna('Unknown')
        tub_df['tbm_gtcode_tub'] = tub_df['tbm_gtcode_tub'].fillna('Unknown')
        cimcorp_df['tbm_gtcode'] = cimcorp_df['tbm_gtcode'].fillna('Unknown')
        tub_df['cimcorp_tbm_tub'] = tub_df['cimcorp_tbm_tub'].fillna('Unknown')
        cimcorp_df['tbm'] = cimcorp_df['tbm'].fillna('Unknown')
        
        tub_df['curing_press_tub'] = tub_df['curing_press_tub'].fillna('Unknown')
        cimcorp_df['curing_press'] = cimcorp_df['curing_press'].fillna('Unknown')
        tub_df['curing_mouldid_tub'] = tub_df['curing_mouldid_tub'].fillna('Unknown')
        cimcorp_df['curing_mouldid'] = cimcorp_df['curing_mouldid'].fillna('Unknown')
        tub_df['curing_infeedtime_tub'] = tub_df['curing_infeedtime_tub'].fillna('Unknown')
        cimcorp_df['curing_infeedtime'] = cimcorp_df['curing_infeedtime'].fillna('Unknown')
        tub_df['tbm_buildtime_tub'] = tub_df['tbm_buildtime_tub'].fillna('Unknown')
        cimcorp_df['tbm_buildtime'] = cimcorp_df['tbm_buildtime'].fillna('Unknown')
        tub_df['build_date_tub'] = tub_df['build_date_tub'].fillna('Unknown')
        cimcorp_df['build_date'] = cimcorp_df['build_date'].fillna('Unknown')
        tub_df['tbm_buildtime_shift'] = tub_df['tbm_buildtime_tub'].apply(get_shift)
 
        # Define masks for conditions
        mask_unknown_uf_tub = (tub_df['uftesting_result_tub'] == 'Unknown')
        mask_operation_known_tub = (tub_df['operation_tub'] != 'Unknown')
        mask_operation_unknown_tub = (tub_df['operation_tub'] == 'Unknown')
 
        mask_rank_123_tub = tub_df['total_rank'].isin([1,2,3])
        mask_rank_12_tub = tub_df['total_rank'].isin([1,2])
        mask_rank_3_tub = tub_df['total_rank'].isin([3])
       
        # Condition 1: Unknown uftesting_result + known operation + total_rank in [1,2,3] -> "Repl"
        cond1_tub = mask_unknown_uf_tub & mask_operation_known_tub & mask_rank_123_tub
        # Condition 2: operation Unknown + total_rank in [1,2] -> "OE1"
        cond2_tub = mask_operation_unknown_tub & mask_rank_12_tub
        cond3_tub = mask_unknown_uf_tub & mask_operation_unknown_tub & mask_rank_3_tub
 
        # Apply condition 1 replacements
        tub_df.loc[cond1_tub, ['uftesting_result_tub']] = "Repl"
 
        # Apply condition 2 replacements
        tub_df.loc[cond2_tub, ['uftesting_result_tub']] = "OE1"
 
        tub_df.loc[cond3_tub, ['uftesting_result_tub']] = "Repl"
 
        # Rename columns (excluding barcode)
        tub_cols = {col: f'{col}' for col in tub_df.columns if col != 'barcode'}
        cimcorp_cols = {col: f'{col}' for col in cimcorp_df.columns if col != 'barcode'}
        tub_df.rename(columns=tub_cols, inplace=True)
        if not cimcorp_df.empty:
            cimcorp_cols = {col: f'{col}' for col in cimcorp_df.columns if col != 'barcode'}
            cimcorp_df.rename(columns=cimcorp_cols, inplace=True)
 
        # Create different merge types
        data_merged_tub = tub_df.copy()  # No merge, just use tub_df
        data_merged_inner = pd.DataFrame()  # Initialize empty for inner join

        if not cimcorp_df.empty:
            data_merged_inner = cimcorp_df.merge(tub_df, on='barcode', how='inner')  # Inner join (only matching records)
            if data_merged_inner.empty:
                output_messages.append("No matching records found between TUB and CIMCORP data for inner join.")

        # Get channel mapping data
        db = client[config_databse]
        channel_no_collection = list(db[channel_no_collect].find())
        channel_no_collection = pd.DataFrame(channel_no_collection)

        # --- ✅ Handle channel_no_collect (gt_code & modelno mapping) ---
        if not channel_no_collection.empty and 'modelno' in channel_no_collection.columns:
            # Expand gt_code list if any row has multiple gt_codes
            channel_no_collection = channel_no_collection.explode('gt_code').reset_index(drop=True)
            # print("channel_no_collection:",channel_no_collection)

            # --- ✅ Map gt_code from channel collection to tub_df based on modelno ---
            tub_df = tub_df.merge(
                channel_no_collection[['modelno', 'gt_code']],
                on='modelno',
                how='left'
            )

            # --- ✅ Inner join cimcorp_df using gt_code equality ---
            if 'gt_code' in tub_df.columns and 'gt_code' in cimcorp_df.columns:
                data_merged_inner = cimcorp_df.merge(
                    tub_df,
                    on='gt_code',
                    how='inner'
                )

                if data_merged_inner.empty:
                    output_messages.append("No matching records found between TUB and CIMCORP data for gt_code inner join.")
            else:
                output_messages.append("gt_code column missing in either tub_df or cimcorp_df.")
        else:
            output_messages.append("Channel number collection is empty or missing modelno column.")
 
        # Validation checks
        if not isinstance(channel_no_collection, pd.DataFrame):
            raise TypeError("channel_no_collection must be a pandas DataFrame.")
        if 'sku' not in channel_no_collection.columns:
            raise ValueError("The 'sku' column is missing in channel_no_collection dataframe.")
        if 'modelno' not in channel_no_collection.columns:
            raise ValueError("The 'modelno' column is missing in channel_no_collection dataframe.")

        # Clean model numbers
        def clean_modelno(x):
            try:
                return str(eval(x))
            except (SyntaxError, NameError, TypeError):
                return x
 
        channel_no_collection['modelno'] = channel_no_collection['modelno'].apply(clean_modelno)
        sku_to_modelno = channel_no_collection.set_index('sku')['modelno'].to_dict()
       
        # Apply model numbers to data_merged_inner
        if not data_merged_inner.empty:
            data_merged_inner['modelno_cimcorp'] = data_merged_inner['sku'].map(sku_to_modelno)
            data_merged_inner['modelno_cimcorp'] = data_merged_inner['modelno_cimcorp'].fillna('Unknown')
 
        # Post-Processing for all DataFrames
        data_merged_tub.drop_duplicates(inplace=True)
        if not data_merged_inner.empty:
            data_merged_inner.drop_duplicates(inplace=True)
        data_merged_tub.drop_duplicates(inplace=True)
        data_merged_inner.drop_duplicates(inplace=True)
       
        # Process TUB DataFrame
        data_merged_tub['alert_id_ufm_ufm'] = None
        data_merged_tub['alert_id_ubno_balancing'] = None
        data_merged_tub['alert_id_ubno_yield_drop'] = None
        data_merged_tub['alert_id_ubno_load_pressure'] = None
        data_to_insert_tub = data_merged_tub.to_dict(orient="records")
 
        # Process INNER JOIN DataFrame
        data_to_insert_inner = []
        if not data_merged_inner.empty:
            data_merged_inner['alert_id_ufm_ufm'] = None
            data_merged_inner['alert_id_ubno_balancing'] = None
            data_merged_inner['alert_id_ubno_yield_drop'] = None
            data_merged_inner['alert_id_curing_ufm'] = None
            data_merged_inner['alert_id_curing_balancing'] = None
            data_merged_inner['alert_id_tbm_ufm'] = None
            data_merged_inner['alert_id_tbm_balancing'] = None
            data_merged_inner['alert_id_tbm_yield_drop'] = None
            data_merged_inner['alert_id_curing_yield_drop'] = None
            data_merged_inner['alert_id_tbm_load_pressure'] = None
            data_merged_inner['alert_id_curing_load_pressure'] = None
 
            data_merged_inner['uftesting_result'] = data_merged_inner['uftesting_result'].fillna('Unknown')
            data_merged_inner['uftesting_result'] = data_merged_inner['uftesting_result'].replace('nan', 'Unknown')
 
            mask_unknown_uf_cimcorp = (data_merged_inner['uftesting_result'] == 'Unknown')
            mask_operation_known_cimcorp = (data_merged_inner['operation'] != 'Unknown')
            mask_operation_unknown_cimcorp = (data_merged_inner['operation'] == 'Unknown')
 
            mask_rank_123_cimcorp = data_merged_inner['total_rank'].isin([1,2,3])
            mask_rank_12_cimcorp = data_merged_inner['total_rank'].isin([1,2])
            mask_rank_3_cimcorp = data_merged_inner['total_rank'].isin([3])
 
            # Condition 1: Unknown uftesting_result + known operation + total_rank in [1,2,3] -> "Repl"
            cond1_cimcorp = mask_unknown_uf_cimcorp & mask_operation_known_cimcorp & mask_rank_123_cimcorp  
 
            # Condition 2: operation Unknown + total_rank in [1,2] -> "OE1"
            cond2_cimcorp = mask_operation_unknown_cimcorp & mask_rank_12_cimcorp
 
            cond3_cimcorp = mask_unknown_uf_cimcorp & mask_operation_unknown_cimcorp & mask_rank_3_cimcorp
 
            # Apply condition 1 replacements
            data_merged_inner.loc[cond1_cimcorp, ['uftesting_result']] = "Repl"
 
            # Apply condition 2 replacements
            data_merged_inner.loc[cond2_cimcorp, ['uftesting_result']] = "OE1"
 
            data_merged_inner.loc[cond3_cimcorp, ['uftesting_result']] = "Repl"
 
            data_to_insert_inner = data_merged_inner.to_dict(orient="records")
 
        # Check for empty DataFrames
        if data_merged_tub.empty:
            output_messages.append("No data found in tub dataset to be copied.")
        if data_merged_inner.empty:
            output_messages.append("No matching records found between TUB and CIMCORP data.")
 
        # Insert data into MongoDB with duplicate checking
        def insert_with_check(collection_name, data_to_insert, existing_barcodes):
            new_records = [doc for doc in data_to_insert if doc['barcode'] not in existing_barcodes]
            if new_records:
                target_db[collection_name].insert_many(new_records)
                output_messages.append(f"Successfully appended {len(new_records)} new records into {target_db.name}.{collection_name}")
            else:
                output_messages.append(f"No new records to append to {collection_name}")
            return len(new_records)
 
        # # Get existing barcodes for each collection
        # existing_barcodes = {
        #     tub_collection_name: set(doc['barcode'] for doc in target_db[tub_collection_name].find({}, {'barcode': 1, '_id': 0})),
        #     inner_join_collection_name: set(doc['barcode'] for doc in target_db[inner_join_collection_name].find({}, {'barcode': 1, '_id': 0}))
        # }
 
        # # Insert data into collections
        # insert_with_check(tub_collection_name, data_to_insert_tub, existing_barcodes.get(tub_collection_name, set()))
        # insert_with_check(inner_join_collection_name, data_to_insert_inner, existing_barcodes.get(inner_join_collection_name, set()))
 
        # # Remove duplicates from all collections
        # output_messages.extend(remove_duplicates_collection(target_db[tub_collection_name]))
        # output_messages.extend(remove_duplicates_collection(target_db[inner_join_collection_name]))
 
 
        # 1. Define originals and their duplicate names
        orig_collections = [tub_collection_name, inner_join_collection_name]
        dup_collections  = [f"new_{name}" for name in orig_collections]
 
        # 2. Duplicate each original (if not already duplicated)
        for orig, dup in zip(orig_collections, dup_collections):
            if dup not in target_db.list_collection_names():
                docs = list(target_db[orig].find())
                if docs:
                    # strip out the old _id so MongoDB will generate new ones
                    to_insert = [{k: v for k, v in doc.items() if k != "_id"} for doc in docs]
                    target_db[dup].insert_many(to_insert)
                    output_messages.append(f"Copied {len(to_insert)} docs from '{orig}' → '{dup}'")
                else:
                    output_messages.append(f"No documents found in '{orig}' to duplicate")
            else:
                output_messages.append(f"'{dup}' already exists, skipping duplication")
 
        # 3. Now run insert_with_check + remove_duplicates on all four collections
        for (col_name, data_to_insert) in [
            (tub_collection_name,         data_to_insert_tub),
            (inner_join_collection_name,  data_to_insert_inner),
            (dup_collections[0],          data_to_insert_tub),
            (dup_collections[1],          data_to_insert_inner),
        ]:
            # fetch current barcodes
            existing = {
                doc['barcode']
                for doc in target_db[col_name].find({}, {'barcode': 1, '_id': 0})
            }
 
            # insert only new barcodes
            insert_with_check(col_name, data_to_insert, existing)
 
            # clean up any accidental full-document duplicates
            output_messages.extend(remove_duplicates_collection(target_db[col_name]))
 
 
    except Exception as e:
        error_message = f"An error occurred: {e}"
        output_messages.append(error_message)
        output_messages.append(traceback.format_exc())
   
    return output_messages

def make_hashable(value):
    logging.info("make_hashable function is just started")
    if isinstance(value, list):
        return tuple(make_hashable(v) for v in value)
    elif isinstance(value, dict):
        return tuple(sorted((k, make_hashable(v)) for k, v in value.items()))
    elif isinstance(value, np.generic):
        return value.item()
    
    logging.info("make_hashable function is completed")
    return value

def deduplicate_target_collection_api(target_db, target_collection_name):
    logging.info("deduplicate_target_collection_api function is just started")
    output_messages = []
    try:
        existing_data = list(target_db[target_collection_name].find({}, {"_id": 0}))
        existing_df = pd.DataFrame(existing_data)

        if existing_df.empty:
            output_messages.append(f"⚠️ Target collection {target_collection_name} is empty. No duplicates to remove.")
            return output_messages

        existing_df = existing_df.applymap(make_hashable)
        existing_df.drop_duplicates(inplace=True)
        output_messages.append(f"Target collection after dropping duplicates: {len(existing_df)} documents")

        deduplicated_data = existing_df.to_dict(orient="records")

        target_db.drop_collection(target_collection_name)
        target_db.create_collection(target_collection_name)
        if deduplicated_data:
            target_db[target_collection_name].insert_many(deduplicated_data)
            output_messages.append(f"Successfully replaced target collection {target_collection_name} with deduplicated data.")
        else:
            output_messages.append(f"No data to insert into target collection {target_collection_name} after deduplication.")

    except Exception as e:
        error_message = f"An error occurred during target collection deduplication: {e}"
        output_messages.append(error_message)

    logging.info("deduplicate_target_collection_api function is completed")
    return output_messages

def run_data_cleaning():
    start_time = time.time()
    output_messages = []
    cleaned_collection_names = []
    invalid_collection_names = []
    logging.info("run_data_cleaning function is just started")

    try:
        files_to_process = get_unprocessed_files_api()
        if not files_to_process:
            output_messages.append("No unprocessed files found for data cleaning.")
            final_message = "No unprocessed files found for data cleaning."
            return {"status": "success", "message": final_message, "output_log": output_messages, "cleaned_collections": cleaned_collection_names, "invalid_collections": invalid_collection_names, "execution_time": 0}


        for file_data in files_to_process:
            database_name = file_data["database_name"]
            filename = file_data["filename"]
            file_id = file_data["_id"]

            output_messages.append(f"Processing file: {filename} from database: {database_name}")

            source_collection_name = sanitize_collection_name(filename)
            source_db_name = TUB_DB if database_name == "TUB" else CIMCORP_DB

            source_data = client[source_db_name][source_collection_name].find()

            process_msgs, cleaned_collect_name, invalid_collect_name = process_data_for_api(client, source_data, database_name, filename)
            output_messages.extend(process_msgs)
            cleaned_collection_names.append(cleaned_collect_name)
            invalid_collection_names.append(invalid_collect_name)


            db = client[FILE_TRACKING_DB]
            db[FILE_TRACKING_COLLECTION].update_one(
                {"_id": file_id},
                {"$set": {"processed": True, "processed_timestamp":some_function()}}
            )
            output_messages.append(f"File {filename} processed and marked as processed in file tracking.")

        output_messages.append("Copying to single collection to final database...")
        copy_msgs = copy_to_single_collection_api()
        output_messages.extend(copy_msgs)

        final_message = "Data cleaning and processing completed successfully."
        end_time = time.time()
        execution_time = end_time - start_time
        output_messages.append(f"Data cleaning process completed in {execution_time:.2f} seconds.")
        
        logging.info("run_data_cleaning function is completed inside loop")
        return {"status": "success", "message": final_message, "output_log": output_messages, "cleaned_collections": cleaned_collection_names, "invalid_collections": invalid_collection_names, "execution_time": execution_time}

    except Exception as e:
        error_message = f"An error occurred in the data cleaning function: {e}"
        output_messages.append(error_message)

        logging.info("run_data_cleaning function is completed")
        return {"status": "error", "message": error_message, "output_log": output_messages, "cleaned_collections": cleaned_collection_names, "invalid_collections": invalid_collection_names, "execution_time": 0}

date_formats = ["%d-%m-%Y", "%Y-%m-%d"]
# Fetch documents

# --- Helper Functions (Ensure these are defined or imported) ---

def get_rank_columns(batch_data):
    # logging.info("get_rank_columns function is just started")
    # Placeholder: Implement logic to extract/summarize rank columns
    rank_cols = [col for col in batch_data.columns if 'rank' in col.lower()]
    summary = {}
    for col in rank_cols:
        if col in batch_data.columns:
            # Example: Count occurrences of each rank
            summary[col] = batch_data[col].value_counts().to_dict()
            # Convert keys to string if they are numbers for JSON compatibility
            summary[col] = {str(k): v for k, v in summary[col].items()}
    # logging.info("get_rank_columns function is completed")
    return summary

def convert_numpy_types(data):
    # logging.info("convert_numpy_types function is just started")
    if isinstance(data, np.generic):
        return data.item()
    elif isinstance(data, dict):
        return {k: convert_numpy_types(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_numpy_types(v) for v in data]
    # Handle pandas Timestamp
    elif isinstance(data, pd.Timestamp):
        return data.to_pydatetime() # Convert to Python datetime
    # logging.info("convert_numpy_types function is completed")
    return data
# --- End Helper Functions ---


def process_tbm_lot_and_alert_data():
    start_time = time.time()
    output_messages = []
    inserted_counts = {}
    updated_counts = {}
    logging.info("process_tbm_lot_and_alert_data function is just started")

    # Initialize MongoDB collections
    try:
        merge_collect = client[TUB_CIMCORP_DB][cimcorp_collection_name]
        channel_collect_tbm = client[config_databse][channel_no_collect_tbm]
        channel_collect_ufm = client[config_databse][channel_no_collect]
        lot_tbm_collect = client[lot_db][lot_tbm_collect_name]
        alert_collect = client[alert_table][alert_tbm_collect_name]
        averages_collect = client[average_db][averages_tbm_table]
        machine_collect = client[config_databse][machine_no_collect]
        curing_press_collect = client[config_databse][curing_press_dict_collect]
        alert_triggering_collect = client[alert_db][alert_triggering_collect_name]
        summary_triggering_collect = client[summary_db][summary_triggering_collect_name_tbm]
        alert_triggering_deviated_collect = client[alert_deviated][alert_triggering_deviated_tbm]
        alert_triggering_collect_dashboard = client[alert_db_dashboard][alert_triggering_collect_name]

        # === NEW: Virgin Yield & Final Yield Collections ===
        alert_triggering_virgin_yield_collect = client[alert_db_virgin_yield][alert_triggering_collect_name]
        alert_triggering_final_yield_collect = client[alert_db_final_yield][alert_triggering_collect_name]
        
        # === NEW: Final Yield Only Alert Collections ===
        alert_table_final_yield_db = "alert_table_final_yield"
        alert_final_yield_collect = client[alert_table_final_yield_db][alert_tbm_collect_name]

        alert_deviated_final_yield_db = "alert_deviated_final_yield"
        alert_triggering_deviated_final_yield_collect = client[alert_deviated_final_yield_db][alert_triggering_deviated_tbm]

        tbm_gtcodes_collect = client[model_db][tbm_collection]
        tbm_machine_collect = client[model_db][tbm_collection_machine]

        output_messages.append("✅ All TBM Collections loaded successfully including Virgin & Final Yield DBs.")
    except Exception as e:
        return {"status": "error", "message": "Database connection failed", "output_log": output_messages}, 500

    # Fetch active machines from model_workcenter_db.tbm_machine
    try:
        active_machines = set(tbm_machine_collect.distinct("machine", {"status": "active"}))
        output_messages.append(f"✅ Fetched {len(active_machines)} active machines: {active_machines}")
        if not active_machines:
            output_messages.append("❌ No active machines found in model_workcenter_db.tbm_machine. Exiting.")
            return {"status": "error", "message": "No active machines found", "output_log": output_messages}, 500
    except Exception as e:
        output_messages.append(f"❌ Error fetching active machines: {e}")
        return {"status": "error", "message": "Failed to fetch active machines", "output_log": output_messages}, 500

    # Fetch active model numbers
    try:
        active_modelnos = set(tbm_gtcodes_collect.distinct("modelno", {"status": "active"}))
        output_messages.append(f"✅ Fetched {len(active_modelnos)} active model numbers: {active_modelnos}")
    except Exception as e:
        output_messages.append(f"❌ Error fetching active model numbers: {e}")
        return {"status": "error", "message": "Failed to fetch active model numbers", "output_log": output_messages}, 500

    # Load data into DataFrames (unchanged until merge_df processing)
    try:
        merge1 = list(merge_collect.find({}))
        merge_df = pd.DataFrame(merge1)
        channel_data_tbm = list(channel_collect_tbm.find({}))
        channel_df = pd.DataFrame(channel_data_tbm)
        channel_data_ufm = list(channel_collect_ufm.find({}))
        channel_df_ufm = pd.DataFrame(channel_data_ufm)
        machine_data = list(machine_collect.find({}))
        machine_df = pd.DataFrame(machine_data)
        output_messages.append(f"✅ TBM Data loaded into DataFrames.")
    except Exception as e:
        return {"status": "error", "message": "Data loading failed", "output_log": output_messages}, 500

    # Get distinct model numbers
    channel_modelnos = set(channel_collect_tbm.distinct("modelno"))

    # Check required columns
    required_columns = {"modelno_cimcorp", "alert_id_tbm_ufm"}
    if not required_columns.issubset(merge_df.columns):
        print("Required columns are missing in merge_df. Exiting script.")
        exit()

    # Filter data
    filtered_df = merge_df[merge_df["modelno_cimcorp"].isin(channel_modelnos) & merge_df["alert_id_tbm_ufm"].isna()]
    if not filtered_df.empty:
        merge_df = filtered_df
    else:
        print("No data after filtering or condition not satisfied. Exiting script.")
        exit()

    # Assign date columns
    merge_df["date_of_tbm"] = merge_df.get("tbm_buildtime", pd.Series([None] * len(merge_df)))
    merge_df["date_of_curing"] = merge_df.get("curing_infeedtime", pd.Series([None] * len(merge_df)))

    # Function to handle datetime conversion
    def convert_datetime_with_invalid_mask(series, col_name):
        try:
            original_series = series.copy()
            cleaned_series = original_series.astype(str).str.strip().str.replace(r'\s+', ' ', regex=True)
            datetime_series = pd.to_datetime(cleaned_series, format='%Y-%m-%d %H:%M:%S', errors='coerce')
            invalid_mask = datetime_series.isna() & original_series.notna()
            datetime_series[invalid_mask] = DEFAULT_DATETIME
            return datetime_series.dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            return pd.Series([DEFAULT_DATETIME] * len(series))

    # Process datetime columns
    try:
        merge_df["date_and_time"] = convert_datetime_with_invalid_mask(merge_df["date_and_time"], "date_and_time")
        merge_df["date_and_time"] = merge_df["date_and_time"].astype(object).where(merge_df["date_and_time"].notnull(), "No Date")
        merge_df["date_of_tbm"] = convert_datetime_with_invalid_mask(merge_df["date_of_tbm"], "date_of_tbm")
        merge_df["date_of_tbm"] = merge_df["date_of_tbm"].astype(object).where(merge_df["date_of_tbm"].notnull(), "No Date")
        merge_df["date_of_curing"] = convert_datetime_with_invalid_mask(merge_df["date_of_curing"], "date_of_curing")
        merge_df["date_of_curing"] = merge_df["date_of_curing"].astype(object).where(merge_df["date_of_curing"].notnull(), "No Date")
        merge_df["time_of_ufm"] = pd.to_datetime(merge_df["time"], errors="coerce").dt.strftime("%H:%M:%S")
        time_invalid_mask = merge_df["time_of_ufm"].isna() & merge_df["time"].notna()
        if time_invalid_mask.any():
            logging.info("⚠️ Found %s invalid time values", time_invalid_mask.sum())
    except Exception as e:
        logging.info("❌ Error during datetime conversion: %s", e)
        return {"status": "error", "message": f"Error during datetime conversion: {e}", "output_log": output_messages}, 500

    merge_df["tbm_shift"] = merge_df["date_of_tbm"].apply(get_shift)

    # Fetch last alert ID
    try:
        last_alert = alert_collect.find_one(sort=[("alert_id_tbm_ufm", -1)])
        last_alert_id = int(last_alert["alert_id_tbm_ufm"]) if last_alert and "alert_id_tbm_ufm" in last_alert else 0
    except Exception as e:
        error_message = f"❌ TBM Error fetching last alert ID for lot/alert processing: {e}"
        output_messages.append(error_message)
        last_alert_id = 0

    # Fetch last lot IDs per machine and model
    try:
        last_lots = list(lot_tbm_collect.aggregate([
            {
                "$match": {
                    "lot_id": {"$regex": "^TBM_[^_]+_[^_]+_\\d+$"},
                    "modelno": {"$in": list(active_modelnos)},
                    "machine": {"$in": list(active_machines)}
                }
            },
            {
                "$addFields": {
                    "lot_number": {
                        "$toInt": {
                            "$arrayElemAt": [
                                {"$split": ["$lot_id", "_"]},
                                -1
                            ]
                        }
                    },
                    "modelno": "$modelno",
                    "machine": "$machine"
                }
            },
            {
                "$sort": {"lot_number": -1}
            },
            {
                "$group": {
                    "_id": {"modelno": "$modelno", "machine": "$machine"},
                    "max_lot_number": {"$first": "$lot_number"},
                    "last_lot_id": {"$first": "$lot_id"}
                }
            }
        ]))
        last_lot_dict = {
            (item["_id"]["modelno"], item["_id"]["machine"]): {
                "max_lot_number": item["max_lot_number"],
                "last_lot_id": item["last_lot_id"]
            }
            for item in last_lots
        }
    except Exception as e:
        error_message = f"❌ TBM Error fetching last lot IDs: {e}"
        logging.error(error_message)
        output_messages.append(error_message)
        last_lot_dict = {}

    # Prepare merge_df
    merge_df["tbm"] = merge_df["tbm"].astype(str)
    machine_df["machine"] = machine_df["machine"].astype(str)
    machine_df["tbm"] = machine_df["tbm"].astype(str)
    merge_df = merge_df.merge(machine_df[["tbm", "machine"]], on="tbm", how="left")
    merge_df["tbm"] = merge_df["machine"].fillna(merge_df["tbm"])
    # NEW: Filter merge_df to include only active machines
    merge_df = merge_df[merge_df["machine"].isin(active_machines)]
    if merge_df.empty:
        output_messages.append("❌ No data in merge_df after filtering for active machines. Exiting.")
        return {"status": "error", "message": "No data after active machine filtering", "output_log": output_messages}, 500

    # Initialize tracking variables
    machine_lot_avg_map = {}
    tbm_previous_machine_model_lot_ids = {}
    # === NEW: Bulk operations for Final Yield Only DBs ===
    final_yield_only_bulk = []           # alert_table_final_yield
    final_yield_deviated_only_bulk = []  # alert_deviated_final_yield
    alert_bulk_operations = []
    averages_bulk_operations = []
    triggering_bulk_operations = []
    deviated_triggering_bulk_operations = []
    summary_bulk_operation = []
    debug_data_list_tbm = []

    # Pre-populate machine_lot_avg_map and tbm_previous_machine_model_lot_ids
    try:
        last_averages = list(averages_collect.aggregate([
            {
                "$match": {
                "tbm_machine_model_lot_id": {"$regex": "^[^_]+_[^_]+_TBM_[^_]+_[^_]+_\\d+_\\d+$"},
                "modelno": {"$in": list(active_modelnos)},
                "machine": {"$in": list(active_machines)}
            }
            },
            {
                "$addFields": {
                    "lot_number": {
                        "$toInt": {
                            "$arrayElemAt": [
                                {"$split": ["$lot_id", "_"]},
                                -1
                            ]
                        }
                    }
                }
            },
            {
                "$sort": {"lot_number": -1}
            },
            {
                "$group": {
                    "_id": {"modelno": "$modelno", "machine": "$machine"},
                    "last_tbm_lot_id": {"$first": "$tbm_machine_model_lot_id"},
                    "last_averages": {"$first": "$current_lot_averages"}
                }
            }
        ]))
        for item in last_averages:
            modelno = item["_id"]["modelno"]
            machine = item["_id"]["machine"]
            tbm_lot_id = item["last_tbm_lot_id"]
            averages = item["last_averages"]
            tbm_previous_machine_model_lot_ids[(modelno, machine)] = tbm_lot_id
            if averages:
                machine_lot_avg_map[(modelno, machine, tbm_lot_id)] = averages
                # logging.info(f"Pre-populated machine_lot_avg_map with averages for {tbm_lot_id}")
            else:
                logging.warning(f"No averages found for {tbm_lot_id} in averages_collect")
        # logging.info(f"Initialized tbm_previous_machine_model_lot_ids: {tbm_previous_machine_model_lot_ids}")
    except Exception as e:
        logging.error(f"Error pre-populating machine_lot_avg_map and tbm_previous_machine_model_lot_ids: {e}")
        output_messages.append(f"❌ Error pre-populating machine_lot_avg_map and tbm_previous_machine_model_lot_ids: {e}")

    numeric_params = [
        "con_kgf", "cwrfv_oa_kgf", "ccwrfv_oa_kgf", "cwrfv_oa1h_kgf", "ccwrfv_oa1h_kgf",
        "lfv_oa_kgf", "lfv_oa_1_kgf", "upper_g", "lower_g", "static_g", "up_low_g",
        "load1", "airpress1"
    ]
    yield_params = [
        "total_rank_yield", "rfv_yield", "rfh1_yield", "lfv_yield", "conicity_yield",
        "ufm_yield", "rorank_yield", "radial_runout_yield", "radial_runout_first_harmonic_yield",
        "lateral_run_out_yield", "lateral_run_out_top_over_yield", "lateral_run_out_bottom_over_yield",
        "lateral_run_out_harmonic_yield", "lateral_run_out_top_first_harmonic_yield",
        "lateral_run_out_bottom_first_harmonic_yield", "bulge_yield", "bulge_top_over_yield",
        "bulge_bottom_over_yield", "dent_yield", "dent_top_over_yield", "dent_bottom_over_yield",
        "balancing_yield", "upper_yield", "lower_yield", "upper_lower_yield", "static_yield",
        "warehouse_yield", "visual_yield"
    ]
    rank_columns = [
        'total_rank', 'cwrfv_oa_rank', 'ccwrfv_oa_rank', 'cwrfv_oa1h_rank',
        'ccwrfv_oa1h_rank', 'lfv_oa_rank', 'lfv_oa_1_rank', 'con_rank',
        'ufm_rank', 'rc_oa_rank', 'rc_oa1h_rank', 'rorank', 'lt_oa_rank',
        'lb_oa_rank', 'lateral_run_out_rank', 'lb_oa1h_rank', 'lt_oa1h_rank',
        'lateral_run_out_harmonic_rank', 'ltbulg_rank', 'lbbulg_rank',
        'bulge_rank', 'dent_rank', 'dent1_rank', 'dent_overall_rank',
        'bal_rank', 'upper_rank', 'lower_rank', 'up_low_rank', 'static_rank'
    ]
    
    # Define rank parameter mappings
    rank_param_mappings = {
        "total_rank_yield": ["total_rank"],
        "rfv_yield": ["cwrfv_oa_rank", "ccwrfv_oa_rank"],
        "rfh1_yield": ["cwrfv_oa1h_rank", "ccwrfv_oa1h_rank"],
        "lfv_yield": ["lfv_oa_rank", "lfv_oa_1_rank"],
        "conicity_yield": ["con_rank"],
        "ufm_yield": ["ufm_rank"],
        "rorank_yield": ["rorank"], 
        "radial_runout_yield": ["rc_oa_rank"], 
        "radial_runout_first_harmonic_yield": ["rc_oa1h_rank"],
        "lateral_run_out_top_over_yield": ["lt_oa_rank"],
        "lateral_run_out_bottom_over_yield": ["lb_oa_rank"],
        "lateral_run_out_top_first_harmonic_yield": ["lt_oa1h_rank"],
        "lateral_run_out_bottom_first_harmonic_yield": ["lb_oa1h_rank"],
        "bulge_top_over_yield": ["ltbulg_rank"],
        "bulge_bottom_over_yield": ["lbbulg_rank"],
        "dent_top_over_yield": ["dent_rank"],
        "dent_bottom_over_yield": ["dent1_rank"],
        "balancing_yield": ["bal_rank"],
        "upper_yield": ["upper_rank"],
        "lower_yield": ["lower_rank"],
        "upper_lower_yield": ["up_low_rank"],
        "static_yield": ["static_rank"],
        "lateral_run_out_yield": ["lt_oa_rank", "lb_oa_rank"],
        "lateral_run_out_harmonic_yield": ["lt_oa1h_rank", "lb_oa1h_rank"],
        "bulge_yield": ["ltbulg_rank", "lbbulg_rank"],
        "dent_yield": ["dent_rank", "dent1_rank"]
    }

    # === MAIN CHANGE: Inside the model loop (after model_data is created) ===
    # Replace the existing model_data creation block with this enhanced version:

    for _, row in channel_df.iterrows():
        try:
            model_number = str(row.get("modelno", "")).strip()
            if model_number not in active_modelnos:
                output_messages.append(f"Skipping TBM model {model_number} as it is not active.")
                continue

            gt_codes = row.get("gt_code", [])
            if not isinstance(gt_codes, list):
                gt_codes = [gt_codes]

            target_lot_size = int(row.get("lot_size", 0)) or 0
            if target_lot_size == 0:
                output_messages.append(f"Skipping TBM model {model_number} due to invalid lot size.")
                continue

            spec_con_kgf = float(row.get("spec_con_kgf", 0))
            sizecode = str(row.get("sizecode", ""))
            # gt_code = str(row.get("gt_code", ""))
            # spec_rfv = float(row.get("spec_rfv", 0))
            spec_cwrfv_oa_kgf = float(row.get("spec_cwrfv_oa_kgf", 0))
            spec_ccwrfv_oa_kgf = float(row.get("spec_ccwrfv_oa_kgf", 0))
            # spec_rfh1 = float(row.get("spec_rfh1", 0))
            spec_cwrfv_oa1h_kgf = float(row.get("spec_cwrfv_oa1h_kgf", 0))
            spec_ccwrfv_oa1h_kgf = float(row.get("spec_ccwrfv_oa1h_kgf", 0))
            # spec_lfv = float(row.get("spec_lfv", 0))
            spec_lfv_oa_kgf = float(row.get("spec_lfv_oa_kgf", 0))
            spec_lfv_oa_1_kgf = float(row.get("spec_lfv_oa_1_kgf", 0))
            spec_static = float(row.get("spec_static", 0))
            spec_upper_g = float(row.get("spec_upper_g", 0))
            spec_lower_g = float(row.get("spec_lower_g", 0))
            spec_up_low_g = float(row.get("spec_up_low_g", 0))
            test_load = float(row.get("test_load", 0))
            pressure = float(row.get("pressure", 0))
            # spec_yield = float(row.get("spec_yield", 0))

            ############### Add 16-07 start ######################

            total_rank_yield_spec_yield = float(row.get("total_rank_yield_spec_yield", 0))
            rfv_yield_spec_yield = float(row.get("rfv_yield_spec_yield", 0))
            rfh1_yield_spec_yield = float(row.get("rfh1_yield_spec_yield", 0))
            lfv_yield_spec_yield = float(row.get("lfv_yield_spec_yield", 0))
            conicity_yield_spec_yield = float(row.get("conicity_yield_spec_yield", 0))
            ufm_yield_spec_yield = float(row.get("ufm_yield_spec_yield", 0))
            rorank_yield_spec_yield = float(row.get("rorank_yield_spec_yield", 0))
            radial_runout_yield_spec_yield = float(row.get("radial_runout_yield_spec_yield", 0))
            radial_runout_first_harmonic_yield_spec_yield = float(row.get("radial_runout_first_harmonic_yield_spec_yield", 0))
            lateral_run_out_yield_spec_yield = float(row.get("lateral_run_out_yield_spec_yield", 0))
            lateral_run_out_top_over_yield_spec_yield = float(row.get("lateral_run_out_top_over_yield_spec_yield", 0))
            lateral_run_out_bottom_over_yield_spec_yield = float(row.get("lateral_run_out_bottom_over_yield_spec_yield", 0))
            lateral_run_out_harmonic_yield_spec_yield = float(row.get("lateral_run_out_harmonic_yield_spec_yield", 0))
            lateral_run_out_top_first_harmonic_yield_spec_yield = float(row.get("lateral_run_out_top_first_harmonic_yield_spec_yield", 0))
            lateral_run_out_bottom_first_harmonic_yield_spec_yield = float(row.get("lateral_run_out_bottom_first_harmonic_yield_spec_yield", 0))
            bulge_yield_spec_yield = float(row.get("bulge_yield_spec_yield", 0))
            bulge_top_over_yield_spec_yield = float(row.get("bulge_top_over_yield_spec_yield", 0))
            bulge_bottom_over_yield_spec_yield = float(row.get("bulge_bottom_over_yield_spec_yield", 0))
            dent_yield_spec_yield = float(row.get("dent_yield_spec_yield", 0))
            dent_top_over_yield_spec_yield = float(row.get("dent_top_over_yield_spec_yield", 0))
            dent_bottom_over_yield_spec_yield = float(row.get("dent_bottom_over_yield_spec_yield", 0))
            balancing_yield_spec_yield = float(row.get("balancing_yield_spec_yield", 0))
            upper_yield_spec_yield = float(row.get("upper_yield_spec_yield", 0))
            lower_yield_spec_yield = float(row.get("lower_yield_spec_yield", 0))
            upper_lower_yield_spec_yield = float(row.get("upper_lower_yield_spec_yield", 0))
            static_yield_spec_yield = float(row.get("static_yield_spec_yield", 0))
            warehouse_yield_spec_yield = float(row.get("warehouse_yield_spec_yield", 0))
            visual_yield_spec_yield = float(row.get("visual_yield_spec_yield", 0))

            ############### Add 16-07 end ######################

            # threshold_value = float(row.get("threshold_value", 0.5))
            threshold_value_con_kgf = float(row.get("threshold_value_con_kgf", 0.5))
            threshold_value_cwrfv_oa_kgf = float(row.get("threshold_value_cwrfv_oa_kgf", 0.5))
            threshold_value_ccwrfv_oa_kgf = float(row.get("threshold_value_ccwrfv_oa_kgf", 0.5))
            threshold_value_cwrfv_oa1h_kgf = float(row.get("threshold_value_cwrfv_oa1h_kgf", 0.5))
            threshold_value_ccwrfv_oa1h_kgf = float(row.get("threshold_value_ccwrfv_oa1h_kgf", 0.5))
            threshold_value_lfv_oa_kgf = float(row.get("threshold_value_lfv_oa_kgf", 0.5))
            threshold_value_lfv_oa_1_kgf = float(row.get("threshold_value_lfv_oa_1_kgf", 0.5))
            # threshold_value_bal = float(row.get("threshold_value_bal", 5))
            threshold_value_bal_upper_g = float(row.get("threshold_value_bal_upper_g", 5))
            threshold_value_bal_lower_g = float(row.get("threshold_value_bal_lower_g", 5))
            threshold_value_bal_up_low_g = float(row.get("threshold_value_bal_up_low_g", 5))
            threshold_value_bal_static_g = float(row.get("threshold_value_bal_static_g", 5))
            test_load_tolerance = float(row.get("test_load_tolerance", 5))
            pressure_tolerance = float(row.get("pressure_tolerance", 2))
            # threshold_value_yield = float(row.get("threshold_value_yield", 0))
            threshold_value_yield_total_rank_yield = float(row.get("threshold_value_yield_total_rank_yield", 0))
            threshold_value_yield_rfv_yield = float(row.get("threshold_value_yield_rfv_yield", 0))
            threshold_value_yield_rfh1_yield = float(row.get("threshold_value_yield_rfh1_yield", 0))
            threshold_value_yield_lfv_yield = float(row.get("threshold_value_yield_lfv_yield", 0))
            threshold_value_yield_conicity_yield = float(row.get("threshold_value_yield_conicity_yield", 0))
            threshold_value_yield_ufm_yield = float(row.get("threshold_value_yield_ufm_yield", 0))
            threshold_value_yield_rorank_yield = float(row.get("threshold_value_yield_rorank_yield", 0))
            threshold_value_yield_radial_runout_yield = float(row.get("threshold_value_yield_radial_runout_yield", 0))
            threshold_value_yield_radial_runout_first_harmonic_yield = float(row.get("threshold_value_yield_radial_runout_first_harmonic_yield", 0))
            threshold_value_yield_lateral_run_out_yield = float(row.get("threshold_value_yield_lateral_run_out_yield", 0))
            threshold_value_yield_lateral_run_out_top_over_yield = float(row.get("threshold_value_yield_lateral_run_out_top_over_yield", 0))
            threshold_value_yield_lateral_run_out_bottom_over_yield = float(row.get("threshold_value_yield_lateral_run_out_bottom_over_yield", 0))
            threshold_value_yield_lateral_run_out_harmonic_yield = float(row.get("threshold_value_yield_lateral_run_out_harmonic_yield", 0))
            threshold_value_yield_lateral_run_out_top_first_harmonic_yield = float(row.get("threshold_value_yield_lateral_run_out_top_first_harmonic_yield", 0))
            threshold_value_yield_lateral_run_out_bottom_first_harmonic_yield = float(row.get("threshold_value_yield_lateral_run_out_bottom_first_harmonic_yield", 0))
            threshold_value_yield_bulge_yield = float(row.get("threshold_value_yield_bulge_yield", 0))
            threshold_value_yield_bulge_top_over_yield = float(row.get("threshold_value_yield_bulge_top_over_yield", 0))
            threshold_value_yield_bulge_bottom_over_yield = float(row.get("threshold_value_yield_bulge_bottom_over_yield", 0))
            threshold_value_yield_dent_yield = float(row.get("threshold_value_yield_dent_yield", 0))
            threshold_value_yield_dent_top_over_yield = float(row.get("threshold_value_yield_dent_top_over_yield", 0))
            threshold_value_yield_dent_bottom_over_yield = float(row.get("threshold_value_yield_dent_bottom_over_yield", 0))
            threshold_value_yield_balancing_yield = float(row.get("threshold_value_yield_balancing_yield", 0))
            threshold_value_yield_upper_yield = float(row.get("threshold_value_yield_upper_yield", 0))
            threshold_value_yield_lower_yield = float(row.get("threshold_value_yield_lower_yield", 0))
            threshold_value_yield_upper_lower_yield = float(row.get("threshold_value_yield_upper_lower_yield", 0))
            threshold_value_yield_static_yield = float(row.get("threshold_value_yield_static_yield", 0))
            threshold_value_yield_warehouse_yield = float(row.get("threshold_value_yield_warehouse_yield", 0))
            threshold_value_yield_visual_yield = float(row.get("threshold_value_yield_visual_yield", 0))

            for gt_code in gt_codes:
                gt_code = str(gt_code).strip()

                # Base filtered data
                model_data_full = merge_df[
                    (merge_df["modelno"] == model_number) &
                    (merge_df["modelno_cimcorp"] == model_number) &
                    (merge_df["tbm_gtcode"] == gt_code) &
                    (merge_df["machine"].isin(active_machines))
                ].copy()

                if model_data_full.empty:
                    continue

                total_barcodes_model = len(model_data_full)

                if total_barcodes_model < target_lot_size:
                    output_messages.append(f"Skipping TBM model {model_number} (GT: {gt_code}) due to insufficient barcodes ({total_barcodes_model} < {target_lot_size}).")
                    continue
                
                # Ensure date_of_tbm is proper datetime for sorting
                model_data_full["date_of_dt"] = pd.to_datetime(model_data_full["date_and_time"], errors='coerce')

                # === 1. Virgin Yield: Keep FIRST occurrence of each barcode ===
                virgin_data = model_data_full.sort_values("date_of_dt").drop_duplicates(subset="barcode", keep="first").copy()
                # >>> ADD THESE PRINT STATEMENTS <<<
                virgin_dupes = model_data_full[model_data_full.duplicated(subset="barcode", keep=False)]
                if not virgin_dupes.empty:
                    first_occ = virgin_dupes.sort_values("date_of_dt").drop_duplicates("barcode", keep="first")
                    # print("=== VIRGIN YIELD - Duplicate barcodes (first occurrence shown) ===")
                    # for _, r in first_occ.iterrows():
                    #     print(f"Barcode: {r['barcode']} | First seen: {r['date_and_time']} | Machine: {r['machine']} | Model: {model_number}")
                # <<< END OF ADDED PRINT >>>

                # === 2. Final Yield: Keep LAST occurrence of each barcode ===
                final_data = model_data_full.sort_values("date_of_dt").drop_duplicates(subset="barcode", keep="last").copy()
                # >>> ADD THESE PRINT STATEMENTS <<<
                final_dupes = model_data_full[model_data_full.duplicated(subset="barcode", keep=False)]
                if not final_dupes.empty:
                    last_occ = final_dupes.sort_values("date_of_dt").drop_duplicates("barcode", keep="last")
                    # print("=== FINAL YIELD - Duplicate barcodes (last occurrence shown) ===")
                    # for _, r in last_occ.iterrows():
                    #     print(f"Barcode: {r['barcode']} | Last seen: {r['date_and_time']} | Machine: {r['machine']} | Model: {model_number}")
                # <<< END OF ADDED PRINT >>>

                # Sort both for lot processing
                virgin_data.sort_values("date_of_dt", inplace=True)
                final_data.sort_values("date_of_dt", inplace=True)

                # Use original full data for main lot processing (existing logic)
                model_data = model_data_full.copy()
                model_data.sort_values("date_of_tbm", ascending=True, inplace=True)

                alert_map = {}

                # === PROCESS LOTS USING ORIGINAL DATA (unchanged logic below) ===
                for tbm_value, tbm_data in model_data.groupby("tbm"):
                    if tbm_value not in active_machines:
                        continue

                    tbm_data = tbm_data[tbm_data["tbm_machine_model_lot_id"].isnull()].copy()
                    if tbm_data.empty:
                        continue

                    total_tbm_barcodes = len(tbm_data)
                    if total_tbm_barcodes < target_lot_size:
                        continue

                    machine_no = str(tbm_value)
                    barcode_counter = 0

                    while barcode_counter < total_tbm_barcodes:
                        remaining = total_tbm_barcodes - barcode_counter
                        if remaining < target_lot_size:
                            break

                        batch_size = target_lot_size
                        batch_indices = tbm_data.iloc[barcode_counter: barcode_counter + batch_size].index
                        batch_data = tbm_data.iloc[barcode_counter: barcode_counter + batch_size].copy()

                        # Generate lot_id and tbm_machine_model_lot_id
                        if (model_number, machine_no) not in last_lot_dict:
                            last_lot_dict[(model_number, machine_no)] = {
                                "max_lot_number": 0,
                                "last_lot_id": None
                            }
                        last_lot_number = last_lot_dict[(model_number, machine_no)]["max_lot_number"] + 1
                        unique_lot_id_current = f"TBM_{machine_no}_{model_number}_{last_lot_number}"

                        current_barcode_counter = barcode_counter + batch_size
                        current_tbm_machine_model_lot_id = f"{machine_no}_{model_number}_TBM_{machine_no}_{model_number}_{last_lot_number}_{current_barcode_counter}"

                        last_lot_dict[(model_number, machine_no)]["max_lot_number"] = last_lot_number
                        last_lot_dict[(model_number, machine_no)]["last_lot_id"] = unique_lot_id_current

                        if not re.match(r"^TBM_[^_]+_[^_]+_\d+$", unique_lot_id_current):
                            logging.error(f"Invalid lot_id format: {unique_lot_id_current}")
                            continue
                        if not re.match(r"^[^_]+_[^_]+_TBM_[^_]+_[^_]+_\d+_\d+$", current_tbm_machine_model_lot_id):
                            logging.error(f"Invalid tbm_machine_model_lot_id format: {current_tbm_machine_model_lot_id}")
                            continue

                        tbm_previous_machine_model_lot_id = tbm_previous_machine_model_lot_ids.get((model_number, machine_no))
                        logging.info(f"Initial tbm_previous_machine_model_lot_id: {tbm_previous_machine_model_lot_id} for model {model_number}, machine {machine_no}")
                        if tbm_previous_machine_model_lot_id is None and last_lot_number > 1:
                            prev_lot_number = last_lot_number - 1
                            prev_lot_id = f"TBM_{machine_no}_{model_number}_{prev_lot_number}"
                            logging.info(f"Fetching previous lot for lot_id: {prev_lot_id}")
                            try:
                                prev_lot = averages_collect.find_one(
                                    {
                                        "modelno": model_number,
                                        "machine": machine_no,
                                        "lot_id": prev_lot_id
                                    }
                                )
                                if prev_lot:
                                    tbm_previous_machine_model_lot_id = prev_lot["tbm_machine_model_lot_id"]
                                    logging.info(f"Found previous lot: {tbm_previous_machine_model_lot_id}")
                                else:
                                    logging.warning(f"No previous lot found for lot_id: {prev_lot_id}")
                                    tbm_previous_machine_model_lot_id = None
                            except Exception as e:
                                logging.error(f"Error fetching previous tbm_machine_model_lot_id for lot {prev_lot_number}: {e}")
                                tbm_previous_machine_model_lot_id = None

                        previous_lot_id = f"TBM_{machine_no}_{model_number}_{last_lot_number - 1}" if last_lot_number > 1 else None
                        merge_df.loc[batch_indices, "lot_id"] = unique_lot_id_current
                        merge_df.loc[batch_indices, "previous_lot_id"] = previous_lot_id
                        merge_df.loc[batch_indices, "tbm_machine_model_lot_id"] = current_tbm_machine_model_lot_id
                        merge_df.loc[batch_indices, "tbm_previous_machine_model_lot_id"] = tbm_previous_machine_model_lot_id

                        prev_key = (model_number, machine_no, tbm_previous_machine_model_lot_id)
                        prev_lot_avg = machine_lot_avg_map.get(prev_key, None)
                        if prev_lot_avg is None and tbm_previous_machine_model_lot_id is not None:
                            try:
                                prev_lot_doc = averages_collect.find_one(
                                    {
                                        "modelno": model_number,
                                        "machine": machine_no,
                                        "tbm_machine_model_lot_id": tbm_previous_machine_model_lot_id
                                    },
                                    {"current_lot_averages": 1}
                                )
                                prev_lot_avg = prev_lot_doc.get("current_lot_averages", {}) if prev_lot_doc else {}
                                if prev_lot_avg:
                                    machine_lot_avg_map[prev_key] = prev_lot_avg
                                    logging.info(f"Fetched previous lot averages for {tbm_previous_machine_model_lot_id} from database")
                                else:
                                    logging.warning(f"No previous lot averages found for {tbm_previous_machine_model_lot_id} in database")
                            except Exception as e:
                                logging.error(f"Error fetching previous lot averages for {tbm_previous_machine_model_lot_id}: {e}")
                                prev_lot_avg = {}
                        if last_lot_number == 1:
                            prev_lot_avg = {}
                            logging.info(f"First lot for model {model_number}, machine {machine_no}. Setting previous_lot_averages to empty.")
                        # logging.info(f"prev_key: {prev_key}, prev_lot_avg: {prev_lot_avg}")

                        lot_doc = {
                            "modelno": model_number,
                            "machine": machine_no,
                            "lot_id": unique_lot_id_current,
                            "timestamp": some_function()
                        }
                        try:
                            if not lot_tbm_collect.find_one({"lot_id": unique_lot_id_current}):
                                lot_tbm_collect.insert_one(lot_doc)
                                output_messages.append(f"Inserted lot_id {unique_lot_id_current} into lot_tbm_collect")
                        except Exception as e:
                            logging.error(f"Error inserting lot_id {unique_lot_id_current} into lot_tbm_collect: {e}")
                            output_messages.append(f"❌ Error inserting lot_id {unique_lot_id_current}: {e}")

                        output_messages.append(f"Creating TBM Lot: lot_id={unique_lot_id_current}, tbm_machine_model_lot_id={current_tbm_machine_model_lot_id}, tbm_previous_machine_model_lot_id={tbm_previous_machine_model_lot_id}")

                        for param in numeric_params:
                            if param in batch_data.columns:
                                batch_data[param] = pd.to_numeric(batch_data[param], errors='coerce')

                        processed_machine_barcodes = batch_size
                        balance_machine_barcodes = total_tbm_barcodes - (barcode_counter + batch_size)
                        balance_barcodes = total_barcodes_model - (barcode_counter + batch_size)

                        lot_averages = {}
                        for param in numeric_params:
                            if param in batch_data.columns:
                                valid_vals = batch_data[param].dropna()
                                if not valid_vals.empty:
                                    lot_averages[param] = float(valid_vals.mean())
                                else:
                                    lot_averages[param] = None
                            else:
                                lot_averages[param] = None

                        lot_averages.update({
                            "total_rank_yield": total_rank_yield(batch_data),
                            "rfv_yield": rfv_yield(batch_data),
                            "rfh1_yield": rfh1_yield(batch_data),
                            "lfv_yield": lfv_yield(batch_data),
                            "conicity_yield": conicity_yield(batch_data),
                            "ufm_yield": ufm_yield(batch_data),
                            "rorank_yield": rorank_yield(batch_data),
                            "radial_runout_yield": radial_runout_yield(batch_data),
                            "radial_runout_first_harmonic_yield": radial_runout_first_harmonic_yield(batch_data),
                            "lateral_run_out_yield": lateral_run_out_yield(batch_data),
                            "lateral_run_out_top_over_yield": lateral_run_out_top_over(batch_data),
                            "lateral_run_out_bottom_over_yield": lateral_run_out_bottom_over(batch_data),
                            "lateral_run_out_harmonic_yield": lateral_run_out_harmonic_yield(batch_data),
                            "lateral_run_out_top_first_harmonic_yield": lateral_run_out_top_first_harmonic(batch_data),
                            "lateral_run_out_bottom_first_harmonic_yield": lateral_run_out_bottom_first_harmonic(batch_data),
                            "bulge_yield": bulge_yield(batch_data),
                            "bulge_top_over_yield": bulge_top_over(batch_data),
                            "bulge_bottom_over_yield": bulge_bottom_over(batch_data),
                            "dent_yield": dent_yield(batch_data),
                            "dent_top_over_yield": dent_top_over(batch_data),
                            "dent_bottom_over_yield": dent_bottom_over(batch_data),
                            "balancing_yield": balancing_yield(batch_data),
                            "upper_yield": upper_yield(batch_data),
                            "lower_yield": lower_yield(batch_data),
                            "upper_lower_yield": upper_lower_yield(batch_data),
                            "static_yield": static_yield(batch_data),
                            "warehouse_yield": warehouse_yield(batch_data),
                            "visual_yield": visual_yield(batch_data)
                        })

                        grouped_averages = {}
                        param_spec_map = {
                            "con_kgf": spec_con_kgf,
                            "cwrfv_oa_kgf": spec_cwrfv_oa_kgf,
                            "ccwrfv_oa_kgf": spec_ccwrfv_oa_kgf,
                            "cwrfv_oa1h_kgf": spec_cwrfv_oa1h_kgf,
                            "ccwrfv_oa1h_kgf": spec_ccwrfv_oa1h_kgf,
                            "lfv_oa_kgf": spec_lfv_oa_kgf,
                            "lfv_oa_1_kgf": spec_lfv_oa_1_kgf,
                            "upper_g": spec_upper_g,
                            "lower_g": spec_lower_g,
                            "up_low_g": spec_up_low_g,
                            "static_g": spec_static,
                            "load1": test_load,
                            "airpress1": pressure,

                        ############### Add 16-07 start ######################

                            "total_rank_yield": total_rank_yield_spec_yield,
                            "rfv_yield": rfv_yield_spec_yield,
                            "rfh1_yield": rfh1_yield_spec_yield,
                            "lfv_yield": lfv_yield_spec_yield,
                            "conicity_yield": conicity_yield_spec_yield,
                            "ufm_yield": ufm_yield_spec_yield,
                            "rorank_yield": rorank_yield_spec_yield,
                            "radial_runout_yield": radial_runout_yield_spec_yield,
                            "radial_runout_first_harmonic_yield": radial_runout_first_harmonic_yield_spec_yield,
                            "lateral_run_out_yield": lateral_run_out_yield_spec_yield,
                            "lateral_run_out_top_over_yield": lateral_run_out_top_over_yield_spec_yield,
                            "lateral_run_out_bottom_over_yield": lateral_run_out_bottom_over_yield_spec_yield,
                            "lateral_run_out_harmonic_yield": lateral_run_out_harmonic_yield_spec_yield,
                            "lateral_run_out_top_first_harmonic_yield": lateral_run_out_top_first_harmonic_yield_spec_yield,
                            "lateral_run_out_bottom_first_harmonic_yield": lateral_run_out_bottom_first_harmonic_yield_spec_yield,
                            "bulge_yield": bulge_yield_spec_yield,
                            "bulge_top_over_yield": bulge_top_over_yield_spec_yield,
                            "bulge_bottom_over_yield": bulge_bottom_over_yield_spec_yield,
                            "dent_yield": dent_yield_spec_yield,
                            "dent_top_over_yield": dent_top_over_yield_spec_yield,
                            "dent_bottom_over_yield": dent_bottom_over_yield_spec_yield,
                            "balancing_yield": balancing_yield_spec_yield,
                            "upper_yield": upper_yield_spec_yield,
                            "lower_yield": lower_yield_spec_yield,
                            "upper_lower_yield": upper_lower_yield_spec_yield,
                            "static_yield": static_yield_spec_yield,
                            "warehouse_yield": warehouse_yield_spec_yield,
                            "visual_yield": visual_yield_spec_yield

                        ############### Add 16-07 end ######################
                        }
                        try:
                            batch_data["ubno"] = batch_data["ubno"].astype(str).str.strip()
                            batch_data.sort_values("date_and_time", ascending=True, inplace=True)
                            ubno_grouped = batch_data.groupby("ubno")
                            ubno_records = []
                            for ubno, group in ubno_grouped:
                                group.sort_values("date_and_time", ascending=True)
                                record = {
                                    "ubno": ubno,
                                    "barcode_count": len(group["barcode"]) if "barcode" in group.columns else 0,
                                    "tub_date_start_time": group["date_and_time"].iloc[0] if not group["date_and_time"].empty else None,
                                    "tub_date_end_time": group["date_and_time"].iloc[-1] if not group["date_and_time"].empty else None,
                                    "barcodes": group["barcode"].tolist() if "barcode" in group.columns else []
                                }
                                parameters = []
                                for param in numeric_params:
                                    try:
                                        if param in group.columns:
                                            valid_vals = group[param].dropna()
                                            avg_val = float(valid_vals.mean()) if not valid_vals.empty else None
                                        else:
                                            avg_val = None
                                        record[f"avg_{param}"] = avg_val
                                        spec = param_spec_map.get(param)
                                        exceeded = False
                                        if param == "con_kgf" and spec_con_kgf is not None and avg_val is not None:
                                            exceeded = not (-spec_con_kgf <= avg_val <= spec_con_kgf)
                                        if param == "load1" and test_load is not None and test_load_tolerance is not None and avg_val is not None:
                                            exceeded = not (test_load - test_load_tolerance <= avg_val <= test_load + test_load_tolerance)
                                        if param == "airpress1" and pressure is not None and pressure_tolerance is not None and avg_val is not None:
                                            exceeded = not (pressure - pressure_tolerance <= avg_val <= pressure + pressure_tolerance)
                                        if spec is not None and param not in ["con_kgf", "load1", "airpress1"] and avg_val is not None:
                                            exceeded = avg_val > spec
                                        parameters.append({
                                            "parameter": param,
                                            "avg_value": avg_val,
                                            "spec_value": spec,
                                            "spec_threshold_exceeded": bool(exceeded)
                                        })
                                    except Exception as e:
                                        logging.info("[ERROR] Processing numeric param %s: %s", param, e)
                                        record[f"avg_{param}"] = None
                                        parameters.append({
                                            "parameter": param,
                                            "avg_value": None,
                                            "spec_value": param_spec_map.get(param),
                                            "spec_threshold_exceeded": False
                                        })
                                for param in yield_params:
                                    try:
                                        yield_func = globals()[param]
                                        avg_val = yield_func(group)
                                        record[param] = avg_val
                                        spec = param_spec_map.get(param)
                                        exceeded = avg_val < spec if spec is not None and avg_val is not None else False
                                        parameters.append({
                                            "parameter": param,
                                            "avg_value": avg_val,
                                            "spec_value": spec,
                                            "spec_threshold_exceeded": bool(exceeded)
                                        })
                                    except Exception as e:
                                        record[param] = None
                                        parameters.append({
                                            "parameter": param,
                                            "avg_value": None,
                                            "spec_value": param_spec_map.get(param),
                                            "spec_threshold_exceeded": False
                                        })
                                record["parameter_list"] = parameters
                                ubno_records.append(record)
                            grouped_averages["ubno"] = ubno_records
                        except Exception as e:
                            logging.error(f"Error processing ubno grouped averages: {e}")
                            grouped_averages["ubno"] = []

                        batch_data1 = batch_data.copy()
                        batch_data.sort_values("date_of_tbm", ascending=True, inplace=True)
                        batch_data1.sort_values("date_and_time", ascending=True, inplace=True)
                        machine_lot_start_build_time = batch_data["date_of_tbm"].iloc[0]
                        machine_lot_end_build_time = batch_data["date_of_tbm"].iloc[-1]
                        tub_date_start_time = batch_data1["date_and_time"].iloc[0]
                        tub_date_end_time = batch_data1["date_and_time"].iloc[-1]
                        lot_start_time = some_function()
                        lot_end_time = some_function()

                        parameter_alerts = []
                        overall_shift_occurred = False
                        prev_key = (model_number, machine_no, tbm_previous_machine_model_lot_id)
                        prev_lot_avg = machine_lot_avg_map.get(prev_key, None)
                        # When prev_lot_avg exists
                        if prev_lot_avg:
                            for param in numeric_params + yield_params:
                                shift = False
                                shift_occurred_spec = False
                                current_val = lot_averages.get(param)
                                prev_val = prev_lot_avg.get(param)
                                if current_val is not None and prev_val is not None:
                                    if param == "con_kgf" and spec_con_kgf is not None:
                                        if abs(current_val - prev_val) > threshold_value_con_kgf:
                                            shift = True
                                        if not (-spec_con_kgf <= current_val <= spec_con_kgf):
                                            shift_occurred_spec = True
                                    if param == "cwrfv_oa_kgf" and spec_cwrfv_oa_kgf is not None:
                                        if (current_val - prev_val) > threshold_value_cwrfv_oa_kgf:
                                            shift = True
                                        if current_val > spec_ccwrfv_oa_kgf:
                                            shift_occurred_spec = True
                                    if param == "ccwrfv_oa_kgf" and spec_ccwrfv_oa_kgf is not None:
                                        if (current_val - prev_val) > threshold_value_ccwrfv_oa_kgf:
                                            shift = True
                                        if current_val > spec_ccwrfv_oa_kgf:
                                            shift_occurred_spec = True
                                    if param == "cwrfv_oa1h_kgf" and spec_cwrfv_oa1h_kgf is not None:
                                        if (current_val - prev_val) > threshold_value_cwrfv_oa1h_kgf:
                                            shift = True
                                        if current_val > spec_cwrfv_oa1h_kgf:
                                            shift_occurred_spec = True
                                    if param == "ccwrfv_oa1h_kgf" and spec_ccwrfv_oa1h_kgf is not None:
                                        if (current_val - prev_val) > threshold_value_ccwrfv_oa1h_kgf:
                                            shift = True
                                        if current_val > spec_ccwrfv_oa1h_kgf:
                                            shift_occurred_spec = True
                                    if param == "lfv_oa_kgf" and spec_lfv_oa_kgf is not None:
                                        if (current_val - prev_val) > threshold_value_lfv_oa_kgf:
                                            shift = True
                                        if current_val > spec_lfv_oa_kgf:
                                            shift_occurred_spec = True
                                    if param == "lfv_oa_1_kgf" and spec_lfv_oa_1_kgf is not None:
                                        if (current_val - prev_val) > threshold_value_lfv_oa_1_kgf:
                                            shift = True
                                        if current_val > spec_lfv_oa_1_kgf:
                                            shift_occurred_spec = True
                                    if param == "upper_g" and spec_upper_g is not None:
                                        if (current_val - prev_val) > threshold_value_bal_upper_g:
                                            shift = True
                                        if current_val > spec_upper_g:
                                            shift_occurred_spec = True
                                    if param == "lower_g" and spec_lower_g is not None:
                                        if (current_val - prev_val) > threshold_value_bal_lower_g:
                                            shift = True
                                        if current_val > spec_lower_g:
                                            shift_occurred_spec = True
                                    if param == "up_low_g" and spec_up_low_g is not None:
                                        if (current_val - prev_val) > threshold_value_bal_up_low_g:
                                            shift = True
                                        if current_val > spec_up_low_g:
                                            shift_occurred_spec = True
                                    if param == "static_g" and spec_static is not None:
                                        if (current_val - prev_val) > threshold_value_bal_static_g:
                                            shift = True
                                        if current_val > spec_static:
                                            shift_occurred_spec = True
                                    
                                    ############### Add 16-07 start ######################

                                    # if param in yield_params:
                                    #     spec_yield = param_spec_map.get(param)
                                    #     if spec_yield is not None:
                                    #         if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield)):
                                    #             shift = True
                                    #         if abs(current_val < spec_yield):
                                    #             shift_occurred_spec = True

                                    ############### Add 16-07 end ######################

                                    if param == "total_rank_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_total_rank_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "rfv_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_rfv_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "rfh1_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_rfh1_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "lfv_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_lfv_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "conicity_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_conicity_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "ufm_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_ufm_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "rorank_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_rorank_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "radial_runout_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_radial_runout_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "radial_runout_first_harmonic_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_radial_runout_first_harmonic_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "lateral_run_out_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_lateral_run_out_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "lateral_run_out_top_over_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_lateral_run_out_top_over_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "lateral_run_out_bottom_over_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_lateral_run_out_bottom_over_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "lateral_run_out_harmonic_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_lateral_run_out_harmonic_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "lateral_run_out_top_first_harmonic_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_lateral_run_out_top_first_harmonic_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "lateral_run_out_bottom_first_harmonic_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_lateral_run_out_bottom_first_harmonic_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "bulge_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_bulge_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "bulge_top_over_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_bulge_top_over_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "bulge_bottom_over_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_bulge_bottom_over_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "dent_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_dent_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "dent_top_over_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_dent_top_over_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "dent_bottom_over_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_dent_bottom_over_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "balancing_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_balancing_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "upper_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_upper_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "lower_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_lower_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "upper_lower_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_upper_lower_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "static_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_static_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "warehouse_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_warehouse_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "visual_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_visual_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True

                                    if param == "load1" and test_load is not None and test_load_tolerance is not None:
                                        if abs(current_val - prev_val) > test_load_tolerance:
                                            shift = True
                                        if not (test_load - test_load_tolerance <= current_val <= test_load + test_load_tolerance):
                                            shift_occurred_spec = True
                                    if param == "airpress1" and pressure is not None and pressure_tolerance is not None:
                                        if abs(current_val - prev_val) > pressure_tolerance:
                                            shift = True
                                        if not (pressure - pressure_tolerance <= current_val <= pressure + pressure_tolerance):
                                            shift_occurred_spec = True
                                shift_occurred = shift or shift_occurred_spec
                                spec_value = param_spec_map.get(param)
                                spec_diff = (current_val - spec_value if spec_value is not None and current_val is not None and isinstance(spec_value, (int, float)) and isinstance(current_val, (int, float)) else None)
                                alert_dict = {
                                    "parameter": param,
                                    "current_value": current_val,
                                    "previous_value": prev_val,
                                    "difference": current_val - prev_val if current_val is not None and prev_val is not None else None,
                                    "shift_occurred_thershold": shift,
                                    "spec_value": spec_value,
                                    "spec_difference": spec_diff,
                                    "shift_occurred_spec": shift_occurred_spec,
                                    "shift_occurred": shift_occurred
                                }
                                if param == "load1":
                                    alert_dict["test_load_tolerance"] = test_load_tolerance
                                if param == "airpress1":
                                    alert_dict["pressure_tolerance"] = pressure_tolerance
                                parameter_alerts.append(alert_dict)
                                if shift_occurred:
                                    overall_shift_occurred = True
                        else:
                            for param in numeric_params + yield_params:
                                shift_occurred_thershold = False
                                shift_occurred_spec = False
                                current_val = lot_averages.get(param)
                                if current_val is not None:
                                    if param == "con_kgf" and spec_con_kgf is not None:
                                        if not (-spec_con_kgf <= current_val <= spec_con_kgf):
                                            shift_occurred_spec = True
                                    if param == "cwrfv_oa_kgf" and spec_cwrfv_oa_kgf is not None:
                                        if current_val > spec_cwrfv_oa_kgf:
                                            shift_occurred_spec = True
                                    if param == "ccwrfv_oa_kgf" and spec_ccwrfv_oa_kgf is not None:
                                        if current_val > spec_ccwrfv_oa_kgf:
                                            shift_occurred_spec = True
                                    if param == "cwrfv_oa1h_kgf" and spec_cwrfv_oa1h_kgf is not None:
                                        if current_val > spec_cwrfv_oa1h_kgf:
                                            shift_occurred_spec = True
                                    if param == "ccwrfv_oa1h_kgf" and spec_ccwrfv_oa1h_kgf is not None:
                                        if current_val > spec_ccwrfv_oa1h_kgf:
                                            shift_occurred_spec = True
                                    if param == "lfv_oa_kgf" and spec_lfv_oa_kgf is not None:
                                        if current_val > spec_lfv_oa_kgf:
                                            shift_occurred_spec = True
                                    if param == "lfv_oa_1_kgf" and spec_lfv_oa_1_kgf is not None:
                                        if current_val > spec_lfv_oa_1_kgf:
                                            shift_occurred_spec = True
                                    if param == "upper_g" and spec_upper_g is not None:
                                        if current_val > spec_upper_g:
                                            shift_occurred_spec = True
                                    if param == "lower_g" and spec_lower_g is not None:
                                        if current_val > spec_lower_g:
                                            shift_occurred_spec = True
                                    if param == "up_low_g" and spec_up_low_g is not None:
                                        if current_val > spec_up_low_g:
                                            shift_occurred_spec = True
                                    if param == "static_g" and spec_static is not None:
                                        if current_val > spec_static:
                                            shift_occurred_spec = True
                                
                                ############### Add 16-07 start ######################

                                    if param in yield_params:
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if current_val < spec_yield:
                                                shift_occurred_spec = True
                                
                                ############### Add 16-07 end ######################

                                    if param == "load1" and test_load is not None and test_load_tolerance is not None:
                                        if not (test_load - test_load_tolerance <= current_val <= test_load + test_load_tolerance):
                                            shift_occurred_spec = True
                                    if param == "airpress1" and pressure is not None and pressure_tolerance is not None:
                                        if not (pressure - pressure_tolerance <= current_val <= pressure + pressure_tolerance):
                                            shift_occurred_spec = True
                                shift_occurred = shift_occurred_thershold or shift_occurred_spec
                                spec_value = param_spec_map.get(param)
                                spec_diff = (current_val - spec_value if spec_value is not None and current_val is not None and isinstance(spec_value, (int, float)) and isinstance(current_val, (int, float)) else None)
                                alert_dict = {
                                    "parameter": param,
                                    "current_value": current_val,
                                    "previous_value": None,
                                    "difference": None,
                                    "shift_occurred_thershold": shift_occurred_thershold,
                                    "spec_value": spec_value,
                                    "spec_difference": spec_diff,
                                    "shift_occurred_spec": shift_occurred_spec,
                                    "shift_occurred": shift_occurred
                                }
                                if param == "load1":
                                    alert_dict["test_load_tolerance"] = test_load_tolerance
                                if param == "airpress1":
                                    alert_dict["pressure_tolerance"] = pressure_tolerance
                                parameter_alerts.append(alert_dict)
                                if shift_occurred:
                                    overall_shift_occurred = True

                        shift_alerts = [alert for alert in parameter_alerts if alert["shift_occurred"]]
                        logging.debug(f"Model {model_number}, Machine {machine_no}, Lot {current_tbm_machine_model_lot_id}: {len(shift_alerts)} parameter alerts with shift_occurred=True")
                        if shift_alerts:
                            logging.debug(f"Shift alerts: {[alert['parameter'] for alert in shift_alerts]}")

                        overall_shift_occurred = bool(overall_shift_occurred)

                        # NEW: Filter parameters for derived alerts
                        deviated_parameter_alerts = [
                            alert for alert in parameter_alerts
                            if alert.get("shift_occurred_thershold", False) or alert.get("shift_occurred_spec", False)
                        ]

                        # Add rank counts to deviated parameter alerts
                        for alert in deviated_parameter_alerts:
                            param = alert.get("parameter", "")
                            if param.endswith("_yield"):
                                if param in rank_param_mappings:
                                    rank_counts = {}
                                    for col in rank_param_mappings[param]:
                                        if col in batch_data.columns:
                                            value_counts = batch_data[col].value_counts().to_dict()
                                            for rank_value, count in value_counts.items():
                                                rank_value = int(rank_value) if isinstance(rank_value, (int, float)) else rank_value
                                                rank_counts[f"{col}_{rank_value}"] = int(count)
                                    alert["rank_counts"] = rank_counts

                        if deviated_parameter_alerts:
                            deviated_doc = {
                                "alert_id_tbm_ufm": last_alert_id + 1,
                                "modelno": model_number,
                                "lot_id": unique_lot_id_current,
                                "machine": machine_no,
                                "comments": [],
                                "alert_assign_user": [],  # Add 05-07
                                "tbm_machine_model_lot_id": current_tbm_machine_model_lot_id,
                                "tbm_previous_machine_model_lot_id": tbm_previous_machine_model_lot_id,
                                "parameter_alerts": deviated_parameter_alerts,
                                "timestamp_alert_generated": some_function()
                            }
                            deviated_triggering_bulk_operations.append(pymongo.InsertOne(deviated_doc))
                            logging.info(f"Prepared {len(deviated_parameter_alerts)} derived alerts for machine {machine_no}, lot {unique_lot_id_current}")

                        # Do not filter; include all parameters (both yield and non-yield)
                        parameter_alerts = [
                        alert for alert in parameter_alerts
                        if alert.get("parameter", "").endswith("_yield") or not alert.get("parameter", "").endswith("_yield")
                        ]

                        # Add rank counts only for alerts:
                        # - where a shift occurred (threshold or spec)
                        # - and parameter name ends with '_yield'
                        for alert in parameter_alerts:
                            param = alert.get("parameter", "")
                            if (
                                (alert.get("shift_occurred_thershold", False) or alert.get("shift_occurred_spec", False))
                                and param.endswith("_yield")
                            ):
                                if param in rank_param_mappings:
                                    rank_counts = {}
                                    for col in rank_param_mappings[param]:
                                        if col in batch_data.columns:
                                            value_counts = batch_data[col].value_counts().to_dict()
                                            for rank_value, count in value_counts.items():
                                                rank_value = int(rank_value) if isinstance(rank_value, (int, float)) else rank_value
                                                rank_counts[f"{col}_{rank_value}"] = int(count)
                                    alert["rank_counts"] = rank_counts

                        # ------------------------------------------------------------------
                        #  NEW (correct – one entry per **continuous** shift block)
                        # ------------------------------------------------------------------
                        batch_data["tbm_shift"] = batch_data["date_of_tbm"].apply(get_shift)

                        # Sort by date_of_tbm to guarantee chronological order
                        batch_data = batch_data.sort_values("date_of_tbm").reset_index(drop=True)

                        shifts_data_list = []
                        current_shift = None
                        current_start = None
                        current_barcodes = []

                        for idx, row in batch_data.iterrows():
                            this_shift = row["tbm_shift"]
                            this_dt    = row["date_of_tbm"]
                            this_bc    = row.get("barcode")

                            if this_shift != current_shift or current_shift is None:
                                # finish previous block (if any)
                                if current_shift is not None:
                                    shifts_data_list.append({
                                        "shift": current_shift,
                                        "barcode_count": len(current_barcodes),
                                        "machine_shift_tbm_start_time": current_start,
                                        "machine_shift_tbm_end_time":   batch_data.at[idx-1, "date_of_tbm"],
                                        "barcodes": current_barcodes[:]
                                    })
                                # start new block
                                current_shift   = this_shift
                                current_start   = this_dt
                                current_barcodes = [this_bc] if this_bc is not None else []
                            else:
                                current_barcodes.append(this_bc if this_bc is not None else None)

                        # do NOT forget the last block
                        if current_shift is not None:
                            shifts_data_list.append({
                                "shift": current_shift,
                                "barcode_count": len(current_barcodes),
                                "machine_shift_tbm_start_time": current_start,
                                "machine_shift_tbm_end_time":   batch_data["date_of_tbm"].iloc[-1],
                                "barcodes": current_barcodes
                            })

                        any_shift_flag = any(alert["shift_occurred"] for alert in parameter_alerts)

                        averages_doc = {
                            "modelno": model_number,
                            "lot_id": unique_lot_id_current,
                            "previous_lot_id": previous_lot_id,
                            "machine": machine_no,
                            "tbm_machine_model_lot_id": current_tbm_machine_model_lot_id,
                            "tbm_previous_machine_model_lot_id": tbm_previous_machine_model_lot_id,
                            "current_lot_averages": lot_averages,
                            "grouped_averages": grouped_averages,
                            "previous_lot_averages": prev_lot_avg,
                            "parameter_alerts": parameter_alerts,
                            "tbm_shift_occurred": overall_shift_occurred,
                            "processed_machine_barcodes": int(processed_machine_barcodes),
                            "target_machine_lot_size": target_lot_size,
                            "balance_machine_barcodes": int(balance_machine_barcodes),
                            "model_processed_barcodes": int(batch_size),
                            "model_target_lot_size": target_lot_size,
                            "model_balance_barcodes": int(balance_barcodes),
                            "model_total_barcodes": int(total_barcodes_model),
                            "batch_indices": list(batch_indices),
                            "timestamp_alert_generated": some_function()
                        }
                        averages_query = {
                            "modelno": model_number,
                            "lot_id": unique_lot_id_current,
                            "tbm_machine_model_lot_id": current_tbm_machine_model_lot_id
                        }
                        if not averages_collect.find_one(averages_query):
                            averages_bulk_operations.append(pymongo.InsertOne(averages_doc))

                        current_key = (model_number, machine_no, current_tbm_machine_model_lot_id)
                        machine_lot_avg_map[current_key] = lot_averages
                        tbm_previous_machine_model_lot_ids[(model_number, machine_no)] = current_tbm_machine_model_lot_id

                        machine_alert = {
                            "machine": machine_no,
                            "tbm_shift_occurred": any_shift_flag,
                            "shifts_data": shifts_data_list,
                            "ufm_yield": ufm_yield(batch_data),
                            "rorank_yield": rorank_yield(batch_data),
                            "balancing_yield": balancing_yield(batch_data),
                            "total_rank_yield": total_rank_yield(batch_data),
                            "visual_yield": visual_yield(batch_data),
                            "sizecode": sizecode,
                            "gt_code": str(gt_code),    # Add 16-07
                            "timestamp_lot_started": lot_start_time,
                            "timestamp_lot_ended": lot_end_time,
                            "target_machine_lot_size": target_lot_size,
                            "processed_machine_barcodes": int(processed_machine_barcodes),
                            "balance_machine_barcodes": int(balance_machine_barcodes),
                            "total_machine_barcodes": int(total_tbm_barcodes),
                            "machine_lot_start_build_time": machine_lot_start_build_time,
                            "machine_lot_end_build_time": machine_lot_end_build_time,
                            "tub_date_start_time": tub_date_start_time,
                            "tub_date_end_time": tub_date_end_time,
                            "parameter_alerts": parameter_alerts,
                            "lot_averages": lot_averages,
                            "grouped_averages": grouped_averages,
                            "previous_lot_averages": machine_lot_avg_map.get((model_number, machine_no, tbm_previous_machine_model_lot_id), {}),
                            "first_barcode_machine": batch_data.iloc[0]["barcode"] if "barcode" in batch_data.columns else None,
                            "last_barcode_machine": batch_data.iloc[-1]["barcode"] if "barcode" in batch_data.columns else None,
                            "tbm_machine_model_lot_id": current_tbm_machine_model_lot_id,
                            "tbm_previous_machine_model_lot_id": tbm_previous_machine_model_lot_id,
                            "rank_columns": get_rank_columns(batch_data),
                            "barcodes": batch_data["barcode"].tolist() if "barcode" in batch_data.columns else []
                        }

                        alert_key = (model_number, unique_lot_id_current)
                        if alert_key not in alert_map:
                            last_alert_id += 1
                            alert_map[alert_key] = {
                                "alert_id_tbm_ufm": last_alert_id,
                                "modelno": model_number,
                                "lot_id": unique_lot_id_current,
                                "machine_alerts": [],
                                "first_barcode_model": None,
                                "last_barcode_model": None,
                                "shift_occurred": False
                            }
                        alert_map[alert_key]["machine_alerts"].append(machine_alert)
                        if machine_alert["tbm_shift_occurred"]:
                            alert_map[alert_key]["shift_occurred"] = True
                        if alert_map[alert_key]["first_barcode_model"] is None:
                            alert_map[alert_key]["first_barcode_model"] = machine_alert["first_barcode_machine"]
                        alert_map[alert_key]["last_barcode_model"] = machine_alert["last_barcode_machine"]
                        if overall_shift_occurred:
                            merge_df.loc[batch_indices, "alert_id_tbm_ufm"] = last_alert_id
                        barcode_counter += batch_size

                for alert_key, alert_obj in alert_map.items():
                    final_alert = {
                        "alert_id_tbm_ufm": alert_obj["alert_id_tbm_ufm"],
                        "modelno": alert_obj["modelno"],
                        "lot_id": alert_obj["lot_id"],
                        "alert_flag": False,
                        "dashboard_flag": False,
                        "machine_flag": False,
                        "alert_status_check": "o",
                        "alert_status_timestamp": None,
                        "alert_status_comment": [],
                        "alert_assign_user": [],  # Add 05-07
                        "machine_alerts": alert_obj["machine_alerts"],
                        "first_barcode_model": alert_obj["first_barcode_model"],
                        "last_barcode_model": alert_obj["last_barcode_model"],
                        "timestamp_alert_generated": some_function()
                    }
                    alert_bulk_operations.append(pymongo.InsertOne(final_alert))
                    # === ADD THESE 2 LINES ONLY ===
                    triggering_bulk_operations.append(pymongo.InsertOne(final_alert))                    # → alert_db
                    alert_triggering_collect_dashboard.bulk_write([pymongo.InsertOne(final_alert)], ordered=False)  # → alert_db_dashboard
                    # ================================
                
                # === AFTER alert_map is built - INSERT VIRGIN & FINAL YIELD (UNCHANGED + NEW) ===
                for alert_key, alert_obj in alert_map.items():
                    # true_machine_alerts = [ma for ma in alert_obj["machine_alerts"] if ma.get("tbm_shift_occurred", False)]
                    true_machine_alerts = [ma for ma in alert_obj["machine_alerts"] if ma.get("tbm_shift_occurred", False) in [True, False]]  # Keep both True/False
                    if not true_machine_alerts:
                        continue

                    # Pre-compute barcode → row mapping
                    virgin_barcode_to_row = {row["barcode"]: row for _, row in virgin_data.iterrows()}
                    final_barcode_to_row = {row["barcode"]: row for _, row in final_data.iterrows()}

                    # === 1. VIRGIN YIELD: YOUR ORIGINAL CODE (100% UNCHANGED) ===
                    virgin_machine_alerts = []
                    virgin_all_barcodes = set()
                    for ma in true_machine_alerts:
                        original_barcodes = ma.get("barcodes", [])
                        if not original_barcodes:
                            continue
                        filtered_barcodes = []
                        for bc in original_barcodes:
                            bc_str = str(bc)
                            if bc_str in virgin_barcode_to_row and bc_str not in virgin_all_barcodes:
                                filtered_barcodes.append(bc_str)
                                virgin_all_barcodes.add(bc_str)
                        if filtered_barcodes:
                            virgin_ma = ma.copy()
                            virgin_ma["barcodes"] = filtered_barcodes
                            virgin_ma["barcode_count"] = len(filtered_barcodes)
                            virgin_machine_alerts.append(virgin_ma)

                    if virgin_machine_alerts and virgin_all_barcodes:
                        all_virgin_bcs = sorted(virgin_all_barcodes,
                            key=lambda x: virgin_data[virgin_data["barcode"] == x]["date_of_dt"].iloc[0])
                        doc_virgin = {
                            "alert_id_tbm_ufm": alert_obj["alert_id_tbm_ufm"],
                            "modelno": alert_obj["modelno"],
                            "lot_id": alert_obj["lot_id"],
                            "alert_flag": False,
                            "dashboard_flag": False,
                            "machine_flag": False,
                            "alert_status_check": "o",
                            "alert_status_timestamp": None,
                            "alert_status_comment": [],
                            "alert_assign_user": [],
                            "machine_alerts": virgin_machine_alerts,
                            "first_barcode_model": all_virgin_bcs[0] if all_virgin_bcs else None,
                            "last_barcode_model": all_virgin_bcs[-1] if all_virgin_bcs else None,
                            "timestamp_alert_generated": some_function(),
                            "yield_type": "virgin"
                        }
                        alert_triggering_virgin_yield_collect.insert_one(doc_virgin)  # UNCHANGED

                    # === 2. FINAL YIELD: YOUR ORIGINAL CODE (KEPT 100%) ===
                    final_machine_alerts = []
                    final_all_barcodes = set()
                    for ma in true_machine_alerts:
                        original_barcodes = ma.get("barcodes", [])
                        if not original_barcodes:
                            continue
                        filtered_barcodes = []
                        for bc in original_barcodes:
                            bc_str = str(bc)
                            if bc_str in final_barcode_to_row and bc_str not in final_all_barcodes:
                                filtered_barcodes.append(bc_str)
                                final_all_barcodes.add(bc_str)
                        if filtered_barcodes:
                            final_ma = ma.copy()
                            final_ma["barcodes"] = filtered_barcodes
                            final_ma["barcode_count"] = len(filtered_barcodes)
                            final_machine_alerts.append(final_ma)

                    if final_machine_alerts and final_all_barcodes:
                        all_final_bcs = sorted(final_all_barcodes,
                            key=lambda x: final_data[final_data["barcode"] == x]["date_of_dt"].iloc[0])

                        # === ORIGINAL INSERT (KEPT) ===
                        doc_final = {
                            "alert_id_tbm_ufm": alert_obj["alert_id_tbm_ufm"],
                            "modelno": alert_obj["modelno"],
                            "lot_id": alert_obj["lot_id"],
                            "alert_flag": False,
                            "dashboard_flag": False,
                            "machine_flag": False,
                            "alert_status_check": "o",
                            "alert_status_timestamp": None,
                            "alert_status_comment": [],
                            "alert_assign_user": [],
                            "machine_alerts": final_machine_alerts,
                            "first_barcode_model": all_final_bcs[0] if all_final_bcs else None,
                            "last_barcode_model": all_final_bcs[-1] if all_final_bcs else None,
                            "timestamp_alert_generated": some_function(),
                            "yield_type": "final"
                        }
                        alert_triggering_final_yield_collect.insert_one(doc_final)  # YOUR ORIGINAL - UNCHANGED

                        # === NEW: Insert into alert_table_final_yield (DEDICATED FINAL YIELD DB) ===
                        doc_final_only = doc_final.copy()
                        doc_final_only["tbm_shift_occurred"] = alert_obj.get("shift_occurred", False)
                        final_yield_only_bulk.append(pymongo.InsertOne(doc_final_only))
                        
                        # === NEW: Extract Deviated Parameters from alert_db_final_yield (alert_triggering_final_yield_collect) ===
                        deviated_params = []

                        # Fetch the already-saved Final Yield alert document using alert_id_tbm_ufm
                        existing_final_yield_doc = alert_triggering_final_yield_collect.find_one(
                            {"alert_id_tbm_ufm": alert_obj["alert_id_tbm_ufm"]},
                            {"machine_alerts.parameter_alerts": 1, "machine_alerts.barcodes": 1, "machine_alerts.machine": 1, "machine_alerts.tbm_machine_model_lot_id": 1, "machine_alerts.tbm_previous_machine_model_lot_id": 1}
                        )

                        if existing_final_yield_doc:
                            machine_alerts = existing_final_yield_doc.get("machine_alerts", [])
                            for ma in machine_alerts:
                                param_alerts = ma.get("parameter_alerts", [])
                                barcodes_in_ma = ma.get("barcodes", [])
                                machine_no_final = ma.get("machine", "unknown")
                                machine_tbm_machine_model_lot_id = ma.get("tbm_machine_model_lot_id", "unknoum")
                                machine_tbm_previous_machine_model_lot_id = ma.get("tbm_previous_machine_model_lot_id", None)

                                for alert in param_alerts:
                                    if alert.get("shift_occurred_thershold", False) or alert.get("shift_occurred_spec", False):
                                        param = alert.get("parameter", "")

                                        # Only add rank_counts for yield parameters
                                        if param.endswith("_yield") and param in rank_param_mappings and barcodes_in_ma:
                                            # Fetch actual tire data for these barcodes (last occurrence = Final Yield)
                                            tire_data = final_data[final_data["barcode"].isin(barcodes_in_ma)]
                                            rank_counts = {}

                                            for col in rank_param_mappings[param]:
                                                if col in tire_data.columns:
                                                    counts = tire_data[col].value_counts().to_dict()
                                                    for val, cnt in counts.items():
                                                        val = int(val) if isinstance(val, (int, float)) else val
                                                        rank_counts[f"{col}_{val}"] = int(cnt)
                                            alert["rank_counts"] = rank_counts

                                        deviated_params.append(alert)

                        # === Insert into alert_deviated_final_yield ===
                        if deviated_params:
                            deviated_doc = {
                                "alert_id_tbm_ufm": alert_obj["alert_id_tbm_ufm"],
                                "modelno": alert_obj["modelno"],
                                "lot_id": alert_obj["lot_id"],
                                "machine": machine_no_final if 'machine_no_final' in locals() else "unknown",
                                "comments": [],
                                "alert_assign_user": [],
                                "tbm_machine_model_lot_id": machine_tbm_machine_model_lot_id,
                                "tbm_previous_machine_model_lot_id": machine_tbm_previous_machine_model_lot_id,
                                "parameter_alerts": deviated_params,
                                "timestamp_alert_generated": some_function(),
                                "yield_type": "final",
                                "source": "alert_db_final_yield"  # Optional: for traceability
                            }
                            final_yield_deviated_only_bulk.append(pymongo.InsertOne(deviated_doc))

                        # # === NEW: Extract Deviated Parameters for alert_deviated_final_yield ===
                        # deviated_params = []
                        # for ma in final_machine_alerts:
                        #     for alert in ma.get("parameter_alerts", []):
                        #         if alert.get("shift_occurred_thershold", False) or alert.get("shift_occurred_spec", False):
                        #             param = alert.get("parameter", "")
                        #             if param.endswith("_yield") and param in rank_param_mappings:
                        #                 relevant_data = final_data[final_data["barcode"].isin(ma["barcodes"])]
                        #                 rank_counts = {}
                        #                 for col in rank_param_mappings[param]:
                        #                     if col in relevant_data.columns:
                        #                         counts = relevant_data[col].value_counts().to_dict()
                        #                         for val, cnt in counts.items():
                        #                             val = int(val) if isinstance(val, (int, float)) else val
                        #                             rank_counts[f"{col}_{val}"] = int(cnt)
                        #                 alert["rank_counts"] = rank_counts
                        #             deviated_params.append(alert)

                        # if deviated_params:
                        #     machine_no_final = final_machine_alerts[0]["machine"]
                        #     deviated_doc = {
                        #         "alert_id_tbm_ufm": alert_obj["alert_id_tbm_ufm"],
                        #         "modelno": alert_obj["modelno"],
                        #         "lot_id": alert_obj["lot_id"],
                        #         "machine": machine_no_final,
                        #         "comments": [],
                        #         "alert_assign_user": [],
                        #         "tbm_machine_model_lot_id": current_tbm_machine_model_lot_id,
                        #         "tbm_previous_machine_model_lot_id": tbm_previous_machine_model_lot_id,
                        #         "parameter_alerts": deviated_params,
                        #         "timestamp_alert_generated": some_function(),
                        #         "yield_type": "final"
                        #     }
                        #     final_yield_deviated_only_bulk.append(pymongo.InsertOne(deviated_doc))

        except Exception as e:
            logging.error(f"Error in model loop: {e}")
            continue

    # === BULK WRITE NEW DATABASES ===
    if final_yield_only_bulk:
        try:
            result = alert_final_yield_collect.bulk_write(final_yield_only_bulk, ordered=False)
            inserted_counts["alert_table_final_yield"] = result.inserted_count
            output_messages.append(f"Inserted {result.inserted_count} docs → alert_table_final_yield.alert_tbm_collect_name")
        except Exception as e:
            output_messages.append(f"Error alert_table_final_yield: {e}")

    if final_yield_deviated_only_bulk:
        try:
            result = alert_triggering_deviated_final_yield_collect.bulk_write(final_yield_deviated_only_bulk, ordered=False)
            inserted_counts["alert_deviated_final_yield"] = result.inserted_count
            output_messages.append(f"Inserted {result.inserted_count} deviated docs → alert_deviated_final_yield.alert_triggering_deviated_tbm")
        except Exception as e:
            output_messages.append(f"Error alert_deviated_final_yield: {e}")

    # Execute bulk operations
    if averages_bulk_operations:
        try:
            result = averages_collect.bulk_write(averages_bulk_operations, ordered=False)
            inserted_counts["averages_tbm_table"] = result.inserted_count
            output_messages.append(f"✅ TBM Inserted {result.inserted_count} averages documents into averages_tbm_table.")
        except Exception as e:
            error_message = f"❌ TBM Error performing bulk write to averages_tbm_table: {e}"
            output_messages.append(error_message)
    else:
        output_messages.append("No TBM averages documents to insert.")

    if alert_bulk_operations:
        try:
            result = alert_collect.bulk_write(alert_bulk_operations, ordered=False)
            inserted_counts["alert_tbm_table_with_lot_size"] = result.inserted_count
            output_messages.append(f"✅ TBM Inserted {result.inserted_count} alert documents into alert_tbm_table_with_lot_size.")
        except Exception as e:
            error_message = f"❌ TBM Error performing bulk write to alert_tbm_table_with_lot_size: {e}"
            output_messages.append(error_message)
    else:
        output_messages.append("No TBM alert documents to insert.")

    if triggering_bulk_operations:
        try:
            # Write to alert_db
            result = alert_triggering_collect.bulk_write(triggering_bulk_operations, ordered=False)
            inserted_counts["alert_triggering_tbm_with_lot_size"] = result.inserted_count
            output_messages.append(f"✅ TBM Inserted {result.inserted_count} machine_alert documents into alert_db.alert_triggering_tbm_with_lot_size.")
            # NEW: Write to alert_db_dashboard
            result_dashboard = alert_triggering_collect_dashboard.bulk_write(triggering_bulk_operations, ordered=False)
            inserted_counts["alert_triggering_tbm_dashboard"] = result_dashboard.inserted_count
            output_messages.append(f"✅ TBM Inserted {result_dashboard.inserted_count} machine_alert documents into alert_db_dashboard.alert_triggering_tbm_with_lot_size.")
        except Exception as e:
            error_message = f"❌ TBM Error performing bulk write to alert_triggering collections: {e}"
            output_messages.append(error_message)
    else:
        output_messages.append("No TBM machine_alert documents to insert into alert_triggering collections.")

    if deviated_triggering_bulk_operations:
        try:
            # Write to alert_deviated
            result = alert_triggering_deviated_collect.bulk_write(deviated_triggering_bulk_operations, ordered=False)
            inserted_counts["alert_tbm_with_lot_size"] = result.inserted_count
            output_messages.append(f"✅ TBM Inserted {result.inserted_count} deviated alert documents into alert_deviated.alert_tbm_with_lot_size.")
        except Exception as e:
            error_message = f"❌ TBM Error performing bulk write to deviated alert collections: {e}"
            output_messages.append(error_message)
    else:
        output_messages.append("No TBM deviated alert documents to insert into deviated alert collections.")

    bulk_updates = []
    try:
        for index, row in merge_df.iterrows():
            query = {"_id": row["_id"]}
            update = {
                "$set": {
                    "alert_id_tbm_ufm": row.get("alert_id_tbm_ufm"),
                    "lot_id": row.get("lot_id"),
                    "previous_lot_id": row.get("previous_lot_id"),
                    "tbm_machine_model_lot_id": row.get("tbm_machine_model_lot_id"),
                    "tbm_previous_machine_model_lot_id": row.get("tbm_previous_machine_model_lot_id")
                }
            }
            bulk_updates.append(pymongo.UpdateOne(query, update))
    except Exception as e:
        logging.error(f"Error preparing bulk updates for merge collection: {e}")
        output_messages.append(f"❌ Error preparing bulk updates: {e}")

    if bulk_updates:
        try:
            result = merge_collect.bulk_write(bulk_updates, ordered=False)
            updated_counts["data_mereged_dashboard"] = result.modified_count
            output_messages.append(f"✅ TBM Updated {result.modified_count} documents in data_mereged_dashboard.")
        except Exception as e:
            error_message = f"❌ TBM Error updating data_mereged_dashboard: {e}"
            output_messages.append(error_message)
    else:
        output_messages.append("No TBM documents to update in data_mereged_dashboard.")

    end_time = time.time()
    execution_time = end_time - start_time
    final_message = f"✅ TBM Lot and Alert Process completed successfully in {execution_time:.2f} seconds! Virgin & Final Yield alerts saved."
    output_messages.append(final_message)
    logging.info(final_message)

    return {
        "status": "success",
        "message": final_message,
        "output_log": output_messages,
        "inserted_counts": inserted_counts,
        "updated_counts": updated_counts,
        "execution_time": execution_time
    }, 200

def process_curing_data_and_generate_lots_alerts():
    start_time = time.time()
    output_messages = []
    inserted_counts = {}
    updated_counts = {}
    logging.info("process_curing_data_and_generate_lots_alerts function is just started")

    try:
        merge_collect = client[TUB_CIMCORP_DB][cimcorp_collection_name]
        channel_collect_curing = client[config_databse][channel_no_collect_curing]
        channel_collect_ufm = client[config_databse][channel_no_collect]
        lot_curing_collect = client[lot_db][lot_curing_collect_name]
        alert_collect = client[alert_table][alert_curing_collect_name]
        averages_collect = client[average_db][averages_curing_table_name]
        machine_collect = client[config_databse][machine_no_collect]
        curing_press_collect = client[config_databse][curing_press_dict_collect]
        alert_triggering_collect = client[alert_db][alert_triggering_curing_collect_name]
        alert_triggering_deviated_collect = client[alert_deviated][alert_triggering_deviated_curing]
        # === NEW: Virgin Yield & Final Yield Collections ===
        alert_triggering_virgin_yield_collect = client[alert_db_virgin_yield][alert_triggering_curing_collect_name]
        alert_triggering_final_yield_collect = client[alert_db_final_yield][alert_triggering_curing_collect_name]
        # === NEW: Dedicated Final Yield Only Databases (YOUR REQUEST) ===
        alert_final_yield_collect = client["alert_table_final_yield"][alert_curing_collect_name]  # Only Final Yield
        alert_triggering_deviated_final_yield_collect = client["alert_deviated_final_yield"][alert_triggering_deviated_curing]  # Deviated from Final Yield

        alert_triggering_collect_dashboard_curing = client[alert_db_dashboard][alert_triggering_curing_collect_name]
        curing_collect_data = client[model_db][curing_collection]
        curing_modelno_collect = client[model_db][curing_collection_machine]
        output_messages.append("✅ TBM Collections loaded successfully for lot and alert processing in both alert_db and alert_db_dashboard, and model_workcenter_db.")
    except pymongo.errors.OperationFailure as e:
        error_message = f"❌ Curing Database or collection error: {e}"
        output_messages.append(error_message)
        return {"status": "error", "message": error_message, "output_log": output_messages}, 500

    try:
        active_curing_press_ids = set(curing_modelno_collect.distinct("curing_press_id", {"status": "active"}))
        output_messages.append(f"✅ Fetched {len(active_curing_press_ids)} active curing_press_id values: {active_curing_press_ids}")
    except Exception as e:
        error_message = f"❌ Error fetching active curing_press_id from curing_modelno: {e}"
        output_messages.append(error_message)
        return {"status": "error", "message": "Failed to fetch active curing_press_id", "output_log": output_messages}, 500

    try:
        active_modelnos = set(curing_collect_data.distinct("modelno", {"status": "active"}))
        output_messages.append(f"✅ Fetched {len(active_modelnos)} active model numbers: {active_modelnos}")
    except Exception as e:
        output_messages.append(f"❌ Error fetching active model numbers: {e}")
        return {"status": "error", "message": "Failed to fetch active model numbers", "output_log": output_messages}, 500

    try:
        merge1 = list(merge_collect.find({}))
        merge_df = pd.DataFrame(merge1)
        channel_data_curing = list(channel_collect_curing.find({"modelno": {"$in": list(active_modelnos)}}))
        channel_data_curing = list(channel_collect_curing.find({}))
        channel_df = pd.DataFrame(channel_data_curing)
        channel_data_ufm = list(channel_collect_ufm.find({}))
        channel_df_ufm = pd.DataFrame(channel_data_ufm)
        machine_data = list(machine_collect.find({}))
        machine_df = pd.DataFrame(machine_data)
        curing_press_data = list(curing_press_collect.find({"curing_press_id": {"$in": list(active_curing_press_ids)}}))
        curing_press_data = list(curing_press_collect.find({}))
        curing_df = pd.DataFrame(curing_press_data)
        output_messages.append("✅ Curing Data loaded from collections into DataFrames.")
    except Exception as e:
        error_message = f"❌ Curing Error loading data: {e}"
        output_messages.append(error_message)
        return {"status": "error", "message": error_message, "output_log": output_messages}, 500

    # MODIFIED: Check for tbm column before adding tbm_machine column
    if 'tbm' in merge_df.columns and 'tbm' in machine_df.columns and 'machine' in machine_df.columns:
        try:
            merge_df["tbm_machine"] = None  # Initialize tbm_machine column
            for index, row in merge_df.iterrows():
                tbm_value = row.get("tbm")
                if tbm_value:
                    matching_channel = machine_df[machine_df["tbm"] == tbm_value]
                    if not matching_channel.empty:
                        merge_df.at[index, "tbm_machine"] = matching_channel["machine"].iloc[0]  # NEW: Use machine column
            output_messages.append("✅ Added tbm_machine column to merge_df based on matching tbm values.")
        except Exception as e:
            output_messages.append(f"⚠️ Warning: Failed to add tbm_machine column to merge_df: {e}. Continuing without tbm_machine.")
    else:
        output_messages.append("⚠️ Warning: 'tbm' column not found in merge_df or machine_df. Skipping tbm_machine column creation.")

    # MODIFIED: Check for tbm column before updating machine_df
    if 'tbm' in machine_df.columns and 'tbm' in merge_df.columns and 'tbm_machine' in merge_df.columns:
        try:
            for index, row in machine_df.iterrows():
                tbm_value = row.get("tbm")
                if tbm_value:
                    matching_merge = merge_df[merge_df["tbm"] == tbm_value]
                    if not matching_merge.empty and "tbm_machine" in matching_merge.columns:
                        machine_df.at[index, "machine"] = matching_merge["tbm_machine"].iloc[0]
            output_messages.append("✅ Updated machine_df machine column with tbm_machine values from merge_df.")
        except Exception as e:
            output_messages.append(f"⚠️ Warning: Failed to update machine_df machine column: {e}. Continuing without updating channel_df machine.")
    else:
        output_messages.append("⚠️ Warning: 'tbm' column not found in merge_df or machine_df. Skipping machine_df machine column update.")

    channel_modelnos = set(channel_collect_curing.distinct("modelno"))
    merge_df = merge_df[merge_df["modelno_cimcorp"].isin(channel_modelnos)]
    merge_df = merge_df[merge_df["alert_id_curing_ufm"].isna()]

    merge_df["date_of_tbm"] = merge_df["tbm_buildtime"]
    merge_df["date_of_curing"] = merge_df["curing_infeedtime"]

    def convert_datetime_with_invalid_mask(series, col_name):
        try:
            original_series = series.copy()
            cleaned_series = original_series.astype(str).str.strip().str.replace(r'\s+', ' ', regex=True)
            datetime_series = pd.to_datetime(cleaned_series, format='%Y-%m-%d %H:%M:%S', errors='coerce')
            invalid_mask = datetime_series.isna() & original_series.notna()
            datetime_series[invalid_mask] = DEFAULT_DATETIME
            return datetime_series.dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            return pd.Series([DEFAULT_DATETIME] * len(series))

    try:
        merge_df["date_and_time"] = convert_datetime_with_invalid_mask(merge_df["date_and_time"], "date_and_time")
        merge_df["date_and_time"] = merge_df["date_and_time"].astype(object).where(merge_df["date_and_time"].notnull(), "No Date")
        merge_df["date_of_tbm"] = convert_datetime_with_invalid_mask(merge_df["date_of_tbm"], "date_of_tbm")
        merge_df["date_of_tbm"] = merge_df["date_of_tbm"].astype(object).where(merge_df["date_of_tbm"].notnull(), "No Date")
        merge_df["date_of_curing"] = convert_datetime_with_invalid_mask(merge_df["date_of_curing"], "date_of_curing")
        merge_df["date_of_curing"] = merge_df["date_of_curing"].astype(object).where(merge_df["date_of_curing"].notnull(), "No Date")
        merge_df["time_of_ufm"] = pd.to_datetime(merge_df["time"], errors="coerce").dt.strftime("%H:%M:%S")
        time_invalid_mask = merge_df["time_of_ufm"].isna() & merge_df["time"].notna()
        if time_invalid_mask.any():
            logging.info("⚠️ Found %s invalid time values", time_invalid_mask.sum())
    except Exception as e:
        logging.info("❌ Error during datetime conversion: %s", e)
        exit()

    merge_df["tbm_shift"] = merge_df["date_of_tbm"].apply(get_shift)
    merge_df["curing_shift"] = merge_df["date_of_curing"].apply(get_shift)

    try:
        last_alert = alert_collect.find_one(sort=[("alert_id_curing_ufm", -1)])
        last_alert_id = int(last_alert["alert_id_curing_ufm"]) if last_alert and "alert_id_curing_ufm" in last_alert else 0
    except Exception as e:
        error_message = f"❌ Curing Error fetching last alert ID: {e}"
        output_messages.append(error_message)
        last_alert_id = 0

    try:
        last_alert = alert_collect.find_one(sort=[("alert_id_curing_ufm", -1)])
        last_alert_id = int(last_alert["alert_id_curing_ufm"]) if last_alert and "alert_id_curing_ufm" in last_alert else 0
    except Exception as e:
        error_message = f"❌ Curing Error fetching last alert ID for lot/alert processing: {e}"
        output_messages.append(error_message)
        last_alert_id = 0

    try:
        last_lots = list(lot_curing_collect.aggregate([
            {"$match": {"lot_id": {"$regex": "^Curing_[^_]+-[^_]*_[^_]+_\\d+$"},
                        "modelno": {"$in": list(active_modelnos)}}},
            {"$addFields": {
                "lot_number": {"$toInt": {"$arrayElemAt": [{"$split": ["$lot_id", "_"]}, -1]}},
                "modelno": "$modelno",
                "machine": "$machine"
            }},
            {"$sort": {"lot_number": -1}},
            {"$group": {
                "_id": {"modelno": "$modelno", "machine": "$machine"},
                "max_lot_number": {"$first": "$lot_number"},
                "last_lot_id": {"$first": "$lot_id"}
            }}
        ]))
        last_lot_dict = {
            (item["_id"]["modelno"], item["_id"]["machine"]): {
                "max_lot_number": item["max_lot_number"],
                "last_lot_id": item["last_lot_id"]
            } for item in last_lots
        }
        logging.info(f"ℹ️ Fetched last lot numbers: {len(last_lot_dict)} entries")
        output_messages.append(f"ℹ️ Fetched last lot numbers: {len(last_lot_dict)} entries")
    except Exception as e:
        error_message = f"❌ Curing Error fetching last lot IDs: {e}"
        logging.error(error_message)
        output_messages.append(error_message)
        last_lot_dict = {}

    curing_previous_machine_model_lot_ids = {}
    machine_lot_avg_map = {}
    # === NEW: Bulk operations for Final Yield Only DBs ===
    final_yield_only_bulk = []           # alert_table_final_yield
    final_yield_deviated_only_bulk = []  # alert_deviated_final_yield
    lot_bulk_operations = []
    alert_bulk_operations = []
    averages_bulk_operations = []
    deviated_triggering_bulk_operations = []
    triggering_bulk_operations = []
    bulk_operations = []

    try:
        last_averages = list(averages_collect.aggregate([
            {"$match": {"curing_machine_model_lot_id": {"$regex": "^[^_]+-[^_]*_[^_]+_Curing_[^_]+-[^_]*_[^_]+_\\d+_\\d+$"},
                        "modelno": {"$in": list(active_modelnos)}}},
            {"$sort": {"curing_machine_model_lot_id": -1}},
            {"$group": {
                "_id": {"modelno": "$modelno", "machine": "$machine"},
                "last_curing_lot_id": {"$first": "$curing_machine_model_lot_id"},
                "last_averages": {"$first": "$current_lot_averages"}
            }}
        ]))
        for item in last_averages:
            modelno = item["_id"]["modelno"]
            machine = item["_id"]["machine"]
            curing_lot_id = item["last_curing_lot_id"]
            averages = item["last_averages"]
            curing_previous_machine_model_lot_ids[(modelno, machine)] = curing_lot_id
            if averages:
                machine_lot_avg_map[(modelno, machine, curing_lot_id)] = averages
                logging.info(f"Pre-populated machine_lot_avg_map with averages for {curing_lot_id}")
            else:
                logging.warning(f"No averages found for {curing_lot_id} in averages_collect")
        logging.info(f"Initialized curing_previous_machine_model_lot_ids with {len(curing_previous_machine_model_lot_ids)} entries")
        output_messages.append(f"Initialized curing_previous_machine_model_lot_ids with {len(curing_previous_machine_model_lot_ids)} entries")
    except Exception as e:
        logging.error(f"❌ Error pre-populating machine_lot_avg_map and curing_previous_machine_model_lot_ids: {e}")
        output_messages.append(f"❌ Error pre-populating machine_lot_avg_map and curing_previous_machine_model_lot_ids: {e}")

    # try:
    #     curing_df["curing_press_id"] = curing_df["curing_press_id"].astype(str)
    #     curing_df["value"] = curing_df["value"].astype(str)
    #     merge_df = merge_df.merge(
    #         curing_df[["value", "curing_press_id"]],
    #         left_on="curing_press",
    #         right_on="value",
    #         how="left"
    #     )
    #     merge_df["curing_press"] = merge_df["curing_press_id"].fillna(merge_df["curing_press"])
    #     merge_df.drop(columns=["value", "curing_press_id"], inplace=True)
    #     logging.info("✅ Merged merge_df with curing_press data.")
    #     output_messages.append("✅ Merged merge_df with curing_press data.")
    # except Exception as e:
    #     error_message = f"❌ Curing Error merging with curing_press data: {e}"
    #     logging.error(error_message)
    #     output_messages.append(error_message)
    #     return {"status": "error", "message": error_message, "output_log": output_messages}, 500

    conicity_params = ["con_kgf", "cwrfv_oa_kgf", "ccwrfv_oa_kgf",
                       "cwrfv_oa1h_kgf", "ccwrfv_oa1h_kgf", "lfv_oa_kgf", "lfv_oa_1_kgf", "upper_g","lower_g","static_g","up_low_g", "load1","airpress1"]

    yield_params = [
        "total_rank_yield", "rfv_yield", "rfh1_yield", "lfv_yield", "conicity_yield",
        "ufm_yield", "rorank_yield", "radial_runout_yield", "radial_runout_first_harmonic_yield",
        "lateral_run_out_yield", "lateral_run_out_top_over_yield", "lateral_run_out_bottom_over_yield",
        "lateral_run_out_harmonic_yield", "lateral_run_out_top_first_harmonic_yield",
        "lateral_run_out_bottom_first_harmonic_yield", "bulge_yield", "bulge_top_over_yield",
        "bulge_bottom_over_yield", "dent_yield", "dent_top_over_yield", "dent_bottom_over_yield",
        "balancing_yield", "upper_yield", "lower_yield", "upper_lower_yield", "static_yield","warehouse_yield", "visual_yield"]

    rank_param_mappings = {
        "total_rank_yield": ["total_rank"],
        "rfv_yield": ["cwrfv_oa_rank", "ccwrfv_oa_rank"],
        "rfh1_yield": ["cwrfv_oa1h_rank", "ccwrfv_oa1h_rank"],
        "lfv_yield": ["lfv_oa_rank", "lfv_oa_1_rank"],
        "conicity_yield": ["con_rank"],
        "ufm_yield": ["ufm_rank"],
        "rorank_yield": ["rorank"],
        "radial_runout_yield": ["rc_oa_rank"],
        "radial_runout_first_harmonic_yield": ["rc_oa1h_rank"],
        "lateral_run_out_top_over_yield": ["lt_oa_rank"],
        "lateral_run_out_bottom_over_yield": ["lb_oa_rank"],
        "lateral_run_out_top_first_harmonic_yield": ["lt_oa1h_rank"],
        "lateral_run_out_bottom_first_harmonic_yield": ["lb_oa1h_rank"],
        "bulge_top_over_yield": ["ltbulg_rank"],
        "bulge_bottom_over_yield": ["lbbulg_rank"],
        "dent_top_over_yield": ["dent_rank"],
        "dent_bottom_over_yield": ["dent1_rank"],
        "balancing_yield": ["bal_rank"],
        "upper_yield": ["upper_rank"],
        "lower_yield": ["lower_rank"],
        "upper_lower_yield": ["up_low_rank"],
        "static_yield": ["static_rank"],
        "lateral_run_out_yield": ["lt_oa_rank", "lb_oa_rank"],
        "lateral_run_out_harmonic_yield": ["lt_oa1h_rank", "lb_oa1h_rank"],
        "bulge_yield": ["ltbulg_rank", "lbbulg_rank"],
        "dent_yield": ["dent_rank", "dent1_rank"]
    }

    for _, row in channel_df.iterrows():
        try:
            model_number = str(row.get("modelno", "")).strip()
            if model_number not in active_modelnos:
                output_messages.append(f"Skipping CURING model {model_number} as it is not active.")
                continue
            gt_codes = row.get("gt_code", [])

            if not isinstance(gt_codes, list):
                gt_codes = [gt_codes]

            target_lot_size = int(row.get("lot_size", 0))

            # spec_rfv = float(row.get("spec_rfv", 0))
            spec_con_kgf = float(row.get("spec_con_kgf", 0))
            sizecode = str(row.get("sizecode", ""))
            spec_cwrfv_oa_kgf = float(row.get("spec_cwrfv_oa_kgf", 0))
            spec_ccwrfv_oa_kgf = float(row.get("spec_ccwrfv_oa_kgf", 0))
            # spec_rfh1 = float(row.get("spec_rfh1", 0))
            spec_cwrfv_oa1h_kgf = float(row.get("spec_cwrfv_oa1h_kgf", 0))
            spec_ccwrfv_oa1h_kgf = float(row.get("spec_ccwrfv_oa1h_kgf", 0))
            # spec_lfv = float(row.get("spec_lfv", 0))
            spec_lfv_oa_kgf = float(row.get("spec_lfv_oa_kgf", 0))
            spec_lfv_oa_1_kgf = float(row.get("spec_lfv_oa_1_kgf", 0))
            spec_static = float(row.get("spec_static", 0))
            spec_upper_g = float(row.get("spec_upper_g", 0))
            spec_lower_g = float(row.get("spec_lower_g", 0))
            spec_up_low_g = float(row.get("spec_up_low_g", 0))
            test_load = float(row.get("test_load", 0))
            pressure = float(row.get("pressure", 0))
            # spec_yield = float(row.get("spec_yield", 0))

            total_rank_yield_spec_yield = float(row.get("total_rank_yield_spec_yield", 0))
            rfv_yield_spec_yield = float(row.get("rfv_yield_spec_yield", 0))
            rfh1_yield_spec_yield = float(row.get("rfh1_yield_spec_yield", 0))
            lfv_yield_spec_yield = float(row.get("lfv_yield_spec_yield", 0))
            conicity_yield_spec_yield = float(row.get("conicity_yield_spec_yield", 0))
            ufm_yield_spec_yield = float(row.get("ufm_yield_spec_yield", 0))
            rorank_yield_spec_yield = float(row.get("rorank_yield_spec_yield", 0))
            radial_runout_yield_spec_yield = float(row.get("radial_runout_yield_spec_yield", 0))
            radial_runout_first_harmonic_yield_spec_yield = float(row.get("radial_runout_first_harmonic_yield_spec_yield", 0))
            lateral_run_out_yield_spec_yield = float(row.get("lateral_run_out_yield_spec_yield", 0))
            lateral_run_out_top_over_yield_spec_yield = float(row.get("lateral_run_out_top_over_yield_spec_yield", 0))
            lateral_run_out_bottom_over_yield_spec_yield = float(row.get("lateral_run_out_bottom_over_yield_spec_yield", 0))
            lateral_run_out_harmonic_yield_spec_yield = float(row.get("lateral_run_out_harmonic_yield_spec_yield", 0))
            lateral_run_out_top_first_harmonic_yield_spec_yield = float(row.get("lateral_run_out_top_first_harmonic_yield_spec_yield", 0))
            lateral_run_out_bottom_first_harmonic_yield_spec_yield = float(row.get("lateral_run_out_bottom_first_harmonic_yield_spec_yield", 0))
            bulge_yield_spec_yield = float(row.get("bulge_yield_spec_yield", 0))
            bulge_top_over_yield_spec_yield = float(row.get("bulge_top_over_yield_spec_yield", 0))
            bulge_bottom_over_yield_spec_yield = float(row.get("bulge_bottom_over_yield_spec_yield", 0))
            dent_yield_spec_yield = float(row.get("dent_yield_spec_yield", 0))
            dent_top_over_yield_spec_yield = float(row.get("dent_top_over_yield_spec_yield", 0))
            dent_bottom_over_yield_spec_yield = float(row.get("dent_bottom_over_yield_spec_yield", 0))
            balancing_yield_spec_yield = float(row.get("balancing_yield_spec_yield", 0))
            upper_yield_spec_yield = float(row.get("upper_yield_spec_yield", 0))
            lower_yield_spec_yield = float(row.get("lower_yield_spec_yield", 0))
            upper_lower_yield_spec_yield = float(row.get("upper_lower_yield_spec_yield", 0))
            static_yield_spec_yield = float(row.get("static_yield_spec_yield", 0))
            warehouse_yield_spec_yield = float(row.get("warehouse_yield_spec_yield", 0))
            visual_yield_spec_yield = float(row.get("visual_yield_spec_yield", 0))
            
            # threshold_value = float(row.get("threshold_value", 0.5))
            threshold_value_con_kgf = float(row.get("threshold_value_con_kgf", 0.5))
            threshold_value_cwrfv_oa_kgf = float(row.get("threshold_value_cwrfv_oa_kgf", 0.5))
            threshold_value_ccwrfv_oa_kgf = float(row.get("threshold_value_ccwrfv_oa_kgf", 0.5))
            threshold_value_cwrfv_oa1h_kgf = float(row.get("threshold_value_cwrfv_oa1h_kgf", 0.5))
            threshold_value_ccwrfv_oa1h_kgf = float(row.get("threshold_value_ccwrfv_oa1h_kgf", 0.5))
            threshold_value_lfv_oa_kgf = float(row.get("threshold_value_lfv_oa_kgf", 0.5))
            threshold_value_lfv_oa_1_kgf = float(row.get("threshold_value_lfv_oa_1_kgf", 0.5))
            # threshold_value_bal = float(row.get("threshold_value_bal", 5))
            threshold_value_bal_upper_g = float(row.get("threshold_value_bal_upper_g", 5))
            threshold_value_bal_lower_g = float(row.get("threshold_value_bal_lower_g", 5))
            threshold_value_bal_up_low_g = float(row.get("threshold_value_bal_up_low_g", 5))
            threshold_value_bal_static_g = float(row.get("threshold_value_bal_static_g", 5))
            test_load_tolerance = float(row.get("test_load_tolerance", 5))
            pressure_tolerance = float(row.get("pressure_tolerance", 2))
            # threshold_value_yield = float(row.get("threshold_value_yield", 0))
            threshold_value_yield_total_rank_yield = float(row.get("threshold_value_yield_total_rank_yield", 0))
            threshold_value_yield_rfv_yield = float(row.get("threshold_value_yield_rfv_yield", 0))
            threshold_value_yield_rfh1_yield = float(row.get("threshold_value_yield_rfh1_yield", 0))
            threshold_value_yield_lfv_yield = float(row.get("threshold_value_yield_lfv_yield", 0))
            threshold_value_yield_conicity_yield = float(row.get("threshold_value_yield_conicity_yield", 0))
            threshold_value_yield_ufm_yield = float(row.get("threshold_value_yield_ufm_yield", 0))
            threshold_value_yield_rorank_yield = float(row.get("threshold_value_yield_rorank_yield", 0))
            threshold_value_yield_radial_runout_yield = float(row.get("threshold_value_yield_radial_runout_yield", 0))
            threshold_value_yield_radial_runout_first_harmonic_yield = float(row.get("threshold_value_yield_radial_runout_first_harmonic_yield", 0))
            threshold_value_yield_lateral_run_out_yield = float(row.get("threshold_value_yield_lateral_run_out_yield", 0))
            threshold_value_yield_lateral_run_out_top_over_yield = float(row.get("threshold_value_yield_lateral_run_out_top_over_yield", 0))
            threshold_value_yield_lateral_run_out_bottom_over_yield = float(row.get("threshold_value_yield_lateral_run_out_bottom_over_yield", 0))
            threshold_value_yield_lateral_run_out_harmonic_yield = float(row.get("threshold_value_yield_lateral_run_out_harmonic_yield", 0))
            threshold_value_yield_lateral_run_out_top_first_harmonic_yield = float(row.get("threshold_value_yield_lateral_run_out_top_first_harmonic_yield", 0))
            threshold_value_yield_lateral_run_out_bottom_first_harmonic_yield = float(row.get("threshold_value_yield_lateral_run_out_bottom_first_harmonic_yield", 0))
            threshold_value_yield_bulge_yield = float(row.get("threshold_value_yield_bulge_yield", 0))
            threshold_value_yield_bulge_top_over_yield = float(row.get("threshold_value_yield_bulge_top_over_yield", 0))
            threshold_value_yield_bulge_bottom_over_yield = float(row.get("threshold_value_yield_bulge_bottom_over_yield", 0))
            threshold_value_yield_dent_yield = float(row.get("threshold_value_yield_dent_yield", 0))
            threshold_value_yield_dent_top_over_yield = float(row.get("threshold_value_yield_dent_top_over_yield", 0))
            threshold_value_yield_dent_bottom_over_yield = float(row.get("threshold_value_yield_dent_bottom_over_yield", 0))
            threshold_value_yield_balancing_yield = float(row.get("threshold_value_yield_balancing_yield", 0))
            threshold_value_yield_upper_yield = float(row.get("threshold_value_yield_upper_yield", 0))
            threshold_value_yield_lower_yield = float(row.get("threshold_value_yield_lower_yield", 0))
            threshold_value_yield_upper_lower_yield = float(row.get("threshold_value_yield_upper_lower_yield", 0))
            threshold_value_yield_static_yield = float(row.get("threshold_value_yield_static_yield", 0))
            threshold_value_yield_warehouse_yield = float(row.get("threshold_value_yield_warehouse_yield", 0))
            threshold_value_yield_visual_yield = float(row.get("threshold_value_yield_visual_yield", 0))

            for gt_code in gt_codes:
                gt_code = str(gt_code).strip()
                model_data_full = merge_df[
                    (merge_df["modelno"] == model_number) &
                    (merge_df["modelno_cimcorp"] == model_number) &
                    (merge_df["tbm_gtcode"] == gt_code)
                ].copy()
                
                if model_data_full.empty:
                    continue

                total_barcodes_model = len(model_data_full)

                if total_barcodes_model < target_lot_size:
                    output_messages.append(f"Skipping Curing model {model_number} due to insufficient model barcodes.")
                    continue
                
                # Ensure date_of_tbm is proper datetime for sorting
                model_data_full["date_of_dt"] = pd.to_datetime(model_data_full["date_and_time"], errors='coerce')

                # === 1. Virgin Yield: Keep FIRST occurrence of each barcode ===
                virgin_data = model_data_full.sort_values("date_of_dt").drop_duplicates(subset="barcode", keep="first").copy()
                # >>> ADD THESE PRINT STATEMENTS <<<
                virgin_dupes = model_data_full[model_data_full.duplicated(subset="barcode", keep=False)]
                if not virgin_dupes.empty:
                    first_occ = virgin_dupes.sort_values("date_of_dt").drop_duplicates("barcode", keep="first")
                    # print("=== VIRGIN YIELD - Duplicate barcodes (first occurrence shown) ===")
                    # for _, r in first_occ.iterrows():
                    #     print(f"Barcode: {r['barcode']} | First seen: {r['date_and_time']} | Machine: {r['machine']} | Model: {model_number}")
                # <<< END OF ADDED PRINT >>>

                # === 2. Final Yield: Keep LAST occurrence of each barcode ===
                final_data = model_data_full.sort_values("date_of_dt").drop_duplicates(subset="barcode", keep="last").copy()
                # >>> ADD THESE PRINT STATEMENTS <<<
                final_dupes = model_data_full[model_data_full.duplicated(subset="barcode", keep=False)]
                if not final_dupes.empty:
                    last_occ = final_dupes.sort_values("date_of_dt").drop_duplicates("barcode", keep="last")
                    # print("=== FINAL YIELD - Duplicate barcodes (last occurrence shown) ===")
                    # for _, r in last_occ.iterrows():
                    #     print(f"Barcode: {r['barcode']} | Last seen: {r['date_and_time']} | Machine: {r['machine']} | Model: {model_number}")
                # <<< END OF ADDED PRINT >>>

                # Sort both for lot processing
                virgin_data.sort_values("date_of_dt", inplace=True)
                final_data.sort_values("date_of_dt", inplace=True)

                # Use original full data for main lot processing (existing logic)
                model_data = model_data_full.copy()
                model_data.sort_values(["date_of_curing"], ascending=True, inplace=True)
                alert_map = {}
                for curing_value, curing_data in model_data.groupby(["curing_press", "curing_mouldid"]):
                    curing_data = curing_data[curing_data["curing_machine_model_lot_id"].isnull()].copy()
                    if curing_data.empty:
                        continue

                    barcode_counter = 0
                    total_curing_barcodes = len(curing_data)
                    curing_press_value, curing_mouldid_value = curing_value
                    machine_no = f"{curing_press_value}-{curing_mouldid_value}"
                    machine_press_value = machine_no.split('-')[0]

                    # MODIFIED: Only access tbm_machine if it exists
                    tbm_machine_value = curing_data["tbm_machine"].iloc[0] if "tbm_machine" in curing_data.columns and not curing_data["tbm_machine"].isna().all() else None
                    output_messages.append(f"DEBUG: Model {model_number}, Machine {machine_no}, tbm_machine: {tbm_machine_value}, Found {total_curing_barcodes} barcodes.")

                    # machine_row = curing_df[curing_df["curing_press_id"] == machine_press_value]
                    # if machine_row.empty:
                    #     output_messages.append(f"Skipping Curing machine {machine_no} for model {model_number} due to unknown machine target")
                    #     continue
                    if total_curing_barcodes < target_lot_size:
                        output_messages.append(f"DEBUG: Skipping machine {machine_no} - Found {total_curing_barcodes}, Need {target_lot_size}.")
                        continue

                    barcode_counter = 0

                    while barcode_counter < total_curing_barcodes:
                        remaining_model = total_curing_barcodes - barcode_counter
                        if remaining_model < target_lot_size:
                            break
                        batch_size = target_lot_size
                        batch_indices = curing_data.iloc[barcode_counter: barcode_counter + batch_size].index
                        batch_data = curing_data.iloc[barcode_counter: barcode_counter + batch_size].copy()

                        machine_model_key = (model_number, machine_no)
                        if machine_model_key not in last_lot_dict:
                            last_lot_dict[machine_model_key] = {
                                "max_lot_number": 0,
                                "last_lot_id": None
                            }
                        last_lot_number = last_lot_dict[(model_number, machine_no)]["max_lot_number"] + 1

                        unique_lot_id_current = f"Curing_{machine_no}_{model_number}_{last_lot_number}"
                        current_barcode_counter = barcode_counter + batch_size
                        current_curing_machine_model_lot_id = f"{machine_no}_{model_number}_Curing_{machine_no}_{model_number}_{last_lot_number}_{current_barcode_counter}"
                        curing_previous_machine_model_lot_id = curing_previous_machine_model_lot_ids.get((model_number, machine_no))

                        last_lot_dict[(model_number, machine_no)]["max_lot_number"] = last_lot_number
                        last_lot_dict[(model_number, machine_no)]["last_lot_id"] = unique_lot_id_current

                        if not re.match(r"^Curing_[^_]+_[^_]+_\d+$", unique_lot_id_current):
                            logging.error(f"Invalid lot_id format: {unique_lot_id_current}")
                            continue
                        if not re.match(r"^[^_]+_[^_]+_Curing_[^_]+_[^_]+_\d+_\d+$", current_curing_machine_model_lot_id):
                            logging.error(f"Invalid curing_machine_model_lot_id format: {current_curing_machine_model_lot_id}")
                            continue

                        curing_previous_machine_model_lot_id = curing_previous_machine_model_lot_ids.get(machine_model_key)
                        logging.info(f"Initial curing_previous_machine_model_lot_id: {curing_previous_machine_model_lot_id} for model {model_number}, machine {machine_no}")
                        if curing_previous_machine_model_lot_id is None and last_lot_number > 1:
                            prev_lot_number = last_lot_number - 1
                            prev_lot_id = f"Curing_{machine_no}_{model_number}_{prev_lot_number}"
                            logging.info(f"Fetching previous lot for lot_id: {prev_lot_id}")
                            try:
                                prev_lot = averages_collect.find_one(
                                    {
                                        "modelno": model_number,
                                        "machine": machine_no,
                                        "lot_id": prev_lot_id
                                    },
                                    sort=[("curing_machine_model_lot_id", -1)]
                                )
                                if prev_lot:
                                    curing_previous_machine_model_lot_id = prev_lot["curing_machine_model_lot_id"]
                                    logging.info(f"Found previous lot: {curing_previous_machine_model_lot_id}")
                                else:
                                    logging.warning(f"No previous lot found for lot_id: {prev_lot_id}")
                                    curing_previous_machine_model_lot_id = None
                            except Exception as e:
                                logging.error(f"Error fetching previous curing_machine_model_lot_id for lot {prev_lot_number}: {e}")
                                curing_previous_machine_model_lot_id = None

                        previous_lot_id = f"Curing_{machine_no}_{model_number}_{last_lot_number - 1}" if last_lot_number > 1 else None
                        merge_df.loc[batch_indices, "lot_id"] = unique_lot_id_current
                        merge_df.loc[batch_indices, "previous_lot_id"] = previous_lot_id
                        merge_df.loc[batch_indices, "curing_machine_model_lot_id"] = current_curing_machine_model_lot_id
                        merge_df.loc[batch_indices, "curing_previous_machine_model_lot_id"] = curing_previous_machine_model_lot_id

                        prev_key = (model_number, machine_no, curing_previous_machine_model_lot_id)
                        prev_lot_avg = machine_lot_avg_map.get(prev_key, None)
                        if prev_lot_avg is None and curing_previous_machine_model_lot_id is not None:
                            try:
                                prev_lot_doc = averages_collect.find_one(
                                    {
                                        "modelno": model_number,
                                        "machine": machine_no,
                                        "curing_machine_model_lot_id": curing_previous_machine_model_lot_id
                                    },
                                    {"current_lot_averages": 1}
                                )
                                prev_lot_avg = prev_lot_doc.get("current_lot_averages", {}) if prev_lot_doc else {}
                                if prev_lot_avg:
                                    machine_lot_avg_map[prev_key] = prev_lot_avg
                                    logging.info(f"Fetched previous lot averages for {curing_previous_machine_model_lot_id} from database")
                                else:
                                    logging.warning(f"No previous lot averages found for {curing_previous_machine_model_lot_id} in database")
                            except Exception as e:
                                logging.error(f"Error fetching previous lot averages for {curing_previous_machine_model_lot_id}: {e}")
                                prev_lot_avg = {}
                        if last_lot_number == 1:
                            prev_lot_avg = {}
                            logging.info(f"First lot for model {model_number}, machine {machine_no}. Setting previous_lot_averages to empty.")
                        # logging.info(f"prev_key: {prev_key}, prev_lot_avg: {prev_lot_avg}")

                        last_lot_dict[machine_model_key]["max_lot_number"] = last_lot_number
                        last_lot_dict[machine_model_key]["last_lot_id"] = unique_lot_id_current

                        lot_doc = {
                            "modelno": model_number,
                            "machine": machine_no,
                            "lot_id": unique_lot_id_current,
                            "timestamp": some_function()
                        }
                        try:
                            if not lot_curing_collect.find_one({"lot_id": unique_lot_id_current}):
                                lot_curing_collect.insert_one(lot_doc)
                                output_messages.append(f"Inserted lot_id {unique_lot_id_current} into lot_curing_collect")
                        except Exception as e:
                            logging.error(f"Error inserting lot_id {unique_lot_id_current} into lot_curing_collect: {e}")
                            output_messages.append(f"❌ Error inserting lot_id {unique_lot_id_current}: {e}")

                        logging.info(f"Assigned lot_id: {unique_lot_id_current}, curing_machine_model_lot_id: {current_curing_machine_model_lot_id}, curing_previous_machine_model_lot_id: {curing_previous_machine_model_lot_id}")

                        for param in conicity_params:
                            if param in batch_data.columns:
                                batch_data[param] = pd.to_numeric(batch_data[param], errors='coerce')

                        processed_machine_barcodes = batch_size
                        balance_machine_barcodes = total_curing_barcodes - (barcode_counter + batch_size)
                        balance_barcodes = total_barcodes_model - (barcode_counter + batch_size)

                        lot_averages = {}
                        for param in conicity_params:
                            if param in batch_data.columns:
                                valid_vals = batch_data[param].dropna()
                                if not valid_vals.empty:
                                    lot_averages[param] = float(valid_vals.mean())
                                else:
                                    lot_averages[param] = None
                            else:
                                lot_averages[param] = None

                        lot_averages.update({
                            "total_rank_yield": total_rank_yield(batch_data),
                            "rfv_yield": rfv_yield(batch_data),
                            "rfh1_yield": rfh1_yield(batch_data),
                            "lfv_yield": lfv_yield(batch_data),
                            "conicity_yield": conicity_yield(batch_data),
                            "ufm_yield": ufm_yield(batch_data),
                            "rorank_yield": rorank_yield(batch_data),
                            "radial_runout_yield": radial_runout_yield(batch_data),
                            "radial_runout_first_harmonic_yield": radial_runout_first_harmonic_yield(batch_data),
                            "lateral_run_out_yield": lateral_run_out_yield(batch_data),
                            "lateral_run_out_top_over_yield": lateral_run_out_top_over(batch_data),
                            "lateral_run_out_bottom_over_yield": lateral_run_out_bottom_over(batch_data),
                            "lateral_run_out_harmonic_yield": lateral_run_out_harmonic_yield(batch_data),
                            "lateral_run_out_top_first_harmonic_yield": lateral_run_out_top_first_harmonic(batch_data),
                            "lateral_run_out_bottom_first_harmonic_yield": lateral_run_out_bottom_first_harmonic(batch_data),
                            "bulge_yield": bulge_yield(batch_data),
                            "bulge_top_over_yield": bulge_top_over(batch_data),
                            "bulge_bottom_over_yield": bulge_bottom_over(batch_data),
                            "dent_yield": dent_yield(batch_data),
                            "dent_top_over_yield": dent_top_over(batch_data),
                            "dent_bottom_over_yield": dent_bottom_over(batch_data),
                            "balancing_yield": balancing_yield(batch_data),
                            "upper_yield": upper_yield(batch_data),
                            "lower_yield": lower_yield(batch_data),
                            "upper_lower_yield": upper_lower_yield(batch_data),
                            "static_yield": static_yield(batch_data),
                            "warehouse_yield": warehouse_yield(batch_data),
                            "visual_yield": visual_yield(batch_data)
                        })

                        grouped_averages = {}
                        param_spec_map = {
                            "con_kgf": spec_con_kgf,
                            "cwrfv_oa_kgf": spec_cwrfv_oa_kgf,
                            "ccwrfv_oa_kgf": spec_ccwrfv_oa_kgf,
                            "cwrfv_oa1h_kgf": spec_cwrfv_oa1h_kgf,
                            "ccwrfv_oa1h_kgf": spec_ccwrfv_oa1h_kgf,
                            "lfv_oa_kgf": spec_lfv_oa_kgf,
                            "lfv_oa_1_kgf": spec_lfv_oa_1_kgf,
                            "upper_g": spec_upper_g,
                            "lower_g": spec_lower_g,
                            "up_low_g": spec_up_low_g,
                            "static_g": spec_static,
                            "load1": test_load,
                            "airpress1": pressure,
                            "total_rank_yield": total_rank_yield_spec_yield,
                            "rfv_yield": rfv_yield_spec_yield,
                            "rfh1_yield": rfh1_yield_spec_yield,
                            "lfv_yield": lfv_yield_spec_yield,
                            "conicity_yield": conicity_yield_spec_yield,
                            "ufm_yield": ufm_yield_spec_yield,
                            "rorank_yield": rorank_yield_spec_yield,
                            "radial_runout_yield": radial_runout_yield_spec_yield,
                            "radial_runout_first_harmonic_yield": radial_runout_first_harmonic_yield_spec_yield,
                            "lateral_run_out_yield": lateral_run_out_yield_spec_yield,
                            "lateral_run_out_top_over_yield": lateral_run_out_top_over_yield_spec_yield,
                            "lateral_run_out_bottom_over_yield": lateral_run_out_bottom_over_yield_spec_yield,
                            "lateral_run_out_harmonic_yield": lateral_run_out_harmonic_yield_spec_yield,
                            "lateral_run_out_top_first_harmonic_yield": lateral_run_out_top_first_harmonic_yield_spec_yield,
                            "lateral_run_out_bottom_first_harmonic_yield": lateral_run_out_bottom_first_harmonic_yield_spec_yield,
                            "bulge_yield": bulge_yield_spec_yield,
                            "bulge_top_over_yield": bulge_top_over_yield_spec_yield,
                            "bulge_bottom_over_yield": bulge_bottom_over_yield_spec_yield,
                            "dent_yield": dent_yield_spec_yield,
                            "dent_top_over_yield": dent_top_over_yield_spec_yield,
                            "dent_bottom_over_yield": dent_bottom_over_yield_spec_yield,
                            "balancing_yield": balancing_yield_spec_yield,
                            "upper_yield": upper_yield_spec_yield,
                            "lower_yield": lower_yield_spec_yield,
                            "upper_lower_yield": upper_lower_yield_spec_yield,
                            "static_yield": static_yield_spec_yield,
                            "warehouse_yield": warehouse_yield_spec_yield,
                            "visual_yield": visual_yield_spec_yield
                        }

                        try:
                            batch_data["ubno"] = batch_data["ubno"].astype(str).str.strip()
                            batch_data.sort_values("date_and_time", ascending=True, inplace=True)
                            ubno_grouped = batch_data.groupby("ubno")
                            ubno_records = []
                            for ubno, group in ubno_grouped:
                                group.sort_values("date_and_time", ascending=True)
                                record = {
                                    "ubno": ubno,
                                    "barcode_count": len(group["barcode"]),
                                    "tub_date_start_time": group["date_and_time"].iloc[0] if not group["date_and_time"].empty else None,
                                    "tub_date_end_time": group["date_and_time"].iloc[-1] if not group["date_and_time"].empty else None,
                                    "barcodes": group["barcode"].tolist() if "barcode" in group.columns else []
                                }
                                parameters = []
                                for param in conicity_params:
                                    try:
                                        if param in group.columns:
                                            valid_vals = group[param].dropna()
                                            avg_val = float(valid_vals.mean()) if not valid_vals.empty else None
                                        else:
                                            avg_val = None
                                        record[f"avg_{param}"] = avg_val
                                        spec = param_spec_map.get(param)
                                        exceeded = False
                                        if param == "con_kgf" and spec_con_kgf is not None:
                                            exceeded = not (-spec_con_kgf <= avg_val <= spec_con_kgf)
                                        if param == "load1" and test_load is not None and test_load_tolerance is not None:
                                            exceeded = not (test_load - test_load_tolerance <= avg_val <= test_load + test_load_tolerance)
                                        if param == "airpress1" and pressure is not None and pressure_tolerance is not None:
                                            exceeded = not (pressure - pressure_tolerance <= avg_val <= pressure + pressure_tolerance)
                                        if spec is not None and param not in ["con_kgf", "load1", "airpress1"]:
                                            exceeded = avg_val > spec
                                        parameters.append({
                                            "parameter": param,
                                            "avg_value": avg_val,
                                            "spec_value": spec,
                                            "spec_threshold_exceeded": bool(exceeded)
                                        })
                                    except Exception as e:
                                        logging.info(f"[ERROR] Processing numeric param {param}: {e}")
                                        record[f"avg_{param}"] = None
                                        parameters.append({
                                            "parameter": param,
                                            "avg_value": None,
                                            "spec_value": param_spec_map.get(param),
                                            "spec_threshold_exceeded": False
                                        })
                                for param in yield_params:
                                    try:
                                        yield_func = globals()[param]
                                        avg_val = yield_func(group)
                                        record[param] = avg_val
                                        spec = param_spec_map.get(param)
                                        exceeded = avg_val < spec if spec is not None else False
                                        parameters.append({
                                            "parameter": param,
                                            "avg_value": avg_val,
                                            "spec_value": spec,
                                            "spec_threshold_exceeded": bool(exceeded)
                                        })
                                    except Exception as e:
                                        record[param] = None
                                        parameters.append({
                                            "parameter": param,
                                            "avg_value": None,
                                            "spec_value": param_spec_map.get(param),
                                            "spec_threshold_exceeded": False
                                        })
                                record["parameter_list"] = parameters
                                ubno_records.append(record)
                            grouped_averages["ubno"] = ubno_records
                        except:
                            grouped_averages["ubno"] = []

                        batch_data1 = batch_data.copy()
                        batch_data.sort_values("date_of_tbm", ascending=True, inplace=True)
                        batch_data1.sort_values("date_and_time", ascending=True, inplace=True)
                        batch_data.sort_values("date_of_curing", ascending=True, inplace=True)
                        batch_data1.sort_values("date_and_time", ascending=True, inplace=True)
                        machine_lot_start_build_time = batch_data["date_of_tbm"].iloc[0]
                        machine_lot_end_build_time = batch_data["date_of_tbm"].iloc[-1]
                        machine_lot_start_curing_time = batch_data["date_of_curing"].iloc[0]
                        machine_lot_end_curing_time = batch_data["date_of_curing"].iloc[-1]
                        tub_date_start_time = batch_data1["date_and_time"].iloc[0]
                        tub_date_end_time = batch_data1["date_and_time"].iloc[-1]
                        lot_start_time = some_function()
                        lot_end_time = some_function()

                        parameter_alerts = []
                        overall_shift_occurred = False
                        prev_key = (model_number, machine_no, curing_previous_machine_model_lot_id)
                        prev_lot_avg = machine_lot_avg_map.get(prev_key, None)

                        if prev_lot_avg:
                            for param in conicity_params + yield_params:
                                shift = False
                                shift_occurred_spec = False
                                current_val = lot_averages.get(param)
                                prev_val = prev_lot_avg.get(param)
                                if current_val is not None and prev_val is not None:
                                    if param == "con_kgf" and spec_con_kgf is not None:
                                        if abs(current_val - prev_val) > threshold_value_con_kgf:
                                            shift = True
                                        if not (-spec_con_kgf <= current_val <= spec_con_kgf):
                                            shift_occurred_spec = True
                                    if param == "cwrfv_oa_kgf" and spec_cwrfv_oa_kgf is not None:
                                        if (current_val - prev_val) > threshold_value_cwrfv_oa_kgf:
                                            shift = True
                                        if current_val > spec_cwrfv_oa_kgf:
                                            shift_occurred_spec = True
                                    if param == "ccwrfv_oa_kgf" and spec_ccwrfv_oa_kgf is not None:
                                        if (current_val - prev_val) > threshold_value_ccwrfv_oa_kgf:
                                            shift = True
                                        if current_val > spec_ccwrfv_oa_kgf:
                                            shift_occurred_spec = True
                                    if param == "cwrfv_oa1h_kgf" and spec_cwrfv_oa1h_kgf is not None:
                                        if (current_val - prev_val) > threshold_value_cwrfv_oa1h_kgf:
                                            shift = True
                                        if current_val > spec_cwrfv_oa1h_kgf:
                                            shift_occurred_spec = True
                                    if param == "ccwrfv_oa1h_kgf" and spec_ccwrfv_oa1h_kgf is not None:
                                        if (current_val - prev_val) > threshold_value_ccwrfv_oa1h_kgf:
                                            shift = True
                                        if current_val > spec_ccwrfv_oa1h_kgf:
                                            shift_occurred_spec = True
                                    if param == "lfv_oa_kgf" and spec_lfv_oa_kgf is not None:
                                        if (current_val - prev_val) > threshold_value_lfv_oa_kgf:
                                            shift = True
                                        if current_val > spec_lfv_oa_kgf:
                                            shift_occurred_spec = True
                                    if param == "lfv_oa_1_kgf" and spec_lfv_oa_1_kgf is not None:
                                        if (current_val - prev_val) > threshold_value_lfv_oa_1_kgf:
                                            shift = True
                                        if current_val > spec_lfv_oa_1_kgf:
                                            shift_occurred_spec = True
                                    if param == "upper_g" and spec_upper_g is not None:
                                        if (current_val - prev_val) > threshold_value_bal_upper_g:
                                            shift = True
                                        if current_val > spec_upper_g:
                                            shift_occurred_spec = True
                                    if param == "lower_g" and spec_lower_g is not None:
                                        if (current_val - prev_val) > threshold_value_bal_lower_g:
                                            shift = True
                                        if current_val > spec_lower_g:
                                            shift_occurred_spec = True
                                    if param == "up_low_g" and spec_up_low_g is not None:
                                        if (current_val - prev_val) > threshold_value_bal_up_low_g:
                                            shift = True
                                        if current_val > spec_up_low_g:
                                            shift_occurred_spec = True
                                    if param == "static_g" and spec_static is not None:
                                        if (current_val - prev_val) > threshold_value_bal_static_g:
                                            shift = True
                                        if current_val > spec_static:
                                            shift_occurred_spec = True
                                    
                                    ############### Add 16-07 start ######################

                                    # if param in yield_params:
                                    #     spec_yield = param_spec_map.get(param)
                                    #     if spec_yield is not None:
                                    #         if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield)):
                                    #             shift = True
                                    #         if abs(current_val < spec_yield):
                                    #             shift_occurred_spec = True

                                    ############### Add 16-07 start ######################

                                    if param == "total_rank_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_total_rank_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "rfv_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_rfv_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "rfh1_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_rfh1_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "lfv_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_lfv_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "conicity_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_conicity_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "ufm_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_ufm_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "rorank_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_rorank_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "radial_runout_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_radial_runout_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "radial_runout_first_harmonic_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_radial_runout_first_harmonic_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "lateral_run_out_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_lateral_run_out_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "lateral_run_out_top_over_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_lateral_run_out_top_over_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "lateral_run_out_bottom_over_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_lateral_run_out_bottom_over_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "lateral_run_out_harmonic_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_lateral_run_out_harmonic_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "lateral_run_out_top_first_harmonic_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_lateral_run_out_top_first_harmonic_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "lateral_run_out_bottom_first_harmonic_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_lateral_run_out_bottom_first_harmonic_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "bulge_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_bulge_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "bulge_top_over_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_bulge_top_over_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "bulge_bottom_over_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_bulge_bottom_over_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "dent_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_dent_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "dent_top_over_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_dent_top_over_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "dent_bottom_over_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_dent_bottom_over_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "balancing_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_balancing_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "upper_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_upper_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "lower_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_lower_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "upper_lower_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_upper_lower_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "static_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_static_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "warehouse_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_warehouse_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "visual_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_visual_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True

                                    if param == "load1":
                                        if (current_val - prev_val) > test_load_tolerance:
                                            shift = True
                                        if not (test_load - test_load_tolerance <= current_val <= test_load + test_load_tolerance):
                                            shift_occurred_spec = True
                                    if param == "airpress1":
                                        if (current_val - prev_val) > pressure_tolerance:
                                            shift = True
                                        if not (pressure - pressure_tolerance <= current_val <= pressure + pressure_tolerance):
                                            shift_occurred_spec = True
                                shift_occurred = shift or shift_occurred_spec
                                spec_value = param_spec_map.get(param)
                                spec_diff = (current_val - spec_value if spec_value is not None and current_val is not None and isinstance(spec_value, (int, float)) and isinstance(current_val, (int, float)) else None)
                                alert_dict = {
                                    "parameter": param,
                                    "current_value": current_val,
                                    "previous_value": prev_val,
                                    "difference": current_val - prev_val if current_val is not None and prev_val is not None else None,
                                    "shift_occurred_thershold": shift,
                                    "spec_value": spec_value,
                                    "spec_difference": spec_diff,
                                    "shift_occurred_spec": shift_occurred_spec,
                                    "shift_occurred": shift_occurred
                                }
                                if param == "load1":
                                    alert_dict["test_load_tolerance"] = test_load_tolerance
                                if param == "airpress1":
                                    alert_dict["pressure_tolerance"] = pressure_tolerance
                                parameter_alerts.append(alert_dict)
                                if shift_occurred:
                                    overall_shift_occurred = True
                        else:
                            for param in conicity_params + yield_params:
                                shift_occurred_thershold = False
                                shift_occurred_spec = False
                                current_val = lot_averages.get(param)
                                if current_val is not None:
                                    if param == "con_kgf" and spec_con_kgf is not None:
                                        if not (-spec_con_kgf <= current_val <= spec_con_kgf):
                                            shift_occurred_spec = True
                                    if param == "cwrfv_oa_kgf" and spec_cwrfv_oa_kgf is not None:
                                        if current_val > spec_cwrfv_oa_kgf:
                                            shift_occurred_spec = True
                                    if param == "ccwrfv_oa_kgf" and spec_ccwrfv_oa_kgf is not None:
                                        if current_val > spec_ccwrfv_oa_kgf:
                                            shift_occurred_spec = True
                                    if param == "cwrfv_oa1h_kgf" and spec_cwrfv_oa1h_kgf is not None:
                                        if current_val > spec_cwrfv_oa1h_kgf:
                                            shift_occurred_spec = True
                                    if param == "ccwrfv_oa1h_kgf" and spec_ccwrfv_oa1h_kgf is not None:
                                        if current_val > spec_ccwrfv_oa1h_kgf:
                                            shift_occurred_spec = True
                                    if param == "lfv_oa_kgf" and spec_lfv_oa_kgf is not None:
                                        if current_val > spec_lfv_oa_kgf:
                                            shift_occurred_spec = True
                                    if param == "lfv_oa_1_kgf" and spec_lfv_oa_1_kgf is not None:
                                        if current_val > spec_lfv_oa_1_kgf:
                                            shift_occurred_spec = True
                                    if param == "upper_g" and spec_upper_g is not None:
                                        if current_val > spec_upper_g:
                                            shift_occurred_spec = True
                                    if param == "lower_g" and spec_lower_g is not None:
                                        if current_val > spec_lower_g:
                                            shift_occurred_spec = True
                                    if param == "up_low_g" and spec_up_low_g is not None:
                                        if current_val > spec_up_low_g:
                                            shift_occurred_spec = True
                                    if param == "static_g" and spec_static is not None:
                                        if current_val > spec_static:
                                            shift_occurred_spec = True
                                
                                ############### Add 16-07 start ######################

                                    if param in yield_params:
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if current_val < spec_yield:
                                                shift_occurred_spec = True
                                    if param == "load1" and test_load is not None and test_load_tolerance is not None:
                                        if not (test_load - test_load_tolerance <= current_val <= test_load + test_load_tolerance):
                                            shift_occurred_spec = True
                                    if param == "airpress1" and pressure is not None and pressure_tolerance is not None:
                                        if not (pressure - pressure_tolerance <= current_val <= pressure + pressure_tolerance):
                                            shift_occurred_spec = True
                                shift_occurred = shift_occurred_thershold or shift_occurred_spec
                                spec_value = param_spec_map.get(param)
                                spec_diff = (current_val - spec_value if spec_value is not None and current_val is not None and isinstance(spec_value, (int, float)) and isinstance(current_val, (int, float)) else None)
                                alert_dict = {
                                    "parameter": param,
                                    "current_value": current_val,
                                    "previous_value": None,
                                    "difference": None,
                                    "shift_occurred_thershold": shift_occurred_thershold,
                                    "spec_value": spec_value,
                                    "spec_difference": spec_diff,
                                    "shift_occurred_spec": shift_occurred_spec,
                                    "shift_occurred": shift_occurred
                                }
                                if param == "load1":
                                    alert_dict["test_load_tolerance"] = test_load_tolerance
                                if param == "airpress1":
                                    alert_dict["pressure_tolerance"] = pressure_tolerance
                                parameter_alerts.append(alert_dict)
                                if shift_occurred:
                                    overall_shift_occurred = True

                        shift_alerts = [alert for alert in parameter_alerts if alert["shift_occurred"]]
                        logging.debug(f"Model {model_number}, Machine {machine_no}, Lot {current_curing_machine_model_lot_id}: {len(shift_alerts)} parameter alerts with shift_occurred=True")
                        if shift_alerts:
                            logging.debug(f"Shift alerts: {[alert['parameter'] for alert in shift_alerts]}")

                        overall_shift_occurred = bool(overall_shift_occurred)

                        deviated_parameter_alerts = [
                            alert for alert in parameter_alerts
                            if alert.get("shift_occurred_thershold", False) or alert.get("shift_occurred_spec", False)
                        ]

                        for alert in deviated_parameter_alerts:
                            param = alert.get("parameter", "")
                            if param.endswith("_yield"):
                                if param in rank_param_mappings:
                                    rank_counts = {}
                                    for col in rank_param_mappings[param]:
                                        if col in batch_data.columns:
                                            value_counts = batch_data[col].value_counts().to_dict()
                                            for rank_value, count in value_counts.items():
                                                rank_value = int(rank_value) if isinstance(rank_value, (int, float)) else rank_value
                                                rank_counts[f"{col}_{rank_value}"] = int(count)
                                    alert["rank_counts"] = rank_counts

                        if deviated_parameter_alerts:
                            deviated_doc = {
                                "alert_id_curing_ufm": last_alert_id + 1,
                                "modelno": model_number,
                                "lot_id": unique_lot_id_current,
                                "machine": machine_no,
                                "comments": [],
                                "alert_assign_user": [],
                                "curing_machine_model_lot_id": current_curing_machine_model_lot_id,
                                "curing_previous_machine_model_lot_id": curing_previous_machine_model_lot_id,
                                "parameter_alerts": deviated_parameter_alerts,
                                "timestamp_alert_generated": some_function()
                            }
                            deviated_triggering_bulk_operations.append(pymongo.InsertOne(deviated_doc))
                            logging.info(f"Prepared {len(deviated_parameter_alerts)} derived alerts for machine {machine_no}, lot {unique_lot_id_current}")

                        parameter_alerts = [
                            alert for alert in parameter_alerts
                            if alert.get("parameter", "").endswith("_yield") or not alert.get("parameter", "").endswith("_yield")
                        ]

                        for alert in parameter_alerts:
                            param = alert.get("parameter", "")
                            if (
                                (alert.get("shift_occurred_thershold", False) or alert.get("shift_occurred_spec", False))
                                and param.endswith("_yield")
                            ):
                                if param in rank_param_mappings:
                                    rank_counts = {}
                                    for col in rank_param_mappings[param]:
                                        if col in batch_data.columns:
                                            value_counts = batch_data[col].value_counts().to_dict()
                                            for rank_value, count in value_counts.items():
                                                rank_value = int(rank_value) if isinstance(rank_value, (int, float)) else rank_value
                                                rank_counts[f"{col}_{rank_value}"] = int(count)
                                    alert["rank_counts"] = rank_counts

                        # ------------------------------------------------------------------
                        #  ORIGINAL (incorrect – one entry per shift even if it spans many days)
                        # ------------------------------------------------------------------
                        # batch_data["tbm_shift"] = batch_data["date_of_tbm"].apply(get_shift)
                        # shift_groups = batch_data.groupby('tbm_shift')
                        # shifts_data_list_tbm = []
                        # for shift_name, shift_data in shift_groups:
                        #     shift_start_time = shift_data["date_of_tbm"].iloc[0]
                        #     shift_end_time = shift_data["date_of_tbm"].iloc[-1]
                        #     shifts_data_list_tbm.append({
                        #         "shift": shift_name,
                        #         "barcode_count": len(shift_data["barcode"]) if "barcode" in shift_data.columns else 0,
                        #         "machine_shift_tbm_start_time": shift_start_time,
                        #         "machine_shift_tbm_end_time": shift_end_time,
                        #         "barcodes": shift_data["barcode"].tolist() if "barcode" in shift_data.columns else []
                        #     })
                        # ------------------------------------------------------------------
                        #  NEW (correct – one entry per **continuous** shift block)
                        # ------------------------------------------------------------------
                        batch_data["tbm_shift"] = batch_data["date_of_tbm"].apply(get_shift)

                        # Sort by date_of_tbm to guarantee chronological order
                        batch_data = batch_data.sort_values("date_of_tbm").reset_index(drop=True)

                        shifts_data_list_tbm = []
                        current_shift = None
                        current_start = None
                        current_barcodes = []

                        for idx, row in batch_data.iterrows():
                            this_shift = row["tbm_shift"]
                            this_dt    = row["date_of_tbm"]
                            this_bc    = row.get("barcode")

                            if this_shift != current_shift or current_shift is None:
                                # finish previous block (if any)
                                if current_shift is not None:
                                    shifts_data_list_tbm.append({
                                        "shift": current_shift,
                                        "barcode_count": len(current_barcodes),
                                        "machine_shift_tbm_start_time": current_start,
                                        "machine_shift_tbm_end_time":   batch_data.at[idx-1, "date_of_tbm"],
                                        "barcodes": current_barcodes[:]
                                    })
                                # start new block
                                current_shift   = this_shift
                                current_start   = this_dt
                                current_barcodes = [this_bc] if this_bc is not None else []
                            else:
                                current_barcodes.append(this_bc if this_bc is not None else None)

                        # do NOT forget the last block
                        if current_shift is not None:
                            shifts_data_list_tbm.append({
                                "shift": current_shift,
                                "barcode_count": len(current_barcodes),
                                "machine_shift_tbm_start_time": current_start,
                                "machine_shift_tbm_end_time":   batch_data["date_of_tbm"].iloc[-1],
                                "barcodes": current_barcodes
                            })


                        batch_data["curing_shift"] = batch_data["date_of_curing"].apply(get_shift)

                        shift_groups = batch_data.groupby('curing_shift')
                        shifts_data_list = []
                        for shift_name, shift_data in shift_groups:
                            shift_start_time = shift_data["date_of_curing"].iloc[0]
                            shift_end_time = shift_data["date_of_curing"].iloc[-1]
                            shifts_data_list.append({
                                "shifts": shift_name,
                                "barcode_count": len(shift_data["barcode"]),
                                "machine_shift_curing_start_time": shift_start_time,
                                "machine_shift_curing_end_time": shift_end_time,
                                "barcodes": shift_data["barcode"].tolist() if "barcode" in shift_data.columns else []
                            })

                        any_shift_flag = any(alert["shift_occurred"] for alert in parameter_alerts)

                        averages_doc = {
                            "modelno": model_number,
                            "lot_id": unique_lot_id_current,
                            "previous_lot_id": previous_lot_id,
                            "machine": machine_no,
                            "curing_machine_model_lot_id": current_curing_machine_model_lot_id,
                            "curing_previous_machine_model_lot_id": curing_previous_machine_model_lot_id,
                            "current_lot_averages": lot_averages,
                            "grouped_averages": grouped_averages,
                            "previous_lot_averages": prev_lot_avg,
                            "parameter_alerts": parameter_alerts,
                            "curing_shift_occurred": overall_shift_occurred,
                            "processed_machine_barcodes": int(processed_machine_barcodes),
                            "target_machine_lot_size": target_lot_size,
                            "balance_machine_barcodes": int(balance_machine_barcodes),
                            "model_processed_barcodes": int(batch_size),
                            "model_target_lot_size": target_lot_size,
                            "model_balance_barcodes": int(balance_barcodes),
                            "model_total_barcodes": int(total_barcodes_model),
                            "batch_indices": list(batch_indices),
                            "timestamp_alert_generated": some_function()
                        }

                        averages_query = {
                            "modelno": model_number,
                            "lot_id": unique_lot_id_current,
                            "curing_machine_model_lot_id": current_curing_machine_model_lot_id
                        }
                        if not averages_collect.find_one(averages_query):
                            averages_bulk_operations.append(pymongo.InsertOne(averages_doc))

                        current_key = (model_number, machine_no, current_curing_machine_model_lot_id)
                        machine_lot_avg_map[current_key] = lot_averages
                        curing_previous_machine_model_lot_ids[(model_number, machine_no)] = current_curing_machine_model_lot_id

                        any_shift_flag = any(alert["shift_occurred"] for alert in parameter_alerts)
                        machine_alert = {
                            # "machine": machine_no,
                            "machine": machine_no[1:] if machine_no.startswith('M') else machine_no,
                            "tbm_machine": tbm_machine_value,  # MODIFIED: tbm_machine may be None if not created
                            "curing_shift_occured": any_shift_flag,
                            "shifts_data": shifts_data_list,
                            "shifts_data_tbm": shifts_data_list_tbm,
                            "ufm_yield": ufm_yield(batch_data),
                            "rorank_yield": rorank_yield(batch_data),
                            "balancing_yield": balancing_yield(batch_data),
                            "total_rank_yield": total_rank_yield(batch_data),
                            "visual_yield": visual_yield(batch_data),
                            "sizecode": sizecode,
                            "gt_code": str(gt_code),
                            "timestamp_lot_started": lot_start_time,
                            "timestamp_lot_ended": lot_end_time,
                            "target_machine_lot_size": target_lot_size,
                            "processed_machine_barcodes": int(processed_machine_barcodes),
                            "balance_machine_barcodes": int(balance_machine_barcodes),
                            "total_machine_barcodes": int(total_curing_barcodes),
                            "machine_lot_start_build_time": machine_lot_start_build_time,
                            "machine_lot_end_build_time": machine_lot_end_build_time,
                            "machine_lot_start_curing_time": machine_lot_start_curing_time,
                            "machine_lot_end_curing_time": machine_lot_end_curing_time,
                            "tub_date_start_time": tub_date_start_time,
                            "tub_date_end_time": tub_date_end_time,
                            "parameter_alerts": parameter_alerts,
                            "lot_averages": lot_averages,
                            "grouped_averages": grouped_averages,
                            "previous_lot_averages": machine_lot_avg_map.get((model_number, machine_no, curing_previous_machine_model_lot_id), {}),
                            "first_barcode_machine": batch_data.iloc[0]["barcode"] if "barcode" in batch_data.columns else None,
                            "last_barcode_machine": batch_data.iloc[-1]["barcode"] if "barcode" in batch_data.columns else None,
                            "curing_machine_model_lot_id": current_curing_machine_model_lot_id,
                            "curing_previous_machine_model_lot_id": curing_previous_machine_model_lot_id,
                            "rank_columns": get_rank_columns(batch_data),
                            "barcodes": batch_data["barcode"].tolist()
                        }

                        alert_key = (model_number, unique_lot_id_current)
                        if alert_key not in alert_map:
                            last_alert_id += 1
                            alert_map[alert_key] = {
                                "alert_id_curing_ufm": last_alert_id,
                                "modelno": model_number,
                                "lot_id": unique_lot_id_current,
                                "machine_alerts": [],
                                "first_barcode_model": None,
                                "last_barcode_model": None
                            }
                        alert_map[alert_key]["machine_alerts"].append(machine_alert)
                        if machine_alert["curing_shift_occured"]:
                            alert_map[alert_key]["shift_occurred"] = True
                        if alert_map[alert_key]["first_barcode_model"] is None:
                            alert_map[alert_key]["first_barcode_model"] = machine_alert["first_barcode_machine"]
                        alert_map[alert_key]["last_barcode_model"] = machine_alert["last_barcode_machine"]

                        if overall_shift_occurred:
                            merge_df.loc[batch_indices, "alert_id_curing_ufm"] = last_alert_id

                        barcode_counter += batch_size

                for alert_key, alert_obj in alert_map.items():
                    final_alert = {
                        "alert_id_curing_ufm": alert_obj["alert_id_curing_ufm"],
                        "modelno": alert_obj["modelno"],
                        "lot_id": alert_obj["lot_id"],
                        "alert_flag": False,
                        "dashboard_flag": False,
                        "machine_flag": False,
                        "alert_status_check":"o",
                        "alert_status_timestamp": None,
                        "alert_status_comment": [],
                        "alert_assign_user": [],
                        "machine_alerts": alert_obj["machine_alerts"],
                        "first_barcode_model": alert_obj["first_barcode_model"],
                        "last_barcode_model": alert_obj["last_barcode_model"],
                        "timestamp_alert_generated": some_function()
                    }
                    alert_bulk_operations.append(pymongo.InsertOne(final_alert))
                    
                    
                # === AFTER alert_map is built - INSERT VIRGIN & FINAL YIELD (UNCHANGED + NEW) ===
                for alert_key, alert_obj in alert_map.items():
                    # true_machine_alerts = [ma for ma in alert_obj["machine_alerts"] if ma.get("tbm_shift_occurred", False)]
                    true_machine_alerts = [ma for ma in alert_obj["machine_alerts"] if ma.get("curing_shift_occured", False) in [True, False]]  # Keep both True/False
                    if not true_machine_alerts:
                        continue

                    # Pre-compute barcode → row mapping
                    virgin_barcode_to_row = {row["barcode"]: row for _, row in virgin_data.iterrows()}
                    final_barcode_to_row = {row["barcode"]: row for _, row in final_data.iterrows()}

                    # === 1. VIRGIN YIELD: YOUR ORIGINAL CODE (100% UNCHANGED) ===
                    virgin_machine_alerts = []
                    virgin_all_barcodes = set()
                    for ma in true_machine_alerts:
                        original_barcodes = ma.get("barcodes", [])
                        if not original_barcodes:
                            continue
                        filtered_barcodes = []
                        for bc in original_barcodes:
                            bc_str = str(bc)
                            if bc_str in virgin_barcode_to_row and bc_str not in virgin_all_barcodes:
                                filtered_barcodes.append(bc_str)
                                virgin_all_barcodes.add(bc_str)
                        if filtered_barcodes:
                            virgin_ma = ma.copy()
                            virgin_ma["barcodes"] = filtered_barcodes
                            virgin_ma["barcode_count"] = len(filtered_barcodes)
                            virgin_machine_alerts.append(virgin_ma)

                    if virgin_machine_alerts and virgin_all_barcodes:
                        all_virgin_bcs = sorted(virgin_all_barcodes,
                            key=lambda x: virgin_data[virgin_data["barcode"] == x]["date_of_dt"].iloc[0])
                        doc_virgin = {
                            "alert_id_curing_ufm": alert_obj["alert_id_curing_ufm"],
                            "modelno": alert_obj["modelno"],
                            "lot_id": alert_obj["lot_id"],
                            "alert_flag": False,
                            "dashboard_flag": False,
                            "machine_flag": False,
                            "alert_status_check": "o",
                            "alert_status_timestamp": None,
                            "alert_status_comment": [],
                            "alert_assign_user": [],
                            "machine_alerts": virgin_machine_alerts,
                            "first_barcode_model": all_virgin_bcs[0] if all_virgin_bcs else None,
                            "last_barcode_model": all_virgin_bcs[-1] if all_virgin_bcs else None,
                            "timestamp_alert_generated": some_function(),
                            "yield_type": "virgin"
                        }
                        alert_triggering_virgin_yield_collect.insert_one(doc_virgin)  # UNCHANGED

                    # === 2. FINAL YIELD: YOUR ORIGINAL CODE (KEPT 100%) ===
                    final_machine_alerts = []
                    final_all_barcodes = set()
                    for ma in true_machine_alerts:
                        original_barcodes = ma.get("barcodes", [])
                        if not original_barcodes:
                            continue
                        filtered_barcodes = []
                        for bc in original_barcodes:
                            bc_str = str(bc)
                            if bc_str in final_barcode_to_row and bc_str not in final_all_barcodes:
                                filtered_barcodes.append(bc_str)
                                final_all_barcodes.add(bc_str)
                        if filtered_barcodes:
                            final_ma = ma.copy()
                            final_ma["barcodes"] = filtered_barcodes
                            final_ma["barcode_count"] = len(filtered_barcodes)
                            final_machine_alerts.append(final_ma)

                    if final_machine_alerts and final_all_barcodes:
                        all_final_bcs = sorted(final_all_barcodes,
                            key=lambda x: final_data[final_data["barcode"] == x]["date_of_dt"].iloc[0])

                        # === ORIGINAL INSERT (KEPT) ===
                        doc_final = {
                            "alert_id_curing_ufm": alert_obj["alert_id_curing_ufm"],
                            "modelno": alert_obj["modelno"],
                            "lot_id": alert_obj["lot_id"],
                            "alert_flag": False,
                            "dashboard_flag": False,
                            "machine_flag": False,
                            "alert_status_check": "o",
                            "alert_status_timestamp": None,
                            "alert_status_comment": [],
                            "alert_assign_user": [],
                            "machine_alerts": final_machine_alerts,
                            "first_barcode_model": all_final_bcs[0] if all_final_bcs else None,
                            "last_barcode_model": all_final_bcs[-1] if all_final_bcs else None,
                            "timestamp_alert_generated": some_function(),
                            "yield_type": "final"
                        }
                        alert_triggering_final_yield_collect.insert_one(doc_final)  # YOUR ORIGINAL - UNCHANGED

                        # === NEW: Insert into alert_table_final_yield (DEDICATED FINAL YIELD DB) ===
                        doc_final_only = doc_final.copy()
                        doc_final_only["curing_shift_occured"] = alert_obj.get("shift_occurred", False)
                        final_yield_only_bulk.append(pymongo.InsertOne(doc_final_only))

                        # === NEW: Extract Deviated Parameters from alert_db_final_yield (alert_triggering_final_yield_collect) ===
                        deviated_params = []

                        # Fetch the already-saved Final Yield alert document using alert_id_curing_ufm
                        existing_final_yield_doc = alert_triggering_final_yield_collect.find_one(
                            {"alert_id_curing_ufm": alert_obj["alert_id_curing_ufm"]},
                            {"machine_alerts.parameter_alerts": 1, "machine_alerts.barcodes": 1, "machine_alerts.machine": 1, "machine_alerts.curing_machine_model_lot_id": 1, "machine_alerts.curing_previous_machine_model_lot_id": 1}
                        )

                        if existing_final_yield_doc:
                            machine_alerts = existing_final_yield_doc.get("machine_alerts", [])
                            for ma in machine_alerts:
                                param_alerts = ma.get("parameter_alerts", [])
                                barcodes_in_ma = ma.get("barcodes", [])
                                machine_no_final = ma.get("machine", "unknown")
                                machine_current_curing_machine_model_lot_id = ma.get("curing_machine_model_lot_id", "unknoum")
                                machine_curing_previous_machine_model_lot_id = ma.get("curing_previous_machine_model_lot_id", None)

                                for alert in param_alerts:
                                    if alert.get("shift_occurred_thershold", False) or alert.get("shift_occurred_spec", False):
                                        param = alert.get("parameter", "")

                                        # Only add rank_counts for yield parameters
                                        if param.endswith("_yield") and param in rank_param_mappings and barcodes_in_ma:
                                            # Fetch actual tire data for these barcodes (last occurrence = Final Yield)
                                            tire_data = final_data[final_data["barcode"].isin(barcodes_in_ma)]
                                            rank_counts = {}

                                            for col in rank_param_mappings[param]:
                                                if col in tire_data.columns:
                                                    counts = tire_data[col].value_counts().to_dict()
                                                    for val, cnt in counts.items():
                                                        val = int(val) if isinstance(val, (int, float)) else val
                                                        rank_counts[f"{col}_{val}"] = int(cnt)
                                            alert["rank_counts"] = rank_counts

                                        deviated_params.append(alert)

                        # === Insert into alert_deviated_final_yield ===
                        if deviated_params:
                            deviated_doc = {
                                "alert_id_curing_ufm": alert_obj["alert_id_curing_ufm"],
                                "modelno": alert_obj["modelno"],
                                "lot_id": alert_obj["lot_id"],
                                "machine": machine_no_final if 'machine_no_final' in locals() else "unknown",
                                "comments": [],
                                "alert_assign_user": [],
                                "curing_machine_model_lot_id": machine_current_curing_machine_model_lot_id,
                                "curing_previous_machine_model_lot_id": machine_curing_previous_machine_model_lot_id,
                                "parameter_alerts": deviated_params,
                                "timestamp_alert_generated": some_function(),
                                "yield_type": "final",
                                "source": "alert_db_final_yield"  # Optional: for traceability
                            }
                            final_yield_deviated_only_bulk.append(pymongo.InsertOne(deviated_doc))
                        # === ADD THESE 2 LINES ONLY ===
                        triggering_bulk_operations.append(pymongo.InsertOne(final_alert))                    # → alert_db
                        alert_triggering_collect_dashboard_curing.bulk_write([pymongo.InsertOne(final_alert)], ordered=False)  # → alert_db_dashboard
                        # ================================

        except Exception as e:
            logging.error(f"Error in model loop: {e}")
            continue

    # === BULK WRITE NEW DATABASES ===
    if final_yield_only_bulk:
        try:
            result = alert_final_yield_collect.bulk_write(final_yield_only_bulk, ordered=False)
            inserted_counts["alert_table_final_yield"] = result.inserted_count
            output_messages.append(f"Inserted {result.inserted_count} docs → alert_table_final_yield.alert_curing_collect_name")
        except Exception as e:
            output_messages.append(f"Error alert_table_final_yield: {e}")

    if final_yield_deviated_only_bulk:
        try:
            result = alert_triggering_deviated_final_yield_collect.bulk_write(final_yield_deviated_only_bulk, ordered=False)
            inserted_counts["alert_deviated_final_yield"] = result.inserted_count
            output_messages.append(f"Inserted {result.inserted_count} deviated docs → alert_deviated_final_yield.alert_triggering_deviated_curing")
        except Exception as e:
            output_messages.append(f"Error alert_deviated_final_yield: {e}")

    if bulk_operations:
        try:
            result = lot_curing_collect.bulk_write(bulk_operations, ordered=False)
            inserted_counts["lot_curing_table"] = result.inserted_count
            output_messages.append(f"✅ Curing Inserted {result.inserted_count} documents into lot_curing_table.")
        except Exception as e:
            error_message = f"❌ Curing Error performing bulk write to lot_curing_table: {e}"
            output_messages.append(error_message)

    if alert_bulk_operations:
        try:
            result = alert_collect.bulk_write(alert_bulk_operations, ordered=False)
            inserted_counts["alert_curing_table_with_lot_size"] = result.inserted_count
            output_messages.append(f"✅ Curing Inserted {result.inserted_count} alert documents into alert_curing_table_with_lot_size.")
        except Exception as e:
            error_message = f"❌ Curing Error performing bulk write to alert_curing_table_with_lot_size: {e}"
            output_messages.append(error_message)
    else:
        output_messages.append("No Curing alert documents to insert into alert_curing_table_with_lot_size.")

    if deviated_triggering_bulk_operations:
        try:
            result = alert_triggering_deviated_collect.bulk_write(deviated_triggering_bulk_operations, ordered=False)
            inserted_counts["alert_curing_with_lot_size"] = result.inserted_count
            output_messages.append(f"✅ TBM Inserted {result.inserted_count} deviated alert documents into alert_deviated.alert_curing_with_lot_size.")
        except Exception as e:
            error_message = f"❌ TBM Error performing bulk write to alert_deviated.alert_curing_with_lot_size: {e}"
            output_messages.append(error_message)
    else:
        output_messages.append("No TBM deviated alert documents to insert into alert_deviated.alert_curing_with_lot_size.")

    if averages_bulk_operations:
        try:
            result = averages_collect.bulk_write(averages_bulk_operations, ordered=False)
            inserted_counts["averages_curing_table"] = result.inserted_count
            output_messages.append(f"✅ Curing Inserted {result.inserted_count} new averages documents into averages_curing_table.")
        except Exception as e:
            error_message = f"❌ Curing Error performing bulk write to averages_curing_table: {e}"
            output_messages.append(error_message)
    else:
        output_messages.append("No Curing averages documents to insert into averages_curing_table.")

    end_time = time.time()
    execution_time = end_time - start_time

    bulk_updates = []
    for index, row in merge_df.iterrows():
        query = {"_id": row["_id"]}
        update = {"$set": {
            "alert_id_curing_ufm": row["alert_id_curing_ufm"],
            "lot_id": row["lot_id"],
            "previous_lot_id": row["previous_lot_id"],
            "curing_machine_model_lot_id": row["curing_machine_model_lot_id"],
            "curing_previous_machine_model_lot_id": row["curing_previous_machine_model_lot_id"]
        }}
        # MODIFIED: Only add tbm_machine to update if it exists in merge_df
        if "tbm_machine" in merge_df.columns:
            update["$set"]["tbm_machine"] = row["tbm_machine"]
        bulk_updates.append(pymongo.UpdateOne(query, update))

    if bulk_updates:
        try:
            result = merge_collect.bulk_write(bulk_updates, ordered=False)
            updated_counts["data_mereged_dashboard"] = result.modified_count
            output_messages.append(f"✅ Curing Updated {result.modified_count} documents in data_mereged_dashboard collection.")
        except Exception as e:
            error_message = f"❌ Curing Error updating data_mereged_dashboard collection: {e}"
            output_messages.append(error_message)
    else:
        output_messages.append("No Curing documents to update in data_mereged_dashboard collection.")

    if triggering_bulk_operations:
        try:
            triggering_result = alert_triggering_collect.bulk_write(triggering_bulk_operations, ordered=False)
            inserted_counts["alert_triggering_curing_with_lot_size"] = triggering_result.inserted_count
            output_messages.append(f"✅ Curing Inserted {triggering_result.inserted_count} machine_alert documents into alert_triggering_curing_with_lot_size.")
            result_dashboard = alert_triggering_collect_dashboard_curing.bulk_write(triggering_bulk_operations, ordered=False)
            inserted_counts["alert_triggering_curing_dashboard"] = result_dashboard.inserted_count
            output_messages.append(f"✅ TBM Inserted {result_dashboard.inserted_count} machine_alert documents into alert_db_dashboard.alert_triggering_curing_with_lot_size.")
        except Exception as e:
            error_message = f"❌ Curing Error performing bulk write to alert_triggering_curing_with_lot_size collection: {e}"
            output_messages.append(error_message)
    else:
        output_messages.append("No Curing machine_alert documents to insert into alert_triggering_curing_with_lot_size collection.")

    final_message = f"✅ Curing Process completed successfully in {execution_time:.2f} seconds!"
    output_messages.append(final_message)

    logging.info("process_curing_data_and_generate_lots_alerts function is completed")
    return {"status": "success", "message": final_message, "output_log": output_messages,
            "inserted_counts": inserted_counts, "updated_counts": updated_counts, "execution_time": execution_time}, 200

def process_lot_and_alert_data():
    process_start_time = time.time()
    output_messages = []
    inserted_counts = {}
    updated_counts = {}
    logging.info("process_lot_and_alert_data function is just started")
    try:
        # UFM Specific Collections
        merge_collect = client[TUB_CIMCORP_DB][tub_collection_name]
        channel_collect = client[config_databse][channel_no_collect]
        machine_collect = client[config_databse][machine_no_collect]
        lot_ufm_collect = client[lot_db][lot_table_collect]
        alert_collect = client[alert_table][alert_ufm_collect_name]
        averages_ufm_collect = client[average_db][averages_ufm_table]
        ub_machine_collect = client[config_databse][ub_machine_no_collect]
        alert_ufm_collect = client[alert_db][alert_triggering_ubno_collect_name]
        alert_triggering_deviated_collect = client[alert_deviated][alert_triggering_deviated_ufm]
        alert_triggering_collect_dashboard_ufm = client[alert_db_dashboard][alert_triggering_ubno_collect_name]
        # === NEW: Virgin Yield & Final Yield Collections ===
        alert_triggering_virgin_yield_collect = client[alert_db_virgin_yield][alert_triggering_ubno_collect_name]
        alert_triggering_final_yield_collect = client[alert_db_final_yield][alert_triggering_ubno_collect_name]
        # === NEW: YOUR DEDICATED FINAL YIELD DATABASES (EXACT REQUEST) ===
        alert_final_yield_collect = client["alert_table_final_yield"][alert_ufm_collect_name]  # All Final Yield alerts
        alert_deviated_final_yield_collect = client["alert_deviated_final_yield"][alert_triggering_deviated_ufm]  # Only Final Yield + Shift True
        # NEW: Initialize collections for model_workcenter_db
        ufm_collect_data = client[model_db][ufm_collection]
        ufm_machine_collect = client[model_db][ufm_collection_machine]
        logging.info("✅ UFM Collections loaded successfully for alert_db and alert_db_dashboard.")
        output_messages.append("✅ UFM Collections loaded successfully for lot and alert processing in alert_db and alert_db_dashboard.")
    except pymongo.errors.OperationFailure as e:
        error_message = f"❌ UFM Database or collection error during processing: {e}"
        logging.error(error_message)
        output_messages.append(error_message)
        return {"status": "error", "message": error_message, "output_log": output_messages}, 500

    # Fetch active machines from model_workcenter_db.ufm_machine
    try:
        active_machines = set(ufm_machine_collect.distinct("ubno", {"status": "active"}))
        output_messages.append(f"✅ Fetched {len(active_machines)} active machines: {active_machines}")
        if not active_machines:
            output_messages.append("❌ No active machines found in model_workcenter_db.ufm_machine. Exiting.")
            return {"status": "error", "message": "No active machines found", "output_log": output_messages}, 500
    except Exception as e:
        output_messages.append(f"❌ Error fetching active machines: {e}")
        return {"status": "error", "message": "Failed to fetch active machines", "output_log": output_messages}, 500


    # Fetch active model numbers
    try:
        active_modelnos = set(ufm_collect_data.distinct("modelno", {"status": "active"}))
        output_messages.append(f"✅ Fetched {len(active_modelnos)} active model numbers: {active_modelnos}")
    except Exception as e:
        output_messages.append(f"❌ Error fetching active model numbers: {e}")
        return {"status": "error", "message": "Failed to fetch active model numbers", "output_log": output_messages}, 500

    # Create unique indexes
    try:
        lot_ufm_collect.create_index(
            [
                ("modelno", pymongo.ASCENDING),
                ("machine", pymongo.ASCENDING),
                ("lot_id", pymongo.ASCENDING),
                ("ufm_machine_model_lot_id", pymongo.ASCENDING)
            ],
            unique=True,
            name="ufm_lot_unique_idx"
        )
        alert_ufm_collect.create_index(
            [
                ("modelno", pymongo.ASCENDING),
                ("lot_id", pymongo.ASCENDING),
                ("alert_id_ufm_ufm", pymongo.ASCENDING)
            ],
            unique=True,
            name="ufm_alert_unique_idx"
        )
        logging.info("✅ UFM Indexes checked/created.")
        output_messages.append("✅ UFM Indexes checked/created.")
    except Exception as e:
        logging.warning("⚠️ Warning: Unable to create/check UFM unique indexes. %s", e)
        output_messages.append(f"⚠️ Warning: Unable to create/check UFM unique indexes: {e}")

    # Load data
    try:
        merge1 = list(merge_collect.find({}))
        if not merge1:
            logging.info("ℹ️ No documents found in the merge collection. Exiting UFM processing.")
            output_messages.append("ℹ️ No documents found in the merge collection.")
            return {"status": "success", "message": "No data to process.", "output_log": output_messages}, 200
        merge_df = pd.DataFrame(merge1)
        channel_data = list(channel_collect.find({"modelno": {"$in": list(active_modelnos)}}))
        channel_data = list(channel_collect.find({}))
        channel_df = pd.DataFrame(channel_data)
        machine_data = list(machine_collect.find({}))
        machine_df = pd.DataFrame(machine_data)
        ub_machine_data = list(ub_machine_collect.find({"ubno": {"$in": list(active_machines)}}))
        ub_machine_data = list(ub_machine_collect.find({}))
        ub_machine_df = pd.DataFrame(ub_machine_data)

        logging.info("✅ UFM Data loaded into DataFrames.")
        output_messages.append("✅ UFM Data loaded into DataFrames.")
    except Exception as e:
        error_message = f"❌ UFM Error loading data: {e}"
        logging.error(error_message)
        output_messages.append(error_message)
        return {"status": "error", "message": error_message, "output_log": output_messages}, 500
    
    # MODIFIED: Check for cimcorp_tbm_tub column before adding tbm_machine column
    if 'cimcorp_tbm_tub' in merge_df.columns and 'tbm' in machine_df.columns and 'machine' in machine_df.columns:
        try:
            merge_df["tbm_machine"] = None  # Initialize tbm_machine column
            for index, row in merge_df.iterrows():
                tbm_value = row.get("cimcorp_tbm_tub")
                if tbm_value:
                    matching_channel = machine_df[machine_df["tbm"] == tbm_value]
                    if not matching_channel.empty:
                        merge_df.at[index, "tbm_machine"] = matching_channel["machine"].iloc[0]  # NEW: Use machine column
            output_messages.append("✅ Added tbm_machine column to merge_df based on matching cimcorp_tbm_tub values.")
        except Exception as e:
            output_messages.append(f"⚠️ Warning: Failed to add tbm_machine column to merge_df: {e}. Continuing without tbm_machine.")
    else:
        output_messages.append("⚠️ Warning: 'cimcorp_tbm_tub' column not found in merge_df or machine_df. Skipping tbm_machine column creation.")

    # MODIFIED: Check for cimcorp_tbm_tub column before updating machine_df
    if 'tbm' in machine_df.columns and 'cimcorp_tbm_tub' in merge_df.columns and 'tbm_machine' in merge_df.columns:
        try:
            for index, row in machine_df.iterrows():
                tbm_value = row.get("tbm")
                if tbm_value:
                    matching_merge = merge_df[merge_df["cimcorp_tbm_tub"] == tbm_value]
                    if not matching_merge.empty and "tbm_machine" in matching_merge.columns:
                        machine_df.at[index, "machine"] = matching_merge["tbm_machine"].iloc[0]
                        # print("at line 6708:", matching_merge["tbm_machine"].iloc[0])
            output_messages.append("✅ Updated machine_df machine column with tbm_machine values from merge_df.")
        except Exception as e:
            output_messages.append(f"⚠️ Warning: Failed to update machine_df machine column: {e}. Continuing without updating channel_df machine.")
    else:
        output_messages.append("⚠️ Warning: 'cimcorp_tbm_tub' column not found in merge_df or machine_df. Skipping machine_df machine column update.")

    # Filter data
    try:
        channel_modelnos = set(channel_collect.distinct("modelno"))
        merge_df = merge_df[merge_df["modelno"].isin(channel_modelnos)]
        logging.info("Filtered merge_df for models in channel config. Rows remaining: %s", len(merge_df))
        output_messages.append(f"Filtered merge_df for models in channel config. Rows remaining: {len(merge_df)}")

        merge_df = merge_df[merge_df["alert_id_ufm_ufm"].isna()]
        logging.info("Filtered merge_df for unassigned alert_id_ufm_ufm. Rows remaining: %s", len(merge_df))
        output_messages.append(f"Filtered merge_df for unassigned alert_id_ufm_ufm. Rows remaining: {len(merge_df)}")

        merge_df = merge_df[merge_df["ufm_machine_model_lot_id"].isna()]
        logging.info("Filtered merge_df for unassigned ufm_machine_model_lot_id. Rows remaining: %s", len(merge_df))
        output_messages.append(f"Filtered merge_df for unassigned ufm_machine_model_lot_id. Rows remaining: {len(merge_df)}")

        if merge_df.empty:
            logging.info("No data required lot for processing UFM.")
            output_messages.append("No valid data after filtering for processing UFM.")
            return {"status": "success", "message": "No new data to process UFM.", "output_log": output_messages}, "no data", 200

    except KeyError as e:
        error_message = f"Failed to filter UFM: Error - DataFrame issue: Column missing {e}. Ensure 'alert_id_ufm_ufm' and 'ufm_machine_model_lot_id' are present in '{tub_collection_name}'."
        logging.error(error_message)
        output_messages.append(error_message)
        return {"status": "error", "message": error_message, "output_log": output_messages}, 500
    except Exception as e:
        error_message = f"Failed to filter UFM data: {e}"
        logging.error(error_message)
        output_messages.append(error_message)
        return {"status": "error", "message": error_message, "output_log": output_messages}, 500

    # Date handling & Preprocessing
    try:
        merge_df["date_of_ufm"] = pd.to_datetime(merge_df["date_and_time"], errors="coerce").dt.strftime('%Y-%m-%d %H:%M:%S').fillna("No Date")
        merge_df["date_of_tbm"] = merge_df["tbm_buildtime_tub"]
        # print("At line 4085:", merge_df["date_of_tbm"])

        def convert_datetime_with_invalid_mask(series, col_name):
            try:
                original_series = series.copy()
                cleaned_series = original_series.astype(str).str.strip().str.replace(r'\s+', ' ', regex=True)
                datetime_series = pd.to_datetime(cleaned_series, format='%Y-%m-%d %H:%M:%S', errors='coerce')
                invalid_mask = datetime_series.isna() & original_series.notna()
                datetime_series[invalid_mask] = DEFAULT_DATETIME
                return datetime_series.dt.strftime('%Y-%m-%d %H:%M:%S')
            except Exception as e:
                logging.error(f"Error in convert_datetime_with_invalid_mask for {col_name}: {e}")
                return pd.Series([DEFAULT_DATETIME.strftime('%Y-%m-%d %H:%M:%S')] * len(series))

        merge_df["date_of_ufm"] = convert_datetime_with_invalid_mask(merge_df["date_of_ufm"], "date_of_ufm")
        merge_df["date_of_ufm"] = merge_df["date_of_ufm"].astype(object).where(merge_df["date_of_ufm"].notnull(), "No Date")
        merge_df["date_of_tbm"] = convert_datetime_with_invalid_mask(merge_df["date_of_tbm"], "date_of_tbm")
        merge_df["date_of_tbm"] = merge_df["date_of_tbm"].astype(object).where(merge_df["date_of_tbm"].notnull(), "No Date")

        merge_df["tbm_shift"] = merge_df["date_of_tbm"].apply(get_shift)
        merge_df["shift"] = merge_df["date_of_ufm"].apply(get_shift)


        merge_df["ubno"] = merge_df["ubno"].astype(str)
        ub_machine_df["ubno"] = ub_machine_df["ubno"].astype(str)
        # NEW: Filter merge_df to include only active machines
        merge_df = merge_df[merge_df["ubno"].isin(active_machines)]
        if merge_df.empty:
            output_messages.append("❌ No data in merge_df after filtering for active machines. Exiting.")
            return {"status": "error", "message": "No data after active machine filtering", "output_log": output_messages}, 500

        for col in ["lot_id", "previous_lot_id", "ufm_machine_model_lot_id", "ufm_previous_machine_model_lot_id", "alert_id_ufm_ufm"]:
            if col not in merge_df.columns:
                merge_df[col] = None

        logging.info("✅ UFM Date conversion and preprocessing completed.")
        output_messages.append("✅ UFM Date conversion and preprocessing completed.")
    except Exception as e:
        error_message = f"❌ UFM Error during date/preprocessing: {e}"
        logging.error(error_message)
        output_messages.append(error_message)
        return {"status": "error", "message": error_message, "output_log": output_messages}, 500

    # Fetch last alert ID
    try:
        last_alert_doc = alert_ufm_collect.find_one(sort=[("alert_id_ufm_ufm", -1)])
        last_alert_id = last_alert_doc["alert_id_ufm_ufm"] if last_alert_doc and "alert_id_ufm_ufm" in last_alert_doc else 0
        logging.info("ℹ️ Last UFM Alert ID found: %s", last_alert_id)
        output_messages.append(f"ℹ️ Last UFM Alert ID found: {last_alert_id}")
    except Exception as e:
        logging.warning("⚠️ UFM Warning fetching last alert ID: %s. Starting from 0.", e)
        output_messages.append(f"⚠️ UFM Warning fetching last alert ID: {e}. Starting from 0.")
        last_alert_id = 0

    # Fetch last lot numbers per machine-model combination
    try:
        last_lots_agg = list(lot_ufm_collect.aggregate([
            {"$match": {"lot_id": {"$regex": "^UFM_[^_]+_[^_]+_\\d+$"},
                        "modelno": {"$in": list(active_modelnos)},
                        "machine": {"$in": list(active_machines)}}},
            {"$addFields": {
                "lot_number": {
                    "$toInt": {"$arrayElemAt": [{"$split": ["$lot_id", "_"]}, -1]}
                },
                "modelno": "$modelno",
                "machine": "$machine"
            }},
            {"$sort": {"lot_number": -1}},
            {"$group": {
                "_id": {"modelno": "$modelno", "machine": "$machine"},
                "max_lot_number": {"$first": "$lot_number"},
                "last_lot_id": {"$first": "$lot_id"}
            }}
        ]))
        last_lot_dict = {
            (item["_id"]["modelno"], item["_id"]["machine"]): {
                "max_lot_number": item["max_lot_number"],
                "last_lot_id": item["last_lot_id"]
            } for item in last_lots_agg
        }
        # logging.info("ℹ️ Last UFM Lot numbers fetched: %s", last_lot_dict)
        output_messages.append(f"ℹ️ Last UFM Lot numbers fetched: {len(last_lot_dict)} entries")
    except Exception as e:
        logging.warning("⚠️ UFM Error fetching last lot numbers: %s. Starting counts from 0.", e)
        output_messages.append(f"⚠️ UFM Error fetching last lot numbers: {e}. Starting counts from 0.")
        last_lot_dict = {}

    # Initialize tracking variables
    ufm_previous_machine_model_lot_ids = {}
    ufm_machine_lot_avg_map = {}
    ufm_alert_bulk_operations = []
    ufm_averages_bulk_operations = []
    ufm_lot_bulk_operations = []
    deviated_triggering_bulk_operations = []
    ufm_bulk_operations = []
    ufm_triggering_bulk_operations = []

    # Pre-populate ufm_previous_machine_model_lot_ids and ufm_machine_lot_avg_map
    try:
        last_averages = list(averages_ufm_collect.aggregate([
            {
                "$match": {
                    "ufm_machine_model_lot_id": {"$regex": "^[^_]+_[^_]+_UFM_[^_]+_[^_]+_\\d+_\\d+$"},
                    "modelno": {"$in": list(active_modelnos)},
                    "machine": {"$in": list(active_machines)}
                }
            },
            {
                "$addFields": {
                    "lot_number": {
                        "$toInt": {"$arrayElemAt": [{"$split": ["$lot_id", "_"]}, -1]}
                    }
                }
            },
            {
                "$sort": {"lot_number": -1}
            },
            {
                "$group": {
                    "_id": {"modelno": "$modelno", "machine": "$machine"},
                    "last_ufm_lot_id": {"$first": "$ufm_machine_model_lot_id"},
                    "last_averages": {"$first": "$current_lot_averages"}
                }
            }
        ]))
        for item in last_averages:
            modelno = item["_id"]["modelno"]
            machine = item["_id"]["machine"]
            ufm_lot_id = item["last_ufm_lot_id"]
            averages = item["last_averages"]
            ufm_previous_machine_model_lot_ids[(modelno, machine)] = ufm_lot_id
            if averages:
                ufm_machine_lot_avg_map[(modelno, machine, ufm_lot_id)] = averages
                logging.info(f"Pre-populated ufm_machine_lot_avg_map with averages for {ufm_lot_id}")
            else:
                logging.warning(f"No averages found for {ufm_lot_id} in averages_ufm_collect")
        # logging.info(f"Initialized ufm_previous_machine_model_lot_ids: {ufm_previous_machine_model_lot_ids}")
        output_messages.append(f"Initialized ufm_previous_machine_model_lot_ids with {len(ufm_previous_machine_model_lot_ids)} entries")
    except Exception as e:
        logging.error(f"Error pre-populating ufm_machine_lot_avg_map and ufm_previous_machine_model_lot_ids: {e}")
        output_messages.append(f"❌ Error pre-populating ufm_machine_lot_avg_map and ufm_previous_machine_model_lot_ids: {e}")

    ufm_params = [
        "con_kgf", "cwrfv_oa_kgf", "ccwrfv_oa_kgf", "cwrfv_oa1h_kgf", "ccwrfv_oa1h_kgf",
        "lfv_oa_kgf", "lfv_oa_1_kgf", "upper_g", "lower_g", "up_low_g", "static_g",
        "load1", "airpress1"
    ]
    yield_params = [
        "total_rank_yield", "rfv_yield", "rfh1_yield", "lfv_yield", "conicity_yield",
        "ufm_yield", "rorank_yield", "radial_runout_yield", "radial_runout_first_harmonic_yield",
        "lateral_run_out_yield", "lateral_run_out_top_over_yield", "lateral_run_out_bottom_over_yield",
        "lateral_run_out_harmonic_yield", "lateral_run_out_top_first_harmonic_yield",
        "lateral_run_out_bottom_first_harmonic_yield", "bulge_yield", "bulge_top_over_yield",
        "bulge_bottom_over_yield", "dent_yield", "dent_top_over_yield", "dent_bottom_over_yield",
        "balancing_yield", "upper_yield", "lower_yield", "upper_lower_yield", "static_yield",
        "warehouse_yield", "visual_yield"
    ]

    rank_param_mappings = {
        "total_rank_yield": ["total_rank"],
        "rfv_yield": ["cwrfv_oa_rank", "ccwrfv_oa_rank"],
        "rfh1_yield": ["cwrfv_oa1h_rank", "ccwrfv_oa1h_rank"],
        "lfv_yield": ["lfv_oa_rank", "lfv_oa_1_rank"],
        "conicity_yield": ["con_rank"],
        "ufm_yield": ["ufm_rank"],
        "rorank_yield": ["rorank"],
        "radial_runout_yield": ["rc_oa_rank"],
        "radial_runout_first_harmonic_yield": ["rc_oa1h_rank"],
        "lateral_run_out_top_over_yield": ["lt_oa_rank"],
        "lateral_run_out_bottom_over_yield": ["lb_oa_rank"],
        "lateral_run_out_top_first_harmonic_yield": ["lt_oa1h_rank"],
        "lateral_run_out_bottom_first_harmonic_yield": ["lb_oa1h_rank"],
        "bulge_top_over_yield": ["ltbulg_rank"],
        "bulge_bottom_over_yield": ["lbbulg_rank"],
        "dent_top_over_yield": ["dent_rank"],
        "dent_bottom_over_yield": ["dent1_rank"],
        "balancing_yield": ["bal_rank"],
        "upper_yield": ["upper_rank"],
        "lower_yield": ["lower_rank"],
        "upper_lower_yield": ["up_low_rank"],
        "static_yield": ["static_rank"],
        "lateral_run_out_yield": ["lt_oa_rank", "lb_oa_rank"],
        "lateral_run_out_harmonic_yield": ["lt_oa1h_rank", "lb_oa1h_rank"],
        "bulge_yield": ["ltbulg_rank", "lbbulg_rank"],
        "dent_yield": ["dent_rank", "dent1_rank"]
    }

    for _, channel_row_data in channel_df.iterrows():
        try:
            ############# Add 16-07 start ##############
            model_number = str(channel_row_data.get("modelno", "")).strip()
            if model_number not in active_modelnos:
                output_messages.append(f"Skipping CURING model {model_number} as it is not active.")
                continue
            gt_codes = channel_row_data.get("gt_code", [])

            # Ensure gt_codes is a list
            if not isinstance(gt_codes, list):
                gt_codes = [gt_codes]

            for gt_code in gt_codes:
                gt_code = str(gt_code).strip()

                # # MODIFIED: Filter merge_df for each gt_code individually
                # model_data = merge_df[
                #     (merge_df["modelno"] == model_number)
                # ].copy()
                # MODIFIED: Filter merge_df for each gt_code individually
                model_data = merge_df[
                    (merge_df["modelno"] == model_number) &
                    (merge_df["tbm_gtcode_tub"] == gt_code) &
                    (merge_df["ubno"].isin(active_machines))
                ].copy()
            ############# Add 16-07 end ##############

                target_lot_size = int(channel_row_data.get("lot_size", 0))
                # print("at line 4111 target_lot_size:", target_lot_size)
                if target_lot_size == 0:
                    logging.info(f"Skipping UFM model {model_number} due to invalid lot size.")
                    output_messages.append(f"Skipping UFM model {model_number} due to invalid lot size.")
                    continue
                spec_con_kgf = float(channel_row_data.get("spec_con_kgf", 0))
                sizecode = str(channel_row_data.get("sizecode", ""))
                # gt_code = str(row.get("gt_code", ""))
                # spec_rfv = float(row.get("spec_rfv", 0))
                spec_cwrfv_oa_kgf = float(channel_row_data.get("spec_cwrfv_oa_kgf", 0))
                spec_ccwrfv_oa_kgf = float(channel_row_data.get("spec_ccwrfv_oa_kgf", 0))
                # spec_rfh1 = float(row.get("spec_rfh1", 0))
                spec_cwrfv_oa1h_kgf = float(channel_row_data.get("spec_cwrfv_oa1h_kgf", 0))
                spec_ccwrfv_oa1h_kgf = float(channel_row_data.get("spec_ccwrfv_oa1h_kgf", 0))
                # spec_lfv = float(row.get("spec_lfv", 0))
                spec_lfv_oa_kgf = float(channel_row_data.get("spec_lfv_oa_kgf", 0))
                spec_lfv_oa_1_kgf = float(channel_row_data.get("spec_lfv_oa_1_kgf", 0))
                spec_static = float(channel_row_data.get("spec_static", 0))
                spec_upper_g = float(channel_row_data.get("spec_upper_g", None))
                spec_lower_g = float(channel_row_data.get("spec_lower_g", None))
                spec_up_low_g = float(channel_row_data.get("spec_up_low_g", None))
                test_load = float(channel_row_data.get("test_load", 0))
                pressure = float(channel_row_data.get("pressure", 0))
                # spec_yield = float(row.get("spec_yield", 0))
                total_rank_yield_spec_yield = float(channel_row_data.get("total_rank_yield_spec_yield", 0))
                rfv_yield_spec_yield = float(channel_row_data.get("rfv_yield_spec_yield", 0))
                rfh1_yield_spec_yield = float(channel_row_data.get("rfh1_yield_spec_yield", 0))
                lfv_yield_spec_yield = float(channel_row_data.get("lfv_yield_spec_yield", 0))
                conicity_yield_spec_yield = float(channel_row_data.get("conicity_yield_spec_yield", 0))
                ufm_yield_spec_yield = float(channel_row_data.get("ufm_yield_spec_yield", 0))
                rorank_yield_spec_yield = float(channel_row_data.get("rorank_yield_spec_yield", 0))
                radial_runout_yield_spec_yield = float(channel_row_data.get("radial_runout_yield_spec_yield", 0))
                radial_runout_first_harmonic_yield_spec_yield = float(channel_row_data.get("radial_runout_first_harmonic_yield_spec_yield", 0))
                lateral_run_out_yield_spec_yield = float(channel_row_data.get("lateral_run_out_yield_spec_yield", 0))
                lateral_run_out_top_over_yield_spec_yield = float(channel_row_data.get("lateral_run_out_top_over_yield_spec_yield", 0))
                lateral_run_out_bottom_over_yield_spec_yield = float(channel_row_data.get("lateral_run_out_bottom_over_yield_spec_yield", 0))
                lateral_run_out_harmonic_yield_spec_yield = float(channel_row_data.get("lateral_run_out_harmonic_yield_spec_yield", 0))
                lateral_run_out_top_first_harmonic_yield_spec_yield = float(channel_row_data.get("lateral_run_out_top_first_harmonic_yield_spec_yield", 0))
                lateral_run_out_bottom_first_harmonic_yield_spec_yield = float(channel_row_data.get("lateral_run_out_bottom_first_harmonic_yield_spec_yield", 0))
                bulge_yield_spec_yield = float(channel_row_data.get("bulge_yield_spec_yield", 0))
                bulge_top_over_yield_spec_yield = float(channel_row_data.get("bulge_top_over_yield_spec_yield", 0))
                bulge_bottom_over_yield_spec_yield = float(channel_row_data.get("bulge_bottom_over_yield_spec_yield", 0))
                dent_yield_spec_yield = float(channel_row_data.get("dent_yield_spec_yield", 0))
                dent_top_over_yield_spec_yield = float(channel_row_data.get("dent_top_over_yield_spec_yield", 0))
                dent_bottom_over_yield_spec_yield = float(channel_row_data.get("dent_bottom_over_yield_spec_yield", 0))
                balancing_yield_spec_yield = float(channel_row_data.get("balancing_yield_spec_yield", 0))
                upper_yield_spec_yield = float(channel_row_data.get("upper_yield_spec_yield", 0))
                lower_yield_spec_yield = float(channel_row_data.get("lower_yield_spec_yield", 0))
                upper_lower_yield_spec_yield = float(channel_row_data.get("upper_lower_yield_spec_yield", 0))
                static_yield_spec_yield = float(channel_row_data.get("static_yield_spec_yield", 0))
                warehouse_yield_spec_yield = float(channel_row_data.get("warehouse_yield_spec_yield", 0))
                visual_yield_spec_yield = float(channel_row_data.get("visual_yield_spec_yield", 0))
                
                
                # threshold_value = float(row.get("threshold_value", 0.5))
                threshold_value_con_kgf = float(channel_row_data.get("threshold_value_con_kgf", 0.5))
                threshold_value_cwrfv_oa_kgf = float(channel_row_data.get("threshold_value_cwrfv_oa_kgf", 0.5))
                threshold_value_ccwrfv_oa_kgf = float(channel_row_data.get("threshold_value_ccwrfv_oa_kgf", 0.5))
                threshold_value_cwrfv_oa1h_kgf = float(channel_row_data.get("threshold_value_cwrfv_oa1h_kgf", 0.5))
                threshold_value_ccwrfv_oa1h_kgf = float(channel_row_data.get("threshold_value_ccwrfv_oa1h_kgf", 0.5))
                threshold_value_lfv_oa_kgf = float(channel_row_data.get("threshold_value_lfv_oa_kgf", 0.5))
                threshold_value_lfv_oa_1_kgf = float(channel_row_data.get("threshold_value_lfv_oa_1_kgf", 0.5))
                # threshold_value_bal = float(channel_row_data.get("threshold_value_bal", 5))
                threshold_value_bal_upper_g = float(channel_row_data.get("threshold_value_bal_upper_g", 5))
                threshold_value_bal_lower_g = float(channel_row_data.get("threshold_value_bal_lower_g", 5))
                threshold_value_bal_up_low_g = float(channel_row_data.get("threshold_value_bal_up_low_g", 5))
                threshold_value_bal_static_g = float(channel_row_data.get("threshold_value_bal_static_g", 5))
                test_load_tolerance = float(channel_row_data.get("test_load_tolerance", 5))
                pressure_tolerance = float(channel_row_data.get("pressure_tolerance", 2))
                # threshold_value_yield = float(channel_row_data.get("threshold_value_yield", 0))
                threshold_value_yield_total_rank_yield = float(channel_row_data.get("threshold_value_yield_total_rank_yield", 0))
                threshold_value_yield_rfv_yield = float(channel_row_data.get("threshold_value_yield_rfv_yield", 0))
                threshold_value_yield_rfh1_yield = float(channel_row_data.get("threshold_value_yield_rfh1_yield", 0))
                threshold_value_yield_lfv_yield = float(channel_row_data.get("threshold_value_yield_lfv_yield", 0))
                threshold_value_yield_conicity_yield = float(channel_row_data.get("threshold_value_yield_conicity_yield", 0))
                threshold_value_yield_ufm_yield = float(channel_row_data.get("threshold_value_yield_ufm_yield", 0))
                threshold_value_yield_rorank_yield = float(channel_row_data.get("threshold_value_yield_rorank_yield", 0))
                threshold_value_yield_radial_runout_yield = float(channel_row_data.get("threshold_value_yield_radial_runout_yield", 0))
                threshold_value_yield_radial_runout_first_harmonic_yield = float(channel_row_data.get("threshold_value_yield_radial_runout_first_harmonic_yield", 0))
                threshold_value_yield_lateral_run_out_yield = float(channel_row_data.get("threshold_value_yield_lateral_run_out_yield", 0))
                threshold_value_yield_lateral_run_out_top_over_yield = float(channel_row_data.get("threshold_value_yield_lateral_run_out_top_over_yield", 0))
                threshold_value_yield_lateral_run_out_bottom_over_yield = float(channel_row_data.get("threshold_value_yield_lateral_run_out_bottom_over_yield", 0))
                threshold_value_yield_lateral_run_out_harmonic_yield = float(channel_row_data.get("threshold_value_yield_lateral_run_out_harmonic_yield", 0))
                threshold_value_yield_lateral_run_out_top_first_harmonic_yield = float(channel_row_data.get("threshold_value_yield_lateral_run_out_top_first_harmonic_yield", 0))
                threshold_value_yield_lateral_run_out_bottom_first_harmonic_yield = float(channel_row_data.get("threshold_value_yield_lateral_run_out_bottom_first_harmonic_yield", 0))
                threshold_value_yield_bulge_yield = float(channel_row_data.get("threshold_value_yield_bulge_yield", 0))
                threshold_value_yield_bulge_top_over_yield = float(channel_row_data.get("threshold_value_yield_bulge_top_over_yield", 0))
                threshold_value_yield_bulge_bottom_over_yield = float(channel_row_data.get("threshold_value_yield_bulge_bottom_over_yield", 0))
                threshold_value_yield_dent_yield = float(channel_row_data.get("threshold_value_yield_dent_yield", 0))
                threshold_value_yield_dent_top_over_yield = float(channel_row_data.get("threshold_value_yield_dent_top_over_yield", 0))
                threshold_value_yield_dent_bottom_over_yield = float(channel_row_data.get("threshold_value_yield_dent_bottom_over_yield", 0))
                threshold_value_yield_balancing_yield = float(channel_row_data.get("threshold_value_yield_balancing_yield", 0))
                threshold_value_yield_upper_yield = float(channel_row_data.get("threshold_value_yield_upper_yield", 0))
                threshold_value_yield_lower_yield = float(channel_row_data.get("threshold_value_yield_lower_yield", 0))
                threshold_value_yield_upper_lower_yield = float(channel_row_data.get("threshold_value_yield_upper_lower_yield", 0))
                threshold_value_yield_static_yield = float(channel_row_data.get("threshold_value_yield_static_yield", 0))
                threshold_value_yield_warehouse_yield = float(channel_row_data.get("threshold_value_yield_warehouse_yield", 0))
                threshold_value_yield_visual_yield = float(channel_row_data.get("threshold_value_yield_visual_yield", 0))

                if model_data.empty:
                    logging.info(f"Skipping UFM model {model_number} gt_code {gt_code}: No data available.")
                    output_messages.append(f"Skipping UFM model {model_number} gt_code {gt_code}: No data available.")
                    continue

                total_barcodes_model_processable = len(model_data)
                if total_barcodes_model_processable < target_lot_size:
                    logging.info(f"Skipping UFM model {model_number} gt_code {gt_code}: Insufficient barcodes ({total_barcodes_model_processable} < {target_lot_size}).")
                    output_messages.append(f"Skipping UFM model {model_number} gt_code {gt_code}: Insufficient barcodes ({total_barcodes_model_processable} < {target_lot_size}).")
                    continue
                
                # ==================== INSERTED VIRGIN/FINAL YIELD PRE-PROCESSING ====================
                # Keep a full copy for virgin/final yield handling (includes possible duplicate barcodes)
                model_data_full = model_data.copy()

                # Ensure date_of_tbm is proper datetime for sorting
                model_data_full["date_of_dt"] = pd.to_datetime(model_data_full["date_and_time"], errors='coerce')

                # ==================== INSERTED VIRGIN/FINAL YIELD PRE-PROCESSING ====================
                model_data_full = model_data.copy()
                model_data_full["date_of_dt"] = pd.to_datetime(model_data_full["date_and_time"], errors='coerce')

                # === 1. Virgin Yield: FIRST occurrence of each barcode ===
                virgin_data = model_data_full.sort_values("date_of_dt").drop_duplicates(subset="barcode", keep="first").copy()
                virgin_data.sort_values("date_of_dt", inplace=True)

                # === 2. Final Yield: LAST occurrence of each barcode ===
                final_data = model_data_full.sort_values("date_of_dt").drop_duplicates(subset="barcode", keep="last").copy()
                final_data.sort_values("date_of_dt", inplace=True)

                # Use full data for main lot processing
                model_data = model_data_full.copy()
                model_data.sort_values("date_of_ufm", ascending=True, inplace=True)

                alert_map = {}
                # ==================================================================================

                for ufm_machine_id, ufm_machine_data in model_data.groupby("ubno"):
                    try:
                        machine_no = str(ufm_machine_id)
                        ufm_machine_data = ufm_machine_data[ufm_machine_data["ufm_machine_model_lot_id"].isna()].copy()
                        if ufm_machine_data.empty:
                            logging.info(f"Skipping UFM machine {machine_no} for model {model_number}: No unprocessed data.")
                            continue
                        
                        # MODIFIED: Only access tbm_machine if it exists
                        tbm_machine_value = ufm_machine_data["tbm_machine"].iloc[0] if "tbm_machine" in ufm_machine_data.columns and not ufm_machine_data["tbm_machine"].isna().all() else None
                        # print("at line 7040 tbm_machine_value:",tbm_machine_value)
                        output_messages.append(f"DEBUG: Model {model_number}, Machine {machine_no}, tbm_machine: {tbm_machine_value}.")

                        machine_config_row = ub_machine_df[ub_machine_df["ubno"] == machine_no]
                        if machine_config_row.empty:
                            logging.info(f"Skipping UFM machine {machine_no} for model {model_number}: Unknown machine config.")
                            output_messages.append(f"Skipping UFM machine {machine_no} for model {model_number}: Unknown machine config.")
                            continue

                        # target_lot_size = int(channel_df['lot_size'].iloc[0])
                        # print("at line 4190 target_lot_size:", target_lot_size)
                        total_ufm_machine_barcodes = len(ufm_machine_data)
                        if total_ufm_machine_barcodes < target_lot_size:
                            logging.info(f"Skipping UFM machine {machine_no} for model {model_number}: Insufficient barcodes ({total_ufm_machine_barcodes} < {target_lot_size}).")
                            output_messages.append(f"Skipping UFM machine {machine_no} for model {model_number}: Insufficient barcodes ({total_ufm_machine_barcodes} < {target_lot_size}).")
                            continue

                        machine_barcode_counter = 0
                        machine_model_key = (model_number, machine_no)

                        while machine_barcode_counter < total_ufm_machine_barcodes:
                            if (total_ufm_machine_barcodes - machine_barcode_counter) < target_lot_size:
                                logging.info(f"Insufficient remaining barcodes on UFM machine {machine_no} for model {model_number}: {total_ufm_machine_barcodes - machine_barcode_counter}")
                                break
                            if (model_number, machine_no) not in last_lot_dict:
                                last_lot_dict[(model_number, machine_no)] = {
                                    "max_lot_number": 0,
                                    "last_lot_id": None
                                }

                            current_last_lot_number = last_lot_dict[(model_number, machine_no)]["max_lot_number"] + 1
                            current_model_lot_id = f"UFM_{machine_no}_{model_number}_{current_last_lot_number}"
                            current_barcode_counter = machine_barcode_counter + target_lot_size
                            current_ufm_machine_lot_id = f"{machine_no}_{model_number}_UFM_{machine_no}_{model_number}_{current_last_lot_number}_{current_barcode_counter}"
                            
                            # Update last_lot_dict
                            last_lot_dict[(model_number, machine_no)]["max_lot_number"] = current_last_lot_number
                            last_lot_dict[(model_number, machine_no)]["last_lot_id"] = current_model_lot_id

                            # Validate lot_id and ufm_machine_model_lot_id formats
                            if not re.match(r"^UFM_[^_]+_[^_]+_\d+$", current_model_lot_id):
                                logging.error(f"Invalid lot_id format: {current_model_lot_id}")
                                continue
                            if not re.match(r"^[^_]+_[^_]+_UFM_[^_]+_[^_]+_\d+_\d+$", current_ufm_machine_lot_id):
                                logging.error(f"Invalid ufm_machine_model_lot_id format: {current_ufm_machine_lot_id}")
                                continue

                            sub_batch_start_index = machine_barcode_counter
                            sub_batch_end_index = machine_barcode_counter + target_lot_size
                            machine_batch_data = ufm_machine_data.iloc[sub_batch_start_index:sub_batch_end_index].copy()
                            machine_batch_indices = machine_batch_data.index

                            # Fetch previous lot ID
                            ufm_previous_machine_model_lot_id = ufm_previous_machine_model_lot_ids.get(machine_model_key)
                            # logging.info(f"Initial ufm_previous_machine_model_lot_id: {ufm_previous_machine_model_lot_id} for model {model_number}, machine {machine_no}")
                            if ufm_previous_machine_model_lot_id is None and current_last_lot_number > 1:
                                prev_lot_number = current_last_lot_number - 1
                                prev_lot_id = f"UFM_{machine_no}_{model_number}_{prev_lot_number}"
                                logging.info(f"Fetching previous lot for lot_id: {prev_lot_id}")
                                try:
                                    prev_lot = averages_ufm_collect.find_one(
                                        {
                                            "modelno": model_number,
                                            "machine": machine_no,
                                            "lot_id": prev_lot_id
                                        }
                                    )
                                    if prev_lot:
                                        ufm_previous_machine_model_lot_id = prev_lot["ufm_machine_model_lot_id"]
                                        logging.info(f"Found previous lot: {ufm_previous_machine_model_lot_id}")
                                    else:
                                        logging.warning(f"No previous lot found for lot_id: {prev_lot_id}")
                                        ufm_previous_machine_model_lot_id = None
                                except Exception as e:
                                    logging.error(f"Error fetching previous ufm_machine_model_lot_id for lot {prev_lot_number}: {e}")
                                    ufm_previous_machine_model_lot_id = None

                            # Assign values to merge_df
                            model_level_previous_lot_id = f"UFM_{machine_no}_{model_number}_{current_last_lot_number - 1}" if current_last_lot_number > 1 else None
                            merge_df.loc[machine_batch_indices, "lot_id"] = current_model_lot_id
                            merge_df.loc[machine_batch_indices, "previous_lot_id"] = model_level_previous_lot_id
                            merge_df.loc[machine_batch_indices, "ufm_machine_model_lot_id"] = current_ufm_machine_lot_id
                            merge_df.loc[machine_batch_indices, "ufm_previous_machine_model_lot_id"] = ufm_previous_machine_model_lot_id

                            # Fetch previous lot averages
                            prev_avg_key = (model_number, machine_no, ufm_previous_machine_model_lot_id)
                            prev_lot_avg = ufm_machine_lot_avg_map.get(prev_avg_key, None)
                            if prev_lot_avg is None and ufm_previous_machine_model_lot_id is not None:
                                try:
                                    prev_lot_doc = averages_ufm_collect.find_one(
                                        {
                                            "modelno": model_number,
                                            "machine": machine_no,
                                            "ufm_machine_model_lot_id": ufm_previous_machine_model_lot_id
                                        },
                                        {"current_lot_averages": 1}
                                    )
                                    prev_lot_avg = prev_lot_doc.get("current_lot_averages", {}) if prev_lot_doc else {}
                                    if prev_lot_avg:
                                        ufm_machine_lot_avg_map[prev_avg_key] = prev_lot_avg
                                        logging.info(f"Fetched previous lot averages for {ufm_previous_machine_model_lot_id} from database")
                                    else:
                                        logging.warning(f"No previous lot averages found for {ufm_previous_machine_model_lot_id} in database")
                                except Exception as e:
                                    logging.error(f"Error fetching previous lot averages for {ufm_previous_machine_model_lot_id}: {e}")
                                    prev_lot_avg = {}
                            if current_last_lot_number == 1:
                                prev_lot_avg = {}
                                logging.info(f"First lot for model {model_number}, machine {machine_no}. Setting previous_lot_averages to empty.")
                            # logging.info(f"prev_key: {prev_avg_key}, prev_lot_avg: {prev_lot_avg}")
                            # Update last_lot_dict
                            last_lot_dict[machine_model_key] = {
                                "max_lot_number": current_last_lot_number,
                                                                                                                      
                                "last_ufm_machine_model_lot_id": current_ufm_machine_lot_id
                            }                      

                            # Prepare Lot Document
                            lot_doc_data = {
                                "_id": f"lot_{current_ufm_machine_lot_id}",
                                "modelno": model_number,
                                "lot_id": current_model_lot_id,
                                "previous_lot_id": model_level_previous_lot_id,
                                "machine": machine_no,
                                "ufm_machine_model_lot_id": current_ufm_machine_lot_id,
                                "ufm_previous_machine_model_lot_id": ufm_previous_machine_model_lot_id,
                                "target_machine_lot_size": target_lot_size,
                                "processed_machine_barcodes": target_lot_size,
                                "timestamp_lot_generated": some_function(),
                                "first_barcode": machine_batch_data['barcode'].iloc[0] if not machine_batch_data.empty else None,
                                "last_barcode": machine_batch_data['barcode'].iloc[-1] if not machine_batch_data.empty else None,
                            }
                            if not lot_ufm_collect.find_one({"ufm_machine_model_lot_id": current_ufm_machine_lot_id}):
                                ufm_lot_bulk_operations.append(pymongo.UpdateOne(
                                    {"ufm_machine_model_lot_id": current_ufm_machine_lot_id},
                                    {"$setOnInsert": convert_numpy_types(lot_doc_data)},
                                    upsert=True
                                ))
                                output_messages.append(f"Prepared lot_id {current_model_lot_id} for insertion into lot_ufm_collect")

                            lot_averages = {}
                            for param in ufm_params:
                                if param in machine_batch_data.columns:
                                    numeric_series = pd.to_numeric(machine_batch_data[param], errors='coerce').dropna()
                                    if not numeric_series.empty:
                                        lot_averages[param] = float(numeric_series.mean())
                                    else:
                                        lot_averages[param] = None
                                else:
                                    lot_averages[param] = None

                            lot_averages.update({
                                "total_rank_yield": total_rank_yield(machine_batch_data),
                                "rfv_yield": rfv_yield(machine_batch_data),
                                "rfh1_yield": rfh1_yield(machine_batch_data),
                                "lfv_yield": lfv_yield(machine_batch_data),
                                "conicity_yield": conicity_yield(machine_batch_data),
                                "ufm_yield": ufm_yield(machine_batch_data),
                                "rorank_yield": rorank_yield(machine_batch_data),
                                "radial_runout_yield": radial_runout_yield(machine_batch_data),
                                "radial_runout_first_harmonic_yield": radial_runout_first_harmonic_yield(machine_batch_data),
                                "lateral_run_out_yield": lateral_run_out_yield(machine_batch_data),
                                "lateral_run_out_top_over_yield": lateral_run_out_top_over(machine_batch_data),
                                "lateral_run_out_bottom_over_yield": lateral_run_out_bottom_over(machine_batch_data),
                                "lateral_run_out_harmonic_yield": lateral_run_out_harmonic_yield(machine_batch_data),
                                "lateral_run_out_top_first_harmonic_yield": lateral_run_out_top_first_harmonic(machine_batch_data),
                                "lateral_run_out_bottom_first_harmonic_yield": lateral_run_out_bottom_first_harmonic(machine_batch_data),
                                "bulge_yield": bulge_yield(machine_batch_data),
                                "bulge_top_over_yield": bulge_top_over(machine_batch_data),
                                "bulge_bottom_over_yield": bulge_bottom_over(machine_batch_data),
                                "dent_yield": dent_yield(machine_batch_data),
                                "dent_top_over_yield": dent_top_over(machine_batch_data),
                                "dent_bottom_over_yield": dent_bottom_over(machine_batch_data),
                                "balancing_yield": balancing_yield(machine_batch_data),
                                "upper_yield": upper_yield(machine_batch_data),
                                "lower_yield": lower_yield(machine_batch_data),
                                "upper_lower_yield": upper_lower_yield(machine_batch_data),
                                "static_yield": static_yield(machine_batch_data),
                                "warehouse_yield": warehouse_yield(machine_batch_data),
                                "visual_yield": visual_yield(machine_batch_data)
                            })

                            # Perform Shift Detection
                            parameter_alerts = []
                            machine_lot_overall_shift_occurred = False
                            # Define mapping of yield parameters to their spec_yield variables
                            yield_spec_mapping = {
                                "total_rank_yield": total_rank_yield_spec_yield,
                                "rfv_yield": rfv_yield_spec_yield,
                                "rfh1_yield": rfh1_yield_spec_yield,
                                "lfv_yield": lfv_yield_spec_yield,
                                "conicity_yield": conicity_yield_spec_yield,
                                "ufm_yield": ufm_yield_spec_yield,
                                "rorank_yield": rorank_yield_spec_yield,
                                "radial_runout_yield": radial_runout_yield_spec_yield,
                                "radial_runout_first_harmonic_yield": radial_runout_first_harmonic_yield_spec_yield,
                                "lateral_run_out_yield": lateral_run_out_yield_spec_yield,
                                "lateral_run_out_top_over_yield": lateral_run_out_top_over_yield_spec_yield,
                                "lateral_run_out_bottom_over_yield": lateral_run_out_bottom_over_yield_spec_yield,
                                "lateral_run_out_harmonic_yield": lateral_run_out_harmonic_yield_spec_yield,
                                "lateral_run_out_top_first_harmonic_yield": lateral_run_out_top_first_harmonic_yield_spec_yield,
                                "lateral_run_out_bottom_first_harmonic_yield": lateral_run_out_bottom_first_harmonic_yield_spec_yield,
                                "bulge_yield": bulge_yield_spec_yield,
                                "bulge_top_over_yield": bulge_top_over_yield_spec_yield,
                                "bulge_bottom_over_yield": bulge_bottom_over_yield_spec_yield,
                                "dent_yield": dent_yield_spec_yield,
                                "dent_top_over_yield": dent_top_over_yield_spec_yield,
                                "dent_bottom_over_yield": dent_bottom_over_yield_spec_yield,
                                "balancing_yield": balancing_yield_spec_yield,
                                "upper_yield": upper_yield_spec_yield,
                                "lower_yield": lower_yield_spec_yield,
                                "upper_lower_yield": upper_lower_yield_spec_yield,
                                "static_yield": static_yield_spec_yield,
                                "warehouse_yield": warehouse_yield_spec_yield,
                                "visual_yield": visual_yield_spec_yield
                            }
                            if prev_lot_avg:
                                for param in ufm_params + yield_params:
                                    current_val = lot_averages.get(param)
                                    prev_val = prev_lot_avg.get(param)
                                    if current_val is None or pd.isna(current_val) or prev_val is None or pd.isna(prev_val):
                                        alert_dict = {
                                            "parameter": param,
                                            "current_value": current_val,
                                            "previous_value": prev_val,
                                            "difference": None,
                                            "shift_occurred_thershold": False,
                                            "spec_value": None,
                                            "shift_occurred_spec": False,
                                            "shift_occurred": False
                                        }
                                        if param == "load1":
                                            alert_dict["test_load_tolerance"] = None
                                        if param == "airpress1":
                                            alert_dict["pressure_tolerance"] = None
                                        parameter_alerts.append(alert_dict)
                                        continue

                                    shift_occurred_thershold = False
                                    shift_occurred_spec = False
                                    spec_val = None

                                    if param == "con_kgf" and spec_con_kgf:
                                        shift_occurred_thershold = abs(current_val - prev_val) > threshold_value_con_kgf
                                        shift_occurred_spec = not (-spec_con_kgf <= current_val <= spec_con_kgf)
                                        spec_val = spec_con_kgf
                                    elif param == "cwrfv_oa_kgf" and spec_cwrfv_oa_kgf:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_cwrfv_oa_kgf
                                        shift_occurred_spec = current_val > spec_cwrfv_oa_kgf
                                        spec_val = spec_cwrfv_oa_kgf
                                    elif param == "ccwrfv_oa_kgf" and spec_ccwrfv_oa_kgf:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_ccwrfv_oa_kgf
                                        shift_occurred_spec = current_val > spec_ccwrfv_oa_kgf
                                        spec_val = spec_ccwrfv_oa_kgf
                                    elif param == "cwrfv_oa1h_kgf" and spec_cwrfv_oa1h_kgf:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_cwrfv_oa1h_kgf
                                        shift_occurred_spec = current_val > spec_cwrfv_oa1h_kgf
                                        spec_val = spec_cwrfv_oa1h_kgf
                                    elif param == "ccwrfv_oa1h_kgf" and spec_ccwrfv_oa1h_kgf:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_ccwrfv_oa1h_kgf
                                        shift_occurred_spec = current_val > spec_ccwrfv_oa1h_kgf
                                        spec_val = spec_ccwrfv_oa1h_kgf
                                    elif param == "lfv_oa_kgf" and spec_lfv_oa_kgf:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_lfv_oa_kgf
                                        shift_occurred_spec = current_val > spec_lfv_oa_kgf
                                        spec_val = spec_lfv_oa_kgf
                                    elif param == "lfv_oa_1_kgf" and spec_lfv_oa_1_kgf:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_lfv_oa_1_kgf
                                        shift_occurred_spec = current_val > spec_lfv_oa_1_kgf
                                        spec_val = spec_lfv_oa_1_kgf
                                    elif param == "upper_g" and spec_upper_g is not None:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_bal_upper_g
                                        shift_occurred_spec = current_val > spec_upper_g
                                        spec_val = spec_upper_g
                                    elif param == "lower_g" and spec_lower_g is not None:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_bal_lower_g
                                        shift_occurred_spec = current_val > spec_lower_g
                                        spec_val = spec_lower_g
                                    elif param == "up_low_g" and spec_up_low_g is not None:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_bal_up_low_g
                                        shift_occurred_spec = current_val > spec_up_low_g
                                        spec_val = spec_up_low_g
                                    elif param == "static_g" and spec_static:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_bal_static_g
                                        shift_occurred_spec = current_val > spec_static
                                        spec_val = spec_static

                                    # elif param in yield_params:
                                    #     spec_yield = yield_spec_mapping.get(param, 0)
                                    #     shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield)
                                    #     shift_occurred_spec = current_val < spec_yield
                                    #     spec_val = spec_yield

                                    elif param == "total_rank_yield":
                                        spec_yield = yield_spec_mapping.get(param, 0)
                                        shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_total_rank_yield)
                                        shift_occurred_spec = current_val < spec_yield
                                        spec_val = spec_yield
                                    elif param == "rfv_yield":
                                        spec_yield = yield_spec_mapping.get(param, 0)
                                        shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_rfv_yield)
                                        shift_occurred_spec = current_val < spec_yield
                                        spec_val = spec_yield
                                    elif param == "rfh1_yield":
                                        spec_yield = yield_spec_mapping.get(param, 0)
                                        shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_rfh1_yield)
                                        shift_occurred_spec = current_val < spec_yield
                                        spec_val = spec_yield
                                    elif param == "lfv_yield":
                                        spec_yield = yield_spec_mapping.get(param, 0)
                                        shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_lfv_yield)
                                        shift_occurred_spec = current_val < spec_yield
                                        spec_val = spec_yield
                                    elif param == "conicity_yield":
                                        spec_yield = yield_spec_mapping.get(param, 0)
                                        shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_conicity_yield)
                                        shift_occurred_spec = current_val < spec_yield
                                        spec_val = spec_yield
                                    elif param == "ufm_yield":
                                        spec_yield = yield_spec_mapping.get(param, 0)
                                        shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_conicity_yield)
                                        shift_occurred_spec = current_val < spec_yield
                                        spec_val = spec_yield
                                    elif param == "rorank_yield":
                                        spec_yield = yield_spec_mapping.get(param, 0)
                                        shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_rorank_yield)
                                        shift_occurred_spec = current_val < spec_yield
                                        spec_val = spec_yield
                                    elif param == "radial_runout_yield":
                                        spec_yield = yield_spec_mapping.get(param, 0)
                                        shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_radial_runout_yield)
                                        shift_occurred_spec = current_val < spec_yield
                                        spec_val = spec_yield
                                    elif param == "radial_runout_first_harmonic_yield":
                                        spec_yield = yield_spec_mapping.get(param, 0)
                                        shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_radial_runout_first_harmonic_yield)
                                        shift_occurred_spec = current_val < spec_yield
                                        spec_val = spec_yield
                                    elif param == "lateral_run_out_yield":
                                        spec_yield = yield_spec_mapping.get(param, 0)
                                        shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_lateral_run_out_yield)
                                        shift_occurred_spec = current_val < spec_yield
                                        spec_val = spec_yield
                                    elif param == "lateral_run_out_top_over_yield":
                                        spec_yield = yield_spec_mapping.get(param, 0)
                                        shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_lateral_run_out_top_over_yield)
                                        shift_occurred_spec = current_val < spec_yield
                                        spec_val = spec_yield
                                    elif param == "lateral_run_out_bottom_over_yield":
                                        spec_yield = yield_spec_mapping.get(param, 0)
                                        shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_lateral_run_out_bottom_over_yield)
                                        shift_occurred_spec = current_val < spec_yield
                                        spec_val = spec_yield
                                    elif param == "lateral_run_out_harmonic_yield":
                                        spec_yield = yield_spec_mapping.get(param, 0)
                                        shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_lateral_run_out_harmonic_yield)
                                        shift_occurred_spec = current_val < spec_yield
                                        spec_val = spec_yield
                                    elif param == "lateral_run_out_top_first_harmonic_yield":
                                        spec_yield = yield_spec_mapping.get(param, 0)
                                        shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_lateral_run_out_top_first_harmonic_yield)
                                        shift_occurred_spec = current_val < spec_yield
                                        spec_val = spec_yield
                                    elif param == "lateral_run_out_bottom_first_harmonic_yield":
                                        spec_yield = yield_spec_mapping.get(param, 0)
                                        shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_lateral_run_out_bottom_first_harmonic_yield)
                                        shift_occurred_spec = current_val < spec_yield
                                        spec_val = spec_yield
                                    elif param == "bulge_yield":
                                        spec_yield = yield_spec_mapping.get(param, 0)
                                        shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_bulge_yield)
                                        shift_occurred_spec = current_val < spec_yield
                                        spec_val = spec_yield
                                    elif param == "bulge_top_over_yield":
                                        spec_yield = yield_spec_mapping.get(param, 0)
                                        shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_bulge_top_over_yield)
                                        shift_occurred_spec = current_val < spec_yield
                                        spec_val = spec_yield
                                    elif param == "bulge_bottom_over_yield":
                                        spec_yield = yield_spec_mapping.get(param, 0)
                                        shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_bulge_bottom_over_yield)
                                        shift_occurred_spec = current_val < spec_yield
                                        spec_val = spec_yield
                                    elif param == "dent_yield":
                                        spec_yield = yield_spec_mapping.get(param, 0)
                                        shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_dent_yield)
                                        shift_occurred_spec = current_val < spec_yield
                                        spec_val = spec_yield
                                    elif param == "dent_top_over_yield":
                                        spec_yield = yield_spec_mapping.get(param, 0)
                                        shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_dent_top_over_yield)
                                        shift_occurred_spec = current_val < spec_yield
                                        spec_val = spec_yield
                                    elif param == "dent_bottom_over_yield":
                                        spec_yield = yield_spec_mapping.get(param, 0)
                                        shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_dent_bottom_over_yield)
                                        shift_occurred_spec = current_val < spec_yield
                                        spec_val = spec_yield
                                    elif param == "balancing_yield":
                                        spec_yield = yield_spec_mapping.get(param, 0)
                                        shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_balancing_yield)
                                        shift_occurred_spec = current_val < spec_yield
                                        spec_val = spec_yield
                                    elif param == "upper_yield":
                                        spec_yield = yield_spec_mapping.get(param, 0)
                                        shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_upper_yield)
                                        shift_occurred_spec = current_val < spec_yield
                                        spec_val = spec_yield
                                    elif param == "lower_yield":
                                        spec_yield = yield_spec_mapping.get(param, 0)
                                        shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_lower_yield)
                                        shift_occurred_spec = current_val < spec_yield
                                        spec_val = spec_yield
                                    elif param == "upper_lower_yield":
                                        spec_yield = yield_spec_mapping.get(param, 0)
                                        shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_upper_lower_yield)
                                        shift_occurred_spec = current_val < spec_yield
                                        spec_val = spec_yield
                                    elif param == "static_yield":
                                        spec_yield = yield_spec_mapping.get(param, 0)
                                        shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_static_yield)
                                        shift_occurred_spec = current_val < spec_yield
                                        spec_val = spec_yield
                                    elif param == "warehouse_yield":
                                        spec_yield = yield_spec_mapping.get(param, 0)
                                        shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_warehouse_yield)
                                        shift_occurred_spec = current_val < spec_yield
                                        spec_val = spec_yield
                                    elif param == "visual_yield":
                                        spec_yield = yield_spec_mapping.get(param, 0)
                                        shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_visual_yield)
                                        shift_occurred_spec = current_val < spec_yield
                                        spec_val = spec_yield
                                        
                                    elif param == "load1" and test_load:
                                        shift_occurred_thershold = abs(current_val - prev_val) > test_load_tolerance
                                        shift_occurred_spec = not (test_load - test_load_tolerance <= current_val <= test_load + test_load_tolerance)
                                        spec_val = test_load
                                    elif param == "airpress1" and pressure:
                                        shift_occurred_thershold = abs(current_val - prev_val) > pressure_tolerance
                                        shift_occurred_spec = not (pressure - pressure_tolerance <= current_val <= pressure + pressure_tolerance)
                                        spec_val = pressure

                                    shift_occurred = shift_occurred_thershold or shift_occurred_spec
                                    if shift_occurred:
                                        machine_lot_overall_shift_occurred = True

                                    alert_dict = {
                                        "parameter": param,
                                        "current_value": round(current_val, 3) if current_val is not None else None,
                                        "previous_value": round(prev_val, 3) if prev_val is not None else None,
                                        "difference": round(current_val - prev_val, 3) if current_val is not None and prev_val is not None else None,
                                        "shift_occurred_thershold": bool(shift_occurred_thershold),
                                        "spec_value": spec_val,
                                        "spec_difference": round(current_val - spec_val, 3) if current_val is not None and spec_val is not None else None,
                                        "shift_occurred_spec": bool(shift_occurred_spec),
                                        "shift_occurred": bool(shift_occurred)
                                    }
                                    if param == "load1":
                                        alert_dict["test_load_tolerance"] = test_load_tolerance
                                    if param == "airpress1":
                                        alert_dict["pressure_tolerance"] = pressure_tolerance
                                    parameter_alerts.append(alert_dict)
                            else:
                                for param in ufm_params + yield_params:
                                    current_val = lot_averages.get(param)
                                    if current_val is None or pd.isna(current_val):
                                        alert_dict = {
                                            "parameter": param,
                                            "current_value": current_val,
                                            "previous_value": None,
                                            "difference": None,
                                            "shift_occurred_thershold": False,
                                            "spec_value": None,
                                            "shift_occurred_spec": False,
                                            "shift_occurred": False
                                        }
                                        if param == "load1":
                                            alert_dict["test_load_tolerance"] = None
                                        if param == "airpress1":
                                            alert_dict["pressure_tolerance"] = None
                                        parameter_alerts.append(alert_dict)
                                        continue

                                    shift_occurred_thershold = False
                                    shift_occurred_spec = False
                                    # spec_val = None

                                    if param == "con_kgf" and spec_con_kgf:
                                        shift_occurred_spec = not (-spec_con_kgf <= current_val <= spec_con_kgf)
                                        spec_val = spec_con_kgf
                                    elif param == "cwrfv_oa_kgf" and spec_cwrfv_oa_kgf:
                                        shift_occurred_spec = current_val > spec_cwrfv_oa_kgf
                                        spec_val = spec_cwrfv_oa_kgf
                                    elif param == "ccwrfv_oa_kgf" and spec_ccwrfv_oa_kgf:
                                        shift_occurred_spec = current_val > spec_ccwrfv_oa_kgf
                                        spec_val = spec_ccwrfv_oa_kgf
                                    elif param == "cwrfv_oa1h_kgf" and spec_cwrfv_oa1h_kgf:
                                        shift_occurred_spec = current_val > spec_cwrfv_oa1h_kgf
                                        spec_val = spec_cwrfv_oa1h_kgf
                                    elif param == "ccwrfv_oa1h_kgf" and spec_ccwrfv_oa1h_kgf:
                                        shift_occurred_spec = current_val > spec_ccwrfv_oa1h_kgf
                                        spec_val = spec_ccwrfv_oa1h_kgf
                                    elif param == "lfv_oa_kgf" and spec_lfv_oa_kgf:
                                        shift_occurred_spec = current_val > spec_lfv_oa_kgf
                                        spec_val = spec_lfv_oa_kgf
                                    elif param == "lfv_oa_1_kgf" and spec_lfv_oa_1_kgf:
                                        shift_occurred_spec = current_val > spec_lfv_oa_1_kgf
                                        spec_val = spec_lfv_oa_1_kgf
                                    elif param == "upper_g" and spec_upper_g is not None:
                                        shift_occurred_spec = current_val > spec_upper_g
                                        spec_val = spec_upper_g
                                    elif param == "lower_g" and spec_lower_g is not None:
                                        shift_occurred_spec = current_val > spec_lower_g
                                        spec_val = spec_lower_g
                                    elif param == "up_low_g" and spec_up_low_g is not None:
                                        shift_occurred_spec = current_val > spec_up_low_g
                                        spec_val = spec_up_low_g
                                    elif param == "static_g" and spec_static:
                                        shift_occurred_spec = current_val > spec_static
                                        spec_val = spec_static
                                    elif param in yield_params:
                                        spec_yield = yield_spec_mapping.get(param, 0)
                                        shift_occurred_spec = current_val < spec_yield
                                        spec_val = spec_yield
                                    elif param in yield_params and spec_yield:
                                        shift_occurred_spec = current_val < spec_yield
                                        spec_val = spec_yield
                                    elif param == "load1" and test_load:
                                        shift_occurred_spec = not (test_load - test_load_tolerance <= current_val <= test_load + test_load_tolerance)
                                        spec_val = test_load
                                    elif param == "airpress1" and pressure:
                                        shift_occurred_spec = not (pressure - pressure_tolerance <= current_val <= pressure + pressure_tolerance)
                                        spec_val = pressure

                                    shift_occurred = shift_occurred_thershold or shift_occurred_spec
                                    if shift_occurred:
                                        machine_lot_overall_shift_occurred = True

                                    alert_dict = {
                                        "parameter": param,
                                        "current_value": round(current_val, 3) if current_val is not None else None,
                                        "previous_value": None,
                                        "difference": None,
                                        "shift_occurred_thershold": bool(shift_occurred_thershold),
                                        "spec_value": spec_val,
                                        "spec_difference": round(current_val - spec_val, 3) if current_val is not None and spec_val is not None else None,
                                        "shift_occurred_spec": bool(shift_occurred_spec),
                                        "shift_occurred": bool(shift_occurred)
                                    }
                                    if param == "load1":
                                        alert_dict["test_load_tolerance"] = test_load_tolerance
                                    if param == "airpress1":
                                        alert_dict["pressure_tolerance"] = pressure_tolerance
                                    parameter_alerts.append(alert_dict)

                            machine_lot_overall_shift_occurred = bool(machine_lot_overall_shift_occurred)

                            deviated_parameter_alerts = [
                                alert for alert in parameter_alerts
                                if alert.get("shift_occurred_thershold", False) or alert.get("shift_occurred_spec", False)
                            ]

                            for alert in deviated_parameter_alerts:
                                param = alert.get("parameter", "")
                                if param.endswith("_yield"):
                                    if param in rank_param_mappings:
                                        rank_counts = {}
                                        for col in rank_param_mappings[param]:
                                            if col in machine_batch_data.columns:
                                                value_counts = machine_batch_data[col].value_counts().to_dict()
                                                for rank_value, count in value_counts.items():
                                                    rank_value = int(rank_value) if isinstance(rank_value, (int, float)) else rank_value
                                                    rank_counts[f"{col}_{rank_value}"] = int(count)
                                        alert["rank_counts"] = rank_counts

                            if deviated_parameter_alerts:
                                deviated_doc = {
                                    "alert_id_ufm_ufm": last_alert_id + 1,
                                    "modelno": model_number,
                                    "lot_id": current_model_lot_id,
                                    "machine": machine_no,
                                    "comments": [],
                                    "alert_assign_user": [],
                                    "ufm_machine_model_lot_id": current_ufm_machine_lot_id,
                                    "ufm_previous_machine_model_lot_id": ufm_previous_machine_model_lot_id,
                                    "parameter_alerts": deviated_parameter_alerts,
                                    "timestamp_alert_generated": some_function()
                                }
                                deviated_triggering_bulk_operations.append(pymongo.InsertOne(deviated_doc))
                                logging.info(f"Prepared {len(deviated_parameter_alerts)} derived alerts for machine {machine_no}, lot {current_model_lot_id}")

                            parameter_alerts = [
                                alert for alert in parameter_alerts
                                if alert.get("parameter", "").endswith("_yield") or not alert.get("parameter", "").endswith("_yield")
                            ]

                            for alert in parameter_alerts:
                                param = alert.get("parameter", "")
                                if (
                                    (alert.get("shift_occurred_thershold", False) or alert.get("shift_occurred_spec", False))
                                    and param.endswith("_yield")
                                ):
                                    if param in rank_param_mappings:
                                        rank_counts = {}
                                        for col in rank_param_mappings[param]:
                                            if col in machine_batch_data.columns:
                                                value_counts = machine_batch_data[col].value_counts().to_dict()
                                                for rank_value, count in value_counts.items():
                                                    rank_value = int(rank_value) if isinstance(rank_value, (int, float)) else rank_value
                                                    rank_counts[f"{col}_{rank_value}"] = int(count)
                                        alert["rank_counts"] = rank_counts

                            averages_doc_data = {
                                "_id": f"avg_{current_ufm_machine_lot_id}",
                                "modelno": model_number,
                                "lot_id": current_model_lot_id,
                                "previous_lot_id": model_level_previous_lot_id,
                                "machine": machine_no,
                                "ufm_machine_model_lot_id": current_ufm_machine_lot_id,
                                "ufm_previous_machine_model_lot_id": ufm_previous_machine_model_lot_id,
                                "current_lot_averages": {k: round(v, 3) if v is not None and not pd.isna(v) else None for k, v in lot_averages.items()},
                                "previous_lot_averages": {k: round(v, 3) if v is not None and not pd.isna(v) else None for k, v in prev_lot_avg.items()} if prev_lot_avg else {},
                                "parameter_alerts": parameter_alerts,
                                "ufm_shift_occurred": machine_lot_overall_shift_occurred,
                                "target_machine_lot_size": target_lot_size,
                                "processed_machine_barcodes": target_lot_size,
                                "timestamp_alert_generated": some_function()
                            }
                            if not averages_ufm_collect.find_one({"ufm_machine_model_lot_id": current_ufm_machine_lot_id}):
                                ufm_averages_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(averages_doc_data)))
                                output_messages.append(f"Prepared averages for {current_ufm_machine_lot_id} for insertion into averages_ufm_collect")

                            current_avg_key = (model_number, machine_no, current_ufm_machine_lot_id)
                            ufm_machine_lot_avg_map[current_avg_key] = lot_averages
                            ufm_previous_machine_model_lot_ids[machine_model_key] = current_ufm_machine_lot_id

                            last_alert_id += 1
                            merge_df.loc[machine_batch_indices, "alert_id_ufm_ufm"] = last_alert_id

                            # ------------------------------------------------------------------
                            #  ORIGINAL (incorrect – one entry per shift even if it spans many days)
                            # ------------------------------------------------------------------
                            # machine_batch_data["tbm_shift"] = machine_batch_data["date_of_tbm"].apply(get_shift)
                            # shift_groups = machine_batch_data.groupby('tbm_shift')
                            # shifts_data_list_tbm = []
                            # for shift_name, shift_data in shift_groups:
                            #     shift_start_time = shift_data["date_of_tbm"].iloc[0]
                            #     shift_end_time = shift_data["date_of_tbm"].iloc[-1]
                            #     shifts_data_list_tbm.append({
                            #         "shift": shift_name,
                            #         "barcode_count": len(shift_data["barcode"]) if "barcode" in shift_data.columns else 0,
                            #         "machine_shift_tbm_start_time": shift_start_time,
                            #         "machine_shift_tbm_end_time": shift_end_time,
                            #         "barcodes": shift_data["barcode"].tolist() if "barcode" in shift_data.columns else []
                            #     })
                            
                            # ------------------------------------------------------------------
                            #  NEW (correct – one entry per **continuous** shift block)
                            # ------------------------------------------------------------------
                            machine_batch_data["tbm_shift"] = machine_batch_data["date_of_tbm"].apply(get_shift)

                            # Sort by date_of_tbm to guarantee chronological order
                            machine_batch_data = machine_batch_data.sort_values("date_of_tbm").reset_index(drop=True)

                            shifts_data_list_tbm = []
                            current_shift = None
                            current_start = None
                            current_barcodes = []

                            for idx, row in machine_batch_data.iterrows():
                                this_shift = row["tbm_shift"]
                                this_dt    = row["date_of_tbm"]
                                this_bc    = row.get("barcode")

                                if this_shift != current_shift or current_shift is None:
                                    # finish previous block (if any)
                                    if current_shift is not None:
                                        shifts_data_list_tbm.append({
                                            "shift": current_shift,
                                            "barcode_count": len(current_barcodes),
                                            "machine_shift_tbm_start_time": current_start,
                                            "machine_shift_tbm_end_time":   machine_batch_data.at[idx-1, "date_of_tbm"],
                                            "barcodes": current_barcodes[:]
                                        })
                                    # start new block
                                    current_shift   = this_shift
                                    current_start   = this_dt
                                    current_barcodes = [this_bc] if this_bc is not None else []
                                else:
                                    current_barcodes.append(this_bc if this_bc is not None else None)

                            # do NOT forget the last block
                            if current_shift is not None:
                                shifts_data_list_tbm.append({
                                    "shift": current_shift,
                                    "barcode_count": len(current_barcodes),
                                    "machine_shift_tbm_start_time": current_start,
                                    "machine_shift_tbm_end_time":   machine_batch_data["date_of_tbm"].iloc[-1],
                                    "barcodes": current_barcodes
                                })
                            # print("At line 4836 shifts_data_list_tbm:", shifts_data_list_tbm)

                            shift_groups = machine_batch_data.groupby('shift')
                            shifts_data_list = []
                            for shift_name, shift_data in shift_groups:
                                if not shift_data.empty and 'date_of_ufm' in shift_data.columns:
                                    shift_data_sorted = shift_data.sort_values('date_of_ufm')
                                    shifts_data_list.append({
                                        "shifts": shift_name,
                                        "barcode_count": len(shift_data_sorted["barcode"]) if "barcode" in shift_data_sorted.columns else 0,
                                        "machine_shift_ufm_start_time": shift_data_sorted["date_of_ufm"].iloc[0],
                                        "machine_shift_ufm_end_time": shift_data_sorted["date_of_ufm"].iloc[-1],
                                        "barcodes": shift_data_sorted["barcode"].tolist() if "barcode" in shift_data_sorted.columns else []
                                    })

                            machine_alert_detail = {
                                "machine": machine_no,
                                "tbm_machine": tbm_machine_value,  # MODIFIED: tbm_machine may be None if not created
                                "ufm_shift_occured": machine_lot_overall_shift_occurred,
                                "shifts_data": shifts_data_list,
                                "shifts_data_tbm": shifts_data_list_tbm,
                                "ufm_yield": ufm_yield(machine_batch_data),
                                "rorank_yield": rorank_yield(machine_batch_data),
                                "balancing_yield": balancing_yield(machine_batch_data),
                                "total_rank_yield": total_rank_yield(machine_batch_data),
                                "visual_yield": visual_yield(machine_batch_data),
                                "sizecode": sizecode, 
                                "gt_code": str(gt_code),   # Add 16-07
                                "target_machine_lot_size": target_lot_size,
                                "processed_machine_barcodes": target_lot_size,
                                "machine_lot_start_ufm_time": machine_batch_data["date_of_ufm"].min() if not machine_batch_data["date_of_ufm"].empty else None,
                                "machine_lot_end_ufm_time": machine_batch_data["date_of_ufm"].max() if not machine_batch_data["date_of_ufm"].empty else None,
                                "machine_lot_start_build_time": machine_batch_data["date_of_tbm"].min() if not machine_batch_data["date_of_tbm"].empty else None,
                                "machine_lot_end_build_time": machine_batch_data["date_of_tbm"].max() if not machine_batch_data["date_of_tbm"].empty else None,
                                "parameter_alerts": parameter_alerts,
                                "lot_averages": {k: round(v, 3) if v is not None and not pd.isna(v) else None for k, v in lot_averages.items()},
                                "previous_lot_averages": {k: round(v, 3) if v is not None and not pd.isna(v) else None for k, v in prev_lot_avg.items()} if prev_lot_avg else {},
                                "first_barcode_machine": machine_batch_data.iloc[0]["barcode"] if not machine_batch_data.empty and "barcode" in machine_batch_data.columns else None,
                                "last_barcode_machine": machine_batch_data.iloc[-1]["barcode"] if not machine_batch_data.empty and "barcode" in machine_batch_data.columns else None,
                                "ufm_machine_model_lot_id": current_ufm_machine_lot_id,
                                "ufm_previous_machine_model_lot_id": ufm_previous_machine_model_lot_id,
                                "rank_columns": get_rank_columns(machine_batch_data),
                                "barcodes": machine_batch_data["barcode"].tolist() if "barcode" in machine_batch_data.columns else []
                            }

                            # Alert creation (modified to collect into alert_map)
                            alert_doc_main = {
                                "_id": f"main_alert_{last_alert_id}",
                                "alert_id_ufm_ufm": last_alert_id,
                                "modelno": model_number,
                                "lot_id": current_model_lot_id,
                                "alert_flag": False,
                                "dashboard_flag": False,
                                "machine_flag": False,
                                "alert_status_check": "o",
                                "alert_status_timestamp": None,
                                "alert_status_comment": [],
                                "alert_assign_user": [],
                                "machine_alerts": [machine_alert_detail],
                                "first_barcode_model": machine_alert_detail["first_barcode_machine"],
                                "last_barcode_model": machine_alert_detail["last_barcode_machine"],
                                "timestamp_alert_generated": some_function()
                            }
                            
                            # Store in alert_map for Virgin/Final yield (unchanged)
                            alert_map[last_alert_id] = {
                                "alert_id_ufm_ufm": last_alert_id,
                                "modelno": model_number,
                                "lot_id": current_model_lot_id,
                                "machine_alerts": [machine_alert_detail.copy()]
                            }

                            # Define trigger_doc safely – use unique _id every time
                            trigger_doc = {
                                "_id": f"trigger_alert_{last_alert_id}",   # Unique every time!
                                "alert_id_ufm_ufm": last_alert_id,
                                "modelno": model_number,
                                "lot_id": current_model_lot_id,
                                "type": "ufm_trigger",
                                "timestamp_alert_generated": some_function()
                            }

                            # ALWAYS insert into alert_table and alert_db_dashboard (no duplicates)
                            # 1. Insert main alert document
                            if not alert_collect.find_one({"alert_id_ufm_ufm": last_alert_id}):
                                try:
                                    alert_collect.insert_one(convert_numpy_types(alert_doc_main))
                                    logging.info(f"Inserted alert_doc_main into alert_table for alert_id {last_alert_id}")
                                    output_messages.append(f"Inserted into alert_table: alert_id {last_alert_id}")
                                except Exception as e:
                                    logging.error(f"Failed to insert into alert_table: {e}")

                            # 2. Insert into alert_db_dashboard (main + trigger)
                            if not alert_triggering_collect_dashboard_ufm.find_one({"alert_id_ufm_ufm": last_alert_id}):
                                try:
                                    alert_triggering_collect_dashboard_ufm.insert_one(convert_numpy_types(alert_doc_main))
                                    logging.info(f"Inserted alert_doc_main into alert_db_dashboard for alert_id {last_alert_id}")
                                    output_messages.append(f"Inserted into alert_db_dashboard (main): alert_id {last_alert_id}")
                                except Exception as e:
                                    logging.error(f"Failed to insert main alert into alert_db_dashboard: {e}")

                            if not alert_triggering_collect_dashboard_ufm.find_one({"_id": f"trigger_alert_{last_alert_id}"}):
                                try:
                                    alert_triggering_collect_dashboard_ufm.insert_one(convert_numpy_types(trigger_doc))
                                    logging.info(f"Inserted trigger_doc into alert_db_dashboard for alert_id {last_alert_id}")
                                    output_messages.append(f"Inserted into alert_db_dashboard (trigger): alert_id {last_alert_id}")
                                except Exception as e:
                                    logging.error(f"Failed to insert trigger_doc into alert_db_dashboard: {e}")
                            else:
                                logging.info(f"trigger_doc already exists for alert_id {last_alert_id} – skipped")

                            # Keep your existing bulk operations (only when shift occurs)
                            if machine_lot_overall_shift_occurred:
                                if not alert_ufm_collect.find_one({"alert_id_ufm_ufm": last_alert_id}):
                                    ufm_alert_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(alert_doc_main)))
                                    ufm_triggering_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(trigger_doc)))
                                    ufm_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(trigger_doc)))
                                    logging.info(f"Shift detected - added to bulk ops for alert_id {last_alert_id}")
                                    output_messages.append(f"Shift alert queued for bulk: alert_id {last_alert_id}")

                            last_alert_id += 1
                            merge_df.loc[machine_batch_indices, "alert_id_ufm_ufm"] = last_alert_id
                            machine_barcode_counter += target_lot_size

                    except Exception as e:
                        logging.error("UFM Error processing machine %s for model %s: %s", ufm_machine_id, model_number, e)
                        output_messages.append(f"UFM Error processing machine {ufm_machine_id} for model {model_number}: {e}")
                        continue

                # ==================== VIRGIN & FINAL YIELD ALERT INSERTION (NOW UNCOMMENTED & FIXED) ====================
                try:
                    for alert_key, alert_obj in alert_map.items():
                        true_machine_alerts = [ma for ma in alert_obj["machine_alerts"] if ma.get("ufm_shift_occured", False) in [True, False]]
                        if not true_machine_alerts:
                            continue

                        # Prepare barcode sets
                        virgin_barcodes_set = set(virgin_data["barcode"].dropna().astype(str))
                        final_barcodes_set = set(final_data["barcode"].dropna().astype(str))

                        # === VIRGIN YIELD ===
                        virgin_machine_alerts = []
                        virgin_barcodes_seen = set()
                        for ma in true_machine_alerts:
                            original_barcodes = ma.get("barcodes", [])
                            if not original_barcodes:
                                continue
                            filtered = [bc for bc in original_barcodes if str(bc) in virgin_barcodes_set and str(bc) not in virgin_barcodes_seen]
                            if filtered:
                                virgin_ma = ma.copy()
                                virgin_ma["barcodes"] = filtered
                                virgin_ma["barcode_count"] = len(filtered)
                                virgin_machine_alerts.append(virgin_ma)
                                virgin_barcodes_seen.update(filtered)

                        if virgin_machine_alerts:
                            all_virgin_bcs = sorted(virgin_barcodes_seen,
                                key=lambda x: virgin_data[virgin_data["barcode"].astype(str) == x]["date_of_dt"].iloc[0])
                            doc_virgin = {
                                "alert_id_ufm_ufm": alert_obj["alert_id_ufm_ufm"],
                                "modelno": alert_obj["modelno"],
                                "lot_id": alert_obj["lot_id"],
                                "alert_flag": False,
                                "dashboard_flag": False,
                                "machine_flag": False,
                                "alert_status_check": "o",
                                "alert_status_timestamp": None,
                                "alert_status_comment": [],
                                "alert_assign_user": [],
                                "machine_alerts": virgin_machine_alerts,
                                "first_barcode_model": all_virgin_bcs[0] if all_virgin_bcs else None,
                                "last_barcode_model": all_virgin_bcs[-1] if all_virgin_bcs else None,
                                "timestamp_alert_generated": some_function(),
                                "yield_type": "virgin"
                            }
                            alert_triggering_virgin_yield_collect.insert_one(convert_numpy_types(doc_virgin))
                            logging.info(f"Inserted Virgin Yield Alert {alert_obj['alert_id_ufm_ufm']} into alert_db_virgin_yield")
                            output_messages.append(f"Inserted Virgin Yield Alert {alert_obj['alert_id_ufm_ufm']}")

                        # === FINAL YIELD (already working, kept as-is with small cleanup) ===
                        final_machine_alerts = []
                        final_barcodes_seen = set()
                        for ma in true_machine_alerts:
                            original_barcodes = ma.get("barcodes", [])
                            if not original_barcodes:
                                continue
                            filtered = [bc for bc in original_barcodes if str(bc) in final_barcodes_set and str(bc) not in final_barcodes_seen]
                            if filtered:
                                final_ma = ma.copy()
                                final_ma["barcodes"] = filtered
                                final_ma["barcode_count"] = len(filtered)
                                final_machine_alerts.append(final_ma)
                                final_barcodes_seen.update(filtered)

                        if final_machine_alerts:
                            all_final_bcs = sorted(final_barcodes_seen,
                                key=lambda x: final_data[final_data["barcode"].astype(str) == x]["date_of_dt"].iloc[0])
                            doc_final = {
                                "alert_id_ufm_ufm": alert_obj["alert_id_ufm_ufm"],
                                "modelno": alert_obj["modelno"],
                                "lot_id": alert_obj["lot_id"],
                                "alert_flag": False,
                                "dashboard_flag": False,
                                "machine_flag": False,
                                "alert_status_check": "o",
                                "alert_status_timestamp": None,
                                "alert_status_comment": [],
                                "alert_assign_user": [],
                                "machine_alerts": final_machine_alerts,
                                "first_barcode_model": all_final_bcs[0] if all_final_bcs else None,
                                "last_barcode_model": all_final_bcs[-1] if all_final_bcs else None,
                                "timestamp_alert_generated": some_function(),
                                "yield_type": "final"
                            }
                            alert_triggering_final_yield_collect.insert_one(convert_numpy_types(doc_final))
                            alert_final_yield_collect.insert_one(convert_numpy_types(doc_final))  # Main Final Yield DB

                            # Deviated Final Yield (only when shift=True)
                            has_shift = any(ma.get("ufm_shift_occured", False) for ma in final_machine_alerts)
                            if has_shift:
                                deviated_parameter_alerts = []
                                for ma in final_machine_alerts:
                                    if not ma.get("ufm_shift_occured", False):
                                        continue
                                    for alert in ma.get("parameter_alerts", []):
                                        if alert.get("shift_occurred_thershold", False) or alert.get("shift_occurred_spec", False):
                                            dev_alert = alert.copy()
                                            param = dev_alert.get("parameter", "")
                                            if param.endswith("_yield") and param in rank_param_mappings:
                                                batch_data = final_data[final_data["barcode"].astype(str).isin(ma["barcodes"])]
                                                rank_counts = {}
                                                for col in rank_param_mappings[param]:
                                                    if col in batch_data.columns:
                                                        counts = batch_data[col].value_counts()
                                                        for val, cnt in counts.items():
                                                            val = int(val) if isinstance(val, (int, float)) else val
                                                            rank_counts[f"{col}_{val}"] = int(cnt)
                                                dev_alert["rank_counts"] = rank_counts
                                            deviated_parameter_alerts.append(dev_alert)
                                            
                                if deviated_parameter_alerts:
                                    # --------------------------------------------------------------
                                    #  NEW: get the lot-ids from the **first** machine-alert that
                                    #       contributed to the deviated final-yield alert.
                                    # --------------------------------------------------------------
                                    first_ma = final_machine_alerts[0]                     # <-- safe because the list is not empty here
                                    ufm_machine_model_lot_id      = first_ma.get("ufm_machine_model_lot_id")
                                    ufm_previous_machine_model_lot_id = first_ma.get("ufm_previous_machine_model_lot_id")

                                    deviated_doc = {
                                        "alert_id_tbm_ufm": alert_obj["alert_id_ufm_ufm"],
                                        "modelno": alert_obj["modelno"],
                                        "lot_id": alert_obj["lot_id"],
                                        "machine": final_machine_alerts[0]["machine"],

                                        # <<<===  ADDED FIELDS  ===>>>
                                        "ufm_machine_model_lot_id": ufm_machine_model_lot_id,
                                        "ufm_previous_machine_model_lot_id": ufm_previous_machine_model_lot_id,
                                        # <<<========================>>>

                                        "comments": [],
                                        "alert_assign_user": [],
                                        "parameter_alerts": deviated_parameter_alerts,
                                        "timestamp_alert_generated": some_function(),
                                        "yield_type": "final"
                                    }

                                    # insert into the dedicated deviated-final-yield collection
                                    alert_deviated_final_yield_collect.insert_one(convert_numpy_types(deviated_doc))
                                    logging.info(
                                        f"Deviated Final Yield Alert {alert_obj['alert_id_ufm_ufm']} → "
                                        f"alert_deviated_final_yield (lot_id={ufm_machine_model_lot_id})"
                                    )
                                    output_messages.append(
                                        f"Deviated Final Yield Alert {alert_obj['alert_id_ufm_ufm']} inserted "
                                        f"with ufm_machine_model_lot_id={ufm_machine_model_lot_id}"
    )

                                # if deviated_parameter_alerts:
                                #     deviated_doc = {
                                #         "alert_id_ufm_ufm": alert_obj["alert_id_ufm_ufm"],
                                #         "modelno": alert_obj["modelno"],
                                #         "lot_id": alert_obj["lot_id"],
                                #         "machine": final_machine_alerts[0]["machine"],
                                #         "comments": [],
                                #         "alert_assign_user": [],
                                #         "parameter_alerts": deviated_parameter_alerts,
                                #         "timestamp_alert_generated": some_function(),
                                #         "yield_type": "final"
                                #     }
                                #     alert_deviated_final_yield_collect.insert_one(convert_numpy_types(deviated_doc))
                                #     logging.info(f"Deviated Final Yield Alert {alert_obj['alert_id_ufm_ufm']} inserted")
                                #     output_messages.append(f"Deviated Final Yield Alert {alert_obj['alert_id_ufm_ufm']} inserted")

                except Exception as e:
                    logging.error(f"Error in Virgin/Final Yield alert insertion: {e}")
                    output_messages.append(f"Error in Virgin/Final Yield alert insertion: {e}")

        except Exception as e:
            logging.error("UFM Error processing model %s: %s", model_number, e)
            output_messages.append(f"UFM Error processing model {model_number}: {e}")
            continue

    # Perform Bulk Writes
    final_log_messages = []
    if ufm_lot_bulk_operations:
        try:
            result = lot_ufm_collect.bulk_write(ufm_lot_bulk_operations, ordered=False)
            inserted_counts["lot_ufm_table"] = result.inserted_count
            final_log_messages.append(f"✅ UFM Inserted {result.inserted_count} documents into UFM lot collection.")
        except pymongo.errors.BulkWriteError as bwe:
            inserted_counts["lot_ufm_table"] = bwe.details['nInserted']
            final_log_messages.append(f"⚠️ UFM Lot Bulk Write Errors: Inserted {bwe.details['nInserted']}. Errors: {bwe.details['writeErrors']}")
        except Exception as e:
            final_log_messages.append(f"❌ UFM Error performing bulk write to UFM lot collection: {e}")

    if ufm_averages_bulk_operations:
        try:
            result = averages_ufm_collect.bulk_write(ufm_averages_bulk_operations, ordered=False)
            inserted_counts["averages_ufm_table"] = result.inserted_count
            final_log_messages.append(f"✅ UFM Inserted {result.inserted_count} documents into UFM averages collection.")
        except pymongo.errors.BulkWriteError as bwe:
            inserted_counts["averages_ufm_table"] = bwe.details['nInserted']
            final_log_messages.append(f"⚠️ UFM Averages Bulk Write Errors: Inserted {bwe.details['nInserted']}. Errors: {bwe.details['writeErrors']}")
        except Exception as e:
            final_log_messages.append(f"❌ UFM Error performing bulk write to UFM averages collection: {e}")

    if deviated_triggering_bulk_operations:
        try:
            result = alert_triggering_deviated_collect.bulk_write(deviated_triggering_bulk_operations, ordered=False)
            inserted_counts["alert_ufm_with_lot_size"] = result.inserted_count
            output_messages.append(f"✅ UFM Inserted {result.inserted_count} deviated alert documents into alert_deviated.alert_ufm_with_lot_size.")
        except Exception as e:
            error_message = f"❌ UFM Error performing bulk write to deviated alert collections: {e}"
            output_messages.append(error_message)
    else:
        output_messages.append("No UFM deviated alert documents to insert into deviated alert collections.")

    if ufm_alert_bulk_operations:
        try:
            result = alert_ufm_collect.bulk_write(ufm_alert_bulk_operations, ordered=False)
            inserted_counts["alert_ufm_collect"] = result.inserted_count  # MODIFIED: Use string key
            final_log_messages.append(f"✅ UFM Inserted {result.inserted_count} documents into UFM alert collection.")
        except pymongo.errors.BulkWriteError as bwe:
            inserted_counts["alert_ufm_collect"] = bwe.details['nInserted']  # MODIFIED: Use string key
            final_log_messages.append(f"⚠️ UFM Alert Bulk Write Errors: Inserted {bwe.details['nInserted']}. Errors: {bwe.details['writeErrors']}")
        except Exception as e:
            final_log_messages.append(f"❌ UFM Error performing bulk write to UFM alert collection: {e}")

    if ufm_triggering_bulk_operations:
        # NEW: Separate try-except for alert_ufm_collect
        try:
            result = alert_ufm_collect.bulk_write(ufm_triggering_bulk_operations, ordered=False)
            inserted_counts["alert_ufm_collect"] = result.inserted_count  # MODIFIED: Use string key
            final_log_messages.append(f"✅ UFM Inserted {result.inserted_count} documents into alert_db.{alert_triggering_ubno_collect_name}.")
        except pymongo.errors.BulkWriteError as bwe:
            inserted_counts["alert_ufm_collect"] = bwe.details['nInserted']  # MODIFIED: Use string key
            final_log_messages.append(f"⚠️ UFM Triggering Alert Bulk Write Errors in alert_db.{alert_triggering_ubno_collect_name}: Inserted {bwe.details['nInserted']}. Errors: {bwe.details['writeErrors']}")
        except Exception as e:
            final_log_messages.append(f"❌ UFM Error performing bulk write to alert_db.{alert_triggering_ubno_collect_name}: {e}")

        # NEW: Separate try-except for alert_db_dashboard
        try:
            result_dashboard = alert_triggering_collect_dashboard_ufm.bulk_write(ufm_triggering_bulk_operations, ordered=False)
            inserted_counts["alert_triggering_collect_dashboard_ufm"] = result_dashboard.inserted_count
            output_messages.append(f"✅ UFM Inserted {result_dashboard.inserted_count} machine_alert documents into alert_db_dashboard.{alert_triggering_ubno_collect_name}.")  # MODIFIED
        except pymongo.errors.BulkWriteError as bwe:
            inserted_counts["alert_triggering_collect_dashboard_ufm"] = bwe.details['nInserted']
            output_messages.append(f"⚠️ UFM Triggering Alert Bulk Write Errors in alert_db_dashboard.{alert_triggering_ubno_collect_name}: Inserted {bwe.details['nInserted']}. Errors: {bwe.details['writeErrors']}")  # NEW
        except Exception as e:
            output_messages.append(f"❌ UFM Error performing bulk write to alert_db_dashboard.{alert_triggering_ubno_collect_name}: {e}")  # NEW
    else:
        output_messages.append(f"No UFM triggering alert documents to insert into alert_db_dashboard.{alert_triggering_ubno_collect_name}.")  # NEW

    if ufm_bulk_operations:
        try:
            result = alert_collect.bulk_write(ufm_bulk_operations, ordered=False)
            inserted_counts["alert_collect"] = result.inserted_count  # MODIFIED: Use string key
            final_log_messages.append(f"✅ UFM Inserted {result.inserted_count} documents into UFM triggering alert collection.")
        except pymongo.errors.BulkWriteError as bwe:
            inserted_counts["alert_collect"] = bwe.details['nInserted']  # MODIFIED: Use string key
            final_log_messages.append(f"⚠️ UFM Triggering Alert Bulk Write Errors: Inserted {bwe.details['nInserted']}. Errors: {bwe.details['writeErrors']}")
        except Exception as e:
            final_log_messages.append(f"❌ UFM Error performing bulk write to UFM triggering alert collection: {e}")

    output_messages.extend(final_log_messages)

    # Final Update to merge_collect
    ufm_bulk_updates_merge = []
    processed_merge_df = merge_df.dropna(subset=["ufm_machine_model_lot_id"])

    if not processed_merge_df.empty:
        for index, row in processed_merge_df.iterrows():
            query = {"_id": row["_id"]}
            update_payload = {"$set": {
                "alert_id_ufm_ufm": row.get("alert_id_ufm_ufm"),
                "lot_id": row.get("lot_id"),
                "previous_lot_id": row.get("previous_lot_id"),
                "ufm_machine_model_lot_id": row.get("ufm_machine_model_lot_id"),
                "ufm_previous_machine_model_lot_id": row.get("ufm_previous_machine_model_lot_id")
            }}
            # MODIFIED: Only add tbm_machine to update if it exists in merge_df
            if "tbm_machine" in merge_df.columns:
                update_payload["$set"]["tbm_machine"] = row["tbm_machine"]
            ufm_bulk_updates_merge.append(pymongo.UpdateOne(query, update_payload))
            update_payload["$set"] = {k: v for k, v in update_payload["$set"].items() if v is not None}
            if update_payload["$set"]:
                ufm_bulk_updates_merge.append(pymongo.UpdateOne(query, update_payload))

        if ufm_bulk_updates_merge:
            logging.info("ℹ️ Preparing final update for %s documents in main merge collection...", len(ufm_bulk_updates_merge))
            try:
                result = merge_collect.bulk_write(ufm_bulk_updates_merge, ordered=False)
                updated_counts["merge_collection_ufm"] = result.modified_count
                logging.info("✅ UFM Updated %s documents in main merge collection with generated IDs.", result.modified_count)
            except pymongo.errors.BulkWriteError as bwe:
                updated_counts["merge_collection_ufm"] = bwe.details['nModified']
                logging.info("⚠️ UFM Final Merge Update Bulk Write Errors: Modified %s. Errors: %s", bwe.details['nModified'], bwe.details['writeErrors'])
            except Exception as e:
                logging.info("❌ UFM Error updating main merge collection: %s", e)
        else:
            logging.info("ℹ️ No final updates required for the main merge collection for UFM.")
    else:
        logging.info("ℹ️ No processed UFM rows found in merge_df for final update.")

    process_end_time = time.time()
    execution_time = process_end_time - process_start_time
    final_message = f"✅ UFM Lot and Alert Process completed successfully in {execution_time:.2f} seconds!"
    logging.info(final_message)

    logging.info("process_lot_and_alert_data function is completed")
    return {
        "status": "success",
        "message": final_message,
        "output_log": output_messages,
        "inserted_counts": inserted_counts,
        "updated_counts": updated_counts,
        "execution_time": execution_time
    }, 200

def process_tbm_lot_and_alert_data_without_lot_size():
    start_time = time.time()
    output_messages = []
    inserted_counts = {}
    updated_counts = {}
    logging.info("process_tbm_lot_and_alert_data_without_lot_size function is just started")
    
    # Initialize MongoDB collections
    try:
        merge_collect = client[TUB_CIMCORP_DB][new_inner_join_collection_collection]
        channel_collect_tbm = client[config_databse][channel_no_collect_tbm]
        channel_collect_ufm = client[config_databse][channel_no_collect]
        lot_tbm_collect = client[lot_db][lot_tbm_collect_name_new]
        alert_collect = client[alert_table][alert_tbm_collect_name_new]
        averages_collect = client[average_db][averages_tbm_table_new]
        machine_collect = client[config_databse][machine_no_collect]
        curing_press_collect = client[config_databse][curing_press_dict_collect]
        alert_triggering_collect = client[alert_db][alert_triggering_collect_name_new]
        summary_triggering_collect = client[summary_db][summary_triggering_collect_name_tbm]
        # NEW: Initialize new collection for deviated alerts
        alert_triggering_deviated_collect = client[alert_deviated][alert_triggering_deviated_tbm_new]
        # NEW: Initialize collections for alert_db_dashboard with the same collection names
        alert_triggering_collect_dashboard = client[alert_db_dashboard][alert_triggering_collect_name_new]
        # === NEW: Virgin Yield & Final Yield Collections ===
        alert_triggering_virgin_yield_collect = client[alert_db_virgin_yield][alert_triggering_collect_name_new]
        alert_triggering_final_yield_collect = client[alert_db_final_yield][alert_triggering_collect_name_new]
        # === NEW: Dedicated Final Yield Databases ===
        alert_final_yield_collect = client["alert_table_final_yield"][alert_tbm_collect_name_new]
        alert_deviated_final_yield_collect = client["alert_deviated_final_yield"][alert_triggering_deviated_tbm_new]
         # NEW: Initialize collections for model_workcenter_db
        tbm_gtcodes_collect = client[model_db][tbm_collection]
        tbm_machine_collect = client[model_db][tbm_collection_machine]
        output_messages.append("✅ TBM Collections loaded successfully for lot and alert processing in both alert_db and alert_db_dashboard.")
    except pymongo.errors.OperationFailure as e:
        error_message = f"❌ TBM Database or collection error during lot/alert processing: {e}"
        output_messages.append(error_message)
        return {"status": "error", "message": error_message, "output_log": output_messages}, 500
    
    # Fetch active machines from model_workcenter_db.tbm_machine
    try:
        active_machines = set(tbm_machine_collect.distinct("machine", {"status": "active"}))
        output_messages.append(f"✅ Fetched {len(active_machines)} active machines: {active_machines}")
        if not active_machines:
            output_messages.append("❌ No active machines found in model_workcenter_db.tbm_machine. Exiting.")
            return {"status": "error", "message": "No active machines found", "output_log": output_messages}, 500
    except Exception as e:
        output_messages.append(f"❌ Error fetching active machines: {e}")
        return {"status": "error", "message": "Failed to fetch active machines", "output_log": output_messages}, 500

    
    # Fetch active model numbers
    try:
        active_modelnos = set(tbm_gtcodes_collect.distinct("modelno", {"status": "active"}))
        output_messages.append(f"✅ Fetched {len(active_modelnos)} active model numbers: {active_modelnos}")
    except Exception as e:
        output_messages.append(f"❌ Error fetching active model numbers: {e}")
        return {"status": "error", "message": "Failed to fetch active model numbers", "output_log": output_messages}, 500

    # Load data into DataFrames
    try:
        merge1 = list(merge_collect.find({}))
        merge_df = pd.DataFrame(merge1)
        channel_data_tbm = list(channel_collect_tbm.find({"modelno": {"$in": list(active_modelnos)}}))
        channel_data_tbm = list(channel_collect_tbm.find({}))
        channel_df = pd.DataFrame(channel_data_tbm)
        channel_data_ufm = list(channel_collect_ufm.find({}))
        channel_df_ufm = pd.DataFrame(channel_data_ufm)
        machine_data = list(machine_collect.find({"machine": {"$in": list(active_machines)}}))
        machine_data = list(machine_collect.find({}))
        machine_df = pd.DataFrame(machine_data)
        output_messages.append("✅ TBM Data loaded from collections into DataFrames for lot and alert processing.")
    except Exception as e:
        error_message = f"❌ TBM Error loading data for lot/alert processing: {e}"
        output_messages.append(error_message)
        return {"status": "error", "message": error_message, "output_log": output_messages}, 500

    # Get distinct model numbers
    channel_modelnos = set(channel_collect_tbm.distinct("modelno"))

    # Check required columns
    required_columns = {"modelno_cimcorp", "alert_id_tbm_ufm"}
    if not required_columns.issubset(merge_df.columns):
        output_messages.append("Required columns are missing in merge_df.")
        return {"status": "error", "message": "Required columns missing", "output_log": output_messages}, 500

    # Filter data
    filtered_df = merge_df[merge_df["modelno_cimcorp"].isin(channel_modelnos) & merge_df["alert_id_tbm_ufm"].isna()]
    if not filtered_df.empty:
        merge_df = filtered_df
    else:
        output_messages.append("No data after filtering or condition not satisfied.")
        return {"status": "error", "message": "No data after filtering", "output_log": output_messages}, 500

    # Assign date columns
    merge_df["date_of_tbm"] = merge_df["tbm_buildtime"]
    merge_df["date_of_curing"] = merge_df["curing_infeedtime"]
    merge_df["date_and_time"] = merge_df["date"].fillna('') + " " + merge_df["time"].fillna('')

    # Function to handle datetime conversion
    def convert_datetime_with_invalid_mask(series, col_name):
        try:
            original_series = series.copy()
            # Normalize whitespace and ensure string type
            cleaned_series = original_series.astype(str).str.strip().str.replace(r'\s+', ' ', regex=True)

            # Use correct format matching your actual data: "YYYY-MM-DD HH:MM:SS"
            datetime_series = pd.to_datetime(cleaned_series, format='%Y-%m-%d %H:%M:%S', errors='coerce')

            # Identify invalid (unparsable) entries
            invalid_mask = datetime_series.isna() & original_series.notna()

            # Replace invalids with DEFAULT_DATETIME (must be a datetime object)
            datetime_series[invalid_mask] = DEFAULT_DATETIME

            # Return formatted datetime string
            return datetime_series.dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            return pd.Series([DEFAULT_DATETIME] * len(series))

    # Process datetime columns
    try:
        merge_df["date_and_time"] = convert_datetime_with_invalid_mask(merge_df["date_and_time"], "date_and_time")
        merge_df["date_and_time"] = merge_df["date_and_time"].astype(object).where(merge_df["date_and_time"].notnull(), "No Date")
        merge_df["date_of_tbm"] = convert_datetime_with_invalid_mask(merge_df["date_of_tbm"], "date_of_tbm")
        merge_df["date_of_tbm"] = merge_df["date_of_tbm"].astype(object).where(merge_df["date_of_tbm"].notnull(), "No Date")
        merge_df["date_of_curing"] = convert_datetime_with_invalid_mask(merge_df["date_of_curing"], "date_of_curing")
        merge_df["date_of_curing"] = merge_df["date_of_curing"].astype(object).where(merge_df["date_of_curing"].notnull(), "No Date")
        merge_df["time_of_ufm"] = pd.to_datetime(merge_df["time"], errors="coerce").dt.strftime("%H:%M:%S")
        time_invalid_mask = merge_df["time_of_ufm"].isna() & merge_df["time"].notna()
        if time_invalid_mask.any():
            logging.info("⚠️ Found %s invalid time values", time_invalid_mask.sum())
    except Exception as e:
        logging.info("❌ Error during datetime conversion: %s", e)
        return {"status": "error", "message": f"Error during datetime conversion: {e}", "output_log": output_messages}, 500

    merge_df["tbm_shift"] = merge_df["date_of_tbm"].apply(get_shift)

    # Fetch last alert ID
    try:
        last_alert = alert_collect.find_one(sort=[("alert_id_tbm_ufm", -1)])
        last_alert_id = last_alert["alert_id_tbm_ufm"] if last_alert else 0
        output_messages.append(f"ℹ️ Last TBM Alert ID found: {last_alert_id}")
    except Exception as e:
        error_message = f"❌ TBM Error fetching last alert ID for lot/alert processing: {e}"
        output_messages.append(error_message)
        last_alert_id = 0

    # Fetch last lot IDs per machine and model
    try:
        last_lots = list(lot_tbm_collect.aggregate([
            {
                "$match": {
                    "lot_id": {"$regex": "^TBM_[^_]+_[^_]+_\\d+$"},
                    "modelno": {"$in": list(active_modelnos)},
                    "machine": {"$in": list(active_machines)}
                }
            },
            {
                "$addFields": {
                    "lot_number": {
                        "$toInt": {
                            "$arrayElemAt": [
                                {"$split": ["$lot_id", "_"]},
                                -1
                            ]
                        }
                    },
                    "modelno": "$modelno",
                    "machine": "$machine"
                }
            },
            {
                "$sort": {"lot_number": -1}
            },
            {
                "$group": {
                    "_id": {"modelno": "$modelno", "machine": "$machine"},
                    "max_lot_number": {"$first": "$lot_number"},
                    "last_lot_id": {"$first": "$lot_id"}
                }
            }
        ]))
        last_lot_dict = {
            (item["_id"]["modelno"], item["_id"]["machine"]): {
                "max_lot_number": item["max_lot_number"],
                "last_lot_id": item["last_lot_id"]
            }
            for item in last_lots
        }
    except Exception as e:
        error_message = f"❌ TBM Error fetching last lot IDs: {e}"
        logging.error(error_message)
        output_messages.append(error_message)
        last_lot_dict = {}

    # Fetch last tbm_machine_model_lot_id per machine and model
    try:
        last_tbm_lots = list(averages_collect.aggregate([
            {
                "$match": {
                    "tbm_machine_model_lot_id": {"$regex": "^[^_]+_[^_]+_TBM_[^_]+_[^_]+_\\d+_\\d+$"},
                    "modelno": {"$in": list(active_modelnos)},
                    "machine": {"$in": list(active_machines)}
                }
            },
            {
                "$addFields": {
                    "lot_number": {
                        "$toInt": {
                            "$arrayElemAt": [
                                {"$split": ["$tbm_machine_model_lot_id", "_"]},
                                -2
                            ]
                        }
                    }
                }
            },
            {
                "$sort": {"lot_number": -1}
            },
            {
                "$group": {
                    "_id": {"modelno": "$modelno", "machine": "$machine"},
                    "max_tbm_lot_id": {"$first": "$tbm_machine_model_lot_id"}
                }
            }
        ]))
        last_tbm_lot_dict = {
            (item["_id"]["modelno"], item["_id"]["machine"]): item["max_tbm_lot_id"]
            for item in last_tbm_lots if item["max_tbm_lot_id"]
        }
    except Exception as e:
        error_message = f"❌ TBM Error fetching last lot IDs for lot/alert processing: {e}"
        output_messages.append(error_message)
        last_tbm_lot_dict = {}

    # Prepare merge_df
    merge_df["tbm"] = merge_df["tbm"].astype(str)
    machine_df["machine"] = machine_df["machine"].astype(str)
    machine_df["tbm"] = machine_df["tbm"].astype(str)
    merge_df = merge_df.merge(machine_df[["tbm", "machine"]], on="tbm", how="left")
    merge_df["tbm"] = merge_df["machine"].fillna(merge_df["tbm"])
    # NEW: Filter merge_df to include only active machines
    merge_df = merge_df[merge_df["machine"].isin(active_machines)]
    if merge_df.empty:
        output_messages.append("❌ No data in merge_df after filtering for active machines. Exiting.")
        return {"status": "error", "message": "No data after active machine filtering", "output_log": output_messages}, 500


    # Initialize tracking variables
    tbm_previous_machine_model_lot_ids = last_tbm_lot_dict.copy()
    machine_lot_avg_map = {}
    alert_bulk_operations = []
    averages_bulk_operations = []
    bulk_operations = []
    triggering_bulk_operations = []
    # NEW: Initialize bulk operations for derived triggering collection
    deviated_triggering_bulk_operations = []
    summary_bulk_operation = []
    debug_data_list_tbm = []

    conicity_params = ["con_kgf", "cwrfv_oa_kgf", "ccwrfv_oa_kgf", "cwrfv_oa1h_kgf", "ccwrfv_oa1h_kgf",
                       "lfv_oa_kgf", "lfv_oa_1_kgf", "upper_g", "lower_g", "static_g", "up_low_g", "load1", "airpress1"]
    yield_params = [
        "total_rank_yield", "rfv_yield", "rfh1_yield", "lfv_yield", "conicity_yield",
        "ufm_yield", "rorank_yield", "radial_runout_yield", "radial_runout_first_harmonic_yield",
        "lateral_run_out_yield", "lateral_run_out_top_over_yield", "lateral_run_out_bottom_over_yield",
        "lateral_run_out_harmonic_yield", "lateral_run_out_top_first_harmonic_yield",
        "lateral_run_out_bottom_first_harmonic_yield", "bulge_yield", "bulge_top_over_yield",
        "bulge_bottom_over_yield", "dent_yield", "dent_top_over_yield", "dent_bottom_over_yield",
        "balancing_yield", "upper_yield", "lower_yield", "upper_lower_yield", "static_yield", "warehouse_yield", "visual_yield"
    ]

    # Define rank parameter mappings
    rank_param_mappings = {
        "total_rank_yield": ["total_rank"],
        "rfv_yield": ["cwrfv_oa_rank", "ccwrfv_oa_rank"],
        "rfh1_yield": ["cwrfv_oa1h_rank", "ccwrfv_oa1h_rank"],
        "lfv_yield": ["lfv_oa_rank", "lfv_oa_1_rank"],
        "conicity_yield": ["con_rank"],
        "ufm_yield": ["ufm_rank"],
        "rorank_yield": ["rorank"], 
        "radial_runout_yield": ["rc_oa_rank"], 
        "radial_runout_first_harmonic_yield": ["rc_oa1h_rank"],
        "lateral_run_out_top_over_yield": ["lt_oa_rank"],
        "lateral_run_out_bottom_over_yield": ["lb_oa_rank"],
        "lateral_run_out_top_first_harmonic_yield": ["lt_oa1h_rank"],
        "lateral_run_out_bottom_first_harmonic_yield": ["lb_oa1h_rank"],
        "bulge_top_over_yield": ["ltbulg_rank"],
        "bulge_bottom_over_yield": ["lbbulg_rank"],
        "dent_top_over_yield": ["dent_rank"],
        "dent_bottom_over_yield": ["dent1_rank"],
        "balancing_yield": ["bal_rank"],
        "upper_yield": ["upper_rank"],
        "lower_yield": ["lower_rank"],
        "upper_lower_yield": ["up_low_rank"],
        "static_yield": ["static_rank"],
        "lateral_run_out_yield": ["lt_oa_rank", "lb_oa_rank"],
        "lateral_run_out_harmonic_yield": ["lt_oa1h_rank", "lb_oa1h_rank"],
        "bulge_yield": ["ltbulg_rank", "lbbulg_rank"],
        "dent_yield": ["dent_rank", "dent1_rank"]
    }

    # TBM-wise processing
    for tbm_value, tbm_data in merge_df.groupby("tbm"):
        try:
            machine_no = tbm_value
            tbm_data = tbm_data[tbm_data["tbm_machine_model_lot_id"].isnull()].copy()
            if tbm_data.empty:
                output_messages.append(f"⚠️ No unprocessed data for TBM {machine_no}")
                continue

            total_tbm_barcodes = len(tbm_data)
            tbm_data.sort_values(["date_of_tbm"], ascending=True, inplace=True)

            # Inside the model processing loop
            for model_number, model_data in tbm_data.groupby("modelno"):
                try:
                    # Get channel data for the model
                    channel_row = channel_df[channel_df["modelno"] == model_number]
                    
                    if model_number not in active_modelnos:
                        output_messages.append(f"Skipping TBM model {model_number} as it is not active.")
                        continue

                    # Check if channel_row is empty
                    if channel_row.empty:
                        output_messages.append(f"⚠️ No channel data for model {model_number} in TBM {machine_no}")
                        continue
                    
                    # Retrieve gt_codes as a list
                    gt_codes = channel_row["gt_code"].iloc[0] if not channel_row["gt_code"].isna().iloc[0] else []
                    
                    # Ensure gt_codes is a list
                    if not isinstance(gt_codes, list):
                        gt_codes = [gt_codes]
                    
                    # Remove any None or NaN values from gt_codes
                    gt_codes = [code for code in gt_codes if pd.notna(code) and code is not None]
                    
                    # Log gt_codes for debugging
                    output_messages.append(f"ℹ️ Retrieved gt_codes for model {model_number}: {gt_codes}")

                    # Filter model_data based on gt_codes if available
                    if gt_codes:
                        model_data = model_data[model_data["tbm_gtcode"].isin(gt_codes)].copy()
                    else:
                        output_messages.append(f"⚠️ No valid gt_codes for model {model_number}, proceeding without gt_code filtering")

                    # Check if model_data is empty after filtering
                    if model_data.empty:
                        output_messages.append(f"⚠️ No matching tbm_gtcode for model {model_number} in TBM {machine_no} with gt_codes {gt_codes}")
                        continue

                    spec_con_kgf = float(channel_row["spec_con_kgf"].iloc[0] or 0)
                    sizecode = channel_row["sizecode"].iloc[0]
                    # gt_code = channel_row["gt_code"].iloc[0]
                    # spec_rfv = float(channel_row["spec_rfv"].iloc[0] or 0)
                    spec_cwrfv_oa_kgf = float(channel_row["spec_cwrfv_oa_kgf"].iloc[0] or 0)
                    spec_ccwrfv_oa_kgf = float(channel_row["spec_ccwrfv_oa_kgf"].iloc[0] or 0)
                    # spec_rfh1 = float(channel_row["spec_rfh1"].iloc[0] or 0)
                    spec_cwrfv_oa1h_kgf = float(channel_row["spec_cwrfv_oa1h_kgf"].iloc[0] or 0)
                    spec_ccwrfv_oa1h_kgf = float(channel_row["spec_ccwrfv_oa1h_kgf"].iloc[0] or 0)
                    # spec_lfv = float(channel_row["spec_lfv"].iloc[0] or 0)
                    spec_lfv_oa_kgf = float(channel_row["spec_lfv_oa_kgf"].iloc[0] or 0)
                    spec_lfv_oa_1_kgf = float(channel_row["spec_lfv_oa_1_kgf"].iloc[0] or 0)
                    spec_upper_g = float(channel_row["spec_upper_g"].iloc[0] or 0)
                    spec_lower_g = float(channel_row["spec_lower_g"].iloc[0] or 0)
                    spec_up_low_g = float(channel_row["spec_up_low_g"].iloc[0] or 0)
                    spec_static = float(channel_row["spec_static"].iloc[0] or 0)
                    # spec_yield = float(channel_row["spec_yield"].iloc[0] or 0)
                    total_rank_yield_spec_yield = float(channel_row["total_rank_yield_spec_yield"].iloc[0] or 0)
                    rfv_yield_spec_yield = float(channel_row["rfv_yield_spec_yield"].iloc[0] or 0)
                    rfh1_yield_spec_yield = float(channel_row["rfh1_yield_spec_yield"].iloc[0] or 0)
                    lfv_yield_spec_yield = float(channel_row["lfv_yield_spec_yield"].iloc[0] or 0)
                    conicity_yield_spec_yield = float(channel_row["conicity_yield_spec_yield"].iloc[0] or 0)
                    ufm_yield_spec_yield = float(channel_row["ufm_yield_spec_yield"].iloc[0] or 0)
                    rorank_yield_spec_yield = float(channel_row["rorank_yield_spec_yield"].iloc[0] or 0)
                    radial_runout_yield_spec_yield = float(channel_row["radial_runout_yield_spec_yield"].iloc[0] or 0)
                    radial_runout_first_harmonic_yield_spec_yield = float(channel_row["radial_runout_first_harmonic_yield_spec_yield"].iloc[0] or 0)
                    lateral_run_out_yield_spec_yield = float(channel_row["lateral_run_out_yield_spec_yield"].iloc[0] or 0)
                    lateral_run_out_top_over_yield_spec_yield = float(channel_row["lateral_run_out_top_over_yield_spec_yield"].iloc[0] or 0)
                    lateral_run_out_bottom_over_yield_spec_yield = float(channel_row["lateral_run_out_bottom_over_yield_spec_yield"].iloc[0] or 0)
                    lateral_run_out_harmonic_yield_spec_yield = float(channel_row["lateral_run_out_harmonic_yield_spec_yield"].iloc[0] or 0)
                    lateral_run_out_top_first_harmonic_yield_spec_yield = float(channel_row["lateral_run_out_top_first_harmonic_yield_spec_yield"].iloc[0] or 0)
                    lateral_run_out_bottom_first_harmonic_yield_spec_yield = float(channel_row["lateral_run_out_bottom_first_harmonic_yield_spec_yield"].iloc[0] or 0)
                    bulge_yield_spec_yield = float(channel_row["bulge_yield_spec_yield"].iloc[0] or 0)
                    bulge_top_over_yield_spec_yield = float(channel_row["bulge_top_over_yield_spec_yield"].iloc[0] or 0)
                    bulge_bottom_over_yield_spec_yield = float(channel_row["bulge_bottom_over_yield_spec_yield"].iloc[0] or 0)
                    dent_yield_spec_yield = float(channel_row["dent_yield_spec_yield"].iloc[0] or 0)
                    dent_top_over_yield_spec_yield = float(channel_row["dent_top_over_yield_spec_yield"].iloc[0] or 0)
                    dent_bottom_over_yield_spec_yield = float(channel_row["dent_bottom_over_yield_spec_yield"].iloc[0] or 0)
                    balancing_yield_spec_yield = float(channel_row["balancing_yield_spec_yield"].iloc[0] or 0)
                    upper_yield_spec_yield = float(channel_row["upper_yield_spec_yield"].iloc[0] or 0)
                    lower_yield_spec_yield = float(channel_row["lower_yield_spec_yield"].iloc[0] or 0)
                    upper_lower_yield_spec_yield = float(channel_row["upper_lower_yield_spec_yield"].iloc[0] or 0)
                    static_yield_spec_yield = float(channel_row["static_yield_spec_yield"].iloc[0] or 0)
                    warehouse_yield_spec_yield = float(channel_row["warehouse_yield_spec_yield"].iloc[0] or 0)
                    visual_yield_spec_yield = float(channel_row["visual_yield_spec_yield"].iloc[0] or 0)
                    test_load = float(channel_row["test_load"].iloc[0] or 0)
                    pressure = float(channel_row["pressure"].iloc[0] or 0)
                    # threshold_value = float(channel_row["threshold_value"].iloc[0] or 0.5)
                    # threshold_value_bal = float(channel_row["threshold_value_bal"].iloc[0] or 5)
                    threshold_value_con_kgf = float(channel_row["threshold_value_con_kgf"].iloc[0] or 0.5)
                    threshold_value_cwrfv_oa_kgf = float(channel_row["threshold_value_cwrfv_oa_kgf"].iloc[0] or 0.5)
                    threshold_value_ccwrfv_oa_kgf = float(channel_row["threshold_value_ccwrfv_oa_kgf"].iloc[0] or 0.5)
                    threshold_value_cwrfv_oa1h_kgf = float(channel_row["threshold_value_cwrfv_oa1h_kgf"].iloc[0] or 0.5)
                    threshold_value_ccwrfv_oa1h_kgf = float(channel_row["threshold_value_ccwrfv_oa1h_kgf"].iloc[0] or 0.5)
                    threshold_value_lfv_oa_kgf = float(channel_row["threshold_value_lfv_oa_kgf"].iloc[0] or 0.5)
                    threshold_value_lfv_oa_1_kgf = float(channel_row["threshold_value_lfv_oa_1_kgf"].iloc[0] or 0.5)
                    threshold_value_bal_upper_g = float(channel_row["threshold_value_bal_upper_g"].iloc[0] or 5)
                    threshold_value_bal_lower_g = float(channel_row["threshold_value_bal_lower_g"].iloc[0] or 5)
                    threshold_value_bal_up_low_g = float(channel_row["threshold_value_bal_up_low_g"].iloc[0] or 5)
                    threshold_value_bal_static_g = float(channel_row["threshold_value_bal_static_g"].iloc[0] or 5)
                    test_load_tolerance = float(channel_row["test_load_tolerance"].iloc[0] or 5)
                    pressure_tolerance = float(channel_row["pressure_tolerance"].iloc[0] or 2)
                    # threshold_value_yield = float(channel_row["test_load"].iloc[0] or 5)
                    threshold_value_yield_total_rank_yield = float(channel_row["threshold_value_yield_total_rank_yield"].iloc[0] or 5)
                    threshold_value_yield_rfv_yield = float(channel_row["threshold_value_yield_rfv_yield"].iloc[0] or 5)
                    threshold_value_yield_rfh1_yield = float(channel_row["threshold_value_yield_rfh1_yield"].iloc[0] or 5)
                    threshold_value_yield_lfv_yield = float(channel_row["threshold_value_yield_lfv_yield"].iloc[0] or 5)
                    threshold_value_yield_conicity_yield = float(channel_row["threshold_value_yield_conicity_yield"].iloc[0] or 5)
                    threshold_value_yield_ufm_yield = float(channel_row["threshold_value_yield_ufm_yield"].iloc[0] or 5)
                    threshold_value_yield_rorank_yield = float(channel_row["threshold_value_yield_rorank_yield"].iloc[0] or 5)
                    threshold_value_yield_radial_runout_yield = float(channel_row["threshold_value_yield_radial_runout_yield"].iloc[0] or 5)
                    threshold_value_yield_radial_runout_first_harmonic_yield = float(channel_row["threshold_value_yield_radial_runout_first_harmonic_yield"].iloc[0] or 5)
                    threshold_value_yield_lateral_run_out_yield = float(channel_row["threshold_value_yield_lateral_run_out_yield"].iloc[0] or 5)
                    threshold_value_yield_lateral_run_out_top_over_yield = float(channel_row["threshold_value_yield_lateral_run_out_top_over_yield"].iloc[0] or 5)
                    threshold_value_yield_lateral_run_out_bottom_over_yield = float(channel_row["threshold_value_yield_lateral_run_out_bottom_over_yield"].iloc[0] or 5)
                    threshold_value_yield_lateral_run_out_harmonic_yield = float(channel_row["threshold_value_yield_lateral_run_out_harmonic_yield"].iloc[0] or 5)
                    threshold_value_yield_lateral_run_out_top_first_harmonic_yield = float(channel_row["threshold_value_yield_lateral_run_out_top_first_harmonic_yield"].iloc[0] or 5)
                    threshold_value_yield_lateral_run_out_bottom_first_harmonic_yield = float(channel_row["threshold_value_yield_lateral_run_out_bottom_first_harmonic_yield"].iloc[0] or 5)
                    threshold_value_yield_bulge_yield = float(channel_row["threshold_value_yield_bulge_yield"].iloc[0] or 5)
                    threshold_value_yield_bulge_top_over_yield = float(channel_row["threshold_value_yield_bulge_top_over_yield"].iloc[0] or 5)
                    threshold_value_yield_bulge_bottom_over_yield = float(channel_row["threshold_value_yield_bulge_bottom_over_yield"].iloc[0] or 5)
                    threshold_value_yield_dent_yield = float(channel_row["threshold_value_yield_dent_yield"].iloc[0] or 5)
                    threshold_value_yield_dent_top_over_yield = float(channel_row["threshold_value_yield_dent_top_over_yield"].iloc[0] or 5)
                    threshold_value_yield_dent_bottom_over_yield = float(channel_row["threshold_value_yield_dent_bottom_over_yield"].iloc[0] or 5)
                    threshold_value_yield_balancing_yield = float(channel_row["threshold_value_yield_balancing_yield"].iloc[0] or 5)
                    threshold_value_yield_upper_yield = float(channel_row["threshold_value_yield_upper_yield"].iloc[0] or 5)
                    threshold_value_yield_lower_yield = float(channel_row["threshold_value_yield_lower_yield"].iloc[0] or 5)
                    threshold_value_yield_upper_lower_yield = float(channel_row["threshold_value_yield_upper_lower_yield"].iloc[0] or 5)
                    threshold_value_yield_static_yield = float(channel_row["threshold_value_yield_static_yield"].iloc[0] or 5)
                    threshold_value_yield_warehouse_yield = float(channel_row["threshold_value_yield_warehouse_yield"].iloc[0] or 5)
                    threshold_value_yield_visual_yield = float(channel_row["threshold_value_yield_visual_yield"].iloc[0] or 5)

                    total_barcodes_model = len(model_data)

                    model_data = model_data[(model_data["modelno_cimcorp"] == model_number)].copy()
                    # model_data = model_data[(model_data["tbm_gtcode"] == gt_code)].copy()
                    if model_data.empty:
                        output_messages.append(f"⚠️ No matching modelno_cimcorp for model {model_number} in TBM {machine_no}")
                        continue
                    
                    model_data_full = model_data.copy()
                    # Ensure date_of_tbm is proper datetime for sorting
                    model_data_full["date_of_dt"] = pd.to_datetime(model_data_full["date_and_time"], errors='coerce')

                    # === 1. Virgin Yield: Keep FIRST occurrence of each barcode ===
                    virgin_data = model_data_full.sort_values("date_of_dt").drop_duplicates(subset="barcode", keep="first").copy()
                    # >>> ADD THESE PRINT STATEMENTS <<<
                    virgin_dupes = model_data_full[model_data_full.duplicated(subset="barcode", keep=False)]
                    if not virgin_dupes.empty:
                        first_occ = virgin_dupes.sort_values("date_of_dt").drop_duplicates("barcode", keep="first")
                        # print("=== VIRGIN YIELD - Duplicate barcodes (first occurrence shown) ===")
                        # for _, r in first_occ.iterrows():
                        #     print(f"Barcode: {r['barcode']} | First seen: {r['date_and_time']} | Machine: {r['machine']} | Model: {model_number}")
                    # <<< END OF ADDED PRINT >>>

                    # === 2. Final Yield: Keep LAST occurrence of each barcode ===
                    final_data = model_data_full.sort_values("date_of_dt").drop_duplicates(subset="barcode", keep="last").copy()
                    # >>> ADD THESE PRINT STATEMENTS <<<
                    final_dupes = model_data_full[model_data_full.duplicated(subset="barcode", keep=False)]
                    if not final_dupes.empty:
                        last_occ = final_dupes.sort_values("date_of_dt").drop_duplicates("barcode", keep="last")
                        # print("=== FINAL YIELD - Duplicate barcodes (last occurrence shown) ===")
                        # for _, r in last_occ.iterrows():
                        #     print(f"Barcode: {r['barcode']} | Last seen: {r['date_and_time']} | Machine: {r['machine']} | Model: {model_number}")
                    # <<< END OF ADDED PRINT >>>

                    # Sort both for lot processing
                    virgin_data.sort_values("date_of_dt", inplace=True)
                    final_data.sort_values("date_of_dt", inplace=True)

                    # Use original full data for main lot processing (existing logic)
                    model_data = model_data_full.copy()
                    model_data.sort_values("date_of_tbm", ascending=True, inplace=True)

                    alert_map = {}

                    # Get last lot number
                    machine_model_key = (model_number, machine_no)
                    last_lot_info = last_lot_dict.get(machine_model_key, {"max_lot_number": 0, "last_lot_id": None})
                    last_lot_number = last_lot_info["max_lot_number"]

                    # Generate lot_id and tbm_machine_model_lot_id
                    last_lot_number += 1
                    unique_lot_id_current = f"TBM_{machine_no}_{model_number}_{last_lot_number}"
                    current_barcode_counter = total_barcodes_model
                    current_tbm_machine_model_lot_id = f"{machine_no}_{model_number}_TBM_{machine_no}_{model_number}_{last_lot_number}_{current_barcode_counter}"
                    previous_lot_id = f"TBM_{machine_no}_{model_number}_{last_lot_number - 1}" if last_lot_number > 1 else None

                    # Fetch previous tbm_machine_model_lot_id
                    tbm_previous_machine_model_lot_id = tbm_previous_machine_model_lot_ids.get(machine_model_key)
                    if tbm_previous_machine_model_lot_id is None and last_lot_number > 1:
                        prev_lot_number = last_lot_number - 1
                        try:
                            prev_lot = averages_collect.find_one(
                                {
                                    "modelno": model_number,
                                    "machine": machine_no,
                                    "lot_id": f"TBM_{machine_no}_{model_number}_{prev_lot_number}"
                                },
                                sort=[("tbm_machine_model_lot_id", -1)]
                            )
                            tbm_previous_machine_model_lot_id = prev_lot["tbm_machine_model_lot_id"] if prev_lot else None
                        except Exception as e:
                            logging.error(f"Error fetching previous tbm_machine_model_lot_id for lot {prev_lot_number}: {e}")
                            tbm_previous_machine_model_lot_id = None

                    # Update last_lot_dict
                    last_lot_dict[machine_model_key] = {
                        "max_lot_number": last_lot_number,
                        "last_lot_id": unique_lot_id_current
                    }

                    # Update merge_df
                    merge_df.loc[model_data.index, "lot_id"] = unique_lot_id_current
                    merge_df.loc[model_data.index, "previous_lot_id"] = previous_lot_id
                    merge_df.loc[model_data.index, "tbm_machine_model_lot_id"] = current_tbm_machine_model_lot_id
                    merge_df.loc[model_data.index, "tbm_previous_machine_model_lot_id"] = tbm_previous_machine_model_lot_id

                    # Fetch previous lot averages
                    prev_lot_avg = None
                    if tbm_previous_machine_model_lot_id:
                        previous_lot = averages_collect.find_one(
                            {"tbm_machine_model_lot_id": tbm_previous_machine_model_lot_id},
                            {"current_lot_averages": 1}
                        )
                        prev_lot_avg = previous_lot["current_lot_averages"] if previous_lot else None

                    tbm_previous_machine_model_lot_ids[machine_model_key] = current_tbm_machine_model_lot_id

                    # Calculate lot averages
                    # lot_averages = {}
                    # for param in conicity_params:
                    #     if param in model_data.columns:
                    #         valid_vals = pd.to_numeric(model_data[param], errors='coerce').dropna()
                    #         lot_averages[param] = float(valid_vals.mean()) if not valid_vals.empty else 0.0
                    #     else:
                    #         lot_averages[param] = 0.0

                    lot_averages = {}
                    for param in conicity_params:
                        if param in model_data.columns:
                            valid_vals = pd.to_numeric(model_data[param], errors='coerce').dropna()
                            if not valid_vals.empty:
                                lot_averages[param] = float(valid_vals.mean())
                            else:
                                lot_averages[param] = None
                        else:
                            lot_averages[param] = None

                    lot_averages.update({
                        "total_rank_yield": total_rank_yield(model_data),
                        "rfv_yield": rfv_yield(model_data),
                        "rfh1_yield": rfh1_yield(model_data),
                        "lfv_yield": lfv_yield(model_data),
                        "conicity_yield": conicity_yield(model_data),
                        "ufm_yield": ufm_yield(model_data),
                        "rorank_yield": rorank_yield(model_data),
                        "radial_runout_yield": radial_runout_yield(model_data),
                        "radial_runout_first_harmonic_yield": radial_runout_first_harmonic_yield(model_data),
                        "lateral_run_out_yield": lateral_run_out_yield(model_data),
                        "lateral_run_out_top_over_yield": lateral_run_out_top_over(model_data),
                        "lateral_run_out_bottom_over_yield": lateral_run_out_bottom_over(model_data),
                        "lateral_run_out_harmonic_yield": lateral_run_out_harmonic_yield(model_data),
                        "lateral_run_out_top_first_harmonic_yield": lateral_run_out_top_first_harmonic(model_data),
                        "lateral_run_out_bottom_first_harmonic_yield": lateral_run_out_bottom_first_harmonic(model_data),
                        "bulge_yield": bulge_yield(model_data),
                        "bulge_top_over_yield": bulge_top_over(model_data),
                        "bulge_bottom_over_yield": bulge_bottom_over(model_data),
                        "dent_yield": dent_yield(model_data),
                        "dent_top_over_yield": dent_top_over(model_data),
                        "dent_bottom_over_yield": dent_bottom_over(model_data),
                        "balancing_yield": balancing_yield(model_data),
                        "upper_yield": upper_yield(model_data),
                        "lower_yield": lower_yield(model_data),
                        "upper_lower_yield": upper_lower_yield(model_data),
                        "static_yield": static_yield(model_data),
                        "warehouse_yield": warehouse_yield(model_data),
                        "visual_yield": visual_yield(model_data)
                    })

                    # Calculate ubno grouped averages
                    grouped_averages = {}
                    param_spec_map = {
                        "con_kgf": spec_con_kgf,
                        "cwrfv_oa_kgf": spec_cwrfv_oa_kgf,
                        "ccwrfv_oa_kgf": spec_ccwrfv_oa_kgf,
                        "cwrfv_oa1h_kgf": spec_cwrfv_oa1h_kgf,
                        "ccwrfv_oa1h_kgf": spec_ccwrfv_oa1h_kgf,
                        "lfv_oa_kgf": spec_lfv_oa_kgf,
                        "lfv_oa_1_kgf": spec_lfv_oa_1_kgf,
                        "upper_g": spec_upper_g,
                        "lower_g": spec_lower_g,
                        "up_low_g": spec_up_low_g,
                        "static_g": spec_static,
                        "load1": test_load,
                        "airpress1": pressure,
                        "total_rank_yield": total_rank_yield_spec_yield,
                        "rfv_yield": rfv_yield_spec_yield,
                        "rfh1_yield": rfh1_yield_spec_yield,
                        "lfv_yield": lfv_yield_spec_yield,
                        "conicity_yield": conicity_yield_spec_yield,
                        "ufm_yield": ufm_yield_spec_yield,
                        "rorank_yield": rorank_yield_spec_yield,
                        "radial_runout_yield": radial_runout_yield_spec_yield,
                        "radial_runout_first_harmonic_yield": radial_runout_first_harmonic_yield_spec_yield,
                        "lateral_run_out_yield": lateral_run_out_yield_spec_yield,
                        "lateral_run_out_top_over_yield": lateral_run_out_top_over_yield_spec_yield,
                        "lateral_run_out_bottom_over_yield": lateral_run_out_bottom_over_yield_spec_yield,
                        "lateral_run_out_harmonic_yield": lateral_run_out_harmonic_yield_spec_yield,
                        "lateral_run_out_top_first_harmonic_yield": lateral_run_out_top_first_harmonic_yield_spec_yield,
                        "lateral_run_out_bottom_first_harmonic_yield": lateral_run_out_bottom_first_harmonic_yield_spec_yield,
                        "bulge_yield": bulge_yield_spec_yield,
                        "bulge_top_over_yield": bulge_top_over_yield_spec_yield,
                        "bulge_bottom_over_yield": bulge_bottom_over_yield_spec_yield,
                        "dent_yield": dent_yield_spec_yield,
                        "dent_top_over_yield": dent_top_over_yield_spec_yield,
                        "dent_bottom_over_yield": dent_bottom_over_yield_spec_yield,
                        "balancing_yield": balancing_yield_spec_yield,
                        "upper_yield": upper_yield_spec_yield,
                        "lower_yield": lower_yield_spec_yield,
                        "upper_lower_yield": upper_lower_yield_spec_yield,
                        "static_yield": static_yield_spec_yield,
                        "warehouse_yield": warehouse_yield_spec_yield,
                        "visual_yield": visual_yield_spec_yield
                    }

                    try:
                        model_data["ubno"] = model_data["ubno"].astype(str).str.strip()
                        model_data.sort_values("date_and_time", ascending=True, inplace=True)
                        ubno_grouped = model_data.groupby("ubno")
                        ubno_records = []
                        for ubno, group in ubno_grouped:
                            if group.empty:
                                output_messages.append(f"⚠️ Empty group for ubno {ubno} in TBM {machine_no}, model {model_number}")
                                continue
                            group.sort_values("date_and_time", ascending=True)
                            record = {
                                "ubno": ubno,
                                "barcode_count": len(group["barcode"]),
                                "tub_date_start_time": group["date_and_time"].iloc[0] if not group["date_and_time"].empty else None,
                                "tub_date_end_time": group["date_and_time"].iloc[-1] if not group["date_and_time"].empty else None,
                                "barcodes": group["barcode"].tolist() if "barcode" in group.columns else []
                            }
                            for param in conicity_params:
                                if param in group.columns:
                                    valid_vals = pd.to_numeric(group[param], errors='coerce').dropna()
                                    record[f"avg_{param}"] = float(valid_vals.mean()) if not valid_vals.empty else None
                                else:
                                    record[f"avg_{param}"] = None
                            for param in yield_params:
                                yield_func = globals()[param]
                                record[param] = yield_func(group)
                            parameters = []
                            for param in conicity_params:
                                try:
                                    if param in group.columns:
                                        valid_vals = pd.to_numeric(group[param], errors='coerce').dropna()
                                        avg_val = float(valid_vals.mean()) if not valid_vals.empty else None
                                    else:
                                        avg_val = None
                                    record[f"avg_{param}"] = avg_val
                                    spec = param_spec_map.get(param)
                                    exceeded = False
                                    if param == "con_kgf" and spec_con_kgf is not None:
                                        exceeded = not (-spec_con_kgf <= avg_val <= spec_con_kgf) if avg_val is not None else False
                                    if param == "load1" and test_load is not None and test_load_tolerance is not None:
                                        exceeded = not (test_load - test_load_tolerance <= avg_val <= test_load + test_load_tolerance) if avg_val is not None else False
                                    if param == "airpress1" and pressure is not None and pressure_tolerance is not None:
                                        exceeded = not (pressure - pressure_tolerance <= avg_val <= pressure + pressure_tolerance) if avg_val is not None else False
                                    if spec is not None and param not in ["con_kgf", "load1", "airpress1"]:
                                        exceeded = avg_val > spec if avg_val is not None else False
                                    parameters.append({
                                        "parameter": param,
                                        "avg_value": avg_val,
                                        "spec_value": spec,
                                        "spec_threshold_exceeded": bool(exceeded)
                                    })
                                except Exception as e:
                                    logging.info(f"[ERROR] Processing numeric param {param}: {e}")
                                    record[f"avg_{param}"] = None
                                    parameters.append({
                                        "parameter": param,
                                        "avg_value": None,
                                        "spec_value": param_spec_map.get(param),
                                        "spec_threshold_exceeded": False
                                    })
                            for param in yield_params:
                                try:
                                    yield_func = globals()[param]
                                    avg_val = yield_func(group)
                                    record[param] = avg_val
                                    spec = param_spec_map.get(param)
                                    exceeded = avg_val < spec if spec is not None and avg_val is not None else False
                                    parameters.append({
                                        "parameter": param,
                                        "avg_value": avg_val,
                                        "spec_value": spec,
                                        "spec_threshold_exceeded": bool(exceeded)
                                    })
                                except Exception as e:
                                    record[param] = None
                                    parameters.append({
                                        "parameter": param,
                                        "avg_value": None,
                                        "spec_value": param_spec_map.get(param),
                                        "spec_threshold_exceeded": False
                                    })
                            record["parameter_list"] = parameters
                            ubno_records.append(record)
                        grouped_averages["ubno"] = ubno_records
                    except Exception as e:
                        grouped_averages["ubno"] = f"Error: {e}"
                        output_messages.append(f"⚠️ Error calculating ubno averages for TBM {machine_no}, model {model_number}: {e}")

                    # Timing and shift data
                    model_data1=model_data.copy()
                    model_data.sort_values("date_of_tbm", ascending=True, inplace=True)
                    model_data1.sort_values("date_and_time", ascending=True, inplace=True)
                    machine_lot_start_build_time = model_data["date_of_tbm"].iloc[0] if not model_data["date_of_tbm"].empty else None
                    machine_lot_end_build_time = model_data["date_of_tbm"].iloc[-1] if not model_data["date_of_tbm"].empty else None
                    tub_date_start_time = model_data1["date_and_time"].iloc[0] if not model_data1["date_and_time"].empty else None
                    tub_date_end_time = model_data1["date_and_time"].iloc[-1] if not model_data1["date_and_time"].empty else None
                    # date_of_ufm = model_data["date_of_ufm"].iloc[0] if "date_of_ufm" in model_data and not model_data["date_of_ufm"].empty else None
                    # date_of_tbm = model_data["date_of_tbm"].iloc[0] if "date_of_tbm" in model_data and not model_data["date_of_tbm"].empty else None
                    lot_start_time = some_function()
                    lot_end_time = some_function()

                    # Store lot data
                    lot_doc = {
                        "modelno": model_number,
                        "machine": machine_no,
                        "lot_id": unique_lot_id_current,
                        "previous_lot_id": previous_lot_id,
                        "tbm_machine_model_lot_id": current_tbm_machine_model_lot_id,
                        "tbm_previous_machine_model_lot_id": tbm_previous_machine_model_lot_id,
                        "processed_barcodes": int(total_barcodes_model),
                        "total_barcodes": int(total_tbm_barcodes),
                        "lot_averages": lot_averages,
                        "timestamp_lot_generated": some_function()
                    }
                    lot_query = {
                        "modelno": model_number,
                        "lot_id": unique_lot_id_current,
                        "tbm_machine_model_lot_id": current_tbm_machine_model_lot_id
                    }
                    if not lot_tbm_collect.find_one(lot_query):
                        bulk_operations.append(pymongo.InsertOne(lot_doc))

                    # Alert logic
                    parameter_alerts = []
                    overall_shift_occurred = False

                    if prev_lot_avg:
                        for param in conicity_params + yield_params:
                            shift = False
                            shift_occurred_spec = False
                            current_val = lot_averages.get(param)
                            prev_val = prev_lot_avg.get(param)
                            if current_val is not None and prev_val is not None:
                                if param == "con_kgf" and spec_con_kgf is not None:
                                    if abs(current_val - prev_val) > threshold_value_con_kgf:
                                        shift = True
                                    if not (-spec_con_kgf <= current_val <= spec_con_kgf):
                                        shift_occurred_spec = True
                                if param == "cwrfv_oa_kgf" and spec_cwrfv_oa_kgf is not None:
                                    if (current_val - prev_val) > threshold_value_cwrfv_oa_kgf:
                                        shift = True
                                    if current_val > spec_ccwrfv_oa_kgf:
                                        shift_occurred_spec = True
                                if param == "ccwrfv_oa_kgf" and spec_ccwrfv_oa_kgf is not None:
                                    if (current_val - prev_val) > threshold_value_ccwrfv_oa_kgf:
                                        shift = True
                                    if current_val > spec_ccwrfv_oa_kgf:
                                        shift_occurred_spec = True
                                if param == "cwrfv_oa1h_kgf" and spec_cwrfv_oa1h_kgf is not None:
                                    if (current_val - prev_val) > threshold_value_cwrfv_oa1h_kgf:
                                        shift = True
                                    if current_val > spec_cwrfv_oa1h_kgf:
                                        shift_occurred_spec = True
                                if param == "ccwrfv_oa1h_kgf" and spec_ccwrfv_oa1h_kgf is not None:
                                    if (current_val - prev_val) > threshold_value_ccwrfv_oa1h_kgf:
                                        shift = True
                                    if current_val > spec_ccwrfv_oa1h_kgf:
                                        shift_occurred_spec = True
                                if param == "lfv_oa_kgf" and spec_lfv_oa_kgf is not None:
                                    if (current_val - prev_val) > threshold_value_lfv_oa_kgf:
                                        shift = True
                                    if current_val > spec_lfv_oa_kgf:
                                        shift_occurred_spec = True
                                if param == "lfv_oa_1_kgf" and spec_lfv_oa_1_kgf is not None:
                                    if (current_val - prev_val) > threshold_value_lfv_oa_1_kgf:
                                        shift = True
                                    if current_val > spec_lfv_oa_1_kgf:
                                        shift_occurred_spec = True
                                if param == "upper_g" and spec_upper_g is not None:
                                    if (current_val - prev_val) > threshold_value_bal_upper_g:
                                        shift = True
                                    if current_val > spec_upper_g:
                                        shift_occurred_spec = True
                                if param == "lower_g" and spec_lower_g is not None:
                                    if (current_val - prev_val) > threshold_value_bal_lower_g:
                                        shift = True
                                    if current_val > spec_lower_g:
                                        shift_occurred_spec = True
                                if param == "up_low_g" and spec_up_low_g is not None:
                                    if (current_val - prev_val) > threshold_value_bal_up_low_g:
                                        shift = True
                                    if current_val > spec_up_low_g:
                                        shift_occurred_spec = True
                                if param == "static_g" and spec_static is not None:
                                    if (current_val - prev_val) > threshold_value_bal_static_g:
                                        shift = True
                                    if current_val > spec_static:
                                        shift_occurred_spec = True
                                
                                ############### Add 16-07 start ######################

                                # if param in yield_params:
                                #     spec_yield = param_spec_map.get(param)
                                #     if spec_yield is not None:
                                #         if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield)):
                                #             shift = True
                                #         if abs(current_val < spec_yield):
                                #             shift_occurred_spec = True

                                ############### Add 16-07 end ######################

                                if param == "total_rank_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_total_rank_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "rfv_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_rfv_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "rfh1_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_rfh1_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "lfv_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_lfv_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "conicity_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_conicity_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "ufm_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_ufm_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "rorank_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_rorank_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "radial_runout_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_radial_runout_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "radial_runout_first_harmonic_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_radial_runout_first_harmonic_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "lateral_run_out_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_lateral_run_out_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "lateral_run_out_top_over_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_lateral_run_out_top_over_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "lateral_run_out_bottom_over_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_lateral_run_out_bottom_over_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "lateral_run_out_harmonic_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_lateral_run_out_harmonic_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "lateral_run_out_top_first_harmonic_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_lateral_run_out_top_first_harmonic_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "lateral_run_out_bottom_first_harmonic_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_lateral_run_out_bottom_first_harmonic_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "bulge_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_bulge_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "bulge_top_over_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_bulge_top_over_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "bulge_bottom_over_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_bulge_bottom_over_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "dent_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_dent_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "dent_top_over_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_dent_top_over_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "dent_bottom_over_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_dent_bottom_over_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "balancing_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_balancing_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "upper_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_upper_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "lower_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_lower_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "upper_lower_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_upper_lower_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "static_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_static_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "warehouse_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_warehouse_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "visual_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_visual_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True

                                if param == "load1" and test_load is not None and test_load_tolerance is not None:
                                    if abs(current_val - prev_val) > test_load_tolerance:
                                        shift = True
                                    if not (test_load - test_load_tolerance <= current_val <= test_load + test_load_tolerance):
                                        shift_occurred_spec = True
                                if param == "airpress1" and pressure is not None and pressure_tolerance is not None:
                                    if abs(current_val - prev_val) > pressure_tolerance:
                                        shift = True
                                    if not (pressure - pressure_tolerance <= current_val <= pressure + pressure_tolerance):
                                        shift_occurred_spec = True
                            shift_occurred = shift or shift_occurred_spec
                            spec_value = param_spec_map.get(param)
                            spec_diff = (current_val - spec_value if spec_value is not None and current_val is not None and isinstance(spec_value, (int, float)) and isinstance(current_val, (int, float)) else None)
                            alert_dict = {
                                "parameter": param,
                                "current_value": current_val,
                                "previous_value": prev_val,
                                "difference": current_val - prev_val if current_val is not None and prev_val is not None else None,
                                "shift_occurred_thershold": shift,
                                "spec_value": spec_value,
                                "spec_difference": spec_diff,
                                "shift_occurred_spec": shift_occurred_spec,
                                "shift_occurred": shift_occurred
                            }
                            if param == "load1":
                                alert_dict["test_load_tolerance"] = test_load_tolerance
                            if param == "airpress1":
                                alert_dict["pressure_tolerance"] = pressure_tolerance
                            parameter_alerts.append(alert_dict)
                            if shift_occurred:
                                overall_shift_occurred = True
                    else:
                        for param in conicity_params + yield_params:
                            shift = False
                            shift_occurred_thershold = False
                            shift_occurred_spec = False
                            current_val = lot_averages.get(param)
                            if current_val is not None:
                                if param == "con_kgf" and spec_con_kgf is not None:
                                    if not (-spec_con_kgf <= current_val <= spec_con_kgf):
                                        shift_occurred_spec = True
                                if param == "cwrfv_oa_kgf" and spec_cwrfv_oa_kgf is not None:
                                    if current_val > spec_cwrfv_oa_kgf:
                                        shift_occurred_spec = True
                                if param == "ccwrfv_oa_kgf" and spec_ccwrfv_oa_kgf is not None:
                                    if current_val > spec_ccwrfv_oa_kgf:
                                        shift_occurred_spec = True
                                if param == "cwrfv_oa1h_kgf" and spec_cwrfv_oa1h_kgf is not None:
                                    if current_val > spec_cwrfv_oa1h_kgf:
                                        shift_occurred_spec = True
                                if param == "ccwrfv_oa1h_kgf" and spec_ccwrfv_oa1h_kgf is not None:
                                    if current_val > spec_ccwrfv_oa1h_kgf:
                                        shift_occurred_spec = True
                                if param == "lfv_oa_kgf" and spec_lfv_oa_kgf is not None:
                                    if current_val > spec_lfv_oa_kgf:
                                        shift_occurred_spec = True
                                if param == "lfv_oa_1_kgf" and spec_lfv_oa_1_kgf is not None:
                                    if current_val > spec_lfv_oa_1_kgf:
                                        shift_occurred_spec = True
                                if param == "upper_g" and spec_upper_g is not None:
                                    if current_val > spec_upper_g:
                                        shift_occurred_spec = True
                                if param == "lower_g" and spec_lower_g is not None:
                                    if current_val > spec_lower_g:
                                        shift_occurred_spec = True
                                if param == "up_low_g" and spec_up_low_g is not None:
                                    if current_val > spec_up_low_g:
                                        shift_occurred_spec = True
                                if param == "static_g" and spec_static is not None:
                                    if current_val > spec_static:
                                        shift_occurred_spec = True
                            
                            ############### Add 16-07 start ######################

                                if param in yield_params:
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if current_val < spec_yield:
                                            shift_occurred_spec = True
                                if param == "load1" and test_load is not None and test_load_tolerance is not None:
                                    if not (test_load - test_load_tolerance <= current_val <= test_load + test_load_tolerance):
                                        shift_occurred_spec = True
                                if param == "airpress1" and pressure is not None and pressure_tolerance is not None:
                                    if not (pressure - pressure_tolerance <= current_val <= pressure + pressure_tolerance):
                                        shift_occurred_spec = True
                            shift_occurred = shift_occurred_thershold or shift_occurred_spec
                            spec_value = param_spec_map.get(param)
                            spec_diff = (current_val - spec_value if spec_value is not None and current_val is not None and isinstance(spec_value, (int, float)) and isinstance(current_val, (int, float)) else None)
                            alert_dict = {
                                "parameter": param,
                                "current_value": current_val,
                                "previous_value": None,
                                "difference": None,
                                "shift_occurred_thershold": shift_occurred_thershold,
                                "spec_value": spec_value,
                                "spec_difference": spec_diff,
                                "shift_occurred_spec": shift_occurred_spec,
                                "shift_occurred": shift_occurred
                            }
                            if param == "load1":
                                alert_dict["test_load_tolerance"] = test_load_tolerance
                            if param == "airpress1":
                                alert_dict["pressure_tolerance"] = pressure_tolerance
                            parameter_alerts.append(alert_dict)
                            if shift_occurred:
                                overall_shift_occurred = True

                    overall_shift_occurred = bool(overall_shift_occurred)

                    # NEW: Filter parameters for derived alerts
                    deviated_parameter_alerts = [
                        alert for alert in parameter_alerts
                        if alert.get("shift_occurred_thershold", False) or alert.get("shift_occurred_spec", False)
                    ]

                    # Add rank counts to deviated parameter alerts
                    for alert in deviated_parameter_alerts:
                        param = alert.get("parameter", "")
                        if param.endswith("_yield"):
                            if param in rank_param_mappings:
                                rank_counts = {}
                                for col in rank_param_mappings[param]:
                                    if col in model_data.columns:
                                        value_counts = model_data[col].value_counts().to_dict()
                                        for rank_value, count in value_counts.items():
                                            rank_value = int(rank_value) if isinstance(rank_value, (int, float)) else rank_value
                                            rank_counts[f"{col}_{rank_value}"] = int(count)
                                alert["rank_counts"] = rank_counts

                    if deviated_parameter_alerts:
                        deviated_doc = {
                            "alert_id_tbm_ufm": last_alert_id + 1,
                            "modelno": model_number,
                            "lot_id": unique_lot_id_current,
                            "machine": machine_no,
                            "comments": [],
                            "alert_assign_user": [],  # Add 05-07
                            "tbm_machine_model_lot_id": current_tbm_machine_model_lot_id,
                            "tbm_previous_machine_model_lot_id": tbm_previous_machine_model_lot_id,
                            "parameter_alerts": deviated_parameter_alerts,
                            "timestamp_alert_generated": some_function()
                        }
                        deviated_triggering_bulk_operations.append(pymongo.InsertOne(deviated_doc))
                        logging.info(f"Prepared {len(deviated_parameter_alerts)} derived alerts for machine {machine_no}, lot {unique_lot_id_current}")

                    # Do not filter; include all parameters (both yield and non-yield)
                    parameter_alerts = [
                    alert for alert in parameter_alerts
                    if alert.get("parameter", "").endswith("_yield") or not alert.get("parameter", "").endswith("_yield")
                    ]

                    # Add rank counts only for alerts:
                    # - where a shift occurred (threshold or spec)
                    # - and parameter name ends with '_yield'
                    for alert in parameter_alerts:
                        param = alert.get("parameter", "")
                        if (
                            (alert.get("shift_occurred_thershold", False) or alert.get("shift_occurred_spec", False))
                            and param.endswith("_yield")
                        ):
                            if param in rank_param_mappings:
                                rank_counts = {}
                                for col in rank_param_mappings[param]:
                                    if col in model_data.columns:
                                        value_counts = model_data[col].value_counts().to_dict()
                                        for rank_value, count in value_counts.items():
                                            rank_value = int(rank_value) if isinstance(rank_value, (int, float)) else rank_value
                                            rank_counts[f"{col}_{rank_value}"] = int(count)
                                alert["rank_counts"] = rank_counts

                    # # Shift grouping
                    # model_data["tbm_shift"] = model_data["date_of_tbm"].apply(get_shift)
                    # shift_groups = model_data.groupby('tbm_shift')
                    # shifts_data_list = []
                    # for shift_name, shift_data in shift_groups:
                    #     shift_start_time = shift_data["date_of_tbm"].iloc[0] if not shift_data["date_of_tbm"].empty else None
                    #     shift_end_time = shift_data["date_of_tbm"].iloc[-1] if not shift_data["date_of_tbm"].empty else None
                    #     shifts_data_list.append({
                    #         "shifts": shift_name,
                    #         "barcode_count": len(shift_data["barcode"]),
                    #         "machine_shift_tbm_start_time": shift_start_time,
                    #         "machine_shift_tbm_end_time": shift_end_time
                    #     })

                    # ------------------------------------------------------------------
                    #  NEW (correct – one entry per **continuous** shift block)
                    # ------------------------------------------------------------------
                    model_data["tbm_shift"] = model_data["date_of_tbm"].apply(get_shift)

                    # Sort by date_of_tbm to guarantee chronological order
                    model_data = model_data.sort_values("date_of_tbm").reset_index(drop=True)

                    shifts_data_list = []
                    current_shift = None
                    current_start = None
                    current_barcodes = []

                    for idx, row in model_data.iterrows():
                        this_shift = row["tbm_shift"]
                        this_dt    = row["date_of_tbm"]
                        this_bc    = row.get("barcode")

                        if this_shift != current_shift or current_shift is None:
                            # finish previous block (if any)
                            if current_shift is not None:
                                shifts_data_list.append({
                                    "shift": current_shift,
                                    "barcode_count": len(current_barcodes),
                                    "machine_shift_tbm_start_time": current_start,
                                    "machine_shift_tbm_end_time":   model_data.at[idx-1, "date_of_tbm"],
                                    "barcodes": current_barcodes[:]
                                })
                            # start new block
                            current_shift   = this_shift
                            current_start   = this_dt
                            current_barcodes = [this_bc] if this_bc is not None else []
                        else:
                            current_barcodes.append(this_bc if this_bc is not None else None)

                    # do NOT forget the last block
                    if current_shift is not None:
                        shifts_data_list.append({
                            "shift": current_shift,
                            "barcode_count": len(current_barcodes),
                            "machine_shift_tbm_start_time": current_start,
                            "machine_shift_tbm_end_time":   model_data["date_of_tbm"].iloc[-1],
                            "barcodes": current_barcodes
                        })

                    # Store averages
                    averages_doc = {
                        "modelno": model_number,
                        "lot_id": unique_lot_id_current,
                        "previous_lot_id": previous_lot_id,
                        "machine": machine_no,
                        "tbm_machine_model_lot_id": current_tbm_machine_model_lot_id,
                        "tbm_previous_machine_model_lot_id": tbm_previous_machine_model_lot_id,
                        "current_lot_averages": lot_averages,
                        "grouped_averages": grouped_averages,
                        "previous_lot_averages": prev_lot_avg,
                        "parameter_alerts": parameter_alerts,
                        "tbm_shift_occurred": overall_shift_occurred,
                        "processed_machine_barcodes": int(total_barcodes_model),
                        "batch_indices": list(model_data.index),
                        "timestamp_alert_generated": some_function()
                    }
                    averages_query = {
                        "modelno": model_number,
                        "lot_id": unique_lot_id_current,
                        "tbm_machine_model_lot_id": current_tbm_machine_model_lot_id
                    }
                    if not averages_collect.find_one(averages_query):
                        averages_bulk_operations.append(pymongo.InsertOne(averages_doc))

                    current_key = (model_number, machine_no, current_tbm_machine_model_lot_id)
                    machine_lot_avg_map[current_key] = lot_averages

                    # Machine alert
                    any_shift_flag = any(alert["shift_occurred"] for alert in parameter_alerts)
                    machine_alert = {
                        "machine": machine_no,
                        "tbm_shift_occurred": any_shift_flag,
                        "shifts_data": shifts_data_list,
                        "ufm_yield": ufm_yield(model_data),
                        "rorank_yield": rorank_yield(model_data),
                        "balancing_yield": balancing_yield(model_data),
                        "total_rank_yield": total_rank_yield(model_data),
                        "visual_yield": visual_yield(model_data),
                        "sizecode": sizecode,
                        "gt_code": gt_codes[0] if gt_codes else "",      # Add 16-07
                        "timestamp_lot_started": lot_start_time,
                        "timestamp_lot_ended": lot_end_time,
                        "processed_machine_barcodes": int(total_barcodes_model),
                        "total_machine_barcodes": int(total_tbm_barcodes),
                        "machine_lot_start_build_time": machine_lot_start_build_time,
                        "machine_lot_end_build_time": machine_lot_end_build_time,
                        "tub_date_start_time": tub_date_start_time,
                        "tub_date_end_time": tub_date_end_time,
                        "parameter_alerts": parameter_alerts,
                        "lot_averages": lot_averages,
                        "grouped_averages": grouped_averages,
                        "previous_lot_averages": prev_lot_avg or {},
                        "first_barcode_machine": model_data.iloc[0]["barcode"] if "barcode" in model_data.columns else None,
                        "last_barcode_machine": model_data.iloc[-1]["barcode"] if "barcode" in model_data.columns else None,
                        "tbm_machine_model_lot_id": current_tbm_machine_model_lot_id,
                        "tbm_previous_machine_model_lot_id": tbm_previous_machine_model_lot_id,
                        "rank_columns": get_rank_columns(model_data),
                        "barcodes": model_data["barcode"].tolist()
                    }

                    alert_key = (model_number, unique_lot_id_current)
                    alert_map = {}
                    if alert_key not in alert_map:
                        last_alert_id += 1
                        alert_map[alert_key] = {
                            "alert_id_tbm_ufm": last_alert_id,
                            "modelno": model_number,
                            "lot_id": unique_lot_id_current,
                            "machine_alerts": [],
                            "first_barcode_model": None,
                            "last_barcode_model": None
                        }
                    alert_map[alert_key]["machine_alerts"].append(machine_alert)
                    if machine_alert["tbm_shift_occurred"]:
                        alert_map[alert_key]["shift_occurred"] = True
                    if alert_map[alert_key]["first_barcode_model"] is None:
                        alert_map[alert_key]["first_barcode_model"] = machine_alert["first_barcode_machine"]
                    alert_map[alert_key]["last_barcode_model"] = machine_alert["last_barcode_machine"]
                    if overall_shift_occurred:
                        merge_df.loc[model_data.index, "alert_id_tbm_ufm"] = last_alert_id

                    # Store alerts
                    for alert_key, alert_obj in alert_map.items():
                        final_alert = {
                            "alert_id_tbm_ufm": alert_obj["alert_id_tbm_ufm"],
                            "modelno": alert_obj["modelno"],
                            "lot_id": alert_obj["lot_id"],
                            "alert_flag": False,
                            "dashboard_flag": False,
                            "machine_flag": False,
                            "alert_status_check": "o",
                            "alert_status_timestamp": None,
                            "alert_status_comment": [],
                            "alert_assign_user": [],  # Add 05-07
                            "machine_alerts": alert_obj["machine_alerts"],
                            "first_barcode_model": alert_obj["first_barcode_model"],
                            "last_barcode_model": alert_obj["last_barcode_model"],
                            "timestamp_alert_generated": some_function()
                        }
                        alert_bulk_operations.append(pymongo.InsertOne(final_alert))
                        
                        triggering_bulk_operations.append(pymongo.InsertOne(final_alert))                    # → alert_db
                        alert_triggering_collect_dashboard.bulk_write([pymongo.InsertOne(final_alert)], ordered=False)  # → alert_db_dashboard
                        # ================================
                        
                    # === INSERT INTO VIRGIN & FINAL YIELD DATABASES + NEW FINAL YIELD MAIN DB + DEVIATED FINAL YIELD ===
                    for alert_key, alert_obj in alert_map.items():
                        true_machine_alerts = [ma for ma in alert_obj["machine_alerts"] if ma.get("tbm_shift_occurred", False) in [True, False]]  # Keep both True/False
                        if not true_machine_alerts:
                            continue

                        # Pre-compute mapping: barcode → its actual row (first or last occurrence)
                        virgin_barcode_to_row = {row["barcode"]: row for _, row in virgin_data.iterrows()}
                        final_barcode_to_row = {row["barcode"]: row for _, row in final_data.iterrows()}
                        
                        # === FINAL YIELD: Only the LAST occurrence of each barcode ===
                        final_machine_alerts = []
                        final_all_barcodes = set()
                        for ma in true_machine_alerts:
                            original_barcodes = ma.get("barcodes", [])
                            if not original_barcodes:
                                continue
                            filtered_barcodes = []
                            for bc in original_barcodes:
                                bc_str = str(bc)
                                if bc_str in final_barcode_to_row and bc_str not in final_all_barcodes:
                                    filtered_barcodes.append(bc_str)
                                    final_all_barcodes.add(bc_str)
                            if filtered_barcodes:
                                final_ma = ma.copy()
                                final_ma["barcodes"] = filtered_barcodes
                                final_ma["barcode_count"] = len(filtered_barcodes)
                                final_machine_alerts.append(final_ma)

                        if final_machine_alerts and final_all_barcodes:
                            all_final_bcs = sorted(final_all_barcodes,
                                                key=lambda x: final_data[final_data["barcode"] == x]["date_of_dt"].iloc[0])

                            # === Main Final Yield Document (kept for backward compatibility) ===
                            doc_final = {
                                "alert_id_tbm_ufm": alert_obj["alert_id_tbm_ufm"],
                                "modelno": alert_obj["modelno"],
                                "lot_id": alert_obj["lot_id"],
                                "alert_flag": False,
                                "dashboard_flag": False,
                                "machine_flag": False,
                                "alert_status_check": "o",
                                "alert_status_timestamp": None,
                                "alert_status_comment": [],
                                "alert_assign_user": [],
                                "machine_alerts": final_machine_alerts,
                                "first_barcode_model": all_final_bcs[0] if all_final_bcs else None,
                                "last_barcode_model": all_final_bcs[-1] if all_final_bcs else None,
                                "timestamp_alert_generated": some_function(),
                                "yield_type": "final"
                            }
                            alert_triggering_final_yield_collect.insert_one(doc_final)

                            # === NEW: Insert into dedicated Final Yield main DB ===
                            try:
                                alert_final_yield_collect.insert_one(doc_final)
                            except Exception as e:
                                logging.error(f"Failed insert into alert_table_final_yield: {e}")

                            # === DEVIATED FINAL YIELD: Same structure as normal deviated alerts ===
                            # We reuse the exact same deviated_doc that we already build earlier in the loop
                            # (the one that goes into alert_deviated → alert_triggering_deviated_collect)

                            # Build deviated_doc exactly like the normal deviated one
                            deviated_parameter_alerts = [
                                alert for alert in parameter_alerts
                                if alert.get("shift_occurred_thershold", False) or alert.get("shift_occurred_spec", False)
                            ]

                            # Add rank counts (same logic you already have)
                            for alert in deviated_parameter_alerts:
                                param = alert.get("parameter", "")
                                if param.endswith("_yield") and param in rank_param_mappings:
                                    rank_counts = {}
                                    for col in rank_param_mappings[param]:
                                        if col in model_data.columns:
                                            value_counts = model_data[col].value_counts().to_dict()
                                            for rank_value, count in value_counts.items():
                                                rank_value = int(rank_value) if isinstance(rank_value, (int, float)) else rank_value
                                                rank_counts[f"{col}_{rank_value}"] = int(count)
                                    alert["rank_counts"] = rank_counts

                            if deviated_parameter_alerts:
                                deviated_doc = {
                                    "alert_id_tbm_ufm": last_alert_id,  # same ID as main alert
                                    "modelno": model_number,
                                    "lot_id": unique_lot_id_current,
                                    "machine": machine_no,
                                    "comments": [],
                                    "alert_assign_user": [],
                                    "tbm_machine_model_lot_id": current_tbm_machine_model_lot_id,
                                    "tbm_previous_machine_model_lot_id": tbm_previous_machine_model_lot_id,
                                    "parameter_alerts": deviated_parameter_alerts,
                                    "timestamp_alert_generated": some_function(),
                                    "yield_type": "final"   # <-- extra field to identify it's Final Yield
                                }

                                # # Insert into the original deviated collection (unchanged behaviour)
                                # deviated_triggering_bulk_operations.append(pymongo.InsertOne(deviated_doc))

                                # === NEW: Insert the EXACT SAME document into the new Final Yield deviated DB ===
                                try:
                                    alert_deviated_final_yield_collect.insert_one(deviated_doc)
                                    output_messages.append(f"Inserted deviated Final Yield alert {last_alert_id} "
                                                        f"(model {model_number}, lot {unique_lot_id_current})")
                                except Exception as e:
                                    logging.error(f"Failed insert into alert_deviated_final_yield: {e}")
                        
                        # === VIRGIN YIELD (Unchanged - kept for backward compatibility) ===
                        virgin_machine_alerts = []
                        virgin_all_barcodes = set()
                        for ma in true_machine_alerts:
                            original_barcodes = ma.get("barcodes", [])
                            if not original_barcodes:
                                continue
                            filtered_barcodes = []
                            for bc in original_barcodes:
                                bc_str = str(bc)
                                if bc_str in virgin_barcode_to_row and bc_str not in virgin_all_barcodes:
                                    filtered_barcodes.append(bc_str)
                                    virgin_all_barcodes.add(bc_str)
                            if filtered_barcodes:
                                virgin_ma = ma.copy()
                                virgin_ma["barcodes"] = filtered_barcodes
                                virgin_ma["barcode_count"] = len(filtered_barcodes)
                                virgin_machine_alerts.append(virgin_ma)

                        if virgin_machine_alerts and virgin_all_barcodes:
                            all_virgin_bcs = sorted(virgin_all_barcodes, key=lambda x: virgin_data[virgin_data["barcode"] == x]["date_of_dt"].iloc[0])
                            doc_virgin = {
                                "alert_id_tbm_ufm": alert_obj["alert_id_tbm_ufm"],
                                "modelno": alert_obj["modelno"],
                                "lot_id": alert_obj["lot_id"],
                                "alert_flag": False,
                                "dashboard_flag": False,
                                "machine_flag": False,
                                "alert_status_check": "o",
                                "alert_status_timestamp": None,
                                "alert_status_comment": [],
                                "alert_assign_user": [],
                                "machine_alerts": virgin_machine_alerts,
                                "first_barcode_model": all_virgin_bcs[0] if all_virgin_bcs else None,
                                "last_barcode_model": all_virgin_bcs[-1] if all_virgin_bcs else None,
                                "timestamp_alert_generated": some_function(),
                                "yield_type": "virgin"
                            }
                            alert_triggering_virgin_yield_collect.insert_one(doc_virgin)
                
                except Exception as e:
                    error_message = f"❌ TBM Error processing model {model_number} for TBM {machine_no}: {e}"
                    output_messages.append(error_message)

        except Exception as e:
            error_message = f"❌ TBM Error processing TBM {machine_no}: {e}"
            output_messages.append(error_message)

    # Perform bulk operations
    if bulk_operations:
        try:
            result = lot_tbm_collect.bulk_write(bulk_operations, ordered=False)
            inserted_counts["lot_tbm_table"] = result.inserted_count
            output_messages.append(f"✅ TBM Inserted {result.inserted_count} documents into lot_tbm_table.")
        except Exception as e:
            error_message = f"❌ TBM Error performing bulk write to lot_tbm_table: {e}"
            output_messages.append(error_message)

    if alert_bulk_operations:
        try:
            result = alert_collect.bulk_write(alert_bulk_operations, ordered=False)
            inserted_counts["alert_tbm_table"] = result.inserted_count
            output_messages.append(f"✅ TBM Inserted {result.inserted_count} documents into alert_tbm_table.")
        except Exception as e:
            error_message = f"❌ TBM Error performing bulk write to alert_tbm_table: {e}"
            output_messages.append(error_message)

    if averages_bulk_operations:
        try:
            result = averages_collect.bulk_write(averages_bulk_operations, ordered=False)
            inserted_counts["averages_tbm_table"] = result.inserted_count
            output_messages.append(f"✅ TBM Inserted {result.inserted_count} averages documents into averages_tbm_table.")
        except Exception as e:
            error_message = f"❌ TBM Error performing bulk write to averages_tbm_table: {e}"
            output_messages.append(error_message)
    else:
        output_messages.append("No TBM averages documents to insert.")

    if triggering_bulk_operations:
        try:
            result = alert_triggering_collect.bulk_write(triggering_bulk_operations, ordered=False)
            inserted_counts["alert_triggering_tbm"] = result.inserted_count
            output_messages.append(f"✅ TBM Inserted {result.inserted_count} machine_alert documents into alert_triggering_tbm.")
            # NEW: Write to alert_db_dashboard
            result_dashboard = alert_triggering_collect_dashboard.bulk_write(triggering_bulk_operations, ordered=False)
            inserted_counts["alert_triggering_tbm_dashboard_new"] = result_dashboard.inserted_count
            output_messages.append(f"✅ TBM Inserted {result_dashboard.inserted_count} machine_alert documents into alert_db_dashboard.alert_triggering_tbm_new.")
        except Exception as e:
            error_message = f"❌ TBM Error performing bulk write to alert_triggering_tbm: {e}"
            output_messages.append(error_message)
    else:
        output_messages.append("No TBM machine_alert documents to insert.")

    # # NEW: Execute bulk write for deviated triggering collection
    # if deviated_triggering_bulk_operations:
    #     try:
    #         result = alert_triggering_deviated_collect.bulk_write(deviated_triggering_bulk_operations, ordered=False)
    #         inserted_counts["alert_tbm_without_lot_size"] = result.inserted_count
    #         output_messages.append(f"✅ TBM Inserted {result.inserted_count} deviated alert documents into alert_deviated.alert_tbm_without_lot_size.")
    #     except Exception as e:
    #         error_message = f"❌ TBM Error performing bulk write to alert_deviated.alert_tbm_without_lot_size: {e}"
    #         output_messages.append(error_message)
    # else:
    #     output_messages.append("No TBM deviated alert documents to insert into alert_deviated.alert_tbm_without_lot_size.")
    
    # === BULK WRITES (unchanged except deviated is now clean) ===
    if deviated_triggering_bulk_operations:
        result = alert_triggering_deviated_collect.bulk_write(deviated_triggering_bulk_operations, ordered=False)
        inserted_counts["deviated_normal"] = result.inserted_count

    # Update merge_df
    bulk_updates = []
    for index, row in merge_df.iterrows():
        query = {"_id": row["_id"]}
        update = {"$set": {
            "alert_id_tbm_ufm": row.get("alert_id_tbm_ufm"),
            "lot_id": row.get("lot_id"),
            "previous_lot_id": row.get("previous_lot_id"),
            "tbm_machine_model_lot_id": row.get("tbm_machine_model_lot_id"),
            "tbm_previous_machine_model_lot_id": row.get("tbm_previous_machine_model_lot_id")
        }}
        bulk_updates.append(pymongo.UpdateOne(query, update))

    if bulk_updates:
        try:
            result = merge_collect.bulk_write(bulk_updates, ordered=False)
            updated_counts["data_mereged_dashboard"] = result.modified_count
            output_messages.append(f"✅ TBM Updated {result.modified_count} documents in data_mereged_dashboard.")
        except Exception as e:
            error_message = f"❌ TBM Error updating data_mereged_dashboard: {e}"
            output_messages.append(error_message)
    else:
        output_messages.append("No TBM documents to update in data_mereged_dashboard.")

    end_time = time.time()
    execution_time = end_time - start_time
    final_message = f"✅ TBM Lot and Alert Process completed successfully in {execution_time:.2f} seconds!"
    output_messages.append(final_message)

    logging.info("process_tbm_lot_and_alert_data_without_lot_size function is completed")
    return {
        "status": "success",
        "message": final_message,
        "output_log": output_messages,
        "inserted_counts": inserted_counts,
        "updated_counts": updated_counts,
        "execution_time": execution_time
    }, 200

def process_curing_data_and_generate_lots_alerts_without_lot_size():
    start_time = time.time()
    output_messages = []
    inserted_counts = {}
    updated_counts = {}
    logging.info("process_curing_data_and_generate_lots_alerts_without_lot_size function is just started")

    try:
        merge_collect = client[TUB_CIMCORP_DB][new_inner_join_collection_collection]
        channel_collect_curing = client[config_databse][channel_no_collect_curing]
        channel_collect_ufm = client[config_databse][channel_no_collect]
        lot_curing_collect = client[lot_db][lot_curing_collect_name_new]
        alert_collect = client[alert_table][alert_curing_collect_name_new]
        averages_collect = client[average_db][averages_curing_table_name_new]
        machine_collect = client[config_databse][machine_no_collect]
        curing_press_collect = client[config_databse][curing_press_dict_collect]
        alert_triggering_collect = client[alert_db][alert_triggering_curing_collect_name_new]
        summary_triggering_collect = client[summary_db][summary_triggering_collect_name_tbm]
        # NEW: Initialize new collection for deviated alerts
        alert_triggering_deviated_collect = client[alert_deviated][alert_triggering_deviated_curing_new]
        # NEW: Initialize collections for alert_db_dashboard with the same collection names
        alert_triggering_collect_dashboard_curing = client[alert_db_dashboard][alert_triggering_curing_collect_name_new]
        # === NEW: Virgin Yield & Final Yield Collections ===
        alert_triggering_virgin_yield_collect = client[alert_db_virgin_yield][alert_triggering_curing_collect_name_new]
        alert_triggering_final_yield_collect = client[alert_db_final_yield][alert_triggering_curing_collect_name_new]
        # === NEW: Dedicated Final Yield Only Databases (YOUR REQUEST) ===
        alert_final_yield_collect = client["alert_table_final_yield"][alert_curing_collect_name_new]  # Only Final Yield
        alert_triggering_deviated_final_yield_collect = client["alert_deviated_final_yield"][alert_triggering_deviated_curing_new]  # Deviated from Final Yield
        # NEW: Initialize collections for model_workcenter_db
        curing_collect_data = client[model_db][curing_collection]
        # NEW: Initialize curing_modelno collection from model_workcenter_db
        curing_modelno_collect = client[model_db][curing_collection_machine]
        output_messages.append("✅ TBM Collections loaded successfully for lot and alert processing in both alert_db and alert_db_dashboard.")
    except pymongo.errors.OperationFailure as e:
        error_message = f"❌ Curing Database or collection error: {e}"
        output_messages.append(error_message)
        return {"status": "error", "message": error_message, "output_log": output_messages}, 500
    
    # NEW: Fetch active curing_press_id from curing_modelno
    try:
        active_curing_press_ids = set(curing_modelno_collect.distinct("curing_press_id", {"status": "active"}))
        output_messages.append(f"✅ Fetched {len(active_curing_press_ids)} active curing_press_id values: {active_curing_press_ids}")
    except Exception as e:
        error_message = f"❌ Error fetching active curing_press_id from curing_modelno: {e}"
        output_messages.append(error_message)
        return {"status": "error", "message": "Failed to fetch active curing_press_id", "output_log": output_messages}, 500

    
    # Fetch active model numbers
    try:
        active_modelnos = set(curing_collect_data.distinct("modelno", {"status": "active"}))
        output_messages.append(f"✅ Fetched {len(active_modelnos)} active model numbers: {active_modelnos}")
    except Exception as e:
        output_messages.append(f"❌ Error fetching active model numbers: {e}")
        return {"status": "error", "message": "Failed to fetch active model numbers", "output_log": output_messages}, 500

    try:
        merge1 = list(merge_collect.find({}))
        merge_df = pd.DataFrame(merge1)
        channel_data_curing = list(channel_collect_curing.find({"modelno": {"$in": list(active_modelnos)}}))
        # channel_data_curing = list(channel_collect_curing.find({}))
        channel_df = pd.DataFrame(channel_data_curing)
        channel_data_ufm = list(channel_collect_ufm.find({}))
        channel_df_ufm = pd.DataFrame(channel_data_ufm)
        machine_data = list(machine_collect.find({}))
        machine_df = pd.DataFrame(machine_data)
        # MODIFIED: Filter curing_press_dict by active curing_press_id
        curing_press_data = list(curing_press_collect.find({"curing_press_id": {"$in": list(active_curing_press_ids)}}))
        curing_press_data = list(curing_press_collect.find({}))
        curing_df = pd.DataFrame(curing_press_data)
        output_messages.append("✅ Curing Data loaded from collections into DataFrames.")
    except Exception as e:
        error_message = f"❌ Curing Error loading data: {e}"
        output_messages.append(error_message)
        return {"status": "error", "message": error_message, "output_log": output_messages}, 500

    channel_modelnos = set(channel_collect_curing.distinct("modelno"))
    merge_df = merge_df[merge_df["modelno_cimcorp"].isin(channel_modelnos)]
    merge_df = merge_df[merge_df["alert_id_curing_ufm"].isna()]

    merge_df["date_of_tbm"] = merge_df["tbm_buildtime"]
    merge_df["date_of_curing"] = merge_df["curing_infeedtime"]
    merge_df["date_and_time"] = merge_df["date"].fillna('') + " " + merge_df["time"].fillna('')

    # Function to handle datetime conversion
    def convert_datetime_with_invalid_mask(series, col_name):
        try:
            original_series = series.copy()
            # Normalize whitespace and ensure string type
            cleaned_series = original_series.astype(str).str.strip().str.replace(r'\s+', ' ', regex=True)

            # Use correct format matching your actual data: "YYYY-MM-DD HH:MM:SS"
            datetime_series = pd.to_datetime(cleaned_series, format='%Y-%m-%d %H:%M:%S', errors='coerce')

            # Identify invalid (unparsable) entries
            invalid_mask = datetime_series.isna() & original_series.notna()

            # Replace invalids with DEFAULT_DATETIME (must be a datetime object)
            datetime_series[invalid_mask] = DEFAULT_DATETIME

            # Return formatted datetime string
            return datetime_series.dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            return pd.Series([DEFAULT_DATETIME] * len(series))

    try:
        merge_df["date_and_time"] = convert_datetime_with_invalid_mask(merge_df["date_and_time"], "date_and_time")
        merge_df["date_and_time"] = merge_df["date_and_time"].astype(object).where(merge_df["date_and_time"].notnull(), "No Date")
        merge_df["date_of_tbm"] = convert_datetime_with_invalid_mask(merge_df["date_of_tbm"], "date_of_tbm")
        merge_df["date_of_tbm"] = merge_df["date_of_tbm"].astype(object).where(merge_df["date_of_tbm"].notnull(), "No Date")
        merge_df["date_of_curing"] = convert_datetime_with_invalid_mask(merge_df["date_of_curing"], "date_of_curing")
        merge_df["date_of_curing"] = merge_df["date_of_curing"].astype(object).where(merge_df["date_of_curing"].notnull(), "No Date")
        merge_df["time_of_ufm"] = pd.to_datetime(merge_df["time"], errors="coerce").dt.strftime("%H:%M:%S")
        time_invalid_mask = merge_df["time_of_ufm"].isna() & merge_df["time"].notna()
        if time_invalid_mask.any():
            logging.info("⚠️ Found %s invalid time values", time_invalid_mask.sum())
    except Exception as e:
        logging.info("❌ Error during datetime conversion: %s", e)
        return {"status": "error", "message": f"Error during datetime conversion: {e}", "output_log": output_messages}, 500

    merge_df["curing_shift"] = merge_df["date_of_curing"].apply(get_shift)

    try:
        last_alert = alert_collect.find_one(sort=[("alert_id_curing_ufm", -1)])
        last_alert_id = last_alert["alert_id_curing_ufm"] if last_alert else 0
        output_messages.append(f"ℹ️ Last Curing Alert ID found: {last_alert_id}")
    except Exception as e:
        error_message = f"❌ Curing Error fetching last alert ID: {e}"
        output_messages.append(error_message)
        last_alert_id = 0

    # Fetch last lot numbers per machine-model combination
    try:
        last_lots = list(lot_curing_collect.aggregate([
            {
                "$match": {
                    "lot_id": {"$regex": "^CURING_[^_]+_[^_]+_\\d+$"},
                    "modelno": {"$in": list(active_modelnos)},
                    "machine_data.curing_press": {"$exists": True}
                }
            },
            {
                "$project": {
                    "modelno": "$modelno",
                    "machine": "$machine_data.curing_press",
                    "lot_id_parts": {"$split": ["$lot_id", "_"]},
                    "curing_machine_model_lot_id": "$machine_data.curing_machine_model_lot_id"
                }
            },
            {
                "$project": {
                    "modelno": 1,
                    "machine": 1,
                    "lot_number": {"$toInt": {"$arrayElemAt": ["$lot_id_parts", -1]}},
                    "curing_machine_model_lot_id": 1
                }
            },
            {
                "$match": {"lot_number": {"$ne": None}}
            },
            {
                "$group": {
                    "_id": {"modelno": "$modelno", "machine": "$machine"},
                    "max_lot_number": {"$max": "$lot_number"},
                    "last_curing_machine_model_lot_id": {"$last": "$curing_machine_model_lot_id"}
                }
            }
        ]))
        last_lot_dict = {
            (item["_id"]["modelno"], item["_id"]["machine"]): {
                "max_lot_number": item["max_lot_number"],
                "last_curing_machine_model_lot_id": item["last_curing_machine_model_lot_id"]
            } for item in last_lots
        }
        output_messages.append(f"ℹ️ Last Curing Lot numbers fetched: {last_lot_dict}")
    except Exception as e:
        error_message = f"❌ Curing Error fetching last lot IDs: {e}"
        output_messages.append(error_message)
        last_lot_dict = {}

    # curing_df["curing_press_id"] = curing_df["curing_press_id"].astype(str)
    # curing_df["value"] = curing_df["value"].astype(str)
    # merge_df = merge_df.merge(
    #     curing_df[["value", "curing_press_id"]],
    #     left_on="curing_press",
    #     right_on="value",
    #     how="left"
    # )
    # merge_df["curing_press"] = merge_df["curing_press_id"].fillna(merge_df["curing_press"])
    # merge_df.drop(columns=["value", "curing_press_id"], inplace=True)
    # output_messages.append("✅ Curing merge_df merged with curing_press_df successfully.")

    curing_previous_machine_model_lot_ids = {
        key: value["last_curing_machine_model_lot_id"] for key, value in last_lot_dict.items()
    }
    machine_lot_avg_map = {}
    # === NEW: Bulk operations for Final Yield Only DBs ===
    final_yield_only_bulk = []           # alert_table_final_yield
    final_yield_deviated_only_bulk = []  # alert_deviated_final_yield
    alert_bulk_operations = []
    averages_bulk_operations = []
    bulk_operations = []
    deviated_triggering_bulk_operations = []
    triggering_bulk_operations = []

    conicity_params = ["con_kgf", "cwrfv_oa_kgf", "ccwrfv_oa_kgf",
                       "cwrfv_oa1h_kgf", "ccwrfv_oa1h_kgf", "lfv_oa_kgf", "lfv_oa_1_kgf",
                       "upper_g", "lower_g", "static_g", "up_low_g", "load1", "airpress1"]
    yield_params = [
        "total_rank_yield", "rfv_yield", "rfh1_yield", "lfv_yield", "conicity_yield",
        "ufm_yield", "rorank_yield", "radial_runout_yield", "radial_runout_first_harmonic_yield",
        "lateral_run_out_yield", "lateral_run_out_top_over_yield", "lateral_run_out_bottom_over_yield",
        "lateral_run_out_harmonic_yield", "lateral_run_out_top_first_harmonic_yield",
        "lateral_run_out_bottom_first_harmonic_yield", "bulge_yield", "bulge_top_over_yield",
        "bulge_bottom_over_yield", "dent_yield", "dent_top_over_yield", "dent_bottom_over_yield",
        "balancing_yield", "upper_yield", "lower_yield", "upper_lower_yield", "static_yield", "warehouse_yield", "visual_yield"
        ]
    
    # Define rank parameter mappings
    rank_param_mappings = {
        "total_rank_yield": ["total_rank"],
        "rfv_yield": ["cwrfv_oa_rank", "ccwrfv_oa_rank"],
        "rfh1_yield": ["cwrfv_oa1h_rank", "ccwrfv_oa1h_rank"],
        "lfv_yield": ["lfv_oa_rank", "lfv_oa_1_rank"],
        "conicity_yield": ["con_rank"],
        "ufm_yield": ["ufm_rank"],
        "rorank_yield": ["rorank"], 
        "radial_runout_yield": ["rc_oa_rank"], 
        "radial_runout_first_harmonic_yield": ["rc_oa1h_rank"],
        "lateral_run_out_top_over_yield": ["lt_oa_rank"],
        "lateral_run_out_bottom_over_yield": ["lb_oa_rank"],
        "lateral_run_out_top_first_harmonic_yield": ["lt_oa1h_rank"],
        "lateral_run_out_bottom_first_harmonic_yield": ["lb_oa1h_rank"],
        "bulge_top_over_yield": ["ltbulg_rank"],
        "bulge_bottom_over_yield": ["lbbulg_rank"],
        "dent_top_over_yield": ["dent_rank"],
        "dent_bottom_over_yield": ["dent1_rank"],
        "balancing_yield": ["bal_rank"],
        "upper_yield": ["upper_rank"],
        "lower_yield": ["lower_rank"],
        "upper_lower_yield": ["up_low_rank"],
        "static_yield": ["static_rank"],
        "lateral_run_out_yield": ["lt_oa_rank", "lb_oa_rank"],
        "lateral_run_out_harmonic_yield": ["lt_oa1h_rank", "lb_oa1h_rank"],
        "bulge_yield": ["ltbulg_rank", "lbbulg_rank"],
        "dent_yield": ["dent_rank", "dent1_rank"]
    }

    # Model-level processing
    barcode_counts = merge_df.groupby("modelno").size().reset_index(name="total_barcodes")
    for _, row in barcode_counts.iterrows():
        try:
            model_number = row["modelno"]
            if model_number not in active_modelnos:
                output_messages.append(f"Skipping CURING model {model_number} as it is not active.")
                continue
            total_barcodes = row["total_barcodes"]
            timestamp_averages = some_function()

            # model_data = merge_df[merge_df["modelno"] == model_number].copy()
            model_data.sort_values("date_of_curing", ascending=True, inplace=True)

            # lot_averages = {}
            # for param in conicity_params:
            #     if param in model_data.columns:
            #         valid_vals = pd.to_numeric(model_data[param], errors='coerce').dropna()
            #         lot_averages[param] = float(valid_vals.mean()) if not valid_vals.empty else 0.0
            #     else:
            #         lot_averages[param] = 0.0
            
            lot_averages = {}
            for param in conicity_params:
                if param in model_data.columns:
                    valid_vals = pd.to_numeric(model_data[param], errors='coerce').dropna()
                    if not valid_vals.empty:
                        lot_averages[param] = float(valid_vals.mean())
                    else:
                        lot_averages[param] = None
                else:
                    lot_averages[param] = None

            lot_averages.update({
                "total_rank_yield": total_rank_yield(model_data),
                "rfv_yield": rfv_yield(model_data),
                "rfh1_yield": rfh1_yield(model_data),
                "lfv_yield": lfv_yield(model_data),
                "conicity_yield": conicity_yield(model_data),
                "ufm_yield": ufm_yield(model_data),
                "rorank_yield": rorank_yield(model_data),
                "radial_runout_yield": radial_runout_yield(model_data),
                "radial_runout_first_harmonic_yield": radial_runout_first_harmonic_yield(model_data),
                "lateral_run_out_yield": lateral_run_out_yield(model_data),
                "lateral_run_out_top_over_yield": lateral_run_out_top_over(model_data),
                "lateral_run_out_bottom_over_yield": lateral_run_out_bottom_over(model_data),
                "lateral_run_out_harmonic_yield": lateral_run_out_harmonic_yield(model_data),
                "lateral_run_out_top_first_harmonic_yield": lateral_run_out_top_first_harmonic(model_data),
                "lateral_run_out_bottom_first_harmonic_yield": lateral_run_out_bottom_first_harmonic(model_data),
                "bulge_yield": bulge_yield(model_data),
                "bulge_top_over_yield": bulge_top_over(model_data),
                "bulge_bottom_over_yield": bulge_bottom_over(model_data),
                "dent_yield": dent_yield(model_data),
                "dent_top_over_yield": dent_top_over(model_data),
                "dent_bottom_over_yield": dent_bottom_over(model_data),
                "balancing_yield": balancing_yield(model_data),
                "upper_yield": upper_yield(model_data),
                "lower_yield": lower_yield(model_data),
                "upper_lower_yield": upper_lower_yield(model_data),
                "static_yield": static_yield(model_data),
                "warehouse_yield": warehouse_yield(model_data),
                "visual_yield": visual_yield(model_data)
            })

            grouped_averages = {}
            try:
                model_data["ubno"] = model_data["ubno"].astype(str).str.strip()
                model_data.sort_values("date_and_time", ascending=True, inplace=True)
                ubno_grouped = model_data.groupby("ubno")
                ubno_records = []
                for ubno, group in ubno_grouped:
                    group.sort_values("date_and_time", ascending=True)
                    record = {
                        "ubno": ubno,
                        "barcode_count": len(group["barcode"]),
                        "tub_date_start_time": group["date_and_time"].iloc[0] if not group["date_and_time"].empty else None,
                        "tub_date_end_time": group["date_and_time"].iloc[-1] if not group["date_and_time"].empty else None,
                        "barcodes": group["barcode"].tolist() if "barcode" in group.columns else []
                    }
                    for param in conicity_params:
                        if param in group.columns:
                            valid_vals = pd.to_numeric(group[param], errors='coerce').dropna()
                            record[f"avg_{param}"] = float(valid_vals.mean()) if not valid_vals.empty else 0.0
                        else:
                            record[f"avg_{param}"] = 0.0
                    ubno_records.append(record)
                grouped_averages["ubno"] = ubno_records
            except Exception as e:
                grouped_averages["ubno"] = f"Error: {e}"
                output_messages.append(f"⚠️ Error calculating ubno averages for model {model_number}: {e}")

            model_data.sort_values("date_and_time", ascending=True, inplace=True)
            tub_date_start_time = model_data["date_and_time"].iloc[0] if not model_data["date_and_time"].empty else None
            tub_date_end_time = model_data["date_and_time"].iloc[-1] if not model_data["date_and_time"].empty else None
            date_of_ufm = model_data['date_of_ufm'].iloc[0] if 'date_of_ufm' in model_data and not model_data['date_of_ufm'].empty else None
            date_of_tbm = model_data['date_of_tbm'].iloc[0] if 'date_of_tbm' in model_data and not model_data['date_of_tbm'].empty else None
            lot_start_time = some_function()
            lot_end_time = some_function()

            model_lot_doc = {
                "modelno": model_number,
                "lot_averages": lot_averages,
                "grouped_averages": grouped_averages,
                "timestamp_alert_generated": timestamp_averages,
                "lot_start_time": lot_start_time,
                "lot_end_time": lot_end_time,
                "tub_date_start_time": tub_date_start_time,
                "tub_date_end_time": tub_date_end_time,
                "date_of_ufm": date_of_ufm,
                "date_of_tbm": date_of_tbm,
                "processed_barcodes": len(model_data)
            }
            bulk_operations.append(pymongo.InsertOne(model_lot_doc))

        except Exception as e:
            error_message = f"❌ Error processing model-level data for {model_number}: {e}"
            output_messages.append(error_message)

    # Curing machine-wise processing with machine-specific lots
    for _, row in channel_df.iterrows():
        try:
            ############# Add 16-07 start ##############
            model_number = str(row.get("modelno", "")).strip()
            gt_codes = row.get("gt_code", [])

            # Ensure gt_codes is a list
            if not isinstance(gt_codes, list):
                gt_codes = [gt_codes]
            ############# Add 16-07 end ##############

            spec_con_kgf = float(row.get("spec_con_kgf", 0)) if row.get("spec_con_kgf") is not None else None
            sizecode = row.get("sizecode", None)
            # gt_code = row.get("gt_code", None)
            spec_cwrfv_oa_kgf = float(row.get("spec_cwrfv_oa_kgf", 0))
            spec_ccwrfv_oa_kgf = float(row.get("spec_ccwrfv_oa_kgf", 0))
            # spec_rfh1 = float(row.get("spec_rfh1", 0))
            spec_cwrfv_oa1h_kgf = float(row.get("spec_cwrfv_oa1h_kgf", 0))
            spec_ccwrfv_oa1h_kgf = float(row.get("spec_ccwrfv_oa1h_kgf", 0))
            # spec_lfv = float(row.get("spec_lfv", 0))
            spec_lfv_oa_kgf = float(row.get("spec_lfv_oa_kgf", 0))
            spec_lfv_oa_1_kgf = float(row.get("spec_lfv_oa_1_kgf", 0))
            spec_static = float(row.get("spec_static", 0))
            spec_upper_g = float(row.get("spec_upper_g", 0))
            spec_lower_g = float(row.get("spec_lower_g", 0))
            spec_up_low_g = float(row.get("spec_up_low_g", 0))
            test_load = float(row.get("test_load", 0))
            pressure = float(row.get("pressure", 0))
            # spec_yield = float(row.get("spec_yield", 0))
            total_rank_yield_spec_yield = float(row.get("total_rank_yield_spec_yield", 0))
            rfv_yield_spec_yield = float(row.get("rfv_yield_spec_yield", 0))
            rfh1_yield_spec_yield = float(row.get("rfh1_yield_spec_yield", 0))
            lfv_yield_spec_yield = float(row.get("lfv_yield_spec_yield", 0))
            conicity_yield_spec_yield = float(row.get("conicity_yield_spec_yield", 0))
            ufm_yield_spec_yield = float(row.get("ufm_yield_spec_yield", 0))
            rorank_yield_spec_yield = float(row.get("rorank_yield_spec_yield", 0))
            radial_runout_yield_spec_yield = float(row.get("radial_runout_yield_spec_yield", 0))
            radial_runout_first_harmonic_yield_spec_yield = float(row.get("radial_runout_first_harmonic_yield_spec_yield", 0))
            lateral_run_out_yield_spec_yield = float(row.get("lateral_run_out_yield_spec_yield", 0))
            lateral_run_out_top_over_yield_spec_yield = float(row.get("lateral_run_out_top_over_yield_spec_yield", 0))
            lateral_run_out_bottom_over_yield_spec_yield = float(row.get("lateral_run_out_bottom_over_yield_spec_yield", 0))
            lateral_run_out_harmonic_yield_spec_yield = float(row.get("lateral_run_out_harmonic_yield_spec_yield", 0))
            lateral_run_out_top_first_harmonic_yield_spec_yield = float(row.get("lateral_run_out_top_first_harmonic_yield_spec_yield", 0))
            lateral_run_out_bottom_first_harmonic_yield_spec_yield = float(row.get("lateral_run_out_bottom_first_harmonic_yield_spec_yield", 0))
            bulge_yield_spec_yield = float(row.get("bulge_yield_spec_yield", 0))
            bulge_top_over_yield_spec_yield = float(row.get("bulge_top_over_yield_spec_yield", 0))
            bulge_bottom_over_yield_spec_yield = float(row.get("bulge_bottom_over_yield_spec_yield", 0))
            dent_yield_spec_yield = float(row.get("dent_yield_spec_yield", 0))
            dent_top_over_yield_spec_yield = float(row.get("dent_top_over_yield_spec_yield", 0))
            dent_bottom_over_yield_spec_yield = float(row.get("dent_bottom_over_yield_spec_yield", 0))
            balancing_yield_spec_yield = float(row.get("balancing_yield_spec_yield", 0))
            upper_yield_spec_yield = float(row.get("upper_yield_spec_yield", 0))
            lower_yield_spec_yield = float(row.get("lower_yield_spec_yield", 0))
            upper_lower_yield_spec_yield = float(row.get("upper_lower_yield_spec_yield", 0))
            static_yield_spec_yield = float(row.get("static_yield_spec_yield", 0))
            warehouse_yield_spec_yield = float(row.get("warehouse_yield_spec_yield", 0))
            visual_yield_spec_yield = float(row.get("visual_yield_spec_yield", 0))
            # threshold_value = float(row.get("threshold_value", 0.5))
            threshold_value_con_kgf = float(row.get("threshold_value_con_kgf", 0.5))
            threshold_value_cwrfv_oa_kgf = float(row.get("threshold_value_cwrfv_oa_kgf", 0.5))
            threshold_value_ccwrfv_oa_kgf = float(row.get("threshold_value_ccwrfv_oa_kgf", 0.5))
            threshold_value_cwrfv_oa1h_kgf = float(row.get("threshold_value_cwrfv_oa1h_kgf", 0.5))
            threshold_value_ccwrfv_oa1h_kgf = float(row.get("threshold_value_ccwrfv_oa1h_kgf", 0.5))
            threshold_value_lfv_oa_kgf = float(row.get("threshold_value_lfv_oa_kgf", 0.5))
            threshold_value_lfv_oa_1_kgf = float(row.get("threshold_value_lfv_oa_1_kgf", 0.5))
            # threshold_value_bal = float(row.get("threshold_value_bal", 5))
            threshold_value_bal_upper_g = float(row.get("threshold_value_bal_upper_g", 5))
            threshold_value_bal_lower_g = float(row.get("threshold_value_bal_lower_g", 5))
            threshold_value_bal_up_low_g = float(row.get("threshold_value_bal_up_low_g", 5))
            threshold_value_bal_static_g = float(row.get("threshold_value_bal_static_g", 5))
            test_load_tolerance = float(row.get("test_load_tolerance", 5))
            pressure_tolerance = float(row.get("pressure_tolerance", 2))
            # threshold_value_yield = float(row.get("threshold_value_yield", 0))
            threshold_value_yield_total_rank_yield = float(row.get("threshold_value_yield_total_rank_yield", 0))
            threshold_value_yield_rfv_yield = float(row.get("threshold_value_yield_rfv_yield", 0))
            threshold_value_yield_rfh1_yield = float(row.get("threshold_value_yield_rfh1_yield", 0))
            threshold_value_yield_lfv_yield = float(row.get("threshold_value_yield_lfv_yield", 0))
            threshold_value_yield_conicity_yield = float(row.get("threshold_value_yield_conicity_yield", 0))
            threshold_value_yield_ufm_yield = float(row.get("threshold_value_yield_ufm_yield", 0))
            threshold_value_yield_rorank_yield = float(row.get("threshold_value_yield_rorank_yield", 0))
            threshold_value_yield_radial_runout_yield = float(row.get("threshold_value_yield_radial_runout_yield", 0))
            threshold_value_yield_radial_runout_first_harmonic_yield = float(row.get("threshold_value_yield_radial_runout_first_harmonic_yield", 0))
            threshold_value_yield_lateral_run_out_yield = float(row.get("threshold_value_yield_lateral_run_out_yield", 0))
            threshold_value_yield_lateral_run_out_top_over_yield = float(row.get("threshold_value_yield_lateral_run_out_top_over_yield", 0))
            threshold_value_yield_lateral_run_out_bottom_over_yield = float(row.get("threshold_value_yield_lateral_run_out_bottom_over_yield", 0))
            threshold_value_yield_lateral_run_out_harmonic_yield = float(row.get("threshold_value_yield_lateral_run_out_harmonic_yield", 0))
            threshold_value_yield_lateral_run_out_top_first_harmonic_yield = float(row.get("threshold_value_yield_lateral_run_out_top_first_harmonic_yield", 0))
            threshold_value_yield_lateral_run_out_bottom_first_harmonic_yield = float(row.get("threshold_value_yield_lateral_run_out_bottom_first_harmonic_yield", 0))
            threshold_value_yield_bulge_yield = float(row.get("threshold_value_yield_bulge_yield", 0))
            threshold_value_yield_bulge_top_over_yield = float(row.get("threshold_value_yield_bulge_top_over_yield", 0))
            threshold_value_yield_bulge_bottom_over_yield = float(row.get("threshold_value_yield_bulge_bottom_over_yield", 0))
            threshold_value_yield_dent_yield = float(row.get("threshold_value_yield_dent_yield", 0))
            threshold_value_yield_dent_top_over_yield = float(row.get("threshold_value_yield_dent_top_over_yield", 0))
            threshold_value_yield_dent_bottom_over_yield = float(row.get("threshold_value_yield_dent_bottom_over_yield", 0))
            threshold_value_yield_balancing_yield = float(row.get("threshold_value_yield_balancing_yield", 0))
            threshold_value_yield_upper_yield = float(row.get("threshold_value_yield_upper_yield", 0))
            threshold_value_yield_lower_yield = float(row.get("threshold_value_yield_lower_yield", 0))
            threshold_value_yield_upper_lower_yield = float(row.get("threshold_value_yield_upper_lower_yield", 0))
            threshold_value_yield_static_yield = float(row.get("threshold_value_yield_static_yield", 0))
            threshold_value_yield_warehouse_yield = float(row.get("threshold_value_yield_warehouse_yield", 0))
            threshold_value_yield_visual_yield = float(row.get("threshold_value_yield_visual_yield", 0))

            required_columns = ["modelno", "modelno_cimcorp", "barcode", "date_of_curing", "date_and_time", "curing_press", "curing_mouldid"] + conicity_params
            missing_columns = [col for col in required_columns if col not in merge_df.columns]
            if missing_columns:
                error_message = f"⚠️ Skipping model number {model_number}: Missing required columns: {missing_columns}"
                output_messages.append(error_message)
                continue

            merge_df["modelno"] = merge_df["modelno"].astype(str).str.strip()
            merge_df["modelno_cimcorp"] = merge_df["modelno_cimcorp"].astype(str).str.strip()
            # model_data = merge_df[(merge_df["modelno"] == str(model_number)) &
            #                      (merge_df["modelno_cimcorp"] == str(model_number))].copy()

            ############# Add 16-07 start ##############
            for gt_code in gt_codes:
                gt_code = str(gt_code).strip()

                # Filter merge_df for each gt_code individually
                model_data = merge_df[
                    (merge_df["modelno"] == model_number) &
                    (merge_df["modelno_cimcorp"] == model_number) &
                    (merge_df["tbm_gtcode"] == gt_code)
                ].copy()
            ############# Add 16-07 end ##############

                total_barcodes_model = len(model_data)
                
                model_data_full = model_data.copy()
                
                # Ensure date_of_tbm is proper datetime for sorting
                model_data_full["date_of_dt"] = pd.to_datetime(model_data_full["date_and_time"], errors='coerce')

                # === 1. Virgin Yield: Keep FIRST occurrence of each barcode ===
                virgin_data = model_data_full.sort_values("date_of_dt").drop_duplicates(subset="barcode", keep="first").copy()
                # >>> ADD THESE PRINT STATEMENTS <<<
                virgin_dupes = model_data_full[model_data_full.duplicated(subset="barcode", keep=False)]
                if not virgin_dupes.empty:
                    first_occ = virgin_dupes.sort_values("date_of_dt").drop_duplicates("barcode", keep="first")
                    # print("=== VIRGIN YIELD - Duplicate barcodes (first occurrence shown) ===")
                    # for _, r in first_occ.iterrows():
                    #     print(f"Barcode: {r['barcode']} | First seen: {r['date_and_time']} | Machine: {r['machine']} | Model: {model_number}")
                # <<< END OF ADDED PRINT >>>

                # === 2. Final Yield: Keep LAST occurrence of each barcode ===
                final_data = model_data_full.sort_values("date_of_dt").drop_duplicates(subset="barcode", keep="last").copy()
                # >>> ADD THESE PRINT STATEMENTS <<<
                final_dupes = model_data_full[model_data_full.duplicated(subset="barcode", keep=False)]
                if not final_dupes.empty:
                    last_occ = final_dupes.sort_values("date_of_dt").drop_duplicates("barcode", keep="last")
                    # print("=== FINAL YIELD - Duplicate barcodes (last occurrence shown) ===")
                    # for _, r in last_occ.iterrows():
                    #     print(f"Barcode: {r['barcode']} | Last seen: {r['date_and_time']} | Machine: {r['machine']} | Model: {model_number}")
                # <<< END OF ADDED PRINT >>>

                # Sort both for lot processing
                virgin_data.sort_values("date_of_dt", inplace=True)
                final_data.sort_values("date_of_dt", inplace=True)

                # Use original full data for main lot processing (existing logic)
                model_data = model_data_full.copy()
                model_data.sort_values(["date_of_curing"], ascending=True, inplace=True)
                alert_map = {}

                # Initialize machine_lot_avg_map with previous averages
                machine_lot_avg_map = {}
                for key, info in last_lot_dict.items():
                    modelno, machine = key
                    lot_id = info["last_curing_machine_model_lot_id"]
                    if lot_id:
                        doc = averages_collect.find_one({"curing_machine_model_lot_id": lot_id}, {"current_lot_averages": 1})
                        if doc and "current_lot_averages" in doc:
                            machine_lot_avg_map[(modelno, machine, lot_id)] = doc["current_lot_averages"]
                # logging.info(f"Initialized machine_lot_avg_map: {machine_lot_avg_map}")

                for (curing_press, curing_mouldid), curing_data in model_data.groupby(["curing_press", "curing_mouldid"]):
                    curing_data = curing_data[curing_data["curing_machine_model_lot_id"].isnull()].copy()
                    if curing_data.empty:
                        output_messages.append(f"DEBUG: No unprocessed curing data for machine {curing_press}_{curing_mouldid}, model {model_number}.")
                        continue

                    total_curing_barcodes = len(curing_data)
                    # Use hyphen for machine_no
                    machine_no = f"{curing_press}-{curing_mouldid}"
                    machine_model_key = (model_number, machine_no)

                    # Initialize lot number for this machine-model combination
                    last_lot_info = last_lot_dict.get(machine_model_key, {"max_lot_number": 0, "last_curing_machine_model_lot_id": None})
                    last_lot_number = last_lot_info["max_lot_number"]
                    curing_previous_machine_model_lot_id = last_lot_info["last_curing_machine_model_lot_id"]
                    previous_lot_id = f"CURING_{machine_no}_{model_number}_{last_lot_number}" if last_lot_number > 0 else None

                    last_lot_number += 1
                    unique_lot_id_current = f"CURING_{machine_no}_{model_number}_{last_lot_number}"
                    current_curing_machine_model_lot_id = f"{machine_no}_{model_number}_CURING_{machine_no}_{model_number}_{last_lot_number}_{total_curing_barcodes}"


                    prev_key = (model_number, machine_no, curing_previous_machine_model_lot_id)
                    prev_lot_avg = machine_lot_avg_map.get(prev_key, None)
                    if prev_lot_avg is None and curing_previous_machine_model_lot_id:
                        previous_lot = averages_collect.find_one(
                            {"curing_machine_model_lot_id": curing_previous_machine_model_lot_id.strip()},
                            {"current_lot_averages": 1}
                        )
                        if previous_lot and "current_lot_averages" in previous_lot:
                            prev_lot_avg = previous_lot["current_lot_averages"]
                            logging.info(f"Found previous_lot_averages for {curing_previous_machine_model_lot_id}: {prev_lot_avg}")
                        else:
                            logging.warning(f"No document or current_lot_averages found for curing_previous_machine_model_lot_id: {curing_previous_machine_model_lot_id}")
                            prev_lot_avg = None

                    merge_df.loc[curing_data.index, "lot_id"] = unique_lot_id_current
                    merge_df.loc[curing_data.index, "previous_lot_id"] = previous_lot_id
                    merge_df.loc[curing_data.index, "curing_machine_model_lot_id"] = current_curing_machine_model_lot_id
                    merge_df.loc[curing_data.index, "curing_previous_machine_model_lot_id"] = curing_previous_machine_model_lot_id

                    curing_previous_machine_model_lot_ids[machine_model_key] = current_curing_machine_model_lot_id
                    processed_machine_barcodes = total_curing_barcodes

                    # Store lot data in lot_curing_collect
                    combined_data = {
                        "modelno": model_number,
                        "lot_id": unique_lot_id_current,
                        "previous_lot_id": previous_lot_id,
                        "processed_barcodes": int(total_curing_barcodes),
                        "total_barcodes": int(total_barcodes_model),
                        "timestamp_lot_generated": some_function(),
                        "machine_data": {
                            "curing_press": machine_no,
                            "curing_machine_model_lot_id": current_curing_machine_model_lot_id,
                            "curing_previous_machine_model_lot_id": curing_previous_machine_model_lot_id,
                            "processed_machine_barcodes": int(processed_machine_barcodes),
                            "total_machine_barcodes": int(total_curing_barcodes),
                            "timestamp_lot_generated": some_function()
                        }
                    }
                    lot_query = {
                        "modelno": model_number,
                        "lot_id": unique_lot_id_current,
                        "machine_data.curing_machine_model_lot_id": current_curing_machine_model_lot_id
                    }
                    if not lot_curing_collect.find_one(lot_query):
                        bulk_operations.append(pymongo.InsertOne(combined_data))

                    # lot_averages = {}
                    # for param in conicity_params:
                    #     if param in curing_data.columns:
                    #         valid_vals = pd.to_numeric(curing_data[param], errors='coerce').dropna()
                    #         lot_averages[param] = float(valid_vals.mean()) if not valid_vals.empty else 0.0
                    #     else:
                    #         lot_averages[param] = 0.0

                    lot_averages = {}
                    for param in conicity_params:
                        if param in curing_data.columns:
                            valid_vals = pd.to_numeric(curing_data[param], errors='coerce').dropna()
                            if not valid_vals.empty:
                                lot_averages[param] = float(valid_vals.mean())
                            else:
                                lot_averages[param] = None
                        else:
                            lot_averages[param] = None

                    lot_averages.update({
                        "total_rank_yield": total_rank_yield(curing_data),
                        "rfv_yield": rfv_yield(curing_data),
                        "rfh1_yield": rfh1_yield(curing_data),
                        "lfv_yield": lfv_yield(curing_data),
                        "conicity_yield": conicity_yield(curing_data),
                        "ufm_yield": ufm_yield(curing_data),
                        "rorank_yield": rorank_yield(curing_data),
                        "radial_runout_yield": radial_runout_yield(curing_data),
                        "radial_runout_first_harmonic_yield": radial_runout_first_harmonic_yield(curing_data),
                        "lateral_run_out_yield": lateral_run_out_yield(curing_data),
                        "lateral_run_out_top_over_yield": lateral_run_out_top_over(curing_data),
                        "lateral_run_out_bottom_over_yield": lateral_run_out_bottom_over(curing_data),
                        "lateral_run_out_harmonic_yield": lateral_run_out_harmonic_yield(curing_data),
                        "lateral_run_out_top_first_harmonic_yield": lateral_run_out_top_first_harmonic(curing_data),
                        "lateral_run_out_bottom_first_harmonic_yield": lateral_run_out_bottom_first_harmonic(curing_data),
                        "bulge_yield": bulge_yield(curing_data),
                        "bulge_top_over_yield": bulge_top_over(curing_data),
                        "bulge_bottom_over_yield": bulge_bottom_over(curing_data),
                        "dent_yield": dent_yield(curing_data),
                        "dent_top_over_yield": dent_top_over(curing_data),
                        "dent_bottom_over_yield": dent_bottom_over(curing_data),
                        "balancing_yield": balancing_yield(curing_data),
                        "upper_yield": upper_yield(curing_data),
                        "lower_yield": lower_yield(curing_data),
                        "upper_lower_yield": upper_lower_yield(curing_data),
                        "static_yield": static_yield(curing_data),
                        "warehouse_yield": warehouse_yield(curing_data),
                        "visual_yield": visual_yield(curing_data)
                    })

                    param_spec_map = {
                        "con_kgf": spec_con_kgf,
                        "cwrfv_oa_kgf": spec_cwrfv_oa_kgf,
                        "ccwrfv_oa_kgf": spec_ccwrfv_oa_kgf,
                        "cwrfv_oa1h_kgf": spec_cwrfv_oa1h_kgf,
                        "ccwrfv_oa1h_kgf": spec_ccwrfv_oa1h_kgf,
                        "lfv_oa_kgf": spec_lfv_oa_kgf,
                        "lfv_oa_1_kgf": spec_lfv_oa_1_kgf,
                        "upper_g": spec_upper_g,
                        "lower_g": spec_lower_g,
                        "up_low_g": spec_up_low_g,
                        "static_g": spec_static,
                        "load1": test_load,
                        "airpress1": pressure,
                        "total_rank_yield": total_rank_yield_spec_yield,
                        "rfv_yield": rfv_yield_spec_yield,
                        "rfh1_yield": rfh1_yield_spec_yield,
                        "lfv_yield": lfv_yield_spec_yield,
                        "conicity_yield": conicity_yield_spec_yield,
                        "ufm_yield": ufm_yield_spec_yield,
                        "rorank_yield": rorank_yield_spec_yield,
                        "radial_runout_yield": radial_runout_yield_spec_yield,
                        "radial_runout_first_harmonic_yield": radial_runout_first_harmonic_yield_spec_yield,
                        "lateral_run_out_yield": lateral_run_out_yield_spec_yield,
                        "lateral_run_out_top_over_yield": lateral_run_out_top_over_yield_spec_yield,
                        "lateral_run_out_bottom_over_yield": lateral_run_out_bottom_over_yield_spec_yield,
                        "lateral_run_out_harmonic_yield": lateral_run_out_harmonic_yield_spec_yield,
                        "lateral_run_out_top_first_harmonic_yield": lateral_run_out_top_first_harmonic_yield_spec_yield,
                        "lateral_run_out_bottom_first_harmonic_yield": lateral_run_out_bottom_first_harmonic_yield_spec_yield,
                        "bulge_yield": bulge_yield_spec_yield,
                        "bulge_top_over_yield": bulge_top_over_yield_spec_yield,
                        "bulge_bottom_over_yield": bulge_bottom_over_yield_spec_yield,
                        "dent_yield": dent_yield_spec_yield,
                        "dent_top_over_yield": dent_top_over_yield_spec_yield,
                        "dent_bottom_over_yield": dent_bottom_over_yield_spec_yield,
                        "balancing_yield": balancing_yield_spec_yield,
                        "upper_yield": upper_yield_spec_yield,
                        "lower_yield": lower_yield_spec_yield,
                        "upper_lower_yield": upper_lower_yield_spec_yield,
                        "static_yield": static_yield_spec_yield,
                        "warehouse_yield": warehouse_yield_spec_yield,
                        "visual_yield": visual_yield_spec_yield
                    }

                    grouped_averages = {}
                    try:
                        curing_data["ubno"] = curing_data["ubno"].astype(str).str.strip()
                        curing_data.sort_values("date_and_time", ascending=True, inplace=True)
                        ubno_grouped = curing_data.groupby("ubno")
                        ubno_records = []
                        for ubno, group in ubno_grouped:
                            if group.empty:
                                output_messages.append(f"⚠️ Empty group for ubno {ubno} in machine {machine_no}")
                                continue
                            group.sort_values("date_and_time", ascending=True)
                            record = {
                                "ubno": ubno,
                                "barcode_count": len(group["barcode"]),
                                "tub_date_start_time": group["date_and_time"].iloc[0] if not group["date_and_time"].empty else None,
                                "tub_date_end_time": group["date_and_time"].iloc[-1] if not group["date_and_time"].empty else None,
                                "barcodes": group["barcode"].tolist() if "barcode" in group.columns else []
                            }
                            for param in conicity_params:
                                if param in group.columns:
                                    valid_vals = pd.to_numeric(group[param], errors='coerce').dropna()
                                    non_numeric = group[param][pd.to_numeric(group[param], errors='coerce').isna() & group[param].notna()]
                                    if not non_numeric.empty:
                                        output_messages.append(f"⚠️ Non-numeric values in {param} for ubno {ubno}: {non_numeric.tolist()[:5]}")
                                    record[f"avg_{param}"] = float(valid_vals.mean()) if not valid_vals.empty else None
                                else:
                                    record[f"avg_{param}"] = None
                                    output_messages.append(f"⚠️ Column {param} missing in ubno {ubno} for machine {machine_no}")
                            parameters = []
                            for param in conicity_params:
                                try:
                                    if param in group.columns:
                                        valid_vals = pd.to_numeric(group[param], errors='coerce').dropna()
                                        avg_val = float(valid_vals.mean()) if not valid_vals.empty else None
                                    else:
                                        avg_val = None
                                    record[f"avg_{param}"] = avg_val
                                    spec = param_spec_map.get(param)
                                    exceeded = False
                                    if param == "con_kgf" and spec_con_kgf is not None:
                                        exceeded = not (-spec_con_kgf <= avg_val <= spec_con_kgf) if avg_val is not None else False
                                    if param == "load1" and test_load is not None and test_load_tolerance is not None:
                                        exceeded = not (test_load - test_load_tolerance <= avg_val <= test_load + test_load_tolerance) if avg_val is not None else False
                                    if param == "airpress1" and pressure is not None and pressure_tolerance is not None:
                                        exceeded = not (pressure - pressure_tolerance <= avg_val <= pressure + pressure_tolerance) if avg_val is not None else False
                                    if spec is not None and param not in ["con_kgf", "load1", "airpress1"]:
                                        exceeded = avg_val > spec if avg_val is not None else False
                                    parameters.append({
                                        "parameter": param,
                                        "avg_value": avg_val,
                                        "spec_value": spec,
                                        "spec_threshold_exceeded": bool(exceeded)
                                    })
                                except Exception as e:
                                    logging.info(f"[ERROR] Processing numeric param {param}: {e}")
                                    record[f"avg_{param}"] = None
                                    parameters.append({
                                        "parameter": param,
                                        "avg_value": None,
                                        "spec_value": param_spec_map.get(param),
                                        "spec_threshold_exceeded": False
                                    })
                            for param in yield_params:
                                try:
                                    yield_func = globals()[param]
                                    avg_val = yield_func(group)
                                    record[param] = avg_val
                                    spec = param_spec_map.get(param)
                                    exceeded = avg_val < spec if spec is not None and avg_val is not None else False
                                    parameters.append({
                                        "parameter": param,
                                        "avg_value": avg_val,
                                        "spec_value": spec,
                                        "spec_threshold_exceeded": bool(exceeded)
                                    })
                                except Exception as e:
                                    record[param] = None
                                    parameters.append({
                                        "parameter": param,
                                        "avg_value": None,
                                        "spec_value": param_spec_map.get(param),
                                        "spec_threshold_exceeded": False
                                    })
                            record["parameter_list"] = parameters
                            ubno_records.append(record)
                        grouped_averages["ubno"] = ubno_records
                    except Exception as e:
                        grouped_averages["ubno"] = f"Error: {e}"
                        output_messages.append(f"⚠️ Error calculating ubno averages for model {model_number}, machine {machine_no}: {e}")

                    # Timing and shift data
                    curing_data1=curing_data.copy()
                    curing_data.sort_values("date_of_curing", ascending=True, inplace=True)
                    curing_data1.sort_values("date_and_time", ascending=True, inplace=True)
                    machine_lot_start_curing_time = curing_data["date_of_curing"].iloc[0] if not curing_data["date_of_curing"].empty else None
                    machine_lot_end_curing_time = curing_data["date_of_curing"].iloc[-1] if not curing_data["date_of_curing"].empty else None
                    tub_date_start_time = curing_data1["date_and_time"].iloc[0] if not curing_data1["date_and_time"].empty else None
                    tub_date_end_time = curing_data1["date_and_time"].iloc[-1] if not curing_data1["date_and_time"].empty else None
                    lot_start_time = some_function()
                    lot_end_time = some_function()

                    parameter_alerts = []
                    overall_shift_occurred = False
                    prev_key = (model_number, machine_no, curing_previous_machine_model_lot_id)
                    prev_lot_avg = machine_lot_avg_map.get(prev_key, None)
                    if prev_lot_avg is None and curing_previous_machine_model_lot_id:
                        previous_lot = averages_collect.find_one(
                            {"curing_machine_model_lot_id": curing_previous_machine_model_lot_id},
                            {"current_lot_averages": 1}
                        )
                        prev_lot_avg = previous_lot["current_lot_averages"] if previous_lot else None
                    if prev_lot_avg:
                        for param in conicity_params + yield_params:
                            shift = False
                            shift_occurred_spec = False
                            current_val = lot_averages.get(param)
                            prev_val = prev_lot_avg.get(param)
                            if current_val is not None and prev_val is not None:
                                if param == "con_kgf" and spec_con_kgf is not None:
                                    if abs(current_val - prev_val) > threshold_value_con_kgf:
                                        shift = True
                                    if not (-spec_con_kgf <= current_val <= spec_con_kgf):
                                        shift_occurred_spec = True
                                if param == "cwrfv_oa_kgf" and spec_cwrfv_oa_kgf is not None:
                                    if (current_val - prev_val) > threshold_value_cwrfv_oa_kgf:
                                        shift = True
                                    if current_val > spec_cwrfv_oa_kgf:
                                        shift_occurred_spec = True
                                if param == "ccwrfv_oa_kgf" and spec_ccwrfv_oa_kgf is not None:
                                    if (current_val - prev_val) > threshold_value_ccwrfv_oa_kgf:
                                        shift = True
                                    if current_val > spec_ccwrfv_oa_kgf:
                                        shift_occurred_spec = True
                                if param == "cwrfv_oa1h_kgf" and spec_cwrfv_oa1h_kgf is not None:
                                    if (current_val - prev_val) > threshold_value_cwrfv_oa1h_kgf:
                                        shift = True
                                    if current_val > spec_cwrfv_oa1h_kgf:
                                        shift_occurred_spec = True
                                if param == "ccwrfv_oa1h_kgf" and spec_ccwrfv_oa1h_kgf is not None:
                                    if (current_val - prev_val) > threshold_value_ccwrfv_oa1h_kgf:
                                        shift = True
                                    if current_val > spec_ccwrfv_oa1h_kgf:
                                        shift_occurred_spec = True
                                if param == "lfv_oa_kgf" and spec_lfv_oa_kgf is not None:
                                    if (current_val - prev_val) > threshold_value_lfv_oa_kgf:
                                        shift = True
                                    if current_val > spec_lfv_oa_kgf:
                                        shift_occurred_spec = True
                                if param == "lfv_oa_1_kgf" and spec_lfv_oa_1_kgf is not None:
                                    if (current_val - prev_val) > threshold_value_lfv_oa_1_kgf:
                                        shift = True
                                    if current_val > spec_lfv_oa_1_kgf:
                                        shift_occurred_spec = True
                                if param == "upper_g" and spec_upper_g is not None:
                                    if (current_val - prev_val) > threshold_value_bal_upper_g:
                                        shift = True
                                    if current_val > spec_upper_g:
                                        shift_occurred_spec = True
                                if param == "lower_g" and spec_lower_g is not None:
                                    if (current_val - prev_val) > threshold_value_bal_lower_g:
                                        shift = True
                                    if current_val > spec_lower_g:
                                        shift_occurred_spec = True
                                if param == "up_low_g" and spec_up_low_g is not None:
                                    if (current_val - prev_val) > threshold_value_bal_up_low_g:
                                        shift = True
                                    if current_val > spec_up_low_g:
                                        shift_occurred_spec = True
                                if param == "static_g" and spec_static is not None:
                                    if (current_val - prev_val) > threshold_value_bal_static_g:
                                        shift = True
                                    if current_val > spec_static:
                                        shift_occurred_spec = True
                                
                                ############### Add 16-07 start ######################

                                # if param in yield_params:
                                #     spec_yield = param_spec_map.get(param)
                                #     if spec_yield is not None:
                                #         if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield)):
                                #             shift = True
                                #         if abs(current_val < spec_yield):
                                #             shift_occurred_spec = True

                                ############### Add 16-07 end ######################

                                if param == "total_rank_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_total_rank_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                if param == "rfv_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_rfv_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "rfh1_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_rfh1_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "lfv_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_lfv_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "conicity_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_conicity_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "ufm_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_ufm_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "rorank_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_rorank_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "radial_runout_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_radial_runout_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "radial_runout_first_harmonic_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_radial_runout_first_harmonic_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "lateral_run_out_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_lateral_run_out_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "lateral_run_out_top_over_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_lateral_run_out_top_over_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "lateral_run_out_bottom_over_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_lateral_run_out_bottom_over_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "lateral_run_out_harmonic_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_lateral_run_out_harmonic_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "lateral_run_out_top_first_harmonic_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_lateral_run_out_top_first_harmonic_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "lateral_run_out_bottom_first_harmonic_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_lateral_run_out_bottom_first_harmonic_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "bulge_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_bulge_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "bulge_top_over_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_bulge_top_over_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "bulge_bottom_over_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_bulge_bottom_over_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "dent_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_dent_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "dent_top_over_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_dent_top_over_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "dent_bottom_over_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_dent_bottom_over_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "balancing_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_balancing_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "upper_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_upper_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "lower_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_lower_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "upper_lower_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_upper_lower_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "static_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_static_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "warehouse_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_warehouse_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "visual_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_visual_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True

                                if param == "load1" and test_load is not None and test_load_tolerance is not None:
                                    if (current_val - prev_val) > test_load_tolerance:
                                        shift = True
                                    if not (test_load - test_load_tolerance <= current_val <= test_load + test_load_tolerance):
                                        shift_occurred_spec = True
                                if param == "airpress1" and pressure is not None and pressure_tolerance is not None:
                                    if (current_val - prev_val) > pressure_tolerance:
                                        shift = True
                                    if not (pressure - pressure_tolerance <= current_val <= pressure + pressure_tolerance):
                                        shift_occurred_spec = True
                            shift_occurred = shift or shift_occurred_spec
                            spec_value = param_spec_map.get(param)
                            spec_diff = (current_val - spec_value if spec_value is not None and current_val is not None and isinstance(spec_value, (int, float)) and isinstance(current_val, (int, float)) else None)
                            alert_dict = {
                                "parameter": param,
                                "current_value": current_val,
                                "previous_value": prev_val,
                                "difference": current_val - prev_val if current_val is not None and prev_val is not None else None,
                                "shift_occurred_thershold": shift,
                                "spec_value": spec_value,
                                "spec_difference": spec_diff,
                                "shift_occurred_spec": shift_occurred_spec,
                                "shift_occurred": shift_occurred
                            }
                            if param == "load1":
                                alert_dict["test_load_tolerance"] = test_load_tolerance
                            if param == "airpress1":
                                alert_dict["pressure_tolerance"] = pressure_tolerance
                            parameter_alerts.append(alert_dict)
                            if shift_occurred:
                                overall_shift_occurred = True
                    else:
                        for param in conicity_params + yield_params:
                            shift = False
                            shift_occurred_thershold = False
                            shift_occurred_spec = False
                            current_val = lot_averages.get(param)
                            if current_val is not None:
                                if param == "con_kgf" and spec_con_kgf is not None:
                                    if not (-spec_con_kgf <= current_val <= spec_con_kgf):
                                        shift_occurred_spec = True
                                if param == "cwrfv_oa_kgf" and spec_cwrfv_oa_kgf is not None:
                                    if current_val > spec_cwrfv_oa_kgf:
                                        shift_occurred_spec = True
                                if param == "ccwrfv_oa_kgf" and spec_ccwrfv_oa_kgf is not None:
                                    if current_val > spec_ccwrfv_oa_kgf:
                                        shift_occurred_spec = True
                                if param == "cwrfv_oa1h_kgf" and spec_cwrfv_oa1h_kgf is not None:
                                    if current_val > spec_cwrfv_oa1h_kgf:
                                        shift_occurred_spec = True
                                if param == "ccwrfv_oa1h_kgf" and spec_ccwrfv_oa1h_kgf is not None:
                                    if current_val > spec_ccwrfv_oa1h_kgf:
                                        shift_occurred_spec = True
                                if param == "lfv_oa_kgf" and spec_lfv_oa_kgf is not None:
                                    if current_val > spec_lfv_oa_kgf:
                                        shift_occurred_spec = True
                                if param == "lfv_oa_1_kgf" and spec_lfv_oa_1_kgf is not None:
                                    if current_val > spec_lfv_oa_1_kgf:
                                        shift_occurred_spec = True
                                if param == "upper_g" and spec_upper_g is not None:
                                    if current_val > spec_upper_g:
                                        shift_occurred_spec = True
                                if param == "lower_g" and spec_lower_g is not None:
                                    if current_val > spec_lower_g:
                                        shift_occurred_spec = True
                                if param == "up_low_g" and spec_up_low_g is not None:
                                    if current_val > spec_up_low_g:
                                        shift_occurred_spec = True
                                if param == "static_g" and spec_static is not None:
                                    if current_val > spec_static:
                                        shift_occurred_spec = True
                            
                            ############### Add 16-07 start ######################

                                if param in yield_params:
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if current_val < spec_yield:
                                            shift_occurred_spec = True
                                if param == "load1" and test_load is not None and test_load_tolerance is not None:
                                    if not (test_load - test_load_tolerance <= current_val <= test_load + test_load_tolerance):
                                        shift_occurred_spec = True
                                if param == "airpress1" and pressure is not None and pressure_tolerance is not None:
                                    if not (pressure - pressure_tolerance <= current_val <= pressure + pressure_tolerance):
                                        shift_occurred_spec = True
                            shift_occurred = shift_occurred_thershold or shift_occurred_spec
                            spec_value = param_spec_map.get(param)
                            spec_diff = (current_val - spec_value if spec_value is not None and current_val is not None and isinstance(spec_value, (int, float)) and isinstance(current_val, (int, float)) else None)
                            alert_dict = {
                                "parameter": param,
                                "current_value": current_val,
                                "previous_value": None,
                                "difference": None,
                                "shift_occurred_thershold": shift_occurred_thershold,
                                "spec_value": spec_value,
                                "spec_difference": spec_diff,
                                "shift_occurred_spec": shift_occurred_spec,
                                "shift_occurred": shift_occurred
                            }
                            if param == "load1":
                                alert_dict["test_load_tolerance"] = test_load_tolerance
                            if param == "airpress1":
                                alert_dict["pressure_tolerance"] = pressure_tolerance
                            parameter_alerts.append(alert_dict)
                            if shift_occurred:
                                overall_shift_occurred = True

                    overall_shift_occurred = bool(overall_shift_occurred)

                    # NEW: Filter parameters for derived alerts
                    deviated_parameter_alerts = [
                        alert for alert in parameter_alerts
                        if alert.get("shift_occurred_thershold", False) or alert.get("shift_occurred_spec", False)
                    ]

                    # Add rank counts to deviated parameter alerts
                    for alert in deviated_parameter_alerts:
                        param = alert.get("parameter", "")
                        if param.endswith("_yield"):
                            if param in rank_param_mappings:
                                rank_counts = {}
                                for col in rank_param_mappings[param]:
                                    if col in curing_data.columns:
                                        value_counts = curing_data[col].value_counts().to_dict()
                                        for rank_value, count in value_counts.items():
                                            rank_value = int(rank_value) if isinstance(rank_value, (int, float)) else rank_value
                                            rank_counts[f"{col}_{rank_value}"] = int(count)
                                alert["rank_counts"] = rank_counts

                    if deviated_parameter_alerts:
                        deviated_doc = {
                            "alert_id_tbm_ufm": last_alert_id + 1,
                            "modelno": model_number,
                            "lot_id": unique_lot_id_current,
                            "machine": machine_no,
                            "comments": [],
                            "alert_assign_user": [],   # Add 05-07
                            "curing_machine_model_lot_id": current_curing_machine_model_lot_id,
                            "curing_previous_machine_model_lot_id": curing_previous_machine_model_lot_id,
                            "parameter_alerts": deviated_parameter_alerts,
                            "timestamp_alert_generated": some_function()
                        }
                        deviated_triggering_bulk_operations.append(pymongo.InsertOne(deviated_doc))
                        logging.info(f"Prepared {len(deviated_parameter_alerts)} derived alerts for machine {machine_no}, lot {unique_lot_id_current}")

                    # Do not filter; include all parameters (both yield and non-yield)
                    parameter_alerts = [
                    alert for alert in parameter_alerts
                    if alert.get("parameter", "").endswith("_yield") or not alert.get("parameter", "").endswith("_yield")
                    ]

                    # Add rank counts only for alerts:
                    # - where a shift occurred (threshold or spec)
                    # - and parameter name ends with '_yield'
                    for alert in parameter_alerts:
                        param = alert.get("parameter", "")
                        if (
                            (alert.get("shift_occurred_thershold", False) or alert.get("shift_occurred_spec", False))
                            and param.endswith("_yield")
                        ):
                            if param in rank_param_mappings:
                                rank_counts = {}
                                for col in rank_param_mappings[param]:
                                    if col in curing_data.columns:
                                        value_counts = curing_data[col].value_counts().to_dict()
                                        for rank_value, count in value_counts.items():
                                            rank_value = int(rank_value) if isinstance(rank_value, (int, float)) else rank_value
                                            rank_counts[f"{col}_{rank_value}"] = int(count)
                                alert["rank_counts"] = rank_counts

                    curing_data["curing_shift"] = curing_data["date_of_curing"].apply(get_shift)
                    shift_groups = curing_data.groupby('curing_shift')
                    shifts_data_list = []
                    for shift_name, shift_data in shift_groups:
                        shift_start_time = shift_data["date_of_curing"].iloc[0] if not shift_data["date_of_curing"].empty else None
                        shift_end_time = shift_data["date_of_curing"].iloc[-1] if not shift_data["date_of_curing"].empty else None
                        shifts_data_list.append({
                            "shifts": shift_name,
                            "barcode_count": len(shift_data["barcode"]),
                            "machine_shift_curing_start_time": shift_start_time,
                            "machine_shift_curing_end_time": shift_end_time
                        })

                    any_shift_flag = any(alert["shift_occurred"] for alert in parameter_alerts)

                    averages_doc = {
                        "modelno": model_number,
                        "lot_id": unique_lot_id_current,
                        "previous_lot_id": previous_lot_id,
                        "machine": machine_no,
                        "curing_machine_model_lot_id": current_curing_machine_model_lot_id,
                        "curing_previous_machine_model_lot_id": curing_previous_machine_model_lot_id,
                        "current_lot_averages": lot_averages,
                        "grouped_averages": grouped_averages,
                        "previous_lot_averages": prev_lot_avg,
                        "parameter_alerts": parameter_alerts,
                        "curing_shift_occurred": overall_shift_occurred,
                        "processed_machine_barcodes": int(total_curing_barcodes),
                        "batch_indices": list(curing_data.index),
                        "timestamp_alert_generated": some_function()
                    }
                    averages_query = {
                        "modelno": model_number,
                        "lot_id": unique_lot_id_current,
                        "curing_machine_model_lot_id": current_curing_machine_model_lot_id
                    }
                    if not averages_collect.find_one(averages_query):
                        averages_bulk_operations.append(pymongo.InsertOne(averages_doc))

                    current_key = (model_number, machine_no, current_curing_machine_model_lot_id)
                    machine_lot_avg_map[current_key] = lot_averages

                    machine_alert = {
                        # "machine": machine_no,
                        "machine": machine_no[1:] if machine_no.startswith('M') else machine_no,
                        "curing_shift_occurred": any_shift_flag,
                        "shifts_data": shifts_data_list,
                        "ufm_yield": ufm_yield(curing_data),
                        "rorank_yield": rorank_yield(curing_data),
                        "balancing_yield": balancing_yield(curing_data),
                        "total_rank_yield": total_rank_yield(curing_data),
                        "visual_yield": visual_yield(curing_data),
                        "sizecode": sizecode,
                        "gt_code": str(gt_code),     # Add 16-07
                        "timestamp_lot_started": lot_start_time,
                        "timestamp_lot_ended": lot_end_time,
                        "processed_machine_barcodes": int(total_curing_barcodes),
                        "machine_lot_start_curing_time": machine_lot_start_curing_time,
                        "machine_lot_end_curing_time": machine_lot_end_curing_time,
                        "tub_date_start_time": tub_date_start_time,
                        "tub_date_end_time": tub_date_end_time,
                        "parameter_alerts": parameter_alerts,
                        "lot_averages": lot_averages,
                        "grouped_averages": grouped_averages,
                        "previous_lot_averages": machine_lot_avg_map.get((model_number, machine_no, curing_previous_machine_model_lot_id), {}),
                        "first_barcode_machine": curing_data.iloc[0]["barcode"] if "barcode" in curing_data.columns else None,
                        "last_barcode_machine": curing_data.iloc[-1]["barcode"] if "barcode" in curing_data.columns else None,
                        "curing_machine_model_lot_id": current_curing_machine_model_lot_id,
                        "curing_previous_machine_model_lot_id": curing_previous_machine_model_lot_id,
                        "rank_columns": get_rank_columns(curing_data),
                        "barcodes": curing_data["barcode"].tolist()
                    }

                    alert_key = (model_number, unique_lot_id_current)
                    if alert_key not in alert_map:
                        last_alert_id += 1
                        alert_map[alert_key] = {
                            "alert_id_curing_ufm": last_alert_id,
                            "modelno": model_number,
                            "lot_id": unique_lot_id_current,
                            "machine_alerts": [],
                            "first_barcode_model": None,
                            "last_barcode_model": None
                        }
                    alert_map[alert_key]["machine_alerts"].append(machine_alert)
                    if machine_alert["curing_shift_occurred"]:
                        alert_map[alert_key]["shift_occurred"] = True
                    if alert_map[alert_key]["first_barcode_model"] is None:
                        alert_map[alert_key]["first_barcode_model"] = machine_alert["first_barcode_machine"]
                    alert_map[alert_key]["last_barcode_model"] = machine_alert["last_barcode_machine"]
                    if overall_shift_occurred:
                        merge_df.loc[curing_data.index, "alert_id_curing_ufm"] = last_alert_id

                    curing_previous_machine_model_lot_ids[machine_model_key] = current_curing_machine_model_lot_id
                    last_lot_dict[machine_model_key] = {
                        "max_lot_number": last_lot_number,
                        "last_curing_machine_model_lot_id": current_curing_machine_model_lot_id
                    }
                    
                for alert_key, alert_obj in alert_map.items():
                        final_alert = {
                            "alert_id_curing_ufm": alert_obj["alert_id_curing_ufm"],
                            "modelno": alert_obj["modelno"],
                            "lot_id": alert_obj["lot_id"],
                            "alert_flag": False,
                            "dashboard_flag": False,
                            "machine_flag": False,
                            "alert_status_check": "o",
                            "alert_status_timestamp": None,
                            "alert_status_comment": [],
                            "alert_assign_user": [],
                            "machine_alerts": alert_obj["machine_alerts"],
                            "first_barcode_model": alert_obj["first_barcode_model"],
                            "last_barcode_model": alert_obj["last_barcode_model"],
                            "timestamp_alert_generated": some_function()
                        }
                        alert_bulk_operations.append(pymongo.InsertOne(final_alert))

                        # FIXED LINE: This ensures alert goes into alert_db & alert_db_dashboard
                        triggering_bulk_operations.append(pymongo.InsertOne(final_alert))
                
                # === AFTER alert_map is built - INSERT VIRGIN & FINAL YIELD (UNCHANGED + NEW) ===
                for alert_key, alert_obj in alert_map.items():
                    # true_machine_alerts = [ma for ma in alert_obj["machine_alerts"] if ma.get("tbm_shift_occurred", False)]
                    true_machine_alerts = [ma for ma in alert_obj["machine_alerts"] if ma.get("curing_shift_occured", False) in [True, False]]  # Keep both True/False
                    if not true_machine_alerts:
                        continue

                    # Pre-compute barcode → row mapping
                    virgin_barcode_to_row = {row["barcode"]: row for _, row in virgin_data.iterrows()}
                    final_barcode_to_row = {row["barcode"]: row for _, row in final_data.iterrows()}

                    # === 1. VIRGIN YIELD: YOUR ORIGINAL CODE (100% UNCHANGED) ===
                    virgin_machine_alerts = []
                    virgin_all_barcodes = set()
                    for ma in true_machine_alerts:
                        original_barcodes = ma.get("barcodes", [])
                        if not original_barcodes:
                            continue
                        filtered_barcodes = []
                        for bc in original_barcodes:
                            bc_str = str(bc)
                            if bc_str in virgin_barcode_to_row and bc_str not in virgin_all_barcodes:
                                filtered_barcodes.append(bc_str)
                                virgin_all_barcodes.add(bc_str)
                        if filtered_barcodes:
                            virgin_ma = ma.copy()
                            virgin_ma["barcodes"] = filtered_barcodes
                            virgin_ma["barcode_count"] = len(filtered_barcodes)
                            virgin_machine_alerts.append(virgin_ma)

                    if virgin_machine_alerts and virgin_all_barcodes:
                        all_virgin_bcs = sorted(virgin_all_barcodes,
                            key=lambda x: virgin_data[virgin_data["barcode"] == x]["date_of_dt"].iloc[0])
                        doc_virgin = {
                            "alert_id_curing_ufm": alert_obj["alert_id_curing_ufm"],
                            "modelno": alert_obj["modelno"],
                            "lot_id": alert_obj["lot_id"],
                            "alert_flag": False,
                            "dashboard_flag": False,
                            "machine_flag": False,
                            "alert_status_check": "o",
                            "alert_status_timestamp": None,
                            "alert_status_comment": [],
                            "alert_assign_user": [],
                            "machine_alerts": virgin_machine_alerts,
                            "first_barcode_model": all_virgin_bcs[0] if all_virgin_bcs else None,
                            "last_barcode_model": all_virgin_bcs[-1] if all_virgin_bcs else None,
                            "timestamp_alert_generated": some_function(),
                            "yield_type": "virgin"
                        }
                        alert_triggering_virgin_yield_collect.insert_one(doc_virgin)  # UNCHANGED

                    # === 2. FINAL YIELD: YOUR ORIGINAL CODE (KEPT 100%) ===
                    final_machine_alerts = []
                    final_all_barcodes = set()
                    for ma in true_machine_alerts:
                        original_barcodes = ma.get("barcodes", [])
                        if not original_barcodes:
                            continue
                        filtered_barcodes = []
                        for bc in original_barcodes:
                            bc_str = str(bc)
                            if bc_str in final_barcode_to_row and bc_str not in final_all_barcodes:
                                filtered_barcodes.append(bc_str)
                                final_all_barcodes.add(bc_str)
                        if filtered_barcodes:
                            final_ma = ma.copy()
                            final_ma["barcodes"] = filtered_barcodes
                            final_ma["barcode_count"] = len(filtered_barcodes)
                            final_machine_alerts.append(final_ma)

                    if final_machine_alerts and final_all_barcodes:
                        all_final_bcs = sorted(final_all_barcodes,
                            key=lambda x: final_data[final_data["barcode"] == x]["date_of_dt"].iloc[0])

                        # === ORIGINAL INSERT (KEPT) ===
                        doc_final = {
                            "alert_id_curing_ufm": alert_obj["alert_id_curing_ufm"],
                            "modelno": alert_obj["modelno"],
                            "lot_id": alert_obj["lot_id"],
                            "alert_flag": False,
                            "dashboard_flag": False,
                            "machine_flag": False,
                            "alert_status_check": "o",
                            "alert_status_timestamp": None,
                            "alert_status_comment": [],
                            "alert_assign_user": [],
                            "machine_alerts": final_machine_alerts,
                            "first_barcode_model": all_final_bcs[0] if all_final_bcs else None,
                            "last_barcode_model": all_final_bcs[-1] if all_final_bcs else None,
                            "timestamp_alert_generated": some_function(),
                            "yield_type": "final"
                        }
                        alert_triggering_final_yield_collect.insert_one(doc_final)  # YOUR ORIGINAL - UNCHANGED

                        # === NEW: Insert into alert_table_final_yield (DEDICATED FINAL YIELD DB) ===
                        doc_final_only = doc_final.copy()
                        doc_final_only["curing_shift_occured"] = alert_obj.get("shift_occurred", False)
                        final_yield_only_bulk.append(pymongo.InsertOne(doc_final_only))

                        # === NEW: Extract Deviated Parameters from alert_db_final_yield (alert_triggering_final_yield_collect) ===
                        deviated_params = []

                        # Fetch the already-saved Final Yield alert document using alert_id_curing_ufm
                        existing_final_yield_doc = alert_triggering_final_yield_collect.find_one(
                            {"alert_id_curing_ufm": alert_obj["alert_id_curing_ufm"]},
                            {"machine_alerts.parameter_alerts": 1, "machine_alerts.barcodes": 1, "machine_alerts.machine": 1, "machine_alerts.curing_machine_model_lot_id": 1, "machine_alerts.curing_previous_machine_model_lot_id": 1}
                        )

                        if existing_final_yield_doc:
                            machine_alerts = existing_final_yield_doc.get("machine_alerts", [])
                            for ma in machine_alerts:
                                param_alerts = ma.get("parameter_alerts", [])
                                barcodes_in_ma = ma.get("barcodes", [])
                                machine_no_final = ma.get("machine", "unknown")
                                machine_current_curing_machine_model_lot_id = ma.get("curing_machine_model_lot_id", "unknoum")
                                machine_curing_previous_machine_model_lot_id = ma.get("curing_previous_machine_model_lot_id", None)

                                for alert in param_alerts:
                                    if alert.get("shift_occurred_thershold", False) or alert.get("shift_occurred_spec", False):
                                        param = alert.get("parameter", "")

                                        # Only add rank_counts for yield parameters
                                        if param.endswith("_yield") and param in rank_param_mappings and barcodes_in_ma:
                                            # Fetch actual tire data for these barcodes (last occurrence = Final Yield)
                                            tire_data = final_data[final_data["barcode"].isin(barcodes_in_ma)]
                                            rank_counts = {}

                                            for col in rank_param_mappings[param]:
                                                if col in tire_data.columns:
                                                    counts = tire_data[col].value_counts().to_dict()
                                                    for val, cnt in counts.items():
                                                        val = int(val) if isinstance(val, (int, float)) else val
                                                        rank_counts[f"{col}_{val}"] = int(cnt)
                                            alert["rank_counts"] = rank_counts

                                        deviated_params.append(alert)

                        # === Insert into alert_deviated_final_yield ===
                        if deviated_params:
                            deviated_doc = {
                                "alert_id_curing_ufm": alert_obj["alert_id_curing_ufm"],
                                "modelno": alert_obj["modelno"],
                                "lot_id": alert_obj["lot_id"],
                                "machine": machine_no_final if 'machine_no_final' in locals() else "unknown",
                                "comments": [],
                                "alert_assign_user": [],
                                "tbm_machine_model_lot_id": machine_current_curing_machine_model_lot_id,
                                "tbm_previous_machine_model_lot_id": machine_curing_previous_machine_model_lot_id,
                                "parameter_alerts": deviated_params,
                                "timestamp_alert_generated": some_function(),
                                "yield_type": "final",
                                "source": "alert_db_final_yield"  # Optional: for traceability
                            }
                            final_yield_deviated_only_bulk.append(pymongo.InsertOne(deviated_doc))

        except Exception as e:
            logging.error(f"Error in model loop: {e}")
            continue

    # === BULK WRITE NEW DATABASES ===
    if final_yield_only_bulk:
        try:
            result = alert_final_yield_collect.bulk_write(final_yield_only_bulk, ordered=False)
            inserted_counts["alert_table_final_yield"] = result.inserted_count
            output_messages.append(f"Inserted {result.inserted_count} docs → alert_table_final_yield.alert_curing_collect_name")
        except Exception as e:
            output_messages.append(f"Error alert_table_final_yield: {e}")

    if final_yield_deviated_only_bulk:
        try:
            result = alert_triggering_deviated_final_yield_collect.bulk_write(final_yield_deviated_only_bulk, ordered=False)
            inserted_counts["alert_deviated_final_yield"] = result.inserted_count
            output_messages.append(f"Inserted {result.inserted_count} deviated docs → alert_deviated_final_yield.alert_triggering_deviated_curing")
        except Exception as e:
            output_messages.append(f"Error alert_deviated_final_yield: {e}")

    # === BULK WRITES (now triggering_bulk_operations is populated correctly) ===
    if triggering_bulk_operations:
        try:
            result = alert_triggering_collect.bulk_write(triggering_bulk_operations, ordered=False)
            inserted_counts["alert_triggering_curing_without_lot_size"] = result.inserted_count
            output_messages.append(f"Inserted {result.inserted_count} documents into alert_db.alert_triggering_curing")

            result_dashboard = alert_triggering_collect_dashboard_curing.bulk_write(triggering_bulk_operations, ordered=False)
            inserted_counts["alert_triggering_curing_dashboard"] = result_dashboard.inserted_count
            output_messages.append(f"Inserted {result_dashboard.inserted_count} documents into alert_db_dashboard")
        except Exception as e:
            error_message = f"Error writing to alert_db/alert_db_dashboard: {e}"
            output_messages.append(error_message)
            
    if deviated_triggering_bulk_operations:
        try:
            result = alert_triggering_deviated_collect.bulk_write(deviated_triggering_bulk_operations, ordered=False)
            inserted_counts["deviated_all"] = result.inserted_count
        except Exception as e:
            output_messages.append(f"Error deviated alerts: {e}")

    if bulk_operations:
        try:
            result = lot_curing_collect.bulk_write(bulk_operations, ordered=False)
            inserted_counts["lot_curing_table"] = result.inserted_count
            output_messages.append(f"✅ Curing Inserted {result.inserted_count} documents into lot_curing_table.")
        except Exception as e:
            error_message = f"❌ Curing Error performing bulk write to lot_curing_table: {e}"
            output_messages.append(error_message)

    if alert_bulk_operations:
        try:
            result = alert_collect.bulk_write(alert_bulk_operations, ordered=False)
            inserted_counts["alert_curing_table_without_lot_size"] = result.inserted_count
            output_messages.append(f"✅ Curing Inserted {result.inserted_count} alert documents into alert_curing_table_without_lot_size.")
        except Exception as e:
            error_message = f"❌ Curing Error performing bulk write to alert_curing_table_without_lot_size: {e}"
            output_messages.append(error_message)
    else:
        output_messages.append("No Curing alert documents to insert into alert_curing_table_without_lot_size.")

    if averages_bulk_operations:
        try:
            result = averages_collect.bulk_write(averages_bulk_operations, ordered=False)
            inserted_counts["averages_curing_table"] = result.inserted_count
            output_messages.append(f"✅ Curing Inserted {result.inserted_count} new averages documents into averages_curing_table.")
        except Exception as e:
            error_message = f"❌ Curing Error performing bulk write to averages_curing_table: {e}"
            output_messages.append(error_message)
    else:
        output_messages.append("No Curing averages documents to insert into averages_curing_table.")

    # if triggering_bulk_operations:
    #     try:
    #         result = alert_triggering_collect.bulk_write(triggering_bulk_operations, ordered=False)
    #         inserted_counts["alert_triggering_curing_without_lot_size"] = result.inserted_count
    #         output_messages.append(f"✅ Curing Inserted {result.inserted_count} machine_alert documents into alert_triggering_curing_without_lot_size.")
    #         # NEW: Write to alert_db_dashboard
    #         result_dashboard = alert_triggering_collect_dashboard_curing.bulk_write(triggering_bulk_operations, ordered=False)
    #         inserted_counts["alert_triggering_curing_dashboard"] = result_dashboard.inserted_count
    #         output_messages.append(f"✅ TBM Inserted {result_dashboard.inserted_count} machine_alert documents into alert_db_dashboard.alert_triggering_curing_without_lot_size.")
    #     except Exception as e:
    #         error_message = f"❌ Curing Error performing bulk write to alert_triggering_curing_without_lot_size collection: {e}"
    #         output_messages.append(error_message)
    # else:
    #     output_messages.append("No Curing machine_alert documents to insert into alert_triggering_curing_without_lot_size collection.")

    # NEW: Execute bulk write for deviated triggering collection
    # if deviated_triggering_bulk_operations:
    #     try:
    #         result = alert_triggering_deviated_collect.bulk_write(deviated_triggering_bulk_operations, ordered=False)
    #         inserted_counts["alert_curing_without_lot_size"] = result.inserted_count
    #         output_messages.append(f"✅ TBM Inserted {result.inserted_count} deviated alert documents into alert_deviated.alert_curing_without_lot_size.")
    #     except Exception as e:
    #         error_message = f"❌ TBM Error performing bulk write to alert_deviated.alert_curing_without_lot_size: {e}"
    #         output_messages.append(error_message)
    # else:
    #     output_messages.append("No TBM deviated alert documents to insert into alert_deviated.alert_curing_without_lot_size.")

    bulk_updates = []
    for index, row in merge_df.iterrows():
        query = {"_id": row["_id"]}
        update = {"$set": {
            "alert_id_curing_ufm": row.get("alert_id_curing_ufm"),
            "lot_id": row.get("lot_id"),
            "previous_lot_id": row.get("previous_lot_id"),
            "curing_machine_model_lot_id": row.get("curing_machine_model_lot_id"),
            "curing_previous_machine_model_lot_id": row.get("curing_previous_machine_model_lot_id")
        }}
        bulk_updates.append(pymongo.UpdateOne(query, update))

    if bulk_updates:
        try:
            result = merge_collect.bulk_write(bulk_updates, ordered=False)
            updated_counts["data_mereged_dashboard"] = result.modified_count
            output_messages.append(f"✅ Curing Updated {result.modified_count} documents in data_mereged_dashboard collection.")
        except Exception as e:
            error_message = f"❌ Curing Error updating data_mereged_dashboard collection: {e}"
            output_messages.append(error_message)
    else:
        output_messages.append("No Curing documents to update in data_mereged_dashboard collection.")

    end_time = time.time()
    execution_time = end_time - start_time
    final_message = f"✅ Curing Process completed successfully in {execution_time:.2f} seconds!"
    output_messages.append(final_message)

    logging.info("process_curing_data_and_generate_lots_alerts_without_lot_size function is completed")
    return {
        "status": "success",
        "message": final_message,
        "output_log": output_messages,
        "inserted_counts": inserted_counts,
        "updated_counts": updated_counts,
        "execution_time": execution_time
    }, 200

def process_ufm_lot_and_alert_data_without_lot_size():
    process_start_time = time.time()
    output_messages = []
    inserted_counts = {}
    updated_counts = {}
    logging.info("process_ufm_lot_and_alert_data_without_lot_size function is just started")
    try:
        # UFM Specific Collections
        merge_collect = client[TUB_CIMCORP_DB][new_tub_collection_collection]
        channel_collect = client[config_databse][channel_no_collect]
        machine_collect = client[config_databse][machine_no_collect]
        lot_ufm_collect = client[lot_db][lot_ufm_collect_name_new]
        alert_collect = client[alert_table][alert_ufm_collect_name_new]
        averages_ufm_collect = client[average_db][averages_ufm_table_new]
        ub_machine_collect = client[config_databse][ub_machine_no_collect]
        alert_ufm_collect = client[alert_db][alert_triggering_ubno_collect_name_new]
        alert_triggering_deviated_collect = client[alert_deviated][alert_triggering_deviated_ufm_new]
        alert_triggering_collect_dashboard_ufm = client[alert_db_dashboard][alert_triggering_ubno_collect_name_new]
        # === NEW: Virgin Yield & Final Yield Collections ===
        alert_triggering_virgin_yield_collect = client[alert_db_virgin_yield][alert_triggering_ubno_collect_name_new]
        alert_triggering_final_yield_collect = client[alert_db_final_yield][alert_triggering_ubno_collect_name_new]
        # === NEW: YOUR DEDICATED FINAL YIELD DATABASES (EXACT REQUEST) ===
        alert_final_yield_collect = client["alert_table_final_yield"][alert_ufm_collect_name_new]  # All Final Yield alerts
        alert_deviated_final_yield_collect = client["alert_deviated_final_yield"][alert_triggering_deviated_ufm_new]  # Only Final Yield + Shift True
        # NEW: Initialize collections for model_workcenter_db
        ufm_collect_data = client[model_db][ufm_collection]
        ufm_machine_collect = client[model_db][ufm_collection_machine]
        logging.info("✅ UFM Collections loaded successfully for alert_db and alert_db_dashboard.")
        output_messages.append("✅ UFM Collections loaded successfully for lot and alert processing in alert_db and alert_db_dashboard.")
    except pymongo.errors.OperationFailure as e:
        error_message = f"❌ UFM Database or collection error during processing: {e}"
        logging.error(error_message)
        output_messages.append(error_message)
        return {"status": "error", "message": error_message, "output_log": output_messages}, 500

    # Fetch active machines from model_workcenter_db.ufm_machine
    try:
        active_machines = set(ufm_machine_collect.distinct("ubno", {"status": "active"}))
        output_messages.append(f"✅ Fetched {len(active_machines)} active machines: {active_machines}")
        if not active_machines:
            output_messages.append("❌ No active machines found in model_workcenter_db.ufm_machine. Exiting.")
            return {"status": "error", "message": "No active machines found", "output_log": output_messages}, 500
    except Exception as e:
        output_messages.append(f"❌ Error fetching active machines: {e}")
        return {"status": "error", "message": "Failed to fetch active machines", "output_log": output_messages}, 500


    # Fetch active model numbers
    try:
        active_modelnos = set(ufm_collect_data.distinct("modelno", {"status": "active"}))
        output_messages.append(f"✅ Fetched {len(active_modelnos)} active model numbers: {active_modelnos}")
    except Exception as e:
        output_messages.append(f"❌ Error fetching active model numbers: {e}")
        return {"status": "error", "message": "Failed to fetch active model numbers", "output_log": output_messages}, 500

    # Create unique indexes
    try:
        lot_ufm_collect.create_index(
            [
                ("modelno", pymongo.ASCENDING),
                ("machine", pymongo.ASCENDING),
                ("lot_id", pymongo.ASCENDING),
                ("ufm_machine_model_lot_id", pymongo.ASCENDING)
            ],
            unique=True,
            name="ufm_lot_unique_idx"
        )
        alert_ufm_collect.create_index(
            [
                ("modelno", pymongo.ASCENDING),
                ("lot_id", pymongo.ASCENDING),
                ("alert_id_ufm_ufm", pymongo.ASCENDING)
            ],
            unique=True,
            name="ufm_alert_unique_idx"
        )
        logging.info("✅ UFM Indexes checked/created.")
        output_messages.append("✅ UFM Indexes checked/created.")
    except Exception as e:
        logging.warning("⚠️ Warning: Unable to create/check UFM unique indexes. %s", e)
        output_messages.append(f"⚠️ Warning: Unable to create/check UFM unique indexes: {e}")

    # Load data
    try:
        merge1 = list(merge_collect.find({}))
        if not merge1:
            logging.info("ℹ️ No documents found in the merge collection. Exiting UFM processing.")
            output_messages.append("ℹ️ No documents found in the merge collection.")
            return {"status": "success", "message": "No data to process.", "output_log": output_messages}, 200
        merge_df = pd.DataFrame(merge1)
        channel_data = list(channel_collect.find({"modelno": {"$in": list(active_modelnos)}}))
        channel_data = list(channel_collect.find({}))
        channel_df = pd.DataFrame(channel_data)
        machine_data = list(machine_collect.find({}))
        machine_df = pd.DataFrame(machine_data)
        ub_machine_data = list(ub_machine_collect.find({"ubno": {"$in": list(active_machines)}}))
        ub_machine_data = list(ub_machine_collect.find({}))
        ub_machine_df = pd.DataFrame(ub_machine_data)

        logging.info("✅ UFM Data loaded into DataFrames.")
        output_messages.append("✅ UFM Data loaded into DataFrames.")
    except Exception as e:
        error_message = f"❌ UFM Error loading data: {e}"
        logging.error(error_message)
        output_messages.append(error_message)
        return {"status": "error", "message": error_message, "output_log": output_messages}, 500
    
    # MODIFIED: Check for cimcorp_tbm_tub column before adding tbm_machine column
    if 'cimcorp_tbm_tub' in merge_df.columns and 'tbm' in machine_df.columns and 'machine' in machine_df.columns:
        try:
            merge_df["tbm_machine"] = None  # Initialize tbm_machine column
            for index, row in merge_df.iterrows():
                tbm_value = row.get("cimcorp_tbm_tub")
                if tbm_value:
                    matching_channel = machine_df[machine_df["tbm"] == tbm_value]
                    if not matching_channel.empty:
                        merge_df.at[index, "tbm_machine"] = matching_channel["machine"].iloc[0]  # NEW: Use machine column
            output_messages.append("✅ Added tbm_machine column to merge_df based on matching cimcorp_tbm_tub values.")
        except Exception as e:
            output_messages.append(f"⚠️ Warning: Failed to add tbm_machine column to merge_df: {e}. Continuing without tbm_machine.")
    else:
        output_messages.append("⚠️ Warning: 'cimcorp_tbm_tub' column not found in merge_df or machine_df. Skipping tbm_machine column creation.")

    # MODIFIED: Check for cimcorp_tbm_tub column before updating machine_df
    if 'tbm' in machine_df.columns and 'cimcorp_tbm_tub' in merge_df.columns and 'tbm_machine' in merge_df.columns:
        try:
            for index, row in machine_df.iterrows():
                tbm_value = row.get("tbm")
                if tbm_value:
                    matching_merge = merge_df[merge_df["cimcorp_tbm_tub"] == tbm_value]
                    if not matching_merge.empty and "tbm_machine" in matching_merge.columns:
                        machine_df.at[index, "machine"] = matching_merge["tbm_machine"].iloc[0]
            output_messages.append("✅ Updated machine_df machine column with tbm_machine values from merge_df.")
        except Exception as e:
            output_messages.append(f"⚠️ Warning: Failed to update machine_df machine column: {e}. Continuing without updating channel_df machine.")
    else:
        output_messages.append("⚠️ Warning: 'cimcorp_tbm_tub' column not found in merge_df or machine_df. Skipping machine_df machine column update.")

    # Filter data
    try:
        channel_modelnos = set(channel_collect.distinct("modelno"))
        merge_df = merge_df[merge_df["modelno"].isin(channel_modelnos)]
        logging.info("Filtered merge_df for models in channel config. Rows remaining: %s", len(merge_df))
        output_messages.append(f"Filtered merge_df for models in channel config. Rows remaining: {len(merge_df)}")

        merge_df = merge_df[merge_df["alert_id_ufm_ufm"].isna()]
        logging.info("Filtered merge_df for unassigned alert_id_ufm_ufm. Rows remaining: %s", len(merge_df))
        output_messages.append(f"Filtered merge_df for unassigned alert_id_ufm_ufm. Rows remaining: {len(merge_df)}")

        merge_df = merge_df[merge_df["ufm_machine_model_lot_id"].isna()]
        logging.info("Filtered merge_df for unassigned ufm_machine_model_lot_id. Rows remaining: %s", len(merge_df))
        output_messages.append(f"Filtered merge_df for unassigned ufm_machine_model_lot_id. Rows remaining: {len(merge_df)}")

        if merge_df.empty:
            logging.info("No data required lot for processing UFM.")
            output_messages.append("No valid data after filtering for processing UFM.")
            return {"status": "success", "message": "No new data to process UFM.", "output_log": output_messages}, "no data", 200

    except KeyError as e:
        error_message = f"Failed to filter UFM: Error - DataFrame issue: Column missing {e}. Ensure 'alert_id_ufm_ufm' and 'ufm_machine_model_lot_id' are present in '{new_tub_collection_collection}'."
        logging.error(error_message)
        output_messages.append(error_message)
        return {"status": "error", "message": error_message, "output_log": output_messages}, 500
    except Exception as e:
        error_message = f"Failed to filter UFM data: {e}"
        logging.error(error_message)
        output_messages.append(error_message)
        return {"status": "error", "message": error_message, "output_log": output_messages}, 500

    # Date handling & Preprocessing
    try:
        merge_df["date_of_ufm"] = pd.to_datetime(merge_df["date_and_time"], errors="coerce").dt.strftime('%Y-%m-%d %H:%M:%S').fillna("No Date")
        merge_df["date_of_tbm"] = merge_df["tbm_buildtime_tub"]

        def convert_datetime_with_invalid_mask(series, col_name):
            try:
                original_series = series.copy()
                cleaned_series = original_series.astype(str).str.strip().str.replace(r'\s+', ' ', regex=True)
                datetime_series = pd.to_datetime(cleaned_series, format='%Y-%m-%d %H:%M:%S', errors='coerce')
                invalid_mask = datetime_series.isna() & original_series.notna()
                datetime_series[invalid_mask] = DEFAULT_DATETIME
                return datetime_series.dt.strftime('%Y-%m-%d %H:%M:%S')
            except Exception as e:
                logging.error(f"Error in convert_datetime_with_invalid_mask for {col_name}: {e}")
                return pd.Series([DEFAULT_DATETIME.strftime('%Y-%m-%d %H:%M:%S')] * len(series))

        merge_df["date_of_ufm"] = convert_datetime_with_invalid_mask(merge_df["date_of_ufm"], "date_of_ufm")
        merge_df["date_of_ufm"] = merge_df["date_of_ufm"].astype(object).where(merge_df["date_of_ufm"].notnull(), "No Date")
        merge_df["date_of_tbm"] = convert_datetime_with_invalid_mask(merge_df["date_of_tbm"], "date_of_tbm")
        merge_df["date_of_tbm"] = merge_df["date_of_tbm"].astype(object).where(merge_df["date_of_tbm"].notnull(), "No Date")

        merge_df["tbm_shift"] = merge_df["date_of_tbm"].apply(get_shift)
        merge_df["shift"] = merge_df["date_of_ufm"].apply(get_shift)


        merge_df["ubno"] = merge_df["ubno"].astype(str)
        ub_machine_df["ubno"] = ub_machine_df["ubno"].astype(str)
        # NEW: Filter merge_df to include only active machines
        merge_df = merge_df[merge_df["ubno"].isin(active_machines)]
        if merge_df.empty:
            output_messages.append("❌ No data in merge_df after filtering for active machines. Exiting.")
            return {"status": "error", "message": "No data after active machine filtering", "output_log": output_messages}, 500

        for col in ["lot_id", "previous_lot_id", "ufm_machine_model_lot_id", "ufm_previous_machine_model_lot_id", "alert_id_ufm_ufm"]:
            if col not in merge_df.columns:
                merge_df[col] = None

        logging.info("✅ UFM Date conversion and preprocessing completed.")
        output_messages.append("✅ UFM Date conversion and preprocessing completed.")
    except Exception as e:
        error_message = f"❌ UFM Error during date/preprocessing: {e}"
        logging.error(error_message)
        output_messages.append(error_message)
        return {"status": "error", "message": error_message, "output_log": output_messages}, 500

    # Fetch last alert ID
    try:
        last_alert_doc = alert_ufm_collect.find_one(sort=[("alert_id_ufm_ufm", -1)])
        last_alert_id = last_alert_doc["alert_id_ufm_ufm"] if last_alert_doc and "alert_id_ufm_ufm" in last_alert_doc else 0
        logging.info("ℹ️ Last UFM Alert ID found: %s", last_alert_id)
        output_messages.append(f"ℹ️ Last UFM Alert ID found: {last_alert_id}")
    except Exception as e:
        logging.warning("⚠️ UFM Warning fetching last alert ID: %s. Starting from 0.", e)
        output_messages.append(f"⚠️ UFM Warning fetching last alert ID: {e}. Starting from 0.")
        last_alert_id = 0

    # Fetch last lot numbers per machine-model combination
    try:
        last_lots_agg = list(lot_ufm_collect.aggregate([
            {"$match": {"lot_id": {"$regex": "^UFM_[^_]+_[^_]+_\\d+$"},
                        "modelno": {"$in": list(active_modelnos)},
                        "machine": {"$in": list(active_machines)}}},
            {"$addFields": {
                "lot_number": {
                    "$toInt": {"$arrayElemAt": [{"$split": ["$lot_id", "_"]}, -1]}
                },
                "modelno": "$modelno",
                "machine": "$machine"
            }},
            {"$sort": {"lot_number": -1}},
            {"$group": {
                "_id": {"modelno": "$modelno", "machine": "$machine"},
                "max_lot_number": {"$first": "$lot_number"},
                "last_lot_id": {"$first": "$lot_id"}
            }}
        ]))
        last_lot_dict = {
            (item["_id"]["modelno"], item["_id"]["machine"]): {
                "max_lot_number": item["max_lot_number"],
                "last_lot_id": item["last_lot_id"]
            } for item in last_lots_agg
        }
        # logging.info("ℹ️ Last UFM Lot numbers fetched: %s", last_lot_dict)
        output_messages.append(f"ℹ️ Last UFM Lot numbers fetched: {len(last_lot_dict)} entries")
    except Exception as e:
        logging.warning("⚠️ UFM Error fetching last lot numbers: %s. Starting counts from 0.", e)
        output_messages.append(f"⚠️ UFM Error fetching last lot numbers: {e}. Starting counts from 0.")
        last_lot_dict = {}

    # Initialize tracking variables
    ufm_previous_machine_model_lot_ids = {}
    ufm_machine_lot_avg_map = {}
    ufm_alert_bulk_operations = []
    ufm_averages_bulk_operations = []
    ufm_lot_bulk_operations = []
    deviated_triggering_bulk_operations = []
    ufm_bulk_operations = []
    ufm_triggering_bulk_operations = []

    # Pre-populate ufm_previous_machine_model_lot_ids and ufm_machine_lot_avg_map
    try:
        last_averages = list(averages_ufm_collect.aggregate([
            {
                "$match": {
                    "ufm_machine_model_lot_id": {"$regex": "^[^_]+_[^_]+_UFM_[^_]+_[^_]+_\\d+_\\d+$"},
                    "modelno": {"$in": list(active_modelnos)},
                    "machine": {"$in": list(active_machines)}
                }
            },
            {
                "$addFields": {
                    "lot_number": {
                        "$toInt": {"$arrayElemAt": [{"$split": ["$lot_id", "_"]}, -1]}
                    }
                }
            },
            {
                "$sort": {"lot_number": -1}
            },
            {
                "$group": {
                    "_id": {"modelno": "$modelno", "machine": "$machine"},
                    "last_ufm_lot_id": {"$first": "$ufm_machine_model_lot_id"},
                    "last_averages": {"$first": "$current_lot_averages"}
                }
            }
        ]))
        prev_cum_dict = {}
        for item in last_averages:
            modelno = item["_id"]["modelno"]
            machine = item["_id"]["machine"]
            last_ufm_lot_id = item["last_ufm_lot_id"]
            averages = item["last_averages"]
            ufm_previous_machine_model_lot_ids[(modelno, machine)] = last_ufm_lot_id
            if last_ufm_lot_id:
                try:
                    cum_str = last_ufm_lot_id.split("_")[-1]
                    prev_cum = int(cum_str)
                except ValueError:
                    prev_cum = 0
            else:
                prev_cum = 0
            prev_cum_dict[(modelno, machine)] = prev_cum
            if averages:
                ufm_machine_lot_avg_map[(modelno, machine, last_ufm_lot_id)] = averages
                logging.info(f"Pre-populated ufm_machine_lot_avg_map with averages for {last_ufm_lot_id}")
            else:
                logging.warning(f"No averages found for {last_ufm_lot_id} in averages_ufm_collect")
        # logging.info(f"Initialized ufm_previous_machine_model_lot_ids: {ufm_previous_machine_model_lot_ids}")
        output_messages.append(f"Initialized ufm_previous_machine_model_lot_ids with {len(ufm_previous_machine_model_lot_ids)} entries")
    except Exception as e:
        logging.error(f"Error pre-populating ufm_machine_lot_avg_map and ufm_previous_machine_model_lot_ids: {e}")
        output_messages.append(f"❌ Error pre-populating ufm_machine_lot_avg_map and ufm_previous_machine_model_lot_ids: {e}")

    ufm_params = [
        "con_kgf", "cwrfv_oa_kgf", "ccwrfv_oa_kgf", "cwrfv_oa1h_kgf", "ccwrfv_oa1h_kgf",
        "lfv_oa_kgf", "lfv_oa_1_kgf", "upper_g", "lower_g", "up_low_g", "static_g",
        "load1", "airpress1"
    ]
    yield_params = [
        "total_rank_yield", "rfv_yield", "rfh1_yield", "lfv_yield", "conicity_yield",
        "ufm_yield", "rorank_yield", "radial_runout_yield", "radial_runout_first_harmonic_yield",
        "lateral_run_out_yield", "lateral_run_out_top_over_yield", "lateral_run_out_bottom_over_yield",
        "lateral_run_out_harmonic_yield", "lateral_run_out_top_first_harmonic_yield",
        "lateral_run_out_bottom_first_harmonic_yield", "bulge_yield", "bulge_top_over_yield",
        "bulge_bottom_over_yield", "dent_yield", "dent_top_over_yield", "dent_bottom_over_yield",
        "balancing_yield", "upper_yield", "lower_yield", "upper_lower_yield", "static_yield",
        "warehouse_yield", "visual_yield"
    ]

    rank_param_mappings = {
        "total_rank_yield": ["total_rank"],
        "rfv_yield": ["cwrfv_oa_rank", "ccwrfv_oa_rank"],
        "rfh1_yield": ["cwrfv_oa1h_rank", "ccwrfv_oa1h_rank"],
        "lfv_yield": ["lfv_oa_rank", "lfv_oa_1_rank"],
        "conicity_yield": ["con_rank"],
        "ufm_yield": ["ufm_rank"],
        "rorank_yield": ["rorank"],
        "radial_runout_yield": ["rc_oa_rank"],
        "radial_runout_first_harmonic_yield": ["rc_oa1h_rank"],
        "lateral_run_out_top_over_yield": ["lt_oa_rank"],
        "lateral_run_out_bottom_over_yield": ["lb_oa_rank"],
        "lateral_run_out_top_first_harmonic_yield": ["lt_oa1h_rank"],
        "lateral_run_out_bottom_first_harmonic_yield": ["lb_oa1h_rank"],
        "bulge_top_over_yield": ["ltbulg_rank"],
        "bulge_bottom_over_yield": ["lbbulg_rank"],
        "dent_top_over_yield": ["dent_rank"],
        "dent_bottom_over_yield": ["dent1_rank"],
        "balancing_yield": ["bal_rank"],
        "upper_yield": ["upper_rank"],
        "lower_yield": ["lower_rank"],
        "upper_lower_yield": ["up_low_rank"],
        "static_yield": ["static_rank"],
        "lateral_run_out_yield": ["lt_oa_rank", "lb_oa_rank"],
        "lateral_run_out_harmonic_yield": ["lt_oa1h_rank", "lb_oa1h_rank"],
        "bulge_yield": ["ltbulg_rank", "lbbulg_rank"],
        "dent_yield": ["dent_rank", "dent1_rank"]
    }

    for _, channel_row_data in channel_df.iterrows():
        try:
            ############# Add 16-07 start ##############
            model_number = str(channel_row_data.get("modelno", "")).strip()
            if model_number not in active_modelnos:
                output_messages.append(f"Skipping CURING model {model_number} as it is not active.")
                continue
            gt_codes = channel_row_data.get("gt_code", [])

            # Ensure gt_codes is a list
            if not isinstance(gt_codes, list):
                gt_codes = [gt_codes]

            for gt_code in gt_codes:
                gt_code = str(gt_code).strip()

                # MODIFIED: Filter merge_df for each gt_code individually
                model_data = merge_df[
                    (merge_df["modelno"] == model_number) &
                    (merge_df["tbm_gtcode_tub"] == gt_code) &
                    (merge_df["ubno"].isin(active_machines))
                ].copy()
            ############# Add 16-07 end ##############

                spec_con_kgf = float(channel_row_data.get("spec_con_kgf", 0))
                sizecode = str(channel_row_data.get("sizecode", ""))
                # gt_code = str(row.get("gt_code", ""))
                # spec_rfv = float(row.get("spec_rfv", 0))
                spec_cwrfv_oa_kgf = float(channel_row_data.get("spec_cwrfv_oa_kgf", 0))
                spec_ccwrfv_oa_kgf = float(channel_row_data.get("spec_ccwrfv_oa_kgf", 0))
                # spec_rfh1 = float(row.get("spec_rfh1", 0))
                spec_cwrfv_oa1h_kgf = float(channel_row_data.get("spec_cwrfv_oa1h_kgf", 0))
                spec_ccwrfv_oa1h_kgf = float(channel_row_data.get("spec_ccwrfv_oa1h_kgf", 0))
                # spec_lfv = float(row.get("spec_lfv", 0))
                spec_lfv_oa_kgf = float(channel_row_data.get("spec_lfv_oa_kgf", 0))
                spec_lfv_oa_1_kgf = float(channel_row_data.get("spec_lfv_oa_1_kgf", 0))
                spec_static = float(channel_row_data.get("spec_static", 0))
                spec_upper_g = float(channel_row_data.get("spec_upper_g", 0))
                spec_lower_g = float(channel_row_data.get("spec_lower_g", 0))
                spec_up_low_g = float(channel_row_data.get("spec_up_low_g", 0))
                test_load = float(channel_row_data.get("test_load", 0))
                pressure = float(channel_row_data.get("pressure", 0))
                # spec_yield = float(row.get("spec_yield", 0))
                total_rank_yield_spec_yield = float(channel_row_data.get("total_rank_yield_spec_yield", 0))
                rfv_yield_spec_yield = float(channel_row_data.get("rfv_yield_spec_yield", 0))
                rfh1_yield_spec_yield = float(channel_row_data.get("rfh1_yield_spec_yield", 0))
                lfv_yield_spec_yield = float(channel_row_data.get("lfv_yield_spec_yield", 0))
                conicity_yield_spec_yield = float(channel_row_data.get("conicity_yield_spec_yield", 0))
                ufm_yield_spec_yield = float(channel_row_data.get("ufm_yield_spec_yield", 0))
                rorank_yield_spec_yield = float(channel_row_data.get("rorank_yield_spec_yield", 0))
                radial_runout_yield_spec_yield = float(channel_row_data.get("radial_runout_yield_spec_yield", 0))
                radial_runout_first_harmonic_yield_spec_yield = float(channel_row_data.get("radial_runout_first_harmonic_yield_spec_yield", 0))
                lateral_run_out_yield_spec_yield = float(channel_row_data.get("lateral_run_out_yield_spec_yield", 0))
                lateral_run_out_top_over_yield_spec_yield = float(channel_row_data.get("lateral_run_out_top_over_yield_spec_yield", 0))
                lateral_run_out_bottom_over_yield_spec_yield = float(channel_row_data.get("lateral_run_out_bottom_over_yield_spec_yield", 0))
                lateral_run_out_harmonic_yield_spec_yield = float(channel_row_data.get("lateral_run_out_harmonic_yield_spec_yield", 0))
                lateral_run_out_top_first_harmonic_yield_spec_yield = float(channel_row_data.get("lateral_run_out_top_first_harmonic_yield_spec_yield", 0))
                lateral_run_out_bottom_first_harmonic_yield_spec_yield = float(channel_row_data.get("lateral_run_out_bottom_first_harmonic_yield_spec_yield", 0))
                bulge_yield_spec_yield = float(channel_row_data.get("bulge_yield_spec_yield", 0))
                bulge_top_over_yield_spec_yield = float(channel_row_data.get("bulge_top_over_yield_spec_yield", 0))
                bulge_bottom_over_yield_spec_yield = float(channel_row_data.get("bulge_bottom_over_yield_spec_yield", 0))
                dent_yield_spec_yield = float(channel_row_data.get("dent_yield_spec_yield", 0))
                dent_top_over_yield_spec_yield = float(channel_row_data.get("dent_top_over_yield_spec_yield", 0))
                dent_bottom_over_yield_spec_yield = float(channel_row_data.get("dent_bottom_over_yield_spec_yield", 0))
                balancing_yield_spec_yield = float(channel_row_data.get("balancing_yield_spec_yield", 0))
                upper_yield_spec_yield = float(channel_row_data.get("upper_yield_spec_yield", 0))
                lower_yield_spec_yield = float(channel_row_data.get("lower_yield_spec_yield", 0))
                upper_lower_yield_spec_yield = float(channel_row_data.get("upper_lower_yield_spec_yield", 0))
                static_yield_spec_yield = float(channel_row_data.get("static_yield_spec_yield", 0))
                warehouse_yield_spec_yield = float(channel_row_data.get("warehouse_yield_spec_yield", 0))
                visual_yield_spec_yield = float(channel_row_data.get("visual_yield_spec_yield", 0))
                
                
                # threshold_value = float(row.get("threshold_value", 0.5))
                threshold_value_con_kgf = float(channel_row_data.get("threshold_value_con_kgf", 0.5))
                threshold_value_cwrfv_oa_kgf = float(channel_row_data.get("threshold_value_cwrfv_oa_kgf", 0.5))
                threshold_value_ccwrfv_oa_kgf = float(channel_row_data.get("threshold_value_ccwrfv_oa_kgf", 0.5))
                threshold_value_cwrfv_oa1h_kgf = float(channel_row_data.get("threshold_value_cwrfv_oa1h_kgf", 0.5))
                threshold_value_ccwrfv_oa1h_kgf = float(channel_row_data.get("threshold_value_ccwrfv_oa1h_kgf", 0.5))
                threshold_value_lfv_oa_kgf = float(channel_row_data.get("threshold_value_lfv_oa_kgf", 0.5))
                threshold_value_lfv_oa_1_kgf = float(channel_row_data.get("threshold_value_lfv_oa_1_kgf", 0.5))
                # threshold_value_bal = float(channel_row_data.get("threshold_value_bal", 5))
                threshold_value_bal_upper_g = float(channel_row_data.get("threshold_value_bal_upper_g", 5))
                threshold_value_bal_lower_g = float(channel_row_data.get("threshold_value_bal_lower_g", 5))
                threshold_value_bal_up_low_g = float(channel_row_data.get("threshold_value_bal_up_low_g", 5))
                threshold_value_bal_static_g = float(channel_row_data.get("threshold_value_bal_static_g", 5))
                test_load_tolerance = float(channel_row_data.get("test_load_tolerance", 5))
                pressure_tolerance = float(channel_row_data.get("pressure_tolerance", 2))
                # threshold_value_yield = float(channel_row_data.get("threshold_value_yield", 0))
                threshold_value_yield_total_rank_yield = float(channel_row_data.get("threshold_value_yield_total_rank_yield", 0))
                threshold_value_yield_rfv_yield = float(channel_row_data.get("threshold_value_yield_rfv_yield", 0))
                threshold_value_yield_rfh1_yield = float(channel_row_data.get("threshold_value_yield_rfh1_yield", 0))
                threshold_value_yield_lfv_yield = float(channel_row_data.get("threshold_value_yield_lfv_yield", 0))
                threshold_value_yield_conicity_yield = float(channel_row_data.get("threshold_value_yield_conicity_yield", 0))
                threshold_value_yield_ufm_yield = float(channel_row_data.get("threshold_value_yield_ufm_yield", 0))
                threshold_value_yield_rorank_yield = float(channel_row_data.get("threshold_value_yield_rorank_yield", 0))
                threshold_value_yield_radial_runout_yield = float(channel_row_data.get("threshold_value_yield_radial_runout_yield", 0))
                threshold_value_yield_radial_runout_first_harmonic_yield = float(channel_row_data.get("threshold_value_yield_radial_runout_first_harmonic_yield", 0))
                threshold_value_yield_lateral_run_out_yield = float(channel_row_data.get("threshold_value_yield_lateral_run_out_yield", 0))
                threshold_value_yield_lateral_run_out_top_over_yield = float(channel_row_data.get("threshold_value_yield_lateral_run_out_top_over_yield", 0))
                threshold_value_yield_lateral_run_out_bottom_over_yield = float(channel_row_data.get("threshold_value_yield_lateral_run_out_bottom_over_yield", 0))
                threshold_value_yield_lateral_run_out_harmonic_yield = float(channel_row_data.get("threshold_value_yield_lateral_run_out_harmonic_yield", 0))
                threshold_value_yield_lateral_run_out_top_first_harmonic_yield = float(channel_row_data.get("threshold_value_yield_lateral_run_out_top_first_harmonic_yield", 0))
                threshold_value_yield_lateral_run_out_bottom_first_harmonic_yield = float(channel_row_data.get("threshold_value_yield_lateral_run_out_bottom_first_harmonic_yield", 0))
                threshold_value_yield_bulge_yield = float(channel_row_data.get("threshold_value_yield_bulge_yield", 0))
                threshold_value_yield_bulge_top_over_yield = float(channel_row_data.get("threshold_value_yield_bulge_top_over_yield", 0))
                threshold_value_yield_bulge_bottom_over_yield = float(channel_row_data.get("threshold_value_yield_bulge_bottom_over_yield", 0))
                threshold_value_yield_dent_yield = float(channel_row_data.get("threshold_value_yield_dent_yield", 0))
                threshold_value_yield_dent_top_over_yield = float(channel_row_data.get("threshold_value_yield_dent_top_over_yield", 0))
                threshold_value_yield_dent_bottom_over_yield = float(channel_row_data.get("threshold_value_yield_dent_bottom_over_yield", 0))
                threshold_value_yield_balancing_yield = float(channel_row_data.get("threshold_value_yield_balancing_yield", 0))
                threshold_value_yield_upper_yield = float(channel_row_data.get("threshold_value_yield_upper_yield", 0))
                threshold_value_yield_lower_yield = float(channel_row_data.get("threshold_value_yield_lower_yield", 0))
                threshold_value_yield_upper_lower_yield = float(channel_row_data.get("threshold_value_yield_upper_lower_yield", 0))
                threshold_value_yield_static_yield = float(channel_row_data.get("threshold_value_yield_static_yield", 0))
                threshold_value_yield_warehouse_yield = float(channel_row_data.get("threshold_value_yield_warehouse_yield", 0))
                threshold_value_yield_visual_yield = float(channel_row_data.get("threshold_value_yield_visual_yield", 0))

                if model_data.empty:
                    # logging.info(f"Skipping UFM model {model_number} gt_code {gt_code}: No data available.")
                    output_messages.append(f"Skipping UFM model {model_number} gt_code {gt_code}: No data available.")
                    continue

                total_barcodes_model_processable = len(model_data)
                if total_barcodes_model_processable == 0:
                    continue
                
                # ==================== INSERTED VIRGIN/FINAL YIELD PRE-PROCESSING ====================
                # Keep a full copy for virgin/final yield handling (includes possible duplicate barcodes)
                model_data_full = model_data.copy()

                # Ensure date_of_tbm is proper datetime for sorting
                model_data_full["date_of_dt"] = pd.to_datetime(model_data_full["date_and_time"], errors='coerce')

                # === 1. Virgin Yield: Keep FIRST occurrence of each barcode ===
                virgin_data = model_data_full.sort_values("date_of_dt").drop_duplicates(subset="barcode", keep="first").copy()
                # >>> ADD THESE PRINT STATEMENTS <<<
                virgin_dupes = model_data_full[model_data_full.duplicated(subset="barcode", keep=False)]
                if not virgin_dupes.empty:
                    first_occ = virgin_dupes.sort_values("date_of_dt").drop_duplicates("barcode", keep="first")
                    # print("=== VIRGIN YIELD - Duplicate barcodes (first occurrence shown) ===")
                    # for _, r in first_occ.iterrows():
                    #     print(f"Barcode: {r['barcode']} | First seen: {r['date_and_time']} | Machine: {r['machine']} | Model: {model_number}")
                # <<< END OF ADDED PRINT >>>

                # === 2. Final Yield: Keep LAST occurrence of each barcode ===
                final_data = model_data_full.sort_values("date_of_dt").drop_duplicates(subset="barcode", keep="last").copy()
                # >>> ADD THESE PRINT STATEMENTS <<<
                final_dupes = model_data_full[model_data_full.duplicated(subset="barcode", keep=False)]
                if not final_dupes.empty:
                    last_occ = final_dupes.sort_values("date_of_dt").drop_duplicates("barcode", keep="last")
                    # print("=== FINAL YIELD - Duplicate barcodes (last occurrence shown) ===")
                    # for _, r in last_occ.iterrows():
                    #     print(f"Barcode: {r['barcode']} | Last seen: {r['date_and_time']} | Machine: {r['machine']} | Model: {model_number}")
                # <<< END OF ADDED PRINT >>>

                # Sort both for lot processing
                virgin_data.sort_values("date_of_dt", inplace=True)
                final_data.sort_values("date_of_dt", inplace=True)

                # Use original full data for main lot processing (existing logic)
                model_data = model_data_full.copy()
                # ==================================================================================

                model_data.sort_values("date_of_ufm", ascending=True, inplace=True)
                
                alert_map = {}

                # model_data.sort_values("date_of_ufm", ascending=True, inplace=True)

                for ufm_machine_id, ufm_machine_data in model_data.groupby("ubno"):
                    try:
                        machine_no = str(ufm_machine_id)
                        ufm_machine_data = ufm_machine_data[ufm_machine_data["ufm_machine_model_lot_id"].isna()].copy()
                        if ufm_machine_data.empty:
                            logging.info(f"Skipping UFM machine {machine_no} for model {model_number}: No unprocessed data.")
                            continue
                        
                        # MODIFIED: Only access tbm_machine if it exists
                        tbm_machine_value = ufm_machine_data["tbm_machine"].iloc[0] if "tbm_machine" in ufm_machine_data.columns and not ufm_machine_data["tbm_machine"].isna().all() else None
                        output_messages.append(f"DEBUG: Model {model_number}, Machine {machine_no}, tbm_machine: {tbm_machine_value}.")

                        machine_config_row = ub_machine_df[ub_machine_df["ubno"] == machine_no]
                        if machine_config_row.empty:
                            logging.info(f"Skipping UFM machine {machine_no} for model {model_number}: Unknown machine config.")
                            output_messages.append(f"Skipping UFM machine {machine_no} for model {model_number}: Unknown machine config.")
                            continue

                        total_ufm_machine_barcodes = len(ufm_machine_data)
                        if total_ufm_machine_barcodes == 0:
                            continue
                        target_lot_size = total_ufm_machine_barcodes

                        machine_model_key = (model_number, machine_no)

                        if machine_model_key not in last_lot_dict:
                            last_lot_dict[machine_model_key] = {
                                "max_lot_number": 0,
                                "last_lot_id": None
                            }

                        current_last_lot_number = last_lot_dict[machine_model_key]["max_lot_number"] + 1
                        current_model_lot_id = f"UFM_{machine_no}_{model_number}_{current_last_lot_number}"
                        previous_cum = prev_cum_dict.get(machine_model_key, 0)
                        current_barcode_counter = previous_cum + target_lot_size
                        current_ufm_machine_lot_id = f"{machine_no}_{model_number}_UFM_{machine_no}_{model_number}_{current_last_lot_number}_{current_barcode_counter}"
                            
                        # Update last_lot_dict
                        last_lot_dict[(model_number, machine_no)]["max_lot_number"] = current_last_lot_number
                        last_lot_dict[(model_number, machine_no)]["last_lot_id"] = current_model_lot_id

                        # Validate lot_id and ufm_machine_model_lot_id formats
                        if not re.match(r"^UFM_[^_]+_[^_]+_\d+$", current_model_lot_id):
                            logging.error(f"Invalid lot_id format: {current_model_lot_id}")
                            continue
                        if not re.match(r"^[^_]+_[^_]+_UFM_[^_]+_[^_]+_\d+_\d+$", current_ufm_machine_lot_id):
                            logging.error(f"Invalid ufm_machine_model_lot_id format: {current_ufm_machine_lot_id}")
                            continue

                        sub_batch_start_index = 0
                        sub_batch_end_index = target_lot_size
                        machine_batch_data = ufm_machine_data.iloc[sub_batch_start_index:sub_batch_end_index].copy()
                        machine_batch_indices = machine_batch_data.index

                        # Fetch previous lot ID
                        ufm_previous_machine_model_lot_id = ufm_previous_machine_model_lot_ids.get(machine_model_key)
                        # logging.info(f"Initial ufm_previous_machine_model_lot_id: {ufm_previous_machine_model_lot_id} for model {model_number}, machine {machine_no}")
                        if ufm_previous_machine_model_lot_id is None and current_last_lot_number > 1:
                            prev_lot_number = current_last_lot_number - 1
                            prev_lot_id = f"UFM_{machine_no}_{model_number}_{prev_lot_number}"
                            logging.info(f"Fetching previous lot for lot_id: {prev_lot_id}")
                            try:
                                prev_lot = averages_ufm_collect.find_one(
                                    {
                                        "modelno": model_number,
                                        "machine": machine_no,
                                        "lot_id": prev_lot_id
                                    }
                                )
                                if prev_lot:
                                    ufm_previous_machine_model_lot_id = prev_lot["ufm_machine_model_lot_id"]
                                    logging.info(f"Found previous lot: {ufm_previous_machine_model_lot_id}")
                                else:
                                    logging.warning(f"No previous lot found for lot_id: {prev_lot_id}")
                                    ufm_previous_machine_model_lot_id = None
                            except Exception as e:
                                logging.error(f"Error fetching previous ufm_machine_model_lot_id for lot {prev_lot_number}: {e}")
                                ufm_previous_machine_model_lot_id = None

                        # Assign values to merge_df
                        model_level_previous_lot_id = f"UFM_{machine_no}_{model_number}_{current_last_lot_number - 1}" if current_last_lot_number > 1 else None
                        merge_df.loc[machine_batch_indices, "lot_id"] = current_model_lot_id
                        merge_df.loc[machine_batch_indices, "previous_lot_id"] = model_level_previous_lot_id
                        merge_df.loc[machine_batch_indices, "ufm_machine_model_lot_id"] = current_ufm_machine_lot_id
                        merge_df.loc[machine_batch_indices, "ufm_previous_machine_model_lot_id"] = ufm_previous_machine_model_lot_id

                        # Fetch previous lot averages
                        prev_avg_key = (model_number, machine_no, ufm_previous_machine_model_lot_id)
                        prev_lot_avg = ufm_machine_lot_avg_map.get(prev_avg_key, None)
                        if prev_lot_avg is None and ufm_previous_machine_model_lot_id is not None:
                            try:
                                prev_lot_doc = averages_ufm_collect.find_one(
                                    {
                                        "modelno": model_number,
                                        "machine": machine_no,
                                        "ufm_machine_model_lot_id": ufm_previous_machine_model_lot_id
                                    },
                                    {"current_lot_averages": 1}
                                )
                                prev_lot_avg = prev_lot_doc.get("current_lot_averages", {}) if prev_lot_doc else {}
                                if prev_lot_avg:
                                    ufm_machine_lot_avg_map[prev_avg_key] = prev_lot_avg
                                    logging.info(f"Fetched previous lot averages for {ufm_previous_machine_model_lot_id} from database")
                                else:
                                    logging.warning(f"No previous lot averages found for {ufm_previous_machine_model_lot_id} in database")
                            except Exception as e:
                                logging.error(f"Error fetching previous lot averages for {ufm_previous_machine_model_lot_id}: {e}")
                                prev_lot_avg = {}
                        if current_last_lot_number == 1:
                            prev_lot_avg = {}
                            logging.info(f"First lot for model {model_number}, machine {machine_no}. Setting previous_lot_averages to empty.")
                        # logging.info(f"prev_key: {prev_avg_key}, prev_lot_avg: {prev_lot_avg}")
                        # Update last_lot_dict
                        last_lot_dict[machine_model_key] = {
                            "max_lot_number": current_last_lot_number,
                            
                            "last_ufm_machine_model_lot_id": current_ufm_machine_lot_id
                        }                      

                        # Prepare Lot Document
                        lot_doc_data = {
                            "_id": f"lot_{current_ufm_machine_lot_id}",
                            "modelno": model_number,
                            "lot_id": current_model_lot_id,
                            "previous_lot_id": model_level_previous_lot_id,
                            "machine": machine_no,
                            "ufm_machine_model_lot_id": current_ufm_machine_lot_id,
                            "ufm_previous_machine_model_lot_id": ufm_previous_machine_model_lot_id,
                            "target_machine_lot_size": target_lot_size,
                            "processed_machine_barcodes": target_lot_size,
                            "timestamp_lot_generated": some_function(),
                            "first_barcode": machine_batch_data['barcode'].iloc[0] if not machine_batch_data.empty else None,
                            "last_barcode": machine_batch_data['barcode'].iloc[-1] if not machine_batch_data.empty else None,
                        }
                        if not lot_ufm_collect.find_one({"ufm_machine_model_lot_id": current_ufm_machine_lot_id}):
                            ufm_lot_bulk_operations.append(pymongo.UpdateOne(
                                {"ufm_machine_model_lot_id": current_ufm_machine_lot_id},
                                {"$setOnInsert": convert_numpy_types(lot_doc_data)},
                                upsert=True
                            ))
                            output_messages.append(f"Prepared lot_id {current_model_lot_id} for insertion into lot_ufm_collect")

                        lot_averages = {}
                        for param in ufm_params:
                            if param in machine_batch_data.columns:
                                numeric_series = pd.to_numeric(machine_batch_data[param], errors='coerce').dropna()
                                if not numeric_series.empty:
                                    lot_averages[param] = float(numeric_series.mean())
                                else:
                                    lot_averages[param] = None
                            else:
                                lot_averages[param] = None

                        lot_averages.update({
                            "total_rank_yield": total_rank_yield(machine_batch_data),
                            "rfv_yield": rfv_yield(machine_batch_data),
                            "rfh1_yield": rfh1_yield(machine_batch_data),
                            "lfv_yield": lfv_yield(machine_batch_data),
                            "conicity_yield": conicity_yield(machine_batch_data),
                            "ufm_yield": ufm_yield(machine_batch_data),
                            "rorank_yield": rorank_yield(machine_batch_data),
                            "radial_runout_yield": radial_runout_yield(machine_batch_data),
                            "radial_runout_first_harmonic_yield": radial_runout_first_harmonic_yield(machine_batch_data),
                            "lateral_run_out_yield": lateral_run_out_yield(machine_batch_data),
                            "lateral_run_out_top_over_yield": lateral_run_out_top_over(machine_batch_data),
                            "lateral_run_out_bottom_over_yield": lateral_run_out_bottom_over(machine_batch_data),
                            "lateral_run_out_harmonic_yield": lateral_run_out_harmonic_yield(machine_batch_data),
                            "lateral_run_out_top_first_harmonic_yield": lateral_run_out_top_first_harmonic(machine_batch_data),
                            "lateral_run_out_bottom_first_harmonic_yield": lateral_run_out_bottom_first_harmonic(machine_batch_data),
                            "bulge_yield": bulge_yield(machine_batch_data),
                            "bulge_top_over_yield": bulge_top_over(machine_batch_data),
                            "bulge_bottom_over_yield": bulge_bottom_over(machine_batch_data),
                            "dent_yield": dent_yield(machine_batch_data),
                            "dent_top_over_yield": dent_top_over(machine_batch_data),
                            "dent_bottom_over_yield": dent_bottom_over(machine_batch_data),
                            "balancing_yield": balancing_yield(machine_batch_data),
                            "upper_yield": upper_yield(machine_batch_data),
                            "lower_yield": lower_yield(machine_batch_data),
                            "upper_lower_yield": upper_lower_yield(machine_batch_data),
                            "static_yield": static_yield(machine_batch_data),
                            "warehouse_yield": warehouse_yield(machine_batch_data),
                            "visual_yield": visual_yield(machine_batch_data)
                        })

                        # Perform Shift Detection
                        parameter_alerts = []
                        machine_lot_overall_shift_occurred = False
                        # Define mapping of yield parameters to their spec_yield variables
                        yield_spec_mapping = {
                            "total_rank_yield": total_rank_yield_spec_yield,
                            "rfv_yield": rfv_yield_spec_yield,
                            "rfh1_yield": rfh1_yield_spec_yield,
                            "lfv_yield": lfv_yield_spec_yield,
                            "conicity_yield": conicity_yield_spec_yield,
                            "ufm_yield": ufm_yield_spec_yield,
                            "rorank_yield": rorank_yield_spec_yield,
                            "radial_runout_yield": radial_runout_yield_spec_yield,
                            "radial_runout_first_harmonic_yield": radial_runout_first_harmonic_yield_spec_yield,
                            "lateral_run_out_yield": lateral_run_out_yield_spec_yield,
                            "lateral_run_out_top_over_yield": lateral_run_out_top_over_yield_spec_yield,
                            "lateral_run_out_bottom_over_yield": lateral_run_out_bottom_over_yield_spec_yield,
                            "lateral_run_out_harmonic_yield": lateral_run_out_harmonic_yield_spec_yield,
                            "lateral_run_out_top_first_harmonic_yield": lateral_run_out_top_first_harmonic_yield_spec_yield,
                            "lateral_run_out_bottom_first_harmonic_yield": lateral_run_out_bottom_first_harmonic_yield_spec_yield,
                            "bulge_yield": bulge_yield_spec_yield,
                            "bulge_top_over_yield": bulge_top_over_yield_spec_yield,
                            "bulge_bottom_over_yield": bulge_bottom_over_yield_spec_yield,
                            "dent_yield": dent_yield_spec_yield,
                            "dent_top_over_yield": dent_top_over_yield_spec_yield,
                            "dent_bottom_over_yield": dent_bottom_over_yield_spec_yield,
                            "balancing_yield": balancing_yield_spec_yield,
                            "upper_yield": upper_yield_spec_yield,
                            "lower_yield": lower_yield_spec_yield,
                            "upper_lower_yield": upper_lower_yield_spec_yield,
                            "static_yield": static_yield_spec_yield,
                            "warehouse_yield": warehouse_yield_spec_yield,
                            "visual_yield": visual_yield_spec_yield
                        }
                        if prev_lot_avg:
                            for param in ufm_params + yield_params:
                                current_val = lot_averages.get(param)
                                prev_val = prev_lot_avg.get(param)
                                if current_val is None or pd.isna(current_val) or prev_val is None or pd.isna(prev_val):
                                    alert_dict = {
                                        "parameter": param,
                                        "current_value": current_val,
                                        "previous_value": prev_val,
                                        "difference": None,
                                        "shift_occurred_thershold": False,
                                        "spec_value": None,
                                        "shift_occurred_spec": False,
                                        "shift_occurred": False
                                    }
                                    if param == "load1":
                                        alert_dict["test_load_tolerance"] = None
                                    if param == "airpress1":
                                        alert_dict["pressure_tolerance"] = None
                                    parameter_alerts.append(alert_dict)
                                    continue

                                shift_occurred_thershold = False
                                shift_occurred_spec = False
                                spec_val = None

                                if param == "con_kgf" and spec_con_kgf:
                                        shift_occurred_thershold = abs(current_val - prev_val) > threshold_value_con_kgf
                                        shift_occurred_spec = not (-spec_con_kgf <= current_val <= spec_con_kgf)
                                        spec_val = spec_con_kgf
                                elif param == "cwrfv_oa_kgf" and spec_cwrfv_oa_kgf:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_cwrfv_oa_kgf
                                    shift_occurred_spec = current_val > spec_cwrfv_oa_kgf
                                    spec_val = spec_cwrfv_oa_kgf
                                elif param == "ccwrfv_oa_kgf" and spec_ccwrfv_oa_kgf:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_ccwrfv_oa_kgf
                                    shift_occurred_spec = current_val > spec_ccwrfv_oa_kgf
                                    spec_val = spec_ccwrfv_oa_kgf
                                elif param == "cwrfv_oa1h_kgf" and spec_cwrfv_oa1h_kgf:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_cwrfv_oa1h_kgf
                                    shift_occurred_spec = current_val > spec_cwrfv_oa1h_kgf
                                    spec_val = spec_cwrfv_oa1h_kgf
                                elif param == "ccwrfv_oa1h_kgf" and spec_ccwrfv_oa1h_kgf:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_ccwrfv_oa1h_kgf
                                    shift_occurred_spec = current_val > spec_ccwrfv_oa1h_kgf
                                    spec_val = spec_ccwrfv_oa1h_kgf
                                elif param == "lfv_oa_kgf" and spec_lfv_oa_kgf:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_lfv_oa_kgf
                                    shift_occurred_spec = current_val > spec_lfv_oa_kgf
                                    spec_val = spec_lfv_oa_kgf
                                elif param == "lfv_oa_1_kgf" and spec_lfv_oa_1_kgf:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_lfv_oa_1_kgf
                                    shift_occurred_spec = current_val > spec_lfv_oa_1_kgf
                                    spec_val = spec_lfv_oa_1_kgf
                                elif param == "upper_g" and spec_upper_g:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_bal_upper_g
                                    shift_occurred_spec = current_val > spec_upper_g
                                    spec_val = spec_upper_g
                                elif param == "lower_g" and spec_lower_g:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_bal_lower_g
                                    shift_occurred_spec = current_val > spec_lower_g
                                    spec_val = spec_lower_g
                                elif param == "up_low_g" and spec_up_low_g:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_bal_up_low_g
                                    shift_occurred_spec = current_val > spec_up_low_g
                                    spec_val = spec_up_low_g
                                elif param == "static_g" and spec_static:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_bal_static_g
                                    shift_occurred_spec = current_val > spec_static
                                    spec_val = spec_static
                                # elif param in yield_params:
                                #     spec_yield = yield_spec_mapping.get(param, 0)
                                #     shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield)
                                #     shift_occurred_spec = current_val < spec_yield
                                #     spec_val = spec_yield

                                elif param == "total_rank_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_total_rank_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "rfv_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_rfv_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "rfh1_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_rfh1_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "lfv_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_lfv_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "conicity_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_conicity_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "ufm_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_conicity_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "rorank_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_rorank_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "radial_runout_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_radial_runout_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "radial_runout_first_harmonic_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_radial_runout_first_harmonic_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "lateral_run_out_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_lateral_run_out_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "lateral_run_out_top_over_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_lateral_run_out_top_over_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "lateral_run_out_bottom_over_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_lateral_run_out_bottom_over_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "lateral_run_out_harmonic_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_lateral_run_out_harmonic_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "lateral_run_out_top_first_harmonic_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_lateral_run_out_top_first_harmonic_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "lateral_run_out_bottom_first_harmonic_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_lateral_run_out_bottom_first_harmonic_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "bulge_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_bulge_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "bulge_top_over_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_bulge_top_over_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "bulge_bottom_over_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_bulge_bottom_over_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "dent_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_dent_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "dent_top_over_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_dent_top_over_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "dent_bottom_over_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_dent_bottom_over_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "balancing_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_balancing_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "upper_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_upper_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "lower_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_lower_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "upper_lower_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_upper_lower_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "static_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_static_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "warehouse_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_warehouse_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "visual_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_visual_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                    
                                elif param == "load1" and test_load:
                                    shift_occurred_thershold = abs(current_val - prev_val) > test_load_tolerance
                                    shift_occurred_spec = not (test_load - test_load_tolerance <= current_val <= test_load + test_load_tolerance)
                                    spec_val = test_load
                                elif param == "airpress1" and pressure:
                                    shift_occurred_thershold = abs(current_val - prev_val) > pressure_tolerance
                                    shift_occurred_spec = not (pressure - pressure_tolerance <= current_val <= pressure + pressure_tolerance)
                                    spec_val = pressure

                                shift_occurred = shift_occurred_thershold or shift_occurred_spec
                                if shift_occurred:
                                    machine_lot_overall_shift_occurred = True

                                alert_dict = {
                                    "parameter": param,
                                    "current_value": round(current_val, 3) if current_val is not None else None,
                                    "previous_value": round(prev_val, 3) if prev_val is not None else None,
                                    "difference": round(current_val - prev_val, 3) if current_val is not None and prev_val is not None else None,
                                    "shift_occurred_thershold": bool(shift_occurred_thershold),
                                    "spec_value": spec_val,
                                    "spec_difference": round(current_val - spec_val, 3) if current_val is not None and spec_val is not None else None,
                                    "shift_occurred_spec": bool(shift_occurred_spec),
                                    "shift_occurred": bool(shift_occurred)
                                }
                                if param == "load1":
                                    alert_dict["test_load_tolerance"] = test_load_tolerance
                                if param == "airpress1":
                                    alert_dict["pressure_tolerance"] = pressure_tolerance
                                parameter_alerts.append(alert_dict)
                        else:
                            for param in ufm_params + yield_params:
                                current_val = lot_averages.get(param)
                                if current_val is None or pd.isna(current_val):
                                    alert_dict = {
                                        "parameter": param,
                                        "current_value": current_val,
                                        "previous_value": None,
                                        "difference": None,
                                        "shift_occurred_thershold": False,
                                        "spec_value": None,
                                        "shift_occurred_spec": False,
                                        "shift_occurred": False
                                    }
                                    if param == "load1":
                                        alert_dict["test_load_tolerance"] = None
                                    if param == "airpress1":
                                        alert_dict["pressure_tolerance"] = None
                                    parameter_alerts.append(alert_dict)
                                    continue

                                shift_occurred_thershold = False
                                shift_occurred_spec = False
                                spec_val = None

                                if param == "con_kgf" and spec_con_kgf:
                                    shift_occurred_spec = not (-spec_con_kgf <= current_val <= spec_con_kgf)
                                    spec_val = spec_con_kgf
                                elif param == "cwrfv_oa_kgf" and spec_cwrfv_oa_kgf:
                                    shift_occurred_spec = current_val > spec_cwrfv_oa_kgf
                                    spec_val = spec_cwrfv_oa_kgf
                                elif param == "ccwrfv_oa_kgf" and spec_ccwrfv_oa_kgf:
                                    shift_occurred_spec = current_val > spec_ccwrfv_oa_kgf
                                    spec_val = spec_ccwrfv_oa_kgf
                                elif param == "cwrfv_oa1h_kgf" and spec_cwrfv_oa1h_kgf:
                                    shift_occurred_spec = current_val > spec_cwrfv_oa1h_kgf
                                    spec_val = spec_cwrfv_oa1h_kgf
                                elif param == "ccwrfv_oa1h_kgf" and spec_ccwrfv_oa1h_kgf:
                                    shift_occurred_spec = current_val > spec_ccwrfv_oa1h_kgf
                                    spec_val = spec_ccwrfv_oa1h_kgf
                                elif param == "lfv_oa_kgf" and spec_lfv_oa_kgf:
                                    shift_occurred_spec = current_val > spec_lfv_oa_kgf
                                    spec_val = spec_lfv_oa_kgf
                                elif param == "lfv_oa_1_kgf" and spec_lfv_oa_1_kgf:
                                    shift_occurred_spec = current_val > spec_lfv_oa_1_kgf
                                    spec_val = spec_lfv_oa_1_kgf
                                elif param == "upper_g" and spec_upper_g:
                                    shift_occurred_spec = current_val > spec_upper_g
                                    spec_val = spec_upper_g
                                elif param == "lower_g" and spec_lower_g:
                                    shift_occurred_spec = current_val > spec_lower_g
                                    spec_val = spec_lower_g
                                elif param == "up_low_g" and spec_up_low_g:
                                    shift_occurred_spec = current_val > spec_up_low_g
                                    spec_val = spec_up_low_g
                                elif param == "static_g" and spec_static:
                                    shift_occurred_spec = current_val > spec_static
                                    spec_val = spec_static
                                elif param in yield_params:
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param in yield_params and spec_yield:
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "load1" and test_load:
                                    shift_occurred_spec = not (test_load - test_load_tolerance <= current_val <= test_load + test_load_tolerance)
                                    spec_val = test_load
                                elif param == "airpress1" and pressure:
                                    shift_occurred_spec = not (pressure - pressure_tolerance <= current_val <= pressure + pressure_tolerance)
                                    spec_val = pressure

                                shift_occurred = shift_occurred_thershold or shift_occurred_spec
                                if shift_occurred:
                                    machine_lot_overall_shift_occurred = True

                                alert_dict = {
                                    "parameter": param,
                                    "current_value": round(current_val, 3) if current_val is not None else None,
                                    "previous_value": None,
                                    "difference": None,
                                    "shift_occurred_thershold": bool(shift_occurred_thershold),
                                    "spec_value": spec_val,
                                    "spec_difference": round(current_val - spec_val, 3) if current_val is not None and spec_val is not None else None,
                                    "shift_occurred_spec": bool(shift_occurred_spec),
                                    "shift_occurred": bool(shift_occurred)
                                }
                                if param == "load1":
                                    alert_dict["test_load_tolerance"] = test_load_tolerance
                                if param == "airpress1":
                                    alert_dict["pressure_tolerance"] = pressure_tolerance
                                parameter_alerts.append(alert_dict)

                        machine_lot_overall_shift_occurred = bool(machine_lot_overall_shift_occurred)

                        deviated_parameter_alerts = [
                            alert for alert in parameter_alerts
                            if alert.get("shift_occurred_thershold", False) or alert.get("shift_occurred_spec", False)
                        ]

                        for alert in deviated_parameter_alerts:
                            param = alert.get("parameter", "")
                            if param.endswith("_yield"):
                                if param in rank_param_mappings:
                                    rank_counts = {}
                                    for col in rank_param_mappings[param]:
                                        if col in machine_batch_data.columns:
                                            value_counts = machine_batch_data[col].value_counts().to_dict()
                                            for rank_value, count in value_counts.items():
                                                rank_value = int(rank_value) if isinstance(rank_value, (int, float)) else rank_value
                                                rank_counts[f"{col}_{rank_value}"] = int(count)
                                    alert["rank_counts"] = rank_counts

                        if deviated_parameter_alerts:
                            deviated_doc = {
                                "alert_id_tbm_ufm": last_alert_id + 1,
                                "modelno": model_number,
                                "lot_id": current_model_lot_id,
                                "machine": machine_no,
                                "comments": [],
                                "alert_assign_user": [],
                                "ufm_machine_model_lot_id": current_ufm_machine_lot_id,
                                "ufm_previous_machine_model_lot_id": ufm_previous_machine_model_lot_id,
                                "parameter_alerts": deviated_parameter_alerts,
                                "timestamp_alert_generated": some_function()
                            }
                            deviated_triggering_bulk_operations.append(pymongo.InsertOne(deviated_doc))
                            logging.info(f"Prepared {len(deviated_parameter_alerts)} derived alerts for machine {machine_no}, lot {current_model_lot_id}")

                        parameter_alerts = [
                            alert for alert in parameter_alerts
                            if alert.get("parameter", "").endswith("_yield") or not alert.get("parameter", "").endswith("_yield")
                        ]

                        for alert in parameter_alerts:
                            param = alert.get("parameter", "")
                            if (
                                (alert.get("shift_occurred_thershold", False) or alert.get("shift_occurred_spec", False))
                                and param.endswith("_yield")
                            ):
                                if param in rank_param_mappings:
                                    rank_counts = {}
                                    for col in rank_param_mappings[param]:
                                        if col in machine_batch_data.columns:
                                            value_counts = machine_batch_data[col].value_counts().to_dict()
                                            for rank_value, count in value_counts.items():
                                                rank_value = int(rank_value) if isinstance(rank_value, (int, float)) else rank_value
                                                rank_counts[f"{col}_{rank_value}"] = int(count)
                                    alert["rank_counts"] = rank_counts

                        averages_doc_data = {
                            "_id": f"avg_{current_ufm_machine_lot_id}",
                            "modelno": model_number,
                            "lot_id": current_model_lot_id,
                            "previous_lot_id": model_level_previous_lot_id,
                            "machine": machine_no,
                            "ufm_machine_model_lot_id": current_ufm_machine_lot_id,
                            "ufm_previous_machine_model_lot_id": ufm_previous_machine_model_lot_id,
                            "current_lot_averages": {k: round(v, 3) if v is not None and not pd.isna(v) else None for k, v in lot_averages.items()},
                            "previous_lot_averages": {k: round(v, 3) if v is not None and not pd.isna(v) else None for k, v in prev_lot_avg.items()} if prev_lot_avg else {},
                            "parameter_alerts": parameter_alerts,
                            "ufm_shift_occurred": machine_lot_overall_shift_occurred,
                            "target_machine_lot_size": target_lot_size,
                            "processed_machine_barcodes": target_lot_size,
                            "timestamp_alert_generated": some_function()
                        }
                        if not averages_ufm_collect.find_one({"ufm_machine_model_lot_id": current_ufm_machine_lot_id}):
                            ufm_averages_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(averages_doc_data)))
                            output_messages.append(f"Prepared averages for {current_ufm_machine_lot_id} for insertion into averages_ufm_collect")

                        current_avg_key = (model_number, machine_no, current_ufm_machine_lot_id)
                        ufm_machine_lot_avg_map[current_avg_key] = lot_averages
                        ufm_previous_machine_model_lot_ids[machine_model_key] = current_ufm_machine_lot_id

                        last_alert_id += 1
                        merge_df.loc[machine_batch_indices, "alert_id_ufm_ufm"] = last_alert_id

                        # machine_batch_data["tbm_shift"] = machine_batch_data["date_of_tbm"].apply(get_shift)
                        # shift_groups = machine_batch_data.groupby('tbm_shift')
                        # shifts_data_list_tbm = []
                        # for shift_name, shift_data in shift_groups:
                        #     shift_start_time = shift_data["date_of_tbm"].iloc[0]
                        #     shift_end_time = shift_data["date_of_tbm"].iloc[-1]
                        #     shifts_data_list_tbm.append({
                        #         "shift": shift_name,
                        #         "barcode_count": len(shift_data["barcode"]) if "barcode" in shift_data.columns else 0,
                        #         "machine_shift_tbm_start_time": shift_start_time,
                        #         "machine_shift_tbm_end_time": shift_end_time,
                        #         "barcodes": shift_data["barcode"].tolist() if "barcode" in shift_data.columns else []
                        #     })

                        # ------------------------------------------------------------------
                        #  NEW (correct – one entry per **continuous** shift block)
                        # ------------------------------------------------------------------
                        machine_batch_data["tbm_shift"] = machine_batch_data["date_of_tbm"].apply(get_shift)

                        # Sort by date_of_tbm to guarantee chronological order
                        machine_batch_data = machine_batch_data.sort_values("date_of_tbm").reset_index(drop=True)

                        shifts_data_list_tbm = []
                        current_shift = None
                        current_start = None
                        current_barcodes = []

                        for idx, row in machine_batch_data.iterrows():
                            this_shift = row["tbm_shift"]
                            this_dt    = row["date_of_tbm"]
                            this_bc    = row.get("barcode")

                            if this_shift != current_shift or current_shift is None:
                                # finish previous block (if any)
                                if current_shift is not None:
                                    shifts_data_list_tbm.append({
                                        "shift": current_shift,
                                        "barcode_count": len(current_barcodes),
                                        "machine_shift_tbm_start_time": current_start,
                                        "machine_shift_tbm_end_time":   machine_batch_data.at[idx-1, "date_of_tbm"],
                                        "barcodes": current_barcodes[:]
                                    })
                                # start new block
                                current_shift   = this_shift
                                current_start   = this_dt
                                current_barcodes = [this_bc] if this_bc is not None else []
                            else:
                                current_barcodes.append(this_bc if this_bc is not None else None)

                        # do NOT forget the last block
                        if current_shift is not None:
                            shifts_data_list_tbm.append({
                                "shift": current_shift,
                                "barcode_count": len(current_barcodes),
                                "machine_shift_tbm_start_time": current_start,
                                "machine_shift_tbm_end_time":   machine_batch_data["date_of_tbm"].iloc[-1],
                                "barcodes": current_barcodes
                            })
                        shift_groups = machine_batch_data.groupby('shift')
                        shifts_data_list = []
                        for shift_name, shift_data in shift_groups:
                            if not shift_data.empty and 'date_of_ufm' in shift_data.columns:
                                shift_data_sorted = shift_data.sort_values('date_of_ufm')
                                shifts_data_list.append({
                                    "shifts": shift_name,
                                    "barcode_count": len(shift_data_sorted["barcode"]) if "barcode" in shift_data_sorted.columns else 0,
                                    "machine_shift_ufm_start_time": shift_data_sorted["date_of_ufm"].iloc[0],
                                    "machine_shift_ufm_end_time": shift_data_sorted["date_of_ufm"].iloc[-1],
                                    "barcodes": shift_data_sorted["barcode"].tolist() if "barcode" in shift_data_sorted.columns else []
                                })

                        machine_alert_detail = {
                            "machine": machine_no,
                            "tbm_machine": tbm_machine_value,  # MODIFIED: tbm_machine may be None if not created
                            "ufm_shift_occured": machine_lot_overall_shift_occurred,
                            "shifts_data": shifts_data_list,
                            "shifts_data_tbm": shifts_data_list_tbm,
                            "ufm_yield": ufm_yield(machine_batch_data),
                            "rorank_yield": rorank_yield(machine_batch_data),
                            "balancing_yield": balancing_yield(machine_batch_data),
                            "total_rank_yield": total_rank_yield(machine_batch_data),
                            "visual_yield": visual_yield(machine_batch_data),
                            "sizecode": sizecode, 
                            "gt_code": str(gt_code),   # Add 16-07
                            "target_machine_lot_size": target_lot_size,
                            "processed_machine_barcodes": target_lot_size,
                            "machine_lot_start_ufm_time": machine_batch_data["date_of_ufm"].min() if not machine_batch_data["date_of_ufm"].empty else None,
                            "machine_lot_end_ufm_time": machine_batch_data["date_of_ufm"].max() if not machine_batch_data["date_of_ufm"].empty else None,
                            "machine_lot_start_build_time": machine_batch_data["date_of_tbm"].min() if not machine_batch_data["date_of_tbm"].empty else None,
                            "machine_lot_end_build_time": machine_batch_data["date_of_tbm"].max() if not machine_batch_data["date_of_tbm"].empty else None,
                            "parameter_alerts": parameter_alerts,
                            "lot_averages": {k: round(v, 3) if v is not None and not pd.isna(v) else None for k, v in lot_averages.items()},
                            "previous_lot_averages": {k: round(v, 3) if v is not None and not pd.isna(v) else None for k, v in prev_lot_avg.items()} if prev_lot_avg else {},
                            "first_barcode_machine": machine_batch_data.iloc[0]["barcode"] if not machine_batch_data.empty and "barcode" in machine_batch_data.columns else None,
                            "last_barcode_machine": machine_batch_data.iloc[-1]["barcode"] if not machine_batch_data.empty and "barcode" in machine_batch_data.columns else None,
                            "ufm_machine_model_lot_id": current_ufm_machine_lot_id,
                            "ufm_previous_machine_model_lot_id": ufm_previous_machine_model_lot_id,
                            "rank_columns": get_rank_columns(machine_batch_data),
                            "barcodes": machine_batch_data["barcode"].tolist() if "barcode" in machine_batch_data.columns else []
                        }

                        alert_doc_main = {
                            "_id": f"main_alert_{last_alert_id}",
                            "alert_id_ufm_ufm": last_alert_id,
                            "modelno": model_number,
                            "lot_id": current_model_lot_id,
                            "alert_flag": False,
                            "dashboard_flag": False,
                            "machine_flag": False,
                            "alert_status_check": "o",
                            "alert_status_timestamp": None,
                            "alert_status_comment": [],
                            "alert_assign_user": [],
                            "machine_alerts": [machine_alert_detail],
                            "first_barcode_model": machine_alert_detail["first_barcode_machine"],
                            "last_barcode_model": machine_alert_detail["last_barcode_machine"],
                            "timestamp_alert_generated": some_function()
                        }
                        
                        # Store in alert_map for Virgin/Final yield (unchanged)
                        alert_map[last_alert_id] = {
                            "alert_id_ufm_ufm": last_alert_id,
                            "modelno": model_number,
                            "lot_id": current_model_lot_id,
                            "machine_alerts": [machine_alert_detail.copy()]
                        }

                        # Define trigger_doc safely – use unique _id every time
                        trigger_doc = {
                            "_id": f"trigger_alert_{last_alert_id}",   # Unique every time!
                            "alert_id_ufm_ufm": last_alert_id,
                            "modelno": model_number,
                            "lot_id": current_model_lot_id,
                            "type": "ufm_trigger",
                            "timestamp_alert_generated": some_function()
                        }

                        # ALWAYS insert into alert_table and alert_db_dashboard (no duplicates)
                        # 1. Insert main alert document
                        if not alert_collect.find_one({"alert_id_ufm_ufm": last_alert_id}):
                            try:
                                alert_collect.insert_one(convert_numpy_types(alert_doc_main))
                                logging.info(f"Inserted alert_doc_main into alert_table for alert_id {last_alert_id}")
                                output_messages.append(f"Inserted into alert_table: alert_id {last_alert_id}")
                            except Exception as e:
                                logging.error(f"Failed to insert into alert_table: {e}")

                        # 2. Insert into alert_db_dashboard (main + trigger)
                        if not alert_triggering_collect_dashboard_ufm.find_one({"alert_id_ufm_ufm": last_alert_id}):
                            try:
                                alert_triggering_collect_dashboard_ufm.insert_one(convert_numpy_types(alert_doc_main))
                                logging.info(f"Inserted alert_doc_main into alert_db_dashboard for alert_id {last_alert_id}")
                                output_messages.append(f"Inserted into alert_db_dashboard (main): alert_id {last_alert_id}")
                            except Exception as e:
                                logging.error(f"Failed to insert main alert into alert_db_dashboard: {e}")

                        if not alert_triggering_collect_dashboard_ufm.find_one({"_id": f"trigger_alert_{last_alert_id}"}):
                            try:
                                alert_triggering_collect_dashboard_ufm.insert_one(convert_numpy_types(trigger_doc))
                                logging.info(f"Inserted trigger_doc into alert_db_dashboard for alert_id {last_alert_id}")
                                output_messages.append(f"Inserted into alert_db_dashboard (trigger): alert_id {last_alert_id}")
                            except Exception as e:
                                logging.error(f"Failed to insert trigger_doc into alert_db_dashboard: {e}")
                        else:
                            logging.info(f"trigger_doc already exists for alert_id {last_alert_id} – skipped")

                        # Keep your existing bulk operations (only when shift occurs)
                        if machine_lot_overall_shift_occurred:
                            if not alert_ufm_collect.find_one({"alert_id_ufm_ufm": last_alert_id}):
                                ufm_alert_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(alert_doc_main)))
                                ufm_triggering_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(trigger_doc)))
                                ufm_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(trigger_doc)))
                                logging.info(f"Shift detected - added to bulk ops for alert_id {last_alert_id}")
                                output_messages.append(f"Shift alert queued for bulk: alert_id {last_alert_id}")

                        # # Store in alert_map for Virgin/Final yield
                        # alert_map[last_alert_id] = {
                        #     "alert_id_ufm_ufm": last_alert_id,
                        #     "modelno": model_number,
                        #     "lot_id": current_model_lot_id,
                        #     "machine_alerts": [machine_alert_detail.copy()]
                        # }

                        # if machine_lot_overall_shift_occurred:
                        #     if not alert_ufm_collect.find_one({"alert_id_ufm_ufm": last_alert_id}):
                        #         ufm_alert_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(alert_doc_main)))
                        #         ufm_triggering_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(trigger_doc)))
                        
                        last_alert_id += 1
                        merge_df.loc[machine_batch_indices, "alert_id_ufm_ufm"] = last_alert_id
                        machine_barcode_counter += target_lot_size

                    except Exception as e:
                        logging.error("UFM Error processing machine %s for model %s: %s", ufm_machine_id, model_number, e)
                        output_messages.append(f"UFM Error processing machine {ufm_machine_id} for model {model_number}: {e}")
                        continue

            # ==================== VIRGIN & FINAL YIELD ALERT INSERTION ====================
            try:
                for alert_key, alert_obj in alert_map.items():
                    true_machine_alerts = [ma for ma in alert_obj["machine_alerts"] if ma.get("ufm_shift_occured", False) in [True, False]]
                    if not true_machine_alerts:
                        continue

                    # Prepare barcode sets from full data (with duplicates)
                    model_data_full = model_data.copy()
                    model_data_full["date_of_dt"] = pd.to_datetime(model_data_full["date_and_time"], errors='coerce')
                    final_data = model_data_full.sort_values("date_of_dt").drop_duplicates(subset="barcode", keep="last").copy()
                    final_barcodes_set = set(final_data["barcode"].dropna().astype(str))

                    # ==================== FINAL YIELD: LAST OCCURRENCE ONLY ====================
                    final_machine_alerts = []
                    final_barcodes_seen = set()

                    for ma in true_machine_alerts:
                        original_barcodes = ma.get("barcodes", [])
                        if not original_barcodes:
                            continue

                        filtered_barcodes = []
                        for bc in original_barcodes:
                            bc_str = str(bc)
                            if bc_str in final_barcodes_set and bc_str not in final_barcodes_seen:
                                filtered_barcodes.append(bc_str)
                                final_barcodes_seen.add(bc_str)

                        if filtered_barcodes:
                            final_ma = ma.copy()
                            final_ma["barcodes"] = filtered_barcodes
                            final_ma["barcode_count"] = len(filtered_barcodes)
                            final_machine_alerts.append(final_ma)

                    if final_machine_alerts:
                        all_final_bcs = sorted(final_barcodes_seen,
                            key=lambda x: final_data[final_data["barcode"] == x]["date_of_dt"].iloc[0])

                        # === 1. MAIN FINAL YIELD ALERT (ALL CASES: shift true or false) ===
                        doc_final = {
                            "alert_id_tbm_ufm": alert_obj["alert_id_ufm_ufm"],
                            "modelno": alert_obj["modelno"],
                            "lot_id": alert_obj["lot_id"],
                            "alert_flag": False,
                            "dashboard_flag": False,
                            "machine_flag": False,
                            "alert_status_check": "o",
                            "alert_status_timestamp": None,
                            "alert_status_comment": [],
                            "alert_assign_user": [],
                            "machine_alerts": final_machine_alerts,
                            "first_barcode_model": all_final_bcs[0] if all_final_bcs else None,
                            "last_barcode_model": all_final_bcs[-1] if all_final_bcs else None,
                            "timestamp_alert_generated": some_function(),
                            "yield_type": "final"
                        }

                        # Insert into existing final yield collection
                        alert_triggering_final_yield_collect.insert_one(doc_final)

                        # NEW: Insert into dedicated Final Yield main database (ALL alerts)
                        try:
                            alert_final_yield_collect.insert_one(doc_final)
                            logging.info(f"Final Yield Alert {alert_obj['alert_id_ufm_ufm']} → alert_table_final_yield")
                        except Exception as e:
                            logging.error(f"Failed insert into alert_table_final_yield: {e}")

                        # === 2. DEVIATED FINAL YIELD: ONLY WHEN SHIFT = TRUE ===
                        has_shift = any(ma.get("ufm_shift_occured", False) for ma in final_machine_alerts)
                        if has_shift:
                            deviated_parameter_alerts = []

                            for ma in final_machine_alerts:
                                if not ma.get("ufm_shift_occured", False):
                                    continue

                                for alert in ma.get("parameter_alerts", []):
                                    if alert.get("shift_occurred_thershold", False) or alert.get("shift_occurred_spec", False):
                                        dev_alert = alert.copy()
                                        param = dev_alert.get("parameter", "")

                                        # Recalculate rank_counts using Final Yield data only
                                        if param.endswith("_yield") and param in rank_param_mappings:
                                            batch_data = final_data[final_data["barcode"].astype(str).isin(ma["barcodes"])]
                                            rank_counts = {}
                                            for col in rank_param_mappings[param]:
                                                if col in batch_data.columns:
                                                    counts = batch_data[col].value_counts()
                                                    for val, cnt in counts.items():
                                                        val = int(val) if isinstance(val, (int, float)) else val
                                                        rank_counts[f"{col}_{val}"] = int(cnt)
                                            dev_alert["rank_counts"] = rank_counts

                                        deviated_parameter_alerts.append(dev_alert)

                            if deviated_parameter_alerts:
                                deviated_doc = {
                                    "alert_id_tbm_ufm": alert_obj["alert_id_ufm_ufm"],
                                    "modelno": alert_obj["modelno"],
                                    "lot_id": alert_obj["lot_id"],
                                    "machine": final_machine_alerts[0]["machine"],
                                    "comments": [],
                                    "alert_assign_user": [],
                                    "tbm_machine_model_lot_id": None,
                                    "tbm_previous_machine_model_lot_id": None,
                                    "parameter_alerts": deviated_parameter_alerts,
                                    "timestamp_alert_generated": some_function(),
                                    "yield_type": "final"
                                }

                                # # Insert into original deviated collection (existing behavior)
                                # deviated_triggering_bulk_operations.append(pymongo.InsertOne(deviated_doc))

                                # NEW: Insert EXACT SAME document into Final Yield deviated DB
                                try:
                                    alert_deviated_final_yield_collect.insert_one(deviated_doc)
                                    logging.info(f"Deviated Final Yield Alert {alert_obj['alert_id_ufm_ufm']} → alert_deviated_final_yield")
                                    output_messages.append(f"Inserted Final Yield deviated alert {alert_obj['alert_id_ufm_ufm']} into alert_deviated_final_yield")
                                except Exception as e:
                                    logging.error(f"Failed insert into alert_deviated_final_yield: {e}")

            except Exception as e:
                logging.error(f"Error in Final Yield processing: {e}")
                output_messages.append(f"Error in Final Yield processing: {e}")
        
            # try:
            #     for alert_key, alert_obj in alert_map.items():
            #         true_machine_alerts = [ma for ma in alert_obj["machine_alerts"] if ma.get("ufm_shift_occured", False)]
            #         if not true_machine_alerts:
            #             continue

            #         virgin_barcodes_set = set(virgin_data["barcode"].dropna().astype(str))
            #         final_barcodes_set = set(final_data["barcode"].dropna().astype(str))

            #         # Virgin Yield
            #         virgin_machine_alerts = []
            #         for ma in true_machine_alerts:
            #             original_barcodes = ma.get("barcodes", [])
            #             if not original_barcodes:
            #                 continue
            #             filtered = [bc for bc in original_barcodes if str(bc) in virgin_barcodes_set]
            #             if filtered:
            #                 virgin_ma = ma.copy()
            #                 virgin_ma["barcodes"] = filtered
            #                 virgin_ma["barcode_count"] = len(filtered)
            #                 virgin_machine_alerts.append(virgin_ma)

            #         if virgin_machine_alerts:
            #             all_virgin_bcs = [bc for ma in virgin_machine_alerts for bc in ma["barcodes"]]
            #             doc_virgin = {
            #                 "alert_id_tbm_ufm": alert_obj["alert_id_ufm_ufm"],
            #                 "modelno": alert_obj["modelno"],
            #                 "lot_id": alert_obj["lot_id"],
            #                 "alert_flag": False,
            #                 "dashboard_flag": False,
            #                 "alert_status_check": "o",
            #                 "alert_status_timestamp": None,
            #                 "alert_status_comment": [],
            #                 "alert_assign_user": [],
            #                 "machine_alerts": virgin_machine_alerts,
            #                 "first_barcode_model": all_virgin_bcs[0] if all_virgin_bcs else None,
            #                 "last_barcode_model": all_virgin_bcs[-1] if all_virgin_bcs else None,
            #                 "timestamp_alert_generated": some_function(),
            #                 "yield_type": "virgin"
            #             }
            #             alert_triggering_virgin_yield_collect.insert_one(doc_virgin)

            #         # Final Yield
            #         final_machine_alerts = []
            #         for ma in true_machine_alerts:
            #             original_barcodes = ma.get("barcodes", [])
            #             if not original_barcodes:
            #                 continue
            #             filtered = [bc for bc in original_barcodes if str(bc) in final_barcodes_set]
            #             if filtered:
            #                 final_ma = ma.copy()
            #                 final_ma["barcodes"] = filtered
            #                 final_ma["barcode_count"] = len(filtered)
            #                 final_machine_alerts.append(final_ma)

            #         if final_machine_alerts:
            #             all_final_bcs = [bc for ma in final_machine_alerts for bc in ma["barcodes"]]
            #             doc_final = {
            #                 "alert_id_tbm_ufm": alert_obj["alert_id_ufm_ufm"],
            #                 "modelno": alert_obj["modelno"],
            #                 "lot_id": alert_obj["lot_id"],
            #                 "alert_flag": False,
            #                 "dashboard_flag": False,
            #                 "alert_status_check": "o",
            #                 "alert_status_timestamp": None,
            #                 "alert_status_comment": [],
            #                 "alert_assign_user": [],
            #                 "machine_alerts": final_machine_alerts,
            #                 "first_barcode_model": all_final_bcs[0] if all_final_bcs else None,
            #                 "last_barcode_model": all_final_bcs[-1] if all_final_bcs else None,
            #                 "timestamp_alert_generated": some_function(),
            #                 "yield_type": "final"
            #             }
            #             alert_triggering_final_yield_collect.insert_one(doc_final)

            # except Exception as e:
            #     logging.error(f"Error processing Virgin/Final yield for model {model_number} gt_code {gt_code}: {e}")

        except Exception as e:
            logging.error("UFM Error processing model %s: %s", model_number, e)
            output_messages.append(f"UFM Error processing model {model_number}: {e}")
            continue
        #                 if not alert_ufm_collect.find_one({"alert_id_ufm_ufm": last_alert_id}):
        #                     ufm_alert_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(alert_doc_main)))
        #                     output_messages.append(f"Prepared alert_id_ufm_ufm {last_alert_id} for insertion into alert_ufm_collect")

        #                 trigger_doc = {
        #                     "_id": f"trigger_alert_{last_alert_id}",
        #                     "alert_id_ufm_ufm": last_alert_id,
        #                     "modelno": model_number,
        #                     "lot_id": current_model_lot_id,
        #                     "alert_flag": False,
        #                     "dashboard_flag": False,
        #                     "alert_status_check": "o",
        #                     "alert_status_timestamp": None,
        #                     "alert_status_comment": [],
        #                     "alert_assign_user": [],
        #                     "machine_alerts": [machine_alert_detail],
        #                     "first_barcode_model": machine_alert_detail["first_barcode_machine"],
        #                     "last_barcode_model": machine_alert_detail["last_barcode_machine"],
        #                     "timestamp_alert_generated": some_function()
        #                 }
        #                 if not alert_ufm_collect.find_one({"alert_id_ufm_ufm": last_alert_id}):
        #                     ufm_triggering_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(trigger_doc)))
        #                     output_messages.append(f"Prepared trigger alert_id_ufm_ufm {last_alert_id} for insertion into alert_ufm_collect")
        #                 if not alert_collect.find_one({"alert_id_ufm_ufm": last_alert_id}):
        #                     ufm_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(trigger_doc)))
        #                     output_messages.append(f"Prepared trigger alert_id_ufm_ufm {last_alert_id} for insertion into alert_collect")

        #             except Exception as e:
        #                 logging.error("❌ UFM Error processing machine %s for model %s: %s", ufm_machine_id, model_number, e)
        #                 output_messages.append(f"❌ UFM Error processing machine {ufm_machine_id} for model {model_number}: {e}")
        #                 continue

        # except Exception as e:
        #     logging.error("❌ UFM Error processing model %s: %s", model_number, e)
        #     output_messages.append(f"❌ UFM Error processing model {model_number}: {e}")
        #     continue

    # Perform Bulk Writes
    final_log_messages = []
    if ufm_lot_bulk_operations:
        try:
            result = lot_ufm_collect.bulk_write(ufm_lot_bulk_operations, ordered=False)
            inserted_counts["lot_ufm_table"] = result.inserted_count
            final_log_messages.append(f"✅ UFM Inserted {result.inserted_count} documents into UFM lot collection.")
        except pymongo.errors.BulkWriteError as bwe:
            inserted_counts["lot_ufm_table"] = bwe.details['nInserted']
            final_log_messages.append(f"⚠️ UFM Lot Bulk Write Errors: Inserted {bwe.details['nInserted']}. Errors: {bwe.details['writeErrors']}")
        except Exception as e:
            final_log_messages.append(f"❌ UFM Error performing bulk write to UFM lot collection: {e}")

    if ufm_averages_bulk_operations:
        try:
            result = averages_ufm_collect.bulk_write(ufm_averages_bulk_operations, ordered=False)
            inserted_counts["averages_ufm_table"] = result.inserted_count
            final_log_messages.append(f"✅ UFM Inserted {result.inserted_count} documents into UFM averages collection.")
        except pymongo.errors.BulkWriteError as bwe:
            inserted_counts["averages_ufm_table"] = bwe.details['nInserted']
            final_log_messages.append(f"⚠️ UFM Averages Bulk Write Errors: Inserted {bwe.details['nInserted']}. Errors: {bwe.details['writeErrors']}")
        except Exception as e:
            final_log_messages.append(f"❌ UFM Error performing bulk write to UFM averages collection: {e}")

    if deviated_triggering_bulk_operations:
        try:
            result = alert_triggering_deviated_collect.bulk_write(deviated_triggering_bulk_operations, ordered=False)
            inserted_counts["alert_ufm_with_lot_size"] = result.inserted_count
            output_messages.append(f"✅ UFM Inserted {result.inserted_count} deviated alert documents into alert_deviated.alert_ufm_with_lot_size.")
        except Exception as e:
            error_message = f"❌ UFM Error performing bulk write to deviated alert collections: {e}"
            output_messages.append(error_message)
    else:
        output_messages.append("No UFM deviated alert documents to insert into deviated alert collections.")

    if ufm_alert_bulk_operations:
        try:
            result = alert_ufm_collect.bulk_write(ufm_alert_bulk_operations, ordered=False)
            inserted_counts["alert_ufm_collect"] = result.inserted_count  # MODIFIED: Use string key
            final_log_messages.append(f"✅ UFM Inserted {result.inserted_count} documents into UFM alert collection.")
        except pymongo.errors.BulkWriteError as bwe:
            inserted_counts["alert_ufm_collect"] = bwe.details['nInserted']  # MODIFIED: Use string key
            final_log_messages.append(f"⚠️ UFM Alert Bulk Write Errors: Inserted {bwe.details['nInserted']}. Errors: {bwe.details['writeErrors']}")
        except Exception as e:
            final_log_messages.append(f"❌ UFM Error performing bulk write to UFM alert collection: {e}")

    if ufm_triggering_bulk_operations:
        # NEW: Separate try-except for alert_ufm_collect
        try:
            result = alert_ufm_collect.bulk_write(ufm_triggering_bulk_operations, ordered=False)
            inserted_counts["alert_ufm_collect"] = result.inserted_count  # MODIFIED: Use string key
            final_log_messages.append(f"✅ UFM Inserted {result.inserted_count} documents into alert_db.{alert_triggering_ubno_collect_name}.")
        except pymongo.errors.BulkWriteError as bwe:
            inserted_counts["alert_ufm_collect"] = bwe.details['nInserted']  # MODIFIED: Use string key
            final_log_messages.append(f"⚠️ UFM Triggering Alert Bulk Write Errors in alert_db.{alert_triggering_ubno_collect_name}: Inserted {bwe.details['nInserted']}. Errors: {bwe.details['writeErrors']}")
        except Exception as e:
            final_log_messages.append(f"❌ UFM Error performing bulk write to alert_db.{alert_triggering_ubno_collect_name}: {e}")

        # NEW: Separate try-except for alert_db_dashboard
        try:
            result_dashboard = alert_triggering_collect_dashboard_ufm.bulk_write(ufm_triggering_bulk_operations, ordered=False)
            inserted_counts["alert_triggering_collect_dashboard_ufm"] = result_dashboard.inserted_count
            output_messages.append(f"✅ UFM Inserted {result_dashboard.inserted_count} machine_alert documents into alert_db_dashboard.{alert_triggering_ubno_collect_name}.")  # MODIFIED
        except pymongo.errors.BulkWriteError as bwe:
            inserted_counts["alert_triggering_collect_dashboard_ufm"] = bwe.details['nInserted']
            output_messages.append(f"⚠️ UFM Triggering Alert Bulk Write Errors in alert_db_dashboard.{alert_triggering_ubno_collect_name}: Inserted {bwe.details['nInserted']}. Errors: {bwe.details['writeErrors']}")  # NEW
        except Exception as e:
            output_messages.append(f"❌ UFM Error performing bulk write to alert_db_dashboard.{alert_triggering_ubno_collect_name}: {e}")  # NEW
    else:
        output_messages.append(f"No UFM triggering alert documents to insert into alert_db_dashboard.{alert_triggering_ubno_collect_name}.")  # NEW

    if ufm_bulk_operations:
        try:
            result = alert_collect.bulk_write(ufm_bulk_operations, ordered=False)
            inserted_counts["alert_collect"] = result.inserted_count  # MODIFIED: Use string key
            final_log_messages.append(f"✅ UFM Inserted {result.inserted_count} documents into UFM triggering alert collection.")
        except pymongo.errors.BulkWriteError as bwe:
            inserted_counts["alert_collect"] = bwe.details['nInserted']  # MODIFIED: Use string key
            final_log_messages.append(f"⚠️ UFM Triggering Alert Bulk Write Errors: Inserted {bwe.details['nInserted']}. Errors: {bwe.details['writeErrors']}")
        except Exception as e:
            final_log_messages.append(f"❌ UFM Error performing bulk write to UFM triggering alert collection: {e}")

    output_messages.extend(final_log_messages)

    # Final Update to merge_collect
    ufm_bulk_updates_merge = []
    processed_merge_df = merge_df.dropna(subset=["ufm_machine_model_lot_id"])

    if not processed_merge_df.empty:
        for index, row in processed_merge_df.iterrows():
            query = {"_id": row["_id"]}
            update_payload = {"$set": {
                "alert_id_ufm_ufm": row.get("alert_id_ufm_ufm"),
                "lot_id": row.get("lot_id"),
                "previous_lot_id": row.get("previous_lot_id"),
                "ufm_machine_model_lot_id": row.get("ufm_machine_model_lot_id"),
                "ufm_previous_machine_model_lot_id": row.get("ufm_previous_machine_model_lot_id")
            }}
            # MODIFIED: Only add tbm_machine to update if it exists in merge_df
            if "tbm_machine" in merge_df.columns:
                update_payload["$set"]["tbm_machine"] = row["tbm_machine"]
            ufm_bulk_updates_merge.append(pymongo.UpdateOne(query, update_payload))
            update_payload["$set"] = {k: v for k, v in update_payload["$set"].items() if v is not None}
            if update_payload["$set"]:
                ufm_bulk_updates_merge.append(pymongo.UpdateOne(query, update_payload))

        if ufm_bulk_updates_merge:
            logging.info("ℹ️ Preparing final update for %s documents in main merge collection...", len(ufm_bulk_updates_merge))
            try:
                result = merge_collect.bulk_write(ufm_bulk_updates_merge, ordered=False)
                updated_counts["merge_collection_ufm"] = result.modified_count
                logging.info("✅ UFM Updated %s documents in main merge collection with generated IDs.", result.modified_count)
            except pymongo.errors.BulkWriteError as bwe:
                updated_counts["merge_collection_ufm"] = bwe.details['nModified']
                logging.info("⚠️ UFM Final Merge Update Bulk Write Errors: Modified %s. Errors: %s", bwe.details['nModified'], bwe.details['writeErrors'])
            except Exception as e:
                logging.info("❌ UFM Error updating main merge collection: %s", e)
        else:
            logging.info("ℹ️ No final updates required for the main merge collection for UFM.")
    else:
        logging.info("ℹ️ No processed UFM rows found in merge_df for final update.")

    process_end_time = time.time()
    execution_time = process_end_time - process_start_time
    final_message = f"✅ UFM Lot and Alert Process completed successfully in {execution_time:.2f} seconds!"
    logging.info(final_message)

    logging.info("process_ufm_lot_and_alert_data_without_lot_size function is completed")
    return {
        "status": "success",
        "message": final_message,
        "output_log": output_messages,
        "inserted_counts": inserted_counts,
        "updated_counts": updated_counts,
        "execution_time": execution_time
    }, 200

##### Newly added below function # Add 05-07 start #######################

def ensure_admin_collection_exists():  # Add 05-07
    logging.info("ensure_admin_collection_exists function is just started")
    try:
        db = client[alert_mail_db]
        collection = db[admin_table]
        alert_users_collection = db[alert_users]  # New collection for copying data                                                              
        # Check if collection exists or is empty
        if admin_table not in db.list_collection_names() or collection.count_documents({}) == 0:
            logging.info(f"Collection '{admin_table}' is missing or empty. Inserting default admin user...")

            hashed_password = pwd_context.hash("1234")
            default_admin = {
                "user_id": 1,
                "email": "manager@apollotyres.com",
                "fname": "Admin",
                "lname": "User",
                "password": hashed_password,
                "role": "manager",
                "created_by_id": 1,
                "created_at": datetime.utcnow()
            }

            collection.insert_one(default_admin)
            alert_users_collection.insert_one(default_admin)  # Copy to alert_users                    
            logging.info("Default manager admin inserted.")
            return

        # Check if a manager role already exists
        if collection.count_documents({"role": "manager"}) == 0:
            logging.info(f"No user with role 'manager' found. Inserting default manager...")

            hashed_password = pwd_context.hash("1234")
            default_admin = {
                "user_id": 1,
                "email": "manager@apollotyres.com",
                "password": hashed_password,
                "role": "manager",
                "created_by_id": 1,
                "created_at": datetime.utcnow()
            }

            collection.insert_one(default_admin)
            alert_users_collection.insert_one(default_admin)  # Copy to alert_users                 
            logging.info("Default manager admin inserted into both admin_table and alert_users.")
        else:
            logging.info("At least one 'manager' user already exists in admin_table. No insert needed.")

    except Exception as e:
        logging.error(f"Error in ensure_admin_collection_exists: {str(e)}")

##### Newly added below function # Add 05-07 end #######################

def main():
    configure_logging()
    # Suppress all warnings
    warnings.filterwarnings("ignore")
    ensure_admin_collection_exists()
    output_messages = [] 
    # Run data cleaning

    if not df_cimcorp.empty:
        # Run data cleaning
        # Run data cleaning
        insert_with_timestamp(df_cimcorp, 'CIMCORP', 'CIMCORP')

        insert_with_timestamp(df_tub, 'TUB', 'TUB')
        data_processing_result = run_data_cleaning()
        lot_alert_result = process_lot_and_alert_data()

        if data_processing_result['status'] == 'success':
            logging.info("Data cleaning completed successfully.")
        else:
            logging.info("Data cleaning failed:", data_processing_result['message'])
            return

        # Process TBM lot and alert data
        logging.info("Starting process_tbm_lot_and_alert_data")
        tbm_process_output_data = process_tbm_lot_and_alert_data()
        if tbm_process_output_data[0]['status'] == 'success':
            logging.info("TBM lot and alert processing completed successfully.")
            output_messages = tbm_process_output_data[0]['output_log']
            tbm_lot_alert_results = tbm_process_output_data[0]
        else:
            logging.info("Data cleaning failed: %s", data_processing_result['message'])
            return tbm_process_output_data

        # Process Curing lot and alert data
        logging.info("Starting process_curing_data_and_generate_lots_alerts")
        curing_process_output_data, curing_status_code = process_curing_data_and_generate_lots_alerts()
        if curing_process_output_data['status'] == 'success':
            logging.info("Curing lot and alert processing completed successfully.")
            output_messages.extend(curing_process_output_data['output_log'])
            curing_lot_alert_results = curing_process_output_data
        else:
            logging.info("TBM lot and alert processing failed: %s", tbm_process_output_data[0]['message'])
            return curing_process_output_data, curing_status_code
        
        # Process ufm lot and alert data
        logging.info("Starting process_ufm_lot_and_alert_data without lot size")
        ufm_process_output_data = process_ufm_lot_and_alert_data_without_lot_size()
        if ufm_process_output_data[0]['status'] == 'success':
            logging.info("process_ufm_lot_and_alert_data_without_lot_size lot and alert processing completed successfully without lot size.")
            output_messages = ufm_process_output_data[0]['output_log']
            ufm_lot_alert_results = ufm_process_output_data[0]
        else:
            logging.info("UFM without lot and alert processing failed: %s", ufm_process_output_data['message'])
            return ufm_process_output_data
        

        logging.info("Starting process_tbm_lot_and_alert_data without lot size")
        tbm_process_output_data = process_tbm_lot_and_alert_data_without_lot_size()
        if tbm_process_output_data[0]['status'] == 'success':
            logging.info("process_tbm_lot_and_alert_data_without_lot_size and alert processing completed successfully.")
            output_messages = tbm_process_output_data[0]['output_log']
            tbm_lot_alert_results = tbm_process_output_data[0]
        else:
            logging.info("process_tbm_lot_and_alert_data_without_lot_size lot and alert processing failed: %s", tbm_process_output_data[0]['message'])
            return tbm_process_output_data
        
        # Process Curing lot and alert data
        logging.info("Starting process_curing_data_and_generate_lots_alerts_without_lot_size")
        curing_process_output_data, curing_status_code = process_curing_data_and_generate_lots_alerts_without_lot_size()
        if curing_process_output_data['status'] == 'success':
            logging.info("Curing lot and alert processing completed successfully.")
            output_messages.extend(curing_process_output_data['output_log'])
            curing_lot_alert_results = curing_process_output_data
        else:
            logging.info("process_curing_lot_and_alert_data_without_lot_size and alert processing failed: %s", curing_process_output_data[0]['message'])
            return curing_process_output_data, curing_status_code


        # Fianal success message
        logging.info("All processes completed successfully.")
        return {
            "status": "success",
            "message": "All processes completed successfully.",
            "output_log": output_messages,
            "tbm_lot_alert_results": tbm_lot_alert_results
            # "curing_lot_alert_results": curing_lot_alert_results
        }

    else:
        logging.info("CIMCORP data is empty, skipping CIMCORP processing.")

        insert_with_timestamp(df_tub, 'TUB', 'TUB')
        # insert_with_timestamp(df_cimcorp, 'CIMCORP', 'CIMCORP')
        data_processing_result = run_data_cleaning()
        lot_alert_result = process_lot_and_alert_data()

        if data_processing_result['status'] == 'success':
            logging.info("Data cleaning completed successfully.")
        else:
            logging.info("Data cleaning failed: %s", data_processing_result['message'])
            return

        # Process ufm lot and alert data
        logging.info("Starting process_ufm_lot_and_alert_data without lot size")
        ufm_process_output_data = process_ufm_lot_and_alert_data_without_lot_size()
        if ufm_process_output_data[0]['status'] == 'success':
            logging.info("process_ufm_lot_and_alert_data_without_lot_size lot and alert processing completed successfully without lot size.")
            output_messages = ufm_process_output_data[0]['output_log']
            ufm_lot_alert_results = ufm_process_output_data[0]
        else:
            logging.info("process_ufm_lot_and_alert_data_without_lot_size lot and alert processing failed:", ufm_process_output_data[0]['message'])
            return ufm_process_output_data
        
        # Final success message
        logging.info("All processes completed successfully.")
        return {
            "status": "success",
            "message": "All processes completed successfully.",
            "output_log": output_messages,
            # "tbm_lot_alert_results": tbm_lot_alert_results
            # "curing_lot_alert_results": curing_lot_alert_results
        }

if __name__ == "__main__":
    result = main()
    # logging.info(result)
    # logging.info("Final result: %s", result)
    client.close()