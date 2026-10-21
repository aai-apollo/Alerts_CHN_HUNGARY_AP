from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
import pandas as pd
import re
from datetime import datetime, timezone, time as datetime_time
from datetime import datetime, date  # Ensure both datetime and date are imported
import traceback
import os
import numpy as np
import time
import redshift_connector								
import pytz
import pymongo
from pymongo import MongoClient
from datetime import datetime, timedelta
from pytz import timezone
import logging
from datetime import datetime
from passlib.context import CryptContext
import csv
import warnings
import bcrypt
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
from config_hungary_uat import (
    client,
    FILE_TRACKING_DB,
    FILE_TRACKING_COLLECTION,
    INVALID_ROWS_DB,
    alert_table,
    lot_db,
    average_db,
    alert_db,
    alert_deviated,
    alert_db_dashboard,
    alert_db_virgin_yield,
    alert_db_final_yield,
    channel_no_collect,
    channel_no_collect_tbm,
    channel_no_collect_curing,
    lot_tbm_collect_name,
    lot_tbm_collect_name_new,
    lot_curing_collect_name,
    lot_curing_collect_name_new,
    lot_table_collect,
    lot_ufm_collect_name_new,
    alert_tbm_collect_name,
    alert_tbm_collect_name_new,
    alert_curing_collect_name,
    alert_curing_collect_name_new,
    alert_ufm_collect_name,
    alert_ufm_collect_name_new,
    averages_tbm_table,
    averages_tbm_table_new,
    averages_curing_table_name,
    averages_curing_table_name_new,
    averages_ufm_table,
    averages_ufm_table_new,
    machine_no_collect,
    curing_press_dict_collect,
    ub_machine_no_collect,
    alert_triggering_collect_name,
    alert_triggering_collect_name_new,
    alert_triggering_curing_collect_name,
    alert_triggering_curing_collect_name_new,
    alert_triggering_ubno_collect_name,
    alert_triggering_ubno_collect_name_new,
    alert_triggering_deviated_tbm,
    alert_triggering_deviated_tbm_new,
    alert_triggering_deviated_curing,
    alert_triggering_deviated_curing_new,
    alert_triggering_deviated_ufm,
    alert_triggering_deviated_ufm_new,
    alert_triggering_collect_name,
    tbm_collection,
    tbm_collection_machine,
    curing_collection_machine,
    ufm_collection_machine,
    re_codes,
    model_db,
    alert_mail_db,
    admin_table,
    alert_users,
    TUB_CIMCORP_VISUAL_MERGED_DB_alert,
    DATA_MERGED_SUMMARY_COLLECTION_alert,
    DATA_MERGED_SCRAP_COLLECTION_alert,
    DATA_MERGED_SUMMARY_COLLECTION_alert_new,
    DATA_MERGED_SCRAP_COLLECTION_alert_new,
    config_databse,
    model_no_status_collection,
)

# =============================================================================
# NO-SAVE PATCH — writes to these DBs are silently suppressed.
# Reads return empty results so all logic continues unchanged.
# =============================================================================
_BLOCKED_DBS = {alert_db, alert_db_dashboard, alert_deviated, alert_table, lot_db, average_db}

class _NoOpCollection:
    """Drop-in replacement for a PyMongo collection that discards all writes."""
    def insert_one(self, *a, **kw):        return type('R', (), {'inserted_id': None})()
    def insert_many(self, *a, **kw):       return type('R', (), {'inserted_ids': []})()
    def bulk_write(self, *a, **kw):        return type('R', (), {'inserted_count': 0, 'modified_count': 0, 'upserted_count': 0})()
    def update_one(self, *a, **kw):        return type('R', (), {'modified_count': 0})()
    def update_many(self, *a, **kw):       return type('R', (), {'modified_count': 0})()
    def delete_one(self, *a, **kw):        return type('R', (), {'deleted_count': 0})()
    def delete_many(self, *a, **kw):       return type('R', (), {'deleted_count': 0})()
    def find_one(self, *a, **kw):          return None
    def find(self, *a, **kw):              return iter([])
    def aggregate(self, *a, **kw):         return iter([])
    def distinct(self, *a, **kw):          return []
    def count_documents(self, *a, **kw):   return 0
    def create_index(self, *a, **kw):      return None
    def __getattr__(self, name):           return lambda *a, **kw: None

class _NoOpDB:
    """Drop-in replacement for a PyMongo database that returns NoOpCollections."""
    def __getitem__(self, name):           return _NoOpCollection()
    def __getattr__(self, name):           return _NoOpCollection()

_original_getitem = client.__class__.__getitem__

def _patched_getitem(self, db_name):
    if db_name in _BLOCKED_DBS:
        return _NoOpDB()
    return _original_getitem(self, db_name)

client.__class__.__getitem__ = _patched_getitem
# =============================================================================

app = Flask(__name__)
app.secret_key = "supersecretkey"

def configure_logging():
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    current_date = datetime.now().strftime('%Y-%m-%d')
    log_filename = f'logs/log_alerts_{current_date}.txt'
    
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, mode='a', encoding='utf-8')
        ]
    )
    warnings.simplefilter("always")
    logging.captureWarnings(True)
    

# Call this at the beginning of your script           
# Define yield calculation functions
def yield_percentage(numerator, denominator):
    return (numerator / denominator * 100) if denominator > 0 else 0

def rfv_yield(batch_data):         # Add 05-07
    try:
        denominator_data = batch_data[
            ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
            batch_data['barcode'].notnull()
        ]
        denominator = len(denominator_data)
        numerator_data = denominator_data[
            (denominator_data['rfppwcgrade'].isin([1])) &
            denominator_data['barcode'].notnull()
        ]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def rfh1_yield(batch_data):         # Add 05-07
    try:
        denominator_data = batch_data[
            ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
            batch_data['barcode'].notnull()
        ]
        denominator = len(denominator_data)
        numerator_data = denominator_data[
            (denominator_data['rfh1wcgrade'].isin([1])) &
            denominator_data['barcode'].notnull()
        ]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def rfh2_yield(batch_data):         # Add 05-07
    try:
        denominator_data = batch_data[
            ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
            batch_data['barcode'].notnull()
        ]
        denominator = len(denominator_data)
        numerator_data = denominator_data[
            (denominator_data['rfh2wcgrade'].isin([1])) &
            denominator_data['barcode'].notnull()
        ]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def lfv_yield(batch_data):         # Add 05-07
    try:
        denominator_data = batch_data[
            ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
            batch_data['barcode'].notnull()
        ]
        denominator = len(denominator_data)
        numerator_data = denominator_data[
            (denominator_data['lfppwcgrade'].isin([1])) &
            denominator_data['barcode'].notnull()
        ]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def conicity_yield(batch_data):        # Add 05-07
    try:
        denominator_data = batch_data[
            ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
            batch_data['barcode'].notnull()
        ]
        denominator = len(denominator_data)
        numerator_data = denominator_data[
            (denominator_data['conygrade'].isin([1])) &
            denominator_data['barcode'].notnull()
        ]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0
    
def plysteer_yield(batch_data):        # Add 05-07
    try:
        denominator_data = batch_data[
            ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
            batch_data['barcode'].notnull()
        ]
        denominator = len(denominator_data)
        numerator_data = denominator_data[
            (denominator_data['plysgrade'].isin([1])) &
            denominator_data['barcode'].notnull()
        ]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def center_radial_run_out_yield(batch_data):        # Add 05-07
    try:
        denominator_data = batch_data[
            ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
            batch_data['barcode'].notnull()
        ]
        denominator = len(denominator_data)
        numerator_data = denominator_data[
            (denominator_data['crrograde'].isin([1])) &
            denominator_data['barcode'].notnull()
        ]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def top_radial_run_out_yield(batch_data):        # Add 05-07
    try:
        denominator_data = batch_data[
            ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
            batch_data['barcode'].notnull()
        ]
        denominator = len(denominator_data)
        numerator_data = denominator_data[
            (denominator_data['trrograde'].isin([1])) &
            denominator_data['barcode'].notnull()
        ]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def bottom_radial_run_out_yield(batch_data):        # Add 05-07
    try:
        denominator_data = batch_data[
            ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
            batch_data['barcode'].notnull()
        ]
        denominator = len(denominator_data)
        numerator_data = denominator_data[
            (denominator_data['brrograde'].isin([1])) &
            denominator_data['barcode'].notnull()
        ]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def lateral_run_out_yield(batch_data):        # Add 05-07
    try:
        denominator_data = batch_data[
            ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
            batch_data['barcode'].notnull()
        ]
        denominator = len(denominator_data)
        numerator_data = denominator_data[
            (denominator_data['lrograde'].isin([1])) &
            denominator_data['barcode'].notnull()
        ]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def lateral_run_out_harmonic_first_yield(batch_data):        # Add 05-07
    try:
        denominator_data = batch_data[
            ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
            batch_data['barcode'].notnull()
        ]
        denominator = len(denominator_data)
        numerator_data = denominator_data[
            (denominator_data['lroh1grade'].isin([1])) &
            denominator_data['barcode'].notnull()
        ]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def center_radial_run_out_harmonic_first_yield(batch_data):        # Add 05-07
    try:
        denominator_data = batch_data[
            ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
            batch_data['barcode'].notnull()
        ]
        denominator = len(denominator_data)
        numerator_data = denominator_data[
            (denominator_data['crroh1grade'].isin([1])) &
            denominator_data['barcode'].notnull()
        ]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def top_radial_run_out_harmonic_first_yield(batch_data):        # Add 05-07
    try:
        denominator_data = batch_data[
            ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
            batch_data['barcode'].notnull()
        ]
        denominator = len(denominator_data)
        numerator_data = denominator_data[
            (denominator_data['trroh1grade'].isin([1])) &
            denominator_data['barcode'].notnull()
        ]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def bottom_radial_run_out_harmonic_first_yield(batch_data):        # Add 05-07
    try:
        denominator_data = batch_data[
            ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
            batch_data['barcode'].notnull()
        ]
        denominator = len(denominator_data)
        numerator_data = denominator_data[
            (denominator_data['brroh1grade'].isin([1])) &
            denominator_data['barcode'].notnull()
        ]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def lateral_radial_run_out_yield(batch_data):        # Add 05-07
    try:
        denominator_data = batch_data[
            ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
            batch_data['barcode'].notnull()
        ]
        denominator = len(denominator_data)
        numerator_data = denominator_data[
            (denominator_data['lrrograde'].isin([1])) &
            denominator_data['barcode'].notnull()
        ]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def static_yield(batch_data):        # Add 05-07
    try:
        denominator_data = batch_data[
            ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
            batch_data['barcode'].notnull()
        ]
        denominator = len(denominator_data)
        numerator_data = denominator_data[
            (denominator_data['sbalgrade'].isin([1])) &
            denominator_data['barcode'].notnull()
        ]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def couple_yield(batch_data):        # Add 05-07
    try:
        denominator_data = batch_data[
            ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
            batch_data['barcode'].notnull()
        ]
        denominator = len(denominator_data)
        numerator_data = denominator_data[
            (denominator_data['couplegrade'].isin([1])) &
            denominator_data['barcode'].notnull()
        ]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def upper_yield(batch_data):        # Add 05-07
    try:
        denominator_data = batch_data[
            ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
            batch_data['barcode'].notnull()
        ]
        denominator = len(denominator_data)
        numerator_data = denominator_data[
            (denominator_data['uppergrade'].isin([1])) &
            denominator_data['barcode'].notnull()
        ]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def lower_yield(batch_data):        # Add 05-07
    try:
        denominator_data = batch_data[
            ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
            batch_data['barcode'].notnull()
        ]
        denominator = len(denominator_data)
        numerator_data = denominator_data[
            (denominator_data['lowergrade'].isin([1])) &
            denominator_data['barcode'].notnull()
        ]
        numerator = len(numerator_data['barcode'])
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def overall_uniformity_yield(batch_data):
    try:
        # --- Measured (overallufmgrade in 1 to 5) ---
        measured_data = batch_data[
            (batch_data['overallufmgrade'].isin([1, 2, 3, 4, 5])) &
            (batch_data['barcode'].notnull())
        ]
        measured = len(measured_data)

        # --- VI Downgrade (vi_yield = 0 within measured) ---
        vi_downgrade_data = batch_data[
            (batch_data['vi_yield'] == 0) & (batch_data['last'] == 1) & (batch_data["vi_yield"].notna())
        ]
        vi_downgrade = len(vi_downgrade_data)

        # --- Denominator = Measured - VI downgrade ---
        denominator = measured - vi_downgrade

        # --- UF pass as OE (grade = 1, excluding VI downgrade) ---
        numerator_data = batch_data[
            (batch_data['overallufmgrade'] == 1) &
            (batch_data['last'] == 1)
        ]
        numerator = len(numerator_data)

        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0

# def overall_visual_yield(batch_data):
#     try:
#         # --- VI Downgrade (vi_yield = 0 and last = 1) ---
#         vi_downgrade_data = batch_data[
#             (batch_data['vi_yield'] == 0) &
#             (batch_data['last'] == 1)
#         ]
#         vi_downgrade = len(vi_downgrade_data)

#         # --- VI Scrap (scrap_category = "VISUAL") ---
#         vi_scrap_data = batch_data[
#             batch_data['scrap_category_tub'] == "VISUAL"
#         ]
#         vi_scrap = len(vi_scrap_data)

#         # --- overallufmgrade IN (1,2,3,4,5) ---
#         ufm_data = batch_data[
#             batch_data['overallufmgrade'].isin([1, 2, 3, 4, 5])
#         ]
#         ufm_count = len(ufm_data)

#         # --- Possible OE count ---
#         possible_oe = (ufm_count + vi_scrap) - vi_downgrade

#         # --- VI Pass ---
#         vi_pass = possible_oe  # as per formula

#         # --- Denominator = VI pass + VI Scrap ---
#         denominator = vi_pass + vi_scrap

#         # --- Numerator = VI pass (OE possible count - VI downgrade already applied) ---
#         numerator = vi_pass

#         return yield_percentage(numerator, denominator)

#     except Exception:
#         return 0.0

def overall_visual_yield(batch_data):
    try:
        # --- VI Downgrade (vi_yield = 0 and last = 1) ---
        vi_downgrade_data = batch_data[
            (batch_data['vi_yield'] == 0) &
            (batch_data['last'] == 1)
        ]
        vi_downgrade = len(vi_downgrade_data)

        # --- VI Scrap (scrap_category = "VISUAL") ---
        vi_scrap_data = batch_data[
            batch_data['scrap_category_tub'] == "VISUAL"
        ]
        vi_scrap = len(vi_scrap_data)

        # --- overallufmgrade IN (1,2,3,4,5) ---
        ufm_data = batch_data[
            batch_data['overallufmgrade'].isin([1, 2, 3, 4, 5])
        ]
        ufm_count = len(ufm_data)

        # --- Possible OE count ---
        possible_oe = (ufm_count + vi_scrap)

        # --- VI Pass ---
        vi_pass = possible_oe  # as per formula
        

        # --- Denominator = VI pass + VI Scrap ---
        denominator = vi_pass

        # --- Numerator = VI pass (OE possible count - VI downgrade already applied) ---
        numerator = (ufm_count - vi_downgrade)

        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0

def overall_warehouse_yield(batch_data):
    try:
        # --- Measured (overallufmgrade in 1 to 5) ---
        measured_data = batch_data[
            (batch_data['overallufmgrade'].isin([1, 2, 3, 4, 5])) &
            (batch_data['barcode'].notnull())
        ]
        measured = len(measured_data)

        # --- VI Scrap (Visual category) ---
        vi_scrap_data = batch_data[
            (batch_data['scrap_category_tub'] == "VISUAL")]
        vi_scrap = len(vi_scrap_data)

        # --- Denominator = Measured + VI scrap ---
        denominator = measured + vi_scrap

        # --- Pass as OE ---
        numerator_data = batch_data[
            (batch_data['overallufmgrade'] == 1) &
            (batch_data['last'] == 1)
        ]
        numerator = len(numerator_data)

        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0
    
def overall_scrap_yield(batch_data):
    try:
        # --- Measured (overallufmgrade in 1 to 5) ---
        measured_data = batch_data[
            (batch_data['overallufmgrade'].isin([1, 2, 3, 4, 5])) &
            (batch_data['barcode'].notnull())
        ]
        measured = len(measured_data)

        # --- Visual Scrap ---
        visual_scrap = batch_data[
            (batch_data['scrap_category_tub'] == "VISUAL") &
            (batch_data['barcode'].notnull())
        ]
        vi_scrap = len(visual_scrap)

        # --- Denominator = Measured + VI scrap ---
        denominator = measured + vi_scrap
        
        # # --- Visual Scrap ---
        # visual_scrap = batch_data[
        #     (batch_data['scrap_category_tub'] == "VISUAL") &
        #     (batch_data['barcode'].notnull())
        # ]
        # vi_scrap = len(visual_scrap)

        # --- UF Scrap ---
        uf_scrap = batch_data[
            (batch_data['scrap_category_tub'] == "UF") &
            (batch_data['barcode'].notnull())
        ]

        # --- Visual Scrap (reuse above) ---
        numerator = len(uf_scrap) + len(visual_scrap)

        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0

def visual_loss_yield(batch_data):
    try:
        # --- Visual Scrap ---
        visual_scrap_data = batch_data[
            (batch_data['scrap_category_tub'] == "VISUAL") &
            (batch_data["scrap_category_tub"].notna()) &
            (batch_data['barcode'].notnull())
        ]
        visual_scrap = len(visual_scrap_data)

        # --- VI Downgrade ---
        vi_downgrade_data = batch_data[
            (batch_data['vi_yield'] == 0) &
            (batch_data['last'] == 1) &
            (batch_data['barcode'].notnull())
        ]
        vi_downgrade = len(vi_downgrade_data)

        # --- Total Visual Loss ---
        visual_loss = visual_scrap + vi_downgrade

        return visual_loss   # For pie chart absolute value

    except Exception:
        return 0
    
def uf_loss_yield(batch_data):
    try:
        # --- UF Scrap ---
        uf_scrap_data = batch_data[
            (batch_data['scrap_category_tub'] == "UF") &
            (batch_data['barcode'].notnull())
        ]
        uf_scrap = len(uf_scrap_data)
 
        # --- UF Downgrade ---
        uf_downgrade_data = batch_data[
            (batch_data['vi_yield'] == 1) &
            (batch_data['last'] == 1) &
            (
                (batch_data['overalltugrade'] > 1) |
                (batch_data['overallbalgrade'] > 1) |
                (batch_data['overallsolgrade'] > 1)
            ) &
            (batch_data['barcode'].notnull())
        ]
        uf_downgrade = len(uf_downgrade_data)
 
        # --- Total UF Loss ---
        uf_loss = uf_scrap + uf_downgrade
 
        return uf_loss  # For pie chart absolute value
 
    except Exception:
        return 0
    
def oe_ok_yield(batch_data):
    try:
        # --- UF OE OK ---
        uf_oe = batch_data[
            (batch_data['overallufmgrade'].isin([1])) &
            (batch_data['barcode'].notnull())
        ]
        uf_pass = len(uf_oe)

        # --- Total OE Ok Loss ---
        uf_oe_ok = uf_pass

        return uf_oe_ok   # For pie chart absolute value

    except Exception:
        return 0

db_tracking = client[FILE_TRACKING_DB]
tracking_collection = db_tracking[FILE_TRACKING_COLLECTION]

def some_function():
    india_time = datetime.now(pytz.timezone('Asia/Kolkata'))
    return india_time

logging.info(some_function())

DEFAULT_DATETIME = datetime(1900, 1, 1, 12, 30, 0)
DEFAULT_DATE = datetime(1900, 1, 1)

file_tracking_db = client['file_tracking']
cimcorp_db = client['VISUAL']
tub_db = client['TUB_CIMCORP']

def insert_with_timestamp(df, collection_name_prefix, db_name):
    if not isinstance(df, pd.DataFrame):
        logging.error(f"Input 'df' is not a pandas DataFrame, got type: {type(df)}")
        raise TypeError("Expected a pandas DataFrame as input")
    
    df = df.copy()
    logging.info(f"Inserting DataFrame with columns: {list(df.columns)}")
    logging.info(f"DataFrame dtypes:\n{df.dtypes}")

    # Sanitize all NaT / tz-aware datetimes before MongoDB insert
    df = sanitize_df_for_mongo(df)

    # Convert any datetime.date objects to strings
    for col in df.select_dtypes(include=['object']).columns:
        if any(isinstance(x, date) and not isinstance(x, datetime) for x in df[col] if x is not None):
            logging.info(f"Converting datetime.date to string in column: {col}")
            df[col] = df[col].apply(
                lambda x: x.strftime('%Y-%m-%d') if isinstance(x, date) and not isinstance(x, datetime) and x is not None else x
            )

    timestamp = some_function().strftime("%Y%m%d_%H%M%S")
    if db_name == 'VISUAL':
        db = cimcorp_db
    elif db_name == 'TUB_CIMCORP':
        db = tub_db
    else:
        raise ValueError("Invalid database name")
    
    collection_name = f"{collection_name_prefix}_{timestamp}"
    collection = db[collection_name]
    collection.insert_many(df_to_mongo_records(df))
    
    track_file_in_file_tracking(collection_name_prefix, timestamp, len(df), db_name)

def track_file_in_file_tracking(file_prefix, timestamp, num_rows, db_name):
    file_tracking = file_tracking_db['file_metadata']
    tracking_entry = {
        "filename": f"{file_prefix}_{timestamp}",
        "inserted_timestamp": some_function(),
        "num_rows": num_rows,
        "database_name": db_name,
        "processed": False
    }
    file_tracking.insert_one(tracking_entry)

class RedshiftDataConnectorr:
    def __init__(self):
        self.conn = redshift_connector.connect(
            host='atl-dl-prd-01.cuvxusm4qify.ap-south-1.redshift.amazonaws.com',
            database='db_prd_plant_gyo',
            port=5439,
            user='usr_gobinath_b',
            password='nl2k#Z#u74FZ#2uP', sslmode='disable'
        )
    def execute_query(self, q):
        data = pd.read_sql(q, self.conn)
        # Strip timezone from all datetime columns — NaT with tz causes
        # "NaTType does not support utcoffset" when serialising to MongoDB
        for col in data.select_dtypes(include=['datetimetz', 'datetime64[ns, UTC]',
                                               'datetime64[ns]']).columns:
            try:
                data[col] = data[col].dt.tz_localize(None) if data[col].dt.tz is None \
                    else data[col].dt.tz_convert(None)
            except Exception:
                data[col] = pd.to_datetime(data[col], errors='coerce').dt.tz_localize(None)
        return data
    
inst = RedshiftDataConnectorr()

################ cronjob setup START ######################
from datetime import datetime, timedelta
import pytz

utc = pytz.utc

## ---------------- CURRENT UTC TIME ----------------
now_utc = datetime.now(utc)

# Example: if cron runs at 11:22 → we force it to 11:00
end_dt = now_utc.replace(minute=0, second=0, microsecond=0)

# Take previous 2 full hours
start_dt = end_dt - timedelta(hours=11)

# Format
start_timestamp = start_dt.strftime("%Y-%m-%d %H:%M:%S")
end_timestamp   = end_dt.strftime("%Y-%m-%d %H:%M:%S")

print("start_timestamp (UTC):", start_timestamp)
print("end_timestamp (UTC):", end_timestamp)

# ############### cronjob setup END ######################

################ Manual trigger setup START ######################
# local_tz = pytz.timezone("Asia/Kolkata")

# utc = pytz.utc

# # ---------------- CURRENT UTC TIME ----------------
# local_tz = datetime.now(utc)

# # ---------------- MANUAL INPUT ----------------
# manual_start_date = '2026-09-09'
# manual_end_date   = '2026-09-10'
# manual_start_time = '04:00:00'
# manual_end_time   = '05:00:00'

# # ---------------- TIME WINDOW LOGIC ----------------
# if all([manual_start_date, manual_end_date, manual_start_time, manual_end_time]):
#     # ✅ MANUAL MODE — use exactly what user provides
#     start_timestamp = f"{manual_start_date} {manual_start_time}"
#     end_timestamp   = f"{manual_end_date} {manual_end_time}"

# else:
#     # ✅ AUTO MODE — last 48 hours
#     now = datetime.now(pytz.utc).astimezone(local_tz)
#     end_dt = now.replace(minute=0, second=0, microsecond=0)
#     start_dt = end_dt - timedelta(hours=48)

#     start_timestamp = start_dt.strftime("%Y-%m-%d %H:%M:%S")
#     end_timestamp   = end_dt.strftime("%Y-%m-%d %H:%M:%S")

# # ---------------- FINAL CHECK ----------------
# print("start_timestamp:", start_timestamp)
# print("end_timestamp:", end_timestamp)
############### Manual trigger setup END ######################

# mongo_db = client["production_db_hungary"]
# channel_coll = mongo_db["channel_no_ufm"]

# # # -------- Fetch model numbers --------
# modelno_list = channel_coll.distinct("sku")

# # Keep all SKU strings as-is (SKUs are alphanumeric e.g. "GV2554520ULAWB")
# modelno_list = [str(m) for m in modelno_list if m]

# if not modelno_list:
#     print("Warning: No model numbers found in MongoDB — continuing with hardcoded SKU list in queries.")
# else:
#     print("Model numbers from MongoDB:", modelno_list)

# modelno_sql = ",".join(f"'{m}'" for m in modelno_list)

# to check the sku's ---
mongo_db = client["production_db_hungary"]
channel_coll = mongo_db["channel_no_ufm"]

# # -------- Fetch model numbers --------
# Only fetch SKUs where oe_replacement_slob == "OE"
modelno_list = channel_coll.distinct("sku", {"oe_replacement_slob": "OE"})

# Keep all SKU strings as-is (SKUs are alphanumeric e.g. "GV2554520ULAWB")
modelno_list = [str(m) for m in modelno_list if m]

if not modelno_list:
    print("Warning: No model numbers found in MongoDB — continuing with hardcoded SKU list in queries.")
else:
    print("Model numbers from MongoDB (OE only):", modelno_list)

modelno_sql = ",".join(f"'{m}'" for m in modelno_list)


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

# =================================================
# QUERY 1 — UFM + TBM + CIMCORP (merged summary)
# =================================================
q1 = f"""
SELECT 
    d.*, 
    prod.*, 
    ch.*
FROM (
    SELECT 
        ztc.barcode,
        ztc.sku,
        ztc.equipmentid AS UF_ID,
        ztc.timestamp AS UF_time,

        overallufmgrade, overalltugrade, overallbalgrade, overallsolgrade,

        rfppwcgrade, rfh1wcgrade, rfh2wcgrade, rfh3wcgrade, rfh4wcgrade,
        rfh5wcgrade, rfh6wcgrade, rfh7wcgrade, rfh8wcgrade, rfh9wcgrade,
        rfh10wcgrade, rfh11wcgrade, rfh12wcgrade, rfh13wcgrade, rfh14wcgrade,
        rfh15wcgrade,

        lfppwcgrade, lfh1wcgrade, lfh2wcgrade, lfh3wcgrade, lfh4wcgrade,
        lfh5wcgrade, lfh6wcgrade, lfh7wcgrade, lfh8wcgrade, lfh9wcgrade,
        lfh10wcgrade, lfh11wcgrade, lfh12wcgrade, lfh13wcgrade, lfh14wcgrade,
        lfh15wcgrade, lfh16wcgrade,

        conygrade, plysgrade, sbalgrade, couplegrade,
        uppergrade, lowergrade, lrograde, lroh1grade,
        crrograde, crroh1grade, trrograde, trroh1grade,
        brrograde, brroh1grade, tdipgrade, lrrograde,

        rfppcc* 0.101972 AS rfppcc,
        rfppcw * 0.101972 AS rfppcw,
        rfh1cc * 0.101972 AS rfh1cc,
        rfh1cw * 0.101972 AS rfh1cw,
        rfh2cc * 0.101972 AS rfh2cc,
        rfh2cw * 0.101972 AS rfh2cw,
        lfppcc * 0.101972 AS lfppcc,
        lfppcw * 0.101972 AS lfppcw,
        
        cony * 0.101972 AS cony,
        plys * 0.101972 AS plys,

        sbal, upper, lower, lrot, lrob, crro, trro, brro,
        tbul, tdep, bbul, bdep,
        bulwcgrade, depwcgrade, aircc, aircw, loadcc, loadcw,

        -- YIELDS
        (CASE WHEN overallufmgrade > 1 THEN 0 ELSE 1 END) * 1.0 AS OE_Total_Yield,
        (CASE WHEN overalltugrade > 1 THEN 0 ELSE 1 END) * 1.0 AS OE_TU_Yield,
        (CASE WHEN overallsolgrade > 1 THEN 0 ELSE 1 END) * 1.0 AS OE_TG_Yield,
        (CASE WHEN overallbalgrade > 1 THEN 0 ELSE 1 END) * 1.0 AS OE_TB_Yield,

        (CASE WHEN overallufmgrade = 3 THEN 1 ELSE 0 END) * 1.0 AS REP_Total_Yield,
        (CASE WHEN overalltugrade = 3 THEN 1 ELSE 0 END) * 1.0 AS REP_U_Yield,
        (CASE WHEN overallsolgrade = 3 THEN 1 ELSE 0 END) * 1.0 AS REP_TG_Yield,
        (CASE WHEN overallbalgrade = 3 THEN 1 ELSE 0 END) * 1.0 AS REP_TB_Yield,

        (CASE WHEN overallufmgrade = 4 THEN 1 ELSE 0 END) * 1.0 AS Rework_Total,
        (CASE WHEN overalltugrade = 4 THEN 1 ELSE 0 END) * 1.0 AS Rework_TU,
        (CASE WHEN overallsolgrade = 4 THEN 1 ELSE 0 END) * 1.0 AS Rework_TG,
        (CASE WHEN overallbalgrade = 4 THEN 1 ELSE 0 END) * 1.0 AS Rework_TB,

        (CASE WHEN overallufmgrade = 5 THEN 1 ELSE 0 END) * 1.0 AS Scrap_Total,
        (CASE WHEN overalltugrade = 5 THEN 1 ELSE 0 END) * 1.0 AS Scrap_TU,
        (CASE WHEN overallsolgrade = 5 THEN 1 ELSE 0 END) * 1.0 AS Scrap_TG,
        (CASE WHEN overallbalgrade = 5 THEN 1 ELSE 0 END) * 1.0 AS Scrap_TB

    FROM sc_apollo_mom.t_zf_test_completed ztc
    WHERE ztc.timestamp BETWEEN '{start_timestamp}' AND '{end_timestamp}'
      AND ztc.sku IN ({modelno_sql})
) d

LEFT JOIN (
    SELECT 
        equipmentid AS PTB_ID,
        producedon AS PTB_time,
        vrl_vhmc_mes_gt_actual_weight AS act_weight,
        vst_vhmc_mes_gt_barcode,
        vst_vhmc_mes_gt_code AS gt_code,
        vrl_vhmc_mes_br1_spot_position, 
        vrl_vhmc_mes_br2_spot_position, 
        vrl_vhmc_mes_cwap_spot_pos, 
        vrl_vhmc_mes_td_spot_position, 
        vrl_vhmc_mes_pa_spot_position, 
        vrl_vhmc_mes_barcode_spot_pos, 
        vrl_vhmc_mes_bp1_spot_pos, 
        vrl_vhmc_mes_bp2_spot_pos
    FROM sc_mhshistory.t_tbm_pcr_productiondata
) prod
ON prod.vst_vhmc_mes_gt_barcode = d.barcode

LEFT JOIN (
    SELECT 
        curingpressid AS CUR_ID,
        curingtimestamp AS Cur_time,
        barcodeid,
        curingmouldid as cur_mouldid
    FROM sc_mhshistory.t_curing_history
) ch
ON ch.barcodeid = d.barcode
"""

print("#########################")

df_tub = inst.execute_query(q1)

# =================================================
# QUERY 2 — SCRAP / REPAIR DATA (data_merged_scrap)
# =================================================
q2 = f"""
WITH barcode_data AS (
    SELECT 
        barcode,
        "timestamp"
    FROM sc_apollo_mom.t_zf_test_completed
    WHERE "timestamp" >= '{start_timestamp}'
      AND "timestamp" < '{end_timestamp}'
      AND sku IN ({modelno_sql})
)

SELECT
    overview.stt_id,
    overview.tire_barcode,
    overview.gt_weighting_time,
    overview.ft_code,
    overview.vi_result,
    overview.ufm_result,
    overview.bal_result,
    overview.sol_result,
    overview.loc_name,
    overview.ct_code as sku,
    overview.last_change_time,
    overview.tbm_name as tbm,
    overview.cp_name as equ,
    repair.sur_stt_id,
    repair.sur_loc_id,
    repair.sur_repair_time,
    repair.sur_repair_code AS re_code,
    repair.sur_repair_comment AS re_comment
FROM sc_wms_apollo_pcr_dal.v_tyre_overview overview

JOIN barcode_data bd
    ON overview.tire_barcode = bd.barcode

INNER JOIN sc_wms_apollo_pcr.stock_unit_repair repair
    ON overview.stt_id = repair.sur_stt_id
where repair.sur_repair_code <> '999';
"""

print("#########################")

df_cimcorp = inst.execute_query(q2)
											  

def _safe_val(x):
    """
    Convert a single value to something PyMongo/BSON can serialize.
    NaT and NaN → None.  tz-aware datetime/Timestamp → strip tz.
    This is called AFTER .to_dict('records') so x is already a Python object.
    """
    # pd.NaT is a singleton — identity check is the only reliable test
    # because pd.NaT.__eq__ raises or returns NaT for many comparisons
    if x is pd.NaT:
        return None
    if isinstance(x, pd.Timestamp):
        # pd.isnull works on Timestamp and returns True for NaT
        try:
            if pd.isnull(x):
                return None
        except Exception:
            return None
        return x.replace(tzinfo=None).to_pydatetime()
    if isinstance(x, datetime):
        return x.replace(tzinfo=None)
    if isinstance(x, float):
        try:
            if pd.isna(x):
                return None
        except Exception:
            pass
    return x


def df_to_mongo_records(df):
    """
    Convert a DataFrame to a list of dicts that are safe for MongoDB.
    Operates on the dict values AFTER .to_dict() so there is no way
    for pandas to sneak NaT back in as a typed object.
    """
    raw = df.to_dict('records')
    return [{k: _safe_val(v) for k, v in row.items()} for row in raw]


# Keep sanitize_df_for_mongo as a no-op passthrough so existing call sites
# that do sanitize_df_for_mongo(df).to_dict(...) still work — the real
# sanitization now happens inside df_to_mongo_records.
def sanitize_df_for_mongo(df):
    return df


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
        logging.info("Error converting Excel to CSV: %s", e)
        return None

def has_special_chars(value):
    if isinstance(value, str):
        return bool(re.search(r'[^0-9]', value))
    return False

def convert_string_to_date(date_string):
    for fmt in date_formats:
        try:
            return datetime.strptime(date_string, fmt)
        except ValueError:
            continue
    return None

def get_shift(dt):
    if dt is None or dt == "No Date":
        return "No Date"
    if isinstance(dt, str):
        try:
            dt = pd.to_datetime(dt, errors='coerce')
            if pd.isna(dt):
                return "No Date"
        except Exception as e:
            logging.info("Error parsing date: %s", e)
            return "No Date"
    if not isinstance(dt, (datetime, pd.Timestamp)):
        return "No Date"
    if pd.isna(dt):
        return "No Date"
    if dt == DEFAULT_DATETIME:
        return "No Date"
    if dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    if dt.hour < 6:
        dt = dt + pd.Timedelta(days=1)
    A_start = datetime.strptime(f"{dt.date()} 06:00:00", "%Y-%m-%d %H:%M:%S")
    A_end   = datetime.strptime(f"{dt.date()} 17:59:59", "%Y-%m-%d %H:%M:%S")
    B_start = datetime.strptime(f"{dt.date()} 18:00:00", "%Y-%m-%d %H:%M:%S")
    # B_end   = datetime.strptime(f"{dt.date()} 22:59:59", "%Y-%m-%d %H:%M:%S")
    # C_start = datetime.strptime(f"{dt.date()} 23:00:00", "%Y-%m-%d %H:%M:%S")
    B_end   = datetime.strptime(f"{(dt + pd.Timedelta(days=1)).date()} 05:59:59", "%Y-%m-%d %H:%M:%S")
    if A_start <= dt <= A_end:
        return "A"
    elif B_start <= dt <= B_end:
        return "B"
    # elif C_start <= dt <= C_end:
    #     return "C"
    # elif A_start >= dt <= C_end:
    #     return "C"
    else:
        return "No Shift"

def clean_tub_data(df):
    output_messages = []
    invalid_rows = pd.DataFrame()
    logging.info("clean_tub_data function is just started")
    # Validate input
    if not isinstance(df, pd.DataFrame):
        # logging.error(f"Input 'df' is not a pandas DataFrame, got type: {type(df)}")
        raise TypeError("Expected a pandas DataFrame as input")
    # Create a copy to avoid modifying the original
    df_clean = df.copy()
    
    # Convert barcode column to string
    if 'barcode' in df_clean.columns:
        df_clean['barcode'] = df_clean['barcode'].astype(str)
        
    # Rename specified columns
    rename_columns_mapping = {
        'ufm': 'ubno',
        'timestamp': 'date_and_time',
        'equipmentid': 'tbm',
        'producedon': 'tbm_buildtime',
        'ctcode': 'sku',
        'curingmouldid': 'curing_mouldid',
        'curingpressid': 'curing_press',
        'curingtimestamp': 'curing_infeedtime',
        'vst_vhmc_mes_gt_code': 'tbm_gtcode',
        'barcodeid': 'barcode',
        'vst_vhmc_mes_gt_barcode': 'barcode_tbm'
        }
    
    # Only rename columns that exist
    existing_rename_mapping = {k: v for k, v in rename_columns_mapping.items() if k in df_clean.columns}
    df_clean.rename(columns=existing_rename_mapping, inplace=True)
    
    # Check for duplicate columns in input
    duplicates = df_clean.columns[df_clean.columns.duplicated(keep=False)].unique()
    if len(duplicates) > 0:
        logging.warning(f"Duplicate column names in input: {list(duplicates)}. Renaming duplicates.")
        cols = pd.Series(df_clean.columns)
        for dup in duplicates:
            cols[cols[cols == dup].index.values.tolist()] = [f"{dup}_{i}" if i != 0 else dup for i in range(sum(cols == dup))]
        df_clean.columns = cols
    # Filter invalid barcodes
    mask = df_clean['barcode'].isnull() | df_clean['barcode'].apply(has_special_chars)
    invalid_rows = df_clean[mask]
    df_clean = df_clean[~mask].drop_duplicates()
    # Drop unnecessary columns
    df_clean.drop(columns=['rn', 'Unnamed: 226', 'Unnamed: 225'], inplace=True, errors='ignore')
    # Clean column names
    df_clean.columns = df_clean.columns.to_series().str.strip().str.lower().str.replace(' ', '_')

    # Add new columns with None (null) values
    df_clean["tbm_machine_model_lot_id"] = None
    df_clean["tbm_previous_machine_model_lot_id"] = None
    df_clean["curing_machine_model_lot_id"] = None
    df_clean["curing_previous_machine_model_lot_id"] = None
    df_clean["ufm_machine_model_lot_id"] = None
    df_clean["ufm_previous_machine_model_lot_id"] = None
    df_clean['alert_id_ufm_ufm'] = None
    df_clean['alert_id_curing_ufm'] = None
    df_clean['alert_id_tbm_ufm'] = None

    return df_clean, invalid_rows, output_messages

def clean_visual_data(df):
    output_messages = []
    invalid_barcode_rows = pd.DataFrame()
    duplicates_df = pd.DataFrame()
    logging.info("clean_visual_data function is just started")

    try:
        # Clean column names only (no data type changes)
        df.columns = [re.sub(r'[.\s]+', '_', col.strip()).lower() for col in df.columns]
        
        # Step 2: Rename columns based on the provided mapping
        rename_columns_mapping = {
            'equipmentid': 'tbm',
            'producedon': 'tbm_buildtime',
            'ctcode': 'sku',
            'curingmouldid': 'curing_mouldid',
            'curingpressid': 'curing_press',
            'curingtimestamp': 'curing_infeedtime',
            'vst_vhmc_mes_gt_code': 'tbm_gtcode',
            'barcodeid': 'barcode',
            'vst_vhmc_mes_gt_barcode': 'barcode_tbm'
        }
        for old_name, new_name in rename_columns_mapping.items():
            if old_name in df.columns:
                df = df.rename(columns={old_name: new_name}, inplace=False)  # inplace=False to reassign df

        # # Convert only curing_press if present
        # if 'curing_press' in df.columns:
        #     df['curing_press'] = 'M' + df['curing_press'].astype(str)

        # Ensure barcode is treated as string only for validation
        df['barcode'] = df['barcode'].astype(str).str.strip()
        df['curing_infeedtime'] = df['curing_infeedtime'].astype(str)

        special_char_regex = re.compile(r'[^0-9]')

        rows_with_null_barcode = df[df['barcode'].isnull() | (df['barcode'] == 'nan')]
        rows_with_special_char_barcode = df[
            df['barcode'].apply(lambda x: bool(special_char_regex.search(x)))
        ]

        invalid_barcode_rows = pd.concat(
            [rows_with_null_barcode, rows_with_special_char_barcode]
        ).drop_duplicates()

        # Remove invalid barcode rows
        df = df[~df.index.isin(invalid_barcode_rows.index)]

        # Drop columns that are completely null
        df = df.dropna(axis=1, how='all')

        # Find duplicates based on barcode
        duplicates_df = df[df.duplicated(subset=['barcode'], keep=False)]

        # Remove duplicate barcodes
        df = df.drop_duplicates(subset=['barcode'])

        output_messages.append("CIMCORP_HISTORICAL_DATA Cleaning successful")
        logging.info("clean_visual_data function is completed")

        return df, invalid_barcode_rows, duplicates_df, output_messages

    except Exception as e:
        error_message = f"Error during cleaning: {e}"
        output_messages.append(error_message)
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), output_messages

def insert_cleaned_data(db_client, cleaned_df, database_name, filename):
    # No-op: intermediate DBs (Data_Clean_TUB_CIMCORP_HOME, VISUAL,
    # all_combined_data_home) are no longer written to.
    return []

# <<<--- CHANGE START -------------------------------------------------
# CORRECT format - list of (field_name, direction) tuples
UNIQUE_INDEX_FIELDS = [
    ("barcode", 1), ("uf_time", 1), ("ptb_time", 1), ("cur_time", 1), 
    ("gt_weighting_time", 1), ("last_change_time", 1), ("sur_repair_time", 1), ("tire_barcode",1)
]

UNIQUE_INDEX_NAME = "idx_unique_duplicate_detection"

def ensure_unique_index(collection):
    existing = collection.index_information()
    if UNIQUE_INDEX_NAME not in existing:
        try:
            # FIX: UNIQUE_INDEX_FIELDS should be a list of (field_name, direction) tuples
            # If UNIQUE_INDEX_FIELDS is already in that format, use it directly
            collection.create_index(
                UNIQUE_INDEX_FIELDS,  # Use the list directly instead of wrapping each field
                name=UNIQUE_INDEX_NAME,
                unique=True,
                background=True
            )
            logging.info(f"Created unique index on {collection.name}")
        except Exception as e:
            logging.warning(f"Index creation failed: {e}")
    else:
        logging.info(f"Unique index already exists on {collection.name}")

def chunked_insert(collection, records, chunk_size=5000):
    """Insert with ordered=False → MongoDB skips duplicates instantly."""
    total = len(records)
    inserted = 0
    for i in range(0, total, chunk_size):
        chunk = records[i:i + chunk_size]
        try:
            result = collection.insert_many(chunk, ordered=False)
            inserted += len(result.inserted_ids)
        except pymongo.errors.BulkWriteError as bwe:
            inserted += bwe.details['nInserted']
    return inserted

def process_data_for_api(db_client, source_data, database_name, filename):
    """
    Cleans source data and returns the cleaned DataFrame directly.
    No intermediate DBs (TUB_CIMCORP, Data_Clean_TUB_CIMCORP_HOME,
    all_combined_data_home) are written to — data flows straight to the
    final merged DB via copy_to_single_collection_api.
    """
    output_messages = []
    cleaned_df = pd.DataFrame()
    logging.info("process_data_for_api function started")
    try:
        df = pd.DataFrame(list(source_data))
        if database_name == "TUB_CIMCORP":
            cleaned_df, invalid_df, tub_messages = clean_tub_data(df.copy())
            output_messages.extend(tub_messages)
            if not invalid_df.empty:
                invalid_df = invalid_df.drop('_id', axis=1, errors='ignore')
                invalid_collection_name = "invalid_data_TUB_" + filename.replace(".csv", "").replace(".xlsx", "")
                db_client[INVALID_ROWS_DB][invalid_collection_name].insert_many(df_to_mongo_records(invalid_df))
                output_messages.append(f"Invalid TUB data saved to {INVALID_ROWS_DB}.{invalid_collection_name}")
            cleaned_df = cleaned_df.drop('_id', axis=1, errors='ignore')

        elif database_name == "VISUAL":
            cleaned_df, invalid_barcode_rows, duplicates_df, cimcorp_messages = clean_visual_data(df.copy())
            output_messages.extend(cimcorp_messages)
            if not invalid_barcode_rows.empty:
                invalid_barcode_rows = invalid_barcode_rows.drop('_id', axis=1, errors='ignore')
                invalid_collection_name = "invalid_data_CIMCORP_" + filename.replace(".csv", "").replace(".xlsx", "")
                db_client[INVALID_ROWS_DB][invalid_collection_name].insert_many(df_to_mongo_records(invalid_barcode_rows))
                output_messages.append(f"Invalid CIMCORP data saved to {INVALID_ROWS_DB}.{invalid_collection_name}")
            if not duplicates_df.empty:
                duplicates_df = duplicates_df.drop('_id', axis=1, errors='ignore')
                dup_collection_name = f"duplicate_CIMCORP_{filename.replace('.csv', '').replace('.xlsx', '')}"
                db_client[INVALID_ROWS_DB][dup_collection_name].insert_many(df_to_mongo_records(duplicates_df))
                output_messages.append(f"Duplicate CIMCORP data saved to {INVALID_ROWS_DB}.{dup_collection_name}")
            cleaned_df = cleaned_df.drop('_id', axis=1, errors='ignore')
        else:
            output_messages.append(f"Unknown database: {database_name}")

    except Exception as e:
        output_messages.append(f"Error processing data: {e}")
        logging.error(f"process_data_for_api error: {e}")

    logging.info("process_data_for_api function completed")
    return output_messages, cleaned_df

def _upsert_records(collection, records, key_field):
    """
    Fast upsert: delete only the barcodes present in this batch,
    then insert_many in one shot.  Previous records for OTHER barcodes
    are left untouched.  Much faster than per-doc ReplaceOne.
    """
    if not records:
        return
    keys = [rec[key_field] for rec in records if rec.get(key_field) is not None]
    if keys:
        collection.delete_many({key_field: {"$in": keys}})
    collection.insert_many(records, ordered=False)

def _upsert_scrap_records(collection, records, key_cols):
    """
    Scrap-specific upsert using a composite key
    (tire_barcode + gt_weighting_time + sur_repair_time + last_change_time).
    Deletes only the exact composite-key combinations present in this batch,
    then inserts — so no duplicates accumulate across runs.
    """
    if not records or not key_cols:
        return
    # Build one $or query covering all composite keys in this batch
    or_clauses = [
        {col: rec.get(col) for col in key_cols}
        for rec in records
        if any(rec.get(col) is not None for col in key_cols)
    ]
    if or_clauses:
        collection.delete_many({"$or": or_clauses})
    collection.insert_many(records, ordered=False)

def copy_to_single_collection_api(df_summary=None, df_scrap=None):
    """
    Writes cleaned data directly to the final merged DB.
    Accepts in-memory DataFrames — no intermediate DB reads needed.
    """
    output_messages = []
    logging.info("copy_to_single_collection_api function started")
    try:
        merged_db = client[TUB_CIMCORP_VISUAL_MERGED_DB_alert]

        # Ensure indexes for fast upsert lookups
        merged_db[DATA_MERGED_SUMMARY_COLLECTION_alert].create_index([("barcode", 1)], background=True)
        merged_db[DATA_MERGED_SCRAP_COLLECTION_alert].create_index([("tire_barcode", 1)], background=True)
        merged_db[DATA_MERGED_SCRAP_COLLECTION_alert].create_index(
            [("tire_barcode", 1), ("gt_weighting_time", 1), ("sur_repair_time", 1), ("last_change_time", 1)],
            background=True
        )
        merged_db[DATA_MERGED_SUMMARY_COLLECTION_alert_new].create_index([("barcode", 1)], background=True)
        merged_db[DATA_MERGED_SCRAP_COLLECTION_alert_new].create_index([("tire_barcode", 1)], background=True)
        merged_db[DATA_MERGED_SCRAP_COLLECTION_alert_new].create_index(
            [("tire_barcode", 1), ("gt_weighting_time", 1), ("sur_repair_time", 1), ("last_change_time", 1)],
            background=True
        )

        # ── 1. Summary (TUB/UFM) data ────────────────────────────────────────
        if df_summary is not None and not df_summary.empty:
            time_col = next((c for c in ['date_and_time', 'uf_time', 'timestamp'] if c in df_summary.columns), None)
            if time_col:
                df_summary['shift'] = df_summary[time_col].apply(get_shift)

            # ── Compute 'last' and 'rerun' columns per barcode based on uf_time ranking ──
            # last:  latest uf_time → 1, second-latest and all middle → -1, oldest → 0
            # rerun: first test of barcode → 0, every subsequent test → 1
            if 'uf_time' in df_summary.columns and 'barcode' in df_summary.columns:
                df_summary['_uf_time_sort'] = pd.to_datetime(df_summary['uf_time'], errors='coerce')
                # Rank descending: rank 1 = latest, rank N = oldest
                df_summary['_rank_desc'] = (
                    df_summary.groupby('barcode')['_uf_time_sort']
                    .rank(method='first', ascending=False)
                    .astype(int)
                )
                # Rank ascending: rank 1 = oldest (first test)
                df_summary['_rank_asc'] = (
                    df_summary.groupby('barcode')['_uf_time_sort']
                    .rank(method='first', ascending=True)
                    .astype(int)
                )
                df_summary['_count'] = df_summary.groupby('barcode')['barcode'].transform('count')

                def _last_val(row):
                    r, c = row['_rank_desc'], row['_count']
                    if c == 1:
                        return 1           # only one record → latest
                    if r == 1:
                        return 1           # latest
                    if row['_rank_asc'] == 1:
                        return 0           # oldest (first ever tested)
                    return -1              # all middle records

                df_summary['last'] = df_summary.apply(_last_val, axis=1)
                # rerun: 0 for the first test (oldest), 1 for every re-test
                df_summary['rerun'] = (df_summary['_rank_asc'] > 1).astype(int)
                df_summary.drop(columns=['_uf_time_sort', '_rank_desc', '_rank_asc', '_count'], inplace=True)

            # uf_yield: sum of overalltugrade + overallbalgrade + overallsolgrade == 3 → 1 else 0
            _uf_cols = ['overalltugrade', 'overallbalgrade', 'overallsolgrade']
            _present = [c for c in _uf_cols if c in df_summary.columns]
            if _present:
                df_summary['uf_yield'] = df_summary[_present].apply(
                    lambda row: 1 if pd.to_numeric(row, errors='coerce').sum() == 3 else 0, axis=1
                )

            # sku_type: QR_Status from production_db_hungary.channel_no_ufm matched by sku
            try:
                qr_docs = list(client[config_databse][channel_no_collect].find(
                    {}, {"_id": 0, "sku": 1, "QR_Status": 1}
                ))
                if qr_docs and 'sku' in df_summary.columns:
                    qr_map = {str(doc["sku"]).strip(): doc.get("QR_Status", "") for doc in qr_docs}
                    df_summary['sku_type'] = df_summary['sku'].apply(
                        lambda s: qr_map.get(str(s).strip(), "")
                    )
            except Exception as _qr_err:
                logging.warning(f"sku_type enrichment failed: {_qr_err}")

            records_summary = df_to_mongo_records(df_summary)
            # Default re_code and re_comment for all records before scrap enrichment
            for rec in records_summary:
                if not str(rec.get('re_code') or '').strip():
                    rec['re_code'] = '0'
                    rec['re_comment'] = ''
            # Stamp vi_yield: re_code 1100 or 102 → 1, else 0
            _vi_codes = {'1100', '102', '2', '0'}
            for rec in records_summary:
                rec['vi_yield'] = 1 if str(rec.get('re_code') or '').strip() in _vi_codes else 0
            _upsert_records(merged_db[DATA_MERGED_SUMMARY_COLLECTION_alert], records_summary, 'barcode')
            output_messages.append(f"Upserted {len(records_summary)} records into {TUB_CIMCORP_VISUAL_MERGED_DB_alert}.{DATA_MERGED_SUMMARY_COLLECTION_alert}")
            _upsert_records(merged_db[DATA_MERGED_SUMMARY_COLLECTION_alert_new], records_summary, 'barcode')
            output_messages.append(f"Upserted {len(records_summary)} records into {TUB_CIMCORP_VISUAL_MERGED_DB_alert}.{DATA_MERGED_SUMMARY_COLLECTION_alert_new}")
        else:
            output_messages.append("No TUB summary data to write.")
            records_summary = []

        # ── 2. Scrap/repair data ─────────────────────────────────────────────
        if df_scrap is not None and not df_scrap.empty:
            # Deduplicate scrap on composite key before inserting
            _scrap_key_cols = ['tire_barcode', 'gt_weighting_time', 'sur_repair_time', 'last_change_time']
            _present_keys = [c for c in _scrap_key_cols if c in df_scrap.columns]
            if _present_keys:
                df_scrap = df_scrap.drop_duplicates(subset=_present_keys, keep='last')

            records_scrap = df_to_mongo_records(df_scrap)
            _upsert_scrap_records(merged_db[DATA_MERGED_SCRAP_COLLECTION_alert], records_scrap, _present_keys)
            output_messages.append(f"Upserted {len(records_scrap)} records into {TUB_CIMCORP_VISUAL_MERGED_DB_alert}.{DATA_MERGED_SCRAP_COLLECTION_alert}")
            _upsert_scrap_records(merged_db[DATA_MERGED_SCRAP_COLLECTION_alert_new], records_scrap, _present_keys)
            output_messages.append(f"Upserted {len(records_scrap)} records into {TUB_CIMCORP_VISUAL_MERGED_DB_alert}.{DATA_MERGED_SCRAP_COLLECTION_alert_new}")

            # ── 3. Enrich summary with RE_CODE/RE_COMMENT via barcode join ───
            if records_summary:
                barcode_col_scrap = next((c for c in ['tire_barcode', 'barcode'] if c in df_scrap.columns), None)
                if barcode_col_scrap:
                    scrap_cols = [c for c in ['re_code', 're_comment', 're_last_change_time',
                                              'RE_CODE', 'RE_COMMENT', 'RE_LAST_CHANGE_TIME'] if c in df_scrap.columns]
                    scrap_dedup = df_scrap[[barcode_col_scrap] + scrap_cols].drop_duplicates(
                        subset=[barcode_col_scrap], keep='last'
                    )
                    # Key the lookup by leading-zero-stripped tire_barcode so it matches
                    # the summary barcode (which has leading zeros removed by clean_tub_data).
                    # tire_barcode in data_merged_scrap is stored with leading zeros intact.
                    scrap_lookup = {
                        str(k).lstrip('0'): v
                        for k, v in scrap_dedup.set_index(barcode_col_scrap)[scrap_cols].to_dict('index').items()
                    }

                    enriched = 0
                    for rec in records_summary:
                        bc = rec.get('barcode')
                        # Normalize barcode for lookup: strip leading zeros to match scrap_lookup keys
                        bc_key = str(bc).lstrip('0') if bc else None
                        if bc_key and bc_key in scrap_lookup:
                            # Barcode matched in scrap — use re_code/re_comment from scrap as-is
                            rec.update({k: _safe_val(v) for k, v in scrap_lookup[bc_key].items()})
                            enriched += 1
                        else:
                            # No scrap match — always default re_code to "0" and re_comment to ""
                            rec['re_code'] = '0'
                            rec['re_comment'] = ''
                    # Always re-stamp vi_yield after enrichment (re_code may have changed)
                    _vi_codes = {'1100', '102', '2', '0'}
                    for rec in records_summary:
                        rec['vi_yield'] = 1 if str(rec.get('re_code') or '').strip() in _vi_codes else 0
                    # Always upsert — even if enriched == 0, defaults need to be persisted
                    _upsert_records(merged_db[DATA_MERGED_SUMMARY_COLLECTION_alert], records_summary, 'barcode')
                    output_messages.append(f"Enriched {enriched} summary records with scrap/repair codes.")
                    _upsert_records(merged_db[DATA_MERGED_SUMMARY_COLLECTION_alert_new], records_summary, 'barcode')
                    output_messages.append(f"Enriched {enriched} summary records with scrap/repair codes (new collection).")

                    # ── 4. Backtrack scrap columns into summary as <col>_tub ─
                    _backtrack_cols = [
                        'gt_weighting_time', 'ft_code', 'vi_result', 'ufm_result',
                        'loc_name', 'last_change_time', 'sur_repair_time',
                        'condition_code', 'description', 'scrap_category'
                    ]
                    _bt_available = [c for c in _backtrack_cols if c in df_scrap.columns]
                    if _bt_available:
                        bt_dedup = df_scrap[[barcode_col_scrap] + _bt_available].drop_duplicates(
                            subset=[barcode_col_scrap], keep='last'
                        )
                        bt_lookup = {
                            str(k).lstrip('0'): v
                            for k, v in bt_dedup.set_index(barcode_col_scrap)[_bt_available].to_dict('index').items()
                        }
                        bt_enriched = 0
                        for rec in records_summary:
                            bc = rec.get('barcode')
                            bc_key = str(bc).lstrip('0') if bc else None
                            if bc_key and bc_key in bt_lookup:
                                for col, val in bt_lookup[bc_key].items():
                                    rec[f"{col}_tub"] = _safe_val(val)
                                bt_enriched += 1
                        if bt_enriched:
                            _upsert_records(merged_db[DATA_MERGED_SUMMARY_COLLECTION_alert], records_summary, 'barcode')
                            output_messages.append(f"Backtracked {len(_bt_available)} scrap columns (_tub) into {bt_enriched} summary records.")
                            _upsert_records(merged_db[DATA_MERGED_SUMMARY_COLLECTION_alert_new], records_summary, 'barcode')
                            output_messages.append(f"Backtracked {len(_bt_available)} scrap columns (_tub) into {bt_enriched} summary records (new collection).")
        else:
            output_messages.append("No scrap data to write.")
            # Even with no scrap data, ensure all summary records have re_code/re_comment defaults
            if records_summary:
                for rec in records_summary:
                    if not str(rec.get('re_code') or '').strip():
                        rec['re_code'] = '0'
                        rec['re_comment'] = ''
                _vi_codes = {'1100', '102', '2', '1'}
                for rec in records_summary:
                    rec['vi_yield'] = 1 if str(rec.get('re_code') or '').strip() in _vi_codes else 0
                _upsert_records(merged_db[DATA_MERGED_SUMMARY_COLLECTION_alert], records_summary, 'barcode')
                _upsert_records(merged_db[DATA_MERGED_SUMMARY_COLLECTION_alert_new], records_summary, 'barcode')

    except Exception as e:
        error_msg = f"Error in copy_to_single_collection_api: {e}"
        output_messages.append(error_msg)
        logging.error(error_msg)

    logging.info("copy_to_single_collection_api function completed")
    return output_messages

def run_data_cleaning(df_tub_input, df_cimcorp_input):
    """
    Cleans df_tub_input and df_cimcorp_input in-memory and writes
    directly to the final merged DB. No intermediate DBs are used.
    """
    start_time = time.time()
    output_messages = []
    logging.info("run_data_cleaning function started")
    try:
        # Clean TUB/UFM summary data
        cleaned_summary, invalid_df, tub_msgs = clean_tub_data(df_tub_input.copy())
        output_messages.extend(tub_msgs)
        if not invalid_df.empty:
            invalid_df = invalid_df.drop('_id', axis=1, errors='ignore')
            client[INVALID_ROWS_DB]["invalid_data_TUB_home"].insert_many(df_to_mongo_records(invalid_df))
            output_messages.append(f"Saved {len(invalid_df)} invalid TUB rows to INVALID_ROWS.invalid_data_TUB_home")

        # Convert datetime columns to string in summary data
        for _col in ['uf_time', 'ptb_time', 'cur_time']:
            if _col in cleaned_summary.columns:
                cleaned_summary[_col] = cleaned_summary[_col].apply(
                    lambda x: None if x is pd.NaT or (isinstance(x, float) and pd.isna(x))
                    else str(x).strip()
                )

        # Clean scrap/repair data (just sanitize — no barcode cleaning needed)
        cleaned_scrap = df_cimcorp_input.copy()
        cleaned_scrap.columns = [c.lower() for c in cleaned_scrap.columns]

        # Convert re_code and re_last_change_time to string
        def _to_str_val(x, strip_decimal=False):
            if x is pd.NaT or (isinstance(x, float) and pd.isna(x)):
                return None
            if strip_decimal:
                try:
                    return str(int(float(x)))
                except (ValueError, TypeError):
                    pass
            return str(x).strip()

        if 're_code' in cleaned_scrap.columns:
            cleaned_scrap['re_code'] = cleaned_scrap['re_code'].apply(lambda x: _to_str_val(x, strip_decimal=True))
            # Replace null / empty string with "0"
            cleaned_scrap['re_code'] = cleaned_scrap['re_code'].apply(
                lambda x: '0' if (x is None or str(x).strip() in ('', 'None', 'nan', 'NaN')) else x
            )
        for _col in ['re_last_change_time', 'sur_repair_time', 'gt_weighting_time', 'last_change_time']:
            if _col in cleaned_scrap.columns:
                cleaned_scrap[_col] = cleaned_scrap[_col].apply(lambda x: _to_str_val(x))
        # Enrich with condition_code and description from production_db_hungary.re_codes
        # Match logic:
        #   1. Try re_code directly against condition_code
        #   2. If no match, extract the first number from re_comment
        #      e.g. "The VI Reason was changed: 516 --> 0"  →  "516"
        #      and match that against condition_code
        def _extract_comment_code(comment):
            """Extract the first standalone number from re_comment."""
            if not comment or not isinstance(comment, str):
                return None
            m = re.search(r'\b(\d+)\b', comment)
            return m.group(1) if m else None

        try:
            re_code_docs = list(client[config_databse]["re_codes"].find({}, {"_id": 0, "condition_code": 1, "description": 1}))
            re_code_docs = list(client[config_databse]["re_codes"].find({}, {"_id": 0, "condition_code": 1, "description": 1}))
            if re_code_docs:
                re_code_map = {
                    str(doc["condition_code"]).strip(): doc.get("description", "")
                    for doc in re_code_docs
                }

                # Lookup 1: condition_code → description
                # Lookup 2: description (lowercase) → (condition_code, description)
                re_desc_map = {
                    desc.lower(): (code, desc)
                    for code, desc in re_code_map.items()
                }

                def _resolve(row):
                    comment = str(row.get("re_comment", "") or "").strip()

                    # Strategy 1: re_code matches condition_code directly
                    key = str(row.get("re_code", "") or "").strip()
                    if key and key in re_code_map:
                        return key, re_code_map[key]

                    # Strategy 2: extract number from re_comment → match condition_code
                    # e.g. "The VI Reason was changed: 516 --> 0"  →  "516"
                    comment_key = _extract_comment_code(comment)
                    if comment_key and comment_key in re_code_map:
                        return comment_key, re_code_map[comment_key]

                    # Strategy 3: re_comment text matches description (case-insensitive)
                    # e.g. re_comment "Force" matches description "Force"
                    if comment:
                        comment_lower = comment.lower()
                        # exact match first
                        if comment_lower in re_desc_map:
                            return re_desc_map[comment_lower]
                        # partial match — comment contains description or vice versa
                        for desc_lower, (code, desc) in re_desc_map.items():
                            if desc_lower in comment_lower or comment_lower in desc_lower:
                                return code, desc

                    return "", ""

                resolved = cleaned_scrap.apply(_resolve, axis=1)
                cleaned_scrap["condition_code"] = resolved.apply(lambda x: x[0])
                cleaned_scrap["description"]    = resolved.apply(lambda x: x[1])

                # Assign scrap_category based on description value
                _uf_keywords = {"force", "runout", "rfv-grinding"}
                cleaned_scrap["scrap_category"] = cleaned_scrap["description"].apply(
                    lambda x: "UF" if str(x).strip().lower() in _uf_keywords else "VISUAL"
                )

                output_messages.append(f"Enriched scrap data with condition_code + description from {len(re_code_map)} re_codes.")
            else:
                output_messages.append("No re_codes found in config DB — skipping enrichment.")
        except Exception as re_err:
            output_messages.append(f"Warning: could not enrich re_codes: {re_err}")
            logging.warning(f"re_codes enrichment failed: {re_err}")

        # Add QR_Status to scrap data by joining:
        #   scrap.tire_barcode → summary.barcode → summary.sku → channel_no_ufm.sku → QR_Status
        try:
            qr_docs = list(client[config_databse][channel_no_collect].find(
                {}, {"_id": 0, "sku": 1, "QR_Status": 1}
            ))
            if qr_docs:
                # sku → QR_Status lookup from channel_no_ufm
                qr_map = {str(doc["sku"]).strip(): doc.get("QR_Status", "") for doc in qr_docs}

                # barcode → sku lookup from cleaned_summary
                barcode_col = next((c for c in ['barcode'] if c in cleaned_summary.columns), None)
                sku_col     = next((c for c in ['sku'] if c in cleaned_summary.columns), None)
                if barcode_col and sku_col:
                    barcode_sku_map = dict(zip(
                        cleaned_summary[barcode_col].astype(str),
                        cleaned_summary[sku_col].astype(str)
                    ))
                    # scrap barcode column
                    scrap_bc_col = next((c for c in ['tire_barcode', 'barcode'] if c in cleaned_scrap.columns), None)
                    if scrap_bc_col:
                        cleaned_scrap["sku"] = cleaned_scrap[scrap_bc_col].apply(
                            lambda bc: barcode_sku_map.get(str(bc).strip(), "")
                        )
                        cleaned_scrap["qr_status"] = cleaned_scrap["sku"].apply(
                            lambda s: qr_map.get(str(s).strip(), "")
                        )
                        output_messages.append("Added qr_status to scrap data via sku lookup.")
                    else:
                        output_messages.append("Warning: no barcode column found in scrap data for QR_Status join.")
                else:
                    output_messages.append("Warning: barcode/sku columns missing in summary for QR_Status join.")
            else:
                output_messages.append("No QR_Status data found in channel_no_ufm — skipping.")
        except Exception as qr_err:
            output_messages.append(f"Warning: could not add QR_Status: {qr_err}")
            logging.warning(f"QR_Status enrichment failed: {qr_err}")

        # ── No barcode intersection filter — all summary records are written.
        # Scrap enrichment happens inside copy_to_single_collection_api:
        # matched barcodes get re_code/re_comment from scrap, unmatched get "0"/"".

        # Write directly to final merged DB
        copy_msgs = copy_to_single_collection_api(
            df_summary=cleaned_summary,
            df_scrap=cleaned_scrap
        )
        output_messages.extend(copy_msgs)

        # Track this run in file_tracking DB — one document per query
        run_timestamp = some_function()
        client[FILE_TRACKING_DB][FILE_TRACKING_COLLECTION].insert_many([
            {
                "query": "Q1_TUB_TBM_CIMCORP",
                "description": "UFM + TBM + Curing merged summary data",
                "filename": f"Q1_TUB_TBM_CIMCORP_{run_timestamp.strftime('%Y%m%d_%H%M%S')}",
                "inserted_timestamp": run_timestamp,
                "start_timestamp": start_timestamp,
                "end_timestamp": end_timestamp,
                "num_rows_raw": len(df_tub_input),
                "num_rows_cleaned": len(cleaned_summary),
                "num_rows_invalid": len(invalid_df) if not invalid_df.empty else 0,
                "target_collection": DATA_MERGED_SUMMARY_COLLECTION_alert,
                "database_name": TUB_CIMCORP_VISUAL_MERGED_DB_alert,
                "processed": True,
                "processed_timestamp": run_timestamp
            },
            {
                "query": "Q2_SCRAP_REPAIR",
                "description": "Scrap / repair code data",
                "filename": f"Q2_SCRAP_REPAIR_{run_timestamp.strftime('%Y%m%d_%H%M%S')}",
                "inserted_timestamp": run_timestamp,
                "start_timestamp": start_timestamp,
                "end_timestamp": end_timestamp,
                "num_rows_raw": len(df_cimcorp_input),
                "num_rows_cleaned": len(cleaned_scrap),
                "num_rows_invalid": 0,
                "target_collection": DATA_MERGED_SCRAP_COLLECTION_alert,
                "database_name": TUB_CIMCORP_VISUAL_MERGED_DB_alert,
                "processed": True,
                "processed_timestamp": run_timestamp
            }
        ])
        output_messages.append("Run tracked in file_tracking DB (Q1 + Q2).")

        execution_time = time.time() - start_time
        output_messages.append(f"Completed in {execution_time:.2f} seconds.")
        logging.info("run_data_cleaning function completed")
        return {
            "status": "success",
            "message": "Data cleaning and processing completed successfully.",
            "output_log": output_messages,
            "execution_time": execution_time
        }
    except Exception as e:
        error_message = f"An error occurred in run_data_cleaning: {e}"
        output_messages.append(error_message)
        logging.error(error_message)
        return {
            "status": "error",
            "message": error_message,
            "output_log": output_messages,
            "execution_time": 0
        }

date_formats = ["%d-%m-%Y", "%Y-%m-%d"]

# --- Helper Functions (Ensure these are defined or imported) ---
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
        merge_collect = client[TUB_CIMCORP_VISUAL_MERGED_DB_alert][DATA_MERGED_SUMMARY_COLLECTION_alert]
        channel_collect_tbm = client[config_databse][channel_no_collect_tbm]
        channel_collect_ufm = client[config_databse][channel_no_collect]
        lot_tbm_collect = client[lot_db][lot_tbm_collect_name]
        alert_collect = client[alert_table][alert_tbm_collect_name]
        averages_collect = client[average_db][averages_tbm_table]
        machine_collect = client[config_databse][machine_no_collect]
        curing_press_collect = client[config_databse][curing_press_dict_collect]
        alert_triggering_collect = client[alert_db][alert_triggering_collect_name]
        # summary_triggering_collect = client[summary_db][summary_triggering_collect_name_tbm]
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

        tbm_gtcodes_collect = client[model_db][model_no_status_collection]
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
        active_skus = set(tbm_gtcodes_collect.distinct("sku", {"status": "active"}))
        output_messages.append(f"✅ Fetched {len(active_skus)} active model numbers: {active_skus}")
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
    channel_skus = set(channel_collect_tbm.distinct("sku"))

    # Check required columns
    required_columns = {"sku","alert_id_tbm_ufm"}
    if not required_columns.issubset(merge_df.columns):
        print("Required columns are missing in merge_df. Exiting script.")
        exit()

    # Filter data
    filtered_df = merge_df[merge_df["sku"].isin(channel_skus) & merge_df["alert_id_tbm_ufm"].isna()]
    if not filtered_df.empty:
        merge_df = filtered_df
    else:
        print("No data after filtering or condition not satisfied. Exiting script.")
        exit()

    # Assign date columns
    merge_df["date_of_tbm"] = merge_df.get("ptb_time", pd.Series([None] * len(merge_df)))
    merge_df["date_of_curing"] = merge_df.get("cur_time", pd.Series([None] * len(merge_df)))
    merge_df["date_and_time"] = merge_df.get("uf_time", pd.Series([None] * len(merge_df)))

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
                    "sku": {"$in": list(active_skus)},
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
                    "sku": "$sku",
                    "machine": "$machine"
                }
            },
            {
                "$sort": {"lot_number": -1}
            },
            {
                "$group": {
                    "_id": {"sku": "$sku", "machine": "$machine"},
                    "max_lot_number": {"$first": "$lot_number"},
                    "last_lot_id": {"$first": "$lot_id"}
                }
            }
        ]))
        last_lot_dict = {
            (item["_id"]["sku"], item["_id"]["machine"]): {
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
    merge_df["tbm"] = merge_df["ptb_id"].astype(str)
    machine_df["machine"] = machine_df["machine"].astype(str)

    # Correct merge: tbm ↔ machine
    merge_df = merge_df.merge(
        machine_df[["machine"]],
        left_on="tbm",
        right_on="machine",
        how="left"
    )

    # Update tbm (if machine exists, else keep original)
    merge_df["tbm"] = merge_df["machine"].fillna(merge_df["tbm"])

    # Filter only active machines
    merge_df = merge_df[merge_df["tbm"].isin(active_machines)]

    if merge_df.empty:
        output_messages.append("❌ No data in merge_df after filtering for active machines. Exiting.")
        return {
            "status": "error",
            "message": "No data after active machine filtering",
            "output_log": output_messages
        }, 500

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
                "sku": {"$in": list(active_skus)},
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
                    "_id": {"sku": "$sku", "machine": "$machine"},
                    "last_tbm_lot_id": {"$first": "$tbm_machine_model_lot_id"},
                    "last_averages": {"$first": "$current_lot_averages"}
                }
            }
        ]))
        for item in last_averages:
            sku = item["_id"]["sku"]
            machine = item["_id"]["machine"]
            tbm_lot_id = item["last_tbm_lot_id"]
            averages = item["last_averages"]
            tbm_previous_machine_model_lot_ids[(sku, machine)] = tbm_lot_id
            if averages:
                machine_lot_avg_map[(sku, machine, tbm_lot_id)] = averages
                # logging.info(f"Pre-populated machine_lot_avg_map with averages for {tbm_lot_id}")
            else:
                logging.warning(f"No averages found for {tbm_lot_id} in averages_collect")
        # logging.info(f"Initialized tbm_previous_machine_model_lot_ids: {tbm_previous_machine_model_lot_ids}")
    except Exception as e:
        logging.error(f"Error pre-populating machine_lot_avg_map and tbm_previous_machine_model_lot_ids: {e}")
        output_messages.append(f"❌ Error pre-populating machine_lot_avg_map and tbm_previous_machine_model_lot_ids: {e}")

    numeric_params = [
        "rfppcc","rfppcw","rfh1cc","rfh1cw","rfh2cc","rfh2cw","lfppcc","lfppcw","cony","plys",
        "crro","trro","brro","lrot","lrob","lroh1grade","tbul","bbul","tdep","bdep",
        "upper","lower","sbal","aircc","aircw","loadcc","loadcw"
    ]
    yield_params = [
        "rfv_yield", "rfh1_yield", "rfh2_yield", "lfv_yield", "conicity_yield", "plysteer_yield",
        "center_radial_run_out_yield", "top_radial_run_out_yield", "bottom_radial_run_out_yield", 
        "lateral_run_out_yield", "lateral_run_out_harmonic_first_yield", "center_radial_run_out_harmonic_first_yield",
        "top_radial_run_out_harmonic_first_yield", "bottom_radial_run_out_harmonic_first_yield",
        "lateral_radial_run_out_yield", "static_yield", "couple_yield",
        "upper_yield", "lower_yield", "overall_uniformity_yield", "overall_visual_yield", "overall_warehouse_yield",
        "overall_scrap_yield", "visual_loss_yield", "uf_loss_yield", "oe_ok_yield"
    ]
    
    # Define rank parameter mappings
    rank_param_mappings = {
        "rfv_yield": ["rfppwcgrade"],
        "rfh1_yield": ["rfh1wcgrade"],
        "rfh2_yield": ["rfh2wcgrade"],
        "lfv_yield": ["lfppwcgrade"],
        "conicity_yield": ["conygrade"],
        "plysteer_yield": ["plysgrade"],
        "center_radial_run_out_yield": ["crrograde"], 
        "top_radial_run_out_yield": ["trrograde"], 
        "bottom_radial_run_out_yield": ["brrograde"],
        "lateral_run_out_yield": ["lrograde"],
        "lateral_run_out_harmonic_first_yield": ["lroh1grade"],
        "center_radial_run_out_harmonic_first_yield": ["crroh1grade"],
        "top_radial_run_out_harmonic_first_yield": ["trroh1grade"],
        "bottom_radial_run_out_harmonic_first_yield": ["brroh1grade"],
        "lateral_radial_run_out_yield": ["lrrograde"],
        "static_yield": ["sbalgrade"],
        "couple_yield": ["couplegrade"],
        "upper_yield": ["uppergrade"],
        "lower_yield": ["lowergrade"]
    }

    # === MAIN CHANGE: Inside the model loop (after model_data is created) ===
    # Replace the existing model_data creation block with this enhanced version:

    for _, row in channel_df.iterrows():
        try:
            model_number = str(row.get("sku", "")).strip()
            if model_number not in active_skus:
                output_messages.append(f"Skipping TBM model {model_number} as it is not active.")
                continue

            gt_codes = row.get("gt_code", [])
            if not isinstance(gt_codes, list):
                gt_codes = [gt_codes]

            target_lot_size = int(row.get("lot_size", 0)) or 0
            if target_lot_size == 0:
                output_messages.append(f"Skipping TBM model {model_number} due to invalid lot size.")
                continue

            spec_cony_upper = float(row.get("spec_cony_upper", 0))
            spec_cony_lower = float(row.get("spec_cony_lower", 0))
            spec_plys_upper = float(row.get("spec_plys_upper", 0))
            spec_plys_lower = float(row.get("spec_plys_lower", 0))
            sizecode = str(row.get("sizecode", ""))
            spec_rfppcc = float(row.get("spec_rfppcc", 0))
            spec_rfppcw = float(row.get("spec_rfppcw", 0))
            spec_rfh1cc = float(row.get("spec_rfh1cc", 0))
            spec_rfh1cw = float(row.get("spec_rfh1cw", 0))
            spec_rfh2cc = float(row.get("spec_rfh2cc", 0))
            spec_rfh2cw = float(row.get("spec_rfh2cw", 0))
            spec_lfppcc = float(row.get("spec_lfppcc", 0))
            spec_lfppcw = float(row.get("spec_lfppcw", 0))
            spec_crro = float(row.get("spec_crro", 0))
            spec_trro = float(row.get("spec_trro", 0))
            spec_brro = float(row.get("spec_brro", 0))
            spec_lrot = float(row.get("spec_lrot", 0))
            spec_lrob = float(row.get("spec_lrob", 0))
            spec_lroh1grade = float(row.get("spec_lroh1grade", 0))
            spec_tbul = float(row.get("spec_tbul", 0))
            spec_bbul = float(row.get("spec_bbul", 0))
            spec_tdep = float(row.get("spec_tdep", 0))
            spec_bdep = float(row.get("spec_bdep", 0))
            spec_upper = float(row.get("spec_upper", 0))
            spec_lower = float(row.get("spec_lower", 0))
            spec_sbal = float(row.get("spec_sbal", 0))
            spec_aircc = float(row.get("spec_aircc", 0))
            spec_aircw = float(row.get("spec_aircw", 0))
            spec_loadcc = float(row.get("spec_yield", 0))
            spec_loadcw = float(row.get("spec_yield", 0))

            ############### Add 16-07 start ######################

            # total_rank_yield_spec_yield = float(row.get("total_rank_yield_spec_yield", 0))
            spec_rfppwc_yield = float(row.get("spec_rfppwc_yield", 0))
            spec_rfh1wc_yield = float(row.get("spec_rfh1wc_yield", 0))
            spec_rfh2wc_yield = float(row.get("spec_rfh2wc_yield", 0))
            spec_lfppwc_yield = float(row.get("spec_lfppwc_yield", 0))
            spec_cony_yield = float(row.get("spec_cony_yield", 0))
            spec_plys_yield = float(row.get("spec_plys_yield", 0))
            spec_crro_yield = float(row.get("spec_crro_yield", 0))
            spec_trro_yield = float(row.get("spec_trro_yield", 0))
            spec_brro_yield = float(row.get("spec_brro_yield", 0))
            spec_lro_yield = float(row.get("spec_lro_yield", 0))
            spec_lroh1_yield = float(row.get("spec_lroh1_yield", 0))
            spec_crroh1_yield = float(row.get("spec_crroh1_yield", 0))
            spec_trroh1_yield = float(row.get("spec_trroh1_yield", 0))
            spec_brroh1_yield = float(row.get("spec_brroh1_yield", 0))
            spec_lrro_yield = float(row.get("spec_lrro_yield", 0))
            spec_sbal_yield = float(row.get("spec_sbal_yield", 0))
            spec_couple_yield = float(row.get("spec_couple_yield", 0))
            spec_upper_yield = float(row.get("spec_upper_yield", 0))
            spec_lower_yield = float(row.get("spec_lower_yield", 0))
            spec_overall_uniformity_yield = float(row.get("spec_overall_uniformity_yield", 0))
            spec_overall_visual_yield = float(row.get("spec_overall_visual_yield", 0))
            spec_overall_warehouse_yield = float(row.get("spec_overall_warehouse_yield", 0))
            spec_overall_scrap_yield = float(row.get("spec_overall_scrap_yield", 0))
            spec_visual_loss_yield = float(row.get("spec_visual_loss_yield", 0))
            spec_uf_loss_yield = float(row.get("spec_uf_loss_yield", 0))
            spec_oe_ok_yield = float(row.get("spec_oe_ok_yield", 0))

            ############### Add 16-07 end ######################

            threshold_value_plys = float(row.get("threshold_value_plys", 0.5))
            threshold_value_cony = float(row.get("threshold_value_cony", 0.5))
            threshold_value_rfppcc = float(row.get("threshold_value_rfppcc", 0.5))
            threshold_value_rfppcw = float(row.get("threshold_value_rfppcw", 0.5))
            threshold_value_rfh1cc = float(row.get("threshold_value_rfh1cc", 0.5))
            threshold_value_rfh1cw = float(row.get("threshold_value_rfh1cw", 0.5))
            threshold_value_rfh2cc = float(row.get("threshold_value_rfh2cc", 0.5))
            threshold_value_rfh2cw = float(row.get("threshold_value_rfh2cw", 0.5))
            threshold_value_lfppcc = float(row.get("threshold_value_lfppcc", 0.5))
            threshold_value_lfppcw = float(row.get("threshold_value_lfppcw", 0.5))
            threshold_value_crro = float(row.get("threshold_value_crro", 5))
            threshold_value_trro = float(row.get("threshold_value_trro", 5))
            threshold_value_brro = float(row.get("threshold_value_brro", 5))
            threshold_value_lrot = float(row.get("threshold_value_lrot", 5))
            threshold_value_lrob = float(row.get("threshold_value_lrob", 5))
            threshold_value_lroh1grade = float(row.get("threshold_value_lroh1grade", 5))
            threshold_value_tbul = float(row.get("threshold_value_tbul", 0))
            threshold_value_bbul = float(row.get("threshold_value_bbul", 5))
            threshold_value_tdep = float(row.get("threshold_value_tdep", 5))
            threshold_value_bdep = float(row.get("threshold_value_bdep", 5))
            threshold_value_upper = float(row.get("threshold_value_upper", 5))
            threshold_value_lower = float(row.get("threshold_value_lower", 5))
            threshold_value_sbal = float(row.get("threshold_value_sbal", 5))
            threshold_value_aircc = float(row.get("threshold_value_aircc", 5))
            threshold_value_aircw = float(row.get("threshold_value_aircw", 5))
            threshold_value_loadcc = float(row.get("threshold_value_loadcc", 5))
            threshold_value_loadcw = float(row.get("threshold_value_loadcw", 5))
            threshold_value_rfppwc_yield = float(row.get("threshold_value_rfppwc_yield", 5))
            threshold_value_rfh1wc_yield = float(row.get("threshold_value_rfh1wc_yield", 5))
            threshold_value_rfh2wc_yield = float(row.get("threshold_value_rfh2wc_yield", 5))
            threshold_value_lfppwc_yield = float(row.get("threshold_value_lfppwc_yield", 5))
            threshold_value_cony_yield = float(row.get("threshold_value_cony_yield", 5))
            threshold_value_plys_yield = float(row.get("threshold_value_plys_yield", 5))
            threshold_value_crro_yield = float(row.get("threshold_value_crro_yield", 5))
            threshold_value_trro_yield = float(row.get("threshold_value_trro_yield", 5))
            threshold_value_brro_yield = float(row.get("threshold_value_brro_yield", 5))
            threshold_value_lro_yield = float(row.get("threshold_value_lro_yield", 5))
            threshold_value_lroh1_yield = float(row.get("threshold_value_lroh1_yield", 5))
            threshold_value_crroh1_yield = float(row.get("threshold_value_crroh1_yield", 5))
            threshold_value_trroh1_yield = float(row.get("threshold_value_trroh1_yield", 5))
            threshold_value_brroh1_yield = float(row.get("threshold_value_brroh1_yield", 5))
            threshold_value_lrro_yield = float(row.get("threshold_value_lrro_yield", 5))
            threshold_value_sbal_yield = float(row.get("threshold_value_sbal_yield", 5))
            threshold_value_couple_yield = float(row.get("threshold_value_couple_yield", 5))
            threshold_value_upper_yield = float(row.get("threshold_value_upper_yield", 5))
            threshold_value_lower_yield = float(row.get("threshold_value_lower_yield", 5))
            threshold_value_overall_uniformity_yield = float(row.get("threshold_value_overall_uniformity_yield", 5))
            threshold_value_overall_visual_yield = float(row.get("threshold_value_overall_visual_yield", 5))
            threshold_value_overall_warehouse_yield = float(row.get("threshold_value_overall_warehouse_yield", 5))
            threshold_value_overall_scrap_yield = float(row.get("threshold_value_overall_scrap_yield", 5))
            threshold_value_visual_loss_yield = float(row.get("threshold_value_visual_loss_yield", 5))
            threshold_value_uf_loss_yield = float(row.get("threshold_value_uf_loss_yield", 5))
            threshold_value_oe_ok_yield = float(row.get("threshold_value_oe_ok_yield", 5))

            for gt_code in gt_codes:
                gt_code = str(gt_code).strip()

                # Base filtered data
                model_data_full = merge_df[
                    (merge_df["sku"] == model_number) &
                    # (merge_df["modelno_cimcorp"] == model_number) &
                    (merge_df["gt_code"] == gt_code) &
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
                                        "sku": model_number,
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
                                        "sku": model_number,
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
                            "sku": model_number,
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
                            "rfv_yield": rfv_yield(batch_data),
                            "rfh1_yield": rfh1_yield(batch_data),
                            "rfh2_yield": rfh2_yield(batch_data),
                            "lfv_yield": lfv_yield(batch_data),
                            "conicity_yield": conicity_yield(batch_data),
                            "plysteer_yield": plysteer_yield(batch_data),
                            "center_radial_run_out_yield": center_radial_run_out_yield(batch_data),
                            "top_radial_run_out_yield": top_radial_run_out_yield(batch_data),
                            "bottom_radial_run_out_yield": bottom_radial_run_out_yield(batch_data),
                            "lateral_run_out_yield": lateral_run_out_yield(batch_data),
                            "lateral_run_out_harmonic_first_yield": lateral_run_out_harmonic_first_yield(batch_data),
                            "center_radial_run_out_harmonic_first_yield": center_radial_run_out_harmonic_first_yield(batch_data),
                            "top_radial_run_out_harmonic_first_yield": top_radial_run_out_harmonic_first_yield(batch_data),
                            "bottom_radial_run_out_harmonic_first_yield": bottom_radial_run_out_harmonic_first_yield(batch_data),
                            "lateral_radial_run_out_yield": lateral_radial_run_out_yield(batch_data),
                            "static_yield": static_yield(batch_data),
                            "couple_yield": couple_yield(batch_data),
                            "upper_yield": upper_yield(batch_data),
                            "lower_yield": lower_yield(batch_data),
                            "overall_uniformity_yield": overall_uniformity_yield(batch_data),
                            "overall_visual_yield": overall_visual_yield(batch_data),
                            "overall_warehouse_yield": overall_warehouse_yield(batch_data),
                            "overall_scrap_yield": overall_scrap_yield(batch_data),
                            "visual_loss_yield": visual_loss_yield(batch_data),
                            "uf_loss_yield": uf_loss_yield(batch_data),
                            "oe_ok_yield": oe_ok_yield(batch_data)
                        })

                        grouped_averages = {}
                        param_spec_map = {
                            "cony": spec_cony_upper,
                            "cony": spec_cony_lower,
                            "plys": spec_plys_upper,
                            "plys": spec_plys_lower,
                            "rfppcc": spec_rfppcc,
                            "rfppcw": spec_rfppcw,
                            "rfh1cc": spec_rfh1cc,
                            "rfh1cw": spec_rfh1cw,
                            "rfh2cc": spec_rfh2cc,
                            "rfh2cw": spec_rfh2cw,
                            "lfppcc": spec_lfppcc,
                            "lfppcw": spec_lfppcw,
                            "crro": spec_crro,
                            "trro": spec_trro,
                            "brro": spec_brro,
                            "lrot": spec_lrot,
                            "lrob": spec_lrob,
                            "lroh1grade": spec_lroh1grade,
                            "tbul": spec_tbul,
                            "bbul": spec_bbul,
                            "tdep": spec_tdep,
                            "upper": spec_upper,
                            "lower": spec_lower,
                            "sbal": spec_sbal,
                            "aircc": spec_aircc,
                            "aircw": spec_aircw,
                            "loadcc": spec_loadcc,
                            "loadcw": spec_loadcw,

                        ############### Add 16-07 start ######################

                            "rfv_yield": spec_rfppwc_yield,
                            "rfh1_yield": spec_rfh1wc_yield,
                            "rfh2_yield": spec_rfh2wc_yield,
                            "lfv_yield": spec_lfppwc_yield,
                            "conicity_yield": spec_cony_yield,
                            "plysteer_yield": spec_plys_yield,
                            "center_radial_run_out_yield": spec_crro_yield,
                            "top_radial_run_out_yield": spec_trro_yield,
                            "bottom_radial_run_out_yield": spec_brro_yield,
                            "lateral_run_out_yield": spec_lro_yield,
                            "lateral_run_out_harmonic_first_yield": spec_lroh1_yield,
                            "center_radial_run_out_harmonic_first_yield": spec_crroh1_yield,
                            "top_radial_run_out_harmonic_first_yield": spec_trroh1_yield,
                            "bottom_radial_run_out_harmonic_first_yield": spec_brroh1_yield,
                            "lateral_radial_run_out_yield": spec_lrro_yield,
                            "static_yield": spec_sbal_yield,
                            "couple_yield": spec_couple_yield,
                            "upper_yield": spec_upper_yield,
                            "lower_yield": spec_lower_yield,
                            "overall_uniformity_yield": spec_overall_uniformity_yield,
                            "overall_visual_yield": spec_overall_visual_yield,
                            "overall_warehouse_yield": spec_overall_warehouse_yield,
                            "overall_scrap_yield": spec_overall_scrap_yield,
                            "visual_loss_yield": spec_visual_loss_yield,
                            "uf_loss_yield": spec_uf_loss_yield,
                            "oe_ok_yield": spec_oe_ok_yield

                        ############### Add 16-07 end ######################
                        }
                        try:
                            batch_data["uf_id"] = batch_data["uf_id"].astype(str).str.strip()
                            batch_data.sort_values("date_and_time", ascending=True, inplace=True)
                            ubno_grouped = batch_data.groupby("uf_id")
                            ubno_records = []
                            for uf_id, group in ubno_grouped:
                                group.sort_values("date_and_time", ascending=True)
                                record = {
                                    "uf_id": uf_id,
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
                                        if param == "cony" and spec_cony_lower is not None and spec_cony_upper is not None and avg_val is not None:
                                            exceeded = not (-spec_cony_lower <= avg_val <= spec_cony_upper)
                                        if param == "plys" and spec_plys_lower is not None and spec_plys_upper is not None and avg_val is not None:
                                            exceeded = not (spec_plys_lower <= avg_val <= spec_plys_upper)
                                        if spec is not None and param not in ["cony", "plys"] and avg_val is not None:
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
                            grouped_averages["uf_id"] = ubno_records
                        except Exception as e:
                            logging.error(f"Error processing uf_id grouped averages: {e}")
                            grouped_averages["uf_id"] = []

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
                                    if param == "cony" and spec_cony_lower is not None and spec_cony_upper is not None:
                                        if abs(current_val - prev_val) > threshold_value_cony:
                                            shift = True
                                        if not (-spec_cony_lower <= current_val <= spec_cony_upper):
                                            shift_occurred_spec = True
                                    if param == "plys" and spec_plys_lower is not None and spec_plys_upper is not None:
                                        if abs(current_val - prev_val) > threshold_value_plys:
                                            shift = True
                                        if not (-spec_plys_lower <= current_val <= spec_plys_upper):
                                            shift_occurred_spec = True
                                    if param == "rfppcc" and spec_rfppcc is not None:
                                        if (current_val - prev_val) > threshold_value_rfppcc:
                                            shift = True
                                        if current_val > spec_rfppcc:
                                            shift_occurred_spec = True
                                    if param == "rfppcw" and spec_rfppcw is not None:
                                        if (current_val - prev_val) > threshold_value_rfppcw:
                                            shift = True
                                        if current_val > spec_rfppcw:
                                            shift_occurred_spec = True
                                    if param == "rfh1cc" and spec_rfh1cc is not None:
                                        if (current_val - prev_val) > threshold_value_rfh1cc:
                                            shift = True
                                        if current_val > spec_rfh1cc:
                                            shift_occurred_spec = True
                                    if param == "rfh1cw" and spec_rfh1cw is not None:
                                        if (current_val - prev_val) > threshold_value_rfh1cw:
                                            shift = True
                                        if current_val > spec_rfh1cw:
                                            shift_occurred_spec = True
                                    if param == "rfh2cc" and spec_rfh2cc is not None:
                                        if (current_val - prev_val) > threshold_value_rfh2cc:
                                            shift = True
                                        if current_val > spec_rfh2cc:
                                            shift_occurred_spec = True
                                    if param == "rfh2cw" and spec_rfh2cw is not None:
                                        if (current_val - prev_val) > threshold_value_rfh2cw:
                                            shift = True
                                        if current_val > spec_rfh2cw:
                                            shift_occurred_spec = True
                                    if param == "lfppcc" and spec_lfppcc is not None:
                                        if (current_val - prev_val) > threshold_value_lfppcc:
                                            shift = True
                                        if current_val > spec_lfppcc:
                                            shift_occurred_spec = True
                                    if param == "lfppcw" and spec_lfppcw is not None:
                                        if (current_val - prev_val) > threshold_value_lfppcw:
                                            shift = True
                                        if current_val > spec_lfppcw:
                                            shift_occurred_spec = True
                                    if param == "crro" and spec_crro is not None:
                                        if (current_val - prev_val) > threshold_value_crro:
                                            shift = True
                                        if current_val > spec_crro:
                                            shift_occurred_spec = True
                                    if param == "trro" and spec_trro is not None:
                                        if (current_val - prev_val) > threshold_value_trro:
                                            shift = True
                                        if current_val > spec_trro:
                                            shift_occurred_spec = True
                                    if param == "brro" and spec_brro is not None:
                                        if (current_val - prev_val) > threshold_value_brro:
                                            shift = True
                                        if current_val > spec_brro:
                                            shift_occurred_spec = True
                                    if param == "lrot" and spec_lrot is not None:
                                        if (current_val - prev_val) > threshold_value_lrot:
                                            shift = True
                                        if current_val > spec_lrot:
                                            shift_occurred_spec = True
                                    if param == "lrob" and spec_lrob is not None:
                                        if (current_val - prev_val) > threshold_value_lrob:
                                            shift = True
                                        if current_val > spec_lrob:
                                            shift_occurred_spec = True
                                    if param == "lroh1grade" and spec_lroh1grade is not None:
                                        if (current_val - prev_val) > threshold_value_lroh1grade:
                                            shift = True
                                        if current_val > spec_lroh1grade:
                                            shift_occurred_spec = True
                                    if param == "tbul" and spec_tbul is not None:
                                        if (current_val - prev_val) > threshold_value_tbul:
                                            shift = True
                                        if current_val > spec_tbul:
                                            shift_occurred_spec = True
                                    if param == "bbul" and spec_bbul is not None:
                                        if (current_val - prev_val) > threshold_value_bbul:
                                            shift = True
                                        if current_val > spec_bbul:
                                            shift_occurred_spec = True
                                    if param == "tdep" and spec_tdep is not None:
                                        if (current_val - prev_val) > threshold_value_tdep:
                                            shift = True
                                        if current_val > spec_tdep:
                                            shift_occurred_spec = True
                                    if param == "bdep" and spec_bdep is not None:
                                        if (current_val - prev_val) > threshold_value_bdep:
                                            shift = True
                                        if current_val > spec_bdep:
                                            shift_occurred_spec = True
                                    if param == "upper" and spec_upper is not None:
                                        if (current_val - prev_val) > threshold_value_upper:
                                            shift = True
                                        if current_val > spec_upper:
                                            shift_occurred_spec = True
                                    if param == "lower" and spec_lower is not None:
                                        if (current_val - prev_val) > threshold_value_lower:
                                            shift = True
                                        if current_val > spec_lower:
                                            shift_occurred_spec = True
                                    if param == "sbal" and spec_sbal is not None:
                                        if (current_val - prev_val) > threshold_value_sbal:
                                            shift = True
                                        if current_val > spec_sbal:
                                            shift_occurred_spec = True
                                    if param == "aircc" and spec_aircc is not None:
                                        if (current_val - prev_val) > threshold_value_aircc:
                                            shift = True
                                        if current_val > spec_aircc:
                                            shift_occurred_spec = True
                                    if param == "aircw" and spec_aircw is not None:
                                        if (current_val - prev_val) > threshold_value_aircw:
                                            shift = True
                                        if current_val > spec_aircw:
                                            shift_occurred_spec = True
                                    if param == "loadcc" and spec_loadcc is not None:
                                        if (current_val - prev_val) > threshold_value_loadcc:
                                            shift = True
                                        if current_val > spec_loadcc:
                                            shift_occurred_spec = True
                                    if param == "loadcw" and spec_loadcw is not None:
                                        if (current_val - prev_val) > threshold_value_loadcw:
                                            shift = True
                                        if current_val > spec_loadcw:
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

                                    if param == "rfv_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_rfppwc_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "rfh1_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_rfh1wc_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "rfh2_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_rfh2wc_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "lfv_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_lfppwc_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "conicity_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_cony_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "plysteer_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_plys_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "center_radial_run_out_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_crro_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "top_radial_run_out_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_trro_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "bottom_radial_run_out_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_brro_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "lateral_run_out_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_lro_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "lateral_run_out_harmonic_first_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_lroh1_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "center_radial_run_out_harmonic_first_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_crroh1_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "top_radial_run_out_harmonic_first_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_trroh1_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "bottom_radial_run_out_harmonic_first_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_brroh1_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "lateral_radial_run_out_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_lrro_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "static_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_sbal_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "couple_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_couple_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "upper_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_upper_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "lower_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_lower_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "overall_uniformity_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_overall_uniformity_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "overall_visual_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_overall_visual_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "overall_warehouse_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_overall_warehouse_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "overall_scrap_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_overall_scrap_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "visual_loss_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_visual_loss_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "uf_loss_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_uf_loss_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True
                                    if param == "oe_ok_yield":
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_oe_ok_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
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
                                # if param == "load1":
                                #     alert_dict["test_load_tolerance"] = test_load_tolerance
                                # if param == "airpress1":
                                #     alert_dict["pressure_tolerance"] = pressure_tolerance
                                parameter_alerts.append(alert_dict)
                                if shift_occurred:
                                    overall_shift_occurred = True
                        else:
                            for param in numeric_params + yield_params:
                                shift_occurred_thershold = False
                                shift_occurred_spec = False
                                current_val = lot_averages.get(param)
                                if current_val is not None:
                                    if param == "cony" and spec_cony_lower is not None and spec_cony_upper is not None:
                                        if not (spec_cony_lower <= current_val <= spec_cony_upper):
                                            shift_occurred_spec = True
                                    if param == "plys" and spec_plys_lower is not None and spec_plys_lower is not None:
                                        if not (spec_plys_lower <= current_val <= spec_plys_upper):
                                            shift_occurred_spec = True
                                    if param == "rfppcc" and spec_rfppcc is not None:
                                        if current_val > spec_rfppcc:
                                            shift_occurred_spec = True
                                    if param == "rfppcw" and spec_rfppcw is not None:
                                        if current_val > spec_rfppcw:
                                            shift_occurred_spec = True
                                    if param == "rfh1cc" and spec_rfh1cc is not None:
                                        if current_val > spec_rfh1cc:
                                            shift_occurred_spec = True
                                    if param == "rfh1cw" and spec_rfh1cw is not None:
                                        if current_val > spec_rfh1cw:
                                            shift_occurred_spec = True
                                    if param == "rfh2cc" and spec_rfh2cc is not None:
                                        if current_val > spec_rfh2cc:
                                            shift_occurred_spec = True
                                    if param == "rfh2cw" and spec_rfh2cw is not None:
                                        if current_val > spec_rfh2cw:
                                            shift_occurred_spec = True
                                    if param == "lfppcc" and spec_lfppcc is not None:
                                        if current_val > spec_lfppcc:
                                            shift_occurred_spec = True
                                    if param == "lfppcw" and spec_lfppcw is not None:
                                        if current_val > spec_lfppcw:
                                            shift_occurred_spec = True
                                    if param == "crro" and spec_crro is not None:
                                        if current_val > spec_crro:
                                            shift_occurred_spec = True
                                    if param == "trro" and spec_trro is not None:
                                        if current_val > spec_trro:
                                            shift_occurred_spec = True
                                    if param == "brro" and spec_brro is not None:
                                        if current_val > spec_brro:
                                            shift_occurred_spec = True
                                    if param == "lrot" and spec_lrot is not None:
                                        if current_val > spec_lrot:
                                            shift_occurred_spec = True
                                    if param == "lrob" and spec_lrob is not None:
                                        if current_val > spec_lrob:
                                            shift_occurred_spec = True
                                    if param == "lroh1grade" and spec_lroh1grade is not None:
                                        if current_val > spec_lroh1grade:
                                            shift_occurred_spec = True
                                    if param == "tbul" and spec_tbul is not None:
                                        if current_val > spec_tbul:
                                            shift_occurred_spec = True
                                    if param == "bbul" and spec_bbul is not None:
                                        if current_val > spec_bbul:
                                            shift_occurred_spec = True
                                    if param == "tdep" and spec_tdep is not None:
                                        if current_val > spec_tdep:
                                            shift_occurred_spec = True
                                    if param == "upper" and spec_upper is not None:
                                        if current_val > spec_upper:
                                            shift_occurred_spec = True
                                    if param == "lower" and spec_lower is not None:
                                        if current_val > spec_lower:
                                            shift_occurred_spec = True
                                    if param == "sbal" and spec_sbal is not None:
                                        if current_val > spec_sbal:
                                            shift_occurred_spec = True
                                    if param == "aircc" and spec_aircc is not None:
                                        if current_val > spec_aircc:
                                            shift_occurred_spec = True
                                    if param == "aircw" and spec_aircw is not None:
                                        if current_val > spec_aircw:
                                            shift_occurred_spec = True
                                    if param == "loadcc" and spec_loadcc is not None:
                                        if current_val > spec_loadcc:
                                            shift_occurred_spec = True
                                    if param == "loadcw" and spec_loadcw is not None:
                                        if current_val > spec_loadcw:
                                            shift_occurred_spec = True
                                
                                ############### Add 16-07 start ######################

                                    if param in yield_params:
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if current_val < spec_yield:
                                                shift_occurred_spec = True
                                
                                ############### Add 16-07 end ######################

                                    # if param == "load1" and test_load is not None and test_load_tolerance is not None:
                                    #     if not (test_load - test_load_tolerance <= current_val <= test_load + test_load_tolerance):
                                    #         shift_occurred_spec = True
                                    # if param == "airpress1" and pressure is not None and pressure_tolerance is not None:
                                    #     if not (pressure - pressure_tolerance <= current_val <= pressure + pressure_tolerance):
                                    #         shift_occurred_spec = True
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
                                # if param == "load1":
                                #     alert_dict["test_load_tolerance"] = test_load_tolerance
                                # if param == "airpress1":
                                #     alert_dict["pressure_tolerance"] = pressure_tolerance
                                # parameter_alerts.append(alert_dict)
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
                                "sku": model_number,
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
                            "sku": model_number,
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
                            "sku": model_number,
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
                            "overall_uniformity_yield": overall_uniformity_yield(batch_data),
                            "overall_warehouse_yield": overall_warehouse_yield(batch_data),
                            "overall_visual_yield": overall_visual_yield(batch_data),
                            "overall_scrap_yield": overall_scrap_yield(batch_data),
                            "visual_loss_yield": visual_loss_yield(batch_data),
                            "uf_loss_yield": uf_loss_yield(batch_data),
                            "oe_ok_yield": oe_ok_yield(batch_data),
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
                            "barcodes": batch_data["barcode"].tolist() if "barcode" in batch_data.columns else []
                        }

                        alert_key = (model_number, unique_lot_id_current)
                        if alert_key not in alert_map:
                            last_alert_id += 1
                            alert_map[alert_key] = {
                                "alert_id_tbm_ufm": last_alert_id,
                                "sku": model_number,
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
                        "sku": alert_obj["sku"],
                        "lot_id": alert_obj["lot_id"],
                        "alert_flag": False,
                        "dashboard_flag": False,
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
                            "sku": alert_obj["sku"],
                            "lot_id": alert_obj["lot_id"],
                            "alert_flag": False,
                            "dashboard_flag": False,
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
                            "sku": alert_obj["sku"],
                            "lot_id": alert_obj["lot_id"],
                            "alert_flag": False,
                            "dashboard_flag": False,
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
                                "sku": alert_obj["sku"],
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

# def process_lot_and_alert_data():
#     process_start_time = time.time()
#     output_messages = []
#     inserted_counts = {}
#     updated_counts = {}
#     logging.info("process_lot_and_alert_data function is just started")
    
#     # ============== FIX: Initialize all bulk operation lists at the beginning ==============
#     ufm_lot_bulk_operations = []
#     ufm_averages_bulk_operations = []
#     ufm_alert_bulk_operations = []
#     ufm_triggering_bulk_operations = []
#     ufm_bulk_operations = []
#     deviated_triggering_bulk_operations = []
#     ufm_bulk_updates_merge = []
    
#     try:
#         # UFM Specific Collections
#         merge_collect = client[TUB_CIMCORP_VISUAL_MERGED_DB_alert][DATA_MERGED_SUMMARY_COLLECTION_alert]
#         channel_collect = client[config_databse][channel_no_collect]
#         machine_collect = client[config_databse][machine_no_collect]
#         lot_ufm_collect = client[lot_db][lot_table_collect]
#         alert_collect = client[alert_table][alert_ufm_collect_name]
#         averages_ufm_collect = client[average_db][averages_ufm_table]
#         ub_machine_collect = client[config_databse][ub_machine_no_collect]
#         alert_ufm_collect = client[alert_db][alert_triggering_ubno_collect_name]
#         alert_triggering_deviated_collect = client[alert_deviated][alert_triggering_deviated_ufm]
#         alert_triggering_collect_dashboard_ufm = client[alert_db_dashboard][alert_triggering_ubno_collect_name]
#         # === NEW: Virgin Yield & Final Yield Collections ===
#         alert_triggering_virgin_yield_collect = client[alert_db_virgin_yield][alert_triggering_ubno_collect_name]
#         alert_triggering_final_yield_collect = client[alert_db_final_yield][alert_triggering_ubno_collect_name]
#         # === NEW: YOUR DEDICATED FINAL YIELD DATABASES (EXACT REQUEST) ===
#         alert_final_yield_collect = client["alert_table_final_yield"][alert_ufm_collect_name]
#         alert_deviated_final_yield_collect = client["alert_deviated_final_yield"][alert_triggering_deviated_ufm]
#         # NEW: Initialize collections for model_workcenter_db
#         ufm_collect_data = client[model_db][model_no_status_collection]
#         ufm_machine_collect = client[model_db][ufm_collection_machine]
        
#         # ============== NEW: Collection to track incomplete lots across runs ==============
#         incomplete_lots_collect = client[config_databse]["incomplete_lots_tracker"]
        
#         logging.info("✅ UFM Collections loaded successfully for alert_db and alert_db_dashboard.")
#         output_messages.append("✅ UFM Collections loaded successfully for lot and alert processing in alert_db and alert_db_dashboard.")
#     except pymongo.errors.OperationFailure as e:
#         error_message = f"❌ UFM Database or collection error during processing: {e}"
#         logging.error(error_message)
#         output_messages.append(error_message)
#         return {"status": "error", "message": error_message, "output_log": output_messages}, 500

#     # Fetch active machines from model_workcenter_db.ufm_machine
#     try:
#         active_machines = set(ufm_machine_collect.distinct("ubno", {"status": "active"}))
#         output_messages.append(f"✅ Fetched {len(active_machines)} active machines: {active_machines}")
#         if not active_machines:
#             output_messages.append("❌ No active machines found in model_workcenter_db.ufm_machine. Exiting.")
#             return {"status": "error", "message": "No active machines found", "output_log": output_messages}, 500
#     except Exception as e:
#         output_messages.append(f"❌ Error fetching active machines: {e}")
#         return {"status": "error", "message": "Failed to fetch active machines", "output_log": output_messages}, 500

#     # # Fetch active model numbers
#     # try:
#     #     active_modelnos = set(ufm_collect_data.distinct("sku", {"status": "active"}))
#     #     output_messages.append(f"✅ Fetched {len(active_modelnos)} active model numbers: {active_modelnos}")
#     # except Exception as e:
#     #     output_messages.append(f"❌ Error fetching active model numbers: {e}")
#     #     return {"status": "error", "message": "Failed to fetch active model numbers", "output_log": output_messages}, 500

#     # Fetch active OE model numbers
#     try:
#         active_modelnos = set(
#             ufm_collect_data.distinct(
#                 "sku",
#                 {
#                     "status": "active",
#                     "oe_replacement_slob": "OE"
#                 }
#             )
#         )

#         output_messages.append(
#             f"✅ Fetched {len(active_modelnos)} active OE model numbers: {active_modelnos}"
#         )

#     except Exception as e:
#         output_messages.append(f"❌ Error fetching active model numbers: {e}")
#         return {
#             "status": "error",
#             "message": "Failed to fetch active model numbers",
#             "output_log": output_messages
#         }, 500

#     # Create unique indexes
#     try:
#         lot_ufm_collect.create_index(
#             [
#                 ("sku", pymongo.ASCENDING),
#                 ("machine", pymongo.ASCENDING),
#                 ("lot_id", pymongo.ASCENDING),
#                 ("ufm_machine_model_lot_id", pymongo.ASCENDING)
#             ],
#             unique=True,
#             name="ufm_lot_unique_idx"
#         )
#         alert_ufm_collect.create_index(
#             [
#                 ("sku", pymongo.ASCENDING),
#                 ("lot_id", pymongo.ASCENDING),
#                 ("alert_id_ufm_ufm", pymongo.ASCENDING)
#             ],
#             unique=True,
#             name="ufm_alert_unique_idx"
#         )
#         # ============== NEW: Create index for incomplete lots tracker ==============
#         incomplete_lots_collect.create_index(
#             [
#                 ("sku", pymongo.ASCENDING),
#                 ("machine", pymongo.ASCENDING),
#                 ("lot_id", pymongo.ASCENDING)
#             ],
#             unique=True,
#             name="incomplete_lots_unique_idx"
#         )
#         logging.info("✅ UFM Indexes checked/created.")
#         output_messages.append("✅ UFM Indexes checked/created.")
#     except Exception as e:
#         logging.warning("⚠️ Warning: Unable to create/check UFM unique indexes. %s", e)
#         output_messages.append(f"⚠️ Warning: Unable to create/check UFM unique indexes: {e}")

#     # Load data
#     try:
#         merge1 = list(merge_collect.find({}))
#         if not merge1:
#             logging.info("ℹ️ No documents found in the merge collection. Exiting UFM processing.")
#             output_messages.append("ℹ️ No documents found in the merge collection.")
#             return {"status": "success", "message": "No data to process.", "output_log": output_messages}, 200
#         merge_df = pd.DataFrame(merge1)
#         channel_data = list(channel_collect.find({"sku": {"$in": list(active_modelnos)}}))
#         channel_data = list(channel_collect.find({}))
#         channel_df = pd.DataFrame(channel_data)
#         machine_data = list(machine_collect.find({}))
#         machine_df = pd.DataFrame(machine_data)
#         ub_machine_data = list(ub_machine_collect.find({"ubno": {"$in": list(active_machines)}}))
#         ub_machine_data = list(ub_machine_collect.find({}))
#         ub_machine_df = pd.DataFrame(ub_machine_data)

#         logging.info("✅ UFM Data loaded into DataFrames.")
#         output_messages.append("✅ UFM Data loaded into DataFrames.")
#     except Exception as e:
#         error_message = f"❌ UFM Error loading data: {e}"
#         logging.error(error_message)
#         output_messages.append(error_message)
#         return {"status": "error", "message": error_message, "output_log": output_messages}, 500
    
#     # MODIFIED: Check for ptb_id column before adding tbm_machine column
#     if 'ptb_id' in merge_df.columns and 'machine' in machine_df.columns:
#         try:
#             merge_df["tbm_machine"] = None
#             for index, row in merge_df.iterrows():
#                 tbm_value = row.get("ptb_id")
#                 if tbm_value:
#                     matching_channel = machine_df[machine_df["machine"] == tbm_value]
#                     if not matching_channel.empty:
#                         merge_df.at[index, "tbm_machine"] = matching_channel["machine"].iloc[0]
#             output_messages.append("✅ Added tbm_machine column to merge_df based on matching cimcorp_tbm_tub values.")
#         except Exception as e:
#             output_messages.append(f"⚠️ Warning: Failed to add tbm_machine column to merge_df: {e}. Continuing without tbm_machine.")
#     else:
#         output_messages.append("⚠️ Warning: 'cimcorp_tbm_tub' column not found in merge_df or machine_df. Skipping tbm_machine column creation.")

#     # MODIFIED: Check for cimcorp_tbm_tub column before updating machine_df
#     if 'machine' in machine_df.columns and 'ptb_id' in merge_df.columns and 'tbm_machine' in merge_df.columns:
#         try:
#             for index, row in machine_df.iterrows():
#                 tbm_value = row.get("machine")
#                 if tbm_value:
#                     matching_merge = merge_df[merge_df["ptb_id"] == tbm_value]
#                     if not matching_merge.empty and "tbm_machine" in matching_merge.columns:
#                         machine_df.at[index, "machine"] = matching_merge["tbm_machine"].iloc[0]
#             output_messages.append("✅ Updated machine_df machine column with tbm_machine values from merge_df.")
#         except Exception as e:
#             output_messages.append(f"⚠️ Warning: Failed to update machine_df machine column: {e}. Continuing without updating channel_df machine.")
#     else:
#         output_messages.append("⚠️ Warning: 'cimcorp_tbm_tub' column not found in merge_df or machine_df. Skipping machine_df machine column update.")

#     # Filter data
#     try:
#         channel_modelnos = set(channel_collect.distinct("sku"))
#         merge_df = merge_df[merge_df["sku"].isin(channel_modelnos)]
#         logging.info("Filtered merge_df for models in channel config. Rows remaining: %s", len(merge_df))
#         output_messages.append(f"Filtered merge_df for models in channel config. Rows remaining: {len(merge_df)}")

#         merge_df = merge_df[merge_df["alert_id_ufm_ufm"].isna()]
#         logging.info("Filtered merge_df for unassigned alert_id_ufm_ufm. Rows remaining: %s", len(merge_df))
#         output_messages.append(f"Filtered merge_df for unassigned alert_id_ufm_ufm. Rows remaining: {len(merge_df)}")

#         merge_df = merge_df[merge_df["ufm_machine_model_lot_id"].isna()]
#         logging.info("Filtered merge_df for unassigned ufm_machine_model_lot_id. Rows remaining: %s", len(merge_df))
#         output_messages.append(f"Filtered merge_df for unassigned ufm_machine_model_lot_id. Rows remaining: {len(merge_df)}")

#         if merge_df.empty:
#             logging.info("No data required lot for processing UFM.")
#             output_messages.append("No valid data after filtering for processing UFM.")
#             return {"status": "success", "message": "No new data to process UFM.", "output_log": output_messages}, "no data", 200

#     except KeyError as e:
#         error_message = f"Failed to filter UFM: Error - DataFrame issue: Column missing {e}. Ensure 'alert_id_ufm_ufm' and 'ufm_machine_model_lot_id' are present in '{tub_collection_name}'."
#         logging.error(error_message)
#         output_messages.append(error_message)
#         return {"status": "error", "message": error_message, "output_log": output_messages}, 500
#     except Exception as e:
#         error_message = f"Failed to filter UFM data: {e}"
#         logging.error(error_message)
#         output_messages.append(error_message)
#         return {"status": "error", "message": error_message, "output_log": output_messages}, 500

#     # Date handling & Preprocessing
#     try:
#         merge_df["date_of_tbm"] = merge_df["ptb_time"]
#         merge_df["date_and_time"] = merge_df["uf_time"]
#         merge_df["date_of_ufm"] = pd.to_datetime(merge_df["date_and_time"], errors="coerce").dt.strftime('%Y-%m-%d %H:%M:%S').fillna("No Date")

#         def convert_datetime_with_invalid_mask(series, col_name):
#             try:
#                 original_series = series.copy()
#                 cleaned_series = original_series.astype(str).str.strip().str.replace(r'\s+', ' ', regex=True)
#                 datetime_series = pd.to_datetime(cleaned_series, format='%Y-%m-%d %H:%M:%S', errors='coerce')
#                 invalid_mask = datetime_series.isna() & original_series.notna()
#                 datetime_series[invalid_mask] = DEFAULT_DATETIME
#                 return datetime_series.dt.strftime('%Y-%m-%d %H:%M:%S')
#             except Exception as e:
#                 logging.error(f"Error in convert_datetime_with_invalid_mask for {col_name}: {e}")
#                 return pd.Series([DEFAULT_DATETIME.strftime('%Y-%m-%d %H:%M:%S')] * len(series))

#         merge_df["date_of_ufm"] = convert_datetime_with_invalid_mask(merge_df["date_of_ufm"], "date_of_ufm")
#         merge_df["date_of_ufm"] = merge_df["date_of_ufm"].astype(object).where(merge_df["date_of_ufm"].notnull(), "No Date")
#         merge_df["date_of_tbm"] = convert_datetime_with_invalid_mask(merge_df["date_of_tbm"], "date_of_tbm")
#         merge_df["date_of_tbm"] = merge_df["date_of_tbm"].astype(object).where(merge_df["date_of_tbm"].notnull(), "No Date")

#         merge_df["tbm_shift"] = merge_df["date_of_tbm"].apply(get_shift)

#         merge_df["uf_id"] = merge_df["uf_id"].astype(str)
#         ub_machine_df["ubno"] = ub_machine_df["ubno"].astype(str)
#         # NEW: Filter merge_df to include only active machines
#         merge_df = merge_df[merge_df["uf_id"].isin(active_machines)]
#         if merge_df.empty:
#             output_messages.append("❌ No data in merge_df after filtering for active machines. Exiting.")
#             return {"status": "error", "message": "No data after active machine filtering", "output_log": output_messages}, 500

#         for col in ["lot_id", "previous_lot_id", "ufm_machine_model_lot_id", "ufm_previous_machine_model_lot_id", "alert_id_ufm_ufm"]:
#             if col not in merge_df.columns:
#                 merge_df[col] = None

#         logging.info("✅ UFM Date conversion and preprocessing completed.")
#         output_messages.append("✅ UFM Date conversion and preprocessing completed.")
#     except Exception as e:
#         error_message = f"❌ UFM Error during date/preprocessing: {e}"
#         logging.error(error_message)
#         output_messages.append(error_message)
#         return {"status": "error", "message": error_message, "output_log": output_messages}, 500

#     # Fetch last alert ID
#     # ------------------------------------------------------------------
#     # FIX: alert_id_ufm_ufm was seeded ONLY from alert_ufm_collect (the
#     # deviated/triggering collection), but a lot's alert is written there
#     # ONLY when a shift occurred. Every lot's virgin & final yield alerts,
#     # however, are always inserted into their own collections. So across
#     # runs the max id in alert_ufm_collect lagged behind the virgin/final
#     # collections, and new lots reused ids that already existed there
#     # (two different lots getting the same alert_id_ufm_ufm).
#     #
#     # We now seed from the MAXIMUM alert_id_ufm_ufm across EVERY collection
#     # that persists it, so each new alert always gets a fresh, unique id.
#     try:
#         _id_source_collections = [
#             alert_ufm_collect,
#             alert_collect,
#             alert_triggering_collect_dashboard_ufm,
#             alert_triggering_deviated_collect,
#             alert_triggering_virgin_yield_collect,
#             alert_triggering_final_yield_collect,
#             alert_final_yield_collect,
#         ]
#         last_alert_id = 0
#         for _coll in _id_source_collections:
#             try:
#                 _doc = _coll.find_one(
#                     {"alert_id_ufm_ufm": {"$ne": None}},
#                     sort=[("alert_id_ufm_ufm", -1)]
#                 )
#                 if _doc and _doc.get("alert_id_ufm_ufm") is not None:
#                     _val = int(_doc["alert_id_ufm_ufm"])
#                     if _val > last_alert_id:
#                         last_alert_id = _val
#             except Exception as _inner:
#                 logging.warning("⚠️ UFM Warning reading max alert id from %s: %s",
#                                 getattr(_coll, "name", "unknown"), _inner)
#         logging.info("ℹ️ Last UFM Alert ID found (max across collections): %s", last_alert_id)
#         output_messages.append(f"ℹ️ Last UFM Alert ID found (max across collections): {last_alert_id}")
#     except Exception as e:
#         logging.warning("⚠️ UFM Warning fetching last alert ID: %s. Starting from 0.", e)
#         output_messages.append(f"⚠️ UFM Warning fetching last alert ID: {e}. Starting from 0.")
#         last_alert_id = 0

#     # ============== FIX: Fetch last lot numbers per machine-model combination from database ==============
#     try:
#         # Get all existing lot numbers for each machine-sku combination
#         last_lots_agg = list(lot_ufm_collect.aggregate([
#             {"$match": {
#                 "lot_id": {"$regex": "^UFM_[^_]+_[^_]+_\\d+$"},
#                 "sku": {"$in": list(active_modelnos)},
#                 "machine": {"$in": list(active_machines)}
#             }},
#             {"$addFields": {
#                 "lot_number": {
#                     "$toInt": {"$arrayElemAt": [{"$split": ["$lot_id", "_"]}, -1]}
#                 }
#             }},
#             {"$sort": {"lot_number": -1}},
#             {"$group": {
#                 "_id": {"sku": "$sku", "machine": "$machine"},
#                 "max_lot_number": {"$first": "$lot_number"},
#                 "last_lot_id": {"$first": "$lot_id"},
#                 "last_ufm_machine_model_lot_id": {"$first": "$ufm_machine_model_lot_id"}
#             }}
#         ]))
#         last_lot_dict = {
#             (item["_id"]["sku"], item["_id"]["machine"]): {
#                 "max_lot_number": item["max_lot_number"],
#                 "last_lot_id": item["last_lot_id"],
#                 "last_ufm_machine_model_lot_id": item["last_ufm_machine_model_lot_id"]
#             } for item in last_lots_agg
#         }
#         output_messages.append(f"ℹ️ Last UFM Lot numbers fetched: {len(last_lot_dict)} entries")
        
#         # Log the current state for debugging
#         for key, value in last_lot_dict.items():
#             logging.info(f"Machine-SKU: {key}, Max Lot: {value['max_lot_number']}, Last UFM ID: {value['last_ufm_machine_model_lot_id']}")
            
#     except Exception as e:
#         logging.warning("⚠️ UFM Error fetching last lot numbers: %s. Starting counts from 0.", e)
#         output_messages.append(f"⚠️ UFM Error fetching last lot numbers: {e}. Starting counts from 0.")
#         last_lot_dict = {}

#     # ============== FIX: Fetch last averages to get previous ufm_machine_model_lot_id ==============
#     try:
#         last_averages = list(averages_ufm_collect.aggregate([
#             {
#                 "$match": {
#                     "ufm_machine_model_lot_id": {"$regex": "^[^_]+_[^_]+_UFM_[^_]+_[^_]+_\\d+_\\d+$"},
#                     "sku": {"$in": list(active_modelnos)},
#                     "machine": {"$in": list(active_machines)}
#                 }
#             },
#             {
#                 "$addFields": {
#                     "lot_number": {
#                         "$toInt": {"$arrayElemAt": [{"$split": ["$lot_id", "_"]}, -1]}
#                     }
#                 }
#             },
#             {
#                 "$sort": {"lot_number": -1}
#             },
#             {
#                 "$group": {
#                     "_id": {"sku": "$sku", "machine": "$machine"},
#                     "last_ufm_machine_model_lot_id": {"$first": "$ufm_machine_model_lot_id"},
#                     "last_averages": {"$first": "$current_lot_averages"}
#                 }
#             }
#         ]))
#         ufm_previous_machine_model_lot_ids = {}
#         ufm_machine_lot_avg_map = {}
#         for item in last_averages:
#             sku = item["_id"]["sku"]
#             machine = item["_id"]["machine"]
#             ufm_lot_id = item["last_ufm_machine_model_lot_id"]
#             averages = item["last_averages"]
#             ufm_previous_machine_model_lot_ids[(sku, machine)] = ufm_lot_id
#             if averages:
#                 ufm_machine_lot_avg_map[(sku, machine, ufm_lot_id)] = averages
#         output_messages.append(f"Initialized ufm_previous_machine_model_lot_ids with {len(ufm_previous_machine_model_lot_ids)} entries")
#     except Exception as e:
#         logging.error(f"Error pre-populating ufm_machine_lot_avg_map and ufm_previous_machine_model_lot_ids: {e}")
#         output_messages.append(f"❌ Error pre-populating ufm_machine_lot_avg_map and ufm_previous_machine_model_lot_ids: {e}")
#         ufm_previous_machine_model_lot_ids = {}
#         ufm_machine_lot_avg_map = {}

#     # ============== NEW: Load incomplete lots from database ==============
#     try:
#         incomplete_lots_from_db = list(incomplete_lots_collect.find({}))
#         incomplete_lots_dict = {}
#         for incomplete_lot in incomplete_lots_from_db:
#             key = (incomplete_lot["sku"], incomplete_lot["machine"])
#             if key not in incomplete_lots_dict:
#                 incomplete_lots_dict[key] = []
#             incomplete_lots_dict[key].append(incomplete_lot)
#         logging.info(f"Loaded {len(incomplete_lots_from_db)} incomplete lots from database")
#         output_messages.append(f"Loaded {len(incomplete_lots_from_db)} incomplete lots from database")
#     except Exception as e:
#         logging.error(f"Error loading incomplete lots from database: {e}")
#         output_messages.append(f"❌ Error loading incomplete lots from database: {e}")
#         incomplete_lots_dict = {}

#     ufm_params = [
#         "rfppcc","rfppcw","rfh1cc","rfh1cw","rfh2cc","rfh2cw","lfppcc","lfppcw","cony","plys",
#         "crro","trro","brro","lrot","lrob","lroh1grade","tbul","bbul","tdep","bdep",
#         "upper","lower","sbal","aircc","aircw","loadcc","loadcw"
#     ]
#     yield_params = [
#         "rfv_yield", "rfh1_yield", "rfh2_yield", "lfv_yield", "conicity_yield", "plysteer_yield",
#         "center_radial_run_out_yield", "top_radial_run_out_yield", "bottom_radial_run_out_yield", 
#         "lateral_run_out_yield", "lateral_run_out_harmonic_first_yield", "center_radial_run_out_harmonic_first_yield",
#         "top_radial_run_out_harmonic_first_yield", "bottom_radial_run_out_harmonic_first_yield",
#         "lateral_radial_run_out_yield", "static_yield", "couple_yield",
#         "upper_yield", "lower_yield", "overall_uniformity_yield", "overall_visual_yield", "overall_warehouse_yield",
#         "overall_scrap_yield", "visual_loss_yield", "uf_loss_yield", "oe_ok_yield"
#     ]

#     rank_param_mappings = {
#         "rfv_yield": ["rfppwcgrade"],
#         "rfh1_yield": ["rfh1wcgrade"],
#         "rfh2_yield": ["rfh2wcgrade"],
#         "lfv_yield": ["lfppwcgrade"],
#         "conicity_yield": ["conygrade"],
#         "plysteer_yield": ["plysgrade"],
#         "center_radial_run_out_yield": ["crrograde"], 
#         "top_radial_run_out_yield": ["trrograde"], 
#         "bottom_radial_run_out_yield": ["brrograde"],
#         "lateral_run_out_yield": ["lrograde"],
#         "lateral_run_out_harmonic_first_yield": ["lroh1grade"],
#         "center_radial_run_out_harmonic_first_yield": ["crroh1grade"],
#         "top_radial_run_out_harmonic_first_yield": ["trroh1grade"],
#         "bottom_radial_run_out_harmonic_first_yield": ["brroh1grade"],
#         "lateral_radial_run_out_yield": ["lrrograde"],
#         "static_yield": ["sbalgrade"],
#         "couple_yield": ["couplegrade"],
#         "upper_yield": ["uppergrade"],
#         "lower_yield": ["lowergrade"]
#     }

#     # ============== FIX: Track already processed barcodes to avoid duplicates ==============
#     processed_barcodes_per_machine_model = {}
    
#     # ============== FIX: Track lot sizes and status to handle partial lots ==============
#     lot_status_tracker = {}
    
#     # ============== FIX: Track alerts that have been inserted in this run ==============
#     inserted_alerts_in_run = set()
    
#     # ============== NEW: Track incomplete lots to update/delete from database ==============
#     incomplete_lots_to_update = []
#     incomplete_lots_to_delete = []

#     for _, row in channel_df.iterrows():
#         try:
#             model_number = str(row.get("sku", "")).strip()
#             if model_number not in active_modelnos:
#                 output_messages.append(f"Skipping CURING model {model_number} as it is not active.")
#                 continue
#             gt_codes = row.get("gt_code", [])

#             # Ensure gt_codes is a list
#             if not isinstance(gt_codes, list):
#                 gt_codes = [gt_codes]

#             for gt_code in gt_codes:
#                 gt_code = str(gt_code).strip()

#                 # MODIFIED: Filter merge_df for each gt_code individually
#                 model_data = merge_df[
#                     (merge_df["sku"] == model_number) &
#                     (merge_df["uf_id"].isin(active_machines))
#                 ].copy()

#                 target_lot_size = int(row.get("lot_size", 0))
#                 if target_lot_size == 0:
#                     logging.info(f"Skipping UFM model {model_number} due to invalid lot size.")
#                     output_messages.append(f"Skipping UFM model {model_number} due to invalid lot size.")
#                     continue
#                 spec_cony_upper = float(row.get("spec_cony_upper", 0))
#                 spec_cony_lower = float(row.get("spec_cony_lower", 0))
#                 spec_plys_upper = float(row.get("spec_plys_upper", 0))
#                 spec_plys_lower = float(row.get("spec_plys_lower", 0))
#                 sizecode = str(row.get("sizecode", ""))
#                 spec_rfppcc = float(row.get("spec_rfppcc", 0))
#                 spec_rfppcw = float(row.get("spec_rfppcw", 0))
#                 spec_rfh1cc = float(row.get("spec_rfh1cc", 0))
#                 spec_rfh1cw = float(row.get("spec_rfh1cw", 0))
#                 spec_rfh2cc = float(row.get("spec_rfh2cc", 0))
#                 spec_rfh2cw = float(row.get("spec_rfh2cw", 0))
#                 spec_lfppcc = float(row.get("spec_lfppcc", 0))
#                 spec_lfppcw = float(row.get("spec_lfppcw", 0))
#                 spec_crro = float(row.get("spec_crro", 0))
#                 spec_trro = float(row.get("spec_trro", 0))
#                 spec_brro = float(row.get("spec_brro", 0))
#                 spec_lrot = float(row.get("spec_lrot", 0))
#                 spec_lrob = float(row.get("spec_lrob", 0))
#                 spec_lroh1grade = float(row.get("spec_lroh1grade", 0))
#                 spec_tbul = float(row.get("spec_tbul", 0))
#                 spec_bbul = float(row.get("spec_bbul", 0))
#                 spec_tdep = float(row.get("spec_tdep", 0))
#                 spec_bdep = float(row.get("spec_bdep", 0))
#                 spec_upper = float(row.get("spec_upper", 0))
#                 spec_lower = float(row.get("spec_lower", 0))
#                 spec_sbal = float(row.get("spec_sbal", 0))
#                 spec_aircc = float(row.get("spec_aircc", 0))
#                 spec_aircw = float(row.get("spec_aircw", 0))
#                 spec_loadcc = float(row.get("spec_yield", 0))
#                 spec_loadcw = float(row.get("spec_yield", 0))

#                 spec_rfppwc_yield = float(row.get("spec_rfppwc_yield", 0))
#                 spec_rfh1wc_yield = float(row.get("spec_rfh1wc_yield", 0))
#                 spec_rfh2wc_yield = float(row.get("spec_rfh2wc_yield", 0))
#                 spec_lfppwc_yield = float(row.get("spec_lfppwc_yield", 0))
#                 spec_cony_yield = float(row.get("spec_cony_yield", 0))
#                 spec_plys_yield = float(row.get("spec_plys_yield", 0))
#                 spec_crro_yield = float(row.get("spec_crro_yield", 0))
#                 spec_trro_yield = float(row.get("spec_trro_yield", 0))
#                 spec_brro_yield = float(row.get("spec_brro_yield", 0))
#                 spec_lro_yield = float(row.get("spec_lro_yield", 0))
#                 spec_lroh1_yield = float(row.get("spec_lroh1_yield", 0))
#                 spec_crroh1_yield = float(row.get("spec_crroh1_yield", 0))
#                 spec_trroh1_yield = float(row.get("spec_trroh1_yield", 0))
#                 spec_brroh1_yield = float(row.get("spec_brroh1_yield", 0))
#                 spec_lrro_yield = float(row.get("spec_lrro_yield", 0))
#                 spec_sbal_yield = float(row.get("spec_sbal_yield", 0))
#                 spec_couple_yield = float(row.get("spec_couple_yield", 0))
#                 spec_upper_yield = float(row.get("spec_upper_yield", 0))
#                 spec_lower_yield = float(row.get("spec_lower_yield", 0))
#                 spec_overall_uniformity_yield = float(row.get("spec_overall_uniformity_yield", 0))
#                 spec_overall_visual_yield = float(row.get("spec_overall_visual_yield", 0))
#                 spec_overall_warehouse_yield = float(row.get("spec_overall_warehouse_yield", 0))
#                 spec_overall_scrap_yield = float(row.get("spec_overall_scrap_yield", 0))
#                 spec_visual_loss_yield = float(row.get("spec_visual_loss_yield", 0))
#                 spec_uf_loss_yield = float(row.get("spec_uf_loss_yield", 0))
#                 spec_oe_ok_yield = float(row.get("spec_oe_ok_yield", 0))

#                 threshold_value_plys = float(row.get("threshold_value_plys", 0.5))
#                 threshold_value_cony = float(row.get("threshold_value_cony", 0.5))
#                 threshold_value_rfppcc = float(row.get("threshold_value_rfppcc", 0.5))
#                 threshold_value_rfppcw = float(row.get("threshold_value_rfppcw", 0.5))
#                 threshold_value_rfh1cc = float(row.get("threshold_value_rfh1cc", 0.5))
#                 threshold_value_rfh1cw = float(row.get("threshold_value_rfh1cw", 0.5))
#                 threshold_value_rfh2cc = float(row.get("threshold_value_rfh2cc", 0.5))
#                 threshold_value_rfh2cw = float(row.get("threshold_value_rfh2cw", 0.5))
#                 threshold_value_lfppcc = float(row.get("threshold_value_lfppcc", 0.5))
#                 threshold_value_lfppcw = float(row.get("threshold_value_lfppcw", 0.5))
#                 threshold_value_crro = float(row.get("threshold_value_crro", 5))
#                 threshold_value_trro = float(row.get("threshold_value_trro", 5))
#                 threshold_value_brro = float(row.get("threshold_value_brro", 5))
#                 threshold_value_lrot = float(row.get("threshold_value_lrot", 5))
#                 threshold_value_lrob = float(row.get("threshold_value_lrob", 5))
#                 threshold_value_lroh1grade = float(row.get("threshold_value_lroh1grade", 5))
#                 threshold_value_tbul = float(row.get("threshold_value_tbul", 0))
#                 threshold_value_bbul = float(row.get("threshold_value_bbul", 5))
#                 threshold_value_tdep = float(row.get("threshold_value_tdep", 5))
#                 threshold_value_bdep = float(row.get("threshold_value_bdep", 5))
#                 threshold_value_upper = float(row.get("threshold_value_upper", 5))
#                 threshold_value_lower = float(row.get("threshold_value_lower", 5))
#                 threshold_value_sbal = float(row.get("threshold_value_sbal", 5))
#                 threshold_value_aircc = float(row.get("threshold_value_aircc", 5))
#                 threshold_value_aircw = float(row.get("threshold_value_aircw", 5))
#                 threshold_value_loadcc = float(row.get("threshold_value_loadcc", 5))
#                 threshold_value_loadcw = float(row.get("threshold_value_loadcw", 5))
#                 threshold_value_rfppwc_yield = float(row.get("threshold_value_rfppwc_yield", 5))
#                 threshold_value_rfh1wc_yield = float(row.get("threshold_value_rfh1wc_yield", 5))
#                 threshold_value_rfh2wc_yield = float(row.get("threshold_value_rfh2wc_yield", 5))
#                 threshold_value_lfppwc_yield = float(row.get("threshold_value_lfppwc_yield", 5))
#                 threshold_value_cony_yield = float(row.get("threshold_value_cony_yield", 5))
#                 threshold_value_plys_yield = float(row.get("threshold_value_plys_yield", 5))
#                 threshold_value_crro_yield = float(row.get("threshold_value_crro_yield", 5))
#                 threshold_value_trro_yield = float(row.get("threshold_value_trro_yield", 5))
#                 threshold_value_brro_yield = float(row.get("threshold_value_brro_yield", 5))
#                 threshold_value_lro_yield = float(row.get("threshold_value_lro_yield", 5))
#                 threshold_value_lroh1_yield = float(row.get("threshold_value_lroh1_yield", 5))
#                 threshold_value_crroh1_yield = float(row.get("threshold_value_crroh1_yield", 5))
#                 threshold_value_trroh1_yield = float(row.get("threshold_value_trroh1_yield", 5))
#                 threshold_value_brroh1_yield = float(row.get("threshold_value_brroh1_yield", 5))
#                 threshold_value_lrro_yield = float(row.get("threshold_value_lrro_yield", 5))
#                 threshold_value_sbal_yield = float(row.get("threshold_value_sbal_yield", 5))
#                 threshold_value_couple_yield = float(row.get("threshold_value_couple_yield", 5))
#                 threshold_value_upper_yield = float(row.get("threshold_value_upper_yield", 5))
#                 threshold_value_lower_yield = float(row.get("threshold_value_lower_yield", 5))
#                 threshold_value_overall_uniformity_yield = float(row.get("threshold_value_overall_uniformity_yield", 5))
#                 threshold_value_overall_visual_yield = float(row.get("threshold_value_overall_visual_yield", 5))
#                 threshold_value_overall_warehouse_yield = float(row.get("threshold_value_overall_warehouse_yield", 5))
#                 threshold_value_overall_scrap_yield = float(row.get("threshold_value_overall_scrap_yield", 5))
#                 threshold_value_visual_loss_yield = float(row.get("threshold_value_visual_loss_yield", 5))
#                 threshold_value_uf_loss_yield = float(row.get("threshold_value_uf_loss_yield", 5))
#                 threshold_value_oe_ok_yield = float(row.get("threshold_value_oe_ok_yield", 5))

#                 if model_data.empty:
#                     logging.info(f"Skipping UFM model {model_number} gt_code {gt_code}: No data available.")
#                     output_messages.append(f"Skipping UFM model {model_number} gt_code {gt_code}: No data available.")
#                     continue

#                 # ============== FIX: account for barcodes already banked in the
#                 # incomplete_lots_tracker for this SKU. The old code skipped the
#                 # whole model whenever THIS run's barcodes alone were < lot_size,
#                 # which prevented incomplete lots from being topped up and never
#                 # rolled over into a new incremented lot. We now let processing
#                 # continue when either (a) enough NEW barcodes arrived, or
#                 # (b) new + already-banked barcodes reach the target, or
#                 # (c) there are new barcodes we can bank as/append to an
#                 #     incomplete lot for a later top-up.
#                 total_barcodes_model_processable = len(model_data)

#                 # Sum banked (incomplete) barcodes across all machines of this SKU.
#                 banked_incomplete_barcodes = 0
#                 for _key, _lots in incomplete_lots_dict.items():
#                     if _key[0] == model_number:
#                         for _lot in _lots:
#                             banked_incomplete_barcodes += int(_lot.get("barcode_count", 0) or 0)

#                 combined_available = total_barcodes_model_processable + banked_incomplete_barcodes

#                 # Only skip when there is genuinely nothing to do: no new barcodes
#                 # this run AND nothing banked to continue. Otherwise fall through so
#                 # the per-machine logic can top up incomplete lots and/or bank new
#                 # barcodes for the next run.
#                 if total_barcodes_model_processable == 0 and banked_incomplete_barcodes == 0:
#                     logging.info(f"Skipping UFM model {model_number} gt_code {gt_code}: No barcodes to process.")
#                     output_messages.append(f"Skipping UFM model {model_number} gt_code {gt_code}: No barcodes to process.")
#                     continue

#                 if combined_available < target_lot_size:
#                     # Not enough yet to complete a full lot, but we still process so
#                     # the new barcodes get banked/appended to an incomplete lot
#                     # (they must NOT be dropped).
#                     logging.info(
#                         f"UFM model {model_number} gt_code {gt_code}: partial fill "
#                         f"({total_barcodes_model_processable} new + {banked_incomplete_barcodes} banked "
#                         f"= {combined_available} < {target_lot_size}). Banking barcodes for later top-up."
#                     )
#                     output_messages.append(
#                         f"UFM model {model_number} gt_code {gt_code}: partial fill "
#                         f"({combined_available} < {target_lot_size}). Banking barcodes for later top-up."
#                     )
                
#                 # ==================== VIRGIN/FINAL YIELD PRE-PROCESSING ====================
#                 model_data_full = model_data.copy()
#                 model_data_full["date_of_dt"] = pd.to_datetime(model_data_full["date_and_time"], errors='coerce')

#                 # Virgin Yield: FIRST occurrence of each barcode
#                 virgin_data = model_data_full.sort_values("date_of_dt").drop_duplicates(subset="barcode", keep="first").copy()
#                 virgin_data.sort_values("date_of_dt", inplace=True)

#                 # Final Yield: LAST occurrence of each barcode
#                 final_data = model_data_full.sort_values("date_of_dt").drop_duplicates(subset="barcode", keep="last").copy()
#                 final_data.sort_values("date_of_dt", inplace=True)
                
#                 # ==================== MAIN LOT PROCESSING ====================
#                 # Use VIRGIN data for main lot processing
#                 model_data = virgin_data.copy()

#                 # Sort by UFM date
#                 model_data.sort_values("date_of_ufm", ascending=True, inplace=True)

#                 # ============== FIX: Use a dictionary to track alerts by lot_id to avoid duplicates ==============
#                 alert_map = {}

#                 for ufm_machine_id, ufm_machine_data in model_data.groupby("uf_id"):
#                     try:
#                         machine_no = str(ufm_machine_id)
                        
#                         # ============== FIX: Filter out already processed barcodes ==============
#                         machine_model_key = (model_number, machine_no)
#                         if machine_model_key not in processed_barcodes_per_machine_model:
#                             processed_barcodes_per_machine_model[machine_model_key] = set()
                        
#                         # Filter out barcodes that have already been processed
#                         processed_barcodes = processed_barcodes_per_machine_model[machine_model_key]
#                         ufm_machine_data = ufm_machine_data[~ufm_machine_data["barcode"].isin(processed_barcodes)].copy()
                        
#                         # Also filter out already assigned rows
#                         ufm_machine_data = ufm_machine_data[ufm_machine_data["ufm_machine_model_lot_id"].isna()].copy()
#                         if ufm_machine_data.empty:
#                             logging.info(f"Skipping UFM machine {machine_no} for model {model_number}: No unprocessed data.")
#                             continue
                        
#                         tbm_machine_value = ufm_machine_data["tbm_machine"].iloc[0] if "tbm_machine" in ufm_machine_data.columns and not ufm_machine_data["tbm_machine"].isna().all() else None
#                         output_messages.append(f"DEBUG: Model {model_number}, Machine {machine_no}, tbm_machine: {tbm_machine_value}.")

#                         machine_config_row = ub_machine_df[ub_machine_df["ubno"] == machine_no]
#                         if machine_config_row.empty:
#                             logging.info(f"Skipping UFM machine {machine_no} for model {model_number}: Unknown machine config.")
#                             output_messages.append(f"Skipping UFM machine {machine_no} for model {model_number}: Unknown machine config.")
#                             continue

#                         total_ufm_machine_barcodes = len(ufm_machine_data)
                        
#                         # ============== FIX: Check if there's an incomplete lot for this machine-sku ==============
#                         incomplete_lot_key = f"{machine_model_key}_incomplete"
#                         incomplete_barcodes_count = 0
#                         incomplete_lot_number = None
#                         incomplete_ufm_id = None
#                         incomplete_batch_data = None
                        
#                         # Check if we have an incomplete lot from database
#                         if machine_model_key in incomplete_lots_dict and incomplete_lots_dict[machine_model_key]:
#                             incomplete_lot_data = incomplete_lots_dict[machine_model_key][0]  # Take the first incomplete lot
#                             incomplete_barcodes_count = incomplete_lot_data.get("barcode_count", 0)
#                             incomplete_lot_number = incomplete_lot_data.get("lot_number", 0)
#                             incomplete_ufm_id = incomplete_lot_data.get("ufm_machine_model_lot_id")
#                             incomplete_batch_data = incomplete_lot_data.get("batch_data", [])
#                             logging.info(f"Found incomplete lot from database: {incomplete_lot_data.get('lot_id')} with {incomplete_barcodes_count} barcodes")
                        
#                         # If we have an incomplete lot, we need to fill it first
#                         if incomplete_barcodes_count > 0 and incomplete_lot_number is not None:
#                             # We have an incomplete lot, fill it with available barcodes
#                             barcodes_needed = target_lot_size - incomplete_barcodes_count
                            
#                             if total_ufm_machine_barcodes >= barcodes_needed:
#                                 # We have enough barcodes to complete the lot
#                                 logging.info(f"Completing incomplete lot {incomplete_lot_data.get('lot_id')} with {barcodes_needed} barcodes")
                                
#                                 # Take the first barcodes_needed barcodes to complete the lot
#                                 completion_data = ufm_machine_data.iloc[:barcodes_needed].copy()
#                                 completion_indices = completion_data.index
                                
#                                 # Combine previous barcodes with new ones
#                                 previous_barcodes = incomplete_batch_data if incomplete_batch_data else []
#                                 all_batch_data = pd.DataFrame(previous_barcodes) if previous_barcodes else pd.DataFrame()
#                                 if not all_batch_data.empty:
#                                     all_batch_data = pd.concat([all_batch_data, completion_data], ignore_index=True)
#                                 else:
#                                     all_batch_data = completion_data
                                
#                                 # Update the lot ID for these barcodes
#                                 lot_id = incomplete_lot_data.get("lot_id")
#                                 previous_lot_id = incomplete_lot_data.get("previous_lot_id")
#                                 ufm_previous_machine_model_lot_id = incomplete_lot_data.get("ufm_previous_machine_model_lot_id")
                                
#                                 merge_df.loc[completion_indices, "lot_id"] = lot_id
#                                 merge_df.loc[completion_indices, "previous_lot_id"] = previous_lot_id
#                                 merge_df.loc[completion_indices, "ufm_machine_model_lot_id"] = incomplete_ufm_id
#                                 merge_df.loc[completion_indices, "ufm_previous_machine_model_lot_id"] = ufm_previous_machine_model_lot_id
                                
#                                 # Mark these barcodes as processed
#                                 processed_barcodes_per_machine_model[machine_model_key].update(set(completion_data["barcode"].astype(str)))
                                
#                                 # Process the full lot
#                                 current_model_lot_id = lot_id
#                                 current_ufm_machine_lot_id = incomplete_ufm_id
#                                 current_last_lot_number = incomplete_lot_number
#                                 model_level_previous_lot_id = previous_lot_id
#                                 machine_batch_data = all_batch_data
#                                 machine_batch_indices = completion_indices
                                
#                                 # Remove the incomplete lot from database
#                                 incomplete_lots_to_delete.append(incomplete_lot_data.get("_id"))
                                
#                                 # Process the full lot (generate alerts, averages, etc.)
#                                 # Get previous lot averages
#                                 prev_lot_avg = None
#                                 if ufm_previous_machine_model_lot_id is not None:
#                                     prev_avg_key = (model_number, machine_no, ufm_previous_machine_model_lot_id)
#                                     prev_lot_avg = ufm_machine_lot_avg_map.get(prev_avg_key)
#                                     if prev_lot_avg is None:
#                                         try:
#                                             prev_lot_doc = averages_ufm_collect.find_one(
#                                                 {
#                                                     "sku": model_number,
#                                                     "machine": machine_no,
#                                                     "ufm_machine_model_lot_id": ufm_previous_machine_model_lot_id
#                                                 },
#                                                 {"current_lot_averages": 1}
#                                             )
#                                             prev_lot_avg = prev_lot_doc.get("current_lot_averages", {}) if prev_lot_doc else {}
#                                             if prev_lot_avg:
#                                                 ufm_machine_lot_avg_map[prev_avg_key] = prev_lot_avg
#                                         except Exception as e:
#                                             logging.error(f"Error fetching previous lot averages for {ufm_previous_machine_model_lot_id}: {e}")
#                                             prev_lot_avg = {}
#                                 else:
#                                     prev_lot_avg = {}

#                                 # Calculate lot averages
#                                 lot_averages = {}
#                                 for param in ufm_params:
#                                     if param in machine_batch_data.columns:
#                                         numeric_series = pd.to_numeric(machine_batch_data[param], errors='coerce').dropna()
#                                         if not numeric_series.empty:
#                                             lot_averages[param] = float(numeric_series.mean())
#                                         else:
#                                             lot_averages[param] = None
#                                     else:
#                                         lot_averages[param] = None

#                                 lot_averages.update({
#                                     "rfv_yield": rfv_yield(machine_batch_data),
#                                     "rfh1_yield": rfh1_yield(machine_batch_data),
#                                     "rfh2_yield": rfh2_yield(machine_batch_data),
#                                     "lfv_yield": lfv_yield(machine_batch_data),
#                                     "conicity_yield": conicity_yield(machine_batch_data),
#                                     "plysteer_yield": plysteer_yield(machine_batch_data),
#                                     "center_radial_run_out_yield": center_radial_run_out_yield(machine_batch_data),
#                                     "top_radial_run_out_yield": top_radial_run_out_yield(machine_batch_data),
#                                     "bottom_radial_run_out_yield": bottom_radial_run_out_yield(machine_batch_data),
#                                     "lateral_run_out_yield": lateral_run_out_yield(machine_batch_data),
#                                     "lateral_run_out_harmonic_first_yield": lateral_run_out_harmonic_first_yield(machine_batch_data),
#                                     "center_radial_run_out_harmonic_first_yield": center_radial_run_out_harmonic_first_yield(machine_batch_data),
#                                     "top_radial_run_out_harmonic_first_yield": top_radial_run_out_harmonic_first_yield(machine_batch_data),
#                                     "bottom_radial_run_out_harmonic_first_yield": bottom_radial_run_out_harmonic_first_yield(machine_batch_data),
#                                     "lateral_radial_run_out_yield": lateral_radial_run_out_yield(machine_batch_data),
#                                     "static_yield": static_yield(machine_batch_data),
#                                     "couple_yield": couple_yield(machine_batch_data),
#                                     "upper_yield": upper_yield(machine_batch_data),
#                                     "lower_yield": lower_yield(machine_batch_data),
#                                     "overall_uniformity_yield": overall_uniformity_yield(machine_batch_data),
#                                     "overall_visual_yield": overall_visual_yield(machine_batch_data),
#                                     "overall_warehouse_yield": overall_warehouse_yield(machine_batch_data),
#                                     "overall_scrap_yield": overall_scrap_yield(machine_batch_data),
#                                     "visual_loss_yield": visual_loss_yield(machine_batch_data),
#                                     "uf_loss_yield": uf_loss_yield(machine_batch_data),
#                                     "oe_ok_yield": oe_ok_yield(machine_batch_data)
#                                 })

#                                 # Perform Shift Detection (same as before - keep the existing logic)
#                                 parameter_alerts = []
#                                 machine_lot_overall_shift_occurred = False
#                                 yield_spec_mapping = {
#                                     "rfv_yield": spec_rfppwc_yield,
#                                     "rfh1_yield": spec_rfh1wc_yield,
#                                     "rfh2_yield": spec_rfh2wc_yield,
#                                     "lfv_yield": spec_lfppwc_yield,
#                                     "conicity_yield": spec_cony_yield,
#                                     "plysteer_yield": spec_plys_yield,
#                                     "center_radial_run_out_yield": spec_crro_yield,
#                                     "top_radial_run_out_yield": spec_trro_yield,
#                                     "bottom_radial_run_out_yield": spec_brro_yield,
#                                     "lateral_run_out_yield": spec_lro_yield,
#                                     "lateral_run_out_harmonic_first_yield": spec_lroh1_yield,
#                                     "center_radial_run_out_harmonic_first_yield": spec_crroh1_yield,
#                                     "top_radial_run_out_harmonic_first_yield": spec_trroh1_yield,
#                                     "bottom_radial_run_out_harmonic_first_yield": spec_brroh1_yield,
#                                     "lateral_radial_run_out_yield": spec_lrro_yield,
#                                     "static_yield": spec_sbal_yield,
#                                     "couple_yield": spec_couple_yield,
#                                     "upper_yield": spec_upper_yield,
#                                     "lower_yield": spec_lower_yield,
#                                     "overall_uniformity_yield": spec_overall_uniformity_yield,
#                                     "overall_visual_yield": spec_overall_visual_yield,
#                                     "overall_warehouse_yield": spec_overall_warehouse_yield,
#                                     "overall_scrap_yield": spec_overall_scrap_yield,
#                                     "visual_loss_yield": spec_visual_loss_yield,
#                                     "uf_loss_yield": spec_uf_loss_yield,
#                                     "oe_ok_yield": spec_oe_ok_yield
#                                 }

#                                 # Shift detection logic (same as before)
#                                 if prev_lot_avg:
#                                     for param in ufm_params + yield_params:
#                                         current_val = lot_averages.get(param)
#                                         prev_val = prev_lot_avg.get(param)
#                                         if current_val is None or pd.isna(current_val) or prev_val is None or pd.isna(prev_val):
#                                             alert_dict = {
#                                                 "parameter": param,
#                                                 "current_value": current_val,
#                                                 "previous_value": prev_val,
#                                                 "difference": None,
#                                                 "shift_occurred_thershold": False,
#                                                 "spec_value": None,
#                                                 "shift_occurred_spec": False,
#                                                 "shift_occurred": False
#                                             }
#                                             parameter_alerts.append(alert_dict)
#                                             continue

#                                         shift_occurred_thershold = False
#                                         shift_occurred_spec = False
#                                         spec_val = None

#                                         if param == "cony" and spec_cony_lower is not None and spec_cony_upper is not None:
#                                             shift_occurred_thershold = abs(current_val - prev_val) > threshold_value_cony
#                                             shift_occurred_spec = not (-spec_cony_lower <= current_val <= spec_cony_upper)
#                                             spec_val = spec_cony_upper
#                                         elif param == "plys" and spec_plys_lower is not None and spec_plys_upper is not None:
#                                             shift_occurred_thershold = abs(current_val - prev_val) > threshold_value_plys
#                                             shift_occurred_spec = not (-spec_plys_lower <= current_val <= spec_plys_upper)
#                                             spec_val = spec_plys_upper
#                                         elif param in yield_params:
#                                             spec_yield = yield_spec_mapping.get(param, 0)
#                                             threshold_key = f"threshold_value_{param}"
#                                             threshold_val = globals().get(threshold_key, 5)
#                                             shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_val)
#                                             shift_occurred_spec = current_val < spec_yield
#                                             spec_val = spec_yield

#                                         shift_occurred = shift_occurred_thershold or shift_occurred_spec
#                                         if shift_occurred:
#                                             machine_lot_overall_shift_occurred = True

#                                         alert_dict = {
#                                             "parameter": param,
#                                             "current_value": round(current_val, 3) if current_val is not None else None,
#                                             "previous_value": round(prev_val, 3) if prev_val is not None else None,
#                                             "difference": round(current_val - prev_val, 3) if current_val is not None and prev_val is not None else None,
#                                             "shift_occurred_thershold": bool(shift_occurred_thershold),
#                                             "spec_value": spec_val,
#                                             "spec_difference": round(current_val - spec_val, 3) if current_val is not None and spec_val is not None else None,
#                                             "shift_occurred_spec": bool(shift_occurred_spec),
#                                             "shift_occurred": bool(shift_occurred)
#                                         }
#                                         parameter_alerts.append(alert_dict)
#                                 else:
#                                     for param in ufm_params + yield_params:
#                                         current_val = lot_averages.get(param)
#                                         if current_val is None or pd.isna(current_val):
#                                             alert_dict = {
#                                                 "parameter": param,
#                                                 "current_value": current_val,
#                                                 "previous_value": None,
#                                                 "difference": None,
#                                                 "shift_occurred_thershold": False,
#                                                 "spec_value": None,
#                                                 "shift_occurred_spec": False,
#                                                 "shift_occurred": False
#                                             }
#                                             parameter_alerts.append(alert_dict)
#                                             continue

#                                         shift_occurred_thershold = False
#                                         shift_occurred_spec = False
#                                         spec_val = None

#                                         if param == "cony" and spec_cony_lower is not None and spec_cony_upper is not None:
#                                             shift_occurred_spec = not (spec_cony_lower <= current_val <= spec_cony_upper)
#                                             spec_val = spec_cony_upper
#                                         elif param == "plys" and spec_plys_lower is not None and spec_plys_upper is not None:
#                                             shift_occurred_spec = not (spec_plys_lower <= current_val <= spec_plys_upper)
#                                             spec_val = spec_plys_upper
#                                         elif param in yield_params:
#                                             spec_yield = yield_spec_mapping.get(param, 0)
#                                             shift_occurred_spec = current_val < spec_yield
#                                             spec_val = spec_yield

#                                         shift_occurred = shift_occurred_thershold or shift_occurred_spec
#                                         if shift_occurred:
#                                             machine_lot_overall_shift_occurred = True

#                                         alert_dict = {
#                                             "parameter": param,
#                                             "current_value": round(current_val, 3) if current_val is not None else None,
#                                             "previous_value": None,
#                                             "difference": None,
#                                             "shift_occurred_thershold": bool(shift_occurred_thershold),
#                                             "spec_value": spec_val,
#                                             "spec_difference": round(current_val - spec_val, 3) if current_val is not None and spec_val is not None else None,
#                                             "shift_occurred_spec": bool(shift_occurred_spec),
#                                             "shift_occurred": bool(shift_occurred)
#                                         }
#                                         parameter_alerts.append(alert_dict)

#                                 machine_lot_overall_shift_occurred = bool(machine_lot_overall_shift_occurred)

#                                 # Deviated parameter alerts
#                                 deviated_parameter_alerts = [
#                                     alert for alert in parameter_alerts
#                                     if alert.get("shift_occurred_thershold", False) or alert.get("shift_occurred_spec", False)
#                                 ]

#                                 for alert in deviated_parameter_alerts:
#                                     param = alert.get("parameter", "")
#                                     if param.endswith("_yield"):
#                                         if param in rank_param_mappings:
#                                             rank_counts = {}
#                                             for col in rank_param_mappings[param]:
#                                                 if col in machine_batch_data.columns:
#                                                     value_counts = machine_batch_data[col].value_counts().to_dict()
#                                                     for rank_value, count in value_counts.items():
#                                                         rank_value = int(rank_value) if isinstance(rank_value, (int, float)) else rank_value
#                                                         rank_counts[f"{col}_{rank_value}"] = int(count)
#                                             alert["rank_counts"] = rank_counts

#                                 if deviated_parameter_alerts:
#                                     deviated_doc = {
#                                         "alert_id_ufm_ufm": last_alert_id + 1,
#                                         "sku": model_number,
#                                         "lot_id": current_model_lot_id,
#                                         "machine": machine_no,
#                                         "comments": [],
#                                         "alert_assign_user": [],
#                                         "ufm_machine_model_lot_id": current_ufm_machine_lot_id,
#                                         "ufm_previous_machine_model_lot_id": ufm_previous_machine_model_lot_id,
#                                         "parameter_alerts": deviated_parameter_alerts,
#                                         "timestamp_alert_generated": some_function()
#                                     }
#                                     deviated_triggering_bulk_operations.append(pymongo.InsertOne(deviated_doc))
#                                     logging.info(f"Prepared {len(deviated_parameter_alerts)} derived alerts for machine {machine_no}, lot {current_model_lot_id}")

#                                 # Prepare averages document
#                                 averages_doc_data = {
#                                     "_id": f"avg_{current_ufm_machine_lot_id}",
#                                     "sku": model_number,
#                                     "lot_id": current_model_lot_id,
#                                     "previous_lot_id": model_level_previous_lot_id,
#                                     "machine": machine_no,
#                                     "ufm_machine_model_lot_id": current_ufm_machine_lot_id,
#                                     "ufm_previous_machine_model_lot_id": ufm_previous_machine_model_lot_id,
#                                     "current_lot_averages": {k: round(v, 3) if v is not None and not pd.isna(v) else None for k, v in lot_averages.items()},
#                                     "previous_lot_averages": {k: round(v, 3) if v is not None and not pd.isna(v) else None for k, v in prev_lot_avg.items()} if prev_lot_avg else {},
#                                     "parameter_alerts": parameter_alerts,
#                                     "ufm_shift_occurred": machine_lot_overall_shift_occurred,
#                                     "target_machine_lot_size": target_lot_size,
#                                     "processed_machine_barcodes": target_lot_size,
#                                     "timestamp_alert_generated": some_function()
#                                 }
                                
#                                 # Insert averages
#                                 ufm_averages_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(averages_doc_data)))
#                                 output_messages.append(f"Prepared averages for {current_ufm_machine_lot_id} for insertion into averages_ufm_collect")

#                                 # Update maps for next lot
#                                 current_avg_key = (model_number, machine_no, current_ufm_machine_lot_id)
#                                 ufm_machine_lot_avg_map[current_avg_key] = lot_averages
#                                 ufm_previous_machine_model_lot_ids[machine_model_key] = current_ufm_machine_lot_id

#                                 # Increment alert ID
#                                 last_alert_id += 1
#                                 merge_df.loc[machine_batch_indices, "alert_id_ufm_ufm"] = last_alert_id

#                                 # Build shift data (existing logic)
#                                 machine_batch_data["tbm_shift"] = machine_batch_data["date_of_tbm"].apply(get_shift)
#                                 machine_batch_data = machine_batch_data.sort_values("date_of_tbm").reset_index(drop=True)

#                                 shifts_data_list_tbm = []
#                                 current_shift = None
#                                 current_start = None
#                                 current_barcodes = []

#                                 for idx, row in machine_batch_data.iterrows():
#                                     this_shift = row["tbm_shift"]
#                                     this_dt = row["date_of_tbm"]
#                                     this_bc = row.get("barcode")

#                                     if this_shift != current_shift or current_shift is None:
#                                         if current_shift is not None:
#                                             shifts_data_list_tbm.append({
#                                                 "shift": current_shift,
#                                                 "barcode_count": len(current_barcodes),
#                                                 "machine_shift_tbm_start_time": current_start,
#                                                 "machine_shift_tbm_end_time": machine_batch_data.at[idx-1, "date_of_tbm"],
#                                                 "barcodes": current_barcodes[:]
#                                             })
#                                         current_shift = this_shift
#                                         current_start = this_dt
#                                         current_barcodes = [this_bc] if this_bc is not None else []
#                                     else:
#                                         current_barcodes.append(this_bc if this_bc is not None else None)

#                                 if current_shift is not None:
#                                     shifts_data_list_tbm.append({
#                                         "shift": current_shift,
#                                         "barcode_count": len(current_barcodes),
#                                         "machine_shift_tbm_start_time": current_start,
#                                         "machine_shift_tbm_end_time": machine_batch_data["date_of_tbm"].iloc[-1],
#                                         "barcodes": current_barcodes
#                                     })

#                                 shift_groups = machine_batch_data.groupby('shift')
#                                 shifts_data_list = []
#                                 for shift_name, shift_data in shift_groups:
#                                     if not shift_data.empty and 'date_of_ufm' in shift_data.columns:
#                                         shift_data_sorted = shift_data.sort_values('date_of_ufm')
#                                         shifts_data_list.append({
#                                             "shifts": shift_name,
#                                             "barcode_count": len(shift_data_sorted["barcode"]) if "barcode" in shift_data_sorted.columns else 0,
#                                             "machine_shift_ufm_start_time": shift_data_sorted["date_of_ufm"].iloc[0],
#                                             "machine_shift_ufm_end_time": shift_data_sorted["date_of_ufm"].iloc[-1],
#                                             "barcodes": shift_data_sorted["barcode"].tolist() if "barcode" in shift_data_sorted.columns else []
#                                         })

#                                 machine_alert_detail = {
#                                     "machine": machine_no,
#                                     "tbm_machine": tbm_machine_value,
#                                     "ufm_shift_occured": machine_lot_overall_shift_occurred,
#                                     "shifts_data": shifts_data_list,
#                                     "shifts_data_tbm": shifts_data_list_tbm,
#                                     "overall_uniformity_yield": overall_uniformity_yield(machine_batch_data),
#                                     "overall_warehouse_yield": overall_warehouse_yield(machine_batch_data),
#                                     "overall_visual_yield": overall_visual_yield(machine_batch_data),
#                                     "overall_scrap_yield": overall_scrap_yield(machine_batch_data),
#                                     "visual_loss_yield": visual_loss_yield(machine_batch_data),
#                                     "uf_loss_yield": uf_loss_yield(machine_batch_data),
#                                     "oe_ok_yield": oe_ok_yield(machine_batch_data),
#                                     "rfv_yield": rfv_yield(machine_batch_data),
#                                     "rfh1_yield": rfh1_yield(machine_batch_data),
#                                     "rfh2_yield": rfh2_yield(machine_batch_data),
#                                     "lfv_yield": lfv_yield(machine_batch_data),
#                                     "plysteer_yield": plysteer_yield(machine_batch_data),
#                                     "sizecode": sizecode, 
#                                     "gt_code": str(gt_code),
#                                     "target_machine_lot_size": target_lot_size,
#                                     "processed_machine_barcodes": target_lot_size,
#                                     "machine_lot_start_ufm_time": machine_batch_data["date_of_ufm"].min() if not machine_batch_data["date_of_ufm"].empty else None,
#                                     "machine_lot_end_ufm_time": machine_batch_data["date_of_ufm"].max() if not machine_batch_data["date_of_ufm"].empty else None,
#                                     "machine_lot_start_build_time": machine_batch_data["date_of_tbm"].min() if not machine_batch_data["date_of_tbm"].empty else None,
#                                     "machine_lot_end_build_time": machine_batch_data["date_of_tbm"].max() if not machine_batch_data["date_of_tbm"].empty else None,
#                                     "parameter_alerts": parameter_alerts,
#                                     "lot_averages": {k: round(v, 3) if v is not None and not pd.isna(v) else None for k, v in lot_averages.items()},
#                                     "previous_lot_averages": {k: round(v, 3) if v is not None and not pd.isna(v) else None for k, v in prev_lot_avg.items()} if prev_lot_avg else {},
#                                     "first_barcode_machine": machine_batch_data.iloc[0]["barcode"] if not machine_batch_data.empty and "barcode" in machine_batch_data.columns else None,
#                                     "last_barcode_machine": machine_batch_data.iloc[-1]["barcode"] if not machine_batch_data.empty and "barcode" in machine_batch_data.columns else None,
#                                     "ufm_machine_model_lot_id": current_ufm_machine_lot_id,
#                                     "ufm_previous_machine_model_lot_id": ufm_previous_machine_model_lot_id,
#                                     "barcodes": machine_batch_data["barcode"].tolist() if "barcode" in machine_batch_data.columns else [],
#                                     "cur_ids": (
#                                         [
#                                             {"cur_id": cur_id, "barcode": int(count)}
#                                             for cur_id, count in machine_batch_data.groupby("cur_id")["barcode"].count().items()
#                                         ]
#                                         if "cur_id" in machine_batch_data.columns and "barcode" in machine_batch_data.columns
#                                         else []
#                                     ),
#                                     "tbm_ids": (
#                                         [
#                                             {"tbm_id": ptb_id, "barcode": int(count)}
#                                             for ptb_id, count in machine_batch_data.groupby("ptb_id")["barcode"].count().items()
#                                         ]
#                                         if "ptb_id" in machine_batch_data.columns and "barcode" in machine_batch_data.columns
#                                         else []
#                                     )
#                                 }

#                                 # Create alert documents
#                                 alert_doc_main = {
#                                     "_id": f"main_alert_{last_alert_id}",
#                                     "alert_id_ufm_ufm": last_alert_id,
#                                     "sku": model_number,
#                                     "lot_id": current_model_lot_id,
#                                     "alert_flag": False,
#                                     "dashboard_flag": False,
#                                     "alert_status_check": "o",
#                                     "alert_status_timestamp": None,
#                                     "alert_status_comment": [],
#                                     "alert_assign_user": [],
#                                     "machine_alerts": [machine_alert_detail],
#                                     "first_barcode_model": machine_alert_detail["first_barcode_machine"],
#                                     "last_barcode_model": machine_alert_detail["last_barcode_machine"],
#                                     "timestamp_alert_generated": some_function()
#                                 }

#                                 # Store in alert_map with lot_id as key
#                                 if current_model_lot_id not in alert_map:
#                                     alert_map[current_model_lot_id] = {
#                                         "alert_id_ufm_ufm": last_alert_id,
#                                         "sku": model_number,
#                                         "lot_id": current_model_lot_id,
#                                         "machine_alerts": []
#                                     }
                                
#                                 alert_map[current_model_lot_id]["machine_alerts"].append(machine_alert_detail.copy())

#                                 trigger_doc = {
#                                     "_id": f"trigger_alert_{last_alert_id}",
#                                     "alert_id_ufm_ufm": last_alert_id,
#                                     "sku": model_number,
#                                     "lot_id": current_model_lot_id,
#                                     "type": "ufm_trigger",
#                                     "timestamp_alert_generated": some_function()
#                                 }

#                                 # Insert main alerts
#                                 if not alert_collect.find_one({"alert_id_ufm_ufm": last_alert_id}):
#                                     try:
#                                         alert_collect.insert_one(convert_numpy_types(alert_doc_main))
#                                         logging.info(f"Inserted alert_doc_main into alert_table for alert_id {last_alert_id}")
#                                         output_messages.append(f"Inserted into alert_table: alert_id {last_alert_id}")
#                                     except Exception as e:
#                                         logging.error(f"Failed to insert into alert_table: {e}")

#                                 if not alert_triggering_collect_dashboard_ufm.find_one({"alert_id_ufm_ufm": last_alert_id}):
#                                     try:
#                                         alert_triggering_collect_dashboard_ufm.insert_one(convert_numpy_types(alert_doc_main))
#                                         logging.info(f"Inserted alert_doc_main into alert_db_dashboard for alert_id {last_alert_id}")
#                                         output_messages.append(f"Inserted into alert_db_dashboard (main): alert_id {last_alert_id}")
#                                     except Exception as e:
#                                         logging.error(f"Failed to insert main alert into alert_db_dashboard: {e}")

#                                 if not alert_triggering_collect_dashboard_ufm.find_one({"_id": f"trigger_alert_{last_alert_id}"}):
#                                     try:
#                                         alert_triggering_collect_dashboard_ufm.insert_one(convert_numpy_types(trigger_doc))
#                                         logging.info(f"Inserted trigger_doc into alert_db_dashboard for alert_id {last_alert_id}")
#                                         output_messages.append(f"Inserted into alert_db_dashboard (trigger): alert_id {last_alert_id}")
#                                     except Exception as e:
#                                         logging.error(f"Failed to insert trigger_doc into alert_db_dashboard: {e}")

#                                 if machine_lot_overall_shift_occurred:
#                                     if not alert_ufm_collect.find_one({"alert_id_ufm_ufm": last_alert_id}):
#                                         ufm_alert_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(alert_doc_main)))
#                                         ufm_triggering_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(trigger_doc)))
#                                         ufm_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(trigger_doc)))
#                                         logging.info(f"Shift detected - added to bulk ops for alert_id {last_alert_id}")
#                                         output_messages.append(f"Shift alert queued for bulk: alert_id {last_alert_id}")

#                                 # Insert Lot document into MongoDB (NOW IT'S COMPLETE!)
#                                 lot_doc_data = {
#                                     "_id": f"lot_{current_ufm_machine_lot_id}",
#                                     "sku": model_number,
#                                     "lot_id": current_model_lot_id,
#                                     "previous_lot_id": model_level_previous_lot_id,
#                                     "machine": machine_no,
#                                     "ufm_machine_model_lot_id": current_ufm_machine_lot_id,
#                                     "ufm_previous_machine_model_lot_id": ufm_previous_machine_model_lot_id,
#                                     "target_machine_lot_size": target_lot_size,
#                                     "processed_machine_barcodes": target_lot_size,
#                                     "timestamp_lot_generated": some_function(),
#                                     "first_barcode": machine_batch_data['barcode'].iloc[0] if not machine_batch_data.empty else None,
#                                     "last_barcode": machine_batch_data['barcode'].iloc[-1] if not machine_batch_data.empty else None,
#                                 }
                                
#                                 ufm_lot_bulk_operations.append(pymongo.UpdateOne(
#                                     {"ufm_machine_model_lot_id": current_ufm_machine_lot_id},
#                                     {"$setOnInsert": convert_numpy_types(lot_doc_data)},
#                                     upsert=True
#                                 ))
#                                 output_messages.append(f"Prepared complete lot_id {current_model_lot_id} for insertion into lot_ufm_collect")

#                                 # Remove the completed barcodes from ufm_machine_data
#                                 ufm_machine_data = ufm_machine_data.iloc[barcodes_needed:].copy()
#                                 total_ufm_machine_barcodes = len(ufm_machine_data)
#                                 machine_barcode_counter = 0
                                
#                                 # Update last_lot_dict for future lots
#                                 last_lot_dict[machine_model_key] = {
#                                     "max_lot_number": current_last_lot_number,
#                                     "last_lot_id": current_model_lot_id,
#                                     "last_ufm_machine_model_lot_id": current_ufm_machine_lot_id
#                                 }
                                
#                                 # Continue to process remaining barcodes as new lots
#                                 if total_ufm_machine_barcodes == 0:
#                                     break
#                                 # If there are remaining barcodes, continue to next lot
#                                 continue
                                
#                             else:
#                                 # Not enough barcodes to complete the lot, add them to the incomplete lot
#                                 logging.info(f"Adding {total_ufm_machine_barcodes} more barcodes to incomplete lot {incomplete_lot_data.get('lot_id')}")
                                
#                                 # Add all barcodes to the incomplete lot
#                                 completion_data = ufm_machine_data.copy()
#                                 completion_indices = completion_data.index
                                
#                                 # Update the lot ID for these barcodes
#                                 merge_df.loc[completion_indices, "lot_id"] = incomplete_lot_data.get("lot_id")
#                                 merge_df.loc[completion_indices, "previous_lot_id"] = incomplete_lot_data.get("previous_lot_id")
#                                 merge_df.loc[completion_indices, "ufm_machine_model_lot_id"] = incomplete_ufm_id
#                                 merge_df.loc[completion_indices, "ufm_previous_machine_model_lot_id"] = incomplete_lot_data.get("ufm_previous_machine_model_lot_id")
                                
#                                 # Mark these barcodes as processed
#                                 processed_barcodes_per_machine_model[machine_model_key].update(set(completion_data["barcode"].astype(str)))
                                
#                                 # Update the incomplete lot data
#                                 previous_barcodes = incomplete_batch_data if incomplete_batch_data else []
#                                 all_batch_data = pd.DataFrame(previous_barcodes) if previous_barcodes else pd.DataFrame()
#                                 if not all_batch_data.empty:
#                                     all_batch_data = pd.concat([all_batch_data, completion_data], ignore_index=True)
#                                 else:
#                                     all_batch_data = completion_data
                                
#                                 # Update incomplete lot in database
#                                 incomplete_lots_to_update.append({
#                                     "_id": incomplete_lot_data.get("_id"),
#                                     "sku": model_number,
#                                     "machine": machine_no,
#                                     "lot_id": incomplete_lot_data.get("lot_id"),
#                                     "lot_number": incomplete_lot_number,
#                                     "previous_lot_id": incomplete_lot_data.get("previous_lot_id"),
#                                     "ufm_machine_model_lot_id": incomplete_ufm_id,
#                                     "ufm_previous_machine_model_lot_id": incomplete_lot_data.get("ufm_previous_machine_model_lot_id"),
#                                     "barcode_count": incomplete_barcodes_count + total_ufm_machine_barcodes,
#                                     "batch_data": all_batch_data.to_dict('records')
#                                 })
                                
#                                 logging.info(f"Incomplete lot now has {incomplete_barcodes_count + total_ufm_machine_barcodes} barcodes out of {target_lot_size}")
                                
#                                 # No more barcodes to process for this machine-sku
#                                 break

#                         # ============== FIX: Get the current max lot number from last_lot_dict ==============
#                         if machine_model_key not in last_lot_dict:
#                             last_lot_dict[machine_model_key] = {
#                                 "max_lot_number": 0,
#                                 "last_lot_id": None,
#                                 "last_ufm_machine_model_lot_id": None
#                             }
#                             logging.info(f"No existing lots found for Machine-SKU: {machine_model_key}. Starting from 0.")
#                         else:
#                             logging.info(f"Found existing lots for Machine-SKU: {machine_model_key}. Max lot number: {last_lot_dict[machine_model_key]['max_lot_number']}")

#                         # If we already processed all barcodes (either completed a lot or added to incomplete), continue
#                         if total_ufm_machine_barcodes == 0:
#                             continue

#                         # Process remaining barcodes as new lots
#                         machine_barcode_counter = 0

#                         while machine_barcode_counter < total_ufm_machine_barcodes:
#                             if (total_ufm_machine_barcodes - machine_barcode_counter) < target_lot_size:
#                                 # Not enough barcodes for a full lot - store as incomplete
#                                 remaining_barcodes = ufm_machine_data.iloc[machine_barcode_counter:].copy()
#                                 current_last_lot_number = last_lot_dict[machine_model_key]["max_lot_number"] + 1
#                                 current_model_lot_id = f"UFM_{machine_no}_{model_number}_{current_last_lot_number}"
#                                 current_ufm_machine_lot_id = f"{machine_no}_{model_number}_UFM_{machine_no}_{model_number}_{current_last_lot_number}_{machine_barcode_counter + len(remaining_barcodes)}"
                                
#                                 if current_last_lot_number == 1:
#                                     ufm_previous_machine_model_lot_id = None
#                                     model_level_previous_lot_id = None
#                                 else:
#                                     ufm_previous_machine_model_lot_id = last_lot_dict[machine_model_key].get("last_ufm_machine_model_lot_id")
#                                     model_level_previous_lot_id = f"UFM_{machine_no}_{model_number}_{current_last_lot_number - 1}"
                                
#                                 # Assign IDs to these barcodes (they will remain incomplete)
#                                 remaining_indices = remaining_barcodes.index
#                                 merge_df.loc[remaining_indices, "lot_id"] = current_model_lot_id
#                                 merge_df.loc[remaining_indices, "previous_lot_id"] = model_level_previous_lot_id
#                                 merge_df.loc[remaining_indices, "ufm_machine_model_lot_id"] = current_ufm_machine_lot_id
#                                 merge_df.loc[remaining_indices, "ufm_previous_machine_model_lot_id"] = ufm_previous_machine_model_lot_id
                                
#                                 # Mark as processed
#                                 processed_barcodes_per_machine_model[machine_model_key].update(set(remaining_barcodes["barcode"].astype(str)))
                                
#                                 # Store incomplete lot in database (to be continued in next run)
#                                 incomplete_lot_doc = {
#                                     "sku": model_number,
#                                     "machine": machine_no,
#                                     "lot_id": current_model_lot_id,
#                                     "lot_number": current_last_lot_number,
#                                     "previous_lot_id": model_level_previous_lot_id,
#                                     "ufm_machine_model_lot_id": current_ufm_machine_lot_id,
#                                     "ufm_previous_machine_model_lot_id": ufm_previous_machine_model_lot_id,
#                                     "barcode_count": len(remaining_barcodes),
#                                     "batch_data": remaining_barcodes.to_dict('records'),
#                                     "target_lot_size": target_lot_size,
#                                     "timestamp_created": some_function()
#                                 }
                                
#                                 # Check if this incomplete lot already exists
#                                 existing_incomplete = incomplete_lots_collect.find_one({
#                                     "sku": model_number,
#                                     "machine": machine_no,
#                                     "lot_id": current_model_lot_id
#                                 })
                                
#                                 if existing_incomplete:
#                                     # Update existing
#                                     incomplete_lots_to_update.append({
#                                         "_id": existing_incomplete["_id"],
#                                         **incomplete_lot_doc
#                                     })
#                                 else:
#                                     # Insert new
#                                     incomplete_lots_collect.insert_one(convert_numpy_types(incomplete_lot_doc))
                                
#                                 logging.info(f"Stored incomplete lot {current_model_lot_id} with {len(remaining_barcodes)} barcodes for {machine_model_key}")
#                                 break

#                             # Process full lot (same as before)
#                             current_last_lot_number = last_lot_dict[machine_model_key]["max_lot_number"] + 1
#                             current_model_lot_id = f"UFM_{machine_no}_{model_number}_{current_last_lot_number}"
#                             current_ufm_machine_lot_id = f"{machine_no}_{model_number}_UFM_{machine_no}_{model_number}_{current_last_lot_number}_{machine_barcode_counter + target_lot_size}"
                            
#                             if current_last_lot_number == 1:
#                                 ufm_previous_machine_model_lot_id = None
#                                 model_level_previous_lot_id = None
#                             else:
#                                 ufm_previous_machine_model_lot_id = last_lot_dict[machine_model_key].get("last_ufm_machine_model_lot_id")
#                                 if ufm_previous_machine_model_lot_id is None:
#                                     prev_lot_number = current_last_lot_number - 1
#                                     prev_lot_id = f"UFM_{machine_no}_{model_number}_{prev_lot_number}"
#                                     try:
#                                         prev_lot = averages_ufm_collect.find_one(
#                                             {
#                                                 "sku": model_number,
#                                                 "machine": machine_no,
#                                                 "lot_id": prev_lot_id
#                                             },
#                                             {"ufm_machine_model_lot_id": 1}
#                                         )
#                                         if prev_lot:
#                                             ufm_previous_machine_model_lot_id = prev_lot["ufm_machine_model_lot_id"]
#                                     except Exception as e:
#                                         logging.error(f"Error fetching previous ufm_machine_model_lot_id for lot {prev_lot_number}: {e}")
#                                         ufm_previous_machine_model_lot_id = None
#                                 model_level_previous_lot_id = f"UFM_{machine_no}_{model_number}_{current_last_lot_number - 1}"

#                             # Update last_lot_dict
#                             last_lot_dict[machine_model_key] = {
#                                 "max_lot_number": current_last_lot_number,
#                                 "last_lot_id": current_model_lot_id,
#                                 "last_ufm_machine_model_lot_id": current_ufm_machine_lot_id
#                             }
                            
#                             logging.info(f"Generated new lot: {current_model_lot_id} for Machine-SKU: {machine_model_key}")

#                             # Validate IDs
#                             if not re.match(r"^UFM_.+_\d+$", current_model_lot_id):
#                                 logging.error(f"Invalid lot_id format: {current_model_lot_id}")
#                                 continue
#                             if not re.match(r"^.+_UFM_.+_\d+_\d+$", current_ufm_machine_lot_id):
#                                 logging.error(f"Invalid ufm_machine_model_lot_id format: {current_ufm_machine_lot_id}")
#                                 continue

#                             sub_batch_start_index = machine_barcode_counter
#                             sub_batch_end_index = machine_barcode_counter + target_lot_size
#                             machine_batch_data = ufm_machine_data.iloc[sub_batch_start_index:sub_batch_end_index].copy()
#                             machine_batch_indices = machine_batch_data.index

#                             # Mark barcodes as processed
#                             processed_barcodes_per_machine_model[machine_model_key].update(set(machine_batch_data["barcode"].astype(str)))

#                             # Assign values to merge_df
#                             merge_df.loc[machine_batch_indices, "lot_id"] = current_model_lot_id
#                             merge_df.loc[machine_batch_indices, "previous_lot_id"] = model_level_previous_lot_id
#                             merge_df.loc[machine_batch_indices, "ufm_machine_model_lot_id"] = current_ufm_machine_lot_id
#                             merge_df.loc[machine_batch_indices, "ufm_previous_machine_model_lot_id"] = ufm_previous_machine_model_lot_id

#                             # Fetch previous lot averages
#                             prev_lot_avg = None
#                             if ufm_previous_machine_model_lot_id is not None:
#                                 prev_avg_key = (model_number, machine_no, ufm_previous_machine_model_lot_id)
#                                 prev_lot_avg = ufm_machine_lot_avg_map.get(prev_avg_key)
#                                 if prev_lot_avg is None:
#                                     try:
#                                         prev_lot_doc = averages_ufm_collect.find_one(
#                                             {
#                                                 "sku": model_number,
#                                                 "machine": machine_no,
#                                                 "ufm_machine_model_lot_id": ufm_previous_machine_model_lot_id
#                                             },
#                                             {"current_lot_averages": 1}
#                                         )
#                                         prev_lot_avg = prev_lot_doc.get("current_lot_averages", {}) if prev_lot_doc else {}
#                                         if prev_lot_avg:
#                                             ufm_machine_lot_avg_map[prev_avg_key] = prev_lot_avg
#                                     except Exception as e:
#                                         logging.error(f"Error fetching previous lot averages for {ufm_previous_machine_model_lot_id}: {e}")
#                                         prev_lot_avg = {}
#                             else:
#                                 prev_lot_avg = {}
#                                 logging.info(f"First lot for model {model_number}, machine {machine_no}. Setting previous_lot_averages to empty.")

#                             logging.info(f"Lot {current_model_lot_id} - Previous UFM ID: {ufm_previous_machine_model_lot_id}")

#                             # Calculate lot averages
#                             lot_averages = {}
#                             for param in ufm_params:
#                                 if param in machine_batch_data.columns:
#                                     numeric_series = pd.to_numeric(machine_batch_data[param], errors='coerce').dropna()
#                                     if not numeric_series.empty:
#                                         lot_averages[param] = float(numeric_series.mean())
#                                     else:
#                                         lot_averages[param] = None
#                                 else:
#                                     lot_averages[param] = None

#                             lot_averages.update({
#                                 "rfv_yield": rfv_yield(machine_batch_data),
#                                 "rfh1_yield": rfh1_yield(machine_batch_data),
#                                 "rfh2_yield": rfh2_yield(machine_batch_data),
#                                 "lfv_yield": lfv_yield(machine_batch_data),
#                                 "conicity_yield": conicity_yield(machine_batch_data),
#                                 "plysteer_yield": plysteer_yield(machine_batch_data),
#                                 "center_radial_run_out_yield": center_radial_run_out_yield(machine_batch_data),
#                                 "top_radial_run_out_yield": top_radial_run_out_yield(machine_batch_data),
#                                 "bottom_radial_run_out_yield": bottom_radial_run_out_yield(machine_batch_data),
#                                 "lateral_run_out_yield": lateral_run_out_yield(machine_batch_data),
#                                 "lateral_run_out_harmonic_first_yield": lateral_run_out_harmonic_first_yield(machine_batch_data),
#                                 "center_radial_run_out_harmonic_first_yield": center_radial_run_out_harmonic_first_yield(machine_batch_data),
#                                 "top_radial_run_out_harmonic_first_yield": top_radial_run_out_harmonic_first_yield(machine_batch_data),
#                                 "bottom_radial_run_out_harmonic_first_yield": bottom_radial_run_out_harmonic_first_yield(machine_batch_data),
#                                 "lateral_radial_run_out_yield": lateral_radial_run_out_yield(machine_batch_data),
#                                 "static_yield": static_yield(machine_batch_data),
#                                 "couple_yield": couple_yield(machine_batch_data),
#                                 "upper_yield": upper_yield(machine_batch_data),
#                                 "lower_yield": lower_yield(machine_batch_data),
#                                 "overall_uniformity_yield": overall_uniformity_yield(machine_batch_data),
#                                 "overall_visual_yield": overall_visual_yield(machine_batch_data),
#                                 "overall_warehouse_yield": overall_warehouse_yield(machine_batch_data),
#                                 "overall_scrap_yield": overall_scrap_yield(machine_batch_data),
#                                 "visual_loss_yield": visual_loss_yield(machine_batch_data),
#                                 "uf_loss_yield": uf_loss_yield(machine_batch_data),
#                                 "oe_ok_yield": oe_ok_yield(machine_batch_data)
#                             })

#                             # Perform Shift Detection (same logic as before - keeping it concise)
#                             parameter_alerts = []
#                             machine_lot_overall_shift_occurred = False
#                             yield_spec_mapping = {
#                                 "rfv_yield": spec_rfppwc_yield,
#                                 "rfh1_yield": spec_rfh1wc_yield,
#                                 "rfh2_yield": spec_rfh2wc_yield,
#                                 "lfv_yield": spec_lfppwc_yield,
#                                 "conicity_yield": spec_cony_yield,
#                                 "plysteer_yield": spec_plys_yield,
#                                 "center_radial_run_out_yield": spec_crro_yield,
#                                 "top_radial_run_out_yield": spec_trro_yield,
#                                 "bottom_radial_run_out_yield": spec_brro_yield,
#                                 "lateral_run_out_yield": spec_lro_yield,
#                                 "lateral_run_out_harmonic_first_yield": spec_lroh1_yield,
#                                 "center_radial_run_out_harmonic_first_yield": spec_crroh1_yield,
#                                 "top_radial_run_out_harmonic_first_yield": spec_trroh1_yield,
#                                 "bottom_radial_run_out_harmonic_first_yield": spec_brroh1_yield,
#                                 "lateral_radial_run_out_yield": spec_lrro_yield,
#                                 "static_yield": spec_sbal_yield,
#                                 "couple_yield": spec_couple_yield,
#                                 "upper_yield": spec_upper_yield,
#                                 "lower_yield": spec_lower_yield,
#                                 "overall_uniformity_yield": spec_overall_uniformity_yield,
#                                 "overall_visual_yield": spec_overall_visual_yield,
#                                 "overall_warehouse_yield": spec_overall_warehouse_yield,
#                                 "overall_scrap_yield": spec_overall_scrap_yield,
#                                 "visual_loss_yield": spec_visual_loss_yield,
#                                 "uf_loss_yield": spec_uf_loss_yield,
#                                 "oe_ok_yield": spec_oe_ok_yield
#                             }

#                             # Shift detection (same as before)
#                             if prev_lot_avg:
#                                 for param in ufm_params + yield_params:
#                                     current_val = lot_averages.get(param)
#                                     prev_val = prev_lot_avg.get(param)
#                                     if current_val is None or pd.isna(current_val) or prev_val is None or pd.isna(prev_val):
#                                         alert_dict = {
#                                             "parameter": param,
#                                             "current_value": current_val,
#                                             "previous_value": prev_val,
#                                             "difference": None,
#                                             "shift_occurred_thershold": False,
#                                             "spec_value": None,
#                                             "shift_occurred_spec": False,
#                                             "shift_occurred": False
#                                         }
#                                         parameter_alerts.append(alert_dict)
#                                         continue

#                                     shift_occurred_thershold = False
#                                     shift_occurred_spec = False
#                                     spec_val = None

#                                     if param == "cony" and spec_cony_lower is not None and spec_cony_upper is not None:
#                                         shift_occurred_thershold = abs(current_val - prev_val) > threshold_value_cony
#                                         shift_occurred_spec = not (-spec_cony_lower <= current_val <= spec_cony_upper)
#                                         spec_val = spec_cony_upper
#                                     elif param == "plys" and spec_plys_lower is not None and spec_plys_upper is not None:
#                                         shift_occurred_thershold = abs(current_val - prev_val) > threshold_value_plys
#                                         shift_occurred_spec = not (-spec_plys_lower <= current_val <= spec_plys_upper)
#                                         spec_val = spec_plys_upper
#                                     elif param in yield_params:
#                                         spec_yield = yield_spec_mapping.get(param, 0)
#                                         threshold_key = f"threshold_value_{param}"
#                                         threshold_val = globals().get(threshold_key, 5)
#                                         shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_val)
#                                         shift_occurred_spec = current_val < spec_yield
#                                         spec_val = spec_yield

#                                     shift_occurred = shift_occurred_thershold or shift_occurred_spec
#                                     if shift_occurred:
#                                         machine_lot_overall_shift_occurred = True

#                                     alert_dict = {
#                                         "parameter": param,
#                                         "current_value": round(current_val, 3) if current_val is not None else None,
#                                         "previous_value": round(prev_val, 3) if prev_val is not None else None,
#                                         "difference": round(current_val - prev_val, 3) if current_val is not None and prev_val is not None else None,
#                                         "shift_occurred_thershold": bool(shift_occurred_thershold),
#                                         "spec_value": spec_val,
#                                         "spec_difference": round(current_val - spec_val, 3) if current_val is not None and spec_val is not None else None,
#                                         "shift_occurred_spec": bool(shift_occurred_spec),
#                                         "shift_occurred": bool(shift_occurred)
#                                     }
#                                     parameter_alerts.append(alert_dict)
#                             else:
#                                 for param in ufm_params + yield_params:
#                                     current_val = lot_averages.get(param)
#                                     if current_val is None or pd.isna(current_val):
#                                         alert_dict = {
#                                             "parameter": param,
#                                             "current_value": current_val,
#                                             "previous_value": None,
#                                             "difference": None,
#                                             "shift_occurred_thershold": False,
#                                             "spec_value": None,
#                                             "shift_occurred_spec": False,
#                                             "shift_occurred": False
#                                         }
#                                         parameter_alerts.append(alert_dict)
#                                         continue

#                                     shift_occurred_thershold = False
#                                     shift_occurred_spec = False
#                                     spec_val = None

#                                     if param == "cony" and spec_cony_lower is not None and spec_cony_upper is not None:
#                                         shift_occurred_spec = not (spec_cony_lower <= current_val <= spec_cony_upper)
#                                         spec_val = spec_cony_upper
#                                     elif param == "plys" and spec_plys_lower is not None and spec_plys_upper is not None:
#                                         shift_occurred_spec = not (spec_plys_lower <= current_val <= spec_plys_upper)
#                                         spec_val = spec_plys_upper
#                                     elif param in yield_params:
#                                         spec_yield = yield_spec_mapping.get(param, 0)
#                                         shift_occurred_spec = current_val < spec_yield
#                                         spec_val = spec_yield

#                                     shift_occurred = shift_occurred_thershold or shift_occurred_spec
#                                     if shift_occurred:
#                                         machine_lot_overall_shift_occurred = True

#                                     alert_dict = {
#                                         "parameter": param,
#                                         "current_value": round(current_val, 3) if current_val is not None else None,
#                                         "previous_value": None,
#                                         "difference": None,
#                                         "shift_occurred_thershold": bool(shift_occurred_thershold),
#                                         "spec_value": spec_val,
#                                         "spec_difference": round(current_val - spec_val, 3) if current_val is not None and spec_val is not None else None,
#                                         "shift_occurred_spec": bool(shift_occurred_spec),
#                                         "shift_occurred": bool(shift_occurred)
#                                     }
#                                     parameter_alerts.append(alert_dict)

#                             machine_lot_overall_shift_occurred = bool(machine_lot_overall_shift_occurred)

#                             # Deviated parameter alerts
#                             deviated_parameter_alerts = [
#                                 alert for alert in parameter_alerts
#                                 if alert.get("shift_occurred_thershold", False) or alert.get("shift_occurred_spec", False)
#                             ]

#                             for alert in deviated_parameter_alerts:
#                                 param = alert.get("parameter", "")
#                                 if param.endswith("_yield"):
#                                     if param in rank_param_mappings:
#                                         rank_counts = {}
#                                         for col in rank_param_mappings[param]:
#                                             if col in machine_batch_data.columns:
#                                                 value_counts = machine_batch_data[col].value_counts().to_dict()
#                                                 for rank_value, count in value_counts.items():
#                                                     rank_value = int(rank_value) if isinstance(rank_value, (int, float)) else rank_value
#                                                     rank_counts[f"{col}_{rank_value}"] = int(count)
#                                         alert["rank_counts"] = rank_counts

#                             if deviated_parameter_alerts:
#                                 deviated_doc = {
#                                     "alert_id_ufm_ufm": last_alert_id + 1,
#                                     "sku": model_number,
#                                     "lot_id": current_model_lot_id,
#                                     "machine": machine_no,
#                                     "comments": [],
#                                     "alert_assign_user": [],
#                                     "ufm_machine_model_lot_id": current_ufm_machine_lot_id,
#                                     "ufm_previous_machine_model_lot_id": ufm_previous_machine_model_lot_id,
#                                     "parameter_alerts": deviated_parameter_alerts,
#                                     "timestamp_alert_generated": some_function()
#                                 }
#                                 deviated_triggering_bulk_operations.append(pymongo.InsertOne(deviated_doc))
#                                 logging.info(f"Prepared {len(deviated_parameter_alerts)} derived alerts for machine {machine_no}, lot {current_model_lot_id}")

#                             # Prepare averages document
#                             averages_doc_data = {
#                                 "_id": f"avg_{current_ufm_machine_lot_id}",
#                                 "sku": model_number,
#                                 "lot_id": current_model_lot_id,
#                                 "previous_lot_id": model_level_previous_lot_id,
#                                 "machine": machine_no,
#                                 "ufm_machine_model_lot_id": current_ufm_machine_lot_id,
#                                 "ufm_previous_machine_model_lot_id": ufm_previous_machine_model_lot_id,
#                                 "current_lot_averages": {k: round(v, 3) if v is not None and not pd.isna(v) else None for k, v in lot_averages.items()},
#                                 "previous_lot_averages": {k: round(v, 3) if v is not None and not pd.isna(v) else None for k, v in prev_lot_avg.items()} if prev_lot_avg else {},
#                                 "parameter_alerts": parameter_alerts,
#                                 "ufm_shift_occurred": machine_lot_overall_shift_occurred,
#                                 "target_machine_lot_size": target_lot_size,
#                                 "processed_machine_barcodes": target_lot_size,
#                                 "timestamp_alert_generated": some_function()
#                             }
                            
#                             ufm_averages_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(averages_doc_data)))
#                             output_messages.append(f"Prepared averages for {current_ufm_machine_lot_id} for insertion into averages_ufm_collect")

#                             # Update maps
#                             current_avg_key = (model_number, machine_no, current_ufm_machine_lot_id)
#                             ufm_machine_lot_avg_map[current_avg_key] = lot_averages
#                             ufm_previous_machine_model_lot_ids[machine_model_key] = current_ufm_machine_lot_id

#                             # Increment alert ID
#                             last_alert_id += 1
#                             merge_df.loc[machine_batch_indices, "alert_id_ufm_ufm"] = last_alert_id

#                             # Build shift data (existing logic)
#                             machine_batch_data["tbm_shift"] = machine_batch_data["date_of_tbm"].apply(get_shift)
#                             machine_batch_data = machine_batch_data.sort_values("date_of_tbm").reset_index(drop=True)

#                             shifts_data_list_tbm = []
#                             current_shift = None
#                             current_start = None
#                             current_barcodes = []

#                             for idx, row in machine_batch_data.iterrows():
#                                 this_shift = row["tbm_shift"]
#                                 this_dt = row["date_of_tbm"]
#                                 this_bc = row.get("barcode")

#                                 if this_shift != current_shift or current_shift is None:
#                                     if current_shift is not None:
#                                         shifts_data_list_tbm.append({
#                                             "shift": current_shift,
#                                             "barcode_count": len(current_barcodes),
#                                             "machine_shift_tbm_start_time": current_start,
#                                             "machine_shift_tbm_end_time": machine_batch_data.at[idx-1, "date_of_tbm"],
#                                             "barcodes": current_barcodes[:]
#                                         })
#                                     current_shift = this_shift
#                                     current_start = this_dt
#                                     current_barcodes = [this_bc] if this_bc is not None else []
#                                 else:
#                                     current_barcodes.append(this_bc if this_bc is not None else None)

#                             if current_shift is not None:
#                                 shifts_data_list_tbm.append({
#                                     "shift": current_shift,
#                                     "barcode_count": len(current_barcodes),
#                                     "machine_shift_tbm_start_time": current_start,
#                                     "machine_shift_tbm_end_time": machine_batch_data["date_of_tbm"].iloc[-1],
#                                     "barcodes": current_barcodes
#                                 })

#                             shift_groups = machine_batch_data.groupby('shift')
#                             shifts_data_list = []
#                             for shift_name, shift_data in shift_groups:
#                                 if not shift_data.empty and 'date_of_ufm' in shift_data.columns:
#                                     shift_data_sorted = shift_data.sort_values('date_of_ufm')
#                                     shifts_data_list.append({
#                                         "shifts": shift_name,
#                                         "barcode_count": len(shift_data_sorted["barcode"]) if "barcode" in shift_data_sorted.columns else 0,
#                                         "machine_shift_ufm_start_time": shift_data_sorted["date_of_ufm"].iloc[0],
#                                         "machine_shift_ufm_end_time": shift_data_sorted["date_of_ufm"].iloc[-1],
#                                         "barcodes": shift_data_sorted["barcode"].tolist() if "barcode" in shift_data_sorted.columns else []
#                                     })

#                             machine_alert_detail = {
#                                 "machine": machine_no,
#                                 "tbm_machine": tbm_machine_value,
#                                 "ufm_shift_occured": machine_lot_overall_shift_occurred,
#                                 "shifts_data": shifts_data_list,
#                                 "shifts_data_tbm": shifts_data_list_tbm,
#                                 "overall_uniformity_yield": overall_uniformity_yield(machine_batch_data),
#                                 "overall_warehouse_yield": overall_warehouse_yield(machine_batch_data),
#                                 "overall_visual_yield": overall_visual_yield(machine_batch_data),
#                                 "overall_scrap_yield": overall_scrap_yield(machine_batch_data),
#                                 "visual_loss_yield": visual_loss_yield(machine_batch_data),
#                                 "uf_loss_yield": uf_loss_yield(machine_batch_data),
#                                 "oe_ok_yield": oe_ok_yield(machine_batch_data),
#                                 "rfv_yield": rfv_yield(machine_batch_data),
#                                 "rfh1_yield": rfh1_yield(machine_batch_data),
#                                 "rfh2_yield": rfh2_yield(machine_batch_data),
#                                 "lfv_yield": lfv_yield(machine_batch_data),
#                                 "plysteer_yield": plysteer_yield(machine_batch_data),
#                                 "sizecode": sizecode, 
#                                 "gt_code": str(gt_code),
#                                 "target_machine_lot_size": target_lot_size,
#                                 "processed_machine_barcodes": target_lot_size,
#                                 "machine_lot_start_ufm_time": machine_batch_data["date_of_ufm"].min() if not machine_batch_data["date_of_ufm"].empty else None,
#                                 "machine_lot_end_ufm_time": machine_batch_data["date_of_ufm"].max() if not machine_batch_data["date_of_ufm"].empty else None,
#                                 "machine_lot_start_build_time": machine_batch_data["date_of_tbm"].min() if not machine_batch_data["date_of_tbm"].empty else None,
#                                 "machine_lot_end_build_time": machine_batch_data["date_of_tbm"].max() if not machine_batch_data["date_of_tbm"].empty else None,
#                                 "parameter_alerts": parameter_alerts,
#                                 "lot_averages": {k: round(v, 3) if v is not None and not pd.isna(v) else None for k, v in lot_averages.items()},
#                                 "previous_lot_averages": {k: round(v, 3) if v is not None and not pd.isna(v) else None for k, v in prev_lot_avg.items()} if prev_lot_avg else {},
#                                 "first_barcode_machine": machine_batch_data.iloc[0]["barcode"] if not machine_batch_data.empty and "barcode" in machine_batch_data.columns else None,
#                                 "last_barcode_machine": machine_batch_data.iloc[-1]["barcode"] if not machine_batch_data.empty and "barcode" in machine_batch_data.columns else None,
#                                 "ufm_machine_model_lot_id": current_ufm_machine_lot_id,
#                                 "ufm_previous_machine_model_lot_id": ufm_previous_machine_model_lot_id,
#                                 "barcodes": machine_batch_data["barcode"].tolist() if "barcode" in machine_batch_data.columns else [],
#                                 "cur_ids": (
#                                     [
#                                         {"cur_id": cur_id, "barcode": int(count)}
#                                         for cur_id, count in machine_batch_data.groupby("cur_id")["barcode"].count().items()
#                                     ]
#                                     if "cur_id" in machine_batch_data.columns and "barcode" in machine_batch_data.columns
#                                     else []
#                                 ),
#                                 "tbm_ids": (
#                                     [
#                                         {"tbm_id": ptb_id, "barcode": int(count)}
#                                         for ptb_id, count in machine_batch_data.groupby("ptb_id")["barcode"].count().items()
#                                     ]
#                                     if "ptb_id" in machine_batch_data.columns and "barcode" in machine_batch_data.columns
#                                     else []
#                                 )
#                             }

#                             # Create alert documents
#                             alert_doc_main = {
#                                 "_id": f"main_alert_{last_alert_id}",
#                                 "alert_id_ufm_ufm": last_alert_id,
#                                 "sku": model_number,
#                                 "lot_id": current_model_lot_id,
#                                 "alert_flag": False,
#                                 "dashboard_flag": False,
#                                 "alert_status_check": "o",
#                                 "alert_status_timestamp": None,
#                                 "alert_status_comment": [],
#                                 "alert_assign_user": [],
#                                 "machine_alerts": [machine_alert_detail],
#                                 "first_barcode_model": machine_alert_detail["first_barcode_machine"],
#                                 "last_barcode_model": machine_alert_detail["last_barcode_machine"],
#                                 "timestamp_alert_generated": some_function()
#                             }

#                             # Store in alert_map with lot_id as key
#                             if current_model_lot_id not in alert_map:
#                                 alert_map[current_model_lot_id] = {
#                                     "alert_id_ufm_ufm": last_alert_id,
#                                     "sku": model_number,
#                                     "lot_id": current_model_lot_id,
#                                     "machine_alerts": []
#                                 }
                            
#                             alert_map[current_model_lot_id]["machine_alerts"].append(machine_alert_detail.copy())

#                             trigger_doc = {
#                                 "_id": f"trigger_alert_{last_alert_id}",
#                                 "alert_id_ufm_ufm": last_alert_id,
#                                 "sku": model_number,
#                                 "lot_id": current_model_lot_id,
#                                 "type": "ufm_trigger",
#                                 "timestamp_alert_generated": some_function()
#                             }

#                             # Insert main alerts
#                             if not alert_collect.find_one({"alert_id_ufm_ufm": last_alert_id}):
#                                 try:
#                                     alert_collect.insert_one(convert_numpy_types(alert_doc_main))
#                                     logging.info(f"Inserted alert_doc_main into alert_table for alert_id {last_alert_id}")
#                                     output_messages.append(f"Inserted into alert_table: alert_id {last_alert_id}")
#                                 except Exception as e:
#                                     logging.error(f"Failed to insert into alert_table: {e}")

#                             if not alert_triggering_collect_dashboard_ufm.find_one({"alert_id_ufm_ufm": last_alert_id}):
#                                 try:
#                                     alert_triggering_collect_dashboard_ufm.insert_one(convert_numpy_types(alert_doc_main))
#                                     logging.info(f"Inserted alert_doc_main into alert_db_dashboard for alert_id {last_alert_id}")
#                                     output_messages.append(f"Inserted into alert_db_dashboard (main): alert_id {last_alert_id}")
#                                 except Exception as e:
#                                     logging.error(f"Failed to insert main alert into alert_db_dashboard: {e}")

#                             if not alert_triggering_collect_dashboard_ufm.find_one({"_id": f"trigger_alert_{last_alert_id}"}):
#                                 try:
#                                     alert_triggering_collect_dashboard_ufm.insert_one(convert_numpy_types(trigger_doc))
#                                     logging.info(f"Inserted trigger_doc into alert_db_dashboard for alert_id {last_alert_id}")
#                                     output_messages.append(f"Inserted into alert_db_dashboard (trigger): alert_id {last_alert_id}")
#                                 except Exception as e:
#                                     logging.error(f"Failed to insert trigger_doc into alert_db_dashboard: {e}")

#                             if machine_lot_overall_shift_occurred:
#                                 if not alert_ufm_collect.find_one({"alert_id_ufm_ufm": last_alert_id}):
#                                     ufm_alert_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(alert_doc_main)))
#                                     ufm_triggering_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(trigger_doc)))
#                                     ufm_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(trigger_doc)))
#                                     logging.info(f"Shift detected - added to bulk ops for alert_id {last_alert_id}")
#                                     output_messages.append(f"Shift alert queued for bulk: alert_id {last_alert_id}")

#                             # Insert Lot document into MongoDB (NOW IT'S COMPLETE!)
#                             lot_doc_data = {
#                                 "_id": f"lot_{current_ufm_machine_lot_id}",
#                                 "sku": model_number,
#                                 "lot_id": current_model_lot_id,
#                                 "previous_lot_id": model_level_previous_lot_id,
#                                 "machine": machine_no,
#                                 "ufm_machine_model_lot_id": current_ufm_machine_lot_id,
#                                 "ufm_previous_machine_model_lot_id": ufm_previous_machine_model_lot_id,
#                                 "target_machine_lot_size": target_lot_size,
#                                 "processed_machine_barcodes": target_lot_size,
#                                 "timestamp_lot_generated": some_function(),
#                                 "first_barcode": machine_batch_data['barcode'].iloc[0] if not machine_batch_data.empty else None,
#                                 "last_barcode": machine_batch_data['barcode'].iloc[-1] if not machine_batch_data.empty else None,
#                             }
                            
#                             ufm_lot_bulk_operations.append(pymongo.UpdateOne(
#                                 {"ufm_machine_model_lot_id": current_ufm_machine_lot_id},
#                                 {"$setOnInsert": convert_numpy_types(lot_doc_data)},
#                                 upsert=True
#                             ))
#                             output_messages.append(f"Prepared complete lot_id {current_model_lot_id} for insertion into lot_ufm_collect")

#                             machine_barcode_counter += target_lot_size

#                     except Exception as e:
#                         logging.error("UFM Error processing machine %s for model %s: %s", ufm_machine_id, model_number, e)
#                         output_messages.append(f"UFM Error processing machine {ufm_machine_id} for model {model_number}: {e}")
#                         continue

#                 # ==================== VIRGIN & FINAL YIELD ALERT INSERTION ====================
#                 try:
#                     # Process each lot_id only once
#                     for lot_id, alert_obj in alert_map.items():
#                         true_machine_alerts = [ma for ma in alert_obj["machine_alerts"] if ma.get("ufm_shift_occured", False) in [True, False]]
#                         if not true_machine_alerts:
#                             continue

#                         # Prepare barcode sets
#                         virgin_barcodes_set = set(virgin_data["barcode"].dropna().astype(str))
#                         final_barcodes_set = set(final_data["barcode"].dropna().astype(str))

#                         # === VIRGIN YIELD ===
#                         virgin_machine_alerts = []
#                         virgin_barcodes_seen = set()
#                         for ma in true_machine_alerts:
#                             original_barcodes = ma.get("barcodes", [])
#                             if not original_barcodes:
#                                 continue
#                             # A lot completed from BANKED data (barcodes carried over from a
#                             # previous run) will have barcodes that are not present in THIS run's
#                             # virgin_data. Filtering only against virgin_data would wrongly shrink
#                             # a full 50-barcode lot (e.g. down to 22). Since these barcodes already
#                             # belong to this completed lot, treat the lot's own barcodes as valid
#                             # too, so a full lot keeps all its processed barcodes.
#                             lot_own_barcodes = {str(bc) for bc in original_barcodes}
#                             allowed_barcodes = virgin_barcodes_set | lot_own_barcodes
#                             filtered = [bc for bc in original_barcodes if str(bc) in allowed_barcodes and str(bc) not in virgin_barcodes_seen]
#                             if filtered:
#                                 virgin_ma = ma.copy()
#                                 virgin_ma["barcodes"] = filtered
#                                 virgin_ma["barcode_count"] = len(filtered)
#                                 virgin_machine_alerts.append(virgin_ma)
#                                 virgin_barcodes_seen.update(filtered)

#                         if virgin_machine_alerts:
#                             def _virgin_sort_key(x):
#                                 _rows = virgin_data[virgin_data["barcode"].astype(str) == x]
#                                 return _rows["date_of_dt"].iloc[0] if not _rows.empty else pd.Timestamp.max
#                             all_virgin_bcs = sorted(virgin_barcodes_seen, key=_virgin_sort_key)
#                             doc_virgin = {
#                                 "alert_id_ufm_ufm": alert_obj["alert_id_ufm_ufm"],
#                                 "sku": alert_obj["sku"],
#                                 "lot_id": alert_obj["lot_id"],
#                                 "alert_flag": False,
#                                 "dashboard_flag": False,
#                                 "alert_status_check": "o",
#                                 "alert_status_timestamp": None,
#                                 "alert_status_comment": [],
#                                 "alert_assign_user": [],
#                                 "machine_alerts": virgin_machine_alerts,
#                                 "first_barcode_model": all_virgin_bcs[0] if all_virgin_bcs else None,
#                                 "last_barcode_model": all_virgin_bcs[-1] if all_virgin_bcs else None,
#                                 "timestamp_alert_generated": some_function(),
#                                 "yield_type": "virgin"
#                             }
                            
#                             # Check if this virgin alert already exists in the database
#                             existing_virgin = alert_triggering_virgin_yield_collect.find_one({
#                                 "sku": alert_obj["sku"],
#                                 "lot_id": alert_obj["lot_id"],
#                                 "yield_type": "virgin"
#                             })
                            
#                             if existing_virgin:
#                                 logging.info(f"Virgin alert already exists for lot: {alert_obj['lot_id']}. This is expected if the lot was completed in a previous run.")
#                             else:
#                                 alert_triggering_virgin_yield_collect.insert_one(convert_numpy_types(doc_virgin))
#                                 logging.info(f"Inserted Virgin Yield Alert for lot: {alert_obj['lot_id']} with alert_id {alert_obj['alert_id_ufm_ufm']}")
#                                 output_messages.append(f"Inserted Virgin Yield Alert for lot: {alert_obj['lot_id']}")

#                         # === FINAL YIELD ===
#                         final_machine_alerts = []
#                         final_barcodes_seen = set()
#                         for ma in true_machine_alerts:
#                             original_barcodes = ma.get("barcodes", [])
#                             if not original_barcodes:
#                                 continue
#                             # Same as virgin: keep the lot's own barcodes even if they are not in
#                             # THIS run's final_data (carried over from a previous run when the lot
#                             # was completed), so a full lot keeps all its processed barcodes.
#                             lot_own_barcodes = {str(bc) for bc in original_barcodes}
#                             allowed_barcodes = final_barcodes_set | lot_own_barcodes
#                             filtered = [bc for bc in original_barcodes if str(bc) in allowed_barcodes and str(bc) not in final_barcodes_seen]
#                             if filtered:
#                                 final_ma = ma.copy()
#                                 final_ma["barcodes"] = filtered
#                                 final_ma["barcode_count"] = len(filtered)
#                                 final_machine_alerts.append(final_ma)
#                                 final_barcodes_seen.update(filtered)

#                         if final_machine_alerts:
#                             def _final_sort_key(x):
#                                 _rows = final_data[final_data["barcode"].astype(str) == x]
#                                 return _rows["date_of_dt"].iloc[0] if not _rows.empty else pd.Timestamp.max
#                             all_final_bcs = sorted(final_barcodes_seen, key=_final_sort_key)
#                             doc_final = {
#                                 "alert_id_ufm_ufm": alert_obj["alert_id_ufm_ufm"],
#                                 "sku": alert_obj["sku"],
#                                 "lot_id": alert_obj["lot_id"],
#                                 "alert_flag": False,
#                                 "dashboard_flag": False,
#                                 "alert_status_check": "o",
#                                 "alert_status_timestamp": None,
#                                 "alert_status_comment": [],
#                                 "alert_assign_user": [],
#                                 "machine_alerts": final_machine_alerts,
#                                 "first_barcode_model": all_final_bcs[0] if all_final_bcs else None,
#                                 "last_barcode_model": all_final_bcs[-1] if all_final_bcs else None,
#                                 "timestamp_alert_generated": some_function(),
#                                 "yield_type": "final"
#                             }
                            
#                             # Check if final alert already exists
#                             existing_final = alert_triggering_final_yield_collect.find_one({
#                                 "sku": alert_obj["sku"],
#                                 "lot_id": alert_obj["lot_id"],
#                                 "yield_type": "final"
#                             })
                            
#                             if existing_final:
#                                 logging.info(f"Final alert already exists for lot: {alert_obj['lot_id']}. This is expected if the lot was completed in a previous run.")
#                             else:
#                                 alert_triggering_final_yield_collect.insert_one(convert_numpy_types(doc_final))
#                                 alert_final_yield_collect.insert_one(convert_numpy_types(doc_final))
#                                 logging.info(f"Inserted Final Yield Alert for lot: {alert_obj['lot_id']}")
#                                 output_messages.append(f"Inserted Final Yield Alert for lot: {alert_obj['lot_id']}")

#                             # Deviated Final Yield (only when shift=True)
#                             has_shift = any(ma.get("ufm_shift_occured", False) for ma in final_machine_alerts)
#                             if has_shift:
#                                 deviated_parameter_alerts = []
#                                 for ma in final_machine_alerts:
#                                     if not ma.get("ufm_shift_occured", False):
#                                         continue
#                                     for alert in ma.get("parameter_alerts", []):
#                                         if alert.get("shift_occurred_thershold", False) or alert.get("shift_occurred_spec", False):
#                                             dev_alert = alert.copy()
#                                             param = dev_alert.get("parameter", "")
#                                             if param.endswith("_yield") and param in rank_param_mappings:
#                                                 batch_data = final_data[final_data["barcode"].astype(str).isin(ma["barcodes"])]
#                                                 rank_counts = {}
#                                                 for col in rank_param_mappings[param]:
#                                                     if col in batch_data.columns:
#                                                         counts = batch_data[col].value_counts()
#                                                         for val, cnt in counts.items():
#                                                             val = int(val) if isinstance(val, (int, float)) else val
#                                                             rank_counts[f"{col}_{val}"] = int(cnt)
#                                                 dev_alert["rank_counts"] = rank_counts
#                                             deviated_parameter_alerts.append(dev_alert)
                                            
#                                 if deviated_parameter_alerts:
#                                     first_ma = final_machine_alerts[0]
#                                     ufm_machine_model_lot_id = first_ma.get("ufm_machine_model_lot_id")
#                                     ufm_previous_machine_model_lot_id = first_ma.get("ufm_previous_machine_model_lot_id")

#                                     deviated_doc = {
#                                         "alert_id_tbm_ufm": alert_obj["alert_id_ufm_ufm"],
#                                         "sku": alert_obj["sku"],
#                                         "lot_id": alert_obj["lot_id"],
#                                         "machine": final_machine_alerts[0]["machine"],
#                                         "ufm_machine_model_lot_id": ufm_machine_model_lot_id,
#                                         "ufm_previous_machine_model_lot_id": ufm_previous_machine_model_lot_id,
#                                         "comments": [],
#                                         "alert_assign_user": [],
#                                         "parameter_alerts": deviated_parameter_alerts,
#                                         "timestamp_alert_generated": some_function(),
#                                         "yield_type": "final"
#                                     }
                                    
#                                     # Check if deviated final alert already exists
#                                     existing_deviated = alert_deviated_final_yield_collect.find_one({
#                                         "sku": alert_obj["sku"],
#                                         "lot_id": alert_obj["lot_id"],
#                                         "yield_type": "final"
#                                     })
                                    
#                                     if existing_deviated:
#                                         logging.info(f"Deviated final alert already exists for lot: {alert_obj['lot_id']}. This is expected if the lot was completed in a previous run.")
#                                     else:
#                                         alert_deviated_final_yield_collect.insert_one(convert_numpy_types(deviated_doc))
#                                         logging.info(f"Inserted Deviated Final Yield Alert for lot: {alert_obj['lot_id']}")
#                                         output_messages.append(f"Inserted Deviated Final Yield Alert for lot: {alert_obj['lot_id']}")

#                 except Exception as e:
#                     logging.error(f"Error in Virgin/Final Yield alert insertion: {e}")
#                     output_messages.append(f"Error in Virgin/Final Yield alert insertion: {e}")

#         except Exception as e:
#             logging.error("UFM Error processing model %s: %s", model_number, e)
#             output_messages.append(f"UFM Error processing model {model_number}: {e}")
#             continue

#     # ============== NEW: Update incomplete lots in database ==============
#     try:
#         # Update incomplete lots
#         for update_doc in incomplete_lots_to_update:
#             incomplete_lots_collect.update_one(
#                 {"_id": update_doc["_id"]},
#                 {"$set": {k: v for k, v in update_doc.items() if k != "_id"}}
#             )
#             logging.info(f"Updated incomplete lot: {update_doc.get('lot_id')}")
        
#         # Delete completed incomplete lots
#         for delete_id in incomplete_lots_to_delete:
#             incomplete_lots_collect.delete_one({"_id": delete_id})
#             logging.info(f"Deleted completed incomplete lot with _id: {delete_id}")
            
#     except Exception as e:
#         logging.error(f"Error updating incomplete lots: {e}")
#         output_messages.append(f"❌ Error updating incomplete lots: {e}")

#     # Perform Bulk Writes
#     final_log_messages = []
#     if ufm_lot_bulk_operations:
#         try:
#             result = lot_ufm_collect.bulk_write(ufm_lot_bulk_operations, ordered=False)
#             inserted_counts["lot_ufm_table"] = result.inserted_count
#             final_log_messages.append(f"✅ UFM Inserted {result.inserted_count} documents into UFM lot collection.")
#         except pymongo.errors.BulkWriteError as bwe:
#             inserted_counts["lot_ufm_table"] = bwe.details['nInserted']
#             final_log_messages.append(f"⚠️ UFM Lot Bulk Write Errors: Inserted {bwe.details['nInserted']}. Errors: {bwe.details['writeErrors']}")
#         except Exception as e:
#             final_log_messages.append(f"❌ UFM Error performing bulk write to UFM lot collection: {e}")

#     if ufm_averages_bulk_operations:
#         try:
#             result = averages_ufm_collect.bulk_write(ufm_averages_bulk_operations, ordered=False)
#             inserted_counts["averages_ufm_table"] = result.inserted_count
#             final_log_messages.append(f"✅ UFM Inserted {result.inserted_count} documents into UFM averages collection.")
#         except pymongo.errors.BulkWriteError as bwe:
#             inserted_counts["averages_ufm_table"] = bwe.details['nInserted']
#             final_log_messages.append(f"⚠️ UFM Averages Bulk Write Errors: Inserted {bwe.details['nInserted']}. Errors: {bwe.details['writeErrors']}")
#         except Exception as e:
#             final_log_messages.append(f"❌ UFM Error performing bulk write to UFM averages collection: {e}")

#     if deviated_triggering_bulk_operations:
#         try:
#             result = alert_triggering_deviated_collect.bulk_write(deviated_triggering_bulk_operations, ordered=False)
#             inserted_counts["alert_ufm_with_lot_size"] = result.inserted_count
#             output_messages.append(f"✅ UFM Inserted {result.inserted_count} deviated alert documents into alert_deviated.alert_ufm_with_lot_size.")
#         except Exception as e:
#             error_message = f"❌ UFM Error performing bulk write to deviated alert collections: {e}"
#             output_messages.append(error_message)
#     else:
#         output_messages.append("No UFM deviated alert documents to insert into deviated alert collections.")

#     if ufm_alert_bulk_operations:
#         try:
#             result = alert_ufm_collect.bulk_write(ufm_alert_bulk_operations, ordered=False)
#             inserted_counts["alert_ufm_collect"] = result.inserted_count
#             final_log_messages.append(f"✅ UFM Inserted {result.inserted_count} documents into UFM alert collection.")
#         except pymongo.errors.BulkWriteError as bwe:
#             inserted_counts["alert_ufm_collect"] = bwe.details['nInserted']
#             final_log_messages.append(f"⚠️ UFM Alert Bulk Write Errors: Inserted {bwe.details['nInserted']}. Errors: {bwe.details['writeErrors']}")
#         except Exception as e:
#             final_log_messages.append(f"❌ UFM Error performing bulk write to UFM alert collection: {e}")

#     if ufm_triggering_bulk_operations:
#         try:
#             result = alert_ufm_collect.bulk_write(ufm_triggering_bulk_operations, ordered=False)
#             inserted_counts["alert_ufm_collect"] = result.inserted_count
#             final_log_messages.append(f"✅ UFM Inserted {result.inserted_count} documents into alert_db.{alert_triggering_ubno_collect_name}.")
#         except pymongo.errors.BulkWriteError as bwe:
#             inserted_counts["alert_ufm_collect"] = bwe.details['nInserted']
#             final_log_messages.append(f"⚠️ UFM Triggering Alert Bulk Write Errors in alert_db.{alert_triggering_ubno_collect_name}: Inserted {bwe.details['nInserted']}. Errors: {bwe.details['writeErrors']}")
#         except Exception as e:
#             final_log_messages.append(f"❌ UFM Error performing bulk write to alert_db.{alert_triggering_ubno_collect_name}: {e}")

#         try:
#             result_dashboard = alert_triggering_collect_dashboard_ufm.bulk_write(ufm_triggering_bulk_operations, ordered=False)
#             inserted_counts["alert_triggering_collect_dashboard_ufm"] = result_dashboard.inserted_count
#             output_messages.append(f"✅ UFM Inserted {result_dashboard.inserted_count} machine_alert documents into alert_db_dashboard.{alert_triggering_ubno_collect_name}.")
#         except pymongo.errors.BulkWriteError as bwe:
#             inserted_counts["alert_triggering_collect_dashboard_ufm"] = bwe.details['nInserted']
#             output_messages.append(f"⚠️ UFM Triggering Alert Bulk Write Errors in alert_db_dashboard.{alert_triggering_ubno_collect_name}: Inserted {bwe.details['nInserted']}. Errors: {bwe.details['writeErrors']}")
#         except Exception as e:
#             output_messages.append(f"❌ UFM Error performing bulk write to alert_db_dashboard.{alert_triggering_ubno_collect_name}: {e}")
#     else:
#         output_messages.append(f"No UFM triggering alert documents to insert into alert_db_dashboard.{alert_triggering_ubno_collect_name}.")

#     if ufm_bulk_operations:
#         try:
#             result = alert_collect.bulk_write(ufm_bulk_operations, ordered=False)
#             inserted_counts["alert_collect"] = result.inserted_count
#             final_log_messages.append(f"✅ UFM Inserted {result.inserted_count} documents into UFM triggering alert collection.")
#         except pymongo.errors.BulkWriteError as bwe:
#             inserted_counts["alert_collect"] = bwe.details['nInserted']
#             final_log_messages.append(f"⚠️ UFM Triggering Alert Bulk Write Errors: Inserted {bwe.details['nInserted']}. Errors: {bwe.details['writeErrors']}")
#         except Exception as e:
#             final_log_messages.append(f"❌ UFM Error performing bulk write to UFM triggering alert collection: {e}")

#     output_messages.extend(final_log_messages)

#     # Final Update to merge_collect
#     ufm_bulk_updates_merge = []
#     processed_merge_df = merge_df.dropna(subset=["ufm_machine_model_lot_id"])

#     if not processed_merge_df.empty:
#         for index, row in processed_merge_df.iterrows():
#             query = {"_id": row["_id"]}
#             update_payload = {"$set": {
#                 "alert_id_ufm_ufm": row.get("alert_id_ufm_ufm"),
#                 "lot_id": row.get("lot_id"),
#                 "previous_lot_id": row.get("previous_lot_id"),
#                 "ufm_machine_model_lot_id": row.get("ufm_machine_model_lot_id"),
#                 "ufm_previous_machine_model_lot_id": row.get("ufm_previous_machine_model_lot_id")
#             }}
#             if "tbm_machine" in merge_df.columns:
#                 update_payload["$set"]["tbm_machine"] = row["tbm_machine"]
#             update_payload["$set"] = {k: v for k, v in update_payload["$set"].items() if v is not None}
#             if update_payload["$set"]:
#                 ufm_bulk_updates_merge.append(pymongo.UpdateOne(query, update_payload))

#         if ufm_bulk_updates_merge:
#             logging.info("ℹ️ Preparing final update for %s documents in main merge collection...", len(ufm_bulk_updates_merge))
#             try:
#                 result = merge_collect.bulk_write(ufm_bulk_updates_merge, ordered=False)
#                 updated_counts["merge_collection_ufm"] = result.modified_count
#                 logging.info("✅ UFM Updated %s documents in main merge collection with generated IDs.", result.modified_count)
#             except pymongo.errors.BulkWriteError as bwe:
#                 updated_counts["merge_collection_ufm"] = bwe.details['nModified']
#                 logging.info("⚠️ UFM Final Merge Update Bulk Write Errors: Modified %s. Errors: %s", bwe.details['nModified'], bwe.details['writeErrors'])
#             except Exception as e:
#                 logging.info("❌ UFM Error updating main merge collection: %s", e)
#         else:
#             logging.info("ℹ️ No final updates required for the main merge collection for UFM.")
#     else:
#         logging.info("ℹ️ No processed UFM rows found in merge_df for final update.")

#     process_end_time = time.time()
#     execution_time = process_end_time - process_start_time
#     final_message = f"✅ UFM Lot and Alert Process completed successfully in {execution_time:.2f} seconds!"
#     logging.info(final_message)

#     logging.info("process_lot_and_alert_data function is completed")
#     return {
#         "status": "success",
#         "message": final_message,
#         "output_log": output_messages,
#         "inserted_counts": inserted_counts,
#         "updated_counts": updated_counts,
#         "execution_time": execution_time
#     }, 200

def process_lot_and_alert_data():
    process_start_time = time.time()
    output_messages = []
    inserted_counts = {}
    updated_counts = {}
    logging.info("process_lot_and_alert_data function is just started")
    
    # ============== FIX: Initialize all bulk operation lists at the beginning ==============
    ufm_lot_bulk_operations = []
    ufm_averages_bulk_operations = []
    ufm_alert_bulk_operations = []
    ufm_triggering_bulk_operations = []
    ufm_bulk_operations = []
    deviated_triggering_bulk_operations = []
    ufm_bulk_updates_merge = []
    
    try:
        # UFM Specific Collections
        merge_collect = client[TUB_CIMCORP_VISUAL_MERGED_DB_alert][DATA_MERGED_SUMMARY_COLLECTION_alert]
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
        alert_final_yield_collect = client["alert_table_final_yield"][alert_ufm_collect_name]
        alert_deviated_final_yield_collect = client["alert_deviated_final_yield"][alert_triggering_deviated_ufm]
        # NEW: Initialize collections for model_workcenter_db
        ufm_collect_data = client[model_db][model_no_status_collection]
        ufm_machine_collect = client[model_db][ufm_collection_machine]
        
        # ============== NEW: Collection to track incomplete lots across runs ==============
        incomplete_lots_collect = client[config_databse]["incomplete_lots_tracker"]
        
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

    # # Fetch active model numbers
    # try:
    #     active_modelnos = set(ufm_collect_data.distinct("sku", {"status": "active"}))
    #     output_messages.append(f"✅ Fetched {len(active_modelnos)} active model numbers: {active_modelnos}")
    # except Exception as e:
    #     output_messages.append(f"❌ Error fetching active model numbers: {e}")
    #     return {"status": "error", "message": "Failed to fetch active model numbers", "output_log": output_messages}, 500

    # Fetch active OE model numbers
    try:
        active_modelnos = set(
            ufm_collect_data.distinct(
                "sku",
                {
                    "status": "active",
                    "oe_replacement_slob": "OE"
                }
            )
        )

        output_messages.append(
            f"✅ Fetched {len(active_modelnos)} active OE model numbers: {active_modelnos}"
        )

    except Exception as e:
        output_messages.append(f"❌ Error fetching active model numbers: {e}")
        return {
            "status": "error",
            "message": "Failed to fetch active model numbers",
            "output_log": output_messages
        }, 500

    # Create unique indexes
    try:
        lot_ufm_collect.create_index(
            [
                ("sku", pymongo.ASCENDING),
                ("machine", pymongo.ASCENDING),
                ("lot_id", pymongo.ASCENDING),
                ("ufm_machine_model_lot_id", pymongo.ASCENDING)
            ],
            unique=True,
            name="ufm_lot_unique_idx"
        )
        alert_ufm_collect.create_index(
            [
                ("sku", pymongo.ASCENDING),
                ("lot_id", pymongo.ASCENDING),
                ("alert_id_ufm_ufm", pymongo.ASCENDING)
            ],
            unique=True,
            name="ufm_alert_unique_idx"
        )
        # ============== NEW: Create index for incomplete lots tracker ==============
        incomplete_lots_collect.create_index(
            [
                ("sku", pymongo.ASCENDING),
                ("machine", pymongo.ASCENDING),
                ("lot_id", pymongo.ASCENDING)
            ],
            unique=True,
            name="incomplete_lots_unique_idx"
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
        channel_data = list(channel_collect.find({"sku": {"$in": list(active_modelnos)}}))
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
    
    # MODIFIED: Check for ptb_id column before adding tbm_machine column
    if 'ptb_id' in merge_df.columns and 'machine' in machine_df.columns:
        try:
            merge_df["tbm_machine"] = None
            for index, row in merge_df.iterrows():
                tbm_value = row.get("ptb_id")
                if tbm_value:
                    matching_channel = machine_df[machine_df["machine"] == tbm_value]
                    if not matching_channel.empty:
                        merge_df.at[index, "tbm_machine"] = matching_channel["machine"].iloc[0]
            output_messages.append("✅ Added tbm_machine column to merge_df based on matching cimcorp_tbm_tub values.")
        except Exception as e:
            output_messages.append(f"⚠️ Warning: Failed to add tbm_machine column to merge_df: {e}. Continuing without tbm_machine.")
    else:
        output_messages.append("⚠️ Warning: 'cimcorp_tbm_tub' column not found in merge_df or machine_df. Skipping tbm_machine column creation.")

    # MODIFIED: Check for cimcorp_tbm_tub column before updating machine_df
    if 'machine' in machine_df.columns and 'ptb_id' in merge_df.columns and 'tbm_machine' in merge_df.columns:
        try:
            for index, row in machine_df.iterrows():
                tbm_value = row.get("machine")
                if tbm_value:
                    matching_merge = merge_df[merge_df["ptb_id"] == tbm_value]
                    if not matching_merge.empty and "tbm_machine" in matching_merge.columns:
                        machine_df.at[index, "machine"] = matching_merge["tbm_machine"].iloc[0]
            output_messages.append("✅ Updated machine_df machine column with tbm_machine values from merge_df.")
        except Exception as e:
            output_messages.append(f"⚠️ Warning: Failed to update machine_df machine column: {e}. Continuing without updating channel_df machine.")
    else:
        output_messages.append("⚠️ Warning: 'cimcorp_tbm_tub' column not found in merge_df or machine_df. Skipping machine_df machine column update.")

    # Filter data
    try:
        channel_modelnos = set(channel_collect.distinct("sku"))
        merge_df = merge_df[merge_df["sku"].isin(channel_modelnos)]
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
        merge_df["date_of_tbm"] = merge_df["ptb_time"]
        merge_df["date_and_time"] = merge_df["uf_time"]
        merge_df["date_of_ufm"] = pd.to_datetime(merge_df["date_and_time"], errors="coerce").dt.strftime('%Y-%m-%d %H:%M:%S').fillna("No Date")

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

        merge_df["uf_id"] = merge_df["uf_id"].astype(str)
        ub_machine_df["ubno"] = ub_machine_df["ubno"].astype(str)
        # NEW: Filter merge_df to include only active machines
        merge_df = merge_df[merge_df["uf_id"].isin(active_machines)]
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
    # ------------------------------------------------------------------
    # FIX: alert_id_ufm_ufm was seeded ONLY from alert_ufm_collect (the
    # deviated/triggering collection), but a lot's alert is written there
    # ONLY when a shift occurred. Every lot's virgin & final yield alerts,
    # however, are always inserted into their own collections. So across
    # runs the max id in alert_ufm_collect lagged behind the virgin/final
    # collections, and new lots reused ids that already existed there
    # (two different lots getting the same alert_id_ufm_ufm).
    #
    # We now seed from the MAXIMUM alert_id_ufm_ufm across EVERY collection
    # that persists it, so each new alert always gets a fresh, unique id.
    try:
        _id_source_collections = [
            alert_ufm_collect,
            alert_collect,
            alert_triggering_collect_dashboard_ufm,
            alert_triggering_deviated_collect,
            alert_triggering_virgin_yield_collect,
            alert_triggering_final_yield_collect,
            alert_final_yield_collect,
        ]
        last_alert_id = 0
        for _coll in _id_source_collections:
            try:
                _doc = _coll.find_one(
                    {"alert_id_ufm_ufm": {"$ne": None}},
                    sort=[("alert_id_ufm_ufm", -1)]
                )
                if _doc and _doc.get("alert_id_ufm_ufm") is not None:
                    _val = int(_doc["alert_id_ufm_ufm"])
                    if _val > last_alert_id:
                        last_alert_id = _val
            except Exception as _inner:
                logging.warning("⚠️ UFM Warning reading max alert id from %s: %s",
                                getattr(_coll, "name", "unknown"), _inner)
        logging.info("ℹ️ Last UFM Alert ID found (max across collections): %s", last_alert_id)
        output_messages.append(f"ℹ️ Last UFM Alert ID found (max across collections): {last_alert_id}")
    except Exception as e:
        logging.warning("⚠️ UFM Warning fetching last alert ID: %s. Starting from 0.", e)
        output_messages.append(f"⚠️ UFM Warning fetching last alert ID: {e}. Starting from 0.")
        last_alert_id = 0

    # ============== FIX: Fetch last lot numbers per machine-model combination from database ==============
    try:
        # Get all existing lot numbers for each machine-sku combination
        last_lots_agg = list(lot_ufm_collect.aggregate([
            {"$match": {
                "lot_id": {"$regex": "^UFM_[^_]+_[^_]+_\\d+$"},
                "sku": {"$in": list(active_modelnos)},
                "machine": {"$in": list(active_machines)}
            }},
            {"$addFields": {
                "lot_number": {
                    "$toInt": {"$arrayElemAt": [{"$split": ["$lot_id", "_"]}, -1]}
                }
            }},
            {"$sort": {"lot_number": -1}},
            {"$group": {
                "_id": {"sku": "$sku", "machine": "$machine"},
                "max_lot_number": {"$first": "$lot_number"},
                "last_lot_id": {"$first": "$lot_id"},
                "last_ufm_machine_model_lot_id": {"$first": "$ufm_machine_model_lot_id"}
            }}
        ]))
        last_lot_dict = {
            (item["_id"]["sku"], item["_id"]["machine"]): {
                "max_lot_number": item["max_lot_number"],
                "last_lot_id": item["last_lot_id"],
                "last_ufm_machine_model_lot_id": item["last_ufm_machine_model_lot_id"]
            } for item in last_lots_agg
        }

        # ==================================================================
        # FIX: lot numbering was seeded ONLY from lot_ufm_collect. That
        # collection is written via bulk ops at the END of the run, so a lot
        # completed in a PREVIOUS run may not be reflected reliably, causing the
        # SAME lot_id (e.g. UFM_<m>_<sku>_1) to be regenerated. The virgin/final
        # yield collections, however, persist each completed lot's lot_id
        # immediately. So we also scan those to raise max_lot_number, ensuring a
        # new lot for a (sku, machine) whose previous lot is already complete
        # gets the NEXT incremented number (and a unique lot_id) instead of
        # colliding with the old one and being skipped as "already exists".
        # ==================================================================
        def _lot_number_from_lot_id(lot_id):
            # lot_id format: UFM_<machine>_<sku>_<n>
            try:
                return int(str(lot_id).rsplit("_", 1)[-1])
            except (ValueError, IndexError, TypeError):
                return None

        def _machine_from_lot_id(lot_id):
            # UFM_<machine>_<sku>_<n> -> <machine>
            try:
                parts = str(lot_id).split("_")
                return parts[1] if len(parts) >= 4 else None
            except (IndexError, TypeError):
                return None

        for _src_coll in (alert_triggering_virgin_yield_collect, alert_triggering_final_yield_collect):
            try:
                for _d in _src_coll.find(
                    {"lot_id": {"$regex": "^UFM_[^_]+_[^_]+_\\d+$"}},
                    {"_id": 0, "sku": 1, "lot_id": 1}
                ):
                    _sku = _d.get("sku")
                    _lot_id = _d.get("lot_id")
                    _num = _lot_number_from_lot_id(_lot_id)
                    _machine = _machine_from_lot_id(_lot_id)
                    if _sku is None or _machine is None or _num is None:
                        continue
                    _key = (_sku, _machine)
                    existing = last_lot_dict.get(_key)
                    if existing is None or _num > existing.get("max_lot_number", 0):
                        last_lot_dict[_key] = {
                            "max_lot_number": _num,
                            "last_lot_id": _lot_id,
                            # keep any ufm id we already had; None if newly seen
                            "last_ufm_machine_model_lot_id": (existing or {}).get("last_ufm_machine_model_lot_id")
                        }
            except Exception as _e:
                logging.warning("⚠️ UFM Warning scanning %s for max lot: %s",
                                getattr(_src_coll, "name", "unknown"), _e)

        output_messages.append(f"ℹ️ Last UFM Lot numbers fetched: {len(last_lot_dict)} entries")
        
        # Log the current state for debugging
        for key, value in last_lot_dict.items():
            logging.info(f"Machine-SKU: {key}, Max Lot: {value['max_lot_number']}, Last UFM ID: {value['last_ufm_machine_model_lot_id']}")
            
    except Exception as e:
        logging.warning("⚠️ UFM Error fetching last lot numbers: %s. Starting counts from 0.", e)
        output_messages.append(f"⚠️ UFM Error fetching last lot numbers: {e}. Starting counts from 0.")
        last_lot_dict = {}

    # ============== FIX: Fetch last averages to get previous ufm_machine_model_lot_id ==============
    try:
        last_averages = list(averages_ufm_collect.aggregate([
            {
                "$match": {
                    "ufm_machine_model_lot_id": {"$regex": "^[^_]+_[^_]+_UFM_[^_]+_[^_]+_\\d+_\\d+$"},
                    "sku": {"$in": list(active_modelnos)},
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
                    "_id": {"sku": "$sku", "machine": "$machine"},
                    "last_ufm_machine_model_lot_id": {"$first": "$ufm_machine_model_lot_id"},
                    "last_averages": {"$first": "$current_lot_averages"}
                }
            }
        ]))
        ufm_previous_machine_model_lot_ids = {}
        ufm_machine_lot_avg_map = {}
        for item in last_averages:
            sku = item["_id"]["sku"]
            machine = item["_id"]["machine"]
            ufm_lot_id = item["last_ufm_machine_model_lot_id"]
            averages = item["last_averages"]
            ufm_previous_machine_model_lot_ids[(sku, machine)] = ufm_lot_id
            if averages:
                ufm_machine_lot_avg_map[(sku, machine, ufm_lot_id)] = averages
        output_messages.append(f"Initialized ufm_previous_machine_model_lot_ids with {len(ufm_previous_machine_model_lot_ids)} entries")
    except Exception as e:
        logging.error(f"Error pre-populating ufm_machine_lot_avg_map and ufm_previous_machine_model_lot_ids: {e}")
        output_messages.append(f"❌ Error pre-populating ufm_machine_lot_avg_map and ufm_previous_machine_model_lot_ids: {e}")
        ufm_previous_machine_model_lot_ids = {}
        ufm_machine_lot_avg_map = {}

    # ============== NEW: Load incomplete lots from database ==============
    try:
        incomplete_lots_from_db = list(incomplete_lots_collect.find({}))
        incomplete_lots_dict = {}
        for incomplete_lot in incomplete_lots_from_db:
            key = (incomplete_lot["sku"], incomplete_lot["machine"])
            if key not in incomplete_lots_dict:
                incomplete_lots_dict[key] = []
            incomplete_lots_dict[key].append(incomplete_lot)
        logging.info(f"Loaded {len(incomplete_lots_from_db)} incomplete lots from database")
        output_messages.append(f"Loaded {len(incomplete_lots_from_db)} incomplete lots from database")
    except Exception as e:
        logging.error(f"Error loading incomplete lots from database: {e}")
        output_messages.append(f"❌ Error loading incomplete lots from database: {e}")
        incomplete_lots_dict = {}

    ufm_params = [
        "rfppcc","rfppcw","rfh1cc","rfh1cw","rfh2cc","rfh2cw","lfppcc","lfppcw","cony","plys",
        "crro","trro","brro","lrot","lrob","lroh1grade","tbul","bbul","tdep","bdep",
        "upper","lower","sbal","aircc","aircw","loadcc","loadcw"
    ]
    yield_params = [
        "rfv_yield", "rfh1_yield", "rfh2_yield", "lfv_yield", "conicity_yield", "plysteer_yield",
        "center_radial_run_out_yield", "top_radial_run_out_yield", "bottom_radial_run_out_yield", 
        "lateral_run_out_yield", "lateral_run_out_harmonic_first_yield", "center_radial_run_out_harmonic_first_yield",
        "top_radial_run_out_harmonic_first_yield", "bottom_radial_run_out_harmonic_first_yield",
        "lateral_radial_run_out_yield", "static_yield", "couple_yield",
        "upper_yield", "lower_yield", "overall_uniformity_yield", "overall_visual_yield", "overall_warehouse_yield",
        "overall_scrap_yield", "visual_loss_yield", "uf_loss_yield", "oe_ok_yield"
    ]

    rank_param_mappings = {
        "rfv_yield": ["rfppwcgrade"],
        "rfh1_yield": ["rfh1wcgrade"],
        "rfh2_yield": ["rfh2wcgrade"],
        "lfv_yield": ["lfppwcgrade"],
        "conicity_yield": ["conygrade"],
        "plysteer_yield": ["plysgrade"],
        "center_radial_run_out_yield": ["crrograde"], 
        "top_radial_run_out_yield": ["trrograde"], 
        "bottom_radial_run_out_yield": ["brrograde"],
        "lateral_run_out_yield": ["lrograde"],
        "lateral_run_out_harmonic_first_yield": ["lroh1grade"],
        "center_radial_run_out_harmonic_first_yield": ["crroh1grade"],
        "top_radial_run_out_harmonic_first_yield": ["trroh1grade"],
        "bottom_radial_run_out_harmonic_first_yield": ["brroh1grade"],
        "lateral_radial_run_out_yield": ["lrrograde"],
        "static_yield": ["sbalgrade"],
        "couple_yield": ["couplegrade"],
        "upper_yield": ["uppergrade"],
        "lower_yield": ["lowergrade"]
    }

    # ============== FIX: Track already processed barcodes to avoid duplicates ==============
    processed_barcodes_per_machine_model = {}
    
    # ============== FIX: Track lot sizes and status to handle partial lots ==============
    lot_status_tracker = {}
    
    # ============== FIX: Track alerts that have been inserted in this run ==============
    inserted_alerts_in_run = set()
    
    # ============== NEW: Track incomplete lots to update/delete from database ==============
    incomplete_lots_to_update = []
    incomplete_lots_to_delete = []

    for _, row in channel_df.iterrows():
        try:
            model_number = str(row.get("sku", "")).strip()
            if model_number not in active_modelnos:
                output_messages.append(f"Skipping CURING model {model_number} as it is not active.")
                continue
            gt_codes = row.get("gt_code", [])

            # Ensure gt_codes is a list
            if not isinstance(gt_codes, list):
                gt_codes = [gt_codes]

            for gt_code in gt_codes:
                gt_code = str(gt_code).strip()

                # MODIFIED: Filter merge_df for each gt_code individually
                model_data = merge_df[
                    (merge_df["sku"] == model_number) &
                    (merge_df["uf_id"].isin(active_machines))
                ].copy()

                target_lot_size = int(row.get("lot_size", 0))
                if target_lot_size == 0:
                    logging.info(f"Skipping UFM model {model_number} due to invalid lot size.")
                    output_messages.append(f"Skipping UFM model {model_number} due to invalid lot size.")
                    continue
                spec_cony_upper = float(row.get("spec_cony_upper", 0))
                spec_cony_lower = float(row.get("spec_cony_lower", 0))
                spec_plys_upper = float(row.get("spec_plys_upper", 0))
                spec_plys_lower = float(row.get("spec_plys_lower", 0))
                sizecode = str(row.get("sizecode", ""))
                spec_rfppcc = float(row.get("spec_rfppcc", 0))
                spec_rfppcw = float(row.get("spec_rfppcw", 0))
                spec_rfh1cc = float(row.get("spec_rfh1cc", 0))
                spec_rfh1cw = float(row.get("spec_rfh1cw", 0))
                spec_rfh2cc = float(row.get("spec_rfh2cc", 0))
                spec_rfh2cw = float(row.get("spec_rfh2cw", 0))
                spec_lfppcc = float(row.get("spec_lfppcc", 0))
                spec_lfppcw = float(row.get("spec_lfppcw", 0))
                spec_crro = float(row.get("spec_crro", 0))
                spec_trro = float(row.get("spec_trro", 0))
                spec_brro = float(row.get("spec_brro", 0))
                spec_lrot = float(row.get("spec_lrot", 0))
                spec_lrob = float(row.get("spec_lrob", 0))
                spec_lroh1grade = float(row.get("spec_lroh1grade", 0))
                spec_tbul = float(row.get("spec_tbul", 0))
                spec_bbul = float(row.get("spec_bbul", 0))
                spec_tdep = float(row.get("spec_tdep", 0))
                spec_bdep = float(row.get("spec_bdep", 0))
                spec_upper = float(row.get("spec_upper", 0))
                spec_lower = float(row.get("spec_lower", 0))
                spec_sbal = float(row.get("spec_sbal", 0))
                spec_aircc = float(row.get("spec_aircc", 0))
                spec_aircw = float(row.get("spec_aircw", 0))
                spec_loadcc = float(row.get("spec_yield", 0))
                spec_loadcw = float(row.get("spec_yield", 0))

                spec_rfppwc_yield = float(row.get("spec_rfppwc_yield", 0))
                spec_rfh1wc_yield = float(row.get("spec_rfh1wc_yield", 0))
                spec_rfh2wc_yield = float(row.get("spec_rfh2wc_yield", 0))
                spec_lfppwc_yield = float(row.get("spec_lfppwc_yield", 0))
                spec_cony_yield = float(row.get("spec_cony_yield", 0))
                spec_plys_yield = float(row.get("spec_plys_yield", 0))
                spec_crro_yield = float(row.get("spec_crro_yield", 0))
                spec_trro_yield = float(row.get("spec_trro_yield", 0))
                spec_brro_yield = float(row.get("spec_brro_yield", 0))
                spec_lro_yield = float(row.get("spec_lro_yield", 0))
                spec_lroh1_yield = float(row.get("spec_lroh1_yield", 0))
                spec_crroh1_yield = float(row.get("spec_crroh1_yield", 0))
                spec_trroh1_yield = float(row.get("spec_trroh1_yield", 0))
                spec_brroh1_yield = float(row.get("spec_brroh1_yield", 0))
                spec_lrro_yield = float(row.get("spec_lrro_yield", 0))
                spec_sbal_yield = float(row.get("spec_sbal_yield", 0))
                spec_couple_yield = float(row.get("spec_couple_yield", 0))
                spec_upper_yield = float(row.get("spec_upper_yield", 0))
                spec_lower_yield = float(row.get("spec_lower_yield", 0))
                spec_overall_uniformity_yield = float(row.get("spec_overall_uniformity_yield", 0))
                spec_overall_visual_yield = float(row.get("spec_overall_visual_yield", 0))
                spec_overall_warehouse_yield = float(row.get("spec_overall_warehouse_yield", 0))
                spec_overall_scrap_yield = float(row.get("spec_overall_scrap_yield", 0))
                spec_visual_loss_yield = float(row.get("spec_visual_loss_yield", 0))
                spec_uf_loss_yield = float(row.get("spec_uf_loss_yield", 0))
                spec_oe_ok_yield = float(row.get("spec_oe_ok_yield", 0))

                threshold_value_plys = float(row.get("threshold_value_plys", 0.5))
                threshold_value_cony = float(row.get("threshold_value_cony", 0.5))
                threshold_value_rfppcc = float(row.get("threshold_value_rfppcc", 0.5))
                threshold_value_rfppcw = float(row.get("threshold_value_rfppcw", 0.5))
                threshold_value_rfh1cc = float(row.get("threshold_value_rfh1cc", 0.5))
                threshold_value_rfh1cw = float(row.get("threshold_value_rfh1cw", 0.5))
                threshold_value_rfh2cc = float(row.get("threshold_value_rfh2cc", 0.5))
                threshold_value_rfh2cw = float(row.get("threshold_value_rfh2cw", 0.5))
                threshold_value_lfppcc = float(row.get("threshold_value_lfppcc", 0.5))
                threshold_value_lfppcw = float(row.get("threshold_value_lfppcw", 0.5))
                threshold_value_crro = float(row.get("threshold_value_crro", 5))
                threshold_value_trro = float(row.get("threshold_value_trro", 5))
                threshold_value_brro = float(row.get("threshold_value_brro", 5))
                threshold_value_lrot = float(row.get("threshold_value_lrot", 5))
                threshold_value_lrob = float(row.get("threshold_value_lrob", 5))
                threshold_value_lroh1grade = float(row.get("threshold_value_lroh1grade", 5))
                threshold_value_tbul = float(row.get("threshold_value_tbul", 0))
                threshold_value_bbul = float(row.get("threshold_value_bbul", 5))
                threshold_value_tdep = float(row.get("threshold_value_tdep", 5))
                threshold_value_bdep = float(row.get("threshold_value_bdep", 5))
                threshold_value_upper = float(row.get("threshold_value_upper", 5))
                threshold_value_lower = float(row.get("threshold_value_lower", 5))
                threshold_value_sbal = float(row.get("threshold_value_sbal", 5))
                threshold_value_aircc = float(row.get("threshold_value_aircc", 5))
                threshold_value_aircw = float(row.get("threshold_value_aircw", 5))
                threshold_value_loadcc = float(row.get("threshold_value_loadcc", 5))
                threshold_value_loadcw = float(row.get("threshold_value_loadcw", 5))
                threshold_value_rfppwc_yield = float(row.get("threshold_value_rfppwc_yield", 5))
                threshold_value_rfh1wc_yield = float(row.get("threshold_value_rfh1wc_yield", 5))
                threshold_value_rfh2wc_yield = float(row.get("threshold_value_rfh2wc_yield", 5))
                threshold_value_lfppwc_yield = float(row.get("threshold_value_lfppwc_yield", 5))
                threshold_value_cony_yield = float(row.get("threshold_value_cony_yield", 5))
                threshold_value_plys_yield = float(row.get("threshold_value_plys_yield", 5))
                threshold_value_crro_yield = float(row.get("threshold_value_crro_yield", 5))
                threshold_value_trro_yield = float(row.get("threshold_value_trro_yield", 5))
                threshold_value_brro_yield = float(row.get("threshold_value_brro_yield", 5))
                threshold_value_lro_yield = float(row.get("threshold_value_lro_yield", 5))
                threshold_value_lroh1_yield = float(row.get("threshold_value_lroh1_yield", 5))
                threshold_value_crroh1_yield = float(row.get("threshold_value_crroh1_yield", 5))
                threshold_value_trroh1_yield = float(row.get("threshold_value_trroh1_yield", 5))
                threshold_value_brroh1_yield = float(row.get("threshold_value_brroh1_yield", 5))
                threshold_value_lrro_yield = float(row.get("threshold_value_lrro_yield", 5))
                threshold_value_sbal_yield = float(row.get("threshold_value_sbal_yield", 5))
                threshold_value_couple_yield = float(row.get("threshold_value_couple_yield", 5))
                threshold_value_upper_yield = float(row.get("threshold_value_upper_yield", 5))
                threshold_value_lower_yield = float(row.get("threshold_value_lower_yield", 5))
                threshold_value_overall_uniformity_yield = float(row.get("threshold_value_overall_uniformity_yield", 5))
                threshold_value_overall_visual_yield = float(row.get("threshold_value_overall_visual_yield", 5))
                threshold_value_overall_warehouse_yield = float(row.get("threshold_value_overall_warehouse_yield", 5))
                threshold_value_overall_scrap_yield = float(row.get("threshold_value_overall_scrap_yield", 5))
                threshold_value_visual_loss_yield = float(row.get("threshold_value_visual_loss_yield", 5))
                threshold_value_uf_loss_yield = float(row.get("threshold_value_uf_loss_yield", 5))
                threshold_value_oe_ok_yield = float(row.get("threshold_value_oe_ok_yield", 5))

                if model_data.empty:
                    logging.info(f"Skipping UFM model {model_number} gt_code {gt_code}: No data available.")
                    output_messages.append(f"Skipping UFM model {model_number} gt_code {gt_code}: No data available.")
                    continue

                # ============== FIX: account for barcodes already banked in the
                # incomplete_lots_tracker for this SKU. The old code skipped the
                # whole model whenever THIS run's barcodes alone were < lot_size,
                # which prevented incomplete lots from being topped up and never
                # rolled over into a new incremented lot. We now let processing
                # continue when either (a) enough NEW barcodes arrived, or
                # (b) new + already-banked barcodes reach the target, or
                # (c) there are new barcodes we can bank as/append to an
                #     incomplete lot for a later top-up.
                total_barcodes_model_processable = len(model_data)

                # Sum banked (incomplete) barcodes across all machines of this SKU.
                banked_incomplete_barcodes = 0
                for _key, _lots in incomplete_lots_dict.items():
                    if _key[0] == model_number:
                        for _lot in _lots:
                            banked_incomplete_barcodes += int(_lot.get("barcode_count", 0) or 0)

                combined_available = total_barcodes_model_processable + banked_incomplete_barcodes

                # Only skip when there is genuinely nothing to do: no new barcodes
                # this run AND nothing banked to continue. Otherwise fall through so
                # the per-machine logic can top up incomplete lots and/or bank new
                # barcodes for the next run.
                if total_barcodes_model_processable == 0 and banked_incomplete_barcodes == 0:
                    logging.info(f"Skipping UFM model {model_number} gt_code {gt_code}: No barcodes to process.")
                    output_messages.append(f"Skipping UFM model {model_number} gt_code {gt_code}: No barcodes to process.")
                    continue

                if combined_available < target_lot_size:
                    # Not enough yet to complete a full lot, but we still process so
                    # the new barcodes get banked/appended to an incomplete lot
                    # (they must NOT be dropped).
                    logging.info(
                        f"UFM model {model_number} gt_code {gt_code}: partial fill "
                        f"({total_barcodes_model_processable} new + {banked_incomplete_barcodes} banked "
                        f"= {combined_available} < {target_lot_size}). Banking barcodes for later top-up."
                    )
                    output_messages.append(
                        f"UFM model {model_number} gt_code {gt_code}: partial fill "
                        f"({combined_available} < {target_lot_size}). Banking barcodes for later top-up."
                    )
                
                # ==================== VIRGIN/FINAL YIELD PRE-PROCESSING ====================
                model_data_full = model_data.copy()
                model_data_full["date_of_dt"] = pd.to_datetime(model_data_full["date_and_time"], errors='coerce')

                # Virgin Yield: FIRST occurrence of each barcode
                virgin_data = model_data_full.sort_values("date_of_dt").drop_duplicates(subset="barcode", keep="first").copy()
                virgin_data.sort_values("date_of_dt", inplace=True)

                # Final Yield: LAST occurrence of each barcode
                final_data = model_data_full.sort_values("date_of_dt").drop_duplicates(subset="barcode", keep="last").copy()
                final_data.sort_values("date_of_dt", inplace=True)
                
                # ==================== MAIN LOT PROCESSING ====================
                # Use VIRGIN data for main lot processing
                model_data = virgin_data.copy()

                # Sort by UFM date
                model_data.sort_values("date_of_ufm", ascending=True, inplace=True)

                # ============== FIX: Use a dictionary to track alerts by lot_id to avoid duplicates ==============
                alert_map = {}

                for ufm_machine_id, ufm_machine_data in model_data.groupby("uf_id"):
                    try:
                        machine_no = str(ufm_machine_id)
                        
                        # ============== FIX: Filter out already processed barcodes ==============
                        machine_model_key = (model_number, machine_no)
                        if machine_model_key not in processed_barcodes_per_machine_model:
                            processed_barcodes_per_machine_model[machine_model_key] = set()
                        
                        # Filter out barcodes that have already been processed
                        processed_barcodes = processed_barcodes_per_machine_model[machine_model_key]
                        ufm_machine_data = ufm_machine_data[~ufm_machine_data["barcode"].isin(processed_barcodes)].copy()
                        
                        # Also filter out already assigned rows
                        ufm_machine_data = ufm_machine_data[ufm_machine_data["ufm_machine_model_lot_id"].isna()].copy()
                        if ufm_machine_data.empty:
                            logging.info(f"Skipping UFM machine {machine_no} for model {model_number}: No unprocessed data.")
                            continue
                        
                        tbm_machine_value = ufm_machine_data["tbm_machine"].iloc[0] if "tbm_machine" in ufm_machine_data.columns and not ufm_machine_data["tbm_machine"].isna().all() else None
                        output_messages.append(f"DEBUG: Model {model_number}, Machine {machine_no}, tbm_machine: {tbm_machine_value}.")

                        machine_config_row = ub_machine_df[ub_machine_df["ubno"] == machine_no]
                        if machine_config_row.empty:
                            logging.info(f"Skipping UFM machine {machine_no} for model {model_number}: Unknown machine config.")
                            output_messages.append(f"Skipping UFM machine {machine_no} for model {model_number}: Unknown machine config.")
                            continue

                        total_ufm_machine_barcodes = len(ufm_machine_data)
                        
                        # ============== FIX: Check if there's an incomplete lot for this machine-sku ==============
                        incomplete_lot_key = f"{machine_model_key}_incomplete"
                        incomplete_barcodes_count = 0
                        incomplete_lot_number = None
                        incomplete_ufm_id = None
                        incomplete_batch_data = None
                        
                        # Check if we have an incomplete lot from database
                        if machine_model_key in incomplete_lots_dict and incomplete_lots_dict[machine_model_key]:
                            incomplete_lot_data = incomplete_lots_dict[machine_model_key][0]  # Take the first incomplete lot
                            incomplete_barcodes_count = incomplete_lot_data.get("barcode_count", 0)
                            incomplete_lot_number = incomplete_lot_data.get("lot_number", 0)
                            incomplete_ufm_id = incomplete_lot_data.get("ufm_machine_model_lot_id")
                            incomplete_batch_data = incomplete_lot_data.get("batch_data", [])
                            logging.info(f"Found incomplete lot from database: {incomplete_lot_data.get('lot_id')} with {incomplete_barcodes_count} barcodes")
                        
                        # If we have an incomplete lot, we need to fill it first
                        if incomplete_barcodes_count > 0 and incomplete_lot_number is not None:
                            # We have an incomplete lot, fill it with available barcodes
                            barcodes_needed = target_lot_size - incomplete_barcodes_count
                            
                            if total_ufm_machine_barcodes >= barcodes_needed:
                                # We have enough barcodes to complete the lot
                                logging.info(f"Completing incomplete lot {incomplete_lot_data.get('lot_id')} with {barcodes_needed} barcodes")
                                
                                # Take the first barcodes_needed barcodes to complete the lot
                                completion_data = ufm_machine_data.iloc[:barcodes_needed].copy()
                                completion_indices = completion_data.index
                                
                                # Combine previous barcodes with new ones
                                previous_barcodes = incomplete_batch_data if incomplete_batch_data else []
                                all_batch_data = pd.DataFrame(previous_barcodes) if previous_barcodes else pd.DataFrame()
                                if not all_batch_data.empty:
                                    all_batch_data = pd.concat([all_batch_data, completion_data], ignore_index=True)
                                else:
                                    all_batch_data = completion_data
                                
                                # Update the lot ID for these barcodes
                                lot_id = incomplete_lot_data.get("lot_id")
                                previous_lot_id = incomplete_lot_data.get("previous_lot_id")
                                ufm_previous_machine_model_lot_id = incomplete_lot_data.get("ufm_previous_machine_model_lot_id")
                                
                                merge_df.loc[completion_indices, "lot_id"] = lot_id
                                merge_df.loc[completion_indices, "previous_lot_id"] = previous_lot_id
                                merge_df.loc[completion_indices, "ufm_machine_model_lot_id"] = incomplete_ufm_id
                                merge_df.loc[completion_indices, "ufm_previous_machine_model_lot_id"] = ufm_previous_machine_model_lot_id
                                
                                # Mark these barcodes as processed
                                processed_barcodes_per_machine_model[machine_model_key].update(set(completion_data["barcode"].astype(str)))
                                
                                # Process the full lot
                                current_model_lot_id = lot_id
                                current_ufm_machine_lot_id = incomplete_ufm_id
                                current_last_lot_number = incomplete_lot_number
                                model_level_previous_lot_id = previous_lot_id
                                machine_batch_data = all_batch_data
                                machine_batch_indices = completion_indices
                                
                                # Remove the incomplete lot from database
                                incomplete_lots_to_delete.append(incomplete_lot_data.get("_id"))
                                
                                # Process the full lot (generate alerts, averages, etc.)
                                # Get previous lot averages
                                prev_lot_avg = None
                                if ufm_previous_machine_model_lot_id is not None:
                                    prev_avg_key = (model_number, machine_no, ufm_previous_machine_model_lot_id)
                                    prev_lot_avg = ufm_machine_lot_avg_map.get(prev_avg_key)
                                    if prev_lot_avg is None:
                                        try:
                                            prev_lot_doc = averages_ufm_collect.find_one(
                                                {
                                                    "sku": model_number,
                                                    "machine": machine_no,
                                                    "ufm_machine_model_lot_id": ufm_previous_machine_model_lot_id
                                                },
                                                {"current_lot_averages": 1}
                                            )
                                            prev_lot_avg = prev_lot_doc.get("current_lot_averages", {}) if prev_lot_doc else {}
                                            if prev_lot_avg:
                                                ufm_machine_lot_avg_map[prev_avg_key] = prev_lot_avg
                                        except Exception as e:
                                            logging.error(f"Error fetching previous lot averages for {ufm_previous_machine_model_lot_id}: {e}")
                                            prev_lot_avg = {}
                                else:
                                    prev_lot_avg = {}

                                # Calculate lot averages
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
                                    "rfv_yield": rfv_yield(machine_batch_data),
                                    "rfh1_yield": rfh1_yield(machine_batch_data),
                                    "rfh2_yield": rfh2_yield(machine_batch_data),
                                    "lfv_yield": lfv_yield(machine_batch_data),
                                    "conicity_yield": conicity_yield(machine_batch_data),
                                    "plysteer_yield": plysteer_yield(machine_batch_data),
                                    "center_radial_run_out_yield": center_radial_run_out_yield(machine_batch_data),
                                    "top_radial_run_out_yield": top_radial_run_out_yield(machine_batch_data),
                                    "bottom_radial_run_out_yield": bottom_radial_run_out_yield(machine_batch_data),
                                    "lateral_run_out_yield": lateral_run_out_yield(machine_batch_data),
                                    "lateral_run_out_harmonic_first_yield": lateral_run_out_harmonic_first_yield(machine_batch_data),
                                    "center_radial_run_out_harmonic_first_yield": center_radial_run_out_harmonic_first_yield(machine_batch_data),
                                    "top_radial_run_out_harmonic_first_yield": top_radial_run_out_harmonic_first_yield(machine_batch_data),
                                    "bottom_radial_run_out_harmonic_first_yield": bottom_radial_run_out_harmonic_first_yield(machine_batch_data),
                                    "lateral_radial_run_out_yield": lateral_radial_run_out_yield(machine_batch_data),
                                    "static_yield": static_yield(machine_batch_data),
                                    "couple_yield": couple_yield(machine_batch_data),
                                    "upper_yield": upper_yield(machine_batch_data),
                                    "lower_yield": lower_yield(machine_batch_data),
                                    "overall_uniformity_yield": overall_uniformity_yield(machine_batch_data),
                                    "overall_visual_yield": overall_visual_yield(machine_batch_data),
                                    "overall_warehouse_yield": overall_warehouse_yield(machine_batch_data),
                                    "overall_scrap_yield": overall_scrap_yield(machine_batch_data),
                                    "visual_loss_yield": visual_loss_yield(machine_batch_data),
                                    "uf_loss_yield": uf_loss_yield(machine_batch_data),
                                    "oe_ok_yield": oe_ok_yield(machine_batch_data)
                                })

                                # Perform Shift Detection (same as before - keep the existing logic)
                                parameter_alerts = []
                                machine_lot_overall_shift_occurred = False
                                yield_spec_mapping = {
                                    "rfv_yield": spec_rfppwc_yield,
                                    "rfh1_yield": spec_rfh1wc_yield,
                                    "rfh2_yield": spec_rfh2wc_yield,
                                    "lfv_yield": spec_lfppwc_yield,
                                    "conicity_yield": spec_cony_yield,
                                    "plysteer_yield": spec_plys_yield,
                                    "center_radial_run_out_yield": spec_crro_yield,
                                    "top_radial_run_out_yield": spec_trro_yield,
                                    "bottom_radial_run_out_yield": spec_brro_yield,
                                    "lateral_run_out_yield": spec_lro_yield,
                                    "lateral_run_out_harmonic_first_yield": spec_lroh1_yield,
                                    "center_radial_run_out_harmonic_first_yield": spec_crroh1_yield,
                                    "top_radial_run_out_harmonic_first_yield": spec_trroh1_yield,
                                    "bottom_radial_run_out_harmonic_first_yield": spec_brroh1_yield,
                                    "lateral_radial_run_out_yield": spec_lrro_yield,
                                    "static_yield": spec_sbal_yield,
                                    "couple_yield": spec_couple_yield,
                                    "upper_yield": spec_upper_yield,
                                    "lower_yield": spec_lower_yield,
                                    "overall_uniformity_yield": spec_overall_uniformity_yield,
                                    "overall_visual_yield": spec_overall_visual_yield,
                                    "overall_warehouse_yield": spec_overall_warehouse_yield,
                                    "overall_scrap_yield": spec_overall_scrap_yield,
                                    "visual_loss_yield": spec_visual_loss_yield,
                                    "uf_loss_yield": spec_uf_loss_yield,
                                    "oe_ok_yield": spec_oe_ok_yield
                                }

                                # Shift detection logic (same as before)
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
                                            parameter_alerts.append(alert_dict)
                                            continue

                                        shift_occurred_thershold = False
                                        shift_occurred_spec = False
                                        spec_val = None

                                        if param == "cony" and spec_cony_lower is not None and spec_cony_upper is not None:
                                            shift_occurred_thershold = abs(current_val - prev_val) > threshold_value_cony
                                            shift_occurred_spec = not (-spec_cony_lower <= current_val <= spec_cony_upper)
                                            spec_val = spec_cony_upper
                                        elif param == "plys" and spec_plys_lower is not None and spec_plys_upper is not None:
                                            shift_occurred_thershold = abs(current_val - prev_val) > threshold_value_plys
                                            shift_occurred_spec = not (-spec_plys_lower <= current_val <= spec_plys_upper)
                                            spec_val = spec_plys_upper
                                        elif param in yield_params:
                                            spec_yield = yield_spec_mapping.get(param, 0)
                                            threshold_key = f"threshold_value_{param}"
                                            threshold_val = globals().get(threshold_key, 5)
                                            shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_val)
                                            shift_occurred_spec = current_val < spec_yield
                                            spec_val = spec_yield

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
                                            parameter_alerts.append(alert_dict)
                                            continue

                                        shift_occurred_thershold = False
                                        shift_occurred_spec = False
                                        spec_val = None

                                        if param == "cony" and spec_cony_lower is not None and spec_cony_upper is not None:
                                            shift_occurred_spec = not (spec_cony_lower <= current_val <= spec_cony_upper)
                                            spec_val = spec_cony_upper
                                        elif param == "plys" and spec_plys_lower is not None and spec_plys_upper is not None:
                                            shift_occurred_spec = not (spec_plys_lower <= current_val <= spec_plys_upper)
                                            spec_val = spec_plys_upper
                                        elif param in yield_params:
                                            spec_yield = yield_spec_mapping.get(param, 0)
                                            shift_occurred_spec = current_val < spec_yield
                                            spec_val = spec_yield

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
                                        parameter_alerts.append(alert_dict)

                                machine_lot_overall_shift_occurred = bool(machine_lot_overall_shift_occurred)

                                # Deviated parameter alerts
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
                                        "sku": model_number,
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

                                # Prepare averages document
                                averages_doc_data = {
                                    "_id": f"avg_{current_ufm_machine_lot_id}",
                                    "sku": model_number,
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
                                
                                # Insert averages
                                ufm_averages_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(averages_doc_data)))
                                output_messages.append(f"Prepared averages for {current_ufm_machine_lot_id} for insertion into averages_ufm_collect")

                                # Update maps for next lot
                                current_avg_key = (model_number, machine_no, current_ufm_machine_lot_id)
                                ufm_machine_lot_avg_map[current_avg_key] = lot_averages
                                ufm_previous_machine_model_lot_ids[machine_model_key] = current_ufm_machine_lot_id

                                # Increment alert ID
                                last_alert_id += 1
                                merge_df.loc[machine_batch_indices, "alert_id_ufm_ufm"] = last_alert_id

                                # Build shift data (existing logic)
                                machine_batch_data["tbm_shift"] = machine_batch_data["date_of_tbm"].apply(get_shift)
                                machine_batch_data = machine_batch_data.sort_values("date_of_tbm").reset_index(drop=True)

                                shifts_data_list_tbm = []
                                current_shift = None
                                current_start = None
                                current_barcodes = []

                                for idx, row in machine_batch_data.iterrows():
                                    this_shift = row["tbm_shift"]
                                    this_dt = row["date_of_tbm"]
                                    this_bc = row.get("barcode")

                                    if this_shift != current_shift or current_shift is None:
                                        if current_shift is not None:
                                            shifts_data_list_tbm.append({
                                                "shift": current_shift,
                                                "barcode_count": len(current_barcodes),
                                                "machine_shift_tbm_start_time": current_start,
                                                "machine_shift_tbm_end_time": machine_batch_data.at[idx-1, "date_of_tbm"],
                                                "barcodes": current_barcodes[:]
                                            })
                                        current_shift = this_shift
                                        current_start = this_dt
                                        current_barcodes = [this_bc] if this_bc is not None else []
                                    else:
                                        current_barcodes.append(this_bc if this_bc is not None else None)

                                if current_shift is not None:
                                    shifts_data_list_tbm.append({
                                        "shift": current_shift,
                                        "barcode_count": len(current_barcodes),
                                        "machine_shift_tbm_start_time": current_start,
                                        "machine_shift_tbm_end_time": machine_batch_data["date_of_tbm"].iloc[-1],
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
                                    "tbm_machine": tbm_machine_value,
                                    "ufm_shift_occured": machine_lot_overall_shift_occurred,
                                    "shifts_data": shifts_data_list,
                                    "shifts_data_tbm": shifts_data_list_tbm,
                                    "overall_uniformity_yield": overall_uniformity_yield(machine_batch_data),
                                    "overall_warehouse_yield": overall_warehouse_yield(machine_batch_data),
                                    "overall_visual_yield": overall_visual_yield(machine_batch_data),
                                    "overall_scrap_yield": overall_scrap_yield(machine_batch_data),
                                    "visual_loss_yield": visual_loss_yield(machine_batch_data),
                                    "uf_loss_yield": uf_loss_yield(machine_batch_data),
                                    "oe_ok_yield": oe_ok_yield(machine_batch_data),
                                    "rfv_yield": rfv_yield(machine_batch_data),
                                    "rfh1_yield": rfh1_yield(machine_batch_data),
                                    "rfh2_yield": rfh2_yield(machine_batch_data),
                                    "lfv_yield": lfv_yield(machine_batch_data),
                                    "plysteer_yield": plysteer_yield(machine_batch_data),
                                    "sizecode": sizecode, 
                                    "gt_code": str(gt_code),
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
                                    "barcodes": machine_batch_data["barcode"].tolist() if "barcode" in machine_batch_data.columns else [],
                                    "cur_ids": (
                                        [
                                            {"cur_id": cur_id, "barcode": int(count)}
                                            for cur_id, count in machine_batch_data.groupby("cur_id")["barcode"].count().items()
                                        ]
                                        if "cur_id" in machine_batch_data.columns and "barcode" in machine_batch_data.columns
                                        else []
                                    ),
                                    "tbm_ids": (
                                        [
                                            {"tbm_id": ptb_id, "barcode": int(count)}
                                            for ptb_id, count in machine_batch_data.groupby("ptb_id")["barcode"].count().items()
                                        ]
                                        if "ptb_id" in machine_batch_data.columns and "barcode" in machine_batch_data.columns
                                        else []
                                    )
                                }

                                # Create alert documents
                                alert_doc_main = {
                                    "_id": f"main_alert_{last_alert_id}",
                                    "alert_id_ufm_ufm": last_alert_id,
                                    "sku": model_number,
                                    "lot_id": current_model_lot_id,
                                    "alert_flag": False,
                                    "dashboard_flag": False,
                                    "alert_status_check": "o",
                                    "alert_status_timestamp": None,
                                    "alert_status_comment": [],
                                    "alert_assign_user": [],
                                    "machine_alerts": [machine_alert_detail],
                                    "first_barcode_model": machine_alert_detail["first_barcode_machine"],
                                    "last_barcode_model": machine_alert_detail["last_barcode_machine"],
                                    "timestamp_alert_generated": some_function()
                                }

                                # Store in alert_map with lot_id as key
                                if current_model_lot_id not in alert_map:
                                    alert_map[current_model_lot_id] = {
                                        "alert_id_ufm_ufm": last_alert_id,
                                        "sku": model_number,
                                        "lot_id": current_model_lot_id,
                                        "machine_alerts": []
                                    }
                                
                                alert_map[current_model_lot_id]["machine_alerts"].append(machine_alert_detail.copy())

                                trigger_doc = {
                                    "_id": f"trigger_alert_{last_alert_id}",
                                    "alert_id_ufm_ufm": last_alert_id,
                                    "sku": model_number,
                                    "lot_id": current_model_lot_id,
                                    "type": "ufm_trigger",
                                    "timestamp_alert_generated": some_function()
                                }

                                # Insert main alerts
                                if not alert_collect.find_one({"alert_id_ufm_ufm": last_alert_id}):
                                    try:
                                        alert_collect.insert_one(convert_numpy_types(alert_doc_main))
                                        logging.info(f"Inserted alert_doc_main into alert_table for alert_id {last_alert_id}")
                                        output_messages.append(f"Inserted into alert_table: alert_id {last_alert_id}")
                                    except Exception as e:
                                        logging.error(f"Failed to insert into alert_table: {e}")

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

                                if machine_lot_overall_shift_occurred:
                                    if not alert_ufm_collect.find_one({"alert_id_ufm_ufm": last_alert_id}):
                                        ufm_alert_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(alert_doc_main)))
                                        ufm_triggering_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(trigger_doc)))
                                        ufm_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(trigger_doc)))
                                        logging.info(f"Shift detected - added to bulk ops for alert_id {last_alert_id}")
                                        output_messages.append(f"Shift alert queued for bulk: alert_id {last_alert_id}")

                                # Insert Lot document into MongoDB (NOW IT'S COMPLETE!)
                                lot_doc_data = {
                                    "_id": f"lot_{current_ufm_machine_lot_id}",
                                    "sku": model_number,
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
                                
                                ufm_lot_bulk_operations.append(pymongo.UpdateOne(
                                    {"ufm_machine_model_lot_id": current_ufm_machine_lot_id},
                                    {"$setOnInsert": convert_numpy_types(lot_doc_data)},
                                    upsert=True
                                ))
                                output_messages.append(f"Prepared complete lot_id {current_model_lot_id} for insertion into lot_ufm_collect")

                                # Remove the completed barcodes from ufm_machine_data
                                ufm_machine_data = ufm_machine_data.iloc[barcodes_needed:].copy()
                                total_ufm_machine_barcodes = len(ufm_machine_data)
                                machine_barcode_counter = 0
                                
                                # Update last_lot_dict for future lots
                                last_lot_dict[machine_model_key] = {
                                    "max_lot_number": current_last_lot_number,
                                    "last_lot_id": current_model_lot_id,
                                    "last_ufm_machine_model_lot_id": current_ufm_machine_lot_id
                                }
                                
                                # Continue to process remaining barcodes as new lots
                                if total_ufm_machine_barcodes == 0:
                                    break
                                # If there are remaining barcodes, continue to next lot
                                continue
                                
                            else:
                                # Not enough barcodes to complete the lot, add them to the incomplete lot
                                logging.info(f"Adding {total_ufm_machine_barcodes} more barcodes to incomplete lot {incomplete_lot_data.get('lot_id')}")
                                
                                # Add all barcodes to the incomplete lot
                                completion_data = ufm_machine_data.copy()
                                completion_indices = completion_data.index
                                
                                # Update the lot ID for these barcodes
                                merge_df.loc[completion_indices, "lot_id"] = incomplete_lot_data.get("lot_id")
                                merge_df.loc[completion_indices, "previous_lot_id"] = incomplete_lot_data.get("previous_lot_id")
                                merge_df.loc[completion_indices, "ufm_machine_model_lot_id"] = incomplete_ufm_id
                                merge_df.loc[completion_indices, "ufm_previous_machine_model_lot_id"] = incomplete_lot_data.get("ufm_previous_machine_model_lot_id")
                                
                                # Mark these barcodes as processed
                                processed_barcodes_per_machine_model[machine_model_key].update(set(completion_data["barcode"].astype(str)))
                                
                                # Update the incomplete lot data
                                previous_barcodes = incomplete_batch_data if incomplete_batch_data else []
                                all_batch_data = pd.DataFrame(previous_barcodes) if previous_barcodes else pd.DataFrame()
                                if not all_batch_data.empty:
                                    all_batch_data = pd.concat([all_batch_data, completion_data], ignore_index=True)
                                else:
                                    all_batch_data = completion_data
                                
                                # Update incomplete lot in database
                                incomplete_lots_to_update.append({
                                    "_id": incomplete_lot_data.get("_id"),
                                    "sku": model_number,
                                    "machine": machine_no,
                                    "lot_id": incomplete_lot_data.get("lot_id"),
                                    "lot_number": incomplete_lot_number,
                                    "previous_lot_id": incomplete_lot_data.get("previous_lot_id"),
                                    "ufm_machine_model_lot_id": incomplete_ufm_id,
                                    "ufm_previous_machine_model_lot_id": incomplete_lot_data.get("ufm_previous_machine_model_lot_id"),
                                    "barcode_count": incomplete_barcodes_count + total_ufm_machine_barcodes,
                                    "batch_data": all_batch_data.to_dict('records')
                                })
                                
                                logging.info(f"Incomplete lot now has {incomplete_barcodes_count + total_ufm_machine_barcodes} barcodes out of {target_lot_size}")
                                
                                # No more barcodes to process for this machine-sku
                                break

                        # ============== FIX: Get the current max lot number from last_lot_dict ==============
                        if machine_model_key not in last_lot_dict:
                            last_lot_dict[machine_model_key] = {
                                "max_lot_number": 0,
                                "last_lot_id": None,
                                "last_ufm_machine_model_lot_id": None
                            }
                            logging.info(f"No existing lots found for Machine-SKU: {machine_model_key}. Starting from 0.")
                        else:
                            logging.info(f"Found existing lots for Machine-SKU: {machine_model_key}. Max lot number: {last_lot_dict[machine_model_key]['max_lot_number']}")

                        # If we already processed all barcodes (either completed a lot or added to incomplete), continue
                        if total_ufm_machine_barcodes == 0:
                            continue

                        # Process remaining barcodes as new lots
                        machine_barcode_counter = 0

                        while machine_barcode_counter < total_ufm_machine_barcodes:
                            if (total_ufm_machine_barcodes - machine_barcode_counter) < target_lot_size:
                                # Not enough barcodes for a full lot - store as incomplete
                                remaining_barcodes = ufm_machine_data.iloc[machine_barcode_counter:].copy()
                                current_last_lot_number = last_lot_dict[machine_model_key]["max_lot_number"] + 1
                                current_model_lot_id = f"UFM_{machine_no}_{model_number}_{current_last_lot_number}"
                                current_ufm_machine_lot_id = f"{machine_no}_{model_number}_UFM_{machine_no}_{model_number}_{current_last_lot_number}_{machine_barcode_counter + len(remaining_barcodes)}"
                                
                                if current_last_lot_number == 1:
                                    ufm_previous_machine_model_lot_id = None
                                    model_level_previous_lot_id = None
                                else:
                                    ufm_previous_machine_model_lot_id = last_lot_dict[machine_model_key].get("last_ufm_machine_model_lot_id")
                                    model_level_previous_lot_id = f"UFM_{machine_no}_{model_number}_{current_last_lot_number - 1}"
                                
                                # Assign IDs to these barcodes (they will remain incomplete)
                                remaining_indices = remaining_barcodes.index
                                merge_df.loc[remaining_indices, "lot_id"] = current_model_lot_id
                                merge_df.loc[remaining_indices, "previous_lot_id"] = model_level_previous_lot_id
                                merge_df.loc[remaining_indices, "ufm_machine_model_lot_id"] = current_ufm_machine_lot_id
                                merge_df.loc[remaining_indices, "ufm_previous_machine_model_lot_id"] = ufm_previous_machine_model_lot_id
                                
                                # Mark as processed
                                processed_barcodes_per_machine_model[machine_model_key].update(set(remaining_barcodes["barcode"].astype(str)))
                                
                                # Store incomplete lot in database (to be continued in next run)
                                incomplete_lot_doc = {
                                    "sku": model_number,
                                    "machine": machine_no,
                                    "lot_id": current_model_lot_id,
                                    "lot_number": current_last_lot_number,
                                    "previous_lot_id": model_level_previous_lot_id,
                                    "ufm_machine_model_lot_id": current_ufm_machine_lot_id,
                                    "ufm_previous_machine_model_lot_id": ufm_previous_machine_model_lot_id,
                                    "barcode_count": len(remaining_barcodes),
                                    "batch_data": remaining_barcodes.to_dict('records'),
                                    "target_lot_size": target_lot_size,
                                    "timestamp_created": some_function()
                                }
                                
                                # Check if this incomplete lot already exists
                                existing_incomplete = incomplete_lots_collect.find_one({
                                    "sku": model_number,
                                    "machine": machine_no,
                                    "lot_id": current_model_lot_id
                                })
                                
                                if existing_incomplete:
                                    # Update existing
                                    incomplete_lots_to_update.append({
                                        "_id": existing_incomplete["_id"],
                                        **incomplete_lot_doc
                                    })
                                else:
                                    # Insert new
                                    incomplete_lots_collect.insert_one(convert_numpy_types(incomplete_lot_doc))
                                
                                logging.info(f"Stored incomplete lot {current_model_lot_id} with {len(remaining_barcodes)} barcodes for {machine_model_key}")
                                break

                            # Process full lot (same as before)
                            current_last_lot_number = last_lot_dict[machine_model_key]["max_lot_number"] + 1
                            current_model_lot_id = f"UFM_{machine_no}_{model_number}_{current_last_lot_number}"
                            current_ufm_machine_lot_id = f"{machine_no}_{model_number}_UFM_{machine_no}_{model_number}_{current_last_lot_number}_{machine_barcode_counter + target_lot_size}"
                            
                            if current_last_lot_number == 1:
                                ufm_previous_machine_model_lot_id = None
                                model_level_previous_lot_id = None
                            else:
                                ufm_previous_machine_model_lot_id = last_lot_dict[machine_model_key].get("last_ufm_machine_model_lot_id")
                                if ufm_previous_machine_model_lot_id is None:
                                    prev_lot_number = current_last_lot_number - 1
                                    prev_lot_id = f"UFM_{machine_no}_{model_number}_{prev_lot_number}"
                                    try:
                                        prev_lot = averages_ufm_collect.find_one(
                                            {
                                                "sku": model_number,
                                                "machine": machine_no,
                                                "lot_id": prev_lot_id
                                            },
                                            {"ufm_machine_model_lot_id": 1}
                                        )
                                        if prev_lot:
                                            ufm_previous_machine_model_lot_id = prev_lot["ufm_machine_model_lot_id"]
                                    except Exception as e:
                                        logging.error(f"Error fetching previous ufm_machine_model_lot_id for lot {prev_lot_number}: {e}")
                                        ufm_previous_machine_model_lot_id = None
                                model_level_previous_lot_id = f"UFM_{machine_no}_{model_number}_{current_last_lot_number - 1}"

                            # Update last_lot_dict
                            last_lot_dict[machine_model_key] = {
                                "max_lot_number": current_last_lot_number,
                                "last_lot_id": current_model_lot_id,
                                "last_ufm_machine_model_lot_id": current_ufm_machine_lot_id
                            }
                            
                            logging.info(f"Generated new lot: {current_model_lot_id} for Machine-SKU: {machine_model_key}")

                            # Validate IDs
                            if not re.match(r"^UFM_.+_\d+$", current_model_lot_id):
                                logging.error(f"Invalid lot_id format: {current_model_lot_id}")
                                continue
                            if not re.match(r"^.+_UFM_.+_\d+_\d+$", current_ufm_machine_lot_id):
                                logging.error(f"Invalid ufm_machine_model_lot_id format: {current_ufm_machine_lot_id}")
                                continue

                            sub_batch_start_index = machine_barcode_counter
                            sub_batch_end_index = machine_barcode_counter + target_lot_size
                            machine_batch_data = ufm_machine_data.iloc[sub_batch_start_index:sub_batch_end_index].copy()
                            machine_batch_indices = machine_batch_data.index

                            # Mark barcodes as processed
                            processed_barcodes_per_machine_model[machine_model_key].update(set(machine_batch_data["barcode"].astype(str)))

                            # Assign values to merge_df
                            merge_df.loc[machine_batch_indices, "lot_id"] = current_model_lot_id
                            merge_df.loc[machine_batch_indices, "previous_lot_id"] = model_level_previous_lot_id
                            merge_df.loc[machine_batch_indices, "ufm_machine_model_lot_id"] = current_ufm_machine_lot_id
                            merge_df.loc[machine_batch_indices, "ufm_previous_machine_model_lot_id"] = ufm_previous_machine_model_lot_id

                            # Fetch previous lot averages
                            prev_lot_avg = None
                            if ufm_previous_machine_model_lot_id is not None:
                                prev_avg_key = (model_number, machine_no, ufm_previous_machine_model_lot_id)
                                prev_lot_avg = ufm_machine_lot_avg_map.get(prev_avg_key)
                                if prev_lot_avg is None:
                                    try:
                                        prev_lot_doc = averages_ufm_collect.find_one(
                                            {
                                                "sku": model_number,
                                                "machine": machine_no,
                                                "ufm_machine_model_lot_id": ufm_previous_machine_model_lot_id
                                            },
                                            {"current_lot_averages": 1}
                                        )
                                        prev_lot_avg = prev_lot_doc.get("current_lot_averages", {}) if prev_lot_doc else {}
                                        if prev_lot_avg:
                                            ufm_machine_lot_avg_map[prev_avg_key] = prev_lot_avg
                                    except Exception as e:
                                        logging.error(f"Error fetching previous lot averages for {ufm_previous_machine_model_lot_id}: {e}")
                                        prev_lot_avg = {}
                            else:
                                prev_lot_avg = {}
                                logging.info(f"First lot for model {model_number}, machine {machine_no}. Setting previous_lot_averages to empty.")

                            logging.info(f"Lot {current_model_lot_id} - Previous UFM ID: {ufm_previous_machine_model_lot_id}")

                            # Calculate lot averages
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
                                "rfv_yield": rfv_yield(machine_batch_data),
                                "rfh1_yield": rfh1_yield(machine_batch_data),
                                "rfh2_yield": rfh2_yield(machine_batch_data),
                                "lfv_yield": lfv_yield(machine_batch_data),
                                "conicity_yield": conicity_yield(machine_batch_data),
                                "plysteer_yield": plysteer_yield(machine_batch_data),
                                "center_radial_run_out_yield": center_radial_run_out_yield(machine_batch_data),
                                "top_radial_run_out_yield": top_radial_run_out_yield(machine_batch_data),
                                "bottom_radial_run_out_yield": bottom_radial_run_out_yield(machine_batch_data),
                                "lateral_run_out_yield": lateral_run_out_yield(machine_batch_data),
                                "lateral_run_out_harmonic_first_yield": lateral_run_out_harmonic_first_yield(machine_batch_data),
                                "center_radial_run_out_harmonic_first_yield": center_radial_run_out_harmonic_first_yield(machine_batch_data),
                                "top_radial_run_out_harmonic_first_yield": top_radial_run_out_harmonic_first_yield(machine_batch_data),
                                "bottom_radial_run_out_harmonic_first_yield": bottom_radial_run_out_harmonic_first_yield(machine_batch_data),
                                "lateral_radial_run_out_yield": lateral_radial_run_out_yield(machine_batch_data),
                                "static_yield": static_yield(machine_batch_data),
                                "couple_yield": couple_yield(machine_batch_data),
                                "upper_yield": upper_yield(machine_batch_data),
                                "lower_yield": lower_yield(machine_batch_data),
                                "overall_uniformity_yield": overall_uniformity_yield(machine_batch_data),
                                "overall_visual_yield": overall_visual_yield(machine_batch_data),
                                "overall_warehouse_yield": overall_warehouse_yield(machine_batch_data),
                                "overall_scrap_yield": overall_scrap_yield(machine_batch_data),
                                "visual_loss_yield": visual_loss_yield(machine_batch_data),
                                "uf_loss_yield": uf_loss_yield(machine_batch_data),
                                "oe_ok_yield": oe_ok_yield(machine_batch_data)
                            })

                            # Perform Shift Detection (same logic as before - keeping it concise)
                            parameter_alerts = []
                            machine_lot_overall_shift_occurred = False
                            yield_spec_mapping = {
                                "rfv_yield": spec_rfppwc_yield,
                                "rfh1_yield": spec_rfh1wc_yield,
                                "rfh2_yield": spec_rfh2wc_yield,
                                "lfv_yield": spec_lfppwc_yield,
                                "conicity_yield": spec_cony_yield,
                                "plysteer_yield": spec_plys_yield,
                                "center_radial_run_out_yield": spec_crro_yield,
                                "top_radial_run_out_yield": spec_trro_yield,
                                "bottom_radial_run_out_yield": spec_brro_yield,
                                "lateral_run_out_yield": spec_lro_yield,
                                "lateral_run_out_harmonic_first_yield": spec_lroh1_yield,
                                "center_radial_run_out_harmonic_first_yield": spec_crroh1_yield,
                                "top_radial_run_out_harmonic_first_yield": spec_trroh1_yield,
                                "bottom_radial_run_out_harmonic_first_yield": spec_brroh1_yield,
                                "lateral_radial_run_out_yield": spec_lrro_yield,
                                "static_yield": spec_sbal_yield,
                                "couple_yield": spec_couple_yield,
                                "upper_yield": spec_upper_yield,
                                "lower_yield": spec_lower_yield,
                                "overall_uniformity_yield": spec_overall_uniformity_yield,
                                "overall_visual_yield": spec_overall_visual_yield,
                                "overall_warehouse_yield": spec_overall_warehouse_yield,
                                "overall_scrap_yield": spec_overall_scrap_yield,
                                "visual_loss_yield": spec_visual_loss_yield,
                                "uf_loss_yield": spec_uf_loss_yield,
                                "oe_ok_yield": spec_oe_ok_yield
                            }

                            # Shift detection (same as before)
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
                                        parameter_alerts.append(alert_dict)
                                        continue

                                    shift_occurred_thershold = False
                                    shift_occurred_spec = False
                                    spec_val = None

                                    if param == "cony" and spec_cony_lower is not None and spec_cony_upper is not None:
                                        shift_occurred_thershold = abs(current_val - prev_val) > threshold_value_cony
                                        shift_occurred_spec = not (-spec_cony_lower <= current_val <= spec_cony_upper)
                                        spec_val = spec_cony_upper
                                    elif param == "plys" and spec_plys_lower is not None and spec_plys_upper is not None:
                                        shift_occurred_thershold = abs(current_val - prev_val) > threshold_value_plys
                                        shift_occurred_spec = not (-spec_plys_lower <= current_val <= spec_plys_upper)
                                        spec_val = spec_plys_upper
                                    elif param in yield_params:
                                        spec_yield = yield_spec_mapping.get(param, 0)
                                        threshold_key = f"threshold_value_{param}"
                                        threshold_val = globals().get(threshold_key, 5)
                                        shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_val)
                                        shift_occurred_spec = current_val < spec_yield
                                        spec_val = spec_yield

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
                                        parameter_alerts.append(alert_dict)
                                        continue

                                    shift_occurred_thershold = False
                                    shift_occurred_spec = False
                                    spec_val = None

                                    if param == "cony" and spec_cony_lower is not None and spec_cony_upper is not None:
                                        shift_occurred_spec = not (spec_cony_lower <= current_val <= spec_cony_upper)
                                        spec_val = spec_cony_upper
                                    elif param == "plys" and spec_plys_lower is not None and spec_plys_upper is not None:
                                        shift_occurred_spec = not (spec_plys_lower <= current_val <= spec_plys_upper)
                                        spec_val = spec_plys_upper
                                    elif param in yield_params:
                                        spec_yield = yield_spec_mapping.get(param, 0)
                                        shift_occurred_spec = current_val < spec_yield
                                        spec_val = spec_yield

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
                                    parameter_alerts.append(alert_dict)

                            machine_lot_overall_shift_occurred = bool(machine_lot_overall_shift_occurred)

                            # Deviated parameter alerts
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
                                    "sku": model_number,
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

                            # Prepare averages document
                            averages_doc_data = {
                                "_id": f"avg_{current_ufm_machine_lot_id}",
                                "sku": model_number,
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
                            
                            ufm_averages_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(averages_doc_data)))
                            output_messages.append(f"Prepared averages for {current_ufm_machine_lot_id} for insertion into averages_ufm_collect")

                            # Update maps
                            current_avg_key = (model_number, machine_no, current_ufm_machine_lot_id)
                            ufm_machine_lot_avg_map[current_avg_key] = lot_averages
                            ufm_previous_machine_model_lot_ids[machine_model_key] = current_ufm_machine_lot_id

                            # Increment alert ID
                            last_alert_id += 1
                            merge_df.loc[machine_batch_indices, "alert_id_ufm_ufm"] = last_alert_id

                            # Build shift data (existing logic)
                            machine_batch_data["tbm_shift"] = machine_batch_data["date_of_tbm"].apply(get_shift)
                            machine_batch_data = machine_batch_data.sort_values("date_of_tbm").reset_index(drop=True)

                            shifts_data_list_tbm = []
                            current_shift = None
                            current_start = None
                            current_barcodes = []

                            for idx, row in machine_batch_data.iterrows():
                                this_shift = row["tbm_shift"]
                                this_dt = row["date_of_tbm"]
                                this_bc = row.get("barcode")

                                if this_shift != current_shift or current_shift is None:
                                    if current_shift is not None:
                                        shifts_data_list_tbm.append({
                                            "shift": current_shift,
                                            "barcode_count": len(current_barcodes),
                                            "machine_shift_tbm_start_time": current_start,
                                            "machine_shift_tbm_end_time": machine_batch_data.at[idx-1, "date_of_tbm"],
                                            "barcodes": current_barcodes[:]
                                        })
                                    current_shift = this_shift
                                    current_start = this_dt
                                    current_barcodes = [this_bc] if this_bc is not None else []
                                else:
                                    current_barcodes.append(this_bc if this_bc is not None else None)

                            if current_shift is not None:
                                shifts_data_list_tbm.append({
                                    "shift": current_shift,
                                    "barcode_count": len(current_barcodes),
                                    "machine_shift_tbm_start_time": current_start,
                                    "machine_shift_tbm_end_time": machine_batch_data["date_of_tbm"].iloc[-1],
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
                                "tbm_machine": tbm_machine_value,
                                "ufm_shift_occured": machine_lot_overall_shift_occurred,
                                "shifts_data": shifts_data_list,
                                "shifts_data_tbm": shifts_data_list_tbm,
                                "overall_uniformity_yield": overall_uniformity_yield(machine_batch_data),
                                "overall_warehouse_yield": overall_warehouse_yield(machine_batch_data),
                                "overall_visual_yield": overall_visual_yield(machine_batch_data),
                                "overall_scrap_yield": overall_scrap_yield(machine_batch_data),
                                "visual_loss_yield": visual_loss_yield(machine_batch_data),
                                "uf_loss_yield": uf_loss_yield(machine_batch_data),
                                "oe_ok_yield": oe_ok_yield(machine_batch_data),
                                "rfv_yield": rfv_yield(machine_batch_data),
                                "rfh1_yield": rfh1_yield(machine_batch_data),
                                "rfh2_yield": rfh2_yield(machine_batch_data),
                                "lfv_yield": lfv_yield(machine_batch_data),
                                "plysteer_yield": plysteer_yield(machine_batch_data),
                                "sizecode": sizecode, 
                                "gt_code": str(gt_code),
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
                                "barcodes": machine_batch_data["barcode"].tolist() if "barcode" in machine_batch_data.columns else [],
                                "cur_ids": (
                                    [
                                        {"cur_id": cur_id, "barcode": int(count)}
                                        for cur_id, count in machine_batch_data.groupby("cur_id")["barcode"].count().items()
                                    ]
                                    if "cur_id" in machine_batch_data.columns and "barcode" in machine_batch_data.columns
                                    else []
                                ),
                                "tbm_ids": (
                                    [
                                        {"tbm_id": ptb_id, "barcode": int(count)}
                                        for ptb_id, count in machine_batch_data.groupby("ptb_id")["barcode"].count().items()
                                    ]
                                    if "ptb_id" in machine_batch_data.columns and "barcode" in machine_batch_data.columns
                                    else []
                                )
                            }

                            # Create alert documents
                            alert_doc_main = {
                                "_id": f"main_alert_{last_alert_id}",
                                "alert_id_ufm_ufm": last_alert_id,
                                "sku": model_number,
                                "lot_id": current_model_lot_id,
                                "alert_flag": False,
                                "dashboard_flag": False,
                                "alert_status_check": "o",
                                "alert_status_timestamp": None,
                                "alert_status_comment": [],
                                "alert_assign_user": [],
                                "machine_alerts": [machine_alert_detail],
                                "first_barcode_model": machine_alert_detail["first_barcode_machine"],
                                "last_barcode_model": machine_alert_detail["last_barcode_machine"],
                                "timestamp_alert_generated": some_function()
                            }

                            # Store in alert_map with lot_id as key
                            if current_model_lot_id not in alert_map:
                                alert_map[current_model_lot_id] = {
                                    "alert_id_ufm_ufm": last_alert_id,
                                    "sku": model_number,
                                    "lot_id": current_model_lot_id,
                                    "machine_alerts": []
                                }
                            
                            alert_map[current_model_lot_id]["machine_alerts"].append(machine_alert_detail.copy())

                            trigger_doc = {
                                "_id": f"trigger_alert_{last_alert_id}",
                                "alert_id_ufm_ufm": last_alert_id,
                                "sku": model_number,
                                "lot_id": current_model_lot_id,
                                "type": "ufm_trigger",
                                "timestamp_alert_generated": some_function()
                            }

                            # Insert main alerts
                            if not alert_collect.find_one({"alert_id_ufm_ufm": last_alert_id}):
                                try:
                                    alert_collect.insert_one(convert_numpy_types(alert_doc_main))
                                    logging.info(f"Inserted alert_doc_main into alert_table for alert_id {last_alert_id}")
                                    output_messages.append(f"Inserted into alert_table: alert_id {last_alert_id}")
                                except Exception as e:
                                    logging.error(f"Failed to insert into alert_table: {e}")

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

                            if machine_lot_overall_shift_occurred:
                                if not alert_ufm_collect.find_one({"alert_id_ufm_ufm": last_alert_id}):
                                    ufm_alert_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(alert_doc_main)))
                                    ufm_triggering_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(trigger_doc)))
                                    ufm_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(trigger_doc)))
                                    logging.info(f"Shift detected - added to bulk ops for alert_id {last_alert_id}")
                                    output_messages.append(f"Shift alert queued for bulk: alert_id {last_alert_id}")

                            # Insert Lot document into MongoDB (NOW IT'S COMPLETE!)
                            lot_doc_data = {
                                "_id": f"lot_{current_ufm_machine_lot_id}",
                                "sku": model_number,
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
                            
                            ufm_lot_bulk_operations.append(pymongo.UpdateOne(
                                {"ufm_machine_model_lot_id": current_ufm_machine_lot_id},
                                {"$setOnInsert": convert_numpy_types(lot_doc_data)},
                                upsert=True
                            ))
                            output_messages.append(f"Prepared complete lot_id {current_model_lot_id} for insertion into lot_ufm_collect")

                            machine_barcode_counter += target_lot_size

                    except Exception as e:
                        logging.error("UFM Error processing machine %s for model %s: %s", ufm_machine_id, model_number, e)
                        output_messages.append(f"UFM Error processing machine {ufm_machine_id} for model {model_number}: {e}")
                        continue

                # ==================== VIRGIN & FINAL YIELD ALERT INSERTION ====================
                try:
                    # Process each lot_id only once
                    for lot_id, alert_obj in alert_map.items():
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
                            # A lot completed from BANKED data (barcodes carried over from a
                            # previous run) will have barcodes that are not present in THIS run's
                            # virgin_data. Filtering only against virgin_data would wrongly shrink
                            # a full 50-barcode lot (e.g. down to 22). Since these barcodes already
                            # belong to this completed lot, treat the lot's own barcodes as valid
                            # too, so a full lot keeps all its processed barcodes.
                            lot_own_barcodes = {str(bc) for bc in original_barcodes}
                            allowed_barcodes = virgin_barcodes_set | lot_own_barcodes
                            filtered = [bc for bc in original_barcodes if str(bc) in allowed_barcodes and str(bc) not in virgin_barcodes_seen]
                            if filtered:
                                virgin_ma = ma.copy()
                                virgin_ma["barcodes"] = filtered
                                virgin_ma["barcode_count"] = len(filtered)
                                virgin_machine_alerts.append(virgin_ma)
                                virgin_barcodes_seen.update(filtered)

                        if virgin_machine_alerts:
                            def _virgin_sort_key(x):
                                _rows = virgin_data[virgin_data["barcode"].astype(str) == x]
                                return _rows["date_of_dt"].iloc[0] if not _rows.empty else pd.Timestamp.max
                            all_virgin_bcs = sorted(virgin_barcodes_seen, key=_virgin_sort_key)
                            doc_virgin = {
                                "alert_id_ufm_ufm": alert_obj["alert_id_ufm_ufm"],
                                "sku": alert_obj["sku"],
                                "lot_id": alert_obj["lot_id"],
                                "alert_flag": False,
                                "dashboard_flag": False,
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
                            
                            # Check if this virgin alert already exists in the database
                            existing_virgin = alert_triggering_virgin_yield_collect.find_one({
                                "sku": alert_obj["sku"],
                                "lot_id": alert_obj["lot_id"],
                                "yield_type": "virgin"
                            })
                            
                            if existing_virgin:
                                logging.info(f"Virgin alert already exists for lot: {alert_obj['lot_id']}. This is expected if the lot was completed in a previous run.")
                            else:
                                alert_triggering_virgin_yield_collect.insert_one(convert_numpy_types(doc_virgin))
                                logging.info(f"Inserted Virgin Yield Alert for lot: {alert_obj['lot_id']} with alert_id {alert_obj['alert_id_ufm_ufm']}")
                                output_messages.append(f"Inserted Virgin Yield Alert for lot: {alert_obj['lot_id']}")

                        # === FINAL YIELD ===
                        final_machine_alerts = []
                        final_barcodes_seen = set()
                        for ma in true_machine_alerts:
                            original_barcodes = ma.get("barcodes", [])
                            if not original_barcodes:
                                continue
                            # Same as virgin: keep the lot's own barcodes even if they are not in
                            # THIS run's final_data (carried over from a previous run when the lot
                            # was completed), so a full lot keeps all its processed barcodes.
                            lot_own_barcodes = {str(bc) for bc in original_barcodes}
                            allowed_barcodes = final_barcodes_set | lot_own_barcodes
                            filtered = [bc for bc in original_barcodes if str(bc) in allowed_barcodes and str(bc) not in final_barcodes_seen]
                            if filtered:
                                final_ma = ma.copy()
                                final_ma["barcodes"] = filtered
                                final_ma["barcode_count"] = len(filtered)
                                final_machine_alerts.append(final_ma)
                                final_barcodes_seen.update(filtered)

                        if final_machine_alerts:
                            def _final_sort_key(x):
                                _rows = final_data[final_data["barcode"].astype(str) == x]
                                return _rows["date_of_dt"].iloc[0] if not _rows.empty else pd.Timestamp.max
                            all_final_bcs = sorted(final_barcodes_seen, key=_final_sort_key)
                            doc_final = {
                                "alert_id_ufm_ufm": alert_obj["alert_id_ufm_ufm"],
                                "sku": alert_obj["sku"],
                                "lot_id": alert_obj["lot_id"],
                                "alert_flag": False,
                                "dashboard_flag": False,
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
                            
                            # Check if final alert already exists
                            existing_final = alert_triggering_final_yield_collect.find_one({
                                "sku": alert_obj["sku"],
                                "lot_id": alert_obj["lot_id"],
                                "yield_type": "final"
                            })
                            
                            if existing_final:
                                logging.info(f"Final alert already exists for lot: {alert_obj['lot_id']}. This is expected if the lot was completed in a previous run.")
                            else:
                                alert_triggering_final_yield_collect.insert_one(convert_numpy_types(doc_final))
                                alert_final_yield_collect.insert_one(convert_numpy_types(doc_final))
                                logging.info(f"Inserted Final Yield Alert for lot: {alert_obj['lot_id']}")
                                output_messages.append(f"Inserted Final Yield Alert for lot: {alert_obj['lot_id']}")

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
                                    first_ma = final_machine_alerts[0]
                                    ufm_machine_model_lot_id = first_ma.get("ufm_machine_model_lot_id")
                                    ufm_previous_machine_model_lot_id = first_ma.get("ufm_previous_machine_model_lot_id")

                                    deviated_doc = {
                                        "alert_id_tbm_ufm": alert_obj["alert_id_ufm_ufm"],
                                        "sku": alert_obj["sku"],
                                        "lot_id": alert_obj["lot_id"],
                                        "machine": final_machine_alerts[0]["machine"],
                                        "ufm_machine_model_lot_id": ufm_machine_model_lot_id,
                                        "ufm_previous_machine_model_lot_id": ufm_previous_machine_model_lot_id,
                                        "comments": [],
                                        "alert_assign_user": [],
                                        "parameter_alerts": deviated_parameter_alerts,
                                        "timestamp_alert_generated": some_function(),
                                        "yield_type": "final"
                                    }
                                    
                                    # Check if deviated final alert already exists
                                    existing_deviated = alert_deviated_final_yield_collect.find_one({
                                        "sku": alert_obj["sku"],
                                        "lot_id": alert_obj["lot_id"],
                                        "yield_type": "final"
                                    })
                                    
                                    if existing_deviated:
                                        logging.info(f"Deviated final alert already exists for lot: {alert_obj['lot_id']}. This is expected if the lot was completed in a previous run.")
                                    else:
                                        alert_deviated_final_yield_collect.insert_one(convert_numpy_types(deviated_doc))
                                        logging.info(f"Inserted Deviated Final Yield Alert for lot: {alert_obj['lot_id']}")
                                        output_messages.append(f"Inserted Deviated Final Yield Alert for lot: {alert_obj['lot_id']}")

                except Exception as e:
                    logging.error(f"Error in Virgin/Final Yield alert insertion: {e}")
                    output_messages.append(f"Error in Virgin/Final Yield alert insertion: {e}")

        except Exception as e:
            logging.error("UFM Error processing model %s: %s", model_number, e)
            output_messages.append(f"UFM Error processing model {model_number}: {e}")
            continue

    # ============== NEW: Update incomplete lots in database ==============
    try:
        # Update incomplete lots
        for update_doc in incomplete_lots_to_update:
            incomplete_lots_collect.update_one(
                {"_id": update_doc["_id"]},
                {"$set": {k: v for k, v in update_doc.items() if k != "_id"}}
            )
            logging.info(f"Updated incomplete lot: {update_doc.get('lot_id')}")
        
        # Delete completed incomplete lots
        for delete_id in incomplete_lots_to_delete:
            incomplete_lots_collect.delete_one({"_id": delete_id})
            logging.info(f"Deleted completed incomplete lot with _id: {delete_id}")
            
    except Exception as e:
        logging.error(f"Error updating incomplete lots: {e}")
        output_messages.append(f"❌ Error updating incomplete lots: {e}")

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
            inserted_counts["alert_ufm_collect"] = result.inserted_count
            final_log_messages.append(f"✅ UFM Inserted {result.inserted_count} documents into UFM alert collection.")
        except pymongo.errors.BulkWriteError as bwe:
            inserted_counts["alert_ufm_collect"] = bwe.details['nInserted']
            final_log_messages.append(f"⚠️ UFM Alert Bulk Write Errors: Inserted {bwe.details['nInserted']}. Errors: {bwe.details['writeErrors']}")
        except Exception as e:
            final_log_messages.append(f"❌ UFM Error performing bulk write to UFM alert collection: {e}")

    if ufm_triggering_bulk_operations:
        try:
            result = alert_ufm_collect.bulk_write(ufm_triggering_bulk_operations, ordered=False)
            inserted_counts["alert_ufm_collect"] = result.inserted_count
            final_log_messages.append(f"✅ UFM Inserted {result.inserted_count} documents into alert_db.{alert_triggering_ubno_collect_name}.")
        except pymongo.errors.BulkWriteError as bwe:
            inserted_counts["alert_ufm_collect"] = bwe.details['nInserted']
            final_log_messages.append(f"⚠️ UFM Triggering Alert Bulk Write Errors in alert_db.{alert_triggering_ubno_collect_name}: Inserted {bwe.details['nInserted']}. Errors: {bwe.details['writeErrors']}")
        except Exception as e:
            final_log_messages.append(f"❌ UFM Error performing bulk write to alert_db.{alert_triggering_ubno_collect_name}: {e}")

        try:
            result_dashboard = alert_triggering_collect_dashboard_ufm.bulk_write(ufm_triggering_bulk_operations, ordered=False)
            inserted_counts["alert_triggering_collect_dashboard_ufm"] = result_dashboard.inserted_count
            output_messages.append(f"✅ UFM Inserted {result_dashboard.inserted_count} machine_alert documents into alert_db_dashboard.{alert_triggering_ubno_collect_name}.")
        except pymongo.errors.BulkWriteError as bwe:
            inserted_counts["alert_triggering_collect_dashboard_ufm"] = bwe.details['nInserted']
            output_messages.append(f"⚠️ UFM Triggering Alert Bulk Write Errors in alert_db_dashboard.{alert_triggering_ubno_collect_name}: Inserted {bwe.details['nInserted']}. Errors: {bwe.details['writeErrors']}")
        except Exception as e:
            output_messages.append(f"❌ UFM Error performing bulk write to alert_db_dashboard.{alert_triggering_ubno_collect_name}: {e}")
    else:
        output_messages.append(f"No UFM triggering alert documents to insert into alert_db_dashboard.{alert_triggering_ubno_collect_name}.")

    if ufm_bulk_operations:
        try:
            result = alert_collect.bulk_write(ufm_bulk_operations, ordered=False)
            inserted_counts["alert_collect"] = result.inserted_count
            final_log_messages.append(f"✅ UFM Inserted {result.inserted_count} documents into UFM triggering alert collection.")
        except pymongo.errors.BulkWriteError as bwe:
            inserted_counts["alert_collect"] = bwe.details['nInserted']
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
            if "tbm_machine" in merge_df.columns:
                update_payload["$set"]["tbm_machine"] = row["tbm_machine"]
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
        merge_collect = client[TUB_CIMCORP_VISUAL_MERGED_DB_alert][DATA_MERGED_SUMMARY_COLLECTION_alert_new]
        channel_collect_tbm = client[config_databse][channel_no_collect_tbm]
        channel_collect_ufm = client[config_databse][channel_no_collect]
        lot_tbm_collect = client[lot_db][lot_tbm_collect_name_new]
        alert_collect = client[alert_table][alert_tbm_collect_name_new]
        averages_collect = client[average_db][averages_tbm_table_new]
        machine_collect = client[config_databse][machine_no_collect]
        curing_press_collect = client[config_databse][curing_press_dict_collect]
        alert_triggering_collect = client[alert_db][alert_triggering_collect_name_new]
        # summary_triggering_collect = client[summary_db][summary_triggering_collect_name_tbm]
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
        tbm_gtcodes_collect = client[model_db][model_no_status_collection]
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
        active_modelnos = set(tbm_gtcodes_collect.distinct("sku", {"status": "active"}))
        output_messages.append(f"✅ Fetched {len(active_modelnos)} active model numbers: {active_modelnos}")
    except Exception as e:
        output_messages.append(f"❌ Error fetching active model numbers: {e}")
        return {"status": "error", "message": "Failed to fetch active model numbers", "output_log": output_messages}, 500

    # Load data into DataFrames
    try:
        merge1 = list(merge_collect.find({}))
        merge_df = pd.DataFrame(merge1)
        channel_data_tbm = list(channel_collect_tbm.find({"sku": {"$in": list(active_modelnos)}}))
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
    channel_modelnos = set(channel_collect_tbm.distinct("sku"))
    logging.info(f"[TBM_NEW] channel_modelnos count: {len(channel_modelnos)}")

    # Check required columns
    required_columns = {"sku", "alert_id_tbm_ufm"}
    if not required_columns.issubset(merge_df.columns):
        logging.warning(f"[TBM_NEW] Required columns missing. merge_df columns: {list(merge_df.columns)}")
        output_messages.append("Required columns are missing in merge_df.")
        return {"status": "error", "message": "Required columns missing", "output_log": output_messages}, 500

    logging.info(f"[TBM_NEW] merge_df total rows before filter: {len(merge_df)}")
    logging.info(f"[TBM_NEW] merge_df columns: {list(merge_df.columns)}")

    # Filter data
    filtered_df = merge_df[merge_df["sku"].isin(channel_modelnos) & merge_df["alert_id_tbm_ufm"].isna()]
    logging.info(f"[TBM_NEW] filtered_df rows after sku+alert_id filter: {len(filtered_df)}")
    if not filtered_df.empty:
        merge_df = filtered_df
    else:
        output_messages.append("No data after filtering or condition not satisfied.")
        logging.warning(f"[TBM_NEW] No data after filtering. channel_modelnos sample: {list(channel_modelnos)[:5]}")
        return {"status": "error", "message": "No data after filtering", "output_log": output_messages}, 500

    # Ensure string columns are object dtype so string values can be assigned without float64 errors
    for _col in ["lot_id", "previous_lot_id", "tbm_machine_model_lot_id", "tbm_previous_machine_model_lot_id"]:
        if _col in merge_df.columns:
            merge_df[_col] = merge_df[_col].astype(object)
        else:
            merge_df[_col] = None

    # # Assign date columns
    # merge_df["date_of_tbm"] = merge_df["tbm_buildtime"]
    # merge_df["date_of_curing"] = merge_df["curing_infeedtime"]
    # merge_df["date_and_time"] = merge_df["date"].fillna('') + " " + merge_df["time"].fillna('')
    
    merge_df["date_of_tbm"] = merge_df["ptb_time"]
    merge_df["date_of_curing"] = merge_df["cur_time"]
    merge_df["date_and_time"] = merge_df["uf_time"]

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
        # merge_df["time_of_ufm"] = pd.to_datetime(merge_df["time"], errors="coerce").dt.strftime("%H:%M:%S")
        # time_invalid_mask = merge_df["time_of_ufm"].isna() & merge_df["time"].notna()
        # if time_invalid_mask.any():
        #     logging.info("⚠️ Found %s invalid time values", time_invalid_mask.sum())
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
                    "sku": {"$in": list(active_modelnos)},
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
                    "sku": "$sku",
                    "machine": "$machine"
                }
            },
            {
                "$sort": {"lot_number": -1}
            },
            {
                "$group": {
                    "_id": {"sku": "$sku", "machine": "$machine"},
                    "max_lot_number": {"$first": "$lot_number"},
                    "last_lot_id": {"$first": "$lot_id"}
                }
            }
        ]))
        last_lot_dict = {
            (item["_id"]["sku"], item["_id"]["machine"]): {
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
                    "sku": {"$in": list(active_modelnos)},
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
                    "_id": {"sku": "$sku", "machine": "$machine"},
                    "max_tbm_lot_id": {"$first": "$tbm_machine_model_lot_id"}
                }
            }
        ]))
        last_tbm_lot_dict = {
            (item["_id"]["sku"], item["_id"]["machine"]): item["max_tbm_lot_id"]
            for item in last_tbm_lots if item["max_tbm_lot_id"]
        }
    except Exception as e:
        error_message = f"❌ TBM Error fetching last lot IDs for lot/alert processing: {e}"
        output_messages.append(error_message)
        last_tbm_lot_dict = {}

    # Prepare merge_df
    merge_df["tbm"] = merge_df["ptb_id"].astype(str)
    machine_df["machine"] = machine_df["machine"].astype(str)

    # Correct merge: tbm ↔ machine
    merge_df = merge_df.merge(
        machine_df[["machine"]],
        left_on="tbm",
        right_on="machine",
        how="left"
    )

    # Update tbm (if machine exists, else keep original)
    merge_df["tbm"] = merge_df["machine"].fillna(merge_df["tbm"])

    # Filter only active machines
    merge_df = merge_df[merge_df["tbm"].isin(active_machines)]
    logging.info(f"[TBM_NEW] merge_df rows after active machine filter: {len(merge_df)}")
    logging.info(f"[TBM_NEW] active_machines: {active_machines}")
    logging.info(f"[TBM_NEW] unique tbm values in merge_df before filter: {merge_df['tbm'].unique().tolist() if 'tbm' in merge_df.columns else 'NO tbm column'}")
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

    conicity_params = [
        "rfppcc","rfppcw","rfh1cc","rfh1cw","rfh2cc","rfh2cw","lfppcc","lfppcw","cony","plys",
        "crro","trro","brro","lrot","lrob","lroh1grade","tbul","bbul","tdep","bdep",
        "upper","lower","sbal","aircc","aircw","loadcc","loadcw"
    ]
    yield_params = [
        "rfv_yield", "rfh1_yield", "rfh2_yield", "lfv_yield", "conicity_yield", "plysteer_yield",
        "center_radial_run_out_yield", "top_radial_run_out_yield", "bottom_radial_run_out_yield", 
        "lateral_run_out_yield", "lateral_run_out_harmonic_first_yield", "center_radial_run_out_harmonic_first_yield",
        "top_radial_run_out_harmonic_first_yield", "bottom_radial_run_out_harmonic_first_yield",
        "lateral_radial_run_out_yield", "static_yield", "couple_yield",
        "upper_yield", "lower_yield", "overall_uniformity_yield", "overall_visual_yield", "overall_warehouse_yield",
        "overall_scrap_yield", "visual_loss_yield", "uf_loss_yield", "oe_ok_yield"
    ]

    # Define rank parameter mappings
    rank_param_mappings = {
        "rfv_yield": ["rfppwcgrade"],
        "rfh1_yield": ["rfh1wcgrade"],
        "rfh2_yield": ["rfh2wcgrade"],
        "lfv_yield": ["lfppwcgrade"],
        "conicity_yield": ["conygrade"],
        "plysteer_yield": ["plysgrade"],
        "center_radial_run_out_yield": ["crrograde"], 
        "top_radial_run_out_yield": ["trrograde"], 
        "bottom_radial_run_out_yield": ["brrograde"],
        "lateral_run_out_yield": ["lrograde"],
        "lateral_run_out_harmonic_first_yield": ["lroh1grade"],
        "center_radial_run_out_harmonic_first_yield": ["crroh1grade"],
        "top_radial_run_out_harmonic_first_yield": ["trroh1grade"],
        "bottom_radial_run_out_harmonic_first_yield": ["brroh1grade"],
        "lateral_radial_run_out_yield": ["lrrograde"],
        "static_yield": ["sbalgrade"],
        "couple_yield": ["couplegrade"],
        "upper_yield": ["uppergrade"],
        "lower_yield": ["lowergrade"]
    }

    # TBM-wise processing
    logging.info(f"[TBM_NEW] Starting TBM loop. merge_df rows: {len(merge_df)}, unique tbm values: {merge_df['tbm'].unique().tolist()}")
    for tbm_value, tbm_data in merge_df.groupby("tbm"):
        try:
            machine_no = tbm_value
            unprocessed = tbm_data[tbm_data["tbm_machine_model_lot_id"].isnull()]
            logging.info(f"[TBM_NEW] Machine {machine_no}: total rows={len(tbm_data)}, unprocessed (tbm_machine_model_lot_id null)={len(unprocessed)}")
            tbm_data = unprocessed.copy()
            if tbm_data.empty:
                output_messages.append(f"⚠️ No unprocessed data for TBM {machine_no}")
                continue

            total_tbm_barcodes = len(tbm_data)
            tbm_data.sort_values(["date_of_tbm"], ascending=True, inplace=True)

            # Inside the model processing loop
            for model_number, model_data in tbm_data.groupby("sku"):
                try:
                    # Get channel data for the model
                    channel_row = channel_df[channel_df["sku"] == model_number]
                    
                    if model_number not in active_modelnos:
                        logging.info(f"[TBM_NEW] Skipping model {model_number} - not in active_modelnos")
                        output_messages.append(f"Skipping TBM model {model_number} as it is not active.")
                        continue

                    # Check if channel_row is empty
                    if channel_row.empty:
                        logging.info(f"[TBM_NEW] Skipping model {model_number} - no channel data")
                        output_messages.append(f"⚠️ No channel data for model {model_number} in TBM {machine_no}")
                        continue

                    # Extract scalar row so .get() works correctly
                    channel_row_dict = channel_row.iloc[0].to_dict()

                    # Retrieve gt_codes as a list
                    gt_codes = channel_row_dict.get("gt_code", [])
                    if not isinstance(gt_codes, list):
                        gt_codes = [gt_codes]
                    gt_codes = [code for code in gt_codes if pd.notna(code) and code is not None]

                    # Log gt_codes for debugging
                    output_messages.append(f"ℹ️ Retrieved gt_codes for model {model_number}: {gt_codes}")

                    # Filter model_data based on gt_codes if available
                    if gt_codes:
                        model_data = model_data[model_data["gt_code"].isin(gt_codes)].copy()
                    else:
                        output_messages.append(f"⚠️ No valid gt_codes for model {model_number}, proceeding without gt_code filtering")

                    # Check if model_data is empty after filtering
                    if model_data.empty:
                        output_messages.append(f"⚠️ No matching gt_code for model {model_number} in TBM {machine_no} with gt_codes {gt_codes}")
                        continue

                    spec_cony_upper = float(channel_row_dict.get("spec_cony_upper", 0) or 0)
                    spec_cony_lower = float(channel_row_dict.get("spec_cony_lower", 0) or 0)
                    spec_plys_upper = float(channel_row_dict.get("spec_plys_upper", 0) or 0)
                    spec_plys_lower = float(channel_row_dict.get("spec_plys_lower", 0) or 0)
                    sizecode = str(channel_row_dict.get("sizecode", "") or "")
                    spec_rfppcc = float(channel_row_dict.get("spec_rfppcc", 0) or 0)
                    spec_rfppcw = float(channel_row_dict.get("spec_rfppcw", 0) or 0)
                    spec_rfh1cc = float(channel_row_dict.get("spec_rfh1cc", 0) or 0)
                    spec_rfh1cw = float(channel_row_dict.get("spec_rfh1cw", 0) or 0)
                    spec_rfh2cc = float(channel_row_dict.get("spec_rfh2cc", 0) or 0)
                    spec_rfh2cw = float(channel_row_dict.get("spec_rfh2cw", 0) or 0)
                    spec_lfppcc = float(channel_row_dict.get("spec_lfppcc", 0) or 0)
                    spec_lfppcw = float(channel_row_dict.get("spec_lfppcw", 0) or 0)
                    spec_crro = float(channel_row_dict.get("spec_crro", 0) or 0)
                    spec_trro = float(channel_row_dict.get("spec_trro", 0) or 0)
                    spec_brro = float(channel_row_dict.get("spec_brro", 0) or 0)
                    spec_lrot = float(channel_row_dict.get("spec_lrot", 0) or 0)
                    spec_lrob = float(channel_row_dict.get("spec_lrob", 0) or 0)
                    spec_lroh1grade = float(channel_row_dict.get("spec_lroh1grade", 0) or 0)
                    spec_tbul = float(channel_row_dict.get("spec_tbul", 0) or 0)
                    spec_bbul = float(channel_row_dict.get("spec_bbul", 0) or 0)
                    spec_tdep = float(channel_row_dict.get("spec_tdep", 0) or 0)
                    spec_bdep = float(channel_row_dict.get("spec_bdep", 0) or 0)
                    spec_upper = float(channel_row_dict.get("spec_upper", 0) or 0)
                    spec_lower = float(channel_row_dict.get("spec_lower", 0) or 0)
                    spec_sbal = float(channel_row_dict.get("spec_sbal", 0) or 0)
                    spec_aircc = float(channel_row_dict.get("spec_aircc", 0) or 0)
                    spec_aircw = float(channel_row_dict.get("spec_aircw", 0) or 0)
                    spec_loadcc = float(channel_row_dict.get("spec_yield", 0) or 0)
                    spec_loadcw = float(channel_row_dict.get("spec_yield", 0) or 0)

                    ############### Add 16-07 start ######################

                    # total_rank_yield_spec_yield = float(channel_row.get("total_rank_yield_spec_yield", 0))
                    spec_rfppwc_yield = float(channel_row_dict.get("spec_rfppwc_yield", 0) or 0)
                    spec_rfh1wc_yield = float(channel_row_dict.get("spec_rfh1wc_yield", 0) or 0)
                    spec_rfh2wc_yield = float(channel_row_dict.get("spec_rfh2wc_yield", 0) or 0)
                    spec_lfppwc_yield = float(channel_row_dict.get("spec_lfppwc_yield", 0) or 0)
                    spec_cony_yield = float(channel_row_dict.get("spec_cony_yield", 0) or 0)
                    spec_plys_yield = float(channel_row_dict.get("spec_plys_yield", 0) or 0)
                    spec_crro_yield = float(channel_row_dict.get("spec_crro_yield", 0) or 0)
                    spec_trro_yield = float(channel_row_dict.get("spec_trro_yield", 0) or 0)
                    spec_brro_yield = float(channel_row_dict.get("spec_brro_yield", 0) or 0)
                    spec_lro_yield = float(channel_row_dict.get("spec_lro_yield", 0) or 0)
                    spec_lroh1_yield = float(channel_row_dict.get("spec_lroh1_yield", 0) or 0)
                    spec_crroh1_yield = float(channel_row_dict.get("spec_crroh1_yield", 0) or 0)
                    spec_trroh1_yield = float(channel_row_dict.get("spec_trroh1_yield", 0) or 0)
                    spec_brroh1_yield = float(channel_row_dict.get("spec_brroh1_yield", 0) or 0)
                    spec_lrro_yield = float(channel_row_dict.get("spec_lrro_yield", 0) or 0)
                    spec_sbal_yield = float(channel_row_dict.get("spec_sbal_yield", 0) or 0)
                    spec_couple_yield = float(channel_row_dict.get("spec_couple_yield", 0) or 0)
                    spec_upper_yield = float(channel_row_dict.get("spec_upper_yield", 0) or 0)
                    spec_lower_yield = float(channel_row_dict.get("spec_lower_yield", 0) or 0)
                    spec_overall_uniformity_yield = float(channel_row_dict.get("spec_overall_uniformity_yield", 0) or 0)
                    spec_overall_visual_yield = float(channel_row_dict.get("spec_overall_visual_yield", 0) or 0)
                    spec_overall_warehouse_yield = float(channel_row_dict.get("spec_overall_warehouse_yield", 0) or 0)
                    spec_overall_scrap_yield = float(channel_row_dict.get("spec_overall_scrap_yield", 0) or 0)
                    spec_visual_loss_yield = float(channel_row_dict.get("spec_visual_loss_yield", 0) or 0)
                    spec_uf_loss_yield = float(channel_row_dict.get("spec_uf_loss_yield", 0) or 0)
                    spec_oe_ok_yield = float(channel_row_dict.get("spec_oe_ok_yield", 0) or 0)

                    ############### Add 16-07 end ######################

                    threshold_value_plys = float(channel_row_dict.get("threshold_value_plys", 0.5) or 0.5)
                    threshold_value_cony = float(channel_row_dict.get("threshold_value_cony", 0.5) or 0.5)
                    threshold_value_rfppcc = float(channel_row_dict.get("threshold_value_rfppcc", 0.5) or 0.5)
                    threshold_value_rfppcw = float(channel_row_dict.get("threshold_value_rfppcw", 0.5) or 0.5)
                    threshold_value_rfh1cc = float(channel_row_dict.get("threshold_value_rfh1cc", 0.5) or 0.5)
                    threshold_value_rfh1cw = float(channel_row_dict.get("threshold_value_rfh1cw", 0.5) or 0.5)
                    threshold_value_rfh2cc = float(channel_row_dict.get("threshold_value_rfh2cc", 0.5) or 0.5)
                    threshold_value_rfh2cw = float(channel_row_dict.get("threshold_value_rfh2cw", 0.5) or 0.5)
                    threshold_value_lfppcc = float(channel_row_dict.get("threshold_value_lfppcc", 0.5) or 0.5)
                    threshold_value_lfppcw = float(channel_row_dict.get("threshold_value_lfppcw", 0.5) or 0.5)
                    threshold_value_crro = float(channel_row_dict.get("threshold_value_crro", 5) or 5)
                    threshold_value_trro = float(channel_row_dict.get("threshold_value_trro", 5) or 5)
                    threshold_value_brro = float(channel_row_dict.get("threshold_value_brro", 5) or 5)
                    threshold_value_lrot = float(channel_row_dict.get("threshold_value_lrot", 5) or 5)
                    threshold_value_lrob = float(channel_row_dict.get("threshold_value_lrob", 5) or 5)
                    threshold_value_lroh1grade = float(channel_row_dict.get("threshold_value_lroh1grade", 5) or 5)
                    threshold_value_tbul = float(channel_row_dict.get("threshold_value_tbul", 0) or 0)
                    threshold_value_bbul = float(channel_row_dict.get("threshold_value_bbul", 5) or 5)
                    threshold_value_tdep = float(channel_row_dict.get("threshold_value_tdep", 5) or 5)
                    threshold_value_bdep = float(channel_row_dict.get("threshold_value_bdep", 5) or 5)
                    threshold_value_upper = float(channel_row_dict.get("threshold_value_upper", 5) or 5)
                    threshold_value_lower = float(channel_row_dict.get("threshold_value_lower", 5) or 5)
                    threshold_value_sbal = float(channel_row_dict.get("threshold_value_sbal", 5) or 5)
                    threshold_value_aircc = float(channel_row_dict.get("threshold_value_aircc", 5) or 5)
                    threshold_value_aircw = float(channel_row_dict.get("threshold_value_aircw", 5) or 5)
                    threshold_value_loadcc = float(channel_row_dict.get("threshold_value_loadcc", 5) or 5)
                    threshold_value_loadcw = float(channel_row_dict.get("threshold_value_loadcw", 5) or 5)
                    threshold_value_rfppwc_yield = float(channel_row_dict.get("threshold_value_rfppwc_yield", 5) or 5)
                    threshold_value_rfh1wc_yield = float(channel_row_dict.get("threshold_value_rfh1wc_yield", 5) or 5)
                    threshold_value_rfh2wc_yield = float(channel_row_dict.get("threshold_value_rfh2wc_yield", 5) or 5)
                    threshold_value_lfppwc_yield = float(channel_row_dict.get("threshold_value_lfppwc_yield", 5) or 5)
                    threshold_value_cony_yield = float(channel_row_dict.get("threshold_value_cony_yield", 5) or 5)
                    threshold_value_plys_yield = float(channel_row_dict.get("threshold_value_plys_yield", 5) or 5)
                    threshold_value_crro_yield = float(channel_row_dict.get("threshold_value_crro_yield", 5) or 5)
                    threshold_value_trro_yield = float(channel_row_dict.get("threshold_value_trro_yield", 5) or 5)
                    threshold_value_brro_yield = float(channel_row_dict.get("threshold_value_brro_yield", 5) or 5)
                    threshold_value_lro_yield = float(channel_row_dict.get("threshold_value_lro_yield", 5) or 5)
                    threshold_value_lroh1_yield = float(channel_row_dict.get("threshold_value_lroh1_yield", 5) or 5)
                    threshold_value_crroh1_yield = float(channel_row_dict.get("threshold_value_crroh1_yield", 5) or 5)
                    threshold_value_trroh1_yield = float(channel_row_dict.get("threshold_value_trroh1_yield", 5) or 5)
                    threshold_value_brroh1_yield = float(channel_row_dict.get("threshold_value_brroh1_yield", 5) or 5)
                    threshold_value_lrro_yield = float(channel_row_dict.get("threshold_value_lrro_yield", 5) or 5)
                    threshold_value_sbal_yield = float(channel_row_dict.get("threshold_value_sbal_yield", 5) or 5)
                    threshold_value_couple_yield = float(channel_row_dict.get("threshold_value_couple_yield", 5) or 5)
                    threshold_value_upper_yield = float(channel_row_dict.get("threshold_value_upper_yield", 5) or 5)
                    threshold_value_lower_yield = float(channel_row_dict.get("threshold_value_lower_yield", 5) or 5)
                    threshold_value_overall_uniformity_yield = float(channel_row_dict.get("threshold_value_overall_uniformity_yield", 5) or 5)
                    threshold_value_overall_visual_yield = float(channel_row_dict.get("threshold_value_overall_visual_yield", 5) or 5)
                    threshold_value_overall_warehouse_yield = float(channel_row_dict.get("threshold_value_overall_warehouse_yield", 5) or 5)
                    threshold_value_overall_scrap_yield = float(channel_row_dict.get("threshold_value_overall_scrap_yield", 5) or 5)
                    threshold_value_visual_loss_yield = float(channel_row_dict.get("threshold_value_visual_loss_yield", 5) or 5)
                    threshold_value_uf_loss_yield = float(channel_row_dict.get("threshold_value_uf_loss_yield", 5) or 5)
                    threshold_value_oe_ok_yield = float(channel_row_dict.get("threshold_value_oe_ok_yield", 5) or 5)

                    total_barcodes_model = len(model_data)

                    model_data = model_data[(model_data["sku"] == model_number)].copy()
                    # model_data = model_data[(model_data["tbm_gtcode"] == gt_code)].copy()
                    if model_data.empty:
                        output_messages.append(f"⚠️ No matching sku for model {model_number} in TBM {machine_no}")
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
                                    "sku": model_number,
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
                        "rfv_yield": rfv_yield(model_data),
                        "rfh1_yield": rfh1_yield(model_data),
                        "rfh2_yield": rfh2_yield(model_data),
                        "lfv_yield": lfv_yield(model_data),
                        "conicity_yield": conicity_yield(model_data),
                        "plysteer_yield": plysteer_yield(model_data),
                        "center_radial_run_out_yield": center_radial_run_out_yield(model_data),
                        "top_radial_run_out_yield": top_radial_run_out_yield(model_data),
                        "bottom_radial_run_out_yield": bottom_radial_run_out_yield(model_data),
                        "lateral_run_out_yield": lateral_run_out_yield(model_data),
                        "lateral_run_out_harmonic_first_yield": lateral_run_out_harmonic_first_yield(model_data),
                        "center_radial_run_out_harmonic_first_yield": center_radial_run_out_harmonic_first_yield(model_data),
                        "top_radial_run_out_harmonic_first_yield": top_radial_run_out_harmonic_first_yield(model_data),
                        "bottom_radial_run_out_harmonic_first_yield": bottom_radial_run_out_harmonic_first_yield(model_data),
                        "lateral_radial_run_out_yield": lateral_radial_run_out_yield(model_data),
                        "static_yield": static_yield(model_data),
                        "couple_yield": couple_yield(model_data),
                        "upper_yield": upper_yield(model_data),
                        "lower_yield": lower_yield(model_data),
                        "overall_uniformity_yield": overall_uniformity_yield(model_data),
                        "overall_visual_yield": overall_visual_yield(model_data),
                        "overall_warehouse_yield": overall_warehouse_yield(model_data),
                        "overall_scrap_yield": overall_scrap_yield(model_data),
                        "visual_loss_yield": visual_loss_yield(model_data),
                        "uf_loss_yield": uf_loss_yield(model_data),
                        "oe_ok_yield": oe_ok_yield(model_data)
                    })

                    # Calculate uf_id grouped averages
                    grouped_averages = {}
                    param_spec_map = {
                        "cony": spec_cony_upper,
                        "cony": spec_cony_lower,
                        "plys": spec_plys_upper,
                        "plys": spec_plys_lower,
                        "rfppcc": spec_rfppcc,
                        "rfppcw": spec_rfppcw,
                        "rfh1cc": spec_rfh1cc,
                        "rfh1cw": spec_rfh1cw,
                        "rfh2cc": spec_rfh2cc,
                        "rfh2cw": spec_rfh2cw,
                        "lfppcc": spec_lfppcc,
                        "lfppcw": spec_lfppcw,
                        "crro": spec_crro,
                        "trro": spec_trro,
                        "brro": spec_brro,
                        "lrot": spec_lrot,
                        "lrob": spec_lrob,
                        "lroh1grade": spec_lroh1grade,
                        "tbul": spec_tbul,
                        "bbul": spec_bbul,
                        "tdep": spec_tdep,
                        "upper": spec_upper,
                        "lower": spec_lower,
                        "sbal": spec_sbal,
                        "aircc": spec_aircc,
                        "aircw": spec_aircw,
                        "loadcc": spec_loadcc,
                        "loadcw": spec_loadcw,

                    ############### Add 16-07 start ######################

                        "rfv_yield": spec_rfppwc_yield,
                        "rfh1_yield": spec_rfh1wc_yield,
                        "rfh2_yield": spec_rfh2wc_yield,
                        "lfv_yield": spec_lfppwc_yield,
                        "conicity_yield": spec_cony_yield,
                        "plysteer_yield": spec_plys_yield,
                        "center_radial_run_out_yield": spec_crro_yield,
                        "top_radial_run_out_yield": spec_trro_yield,
                        "bottom_radial_run_out_yield": spec_brro_yield,
                        "lateral_run_out_yield": spec_lro_yield,
                        "lateral_run_out_harmonic_first_yield": spec_lroh1_yield,
                        "center_radial_run_out_harmonic_first_yield": spec_crroh1_yield,
                        "top_radial_run_out_harmonic_first_yield": spec_trroh1_yield,
                        "bottom_radial_run_out_harmonic_first_yield": spec_brroh1_yield,
                        "lateral_radial_run_out_yield": spec_lrro_yield,
                        "static_yield": spec_sbal_yield,
                        "couple_yield": spec_couple_yield,
                        "upper_yield": spec_upper_yield,
                        "lower_yield": spec_lower_yield,
                        "overall_uniformity_yield": spec_overall_uniformity_yield,
                        "overall_visual_yield": spec_overall_visual_yield,
                        "overall_warehouse_yield": spec_overall_warehouse_yield,
                        "overall_scrap_yield": spec_overall_scrap_yield,
                        "visual_loss_yield": spec_visual_loss_yield,
                        "uf_loss_yield": spec_uf_loss_yield,
                        "oe_ok_yield": spec_oe_ok_yield
                    }

                    try:
                        model_data["uf_id"] = model_data["uf_id"].astype(str).str.strip()
                        model_data.sort_values("date_and_time", ascending=True, inplace=True)
                        ubno_grouped = model_data.groupby("uf_id")
                        ubno_records = []
                        for uf_id, group in ubno_grouped:
                            if group.empty:
                                output_messages.append(f"⚠️ Empty group for uf_id {uf_id} in TBM {machine_no}, model {model_number}")
                                continue
                            group.sort_values("date_and_time", ascending=True)
                            record = {
                                "uf_id": uf_id,
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
                                    if param == "cony" and spec_cony_lower is not None and spec_cony_upper is not None and avg_val is not None:
                                        exceeded = not (-spec_cony_lower <= avg_val <= spec_cony_upper)
                                    if param == "plys" and spec_plys_lower is not None and spec_plys_upper is not None and avg_val is not None:
                                        exceeded = not (spec_plys_lower <= avg_val <= spec_plys_upper)
                                    if spec is not None and param not in ["cony", "plys"] and avg_val is not None:
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
                        grouped_averages["uf_id"] = ubno_records
                    except Exception as e:
                        grouped_averages["uf_id"] = f"Error: {e}"
                        output_messages.append(f"⚠️ Error calculating uf_id averages for TBM {machine_no}, model {model_number}: {e}")

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
                        "sku": model_number,
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
                        "sku": model_number,
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
                                if param == "cony" and spec_cony_lower is not None and spec_cony_upper is not None:
                                    if abs(current_val - prev_val) > threshold_value_cony:
                                        shift = True
                                    if not (-spec_cony_lower <= current_val <= spec_cony_upper):
                                        shift_occurred_spec = True
                                if param == "plys" and spec_plys_lower is not None and spec_plys_upper is not None:
                                    if abs(current_val - prev_val) > threshold_value_plys:
                                        shift = True
                                    if not (-spec_plys_lower <= current_val <= spec_plys_upper):
                                        shift_occurred_spec = True
                                if param == "rfppcc" and spec_rfppcc is not None:
                                    if (current_val - prev_val) > threshold_value_rfppcc:
                                        shift = True
                                    if current_val > spec_rfppcc:
                                        shift_occurred_spec = True
                                if param == "rfppcw" and spec_rfppcw is not None:
                                    if (current_val - prev_val) > threshold_value_rfppcw:
                                        shift = True
                                    if current_val > spec_rfppcw:
                                        shift_occurred_spec = True
                                if param == "rfh1cc" and spec_rfh1cc is not None:
                                    if (current_val - prev_val) > threshold_value_rfh1cc:
                                        shift = True
                                    if current_val > spec_rfh1cc:
                                        shift_occurred_spec = True
                                if param == "rfh1cw" and spec_rfh1cw is not None:
                                    if (current_val - prev_val) > threshold_value_rfh1cw:
                                        shift = True
                                    if current_val > spec_rfh1cw:
                                        shift_occurred_spec = True
                                if param == "rfh2cc" and spec_rfh2cc is not None:
                                    if (current_val - prev_val) > threshold_value_rfh2cc:
                                        shift = True
                                    if current_val > spec_rfh2cc:
                                        shift_occurred_spec = True
                                if param == "rfh2cw" and spec_rfh2cw is not None:
                                    if (current_val - prev_val) > threshold_value_rfh2cw:
                                        shift = True
                                    if current_val > spec_rfh2cw:
                                        shift_occurred_spec = True
                                if param == "lfppcc" and spec_lfppcc is not None:
                                    if (current_val - prev_val) > threshold_value_lfppcc:
                                        shift = True
                                    if current_val > spec_lfppcc:
                                        shift_occurred_spec = True
                                if param == "lfppcw" and spec_lfppcw is not None:
                                    if (current_val - prev_val) > threshold_value_lfppcw:
                                        shift = True
                                    if current_val > spec_lfppcw:
                                        shift_occurred_spec = True
                                if param == "crro" and spec_crro is not None:
                                    if (current_val - prev_val) > threshold_value_crro:
                                        shift = True
                                    if current_val > spec_crro:
                                        shift_occurred_spec = True
                                if param == "trro" and spec_trro is not None:
                                    if (current_val - prev_val) > threshold_value_trro:
                                        shift = True
                                    if current_val > spec_trro:
                                        shift_occurred_spec = True
                                if param == "brro" and spec_brro is not None:
                                    if (current_val - prev_val) > threshold_value_brro:
                                        shift = True
                                    if current_val > spec_brro:
                                        shift_occurred_spec = True
                                if param == "lrot" and spec_lrot is not None:
                                    if (current_val - prev_val) > threshold_value_lrot:
                                        shift = True
                                    if current_val > spec_lrot:
                                        shift_occurred_spec = True
                                if param == "lrob" and spec_lrob is not None:
                                    if (current_val - prev_val) > threshold_value_lrob:
                                        shift = True
                                    if current_val > spec_lrob:
                                        shift_occurred_spec = True
                                if param == "lroh1grade" and spec_lroh1grade is not None:
                                    if (current_val - prev_val) > threshold_value_lroh1grade:
                                        shift = True
                                    if current_val > spec_lroh1grade:
                                        shift_occurred_spec = True
                                if param == "tbul" and spec_tbul is not None:
                                    if (current_val - prev_val) > threshold_value_tbul:
                                        shift = True
                                    if current_val > spec_tbul:
                                        shift_occurred_spec = True
                                if param == "bbul" and spec_bbul is not None:
                                    if (current_val - prev_val) > threshold_value_bbul:
                                        shift = True
                                    if current_val > spec_bbul:
                                        shift_occurred_spec = True
                                if param == "tdep" and spec_tdep is not None:
                                    if (current_val - prev_val) > threshold_value_tdep:
                                        shift = True
                                    if current_val > spec_tdep:
                                        shift_occurred_spec = True
                                if param == "bdep" and spec_bdep is not None:
                                    if (current_val - prev_val) > threshold_value_bdep:
                                        shift = True
                                    if current_val > spec_bdep:
                                        shift_occurred_spec = True
                                if param == "upper" and spec_upper is not None:
                                    if (current_val - prev_val) > threshold_value_upper:
                                        shift = True
                                    if current_val > spec_upper:
                                        shift_occurred_spec = True
                                if param == "lower" and spec_lower is not None:
                                    if (current_val - prev_val) > threshold_value_lower:
                                        shift = True
                                    if current_val > spec_lower:
                                        shift_occurred_spec = True
                                if param == "sbal" and spec_sbal is not None:
                                    if (current_val - prev_val) > threshold_value_sbal:
                                        shift = True
                                    if current_val > spec_sbal:
                                        shift_occurred_spec = True
                                if param == "aircc" and spec_aircc is not None:
                                    if (current_val - prev_val) > threshold_value_aircc:
                                        shift = True
                                    if current_val > spec_aircc:
                                        shift_occurred_spec = True
                                if param == "aircw" and spec_aircw is not None:
                                    if (current_val - prev_val) > threshold_value_aircw:
                                        shift = True
                                    if current_val > spec_aircw:
                                        shift_occurred_spec = True
                                if param == "loadcc" and spec_loadcc is not None:
                                    if (current_val - prev_val) > threshold_value_loadcc:
                                        shift = True
                                    if current_val > spec_loadcc:
                                        shift_occurred_spec = True
                                if param == "loadcw" and spec_loadcw is not None:
                                    if (current_val - prev_val) > threshold_value_loadcw:
                                        shift = True
                                    if current_val > spec_loadcw:
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

                                if param == "rfv_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_rfppwc_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "rfh1_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_rfh1wc_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "rfh2_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_rfh2wc_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "lfv_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_lfppwc_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "conicity_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_cony_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "plysteer_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_plys_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "center_radial_run_out_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_crro_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "top_radial_run_out_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_trro_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "bottom_radial_run_out_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_brro_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "lateral_run_out_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_lro_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "lateral_run_out_harmonic_first_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_lroh1_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "center_radial_run_out_harmonic_first_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_crroh1_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "top_radial_run_out_harmonic_first_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_trroh1_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "bottom_radial_run_out_harmonic_first_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_brroh1_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "lateral_radial_run_out_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_lrro_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "static_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_sbal_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "couple_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_couple_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "upper_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_upper_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "lower_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_lower_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "overall_uniformity_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_overall_uniformity_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "overall_visual_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_overall_visual_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "overall_warehouse_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_overall_warehouse_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "overall_scrap_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_overall_scrap_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "visual_loss_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_visual_loss_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "uf_loss_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_uf_loss_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "oe_ok_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_oe_ok_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
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
                            # if param == "load1":
                            #     alert_dict["test_load_tolerance"] = test_load_tolerance
                            # if param == "airpress1":
                            #     alert_dict["pressure_tolerance"] = pressure_tolerance
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
                                if param == "cony" and spec_cony_lower is not None and spec_cony_upper is not None:
                                    if not (spec_cony_lower <= current_val <= spec_cony_upper):
                                        shift_occurred_spec = True
                                if param == "plys" and spec_plys_lower is not None and spec_plys_lower is not None:
                                    if not (spec_plys_lower <= current_val <= spec_plys_upper):
                                        shift_occurred_spec = True
                                if param == "rfppcc" and spec_rfppcc is not None:
                                    if current_val > spec_rfppcc:
                                        shift_occurred_spec = True
                                if param == "rfppcw" and spec_rfppcw is not None:
                                    if current_val > spec_rfppcw:
                                        shift_occurred_spec = True
                                if param == "rfh1cc" and spec_rfh1cc is not None:
                                    if current_val > spec_rfh1cc:
                                        shift_occurred_spec = True
                                if param == "rfh1cw" and spec_rfh1cw is not None:
                                    if current_val > spec_rfh1cw:
                                        shift_occurred_spec = True
                                if param == "rfh2cc" and spec_rfh2cc is not None:
                                    if current_val > spec_rfh2cc:
                                        shift_occurred_spec = True
                                if param == "rfh2cw" and spec_rfh2cw is not None:
                                    if current_val > spec_rfh2cw:
                                        shift_occurred_spec = True
                                if param == "lfppcc" and spec_lfppcc is not None:
                                    if current_val > spec_lfppcc:
                                        shift_occurred_spec = True
                                if param == "lfppcw" and spec_lfppcw is not None:
                                    if current_val > spec_lfppcw:
                                        shift_occurred_spec = True
                                if param == "crro" and spec_crro is not None:
                                    if current_val > spec_crro:
                                        shift_occurred_spec = True
                                if param == "trro" and spec_trro is not None:
                                    if current_val > spec_trro:
                                        shift_occurred_spec = True
                                if param == "brro" and spec_brro is not None:
                                    if current_val > spec_brro:
                                        shift_occurred_spec = True
                                if param == "lrot" and spec_lrot is not None:
                                    if current_val > spec_lrot:
                                        shift_occurred_spec = True
                                if param == "lrob" and spec_lrob is not None:
                                    if current_val > spec_lrob:
                                        shift_occurred_spec = True
                                if param == "lroh1grade" and spec_lroh1grade is not None:
                                    if current_val > spec_lroh1grade:
                                        shift_occurred_spec = True
                                if param == "tbul" and spec_tbul is not None:
                                    if current_val > spec_tbul:
                                        shift_occurred_spec = True
                                if param == "bbul" and spec_bbul is not None:
                                    if current_val > spec_bbul:
                                        shift_occurred_spec = True
                                if param == "tdep" and spec_tdep is not None:
                                    if current_val > spec_tdep:
                                        shift_occurred_spec = True
                                if param == "upper" and spec_upper is not None:
                                    if current_val > spec_upper:
                                        shift_occurred_spec = True
                                if param == "lower" and spec_lower is not None:
                                    if current_val > spec_lower:
                                        shift_occurred_spec = True
                                if param == "sbal" and spec_sbal is not None:
                                    if current_val > spec_sbal:
                                        shift_occurred_spec = True
                                if param == "aircc" and spec_aircc is not None:
                                    if current_val > spec_aircc:
                                        shift_occurred_spec = True
                                if param == "aircw" and spec_aircw is not None:
                                    if current_val > spec_aircw:
                                        shift_occurred_spec = True
                                if param == "loadcc" and spec_loadcc is not None:
                                    if current_val > spec_loadcc:
                                        shift_occurred_spec = True
                                if param == "loadcw" and spec_loadcw is not None:
                                    if current_val > spec_loadcw:
                                        shift_occurred_spec = True
                            
                            ############### Add 16-07 start ######################

                                if param in yield_params:
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if current_val < spec_yield:
                                            shift_occurred_spec = True
                                # if param == "load1" and test_load is not None and test_load_tolerance is not None:
                                #     if not (test_load - test_load_tolerance <= current_val <= test_load + test_load_tolerance):
                                #         shift_occurred_spec = True
                                # if param == "airpress1" and pressure is not None and pressure_tolerance is not None:
                                #     if not (pressure - pressure_tolerance <= current_val <= pressure + pressure_tolerance):
                                #         shift_occurred_spec = True
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
                            # if param == "load1":
                            #     alert_dict["test_load_tolerance"] = test_load_tolerance
                            # if param == "airpress1":
                            #     alert_dict["pressure_tolerance"] = pressure_tolerance
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
                            "sku": model_number,
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
                        "sku": model_number,
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
                        "sku": model_number,
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
                        "overall_uniformity_yield": overall_uniformity_yield(model_data),
                        "overall_warehouse_yield": overall_warehouse_yield(model_data),
                        "overall_visual_yield": overall_visual_yield(model_data),
                        "overall_scrap_yield": overall_scrap_yield(model_data),
                        "visual_loss_yield": visual_loss_yield(model_data),
                        "uf_loss_yield": uf_loss_yield(model_data),
                        "oe_ok_yield": oe_ok_yield(model_data),
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
                        # "rank_columns": get_rank_columns(model_data),
                        "barcodes": model_data["barcode"].tolist()
                    }

                    alert_key = (model_number, unique_lot_id_current)
                    if alert_key not in alert_map:
                        last_alert_id += 1
                        alert_map[alert_key] = {
                            "alert_id_tbm_ufm": last_alert_id,
                            "sku": model_number,
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
                            "sku": alert_obj["sku"],
                            "lot_id": alert_obj["lot_id"],
                            "alert_flag": False,
                            "dashboard_flag": False,
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
                        # alert_db_dashboard is handled via triggering_bulk_operations bulk write at the end
                        
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
                                "sku": alert_obj["sku"],
                                "lot_id": alert_obj["lot_id"],
                                "alert_flag": False,
                                "dashboard_flag": False,
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
                                    "sku": model_number,
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
                                "sku": alert_obj["sku"],
                                "lot_id": alert_obj["lot_id"],
                                "alert_flag": False,
                                "dashboard_flag": False,
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
    logging.info(f"[TBM_NEW] Bulk ops summary: lot={len(bulk_operations)}, alert={len(alert_bulk_operations)}, averages={len(averages_bulk_operations)}, triggering={len(triggering_bulk_operations)}, deviated={len(deviated_triggering_bulk_operations)}")
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
        merge_collect = client[TUB_CIMCORP_VISUAL_MERGED_DB_alert][DATA_MERGED_SUMMARY_COLLECTION_alert_new]
        channel_collect_curing = client[config_databse][channel_no_collect_curing]
        channel_collect_ufm = client[config_databse][channel_no_collect]
        lot_curing_collect = client[lot_db][lot_curing_collect_name_new]
        alert_collect = client[alert_table][alert_curing_collect_name_new]
        averages_collect = client[average_db][averages_curing_table_name_new]
        machine_collect = client[config_databse][machine_no_collect]
        curing_press_collect = client[config_databse][curing_press_dict_collect]
        alert_triggering_collect = client[alert_db][alert_triggering_curing_collect_name_new]
        # summary_triggering_collect = client[summary_db][summary_triggering_collect_name_tbm]
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
        curing_collect_data = client[model_db][model_no_status_collection]
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
        active_modelnos = set(curing_collect_data.distinct("sku", {"status": "active"}))
        output_messages.append(f"✅ Fetched {len(active_modelnos)} active model numbers: {active_modelnos}")
    except Exception as e:
        output_messages.append(f"❌ Error fetching active model numbers: {e}")
        return {"status": "error", "message": "Failed to fetch active model numbers", "output_log": output_messages}, 500

    try:
        merge1 = list(merge_collect.find({}))
        merge_df = pd.DataFrame(merge1)
        channel_data_curing = list(channel_collect_curing.find({"sku": {"$in": list(active_modelnos)}}))
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

    channel_modelnos = set(channel_collect_curing.distinct("sku"))
    merge_df = merge_df[merge_df["sku"].isin(channel_modelnos)]
    merge_df = merge_df[merge_df["alert_id_curing_ufm"].isna()]

    # Ensure string columns are object dtype so string values can be assigned without float64 errors
    for _col in ["lot_id", "previous_lot_id", "curing_machine_model_lot_id", "curing_previous_machine_model_lot_id"]:
        if _col in merge_df.columns:
            merge_df[_col] = merge_df[_col].astype(object)
        else:
            merge_df[_col] = None

    # merge_df["date_of_tbm"] = merge_df["tbm_buildtime"]
    # merge_df["date_of_curing"] = merge_df["curing_infeedtime"]
    # merge_df["date_and_time"] = merge_df["date"].fillna('') + " " + merge_df["time"].fillna('')
    
    merge_df["date_of_tbm"] = merge_df["ptb_time"]
    merge_df["date_of_curing"] = merge_df["cur_time"]
    merge_df["date_and_time"] = merge_df["uf_time"]

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
        # merge_df["time_of_ufm"] = pd.to_datetime(merge_df["time"], errors="coerce").dt.strftime("%H:%M:%S")
        # time_invalid_mask = merge_df["time_of_ufm"].isna() & merge_df["time"].notna()
        # if time_invalid_mask.any():
        #     logging.info("⚠️ Found %s invalid time values", time_invalid_mask.sum())
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
                    "sku": {"$in": list(active_modelnos)},
                    "machine_data.cur_id": {"$exists": True}
                }
            },
            {
                "$project": {
                    "sku": "$sku",
                    "machine": "$machine_data.cur_id",
                    "lot_id_parts": {"$split": ["$lot_id", "_"]},
                    "curing_machine_model_lot_id": "$machine_data.curing_machine_model_lot_id"
                }
            },
            {
                "$project": {
                    "sku": 1,
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
                    "_id": {"sku": "$sku", "machine": "$machine"},
                    "max_lot_number": {"$max": "$lot_number"},
                    "last_curing_machine_model_lot_id": {"$last": "$curing_machine_model_lot_id"}
                }
            }
        ]))
        last_lot_dict = {
            (item["_id"]["sku"], item["_id"]["machine"]): {
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

    conicity_params = [
        "rfppcc","rfppcw","rfh1cc","rfh1cw","rfh2cc","rfh2cw","lfppcc","lfppcw","cony","plys",
        "crro","trro","brro","lrot","lrob","lroh1grade","tbul","bbul","tdep","bdep",
        "upper","lower","sbal","aircc","aircw","loadcc","loadcw"
    ]
    yield_params = [
        "rfv_yield", "rfh1_yield", "rfh2_yield", "lfv_yield", "conicity_yield", "plysteer_yield",
        "center_radial_run_out_yield", "top_radial_run_out_yield", "bottom_radial_run_out_yield", 
        "lateral_run_out_yield", "lateral_run_out_harmonic_first_yield", "center_radial_run_out_harmonic_first_yield",
        "top_radial_run_out_harmonic_first_yield", "bottom_radial_run_out_harmonic_first_yield",
        "lateral_radial_run_out_yield", "static_yield", "couple_yield",
        "upper_yield", "lower_yield", "overall_uniformity_yield", "overall_visual_yield", "overall_warehouse_yield",
        "overall_scrap_yield", "visual_loss_yield", "uf_loss_yield", "oe_ok_yield"
    ]
    
    # Define rank parameter mappings
    rank_param_mappings = {
        "rfv_yield": ["rfppwcgrade"],
        "rfh1_yield": ["rfh1wcgrade"],
        "rfh2_yield": ["rfh2wcgrade"],
        "lfv_yield": ["lfppwcgrade"],
        "conicity_yield": ["conygrade"],
        "plysteer_yield": ["plysgrade"],
        "center_radial_run_out_yield": ["crrograde"], 
        "top_radial_run_out_yield": ["trrograde"], 
        "bottom_radial_run_out_yield": ["brrograde"],
        "lateral_run_out_yield": ["lrograde"],
        "lateral_run_out_harmonic_first_yield": ["lroh1grade"],
        "center_radial_run_out_harmonic_first_yield": ["crroh1grade"],
        "top_radial_run_out_harmonic_first_yield": ["trroh1grade"],
        "bottom_radial_run_out_harmonic_first_yield": ["brroh1grade"],
        "lateral_radial_run_out_yield": ["lrrograde"],
        "static_yield": ["sbalgrade"],
        "couple_yield": ["couplegrade"],
        "upper_yield": ["uppergrade"],
        "lower_yield": ["lowergrade"]
    }

    # Model-level processing
    barcode_counts = merge_df.groupby("sku").size().reset_index(name="total_barcodes")
    for _, row in barcode_counts.iterrows():
        try:
            model_number = row["sku"]
            if model_number not in active_modelnos:
                output_messages.append(f"Skipping CURING model {model_number} as it is not active.")
                continue
            total_barcodes = row["total_barcodes"]
            timestamp_averages = some_function()

            # model_data = merge_df[merge_df["sku"] == model_number].copy()
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
                "rfv_yield": rfv_yield(model_data),
                "rfh1_yield": rfh1_yield(model_data),
                "rfh2_yield": rfh2_yield(model_data),
                "lfv_yield": lfv_yield(model_data),
                "conicity_yield": conicity_yield(model_data),
                "plysteer_yield": plysteer_yield(model_data),
                "center_radial_run_out_yield": center_radial_run_out_yield(model_data),
                "top_radial_run_out_yield": top_radial_run_out_yield(model_data),
                "bottom_radial_run_out_yield": bottom_radial_run_out_yield(model_data),
                "lateral_run_out_yield": lateral_run_out_yield(model_data),
                "lateral_run_out_harmonic_first_yield": lateral_run_out_harmonic_first_yield(model_data),
                "center_radial_run_out_harmonic_first_yield": center_radial_run_out_harmonic_first_yield(model_data),
                "top_radial_run_out_harmonic_first_yield": top_radial_run_out_harmonic_first_yield(model_data),
                "bottom_radial_run_out_harmonic_first_yield": bottom_radial_run_out_harmonic_first_yield(model_data),
                "lateral_radial_run_out_yield": lateral_radial_run_out_yield(model_data),
                "static_yield": static_yield(model_data),
                "couple_yield": couple_yield(model_data),
                "upper_yield": upper_yield(model_data),
                "lower_yield": lower_yield(model_data),
                "overall_uniformity_yield": overall_uniformity_yield(model_data),
                "overall_visual_yield": overall_visual_yield(model_data),
                "overall_warehouse_yield": overall_warehouse_yield(model_data),
                "overall_scrap_yield": overall_scrap_yield(model_data),
                "visual_loss_yield": visual_loss_yield(model_data),
                "uf_loss_yield": uf_loss_yield(model_data),
                "oe_ok_yield": oe_ok_yield(model_data)
            })

            grouped_averages = {}
            try:
                model_data["uf_id"] = model_data["uf_id"].astype(str).str.strip()
                model_data.sort_values("date_and_time", ascending=True, inplace=True)
                ubno_grouped = model_data.groupby("uf_id")
                ubno_records = []
                for uf_id, group in ubno_grouped:
                    group.sort_values("date_and_time", ascending=True)
                    record = {
                        "uf_id": uf_id,
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
                grouped_averages["uf_id"] = ubno_records
            except Exception as e:
                grouped_averages["uf_id"] = f"Error: {e}"
                output_messages.append(f"⚠️ Error calculating uf_id averages for model {model_number}: {e}")

            model_data.sort_values("date_and_time", ascending=True, inplace=True)
            tub_date_start_time = model_data["date_and_time"].iloc[0] if not model_data["date_and_time"].empty else None
            tub_date_end_time = model_data["date_and_time"].iloc[-1] if not model_data["date_and_time"].empty else None
            date_of_ufm = model_data['date_of_ufm'].iloc[0] if 'date_of_ufm' in model_data and not model_data['date_of_ufm'].empty else None
            date_of_tbm = model_data['date_of_tbm'].iloc[0] if 'date_of_tbm' in model_data and not model_data['date_of_tbm'].empty else None
            lot_start_time = some_function()
            lot_end_time = some_function()

            model_lot_doc = {
                "sku": model_number,
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
            model_number = str(row.get("sku", "")).strip()
            gt_codes = row.get("gt_code", [])

            # Ensure gt_codes is a list
            if not isinstance(gt_codes, list):
                gt_codes = [gt_codes]
            ############# Add 16-07 end ##############

            spec_cony_upper = float(row.get("spec_cony_upper", 0))
            spec_cony_lower = float(row.get("spec_cony_lower", 0))
            spec_plys_upper = float(row.get("spec_plys_upper", 0))
            spec_plys_lower = float(row.get("spec_plys_lower", 0))
            sizecode = str(row.get("sizecode", ""))
            spec_rfppcc = float(row.get("spec_rfppcc", 0))
            spec_rfppcw = float(row.get("spec_rfppcw", 0))
            spec_rfh1cc = float(row.get("spec_rfh1cc", 0))
            spec_rfh1cw = float(row.get("spec_rfh1cw", 0))
            spec_rfh2cc = float(row.get("spec_rfh2cc", 0))
            spec_rfh2cw = float(row.get("spec_rfh2cw", 0))
            spec_lfppcc = float(row.get("spec_lfppcc", 0))
            spec_lfppcw = float(row.get("spec_lfppcw", 0))
            spec_crro = float(row.get("spec_crro", 0))
            spec_trro = float(row.get("spec_trro", 0))
            spec_brro = float(row.get("spec_brro", 0))
            spec_lrot = float(row.get("spec_lrot", 0))
            spec_lrob = float(row.get("spec_lrob", 0))
            spec_lroh1grade = float(row.get("spec_lroh1grade", 0))
            spec_tbul = float(row.get("spec_tbul", 0))
            spec_bbul = float(row.get("spec_bbul", 0))
            spec_tdep = float(row.get("spec_tdep", 0))
            spec_bdep = float(row.get("spec_bdep", 0))
            spec_upper = float(row.get("spec_upper", 0))
            spec_lower = float(row.get("spec_lower", 0))
            spec_sbal = float(row.get("spec_sbal", 0))
            spec_aircc = float(row.get("spec_aircc", 0))
            spec_aircw = float(row.get("spec_aircw", 0))
            spec_loadcc = float(row.get("spec_yield", 0))
            spec_loadcw = float(row.get("spec_yield", 0))

            ############### Add 16-07 start ######################

            # total_rank_yield_spec_yield = float(row.get("total_rank_yield_spec_yield", 0))
            spec_rfppwc_yield = float(row.get("spec_rfppwc_yield", 0))
            spec_rfh1wc_yield = float(row.get("spec_rfh1wc_yield", 0))
            spec_rfh2wc_yield = float(row.get("spec_rfh2wc_yield", 0))
            spec_lfppwc_yield = float(row.get("spec_lfppwc_yield", 0))
            spec_cony_yield = float(row.get("spec_cony_yield", 0))
            spec_plys_yield = float(row.get("spec_plys_yield", 0))
            spec_crro_yield = float(row.get("spec_crro_yield", 0))
            spec_trro_yield = float(row.get("spec_trro_yield", 0))
            spec_brro_yield = float(row.get("spec_brro_yield", 0))
            spec_lro_yield = float(row.get("spec_lro_yield", 0))
            spec_lroh1_yield = float(row.get("spec_lroh1_yield", 0))
            spec_crroh1_yield = float(row.get("spec_crroh1_yield", 0))
            spec_trroh1_yield = float(row.get("spec_trroh1_yield", 0))
            spec_brroh1_yield = float(row.get("spec_brroh1_yield", 0))
            spec_lrro_yield = float(row.get("spec_lrro_yield", 0))
            spec_sbal_yield = float(row.get("spec_sbal_yield", 0))
            spec_couple_yield = float(row.get("spec_couple_yield", 0))
            spec_upper_yield = float(row.get("spec_upper_yield", 0))
            spec_lower_yield = float(row.get("spec_lower_yield", 0))
            spec_overall_uniformity_yield = float(row.get("spec_overall_uniformity_yield", 0))
            spec_overall_visual_yield = float(row.get("spec_overall_visual_yield", 0))
            spec_overall_warehouse_yield = float(row.get("spec_overall_warehouse_yield", 0))
            spec_overall_scrap_yield = float(row.get("spec_overall_scrap_yield", 0))
            spec_visual_loss_yield = float(row.get("spec_visual_loss_yield", 0))
            spec_uf_loss_yield = float(row.get("spec_uf_loss_yield", 0))
            spec_oe_ok_yield = float(row.get("spec_oe_ok_yield", 0))

            ############### Add 16-07 end ######################

            threshold_value_plys = float(row.get("threshold_value_plys", 0.5))
            threshold_value_cony = float(row.get("threshold_value_cony", 0.5))
            threshold_value_rfppcc = float(row.get("threshold_value_rfppcc", 0.5))
            threshold_value_rfppcw = float(row.get("threshold_value_rfppcw", 0.5))
            threshold_value_rfh1cc = float(row.get("threshold_value_rfh1cc", 0.5))
            threshold_value_rfh1cw = float(row.get("threshold_value_rfh1cw", 0.5))
            threshold_value_rfh2cc = float(row.get("threshold_value_rfh2cc", 0.5))
            threshold_value_rfh2cw = float(row.get("threshold_value_rfh2cw", 0.5))
            threshold_value_lfppcc = float(row.get("threshold_value_lfppcc", 0.5))
            threshold_value_lfppcw = float(row.get("threshold_value_lfppcw", 0.5))
            threshold_value_crro = float(row.get("threshold_value_crro", 5))
            threshold_value_trro = float(row.get("threshold_value_trro", 5))
            threshold_value_brro = float(row.get("threshold_value_brro", 5))
            threshold_value_lrot = float(row.get("threshold_value_lrot", 5))
            threshold_value_lrob = float(row.get("threshold_value_lrob", 5))
            threshold_value_lroh1grade = float(row.get("threshold_value_lroh1grade", 5))
            threshold_value_tbul = float(row.get("threshold_value_tbul", 0))
            threshold_value_bbul = float(row.get("threshold_value_bbul", 5))
            threshold_value_tdep = float(row.get("threshold_value_tdep", 5))
            threshold_value_bdep = float(row.get("threshold_value_bdep", 5))
            threshold_value_upper = float(row.get("threshold_value_upper", 5))
            threshold_value_lower = float(row.get("threshold_value_lower", 5))
            threshold_value_sbal = float(row.get("threshold_value_sbal", 5))
            threshold_value_aircc = float(row.get("threshold_value_aircc", 5))
            threshold_value_aircw = float(row.get("threshold_value_aircw", 5))
            threshold_value_loadcc = float(row.get("threshold_value_loadcc", 5))
            threshold_value_loadcw = float(row.get("threshold_value_loadcw", 5))
            threshold_value_rfppwc_yield = float(row.get("threshold_value_rfppwc_yield", 5))
            threshold_value_rfh1wc_yield = float(row.get("threshold_value_rfh1wc_yield", 5))
            threshold_value_rfh2wc_yield = float(row.get("threshold_value_rfh2wc_yield", 5))
            threshold_value_lfppwc_yield = float(row.get("threshold_value_lfppwc_yield", 5))
            threshold_value_cony_yield = float(row.get("threshold_value_cony_yield", 5))
            threshold_value_plys_yield = float(row.get("threshold_value_plys_yield", 5))
            threshold_value_crro_yield = float(row.get("threshold_value_crro_yield", 5))
            threshold_value_trro_yield = float(row.get("threshold_value_trro_yield", 5))
            threshold_value_brro_yield = float(row.get("threshold_value_brro_yield", 5))
            threshold_value_lro_yield = float(row.get("threshold_value_lro_yield", 5))
            threshold_value_lroh1_yield = float(row.get("threshold_value_lroh1_yield", 5))
            threshold_value_crroh1_yield = float(row.get("threshold_value_crroh1_yield", 5))
            threshold_value_trroh1_yield = float(row.get("threshold_value_trroh1_yield", 5))
            threshold_value_brroh1_yield = float(row.get("threshold_value_brroh1_yield", 5))
            threshold_value_lrro_yield = float(row.get("threshold_value_lrro_yield", 5))
            threshold_value_sbal_yield = float(row.get("threshold_value_sbal_yield", 5))
            threshold_value_couple_yield = float(row.get("threshold_value_couple_yield", 5))
            threshold_value_upper_yield = float(row.get("threshold_value_upper_yield", 5))
            threshold_value_lower_yield = float(row.get("threshold_value_lower_yield", 5))
            threshold_value_overall_uniformity_yield = float(row.get("threshold_value_overall_uniformity_yield", 5))
            threshold_value_overall_visual_yield = float(row.get("threshold_value_overall_visual_yield", 5))
            threshold_value_overall_warehouse_yield = float(row.get("threshold_value_overall_warehouse_yield", 5))
            threshold_value_overall_scrap_yield = float(row.get("threshold_value_overall_scrap_yield", 5))
            threshold_value_visual_loss_yield = float(row.get("threshold_value_visual_loss_yield", 5))
            threshold_value_uf_loss_yield = float(row.get("threshold_value_uf_loss_yield", 5))
            threshold_value_oe_ok_yield = float(row.get("threshold_value_oe_ok_yield", 5))

            required_columns = ["sku", "barcode", "date_of_curing", "date_and_time"] + conicity_params
            missing_columns = [col for col in required_columns if col not in merge_df.columns]
            if missing_columns:
                error_message = f"⚠️ Skipping model number {model_number}: Missing required columns: {missing_columns}"
                output_messages.append(error_message)
                continue

            merge_df["sku"] = merge_df["sku"].astype(str).str.strip()
            # model_data = merge_df[(merge_df["sku"] == str(model_number)) &
            #                      (merge_df["modelno_cimcorp"] == str(model_number))].copy()

            ############# Add 16-07 start ##############
            for gt_code in gt_codes:
                gt_code = str(gt_code).strip()

                # Filter merge_df for each gt_code individually
                model_data = merge_df[
                    (merge_df["sku"] == model_number) &
                    # (merge_df["modelno_cimcorp"] == model_number) &
                    (merge_df["gt_code"] == gt_code)
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

                for (curing_press, curing_mouldid), curing_data in model_data.groupby(["cur_id", "cur_mouldid"]):
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
                        "sku": model_number,
                        "lot_id": unique_lot_id_current,
                        "previous_lot_id": previous_lot_id,
                        "processed_barcodes": int(total_curing_barcodes),
                        "total_barcodes": int(total_barcodes_model),
                        "timestamp_lot_generated": some_function(),
                        "machine_data": {
                            "cur_id": machine_no,
                            "curing_machine_model_lot_id": current_curing_machine_model_lot_id,
                            "curing_previous_machine_model_lot_id": curing_previous_machine_model_lot_id,
                            "processed_machine_barcodes": int(processed_machine_barcodes),
                            "total_machine_barcodes": int(total_curing_barcodes),
                            "timestamp_lot_generated": some_function()
                        }
                    }
                    lot_query = {
                        "sku": model_number,
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
                        "rfv_yield": rfv_yield(curing_data),
                        "rfh1_yield": rfh1_yield(curing_data),
                        "rfh2_yield": rfh2_yield(curing_data),
                        "lfv_yield": lfv_yield(curing_data),
                        "conicity_yield": conicity_yield(curing_data),
                        "plysteer_yield": plysteer_yield(curing_data),
                        "center_radial_run_out_yield": center_radial_run_out_yield(curing_data),
                        "top_radial_run_out_yield": top_radial_run_out_yield(curing_data),
                        "bottom_radial_run_out_yield": bottom_radial_run_out_yield(curing_data),
                        "lateral_run_out_yield": lateral_run_out_yield(curing_data),
                        "lateral_run_out_harmonic_first_yield": lateral_run_out_harmonic_first_yield(curing_data),
                        "center_radial_run_out_harmonic_first_yield": center_radial_run_out_harmonic_first_yield(curing_data),
                        "top_radial_run_out_harmonic_first_yield": top_radial_run_out_harmonic_first_yield(curing_data),
                        "bottom_radial_run_out_harmonic_first_yield": bottom_radial_run_out_harmonic_first_yield(curing_data),
                        "lateral_radial_run_out_yield": lateral_radial_run_out_yield(curing_data),
                        "static_yield": static_yield(curing_data),
                        "couple_yield": couple_yield(curing_data),
                        "upper_yield": upper_yield(curing_data),
                        "lower_yield": lower_yield(curing_data),
                        "overall_uniformity_yield": overall_uniformity_yield(curing_data),
                        "overall_visual_yield": overall_visual_yield(curing_data),
                        "overall_warehouse_yield": overall_warehouse_yield(curing_data),
                        "overall_scrap_yield": overall_scrap_yield(curing_data),
                        "visual_loss_yield": visual_loss_yield(curing_data),
                        "uf_loss_yield": uf_loss_yield(curing_data),
                        "oe_ok_yield": oe_ok_yield(curing_data)
                    })

                    param_spec_map = {
                        "cony": spec_cony_upper,
                        "cony": spec_cony_lower,
                        "plys": spec_plys_upper,
                        "plys": spec_plys_lower,
                        "rfppcc": spec_rfppcc,
                        "rfppcw": spec_rfppcw,
                        "rfh1cc": spec_rfh1cc,
                        "rfh1cw": spec_rfh1cw,
                        "rfh2cc": spec_rfh2cc,
                        "rfh2cw": spec_rfh2cw,
                        "lfppcc": spec_lfppcc,
                        "lfppcw": spec_lfppcw,
                        "crro": spec_crro,
                        "trro": spec_trro,
                        "brro": spec_brro,
                        "lrot": spec_lrot,
                        "lrob": spec_lrob,
                        "lroh1grade": spec_lroh1grade,
                        "tbul": spec_tbul,
                        "bbul": spec_bbul,
                        "tdep": spec_tdep,
                        "upper": spec_upper,
                        "lower": spec_lower,
                        "sbal": spec_sbal,
                        "aircc": spec_aircc,
                        "aircw": spec_aircw,
                        "loadcc": spec_loadcc,
                        "loadcw": spec_loadcw,

                    ############### Add 16-07 start ######################

                        "rfv_yield": spec_rfppwc_yield,
                        "rfh1_yield": spec_rfh1wc_yield,
                        "rfh2_yield": spec_rfh2wc_yield,
                        "lfv_yield": spec_lfppwc_yield,
                        "conicity_yield": spec_cony_yield,
                        "plysteer_yield": spec_plys_yield,
                        "center_radial_run_out_yield": spec_crro_yield,
                        "top_radial_run_out_yield": spec_trro_yield,
                        "bottom_radial_run_out_yield": spec_brro_yield,
                        "lateral_run_out_yield": spec_lro_yield,
                        "lateral_run_out_harmonic_first_yield": spec_lroh1_yield,
                        "center_radial_run_out_harmonic_first_yield": spec_crroh1_yield,
                        "top_radial_run_out_harmonic_first_yield": spec_trroh1_yield,
                        "bottom_radial_run_out_harmonic_first_yield": spec_brroh1_yield,
                        "lateral_radial_run_out_yield": spec_lrro_yield,
                        "static_yield": spec_sbal_yield,
                        "couple_yield": spec_couple_yield,
                        "upper_yield": spec_upper_yield,
                        "lower_yield": spec_lower_yield,
                        "overall_uniformity_yield": spec_overall_uniformity_yield,
                        "overall_visual_yield": spec_overall_visual_yield,
                        "overall_warehouse_yield": spec_overall_warehouse_yield,
                        "overall_scrap_yield": spec_overall_scrap_yield,
                        "visual_loss_yield": spec_visual_loss_yield,
                        "uf_loss_yield": spec_uf_loss_yield,
                        "oe_ok_yield": spec_oe_ok_yield
                    }

                    grouped_averages = {}
                    try:
                        curing_data["uf_id"] = curing_data["uf_id"].astype(str).str.strip()
                        curing_data.sort_values("date_and_time", ascending=True, inplace=True)
                        ubno_grouped = curing_data.groupby("uf_id")
                        ubno_records = []
                        for uf_id, group in ubno_grouped:
                            if group.empty:
                                output_messages.append(f"⚠️ Empty group for uf_id {uf_id} in machine {machine_no}")
                                continue
                            group.sort_values("date_and_time", ascending=True)
                            record = {
                                "uf_id": uf_id,
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
                                        output_messages.append(f"⚠️ Non-numeric values in {param} for uf_id {uf_id}: {non_numeric.tolist()[:5]}")
                                    record[f"avg_{param}"] = float(valid_vals.mean()) if not valid_vals.empty else None
                                else:
                                    record[f"avg_{param}"] = None
                                    output_messages.append(f"⚠️ Column {param} missing in uf_id {uf_id} for machine {machine_no}")
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
                                    if param == "cony" and spec_cony_lower is not None and spec_cony_upper is not None and avg_val is not None:
                                        exceeded = not (-spec_cony_lower <= avg_val <= spec_cony_upper)
                                    if param == "plys" and spec_plys_lower is not None and spec_plys_upper is not None and avg_val is not None:
                                        exceeded = not (spec_plys_lower <= avg_val <= spec_plys_upper)
                                    if spec is not None and param not in ["cony", "plys"] and avg_val is not None:
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
                        grouped_averages["uf_id"] = ubno_records
                    except Exception as e:
                        grouped_averages["uf_id"] = f"Error: {e}"
                        output_messages.append(f"⚠️ Error calculating uf_id averages for model {model_number}, machine {machine_no}: {e}")

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
                                if param == "cony" and spec_cony_lower is not None and spec_cony_upper is not None:
                                    if abs(current_val - prev_val) > threshold_value_cony:
                                        shift = True
                                    if not (-spec_cony_lower <= current_val <= spec_cony_upper):
                                        shift_occurred_spec = True
                                if param == "plys" and spec_plys_lower is not None and spec_plys_upper is not None:
                                    if abs(current_val - prev_val) > threshold_value_plys:
                                        shift = True
                                    if not (-spec_plys_lower <= current_val <= spec_plys_upper):
                                        shift_occurred_spec = True
                                if param == "rfppcc" and spec_rfppcc is not None:
                                    if (current_val - prev_val) > threshold_value_rfppcc:
                                        shift = True
                                    if current_val > spec_rfppcc:
                                        shift_occurred_spec = True
                                if param == "rfppcw" and spec_rfppcw is not None:
                                    if (current_val - prev_val) > threshold_value_rfppcw:
                                        shift = True
                                    if current_val > spec_rfppcw:
                                        shift_occurred_spec = True
                                if param == "rfh1cc" and spec_rfh1cc is not None:
                                    if (current_val - prev_val) > threshold_value_rfh1cc:
                                        shift = True
                                    if current_val > spec_rfh1cc:
                                        shift_occurred_spec = True
                                if param == "rfh1cw" and spec_rfh1cw is not None:
                                    if (current_val - prev_val) > threshold_value_rfh1cw:
                                        shift = True
                                    if current_val > spec_rfh1cw:
                                        shift_occurred_spec = True
                                if param == "rfh2cc" and spec_rfh2cc is not None:
                                    if (current_val - prev_val) > threshold_value_rfh2cc:
                                        shift = True
                                    if current_val > spec_rfh2cc:
                                        shift_occurred_spec = True
                                if param == "rfh2cw" and spec_rfh2cw is not None:
                                    if (current_val - prev_val) > threshold_value_rfh2cw:
                                        shift = True
                                    if current_val > spec_rfh2cw:
                                        shift_occurred_spec = True
                                if param == "lfppcc" and spec_lfppcc is not None:
                                    if (current_val - prev_val) > threshold_value_lfppcc:
                                        shift = True
                                    if current_val > spec_lfppcc:
                                        shift_occurred_spec = True
                                if param == "lfppcw" and spec_lfppcw is not None:
                                    if (current_val - prev_val) > threshold_value_lfppcw:
                                        shift = True
                                    if current_val > spec_lfppcw:
                                        shift_occurred_spec = True
                                if param == "crro" and spec_crro is not None:
                                    if (current_val - prev_val) > threshold_value_crro:
                                        shift = True
                                    if current_val > spec_crro:
                                        shift_occurred_spec = True
                                if param == "trro" and spec_trro is not None:
                                    if (current_val - prev_val) > threshold_value_trro:
                                        shift = True
                                    if current_val > spec_trro:
                                        shift_occurred_spec = True
                                if param == "brro" and spec_brro is not None:
                                    if (current_val - prev_val) > threshold_value_brro:
                                        shift = True
                                    if current_val > spec_brro:
                                        shift_occurred_spec = True
                                if param == "lrot" and spec_lrot is not None:
                                    if (current_val - prev_val) > threshold_value_lrot:
                                        shift = True
                                    if current_val > spec_lrot:
                                        shift_occurred_spec = True
                                if param == "lrob" and spec_lrob is not None:
                                    if (current_val - prev_val) > threshold_value_lrob:
                                        shift = True
                                    if current_val > spec_lrob:
                                        shift_occurred_spec = True
                                if param == "lroh1grade" and spec_lroh1grade is not None:
                                    if (current_val - prev_val) > threshold_value_lroh1grade:
                                        shift = True
                                    if current_val > spec_lroh1grade:
                                        shift_occurred_spec = True
                                if param == "tbul" and spec_tbul is not None:
                                    if (current_val - prev_val) > threshold_value_tbul:
                                        shift = True
                                    if current_val > spec_tbul:
                                        shift_occurred_spec = True
                                if param == "bbul" and spec_bbul is not None:
                                    if (current_val - prev_val) > threshold_value_bbul:
                                        shift = True
                                    if current_val > spec_bbul:
                                        shift_occurred_spec = True
                                if param == "tdep" and spec_tdep is not None:
                                    if (current_val - prev_val) > threshold_value_tdep:
                                        shift = True
                                    if current_val > spec_tdep:
                                        shift_occurred_spec = True
                                if param == "bdep" and spec_bdep is not None:
                                    if (current_val - prev_val) > threshold_value_bdep:
                                        shift = True
                                    if current_val > spec_bdep:
                                        shift_occurred_spec = True
                                if param == "upper" and spec_upper is not None:
                                    if (current_val - prev_val) > threshold_value_upper:
                                        shift = True
                                    if current_val > spec_upper:
                                        shift_occurred_spec = True
                                if param == "lower" and spec_lower is not None:
                                    if (current_val - prev_val) > threshold_value_lower:
                                        shift = True
                                    if current_val > spec_lower:
                                        shift_occurred_spec = True
                                if param == "sbal" and spec_sbal is not None:
                                    if (current_val - prev_val) > threshold_value_sbal:
                                        shift = True
                                    if current_val > spec_sbal:
                                        shift_occurred_spec = True
                                if param == "aircc" and spec_aircc is not None:
                                    if (current_val - prev_val) > threshold_value_aircc:
                                        shift = True
                                    if current_val > spec_aircc:
                                        shift_occurred_spec = True
                                if param == "aircw" and spec_aircw is not None:
                                    if (current_val - prev_val) > threshold_value_aircw:
                                        shift = True
                                    if current_val > spec_aircw:
                                        shift_occurred_spec = True
                                if param == "loadcc" and spec_loadcc is not None:
                                    if (current_val - prev_val) > threshold_value_loadcc:
                                        shift = True
                                    if current_val > spec_loadcc:
                                        shift_occurred_spec = True
                                if param == "loadcw" and spec_loadcw is not None:
                                    if (current_val - prev_val) > threshold_value_loadcw:
                                        shift = True
                                    if current_val > spec_loadcw:
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

                                if param == "rfv_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_rfppwc_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "rfh1_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_rfh1wc_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "rfh2_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_rfh2wc_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "lfv_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_lfppwc_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "conicity_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_cony_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "plysteer_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_plys_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "center_radial_run_out_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_crro_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "top_radial_run_out_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_trro_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "bottom_radial_run_out_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_brro_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "lateral_run_out_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_lro_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "lateral_run_out_harmonic_first_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_lroh1_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "center_radial_run_out_harmonic_first_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_crroh1_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "top_radial_run_out_harmonic_first_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_trroh1_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "bottom_radial_run_out_harmonic_first_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_brroh1_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "lateral_radial_run_out_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_lrro_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "static_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_sbal_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "couple_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_couple_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "upper_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_upper_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "lower_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_lower_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "overall_uniformity_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_overall_uniformity_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "overall_visual_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_overall_visual_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "overall_warehouse_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_overall_warehouse_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "overall_scrap_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_overall_scrap_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "visual_loss_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_visual_loss_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "uf_loss_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_uf_loss_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True
                                if param == "oe_ok_yield":
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_oe_ok_yield)):
                                            shift = True
                                        if abs(current_val < spec_yield):
                                            shift_occurred_spec = True

                                # if param == "load1" and test_load is not None and test_load_tolerance is not None:
                                #     if (current_val - prev_val) > test_load_tolerance:
                                #         shift = True
                                #     if not (test_load - test_load_tolerance <= current_val <= test_load + test_load_tolerance):
                                #         shift_occurred_spec = True
                                # if param == "airpress1" and pressure is not None and pressure_tolerance is not None:
                                #     if (current_val - prev_val) > pressure_tolerance:
                                #         shift = True
                                #     if not (pressure - pressure_tolerance <= current_val <= pressure + pressure_tolerance):
                                #         shift_occurred_spec = True
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
                            # if param == "load1":
                            #     alert_dict["test_load_tolerance"] = test_load_tolerance
                            # if param == "airpress1":
                            #     alert_dict["pressure_tolerance"] = pressure_tolerance
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
                                if param == "con_kgf" and spec_cony_lower is not None and spec_cony_upper is not None:
                                    if not (spec_cony_lower <= current_val <= spec_cony_upper):
                                        shift_occurred_spec = True
                                if param == "plys" and spec_plys_lower is not None and spec_plys_lower is not None:
                                    if not (spec_plys_lower <= current_val <= spec_plys_upper):
                                        shift_occurred_spec = True
                                if param == "rfppcc" and spec_rfppcc is not None:
                                    if current_val > spec_rfppcc:
                                        shift_occurred_spec = True
                                if param == "rfppcw" and spec_rfppcw is not None:
                                    if current_val > spec_rfppcw:
                                        shift_occurred_spec = True
                                if param == "rfh1cc" and spec_rfh1cc is not None:
                                    if current_val > spec_rfh1cc:
                                        shift_occurred_spec = True
                                if param == "rfh1cw" and spec_rfh1cw is not None:
                                    if current_val > spec_rfh1cw:
                                        shift_occurred_spec = True
                                if param == "rfh2cc" and spec_rfh2cc is not None:
                                    if current_val > spec_rfh2cc:
                                        shift_occurred_spec = True
                                if param == "rfh2cw" and spec_rfh2cw is not None:
                                    if current_val > spec_rfh2cw:
                                        shift_occurred_spec = True
                                if param == "lfppcc" and spec_lfppcc is not None:
                                    if current_val > spec_lfppcc:
                                        shift_occurred_spec = True
                                if param == "lfppcw" and spec_lfppcw is not None:
                                    if current_val > spec_lfppcw:
                                        shift_occurred_spec = True
                                if param == "crro" and spec_crro is not None:
                                    if current_val > spec_crro:
                                        shift_occurred_spec = True
                                if param == "trro" and spec_trro is not None:
                                    if current_val > spec_trro:
                                        shift_occurred_spec = True
                                if param == "brro" and spec_brro is not None:
                                    if current_val > spec_brro:
                                        shift_occurred_spec = True
                                if param == "lrot" and spec_lrot is not None:
                                    if current_val > spec_lrot:
                                        shift_occurred_spec = True
                                if param == "lrob" and spec_lrob is not None:
                                    if current_val > spec_lrob:
                                        shift_occurred_spec = True
                                if param == "lroh1grade" and spec_lroh1grade is not None:
                                    if current_val > spec_lroh1grade:
                                        shift_occurred_spec = True
                                if param == "tbul" and spec_tbul is not None:
                                    if current_val > spec_tbul:
                                        shift_occurred_spec = True
                                if param == "bbul" and spec_bbul is not None:
                                    if current_val > spec_bbul:
                                        shift_occurred_spec = True
                                if param == "tdep" and spec_tdep is not None:
                                    if current_val > spec_tdep:
                                        shift_occurred_spec = True
                                if param == "upper" and spec_upper is not None:
                                    if current_val > spec_upper:
                                        shift_occurred_spec = True
                                if param == "lower" and spec_lower is not None:
                                    if current_val > spec_lower:
                                        shift_occurred_spec = True
                                if param == "sbal" and spec_sbal is not None:
                                    if current_val > spec_sbal:
                                        shift_occurred_spec = True
                                if param == "aircc" and spec_aircc is not None:
                                    if current_val > spec_aircc:
                                        shift_occurred_spec = True
                                if param == "aircw" and spec_aircw is not None:
                                    if current_val > spec_aircw:
                                        shift_occurred_spec = True
                                if param == "loadcc" and spec_loadcc is not None:
                                    if current_val > spec_loadcc:
                                        shift_occurred_spec = True
                                if param == "loadcw" and spec_loadcw is not None:
                                    if current_val > spec_loadcw:
                                        shift_occurred_spec = True
                            
                            ############### Add 16-07 start ######################

                                if param in yield_params:
                                    spec_yield = param_spec_map.get(param)
                                    if spec_yield is not None:
                                        if current_val < spec_yield:
                                            shift_occurred_spec = True
                                # if param == "load1" and test_load is not None and test_load_tolerance is not None:
                                #     if not (test_load - test_load_tolerance <= current_val <= test_load + test_load_tolerance):
                                #         shift_occurred_spec = True
                                # if param == "airpress1" and pressure is not None and pressure_tolerance is not None:
                                #     if not (pressure - pressure_tolerance <= current_val <= pressure + pressure_tolerance):
                                #         shift_occurred_spec = True
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
                            # if param == "load1":
                            #     alert_dict["test_load_tolerance"] = test_load_tolerance
                            # if param == "airpress1":
                            #     alert_dict["pressure_tolerance"] = pressure_tolerance
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
                            "sku": model_number,
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
                        "sku": model_number,
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
                        "sku": model_number,
                        "lot_id": unique_lot_id_current,
                        "curing_machine_model_lot_id": current_curing_machine_model_lot_id
                    }
                    if not averages_collect.find_one(averages_query):
                        averages_bulk_operations.append(pymongo.InsertOne(averages_doc))

                    current_key = (model_number, machine_no, current_curing_machine_model_lot_id)
                    machine_lot_avg_map[current_key] = lot_averages

                    machine_alert = {
                        "machine": machine_no,
                        "curing_shift_occurred": any_shift_flag,
                        "shifts_data": shifts_data_list,
                        "overall_uniformity_yield": overall_uniformity_yield(curing_data),
                        "overall_warehouse_yield": overall_warehouse_yield(curing_data),
                        "overall_visual_yield": overall_visual_yield(curing_data),
                        "overall_scrap_yield": overall_scrap_yield(curing_data),
                        "visual_loss_yield": visual_loss_yield(curing_data),
                        "uf_loss_yield": uf_loss_yield(curing_data),
                        "oe_ok_yield": oe_ok_yield(curing_data),
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
                        # "rank_columns": get_rank_columns(curing_data),
                        "barcodes": curing_data["barcode"].tolist()
                    }

                    alert_key = (model_number, unique_lot_id_current)
                    if alert_key not in alert_map:
                        last_alert_id += 1
                        alert_map[alert_key] = {
                            "alert_id_curing_ufm": last_alert_id,
                            "sku": model_number,
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
                            "sku": alert_obj["sku"],
                            "lot_id": alert_obj["lot_id"],
                            "alert_flag": False,
                            "dashboard_flag": False,
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
                            "sku": alert_obj["sku"],
                            "lot_id": alert_obj["lot_id"],
                            "alert_flag": False,
                            "dashboard_flag": False,
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
                            "sku": alert_obj["sku"],
                            "lot_id": alert_obj["lot_id"],
                            "alert_flag": False,
                            "dashboard_flag": False,
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
                                "sku": alert_obj["sku"],
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
        merge_collect = client[TUB_CIMCORP_VISUAL_MERGED_DB_alert][DATA_MERGED_SUMMARY_COLLECTION_alert_new]
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
        ufm_collect_data = client[model_db][model_no_status_collection]
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
        active_modelnos = set(ufm_collect_data.distinct("sku", {"status": "active"}))
        output_messages.append(f"✅ Fetched {len(active_modelnos)} active model numbers: {active_modelnos}")
    except Exception as e:
        output_messages.append(f"❌ Error fetching active model numbers: {e}")
        return {"status": "error", "message": "Failed to fetch active model numbers", "output_log": output_messages}, 500

    # Create unique indexes
    try:
        lot_ufm_collect.create_index(
            [
                ("sku", pymongo.ASCENDING),
                ("machine", pymongo.ASCENDING),
                ("lot_id", pymongo.ASCENDING),
                ("ufm_machine_model_lot_id", pymongo.ASCENDING)
            ],
            unique=True,
            name="ufm_lot_unique_idx"
        )
        alert_ufm_collect.create_index(
            [
                ("sku", pymongo.ASCENDING),
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
        channel_data = list(channel_collect.find({"sku": {"$in": list(active_modelnos)}}))
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
    if 'ptb_id' in merge_df.columns and 'machine' in machine_df.columns:
        try:
            merge_df["tbm_machine"] = None  # Initialize tbm_machine column
            for index, row in merge_df.iterrows():
                tbm_value = row.get("ptb_id")
                if tbm_value:
                    matching_channel = machine_df[machine_df["machine"] == tbm_value]
                    if not matching_channel.empty:
                        merge_df.at[index, "tbm_machine"] = matching_channel["machine"].iloc[0]  # NEW: Use machine column
            output_messages.append("✅ Added tbm_machine column to merge_df based on matching cimcorp_tbm_tub values.")
        except Exception as e:
            output_messages.append(f"⚠️ Warning: Failed to add tbm_machine column to merge_df: {e}. Continuing without tbm_machine.")
    else:
        output_messages.append("⚠️ Warning: 'cimcorp_tbm_tub' column not found in merge_df or machine_df. Skipping tbm_machine column creation.")

    # MODIFIED: Check for cimcorp_tbm_tub column before updating machine_df
    if 'machine' in machine_df.columns and 'ptb_id' in merge_df.columns and 'tbm_machine' in merge_df.columns:
        try:
            for index, row in machine_df.iterrows():
                tbm_value = row.get("machine")
                if tbm_value:
                    matching_merge = merge_df[merge_df["ptb_id"] == tbm_value]
                    if not matching_merge.empty and "tbm_machine" in matching_merge.columns:
                        machine_df.at[index, "machine"] = matching_merge["tbm_machine"].iloc[0]
            output_messages.append("✅ Updated machine_df machine column with tbm_machine values from merge_df.")
        except Exception as e:
            output_messages.append(f"⚠️ Warning: Failed to update machine_df machine column: {e}. Continuing without updating channel_df machine.")
    else:
        output_messages.append("⚠️ Warning: 'cimcorp_tbm_tub' column not found in merge_df or machine_df. Skipping machine_df machine column update.")

    # Filter data
    try:
        channel_modelnos = set(channel_collect.distinct("sku"))
        merge_df = merge_df[merge_df["sku"].isin(channel_modelnos)]
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
        merge_df["date_of_tbm"] = merge_df["ptb_time"]
        merge_df["date_and_time"] = merge_df["uf_time"]
        merge_df["date_of_ufm"] = pd.to_datetime(merge_df["date_and_time"], errors="coerce").dt.strftime('%Y-%m-%d %H:%M:%S').fillna("No Date")

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
        # merge_df["shift"] = merge_df["date_of_ufm"].apply(get_shift)


        merge_df["uf_id"] = merge_df["uf_id"].astype(str)
        ub_machine_df["ubno"] = ub_machine_df["ubno"].astype(str)
        # NEW: Filter merge_df to include only active machines
        merge_df = merge_df[merge_df["uf_id"].isin(active_machines)]
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
                        "sku": {"$in": list(active_modelnos)},
                        "machine": {"$in": list(active_machines)}}},
            {"$addFields": {
                "lot_number": {
                    "$toInt": {"$arrayElemAt": [{"$split": ["$lot_id", "_"]}, -1]}
                },
                "sku": "$sku",
                "machine": "$machine"
            }},
            {"$sort": {"lot_number": -1}},
            {"$group": {
                "_id": {"sku": "$sku", "machine": "$machine"},
                "max_lot_number": {"$first": "$lot_number"},
                "last_lot_id": {"$first": "$lot_id"}
            }}
        ]))
        last_lot_dict = {
            (item["_id"]["sku"], item["_id"]["machine"]): {
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
                    "sku": {"$in": list(active_modelnos)},
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
                    "_id": {"sku": "$sku", "machine": "$machine"},
                    "last_ufm_lot_id": {"$first": "$ufm_machine_model_lot_id"},
                    "last_averages": {"$first": "$current_lot_averages"}
                }
            }
        ]))
        prev_cum_dict = {}
        for item in last_averages:
            modelno = item["_id"]["sku"]
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
        "rfppcc","rfppcw","rfh1cc","rfh1cw","rfh2cc","rfh2cw","lfppcc","lfppcw","cony","plys",
        "crro","trro","brro","lrot","lrob","lroh1grade","tbul","bbul","tdep","bdep",
        "upper","lower","sbal","aircc","aircw","loadcc","loadcw"
    ]
    yield_params = [
        "rfv_yield", "rfh1_yield", "rfh2_yield", "lfv_yield", "conicity_yield", "plysteer_yield",
        "center_radial_run_out_yield", "top_radial_run_out_yield", "bottom_radial_run_out_yield", 
        "lateral_run_out_yield", "lateral_run_out_harmonic_first_yield", "center_radial_run_out_harmonic_first_yield",
        "top_radial_run_out_harmonic_first_yield", "bottom_radial_run_out_harmonic_first_yield",
        "lateral_radial_run_out_yield", "static_yield", "couple_yield",
        "upper_yield", "lower_yield", "overall_uniformity_yield", "overall_visual_yield", "overall_warehouse_yield",
        "overall_scrap_yield", "visual_loss_yield", "uf_loss_yield", "oe_ok_yield"
    ]

    rank_param_mappings = {
        "rfv_yield": ["rfppwcgrade"],
        "rfh1_yield": ["rfh1wcgrade"],
        "rfh2_yield": ["rfh2wcgrade"],
        "lfv_yield": ["lfppwcgrade"],
        "conicity_yield": ["conygrade"],
        "plysteer_yield": ["plysgrade"],
        "center_radial_run_out_yield": ["crrograde"], 
        "top_radial_run_out_yield": ["trrograde"], 
        "bottom_radial_run_out_yield": ["brrograde"],
        "lateral_run_out_yield": ["lrograde"],
        "lateral_run_out_harmonic_first_yield": ["lroh1grade"],
        "center_radial_run_out_harmonic_first_yield": ["crroh1grade"],
        "top_radial_run_out_harmonic_first_yield": ["trroh1grade"],
        "bottom_radial_run_out_harmonic_first_yield": ["brroh1grade"],
        "lateral_radial_run_out_yield": ["lrrograde"],
        "static_yield": ["sbalgrade"],
        "couple_yield": ["couplegrade"],
        "upper_yield": ["uppergrade"],
        "lower_yield": ["lowergrade"]
    }

    for _, row in channel_df.iterrows():
        try:
            ############# Add 16-07 start ##############
            model_number = str(row.get("sku", "")).strip()
            if model_number not in active_modelnos:
                output_messages.append(f"Skipping CURING model {model_number} as it is not active.")
                continue
            gt_codes = row.get("gt_code", [])

            # Ensure gt_codes is a list
            if not isinstance(gt_codes, list):
                gt_codes = [gt_codes]

            for gt_code in gt_codes:
                gt_code = str(gt_code).strip()

                # MODIFIED: Filter merge_df for each gt_code individually
                model_data = merge_df[
                    (merge_df["sku"] == model_number) &
                    (merge_df["gt_code"] == gt_code) &
                    (merge_df["uf_id"].isin(active_machines))
                ].copy()
            ############# Add 16-07 end ##############

                spec_cony_upper = float(row.get("spec_cony_upper", 0))
                spec_cony_lower = float(row.get("spec_cony_lower", 0))
                spec_plys_upper = float(row.get("spec_plys_upper", 0))
                spec_plys_lower = float(row.get("spec_plys_lower", 0))
                sizecode = str(row.get("sizecode", ""))
                spec_rfppcc = float(row.get("spec_rfppcc", 0))
                spec_rfppcw = float(row.get("spec_rfppcw", 0))
                spec_rfh1cc = float(row.get("spec_rfh1cc", 0))
                spec_rfh1cw = float(row.get("spec_rfh1cw", 0))
                spec_rfh2cc = float(row.get("spec_rfh2cc", 0))
                spec_rfh2cw = float(row.get("spec_rfh2cw", 0))
                spec_lfppcc = float(row.get("spec_lfppcc", 0))
                spec_lfppcw = float(row.get("spec_lfppcw", 0))
                spec_crro = float(row.get("spec_crro", 0))
                spec_trro = float(row.get("spec_trro", 0))
                spec_brro = float(row.get("spec_brro", 0))
                spec_lrot = float(row.get("spec_lrot", 0))
                spec_lrob = float(row.get("spec_lrob", 0))
                spec_lroh1grade = float(row.get("spec_lroh1grade", 0))
                spec_tbul = float(row.get("spec_tbul", 0))
                spec_bbul = float(row.get("spec_bbul", 0))
                spec_tdep = float(row.get("spec_tdep", 0))
                spec_bdep = float(row.get("spec_bdep", 0))
                spec_upper = float(row.get("spec_upper", 0))
                spec_lower = float(row.get("spec_lower", 0))
                spec_sbal = float(row.get("spec_sbal", 0))
                spec_aircc = float(row.get("spec_aircc", 0))
                spec_aircw = float(row.get("spec_aircw", 0))
                spec_loadcc = float(row.get("spec_yield", 0))
                spec_loadcw = float(row.get("spec_yield", 0))

                ############### Add 16-07 start ######################

                # total_rank_yield_spec_yield = float(row.get("total_rank_yield_spec_yield", 0))
                spec_rfppwc_yield = float(row.get("spec_rfppwc_yield", 0))
                spec_rfh1wc_yield = float(row.get("spec_rfh1wc_yield", 0))
                spec_rfh2wc_yield = float(row.get("spec_rfh2wc_yield", 0))
                spec_lfppwc_yield = float(row.get("spec_lfppwc_yield", 0))
                spec_cony_yield = float(row.get("spec_cony_yield", 0))
                spec_plys_yield = float(row.get("spec_plys_yield", 0))
                spec_crro_yield = float(row.get("spec_crro_yield", 0))
                spec_trro_yield = float(row.get("spec_trro_yield", 0))
                spec_brro_yield = float(row.get("spec_brro_yield", 0))
                spec_lro_yield = float(row.get("spec_lro_yield", 0))
                spec_lroh1_yield = float(row.get("spec_lroh1_yield", 0))
                spec_crroh1_yield = float(row.get("spec_crroh1_yield", 0))
                spec_trroh1_yield = float(row.get("spec_trroh1_yield", 0))
                spec_brroh1_yield = float(row.get("spec_brroh1_yield", 0))
                spec_lrro_yield = float(row.get("spec_lrro_yield", 0))
                spec_sbal_yield = float(row.get("spec_sbal_yield", 0))
                spec_couple_yield = float(row.get("spec_couple_yield", 0))
                spec_upper_yield = float(row.get("spec_upper_yield", 0))
                spec_lower_yield = float(row.get("spec_lower_yield", 0))
                spec_overall_uniformity_yield = float(row.get("spec_overall_uniformity_yield", 0))
                spec_overall_visual_yield = float(row.get("spec_overall_visual_yield", 0))
                spec_overall_warehouse_yield = float(row.get("spec_overall_warehouse_yield", 0))
                spec_overall_scrap_yield = float(row.get("spec_overall_scrap_yield", 0))
                spec_visual_loss_yield = float(row.get("spec_visual_loss_yield", 0))
                spec_uf_loss_yield = float(row.get("spec_uf_loss_yield", 0))
                spec_oe_ok_yield = float(row.get("spec_oe_ok_yield", 0))

                ############### Add 16-07 end ######################

                threshold_value_plys = float(row.get("threshold_value_plys", 0.5))
                threshold_value_cony = float(row.get("threshold_value_cony", 0.5))
                threshold_value_rfppcc = float(row.get("threshold_value_rfppcc", 0.5))
                threshold_value_rfppcw = float(row.get("threshold_value_rfppcw", 0.5))
                threshold_value_rfh1cc = float(row.get("threshold_value_rfh1cc", 0.5))
                threshold_value_rfh1cw = float(row.get("threshold_value_rfh1cw", 0.5))
                threshold_value_rfh2cc = float(row.get("threshold_value_rfh2cc", 0.5))
                threshold_value_rfh2cw = float(row.get("threshold_value_rfh2cw", 0.5))
                threshold_value_lfppcc = float(row.get("threshold_value_lfppcc", 0.5))
                threshold_value_lfppcw = float(row.get("threshold_value_lfppcw", 0.5))
                threshold_value_crro = float(row.get("threshold_value_crro", 5))
                threshold_value_trro = float(row.get("threshold_value_trro", 5))
                threshold_value_brro = float(row.get("threshold_value_brro", 5))
                threshold_value_lrot = float(row.get("threshold_value_lrot", 5))
                threshold_value_lrob = float(row.get("threshold_value_lrob", 5))
                threshold_value_lroh1grade = float(row.get("threshold_value_lroh1grade", 5))
                threshold_value_tbul = float(row.get("threshold_value_tbul", 0))
                threshold_value_bbul = float(row.get("threshold_value_bbul", 5))
                threshold_value_tdep = float(row.get("threshold_value_tdep", 5))
                threshold_value_bdep = float(row.get("threshold_value_bdep", 5))
                threshold_value_upper = float(row.get("threshold_value_upper", 5))
                threshold_value_lower = float(row.get("threshold_value_lower", 5))
                threshold_value_sbal = float(row.get("threshold_value_sbal", 5))
                threshold_value_aircc = float(row.get("threshold_value_aircc", 5))
                threshold_value_aircw = float(row.get("threshold_value_aircw", 5))
                threshold_value_loadcc = float(row.get("threshold_value_loadcc", 5))
                threshold_value_loadcw = float(row.get("threshold_value_loadcw", 5))
                threshold_value_rfppwc_yield = float(row.get("threshold_value_rfppwc_yield", 5))
                threshold_value_rfh1wc_yield = float(row.get("threshold_value_rfh1wc_yield", 5))
                threshold_value_rfh2wc_yield = float(row.get("threshold_value_rfh2wc_yield", 5))
                threshold_value_lfppwc_yield = float(row.get("threshold_value_lfppwc_yield", 5))
                threshold_value_cony_yield = float(row.get("threshold_value_cony_yield", 5))
                threshold_value_plys_yield = float(row.get("threshold_value_plys_yield", 5))
                threshold_value_crro_yield = float(row.get("threshold_value_crro_yield", 5))
                threshold_value_trro_yield = float(row.get("threshold_value_trro_yield", 5))
                threshold_value_brro_yield = float(row.get("threshold_value_brro_yield", 5))
                threshold_value_lro_yield = float(row.get("threshold_value_lro_yield", 5))
                threshold_value_lroh1_yield = float(row.get("threshold_value_lroh1_yield", 5))
                threshold_value_crroh1_yield = float(row.get("threshold_value_crroh1_yield", 5))
                threshold_value_trroh1_yield = float(row.get("threshold_value_trroh1_yield", 5))
                threshold_value_brroh1_yield = float(row.get("threshold_value_brroh1_yield", 5))
                threshold_value_lrro_yield = float(row.get("threshold_value_lrro_yield", 5))
                threshold_value_sbal_yield = float(row.get("threshold_value_sbal_yield", 5))
                threshold_value_couple_yield = float(row.get("threshold_value_couple_yield", 5))
                threshold_value_upper_yield = float(row.get("threshold_value_upper_yield", 5))
                threshold_value_lower_yield = float(row.get("threshold_value_lower_yield", 5))
                threshold_value_overall_uniformity_yield = float(row.get("threshold_value_overall_uniformity_yield", 5))
                threshold_value_overall_visual_yield = float(row.get("threshold_value_overall_visual_yield", 5))
                threshold_value_overall_warehouse_yield = float(row.get("threshold_value_overall_warehouse_yield", 5))
                threshold_value_overall_scrap_yield = float(row.get("threshold_value_overall_scrap_yield", 5))
                threshold_value_visual_loss_yield = float(row.get("threshold_value_visual_loss_yield", 5))
                threshold_value_uf_loss_yield = float(row.get("threshold_value_uf_loss_yield", 5))
                threshold_value_oe_ok_yield = float(row.get("threshold_value_oe_ok_yield", 5))

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

                for ufm_machine_id, ufm_machine_data in model_data.groupby("uf_id"):
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
                                        "sku": model_number,
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
                                        "sku": model_number,
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
                            "sku": model_number,
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
                            "rfv_yield": rfv_yield(machine_batch_data),
                            "rfh1_yield": rfh1_yield(machine_batch_data),
                            "rfh2_yield": rfh2_yield(machine_batch_data),
                            "lfv_yield": lfv_yield(machine_batch_data),
                            "conicity_yield": conicity_yield(machine_batch_data),
                            "plysteer_yield": plysteer_yield(machine_batch_data),
                            "center_radial_run_out_yield": center_radial_run_out_yield(machine_batch_data),
                            "top_radial_run_out_yield": top_radial_run_out_yield(machine_batch_data),
                            "bottom_radial_run_out_yield": bottom_radial_run_out_yield(machine_batch_data),
                            "lateral_run_out_yield": lateral_run_out_yield(machine_batch_data),
                            "lateral_run_out_harmonic_first_yield": lateral_run_out_harmonic_first_yield(machine_batch_data),
                            "center_radial_run_out_harmonic_first_yield": center_radial_run_out_harmonic_first_yield(machine_batch_data),
                            "top_radial_run_out_harmonic_first_yield": top_radial_run_out_harmonic_first_yield(machine_batch_data),
                            "bottom_radial_run_out_harmonic_first_yield": bottom_radial_run_out_harmonic_first_yield(machine_batch_data),
                            "lateral_radial_run_out_yield": lateral_radial_run_out_yield(machine_batch_data),
                            "static_yield": static_yield(machine_batch_data),
                            "couple_yield": couple_yield(machine_batch_data),
                            "upper_yield": upper_yield(machine_batch_data),
                            "lower_yield": lower_yield(machine_batch_data),
                            "overall_uniformity_yield": overall_uniformity_yield(machine_batch_data),
                            "overall_visual_yield": overall_visual_yield(machine_batch_data),
                            "overall_warehouse_yield": overall_warehouse_yield(machine_batch_data),
                            "overall_scrap_yield": overall_scrap_yield(machine_batch_data),
                            "visual_loss_yield": visual_loss_yield(machine_batch_data),
                            "uf_loss_yield": uf_loss_yield(machine_batch_data),
                            "oe_ok_yield": oe_ok_yield(machine_batch_data)
                        })

                        # Perform Shift Detection
                        parameter_alerts = []
                        machine_lot_overall_shift_occurred = False
                        # Define mapping of yield parameters to their spec_yield variables
                        yield_spec_mapping = {
                            "rfv_yield": spec_rfppwc_yield,
                            "rfh1_yield": spec_rfh1wc_yield,
                            "rfh2_yield": spec_rfh2wc_yield,
                            "lfv_yield": spec_lfppwc_yield,
                            "conicity_yield": spec_cony_yield,
                            "plysteer_yield": spec_plys_yield,
                            "center_radial_run_out_yield": spec_crro_yield,
                            "top_radial_run_out_yield": spec_trro_yield,
                            "bottom_radial_run_out_yield": spec_brro_yield,
                            "lateral_run_out_yield": spec_lro_yield,
                            "lateral_run_out_harmonic_first_yield": spec_lroh1_yield,
                            "center_radial_run_out_harmonic_first_yield": spec_crroh1_yield,
                            "top_radial_run_out_harmonic_first_yield": spec_trroh1_yield,
                            "bottom_radial_run_out_harmonic_first_yield": spec_brroh1_yield,
                            "lateral_radial_run_out_yield": spec_lrro_yield,
                            "static_yield": spec_sbal_yield,
                            "couple_yield": spec_couple_yield,
                            "upper_yield": spec_upper_yield,
                            "lower_yield": spec_lower_yield,
                            "overall_uniformity_yield": spec_overall_uniformity_yield,
                            "overall_visual_yield": spec_overall_visual_yield,
                            "overall_warehouse_yield": spec_overall_warehouse_yield,
                            "overall_scrap_yield": spec_overall_scrap_yield,
                            "visual_loss_yield": spec_visual_loss_yield,
                            "uf_loss_yield": spec_uf_loss_yield,
                            "oe_ok_yield": spec_oe_ok_yield
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
                                    # if param == "load1":
                                    #     alert_dict["test_load_tolerance"] = None
                                    # if param == "airpress1":
                                    #     alert_dict["pressure_tolerance"] = None
                                    parameter_alerts.append(alert_dict)
                                    continue

                                shift_occurred_thershold = False
                                shift_occurred_spec = False
                                spec_val = None

                                if param == "cony" and spec_cony_lower is not None and spec_cony_upper is not None:
                                    shift_occurred_thershold = abs(current_val - prev_val) > threshold_value_cony
                                    shift_occurred_spec = not (-spec_cony_lower <= current_val <= spec_cony_upper)
                                    spec_val = spec_cony_upper
                                elif param == "plys" and spec_plys_lower is not None and spec_plys_upper is not None:
                                    shift_occurred_thershold = abs(current_val - prev_val) > threshold_value_plys
                                    shift_occurred_spec = not (-spec_plys_lower <= current_val <= spec_plys_upper)
                                    spec_val = spec_cony_upper
                                elif param == "rfppcc" and spec_rfppcc:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_rfppcc
                                    shift_occurred_spec = current_val > spec_rfppcc
                                    spec_val = spec_rfppcc
                                elif param == "rfppcw" and spec_rfppcw:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_rfppcw
                                    shift_occurred_spec = current_val > spec_rfppcw
                                    spec_val = spec_rfppcw
                                elif param == "rfh1cc" and spec_rfh1cc:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_rfh1cc
                                    shift_occurred_spec = current_val > spec_rfh1cc
                                    spec_val = spec_rfh1cc
                                elif param == "rfh1cw" and spec_rfh1cw:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_rfh1cw
                                    shift_occurred_spec = current_val > spec_rfh1cw
                                    spec_val = spec_rfh1cw
                                elif param == "rfh2cc" and spec_rfh2cc:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_rfh2cc
                                    shift_occurred_spec = current_val > spec_rfh2cc
                                    spec_val = spec_rfh2cc
                                elif param == "rfh2cw" and spec_rfh2cw:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_rfh2cw
                                    shift_occurred_spec = current_val > spec_rfh2cw
                                    spec_val = spec_rfh2cw
                                elif param == "lfppcc" and spec_lfppcc:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_lfppcc
                                    shift_occurred_spec = current_val > spec_lfppcc
                                    spec_val = spec_lfppcc
                                elif param == "lfppcw" and spec_lfppcw:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_lfppcw
                                    shift_occurred_spec = current_val > spec_lfppcw
                                    spec_val = spec_lfppcw
                                elif param == "crro" and spec_crro is not None:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_crro
                                    shift_occurred_spec = current_val > spec_crro
                                    spec_val = spec_crro
                                elif param == "trro" and spec_trro is not None:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_trro
                                    shift_occurred_spec = current_val > spec_trro
                                    spec_val = spec_trro
                                elif param == "brro" and spec_brro is not None:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_brro
                                    shift_occurred_spec = current_val > spec_brro
                                    spec_val = spec_brro
                                elif param == "lrot" and spec_lrot:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_lrot
                                    shift_occurred_spec = current_val > spec_lrot
                                    spec_val = spec_lrot
                                elif param == "lrob" and spec_lrob:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_lrob
                                    shift_occurred_spec = current_val > spec_lrob
                                    spec_val = spec_lrob
                                elif param == "lroh1grade" and spec_lroh1grade:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_lroh1grade
                                    shift_occurred_spec = current_val > spec_lroh1grade
                                    spec_val = spec_lroh1grade
                                elif param == "tbul" and spec_tbul:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_tbul
                                    shift_occurred_spec = current_val > spec_tbul
                                    spec_val = spec_tbul
                                elif param == "bbul" and spec_bbul:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_bbul
                                    shift_occurred_spec = current_val > spec_bbul
                                    spec_val = spec_bbul
                                elif param == "tdep" and spec_tdep is not None:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_tdep
                                    shift_occurred_spec = current_val > spec_tdep
                                    spec_val = spec_tdep
                                elif param == "bdep" and spec_bdep is not None:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_bdep
                                    shift_occurred_spec = current_val > spec_bdep
                                    spec_val = spec_bdep
                                elif param == "upper" and spec_upper is not None:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_upper
                                    shift_occurred_spec = current_val > spec_upper
                                    spec_val = spec_upper
                                elif param == "lower" and spec_lower:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_lower
                                    shift_occurred_spec = current_val > spec_lower
                                    spec_val = spec_lower
                                elif param == "sbal" and spec_sbal:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_sbal
                                    shift_occurred_spec = current_val > spec_sbal
                                    spec_val = spec_sbal
                                elif param == "aircc" and spec_aircc is not None:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_aircc
                                    shift_occurred_spec = current_val > spec_aircc
                                    spec_val = spec_aircc
                                elif param == "aircw" and spec_aircw is not None:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_aircw
                                    shift_occurred_spec = current_val > spec_aircw
                                    spec_val = spec_aircw
                                elif param == "loadcc" and spec_loadcc is not None:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_loadcc
                                    shift_occurred_spec = current_val > spec_loadcc
                                    spec_val = spec_loadcc
                                elif param == "loadcw" and spec_loadcw:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_loadcw
                                    shift_occurred_spec = current_val > spec_loadcw
                                    spec_val = spec_loadcw

                                # elif param in yield_params:
                                #     spec_yield = yield_spec_mapping.get(param, 0)
                                #     shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield)
                                #     shift_occurred_spec = current_val < spec_yield
                                #     spec_val = spec_yield

                                elif param == "rfv_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_rfppwc_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "rfh1_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_rfh1wc_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "rfh2_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_rfh2wc_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "lfv_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_lfppwc_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "conicity_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_cony_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "plysteer_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_plys_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "center_radial_run_out_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_crro_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "top_radial_run_out_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_trro_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "bottom_radial_run_out_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_brro_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "lateral_run_out_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_lro_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "lateral_run_out_harmonic_first_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_lroh1_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "center_radial_run_out_harmonic_first_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_crroh1_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "top_radial_run_out_harmonic_first_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_trroh1_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "bottom_radial_run_out_harmonic_first_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_brroh1_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "lateral_radial_run_out_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_lrro_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "static_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_sbal_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "couple_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_couple_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "upper_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_upper_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "lower_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_lower_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "overall_uniformity_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_overall_uniformity_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "overall_visual_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_overall_visual_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "overall_warehouse_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_overall_warehouse_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "overall_scrap_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_overall_scrap_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "visual_loss_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_visual_loss_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "uf_loss_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_uf_loss_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param == "oe_ok_yield":
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_oe_ok_yield)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                    
                                # elif param == "load1" and test_load:
                                #     shift_occurred_thershold = abs(current_val - prev_val) > test_load_tolerance
                                #     shift_occurred_spec = not (test_load - test_load_tolerance <= current_val <= test_load + test_load_tolerance)
                                #     spec_val = test_load
                                # elif param == "airpress1" and pressure:
                                #     shift_occurred_thershold = abs(current_val - prev_val) > pressure_tolerance
                                #     shift_occurred_spec = not (pressure - pressure_tolerance <= current_val <= pressure + pressure_tolerance)
                                #     spec_val = pressure

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
                                # if param == "load1":
                                #     alert_dict["test_load_tolerance"] = test_load_tolerance
                                # if param == "airpress1":
                                #     alert_dict["pressure_tolerance"] = pressure_tolerance
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
                                    # if param == "load1":
                                    #     alert_dict["test_load_tolerance"] = None
                                    # if param == "airpress1":
                                    #     alert_dict["pressure_tolerance"] = None
                                    parameter_alerts.append(alert_dict)
                                    continue

                                shift_occurred_thershold = False
                                shift_occurred_spec = False
                                spec_val = None

                                if param == "con_kgf" and spec_cony_lower is not None and spec_cony_upper is not None:
                                    shift_occurred_spec = not (spec_cony_lower <= current_val <= spec_cony_upper)
                                    spec_val = spec_cony_upper
                                elif param == "plys" and spec_plys_lower is not None and spec_plys_lower is not None:
                                    shift_occurred_spec = not (spec_plys_lower <= current_val <= spec_plys_upper)
                                    spec_val = spec_plys_upper
                                elif param == "rfppcc" and spec_rfppcc:
                                    shift_occurred_spec = current_val > spec_rfppcc
                                    spec_val = spec_rfppcc
                                elif param == "rfppcw" and spec_rfppcw:
                                    shift_occurred_spec = current_val > spec_rfppcw
                                    spec_val = spec_rfppcw
                                elif param == "rfh1cc" and spec_rfh1cc:
                                    shift_occurred_spec = current_val > spec_rfh1cc
                                    spec_val = spec_rfh1cc
                                elif param == "rfh1cw" and spec_rfh1cw:
                                    shift_occurred_spec = current_val > spec_rfh1cw
                                    spec_val = spec_rfh1cw
                                elif param == "rfh2cc" and spec_rfh2cc:
                                    shift_occurred_spec = current_val > spec_rfh2cc
                                    spec_val = spec_rfh2cc
                                elif param == "rfh2cw" and spec_rfh2cw:
                                    shift_occurred_spec = current_val > spec_rfh2cw
                                    spec_val = spec_rfh2cw
                                elif param == "lfppcc" and spec_lfppcc:
                                    shift_occurred_spec = current_val > spec_lfppcc
                                    spec_val = spec_lfppcc
                                elif param == "lfppcw" and spec_lfppcw:
                                    shift_occurred_spec = current_val > spec_lfppcw
                                    spec_val = spec_lfppcw
                                elif param == "crro" and spec_crro is not None:
                                    shift_occurred_spec = current_val > spec_crro
                                    spec_val = spec_crro
                                elif param == "trro" and spec_trro is not None:
                                    shift_occurred_spec = current_val > spec_trro
                                    spec_val = spec_trro
                                elif param == "lrot" and spec_lrot is not None:
                                    shift_occurred_spec = current_val > spec_lrot
                                    spec_val = spec_lrot
                                elif param == "lrob" and spec_lrob:
                                    shift_occurred_spec = current_val > spec_lrob
                                    spec_val = spec_lrob
                                elif param == "lroh1grade" and spec_lroh1grade:
                                    shift_occurred_spec = current_val > spec_lroh1grade
                                    spec_val = spec_lroh1grade
                                elif param == "tbul" and spec_tbul:
                                    shift_occurred_spec = current_val > spec_tbul
                                    spec_val = spec_tbul
                                elif param == "bbul" and spec_bbul:
                                    shift_occurred_spec = current_val > spec_bbul
                                    spec_val = spec_bbul
                                elif param == "tdep" and spec_tdep:
                                    shift_occurred_spec = current_val > spec_rfh1cw
                                    spec_val = spec_tdep
                                elif param == "upper" and spec_upper:
                                    shift_occurred_spec = current_val > spec_upper
                                    spec_val = spec_upper
                                elif param == "lower" and spec_lower:
                                    shift_occurred_spec = current_val > spec_lower
                                    spec_val = spec_lower
                                elif param == "sbal" and spec_sbal:
                                    shift_occurred_spec = current_val > spec_sbal
                                    spec_val = spec_sbal
                                elif param == "aircc" and spec_aircc:
                                    shift_occurred_spec = current_val > spec_aircc
                                    spec_val = spec_aircc
                                elif param == "aircw" and spec_aircw is not None:
                                    shift_occurred_spec = current_val > spec_aircw
                                    spec_val = spec_aircw
                                elif param == "loadcc" and spec_loadcc is not None:
                                    shift_occurred_spec = current_val > spec_loadcc
                                    spec_val = spec_loadcc
                                elif param == "loadcw" and spec_loadcw is not None:
                                    shift_occurred_spec = current_val > spec_loadcw
                                    spec_val = spec_loadcw
                                elif param in yield_params:
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                elif param in yield_params and spec_yield:
                                    shift_occurred_spec = current_val < spec_yield
                                    spec_val = spec_yield
                                    
                                # elif param == "load1" and test_load:
                                #     shift_occurred_spec = not (test_load - test_load_tolerance <= current_val <= test_load + test_load_tolerance)
                                #     spec_val = test_load
                                # elif param == "airpress1" and pressure:
                                #     shift_occurred_spec = not (pressure - pressure_tolerance <= current_val <= pressure + pressure_tolerance)
                                #     spec_val = pressure

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
                                # if param == "load1":
                                #     alert_dict["test_load_tolerance"] = test_load_tolerance
                                # if param == "airpress1":
                                #     alert_dict["pressure_tolerance"] = pressure_tolerance
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
                                "sku": model_number,
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
                            "sku": model_number,
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
                            "overall_uniformity_yield": overall_uniformity_yield(machine_batch_data),
                            "overall_warehouse_yield": overall_warehouse_yield(machine_batch_data),
                            "overall_visual_yield": overall_visual_yield(machine_batch_data),
                            "overall_scrap_yield": overall_scrap_yield(machine_batch_data),
                            "visual_loss_yield": visual_loss_yield(machine_batch_data),
                            "uf_loss_yield": uf_loss_yield(machine_batch_data),
                            "oe_ok_yield": oe_ok_yield(machine_batch_data),
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
                            # "rank_columns": get_rank_columns(machine_batch_data),
                            "barcodes": machine_batch_data["barcode"].tolist() if "barcode" in machine_batch_data.columns else []
                        }

                        alert_doc_main = {
                            "_id": f"main_alert_{last_alert_id}",
                            "alert_id_ufm_ufm": last_alert_id,
                            "sku": model_number,
                            "lot_id": current_model_lot_id,
                            "alert_flag": False,
                            "dashboard_flag": False,
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
                            "sku": model_number,
                            "lot_id": current_model_lot_id,
                            "machine_alerts": [machine_alert_detail.copy()]
                        }

                        # Define trigger_doc safely – use unique _id every time
                        trigger_doc = {
                            "_id": f"trigger_alert_{last_alert_id}",   # Unique every time!
                            "alert_id_ufm_ufm": last_alert_id,
                            "sku": model_number,
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
                        #     "sku": model_number,
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
                            "sku": alert_obj["sku"],
                            "lot_id": alert_obj["lot_id"],
                            "alert_flag": False,
                            "dashboard_flag": False,
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
                                    "sku": alert_obj["sku"],
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

    logging.info("process_ufm_lot_and_alert_data_without_lot_size function is completed")
    return {
        "status": "success",
        "message": final_message,
        "output_log": output_messages,
        "inserted_counts": inserted_counts,
        "updated_counts": updated_counts,
        "execution_time": execution_time
    }, 200

def ensure_admin_collection_exists():
    logging.info("ensure_admin_collection_exists function is just started")
    try:
        db = client[alert_mail_db]
        collection = db[admin_table]
        alert_users_collection = db[alert_users]
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
            alert_users_collection.insert_one(default_admin)
            logging.info("Default manager admin inserted.")
            return
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
            alert_users_collection.insert_one(default_admin)
            logging.info("Default manager admin inserted into both admin_table and alert_users.")
        else:
            logging.info("At least one 'manager' user already exists in admin_table. No insert needed.")
    except Exception as e:
        logging.error(f"Error in ensure_admin_collection_exists: {str(e)}")

def main():
    try:
        configure_logging()
        warnings.filterwarnings("ignore")
        logging.info("Starting the data cleaning process...")

        ensure_admin_collection_exists()
        logging.info("Admin collection check completed.")

        # Clean and write directly to final merged DB — no intermediate DBs
        result = run_data_cleaning(df_tub, df_cimcorp)
        
        lot_alert_result = process_lot_and_alert_data()

        if result['status'] == 'success':
            logging.info("Data cleaning completed successfully.")
        else:
            logging.info("Data cleaning failed:", result['message'])
            return
        
        # # Process TBM lot and alert data
        # logging.info("Starting process_tbm_lot_and_alert_data")
        # tbm_process_output_data = process_tbm_lot_and_alert_data()
        # if tbm_process_output_data[0]['status'] == 'success':
        #     logging.info("TBM lot and alert processing completed successfully.")
        #     output_messages = tbm_process_output_data[0]['output_log']
        #     tbm_lot_alert_results = tbm_process_output_data[0]
        # else:
        #     logging.info("Data cleaning failed: %s", result['message'])
        #     return tbm_process_output_data
        
        # Process Curing lot and alert data
        # logging.info("Starting process_curing_data_and_generate_lots_alerts")
        # curing_process_output_data, curing_status_code = process_curing_data_and_generate_lots_alerts()
        # if curing_process_output_data['status'] == 'success':
        #     logging.info("Curing lot and alert processing completed successfully.")
        #     output_messages.extend(curing_process_output_data['output_log'])
        #     curing_lot_alert_results = curing_process_output_data
        # else:
        #     logging.info("TBM lot and alert processing failed: %s", tbm_process_output_data[0]['message'])
        #     return curing_process_output_data, curing_status_code
        
        # # Process TBM without lot size and alert data
        # logging.info("Starting process_tbm_lot_and_alert_data without lot size")
        # tbm_process_output_data = process_tbm_lot_and_alert_data_without_lot_size()
        # if tbm_process_output_data[0]['status'] == 'success':
        #     logging.info("process_tbm_lot_and_alert_data_without_lot_size and alert processing completed successfully.")
        #     output_messages = tbm_process_output_data[0]['output_log']
        #     tbm_lot_alert_results = tbm_process_output_data[0]
        # else:
        #     logging.info("process_tbm_lot_and_alert_data_without_lot_size lot and alert processing failed: %s", tbm_process_output_data[0]['message'])
        #     return tbm_process_output_data
        
        # # Process Curing without lot size and alert data
        # logging.info("Starting process_curing_data_and_generate_lots_alerts_without_lot_size")
        # curing_process_output_data, curing_status_code = process_curing_data_and_generate_lots_alerts_without_lot_size()
        # if curing_process_output_data['status'] == 'success':
        #     logging.info("Curing lot and alert processing completed successfully.")
        #     output_messages.extend(curing_process_output_data['output_log'])
        #     curing_lot_alert_results = curing_process_output_data
        # else:
        #     logging.info("process_curing_lot_and_alert_data_without_lot_size and alert processing failed: %s", curing_process_output_data[0]['message'])
        #     return curing_process_output_data, curing_status_code
        
        # Process ufm without lot size and alert data
        logging.info("Starting process_ufm_lot_and_alert_data without lot size")
        ufm_process_output_data = process_ufm_lot_and_alert_data_without_lot_size()
        if ufm_process_output_data[0]['status'] == 'success':
            logging.info("process_ufm_lot_and_alert_data_without_lot_size lot and alert processing completed successfully without lot size.")
            output_messages = ufm_process_output_data[0]['output_log']
            ufm_lot_alert_results = ufm_process_output_data[0]
        else:
            logging.info("UFM without lot and alert processing failed: %s", ufm_process_output_data['message'])
            return ufm_process_output_data
        
        logging.info(f"Data cleaning result: {result['message']}")
        logging.info(f"Execution time: {result['execution_time']:.2f} seconds")
        logging.info("Output log:\n" + "\n".join(result['output_log']))
        print(result["message"])
        print(f"Execution time: {result['execution_time']:.2f} seconds")
        print("Output log:", result["output_log"])
        
    except Exception as e:
        error_message = f"Error in main function: {str(e)}"
        logging.error(error_message)
        logging.error(traceback.format_exc())
        print(error_message)
        print(traceback.format_exc())

if __name__ == "__main__":
    main()

