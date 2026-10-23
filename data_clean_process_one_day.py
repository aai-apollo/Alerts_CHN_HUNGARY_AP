from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
import pandas as pd
import re
from datetime import datetime, timezone, time as datetime_time
from datetime import datetime, date  # Ensure both datetime and date are imported
import traceback
import os
import numpy as np
import time
import pytz
import pymongo
from pymongo import MongoClient
from datetime import datetime, timedelta
from pytz import timezone
import logging
from datetime import datetime
from passlib.context import CryptContext
import redshift_connector							   
import csv
import warnings
import bcrypt

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
from config import (
    client,
    FILE_TRACKING_DB,
    FILE_TRACKING_COLLECTION,
    TUB_DB_history,
    CIMCORP_DB_history,
    Data_Clean_CIMCORP_HISTORICAL_DATA,
    Data_Clean_TUB_HISTORICAL_DATA,
    TUB_CLEANED_PREFIX_history,
    CIMCORP_CLEANED_PREFIX_history,
    TUB_INVALID_PREFIX,
    CIMCORP_INVALID_PREFIX,
    COMBINED_Database_history,
    ALL_COMBINED_TUB_COLLECTION_history,
    ALL_COMBINED_CIMCORP_COLLECTION_history,
    INVALID_ROWS_DB,
    TUB_CIMCORP_DB_history,
    all_combined_tub_data_historical_data,
    # COMBINED_COLLECTION,
    inner_join_collection_name_history,
    tub_collection_name_history,
    config_databse,
    channel_no_collect,
    alert_mail_db,
    admin_table,
    alert_users,DAILY_YIELD_HISTORY ,LATEST_YIELD
)


# =============================================================================
# NO-INSERT PATCH — suppresses WRITES to specific databases only.
# Processing (cleaning, combining, merging) is completely unchanged:
#   * CIMCORP_HISTORICAL_DATA / TUB_HISTORICAL
#                        → in-memory cache: writes are cached, reads come from
#                          the cache so the insert→clean→combine round-trip
#                          works without persisting raw data to MongoDB.
#   * Data_Clean_TUB_HISTORICAL_DATA /
#     Data_Clean_CIMCORP_HISTORICAL_DATA
#                        → writes discarded (never read back inside this file).
# All other databases (all_combined_data_historical_data,
# TUB_CIMCORP_merged_cleaned_data_historical_data, INVALID_ROWS,
# file_tracking, alert_mail_db, etc.) are NOT blocked and persist normally.
# =============================================================================
import copy as _copy

_CACHED_DBS_HIST    = {CIMCORP_DB_history, TUB_DB_history}
_SUPPRESS_WRITE_HIST = {Data_Clean_TUB_HISTORICAL_DATA, Data_Clean_CIMCORP_HISTORICAL_DATA}

_rh = lambda **kw: type('R', (), kw)()

def _to_native_hist(v):
    """Convert numpy / pandas types to plain Python (mirrors MongoDB BSON round-trip)."""
    if v is None:           return None
    if v is pd.NaT:         return None
    if isinstance(v, float) and v != v: return None   # NaN
    if isinstance(v, np.integer):   return int(v)
    if isinstance(v, np.floating):  return None if np.isnan(v) else float(v)
    if isinstance(v, np.bool_):     return bool(v)
    if isinstance(v, np.ndarray):   return v.tolist()
    if isinstance(v, pd.Timestamp): return None if pd.isna(v) else v.to_pydatetime()
    if isinstance(v, datetime):     return v
    return v


class _CachedCollectionHist:
    """In-memory collection: caches writes, serves reads from the cache."""
    def __init__(self, name):
        self.name = name
        self._records = []
    def insert_many(self, records, **kw):
        normalized = [{k: _to_native_hist(v) for k, v in r.items()} for r in list(records)]
        self._records.extend(_copy.deepcopy(normalized))
        return _rh(inserted_ids=[])
    def insert_one(self, record, **kw):
        self._records.append(_copy.deepcopy({k: _to_native_hist(v) for k, v in record.items()}))
        return _rh(inserted_id=None)
    def find(self, *a, **kw):
        return iter(_copy.deepcopy(r) for r in self._records)
    def find_one(self, *a, **kw):
        return _copy.deepcopy(self._records[0]) if self._records else None
    def count_documents(self, *a, **kw): return len(self._records)
    def delete_many(self, *a, **kw):
        n = len(self._records); self._records.clear(); return _rh(deleted_count=n)
    def delete_one(self, *a, **kw):
        if self._records: self._records.pop(0); return _rh(deleted_count=1)
        return _rh(deleted_count=0)
    def update_one(self, *a, **kw):  return _rh(modified_count=0)
    def update_many(self, *a, **kw): return _rh(modified_count=0)
    def bulk_write(self, *a, **kw):
        return _rh(inserted_count=0, modified_count=0, upserted_count=0)
    def create_index(self, *a, **kw):    return None
    def index_information(self, *a, **kw): return {}
    def aggregate(self, *a, **kw):   return iter([])
    def distinct(self, *a, **kw):    return []
    def __getattr__(self, name):     return lambda *a, **kw: None


class _CachedDBHist:
    """Returns _CachedCollectionHist instances (one per collection name)."""
    def __init__(self):
        self._collections = {}
    def __getitem__(self, name):
        if name not in self._collections:
            self._collections[name] = _CachedCollectionHist(name)
        return self._collections[name]
    def list_collection_names(self, *a, **kw): return list(self._collections.keys())
    def __getattr__(self, name): return lambda *a, **kw: None


class _WriteSuppressCollectionHist:
    """Reads pass through to the real collection; writes return fake success."""
    def __init__(self, real_coll):
        self._real = real_coll
    def find(self, *a, **kw):            return self._real.find(*a, **kw)
    def find_one(self, *a, **kw):        return self._real.find_one(*a, **kw)
    def aggregate(self, *a, **kw):       return self._real.aggregate(*a, **kw)
    def count_documents(self, *a, **kw): return self._real.count_documents(*a, **kw)
    def distinct(self, *a, **kw):        return self._real.distinct(*a, **kw)
    def index_information(self, *a, **kw): return self._real.index_information(*a, **kw)
    def insert_one(self, *a, **kw):      return _rh(inserted_id=None)
    def insert_many(self, *a, **kw):     return _rh(inserted_ids=[])
    def bulk_write(self, *a, **kw):
        return _rh(inserted_count=0, modified_count=0, upserted_count=0)
    def update_one(self, *a, **kw):      return _rh(modified_count=0)
    def update_many(self, *a, **kw):     return _rh(modified_count=0)
    def delete_one(self, *a, **kw):      return _rh(deleted_count=0)
    def delete_many(self, *a, **kw):     return _rh(deleted_count=0)
    def create_index(self, *a, **kw):    return None
    @property
    def name(self): return self._real.name
    def __getattr__(self, name): return getattr(self._real, name)


class _WriteSuppressDBHist:
    """Returns _WriteSuppressCollectionHist wrappers over the real database."""
    def __init__(self, real_db):
        self._real = real_db
    def __getitem__(self, name):
        return _WriteSuppressCollectionHist(self._real[name])
    def list_collection_names(self, *a, **kw):
        return self._real.list_collection_names(*a, **kw)
    def __getattr__(self, name): return getattr(self._real, name)


_cached_dbs_hist = {}
def _get_cached_db_hist(db_name):
    if db_name not in _cached_dbs_hist:
        _cached_dbs_hist[db_name] = _CachedDBHist()
    return _cached_dbs_hist[db_name]

_orig_getitem_hist = client.__class__.__getitem__

def _patched_getitem_hist(self, db_name):
    if db_name in _CACHED_DBS_HIST:
        return _get_cached_db_hist(db_name)
    if db_name in _SUPPRESS_WRITE_HIST:
        return _WriteSuppressDBHist(_orig_getitem_hist(self, db_name))
    return _orig_getitem_hist(self, db_name)

client.__class__.__getitem__ = _patched_getitem_hist
# =============================================================================

app = Flask(__name__)
app.secret_key = "supersecretkey"

def configure_logging():
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    current_date = datetime.now().strftime('%Y-%m-%d')
    log_filename = f'logs/log_historical_data_{current_date}.txt'
    
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

db_tracking = client[FILE_TRACKING_DB]
tracking_collection = db_tracking[FILE_TRACKING_COLLECTION]

def some_function():
    india_time = datetime.now(pytz.timezone('Asia/Kolkata'))
    return india_time

logging.info(some_function())

DEFAULT_DATETIME = datetime(1900, 1, 1, 12, 30, 0)
DEFAULT_DATE = datetime(1900, 1, 1)

file_tracking_db = client['file_tracking']
cimcorp_db = client['CIMCORP_HISTORICAL_DATA']
tub_db = client['TUB_HISTORICAL']

# def insert_with_timestamp(df, collection_name_prefix, db_name):
#     timestamp = some_function().strftime("%Y%m%d_%H%M%S")
#     if db_name == 'CIMCORP_HISTORICAL_DATA':
#         db = cimcorp_db
#     elif db_name == 'TUB_HISTORICAL':
#         db = tub_db
#     else:
#         raise ValueError("Invalid database name")
#     collection_name = f"{collection_name_prefix}_{timestamp}"
#     collection = db[collection_name]
#     collection.insert_many(df.to_dict('records'))
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
    if db_name == 'CIMCORP_HISTORICAL_DATA':
        db = cimcorp_db
    elif db_name == 'TUB_HISTORICAL':
        db = tub_db
    else:
        raise ValueError("Invalid database name")
    
    collection_name = f"{collection_name_prefix}_{timestamp}"
    collection = db[collection_name]
    collection.insert_many(df.to_dict('records'))
    
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
            database='db_prd_plant_che',
            port=5439,
            user='usr_thangadurai_a',
            password='QtHJO%1^m^dQEVFc', sslmode='disable'
        )
    def execute_query(self, q):
        data = pd.read_sql(q, self.conn)
        return data
    
inst = RedshiftDataConnectorr()


########### cronjob START for only back track 30 mins ######################
# local_tz = pytz.timezone("Asia/Kolkata")
# now = datetime.now(pytz.utc).astimezone(local_tz)

# print("local_tz:", local_tz)
# print("now:", now)

# minute = now.minute
# print("minute:", minute)

# # ---------------------------------------------------------
# # # Determine window based on required fixed 30-min blocks
# # ---------------------------------------------------------

# if 0 <= minute < 10:
#     # Example: 12:09 → 11:20 to 11:50
#     start_dt = (now - timedelta(hours=1)).replace(minute=20, second=0, microsecond=0)
#     end_dt = (now - timedelta(hours=1)).replace(minute=50, second=0, microsecond=0)

# elif 10 <= minute < 30:
#     # Example: 12:29 → 11:40 to 12:10
#     start_dt = (now - timedelta(hours=1)).replace(minute=40, second=0, microsecond=0)
#     end_dt = now.replace(minute=10, second=0, microsecond=0)

# elif 30 <= minute < 50:
#     # Example: 12:49 → 12:00 to 12:30
#     start_dt = now.replace(minute=0, second=0, microsecond=0)
#     end_dt = now.replace(minute=30, second=0, microsecond=0)

# else:
#     # Example: 13:09 → 12:20 to 12:50
#     start_dt = now.replace(minute=20, second=0, microsecond=0)
#     end_dt = now.replace(minute=50, second=0, microsecond=0)

# print("Window:", start_dt, "→", end_dt)

########### cronjob END for only back track 30 mins ######################


########### cronjob START for back track 8 hours data ######################
from datetime import datetime, timedelta
import pytz

# Define the IST timezone
local_tz = pytz.timezone("Asia/Kolkata")
now = datetime.now(pytz.utc).astimezone(local_tz)

print("local_tz:", local_tz)
print("now:", now)

minute = now.minute
print("minute:", minute)

# ---------------------------------------------------------
# Determine the end time based on the minute block
# Fetch data for the previous 8 hours
# ---------------------------------------------------------

if 0 <= minute < 10:
    # Example: 12:09 PM -> 03:50 AM to 11:50 AM
    end_dt = (now - timedelta(hours=1)).replace(
        minute=50, second=0, microsecond=0
    )

elif 10 <= minute < 30:
    # Example: 12:29 PM -> 04:10 AM to 12:10 PM
    end_dt = now.replace(
        minute=10, second=0, microsecond=0
    )

elif 30 <= minute < 50:
    # Example: 12:49 PM -> 04:30 AM to 12:30 PM
    end_dt = now.replace(
        minute=30, second=0, microsecond=0
    )

else:
    # Example: 12:59 PM -> 04:50 AM to 12:50 PM
    end_dt = now.replace(
        minute=50, second=0, microsecond=0
    )

# Start time: exactly 8 hours before end time
start_dt = end_dt - timedelta(hours=8)

print("Start time:", start_dt)
print("End time:", end_dt)
print("Window:", start_dt, "→", end_dt)

########### cronjob END for back track 8 hours data ######################

################## manual running code start ######################        
# from datetime import datetime
# import pytz

# local_tz = pytz.timezone("Asia/Kolkata")

# # -----------------------------------------------------------------------------------
# # Provide only manual date range (No last-hour fallback)
# # -----------------------------------------------------------------------------------
# manual_start_date = '2026-09-01'
# manual_end_date = '2026-10-20'

# # If you want full-day coverage
# manual_start_time = '07:00:00'
# manual_end_time = '07:00:00'

# # Combine date + time and convert to timezone-aware datetime
# start_dt = local_tz.localize(
#     datetime.strptime(f"{manual_start_date} {manual_start_time}", "%Y-%m-%d %H:%M:%S")
# )

# end_dt = local_tz.localize(
#     datetime.strptime(f"{manual_end_date} {manual_end_time}", "%Y-%m-%d %H:%M:%S")
# )

# # Final formatted values (if needed for query)
# start_date = start_dt.strftime("%Y-%m-%d")
# end_date = end_dt.strftime("%Y-%m-%d")
# start_time = start_dt.strftime("%H:%M:%S")
# end_time = end_dt.strftime("%H:%M:%S")

################## manual running code end ######################


# # #################### Manually triggered code start ################
# --- For SQL
start_date = start_dt.strftime("%Y-%m-%d")
print("start_date:", start_date)
end_date = end_dt.strftime("%Y-%m-%d")
print("end_date:", end_date)
start_time = start_dt.strftime("%H:%M:%S")
print("start_time:", start_time)
end_time = end_dt.strftime("%H:%M:%S")
print("end_time:", end_time)
#################### Manually triggered code end ################

# ------------------------------------------------------------------
# MongoDB: Fetch modelno and gt_code from production_db.channel_no_ufm
# ------------------------------------------------------------------

# MONGO_URI = "mongodb://<host>:<port>/"   # <-- replace with actual URI

# mongo_client = MongoClient(MONGO_URI)
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
WHERE ("date" > '{start_date}' OR ("date" = '{start_date}' AND "time" >= '{start_time}'))
AND ("date" < '{end_date}' OR ("date" = '{end_date}' AND "time" < '{end_time}'))
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
    WHERE ("date" > '{start_date}' OR ("date" = '{start_date}' AND "time" >= '{start_time}'))
AND ("date" < '{end_date}' OR ("date" = '{end_date}' AND "time" < '{end_time}'))
),
transport_units AS (
    SELECT 
        b.barcode, 
        t.transportationunitid
    FROM barcode_data b
    JOIN sc_cimcorp_pcr.transportationunit t 
      ON b.barcode = t.barcode
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
    repair.operation AS operation,
    TO_CHAR(repair.timestamp + INTERVAL '5 hours 30 minutes', 'YYYY-MM-DD HH24:MI:SS') AS operation_time,
    TO_CHAR(visualinspection.timestamp + INTERVAL '5 hours 30 minutes', 'YYYY-MM-DD HH24:MI:SS') AS visualinspection_time
FROM sc_cimcorp_pcr.tbm tbm
JOIN transport_units tu 
  ON tbm.transportationunitid = tu.transportationunitid
JOIN sc_cimcorp_pcr.curing curing 
  ON tbm.transportationunitid = curing.transportationunitid
LEFT JOIN sc_cimcorp_pcr.uftesting uft 
  ON tbm.transportationunitid = uft.transportationunitid
LEFT JOIN sc_cimcorp_pcr.repair repair 
  ON tbm.transportationunitid = repair.transportationunitid
LEFT JOIN sc_cimcorp_pcr.visualinspection visualinspection
  ON tbm.transportationunitid = visualinspection.transportationunitid
WHERE tbm.gttypeid IN ({gt_code_sql});
"""

print("######################### QUERY 2 #########################")
# print(q2)

df_cimcorp = inst.execute_query(q2)


def sanitize_collection_name(filename):
    name = filename.rsplit(".", 1)[0]
    name = re.sub(r'[\/\.\$]', '_', name)
    return name[:120]

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
    if dt == DEFAULT_DATETIME:
        return "No Date"
    if dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    if dt.hour < 7:
        dt = dt + pd.Timedelta(days=1)
    A_start = datetime.strptime(f"{dt.date()} 07:00:00", "%Y-%m-%d %H:%M:%S")
    A_end = datetime.strptime(f"{dt.date()} 14:59:59", "%Y-%m-%d %H:%M:%S")
    B_start = datetime.strptime(f"{dt.date()} 15:00:00", "%Y-%m-%d %H:%M:%S")
    B_end = datetime.strptime(f"{dt.date()} 22:59:59", "%Y-%m-%d %H:%M:%S")
    C_start = datetime.strptime(f"{dt.date()} 23:00:00", "%Y-%m-%d %H:%M:%S")
    C_end = datetime.strptime(f"{(dt + pd.Timedelta(days=1)).date()} 06:59:59", "%Y-%m-%d %H:%M:%S")
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
    # Strip strings in all object columns
    for col in df_clean.select_dtypes(include=['object']).columns:
        df_clean[col] = df_clean[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
    # Clean barcode and modelno
    # if 'barcode' in df_clean.columns:
    # df_clean['barcode'] = df_clean['barcode'].astype(str).apply(lambda x: x[:-2] if x.endswith('.0') else x)
    if 'barcode' in df_clean.columns:
        df_clean['barcode'] = (df_clean['barcode'].astype(str).apply(lambda x: x[:-2] if x.endswith('.0') else x).apply(lambda x: x.lstrip('0'))) # Remove leading zeros)
    
    if 'modelno' in df_clean.columns:
        df_clean['modelno'] = df_clean['modelno'].astype(str).apply(lambda x: x.rstrip('0').rstrip('.'))
    # Define column types
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
    # Add new columns based on max of specified pairs
    if all(col in df_clean.columns for col in ['lt_oa_rank', 'lb_oa_rank']):
        df_clean['lateral_run_out_rank'] = df_clean[['lt_oa_rank', 'lb_oa_rank']].max(axis=1)
    if all(col in df_clean.columns for col in ['lt_oa1h_rank', 'lb_oa1h_rank']):
        df_clean['lateral_run_out_harmonic_rank'] = df_clean[['lt_oa1h_rank', 'lb_oa1h_rank']].max(axis=1)
    if all(col in df_clean.columns for col in ['ltbulg_rank', 'lbbulg_rank']):
        df_clean['bulge_rank'] = df_clean[['ltbulg_rank', 'lbbulg_rank']].max(axis=1)
    if all(col in df_clean.columns for col in ['dent_rank', 'dent1_rank']):
        df_clean['dent_overall_rank'] = df_clean[['dent_rank', 'dent1_rank']].max(axis=1)
    # Update column types for new columns
    column_types.update({
        'lateral_run_out_rank': 'float64',
        'lateral_run_out_harmonic_rank': 'float64',
        'bulge_rank': 'float64',
        'dent_overall_rank': 'float64'
    })
    # Handle date and time columns
    if 'date' in df_clean.columns:
        # Convert date to string format (YYYY-MM-DD)
        df_clean['date'] = pd.to_datetime(df_clean['date'], errors='coerce').dt.strftime('%Y-%m-%d')
        df_clean['ub_date'] = df_clean['date'] # Copy date to ub_date as string
    # Initialize date_and_time
    df_clean['date_and_time'] = None
    if 'date' in df_clean.columns and 'time' in df_clean.columns:
        try:
            def parse_date(date_str):
                if pd.isnull(date_str):
                    return None
                try:
                    return pd.to_datetime(date_str, errors='coerce').strftime('%Y-%m-%d')
                except ValueError:
                    return None
            def parse_time(time_str):
                if pd.isnull(time_str):
                    return None
                try:
                    return pd.to_datetime(time_str, format='%H:%M:%S', errors='coerce').strftime('%H:%M:%S')
                except ValueError:
                    try:
                        return pd.to_datetime(time_str, format='%I:%M:%S %p', errors='coerce').strftime('%H:%M:%S')
                    except ValueError:
                        return None
            df_clean['parsed_date'] = df_clean['date'].apply(parse_date)
            df_clean['parsed_time'] = df_clean['time'].apply(parse_time)
            mask_both = (df_clean['parsed_date'].notnull()) & (df_clean['parsed_time'].notnull())
            mask_date_only = (df_clean['parsed_date'].notnull()) & (df_clean['parsed_time'].isnull())
            mask_time_only = (df_clean['parsed_date'].isnull()) & (df_clean['parsed_time'].notnull())
            df_clean.loc[mask_both, 'date_and_time'] = df_clean.loc[mask_both, 'parsed_date'] + ' ' + df_clean.loc[mask_both, 'parsed_time']
            df_clean.loc[mask_date_only, 'date_and_time'] = df_clean.loc[mask_date_only, 'parsed_date']
            df_clean.loc[mask_time_only, 'date_and_time'] = df_clean.loc[mask_time_only, 'parsed_time']
            df_clean = df_clean.drop(columns=['parsed_date', 'parsed_time'], errors='ignore')
        except Exception as e:
            logging.error(f"Error combining date and time: {e}")
            df_clean['date_and_time'] = None
    # Apply column types
    column_types = {col: dtype for col, dtype in column_types.items() if col in df_clean.columns}
    df_clean = df_clean.astype(column_types, errors='ignore')
    # Add lot_id and previous_lot_id
    if 'modelno' in df_clean.columns:
        df_clean['lot_id'] = df_clean['modelno'].astype(str) + '_' + df_clean.groupby('modelno').cumcount().astype(str)
        df_clean['previous_lot_id'] = df_clean['modelno'].astype(str) + '_' + (df_clean.groupby('modelno').cumcount() - 1).astype(str)
        df_clean.loc[df_clean['previous_lot_id'].str.contains('_-1', na=False), 'previous_lot_id'] = None
    # Ensure no datetime.date objects remain
    for col in df_clean.select_dtypes(include=['object']).columns:
        if any(isinstance(x, date) and not isinstance(x, datetime) for x in df_clean[col] if x is not None):
            logging.info(f"Converting datetime.date to string in column: {col}")
            df_clean[col] = df_clean[col].apply(
                lambda x: x.strftime('%Y-%m-%d') if isinstance(x, date) and not isinstance(x, datetime) and x is not None else x
            )
    # logging.info(f"Cleaned DataFrame columns: {list(df_clean.columns)}")
    # logging.info(f"Cleaned DataFrame dtypes:\n{df_clean.dtypes}")
    # logging.info("clean_tub_data function is completed")
    return df_clean, invalid_rows, output_messages

def clean_cimcorp_data(df):
    output_messages = []
    invalid_barcode_rows = pd.DataFrame()
    duplicates_df = pd.DataFrame()
    logging.info("clean_cimcorp_data function is just started")
    try:
        df = df.map(str)
        df.columns = [re.sub(r'[.\s]+', '_', col.strip()).lower() for col in df.columns]
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
        for col in df.select_dtypes(include=['object']):
            df[col] = df[col].astype(str).str.strip()
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
                df = df.rename(columns={old_name: new_name}, inplace=False)
        # # Convert tbm_buildtime and curing_infeedtime to IST
        # if 'tbm_buildtime' in df.columns:
        #     df['tbm_buildtime'] = (pd.to_datetime(df['tbm_buildtime']) + pd.Timedelta(hours=5, minutes=30)).dt.strftime("%Y-%m-%d %H:%M:%S")
        # if 'curing_infeedtime' in df.columns:
        #     df['curing_infeedtime'] = (pd.to_datetime(df['curing_infeedtime']) + pd.Timedelta(hours=5, minutes=30)).dt.strftime("%Y-%m-%d %H:%M:%S")
        if 'tbm' in df.columns:
            df['tbm'] = 'TBM' + df['tbm'].astype(str)
        # if 'curing_press' in df.columns:
        #     df['curing_press'] = 'M' + df['curing_press'].astype(str)
        special_char_regex = re.compile(r'[^0-9]')
        rows_with_null_barcode = df[df['barcode'].isnull()]
        rows_with_special_char_barcode = df[df['barcode'].astype(str).apply(lambda x: bool(special_char_regex.search(x)))]
        invalid_barcode_rows = pd.concat([rows_with_null_barcode, rows_with_special_char_barcode]).drop_duplicates()
        df = df[~df.index.isin(invalid_barcode_rows.index)]
        df = df.dropna(axis=1, how='all')
        df = df.loc[:, ~df.apply(lambda x: x.apply(lambda val: isinstance(val, str) and val.strip() == '').all())]
        duplicates_df = df[df.duplicated(subset=['barcode'], keep=False)]
        df = df.drop_duplicates(subset=['barcode'])
        if 'barcode' in df.columns:
            df['barcode'] = (df['barcode'].astype(str).apply(lambda x: x[:-2] if x.endswith('.0') else x).apply(lambda x: x.lstrip('0'))) # Remove leading zeros
        df['tbm_shift'] = df['tbm_buildtime'].apply(get_shift)
        df['curing_shift'] = df['curing_infeedtime'].apply(get_shift)
        def extract_and_convert_build_date(row):
            build_date = DEFAULT_DATETIME
            tbm_buildtime = row.get('tbm_buildtime', '')
            if isinstance(tbm_buildtime, str):
                try:
                    build_date = tbm_buildtime[:10]
                    build_date = convert_string_to_date(build_date)
                    if build_date:
                        return build_date.isoformat()
                except Exception as e:
                    logging.info("Error extracting build_date: %s", e)
            return "No Date"
        df['build_date'] = df.apply(extract_and_convert_build_date, axis=1)
        df['build_date'] = pd.to_datetime(df['build_date'], errors='coerce')
        output_messages.append("CIMCORP_HISTORICAL_DATA Cleaning successful")
        logging.info("clean_cimcorp_data function is completed")
        return df, invalid_barcode_rows, duplicates_df, output_messages
    except Exception as e:
        error_message = f"Error during cleaning: {e}"
        output_messages.append(error_message)
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), output_messages
 
# def clean_cimcorp_data(df):
#     output_messages = []
#     invalid_barcode_rows = pd.DataFrame()
#     duplicates_df = pd.DataFrame()
#     logging.info("clean_cimcorp_data function is just started")
#     try:
#         df = df.applymap(str)
#         df.columns = [re.sub(r'[.\s]+', '_', col.strip()).lower() for col in df.columns]
#         df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
#         for col in df.select_dtypes(include=['object']):
#             df[col] = df[col].astype(str).str.strip()
#         rename_columns_mapping = {
#             'deviceid_x': 'tbm',
#             'timestamp_x': 'tbm_buildtime',
#             'skutypeid': 'sku',
#             'mouldid': 'curing_mouldid',
#             'machinenumber': 'curing_press',
#             'timestamp_y': 'curing_infeedtime',
#             'gttypeid': 'tbm_gtcode'
#         }
#         for old_name, new_name in rename_columns_mapping.items():
#             if old_name in df.columns:
#                 df = df.rename(columns={old_name: new_name}, inplace=False)

#         # Convert tbm_buildtime and curing_infeedtime to IST
#         if 'tbm_buildtime' in df.columns:
#             df['tbm_buildtime'] = (pd.to_datetime(df['tbm_buildtime']) + pd.Timedelta(hours=5, minutes=30)).dt.strftime("%Y-%m-%d %H:%M:%S")

#         if 'curing_infeedtime' in df.columns:
#             df['curing_infeedtime'] = (pd.to_datetime(df['curing_infeedtime']) + pd.Timedelta(hours=5, minutes=30)).dt.strftime("%Y-%m-%d %H:%M:%S")

#         if 'tbm' in df.columns:
#             df['tbm'] = 'TBM' + df['tbm'].astype(str)

#         if 'curing_press' in df.columns:
#             df['curing_press'] = 'M' + df['curing_press'].astype(str)

#             # Step 1: Build a lookup map from curing_press_dict: last4 of curing_press_id -> value
#             # Step 2: For each curing_press, find matching record by last 4 digits of curing_press_id
#             # Step 3: If last 4 digits of curing_press != last 4 of value, reassign using value
#             try:
#                 config_collection = client["production_db"]["curing_press_dict"]

#                 # Build map: last 4 of curing_press_id -> value (stripped)
#                 cp_id_to_value = {}
#                 for doc in config_collection.find(
#                     {"curing_press_id": {"$exists": True}, "value": {"$exists": True}},
#                     {"_id": 0, "curing_press_id": 1, "value": 1}
#                 ):
#                     cp_id = str(doc.get("curing_press_id", "")).strip().upper()
#                     val = str(doc.get("value", "")).strip()
#                     if cp_id and val:
#                         cp_id_to_value[cp_id[-4:]] = val  # key = last 4 of curing_press_id

#                 logging.info(f"curing_press_dict map (last4 -> value): {cp_id_to_value}")

#                 def map_curing_press(cp):
#                     if not cp or not cp_id_to_value:
#                         return cp
#                     cp_str = str(cp).strip().upper()
#                     cp_last4 = cp_str[-4:]

#                     # Step 2: find matching config record by last 4 of curing_press_id
#                     matched_value = cp_id_to_value.get(cp_last4)
#                     if matched_value is None:
#                         return cp_str  # no matching config record, return as-is

#                     # Step 3: compare last 4 of curing_press with last 4 of value
#                     if cp_last4 != matched_value[-4:]:
#                         return f"MP0{matched_value}"
#                     return cp_str  # already matches value

#                 df['curing_press'] = df['curing_press'].apply(map_curing_press)
#                 logging.info(f"curing_press sample after mapping: {df['curing_press'].head().tolist()}")

#             except Exception as e:
#                 logging.info(f"Curing press mapping failed: {e}")

#         special_char_regex = re.compile(r'[^0-9]')
#         rows_with_null_barcode = df[df['barcode'].isnull()]
#         rows_with_special_char_barcode = df[df['barcode'].astype(str).apply(lambda x: bool(special_char_regex.search(x)))]
#         invalid_barcode_rows = pd.concat([rows_with_null_barcode, rows_with_special_char_barcode]).drop_duplicates()
#         df = df[~df.index.isin(invalid_barcode_rows.index)]
#         df = df.dropna(axis=1, how='all')
#         df = df.loc[:, ~df.apply(lambda x: x.apply(lambda val: isinstance(val, str) and val.strip() == '').all())]
#         duplicates_df = df[df.duplicated(subset=['barcode'], keep=False)]
#         df = df.drop_duplicates(subset=['barcode'])

#         if 'barcode' in df.columns:
#             df['barcode'] = (df['barcode'].astype(str)
#                              .apply(lambda x: x[:-2] if x.endswith('.0') else x)
#                              .apply(lambda x: x.lstrip('0')))

#         df['tbm_shift'] = df['tbm_buildtime'].apply(get_shift)
#         df['curing_shift'] = df['curing_infeedtime'].apply(get_shift)

#         def extract_and_convert_build_date(row):
#             build_date = DEFAULT_DATETIME
#             tbm_buildtime = row.get('tbm_buildtime', '')
#             if isinstance(tbm_buildtime, str):
#                 try:
#                     build_date = tbm_buildtime[:10]
#                     build_date = convert_string_to_date(build_date)
#                     if build_date:
#                         return build_date.isoformat()
#                 except Exception as e:
#                     logging.info("Error extracting build_date: %s", e)
#             return "No Date"

#         df['build_date'] = df.apply(extract_and_convert_build_date, axis=1)
#         df['build_date'] = pd.to_datetime(df['build_date'], errors='coerce')

#         output_messages.append("CIMCORP_HISTORICAL_DATA Cleaning successful")
#         logging.info("clean_cimcorp_data function is completed")
#         return df, invalid_barcode_rows, duplicates_df, output_messages

#     except Exception as e:
#         error_message = f"Error during cleaning: {e}"
#         output_messages.append(error_message)
#         return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), output_messages 
    
def insert_cleaned_data(db_client, cleaned_df, database_name, filename):
    output_messages = []
    logging.info("insert_cleaned_data function is just started")
    try:
        if cleaned_df.empty:
            output_messages.append("No valid data to save after cleaning.")
            return output_messages
        if database_name == "TUB_HISTORICAL":
            cleaned_collection_name = TUB_CLEANED_PREFIX_history + filename.replace(".csv", "").replace(".xlsx", "")
            target_db_name = Data_Clean_TUB_HISTORICAL_DATA
        elif database_name == "CIMCORP_HISTORICAL_DATA":
            cleaned_collection_name = CIMCORP_CLEANED_PREFIX_history + filename.replace(".csv", "").replace(".xlsx", "")
            target_db_name = Data_Clean_CIMCORP_HISTORICAL_DATA
        else:
            error_message = f"Unknown database: {database_name}. Cannot insert cleaned data."
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

# <<<--- CHANGE START -------------------------------------------------
# CORRECT format - list of (field_name, direction) tuples
UNIQUE_INDEX_FIELDS = [
    ("barcode", 1), ("modelno", 1), ("sku", 1), ("sizecode", 1), ("gt_code", 1),
    ("ubno", 1), ("date", 1), ("time", 1), ("tbm", 1), ("curing_press", 1),
    ("curing_mouldid", 1), ("tbm_buildtime", 1), ("curing_infeedtime", 1),
    ("con_kgf", 1), ("cwrfv_oa_kgf", 1), ("ccwrfv_oa_kgf", 1),
    ("cwrfv_oa1h_kgf", 1), ("ccwrfv_oa1h_kgf", 1), ("lfv_oa_kgf", 1),
    ("lfv_oa_1_kgf", 1), ("upper_g", 1), ("lower_g", 1), ("static_g", 1),
    ("up_low_g", 1), ("total_rank", 1)
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

# <<<--- CHANGE END ---------------------------------------------------

def process_data_for_api(db_client, source_data, database_name, filename):
    # Sanitize record dicts so NaT / NaN / numpy types are safe for MongoDB BSON
    # (these can re-emerge when pandas reconstructs a DataFrame from None values)
    def _sanitize_records(records):
        return [{k: _to_native_hist(v) for k, v in rec.items()} for rec in records]

    output_messages = []
    cleaned_collection_name = None
    invalid_collection_name = None
    logging.info("process_data_for_api function is just started")
    try:
        df = pd.DataFrame(list(source_data))
        if database_name == "TUB_HISTORICAL":
            cleaned_df, invalid_df, tub_messages = clean_tub_data(df.copy())
            output_messages.extend(tub_messages)
            cleaned_collection_name = TUB_CLEANED_PREFIX_history + filename.replace(".csv", "").replace(".xlsx", "")
            invalid_collection_name = "invalid_data_TUB_" + filename.replace(".csv", "").replace(".xlsx", "")
            
            if not invalid_df.empty:
                invalid_df = invalid_df.drop('_id', axis=1, errors='ignore')
                invalid_collect = db_client[INVALID_ROWS_DB][invalid_collection_name]
                db_client[INVALID_ROWS_DB][invalid_collection_name].insert_many(_sanitize_records(invalid_df.to_dict("records")))
                output_messages.append(f"Invalid TUB_HISTORICAL data saved to invalid_data_TUB.{invalid_collection_name}")
            else:
                output_messages.append("No invalid TUB_HISTORICAL data to save.")
            
            output_messages.extend(insert_cleaned_data(db_client, cleaned_df, database_name, filename))
            
            if not cleaned_df.empty:
                cleaned_df = cleaned_df.drop('_id', axis=1, errors='ignore')
                
                # CHANGE 1: Delete all existing data in combined TUB collection before insert
                combined_tub_collection = db_client[COMBINED_Database_history][ALL_COMBINED_TUB_COLLECTION_history]
                delete_result = combined_tub_collection.delete_many({})
                output_messages.append(f"Deleted {delete_result.deleted_count} existing documents from {COMBINED_Database_history}.{ALL_COMBINED_TUB_COLLECTION_history}")
                
                combined_tub_collection.insert_many(_sanitize_records(cleaned_df.to_dict("records")))
                output_messages.append(f"Cleaned TUB_HISTORICAL data pushed to {COMBINED_Database_history}.{ALL_COMBINED_TUB_COLLECTION_history}")
            else:
                output_messages.append("No cleaned TUB_HISTORICAL data to push to combined collection")

        elif database_name == "CIMCORP_HISTORICAL_DATA":
            cleaned_df, invalid_barcode_rows, duplicates_df, cimcorp_messages = clean_cimcorp_data(df.copy())
            output_messages.extend(cimcorp_messages)
            cleaned_collection_name = CIMCORP_CLEANED_PREFIX_history + filename.replace(".csv", "").replace(".xlsx", "")
            invalid_collection_name = "invalid_data_CIMCORP_" + filename.replace(".csv", "").replace(".xlsx", "")
            
            if not invalid_barcode_rows.empty:
                invalid_barcode_rows = invalid_barcode_rows.drop('_id', axis=1, errors='ignore')
                db_client[INVALID_ROWS_DB][invalid_collection_name].insert_many(_sanitize_records(invalid_barcode_rows.to_dict("records")))
                output_messages.append(f"Invalid CIMCORP_HISTORICAL_DATA data saved to invalid_data_CIMCORP.{invalid_collection_name}")
            else:
                output_messages.append("No invalid barcode CIMCORP_HISTORICAL_DATA data to save.")
            
            if not duplicates_df.empty:
                duplicate_collection_name = f"duplicate_CIMCORP_{filename.replace('.csv', '').replace('.xlsx', '')}"
                duplicates_df = duplicates_df.drop('_id', axis=1, errors='ignore')
                db_client[INVALID_ROWS_DB][duplicate_collection_name].insert_many(_sanitize_records(duplicates_df.to_dict("records")))
                output_messages.append(f"Duplicate CIMCORP_HISTORICAL_DATA data saved to {INVALID_ROWS_DB}.{duplicate_collection_name}")
            else:
                output_messages.append("No duplicate CIMCORP_HISTORICAL_DATA data to save.")
            
            output_messages.extend(insert_cleaned_data(db_client, cleaned_df, database_name, filename))
            
            if not cleaned_df.empty:
                cleaned_df = cleaned_df.drop('_id', axis=1, errors='ignore')
                
                # CHANGE 2: Delete all existing data in combined CIMCORP collection before insert
                combined_cimcorp_collection = db_client[COMBINED_Database_history][ALL_COMBINED_CIMCORP_COLLECTION_history]
                delete_result = combined_cimcorp_collection.delete_many({})
                output_messages.append(f"Deleted {delete_result.deleted_count} existing documents from {COMBINED_Database_history}.{ALL_COMBINED_CIMCORP_COLLECTION_history}")
                
                combined_cimcorp_collection.insert_many(_sanitize_records(cleaned_df.to_dict("records")))
                output_messages.append(f"Cleaned CIMCORP_HISTORICAL_DATA data pushed to {COMBINED_Database_history}.{ALL_COMBINED_CIMCORP_COLLECTION_history}")
            else:
                output_messages.append("No cleaned CIMCORP_HISTORICAL_DATA data to push to combined collection")
        else:
            error_message = f"Unknown database: {database_name}"
            output_messages.append(error_message)
            return output_messages, None, None
    except Exception as e:
        error_message = f"Error processing data: {e}"
        output_messages.append(error_message)
        logging.error("process_data_for_api error: %s", e, exc_info=True)
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
        source_db = client[COMBINED_Database_history]
        target_db = client[TUB_CIMCORP_DB_history]

        # Get counts first to estimate size
        tub_count = source_db[ALL_COMBINED_TUB_COLLECTION_history].count_documents({})
        cimcorp_count = source_db[ALL_COMBINED_CIMCORP_COLLECTION_history].count_documents({})
        
        output_messages.append(f"TUB records: {tub_count}, CIMCORP records: {cimcorp_count}")
        
        if tub_count == 0:
            output_messages.append("TUB data is empty. No data to process.")
            return output_messages

        BATCH_SIZE = 5000
        total_tub_processed = 0
        total_inner_processed = 0

        # Get channel mapping data once
        db = client[config_databse]
        channel_cursor = db[channel_no_collect].find({})
        channel_data = list(channel_cursor)
        
        if not channel_data:
            output_messages.append("Channel number collection is empty.")
            return output_messages

        # Pre-process channel data for fast lookup
        sku_to_modelno = {}
        valid_gt_combinations = set()
        
        for record in channel_data:
            modelno = str(record.get('modelno', ''))
            sku = record.get('sku', '')
            gt_codes = record.get('gt_code', [])
            
            if not isinstance(gt_codes, list):
                gt_codes = [gt_codes]
                
            if sku:
                sku_to_modelno[sku] = modelno
                
            for gt_code in gt_codes:
                valid_gt_combinations.add((modelno, str(gt_code)))

        # Build lookups from CIMCORP data - NOW INCLUDING ALL FIELDS
        cimcorp_lookups = build_cimcorp_lookups_complete(source_db, BATCH_SIZE)

        # Build date_and_time lookup from TUB data
        date_and_time_lookup = build_tub_date_lookup(source_db, BATCH_SIZE)

        # Ensure indexes before processing
        for col_name in [tub_collection_name_history, inner_join_collection_name_history]:
            coll = target_db[col_name]
            ensure_unique_index(coll)

        # Process TUB data in batches with streaming
        tub_batch = []
        inner_batch = []
        
        tub_cursor_full = source_db[ALL_COMBINED_TUB_COLLECTION_history].find().batch_size(BATCH_SIZE)
        
        for tub_doc in tub_cursor_full:
            # Process TUB document
            processed_tub_doc = process_tub_document(
                tub_doc, 
                cimcorp_lookups['uftesting_lookup'], 
                cimcorp_lookups['operation_lookup'],
                cimcorp_lookups['operation_time_lookup'],
                cimcorp_lookups['visualinspection_time_lookup'],
                cimcorp_lookups['tbm_gtcode_lookup'],
                cimcorp_lookups['cimcorp_tbm_lookup'], 
                cimcorp_lookups['curing_press_lookup'], 
                cimcorp_lookups['curing_mouldid_lookup'],
                cimcorp_lookups['curing_infeed_lookup'], 
                cimcorp_lookups['tbm_build_lookup'], 
                cimcorp_lookups['build_date_lookup'],
                cimcorp_lookups['sku_lookup'], 
                date_and_time_lookup, 
                cimcorp_lookups['tbm_buildtime_lookup'],
                valid_gt_combinations, 
                sku_to_modelno
            )
            
            if processed_tub_doc:
                tub_batch.append(processed_tub_doc)
                total_tub_processed += 1
                
                # Check if this barcode exists in CIMCORP for inner join
                barcode = tub_doc.get('barcode')
                if barcode and barcode in cimcorp_lookups['complete_docs']:
                    inner_doc = create_inner_join_document_complete(
                        tub_doc, 
                        processed_tub_doc, 
                        cimcorp_lookups['complete_docs'][barcode],
                        cimcorp_lookups['uftesting_lookup'], 
                        cimcorp_lookups['operation_lookup'], 
                        sku_to_modelno
                    )
                    if inner_doc:
                        inner_batch.append(inner_doc)
                        total_inner_processed += 1
            
            # Insert in batches
            if len(tub_batch) >= BATCH_SIZE:
                inserted_tub = chunked_insert(target_db[tub_collection_name_history], tub_batch)
                # output_messages.append(f"Inserted {inserted_tub} TUB records")
                tub_batch = []
                
            if len(inner_batch) >= BATCH_SIZE:
                inserted_inner = chunked_insert(target_db[inner_join_collection_name_history], inner_batch)
                # output_messages.append(f"Inserted {inserted_inner} INNER records")
                inner_batch = []

        # Insert remaining records
        if tub_batch:
            inserted_tub = chunked_insert(target_db[tub_collection_name_history], tub_batch)
            # output_messages.append(f"Inserted final {inserted_tub} TUB records")
            
        if inner_batch:
            inserted_inner = chunked_insert(target_db[inner_join_collection_name_history], inner_batch)
            # output_messages.append(f"Inserted final {inserted_inner} INNER records")

        output_messages.append(f"Total TUB records processed: {total_tub_processed}")
        output_messages.append(f"Total INNER JOIN records processed: {total_inner_processed}")

    except Exception as e:
        error_message = f"An error occurred: {e}"
        output_messages.append(error_message)
        output_messages.append(traceback.format_exc())
   
    return output_messages

def build_cimcorp_lookups_complete(source_db, batch_size):
    """Build complete CIMCORP document lookups including all fields"""
    cimcorp_cursor = source_db[ALL_COMBINED_CIMCORP_COLLECTION_history].find().batch_size(batch_size)
    
    uftesting_lookup = {}
    operation_lookup = {}
    operation_time_lookup = {}
    visualinspection_time_lookup = {}
    tbm_gtcode_lookup = {}
    cimcorp_tbm_lookup = {}
    curing_press_lookup = {}
    curing_mouldid_lookup = {}
    curing_infeed_lookup = {}
    tbm_build_lookup = {}
    build_date_lookup = {}
    sku_lookup = {}
    tbm_buildtime_lookup = {}
    complete_docs = {}  # Store complete CIMCORP documents by barcode

    for doc in cimcorp_cursor:
        barcode = doc.get('barcode')
        if not barcode:
            continue
        
        # Store complete document
        complete_docs[barcode] = doc.copy()
            
        uftesting_lookup[barcode] = doc.get('uftesting_result')
        operation_lookup[barcode] = doc.get('operation')
        operation_time_lookup[barcode] = doc.get('operation_time')
        visualinspection_time_lookup[barcode] = doc.get('visualinspection_time')
        tbm_gtcode_lookup[barcode] = doc.get('tbm_gtcode')
        cimcorp_tbm_lookup[barcode] = doc.get('tbm')
        curing_press_lookup[barcode] = doc.get('curing_press')
        curing_mouldid_lookup[barcode] = doc.get('curing_mouldid')
        curing_infeed_lookup[barcode] = doc.get('curing_infeedtime')
        tbm_build_lookup[barcode] = doc.get('tbm_buildtime')
        build_date_lookup[barcode] = doc.get('build_date')
        sku_lookup[barcode] = doc.get('sku')
        tbm_buildtime_lookup[barcode] = doc.get('tbm_buildtime')

    return {
        'uftesting_lookup': uftesting_lookup,
        'operation_lookup': operation_lookup,
        'operation_time_lookup': operation_time_lookup,
        'visualinspection_time_lookup': visualinspection_time_lookup,                                              
        'tbm_gtcode_lookup': tbm_gtcode_lookup,
        'cimcorp_tbm_lookup': cimcorp_tbm_lookup,
        'curing_press_lookup': curing_press_lookup,
        'curing_mouldid_lookup': curing_mouldid_lookup,
        'curing_infeed_lookup': curing_infeed_lookup,
        'tbm_build_lookup': tbm_build_lookup,
        'build_date_lookup': build_date_lookup,
        'sku_lookup': sku_lookup,
        'tbm_buildtime_lookup': tbm_buildtime_lookup,
        'complete_docs': complete_docs
    }

def create_inner_join_document_complete(tub_doc, processed_tub_doc, cimcorp_doc, uftesting_lookup, operation_lookup, sku_to_modelno):
    """Create complete inner join document with both TUB and CIMCORP fields"""
    
    barcode = tub_doc.get('barcode')
    if not barcode:
        return None

    # Start with CIMCORP document as base (this gives you all CIMCORP fields)
    inner_doc = cimcorp_doc.copy()
    
    # Remove _id from CIMCORP doc to avoid conflicts
    if '_id' in inner_doc:
        del inner_doc['_id']
    
    # Add ALL TUB fields (this will overwrite any duplicate fields with TUB data)
    for key, value in processed_tub_doc.items():
        # Skip _id from TUB doc
        if key != '_id':
            inner_doc[key] = value
    
    # Apply CIMCORP business logic to uftesting_result (from original CIMCORP data)
    total_rank = inner_doc.get('total_rank')
    uftesting_result = inner_doc.get('uftesting_result', 'Unknown')
    operation = inner_doc.get('operation', 'Unknown')
    
    if uftesting_result == 'Unknown' and operation != 'Unknown' and total_rank in [1, 2, 3]:
        inner_doc['uftesting_result'] = "Repl"
    elif operation == 'Unknown' and total_rank in [1, 2]:
        inner_doc['uftesting_result'] = "OE1"
    elif uftesting_result == 'Unknown' and operation == 'Unknown' and total_rank == 3:
        inner_doc['uftesting_result'] = "Repl"

    # Also apply the same logic to the TUB version of the field for consistency
    uftesting_result_tub = inner_doc.get('uftesting_result_tub', 'Unknown')
    operation_tub = inner_doc.get('operation_tub', 'Unknown')
    
    if uftesting_result_tub == 'Unknown' and operation_tub != 'Unknown' and total_rank in [1, 2, 3]:
        inner_doc['uftesting_result_tub'] = "Repl"
    elif operation_tub == 'Unknown' and total_rank in [1, 2]:
        inner_doc['uftesting_result_tub'] = "OE1"
    elif uftesting_result_tub == 'Unknown' and operation_tub == 'Unknown' and total_rank == 3:
        inner_doc['uftesting_result_tub'] = "Repl"

    # Add modelno from SKU mapping
    sku = inner_doc.get('sku', 'Unknown')
    if sku != 'Unknown':
        inner_doc['modelno_cimcorp'] = sku_to_modelno.get(sku, 'Unknown')
    else:
        inner_doc['modelno_cimcorp'] = 'Unknown'

    return inner_doc

# Keep the existing helper functions (they remain the same)
def build_tub_date_lookup(source_db, batch_size):
    """Build date_and_time lookup from TUB data"""
    tub_cursor = source_db[ALL_COMBINED_TUB_COLLECTION_history].find(
        {}, 
        {"barcode": 1, "date_and_time": 1, "_id": 0}
    ).batch_size(batch_size)
    
    date_and_time_lookup = {}
    for doc in tub_cursor:
        if doc.get('barcode'):
            date_and_time_lookup[doc['barcode']] = doc.get('date_and_time')
    
    return date_and_time_lookup

def process_tub_document(tub_doc, uftesting_lookup, operation_lookup, operation_time_lookup, visualinspection_time_lookup, tbm_gtcode_lookup,
                        cimcorp_tbm_lookup, curing_press_lookup, curing_mouldid_lookup,
                        curing_infeed_lookup, tbm_build_lookup, build_date_lookup,
                        sku_lookup, date_and_time_lookup, tbm_buildtime_lookup,
                        valid_gt_combinations, sku_to_modelno):
    """Process a single TUB document and apply all mappings"""
    
    barcode = tub_doc.get('barcode')
    if not barcode:
        return None

    # Create a copy of the document
    processed_doc = tub_doc.copy()
    
    # Remove _id to avoid duplication issues
    if '_id' in processed_doc:
        del processed_doc['_id']

    # Apply all lookups
    processed_doc['uftesting_result_tub'] = uftesting_lookup.get(barcode, 'Unknown')
    processed_doc['operation_tub'] = operation_lookup.get(barcode, 'Unknown')
    processed_doc['operation_time_tub'] = operation_time_lookup.get(barcode, 'Unknown')
    processed_doc['visualinspection_time_tub'] = visualinspection_time_lookup.get(barcode, 'Unknown')
    processed_doc['tbm_gtcode_tub'] = tbm_gtcode_lookup.get(barcode, 'Unknown')
    processed_doc['cimcorp_tbm_tub'] = cimcorp_tbm_lookup.get(barcode, 'Unknown')
    processed_doc['curing_press_tub'] = curing_press_lookup.get(barcode, 'Unknown')
    processed_doc['curing_mouldid_tub'] = curing_mouldid_lookup.get(barcode, 'Unknown')
    processed_doc['curing_infeedtime_tub'] = curing_infeed_lookup.get(barcode, 'Unknown')
    processed_doc['tbm_buildtime_tub'] = tbm_build_lookup.get(barcode, 'Unknown')
    processed_doc['build_date_tub'] = build_date_lookup.get(barcode, 'Unknown')
    processed_doc['sku_tub'] = sku_lookup.get(barcode, 'Unknown')
    processed_doc['datetime_dt_tub'] = date_and_time_lookup.get(barcode, 'Unknown')
    processed_doc['datetime_dt_cim'] = tbm_buildtime_lookup.get(barcode, 'Unknown')

    # Apply business logic for uftesting_result_tub
    total_rank = processed_doc.get('total_rank')
    uftesting_result = processed_doc['uftesting_result_tub']
    operation = processed_doc['operation_tub']
    
    if uftesting_result == 'Unknown' and operation != 'Unknown' and total_rank in [1, 2, 3]:
        processed_doc['uftesting_result_tub'] = "Repl"
    elif operation == 'Unknown' and total_rank in [1, 2]:
        processed_doc['uftesting_result_tub'] = "OE1"
    elif uftesting_result == 'Unknown' and operation == 'Unknown' and total_rank == 3:
        processed_doc['uftesting_result_tub'] = "Repl"

    # Apply shift logic
    if processed_doc['tbm_buildtime_tub'] != 'Unknown':
        processed_doc['tbm_buildtime_shift'] = get_shift(processed_doc['tbm_buildtime_tub'])
    else:
        processed_doc['tbm_buildtime_shift'] = 'Unknown'

    # # Filter by valid GT combinations
    # modelno = str(processed_doc.get('modelno', ''))
    # tbm_gtcode = str(processed_doc.get('tbm_gtcode_tub', ''))
    
    # if (modelno, tbm_gtcode) not in valid_gt_combinations:
    #     return None
    
    
    # Filter by valid GT combinations only when CIMCORP data is available.
    # When CIMCORP is absent, tbm_gtcode_tub is always 'Unknown' so the filter
    # would drop every TUB record — skip it in that case.
    if valid_gt_combinations:
        modelno = str(processed_doc.get('modelno', ''))
        tbm_gtcode = str(processed_doc.get('tbm_gtcode_tub', ''))
        if tbm_gtcode != 'Unknown':
            # CIMCORP data available — apply full GT combination filter
            if (modelno, tbm_gtcode) not in valid_gt_combinations:
                return None
        # else: CIMCORP data absent — modelno was already pre-filtered by the
        # SQL query (IN clause from channel_no_ufm), so allow the record through

    return processed_doc

# def backfill_placeholder_values():
#     """
#     On every run, scan tub_collection_name_history and inner_join_collection_name_history
#     for documents where CIMCORP-sourced columns contain placeholder values
#     ("Unknown", None, "", "None", "nan", "NaT").
#     If the current CIMCORP combined collection now has real values for those
#     barcodes, update the target documents — without touching any existing code.
#     """
#     output_messages = []
#     logging.info("backfill_placeholder_values started")

#     PLACEHOLDER_VALUES = {"Unknown", None, "", "None", "nan", "NaT", "unknown"}

#     # CIMCORP-sourced columns that may have been empty on first run
#     CIMCORP_COLUMNS = [
#         "tbm", "tbm_buildtime", "tbm_gtcode", "curing_press", "curing_mouldid",
#         "curing_infeedtime", "sku", "build_date", "uftesting_result",
#         "operation", "operation_time", "visualinspection_time",
#         "tbm_shift", "curing_shift"
#     ]

#     def is_placeholder(val):
#         if val is None:
#             return True
#         if isinstance(val, float) and (val != val):  # NaN check
#             return True
#         return str(val).strip() in PLACEHOLDER_VALUES

#     try:
#         source_db = client[COMBINED_Database_history]
#         target_db = client[TUB_CIMCORP_DB_history]

#         # Build fresh CIMCORP lookup from current combined collection
#         cimcorp_cursor = source_db[ALL_COMBINED_CIMCORP_COLLECTION_history].find(
#             {}, {col: 1 for col in ["barcode"] + CIMCORP_COLUMNS}
#         )
#         cimcorp_lookup = {}
#         for doc in cimcorp_cursor:
#             bc = doc.get("barcode")
#             if bc:
#                 cimcorp_lookup[bc] = doc

#         if not cimcorp_lookup:
#             output_messages.append("Backfill: no CIMCORP data available, skipping.")
#             return output_messages

#         total_updated = 0

#         for collection_name in [tub_collection_name_history, inner_join_collection_name_history]:
#             coll = target_db[collection_name]
#             updated_in_coll = 0

#             # Find documents that have at least one placeholder in a CIMCORP column
#             # Build a $or filter for efficiency
#             or_conditions = []
#             for col in CIMCORP_COLUMNS:
#                 or_conditions.append({col: {"$in": [None, "", "Unknown", "unknown", "None", "nan", "NaT"]}})
#                 or_conditions.append({col: {"$exists": False}})

#             docs_to_check = coll.find({"$or": or_conditions}, {"barcode": 1, **{col: 1 for col in CIMCORP_COLUMNS}})

#             for doc in docs_to_check:
#                 bc = doc.get("barcode")
#                 if not bc or bc not in cimcorp_lookup:
#                     continue

#                 cimcorp_doc = cimcorp_lookup[bc]
#                 update_fields = {}

#                 for col in CIMCORP_COLUMNS:
#                     current_val = doc.get(col)
#                     new_val = cimcorp_doc.get(col)

#                     # Only update if current is placeholder AND new value is real
#                     if is_placeholder(current_val) and not is_placeholder(new_val):
#                         update_fields[col] = new_val

#                 if update_fields:
#                     coll.update_one(
#                         {"_id": doc["_id"]},
#                         {"$set": update_fields}
#                     )
#                     updated_in_coll += 1

#             output_messages.append(f"Backfill [{collection_name}]: updated {updated_in_coll} documents.")
#             total_updated += updated_in_coll

#         output_messages.append(f"Backfill complete. Total documents updated: {total_updated}")
#         logging.info(f"backfill_placeholder_values complete. Updated: {total_updated}")

#     except Exception as e:
#         msg = f"Backfill error: {e}"
#         output_messages.append(msg)
#         logging.error(msg)

#     return output_messages

# def run_data_cleaning():
#     start_time = time.time()
#     output_messages = []
#     cleaned_collection_names = []
#     invalid_collection_names = []
#     logging.info("run_data_cleaning function is just started")
#     try:
#         files_to_process = get_unprocessed_files_api()
#         if not files_to_process:
#             final_message = "No unprocessed files found for data cleaning."
#             return {"status": "success", "message": final_message, "output_log": output_messages,
#                     "cleaned_collections": cleaned_collection_names, "invalid_collections": invalid_collection_names,
#                     "execution_time": 0}

#         for file_data in files_to_process:
#             database_name = file_data["database_name"]
#             filename = file_data["filename"]
#             file_id = file_data["_id"]
#             output_messages.append(f"Processing file: {filename} from database: {database_name}")

#             source_collection_name = sanitize_collection_name(filename)
#             source_db_name = TUB_DB_history if database_name == "TUB_HISTORICAL" else CIMCORP_DB_history
#             source_data = client[source_db_name][source_collection_name].find()

#             process_msgs, cleaned_collect_name, invalid_collect_name = process_data_for_api(
#                 client, source_data, database_name, filename)
#             output_messages.extend(process_msgs)
#             if cleaned_collect_name:
#                 cleaned_collection_names.append(cleaned_collect_name)
#             if invalid_collect_name:
#                 invalid_collection_names.append(invalid_collect_name)

#             # Mark file as processed
#             client[FILE_TRACKING_DB][FILE_TRACKING_COLLECTION].update_one(
#                 {"_id": file_id},
#                 {"$set": {"processed": True, "processed_timestamp": some_function()}}
#             )
#             output_messages.append(f"File {filename} marked as processed.")

#         # ------------------------------------------------------------------
#         # ONE-TIME MongoDB-native copy (runs once per run_data_cleaning)
#         # ------------------------------------------------------------------
#         output_messages.append("Starting MongoDB-native copy to final collections...")
#         copy_msgs = copy_to_single_collection_api()
#         output_messages.extend(copy_msgs)

#         # ------------------------------------------------------------------
#         # Backfill: update placeholder values with real data from current run
#         # ------------------------------------------------------------------
#         output_messages.append("Starting backfill of placeholder values...")
#         backfill_msgs = backfill_placeholder_values()
#         output_messages.extend(backfill_msgs)

#         end_time = time.time()
#         execution_time = end_time - start_time
#         final_message = "Data cleaning + final copy completed."
#         output_messages.append(f"Total execution time: {execution_time:.2f} s")
#         return {"status": "success", "message": final_message, "output_log": output_messages,
#                 "cleaned_collections": cleaned_collection_names, "invalid_collections": invalid_collection_names,
#                 "execution_time": execution_time}

#     except Exception as e:
#         error_message = f"Error in run_data_cleaning: {e}"
#         output_messages.append(error_message)
#         return {"status": "error", "message": error_message, "output_log": output_messages,
#                 "cleaned_collections": cleaned_collection_names, "invalid_collections": invalid_collection_names,
#                 "execution_time": 0}

def run_data_cleaning():
    start_time = time.time()
    output_messages = []
    cleaned_collection_names = []
    invalid_collection_names = []
    logging.info("run_data_cleaning function is just started")
    try:
        files_to_process = get_unprocessed_files_api()
        if not files_to_process:
            final_message = "No unprocessed files found for data cleaning."
            return {"status": "success", "message": final_message, "output_log": output_messages,
                    "cleaned_collections": cleaned_collection_names, "invalid_collections": invalid_collection_names,
                    "execution_time": 0}

        for file_data in files_to_process:
            database_name = file_data["database_name"]
            filename = file_data["filename"]
            file_id = file_data["_id"]
            output_messages.append(f"Processing file: {filename} from database: {database_name}")

            source_collection_name = sanitize_collection_name(filename)
            source_db_name = TUB_DB_history if database_name == "TUB_HISTORICAL" else CIMCORP_DB_history
            source_data = client[source_db_name][source_collection_name].find()

            process_msgs, cleaned_collect_name, invalid_collect_name = process_data_for_api(
                client, source_data, database_name, filename)
            output_messages.extend(process_msgs)
            if cleaned_collect_name:
                cleaned_collection_names.append(cleaned_collect_name)
            if invalid_collect_name:
                invalid_collection_names.append(invalid_collect_name)

            # Mark file as processed
            client[FILE_TRACKING_DB][FILE_TRACKING_COLLECTION].update_one(
                {"_id": file_id},
                {"$set": {"processed": True, "processed_timestamp": some_function()}}
            )
            output_messages.append(f"File {filename} marked as processed.")

        # ------------------------------------------------------------------
        # ONE-TIME MongoDB-native copy (runs once per run_data_cleaning)
        # ------------------------------------------------------------------
        output_messages.append("Starting MongoDB-native copy to final collections...")
        copy_msgs = copy_to_single_collection_api()
        output_messages.extend(copy_msgs)

        end_time = time.time()
        execution_time = end_time - start_time
        final_message = "Data cleaning + final copy completed."
        output_messages.append(f"Total execution time: {execution_time:.2f} s")
        return {"status": "success", "message": final_message, "output_log": output_messages,
                "cleaned_collections": cleaned_collection_names, "invalid_collections": invalid_collection_names,
                "execution_time": execution_time}

    except Exception as e:
        error_message = f"Error in run_data_cleaning: {e}"
        output_messages.append(error_message)
        return {"status": "error", "message": error_message, "output_log": output_messages,
                "cleaned_collections": cleaned_collection_names, "invalid_collections": invalid_collection_names,
                "execution_time": 0}

date_formats = ["%d-%m-%Y", "%Y-%m-%d"]

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

        logging.info("Inserting TUB_HISTORICAL sample data into MongoDB...")
        insert_with_timestamp(df_tub, TUB_CLEANED_PREFIX_history, "TUB_HISTORICAL")
        logging.info("TUB_HISTORICAL sample data inserted and tracked.")

        if df_cimcorp is not None and not df_cimcorp.empty:
            logging.info("Inserting CIMCORP_HISTORICAL_DATA sample data into MongoDB...")
            insert_with_timestamp(df_cimcorp, CIMCORP_CLEANED_PREFIX_history, "CIMCORP_HISTORICAL_DATA")
            logging.info("CIMCORP_HISTORICAL_DATA sample data inserted and tracked.")
        else:
            logging.info("CIMCORP data is empty — skipping CIMCORP insert, processing TUB data only.")

        ensure_admin_collection_exists()
        logging.info("Admin collection check completed.")
        result = run_data_cleaning()
        logging.info(f"Data cleaning result: {result['message']}")
        logging.info(f"Execution time: {result['execution_time']:.2f} seconds")
        print(result["message"])
        print(f"Execution time: {result['execution_time']:.2f} seconds")
        print("Output log:", result["output_log"])
        print("Cleaned collections:", result["cleaned_collections"])
        print("Invalid collections:", result["invalid_collections"])
    except Exception as e:
        error_message = f"Error in main function: {str(e)}"
        logging.error(error_message)
        logging.error(traceback.format_exc())
        print(error_message)

if __name__ == "__main__":
    main()

# def main():
#     try:
#         configure_logging()
#         warnings.filterwarnings("ignore")
#         logging.info("Starting the data cleaning process...")
#         logging.info("Inserting TUB_HISTORICAL sample data into MongoDB...")
#         insert_with_timestamp(df_tub, TUB_CLEANED_PREFIX_history, "TUB_HISTORICAL")
#         logging.info("TUB_HISTORICAL sample data inserted and tracked.")
#         logging.info("Inserting CIMCORP_HISTORICAL_DATA sample data into MongoDB...")
#         insert_with_timestamp(df_cimcorp, CIMCORP_CLEANED_PREFIX_history, "CIMCORP_HISTORICAL_DATA")
#         logging.info("CIMCORP_HISTORICAL_DATA sample data inserted and tracked.")
#         ensure_admin_collection_exists()
#         logging.info("Admin collection check completed.")
#         result = run_data_cleaning()
#         logging.info(f"Data cleaning result: {result['message']}")
#         logging.info(f"Execution time: {result['execution_time']:.2f} seconds")
#         print(result["message"])
#         print(f"Execution time: {result['execution_time']:.2f} seconds")
#         print("Output log:", result["output_log"])
#         print("Cleaned collections:", result["cleaned_collections"])
#         print("Invalid collections:", result["invalid_collections"])
#     except Exception as e:
#         error_message = f"Error in main function: {str(e)}"
#         logging.error(error_message)
#         logging.error(traceback.format_exc())
#         print(error_message)

# if __name__ == "__main__":
#     main()