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
    channel_no_collect,
    re_codes,
    alert_mail_db,
    admin_table,
    alert_users,
    TUB_CIMCORP_VISUAL_MERGED_DB_hisrory,
    DATA_MERGED_SUMMARY_COLLECTION_history,
    DATA_MERGED_SCRAP_COLLECTION_history,
    DATA_MERGED_TRACKING_COLLECTION_history,
    DATA_MERGED_TRACKING_TBM_COLLECTION_history,
    config_databse,
)

app = Flask(__name__)
app.secret_key = "supersecretkey"

def configure_logging():
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    current_date = datetime.now().strftime('%Y-%m-%d')
    log_filename = f'logs/log_historical_uat_{current_date}.txt'
    
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
cimcorp_db = client['VISUAL_HISTORICAL_DATA']
tub_db = client['TUB_CIMCORP_HOME']

# def insert_with_timestamp(df, collection_name_prefix, db_name):
#     timestamp = some_function().strftime("%Y%m%d_%H%M%S")
#     if db_name == 'VISUAL_HISTORICAL_DATA':
#         db = cimcorp_db
#     elif db_name == 'TUB_CIMCORP_HISTORICAL_DATA':
#         db = tub_db
#     else:
#         raise ValueError("Invalid database name")
#     collection_name = f"{collection_name_prefix}_{timestamp}"
#     collection = db[collection_name]
#     collection.insert_many(df.to_dict('records'))
#     track_file_in_file_tracking(collection_name_prefix, timestamp, len(df), db_name)

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
    if db_name == 'VISUAL_HISTORICAL_DATA':
        db = cimcorp_db
    elif db_name == 'TUB_CIMCORP_HISTORICAL_DATA':
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


############## with cronjo setup START ########################

# from datetime import datetime, timedelta, timezone

# # --- CURRENT UTC TIME ---
# now_utc = datetime.now(timezone.utc)

# # --- ALIGN TO HOUR (00 minutes only) ---
# aligned_now = now_utc.replace(minute=0, second=0, microsecond=0)

# # --- RUN ONLY EVERY 2 HOURS ---
# if aligned_now.hour % 2 != 0:
#     exit()   # ❌ skip execution

# # --- END TIME ---
# end_dt = aligned_now

# # --- START TIME (11 hours back) ---
# start_dt = end_dt - timedelta(hours=11)

# # --- FORMAT ---
# start_timestamp = start_dt.strftime("%Y-%m-%d %H:%M:%S")
# end_timestamp   = end_dt.strftime("%Y-%m-%d %H:%M:%S")

# # --- OUTPUT ---
# print("start_dt:", start_dt)
# print("end_dt:", end_dt)
# print("start_timestamp:", start_timestamp)
# print("end_timestamp:", end_timestamp)

######################################################
from datetime import datetime, timedelta, timezone

# --- CURRENT UTC TIME ---
now_utc = datetime.now(timezone.utc)

# --- ALIGN TO CURRENT HOUR :27 ---
end_dt = now_utc.replace(minute=27, second=0, microsecond=0)

# --- START TIME: 11 HOURS BACK ---
start_dt = end_dt - timedelta(hours=11)

# --- FORMAT ---
start_timestamp = start_dt.strftime("%Y-%m-%d %H:%M:%S")
end_timestamp = end_dt.strftime("%Y-%m-%d %H:%M:%S")

# --- OUTPUT ---
print("Current UTC :", now_utc)
print("Start Time  :", start_timestamp)
print("End Time    :", end_timestamp)

############## with cronjo setup END ########################

############### Manual Trigger START #################################

# utc = pytz.utc
# local_tz = datetime.now(utc)

# # # ---------------- MANUAL INPUT ----------------
# manual_start_date = '2026-09-19'
# manual_end_date   = '2026-09-26'
# manual_start_time = '06:00:00'
# manual_end_time   = '06:00:00'  

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
############### Manual Trigger END #################################

mongo_db = client["production_db_hungary"]
channel_coll = mongo_db["channel_no_ufm"]

# # -------- Fetch model numbers --------
modelno_list = channel_coll.distinct("sku")

# Keep all SKU strings as-is (SKUs are alphanumeric e.g. "GV2554520ULAWB")
modelno_list = [str(m) for m in modelno_list if m]

if not modelno_list:
    print("Warning: No model numbers found in MongoDB — continuing with hardcoded SKU list in queries.")
else:
    print("Model numbers from MongoDB:", modelno_list)

modelno_sql = ",".join(f"'{m}'" for m in modelno_list)

# # -------- Hardcoded SKU list --------
# modelno_list = [
#     "GV1956015ULANV",
#     "GV1755015ULPNH",
#     "GV2056015QT6NH"
# ]

# # Create SQL-ready SKU string
# modelno_sql = ",".join(f"'{m}'" for m in modelno_list)

# print("SKUs:", modelno_list)
# print("SQL SKUs:", modelno_sql)

# # to check the sku's ---
# mongo_db = client["production_db_hungary"]
# channel_coll = mongo_db["channel_no_ufm"]

# # # -------- Fetch model numbers --------
# # Only fetch SKUs where replacement_slob == "REP"
# modelno_list = channel_coll.distinct("sku", {"replacement_slob": "OE"})

# # Keep all SKU strings as-is (SKUs are alphanumeric e.g. "GV2554520ULAWB")
# modelno_list = [str(m) for m in modelno_list if m]

# if not modelno_list:
#     print("Warning: No model numbers found in MongoDB — continuing with hardcoded SKU list in queries.")
# else:
#     print("Model numbers from MongoDB (REP only):", modelno_list)

# modelno_sql = ",".join(f"'{m}'" for m in modelno_list)


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
        shift,

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
        lroth1,crroh1,rroh1ang,trroh1,brroh1,lrotang,lroth1ang,lrobang,lrobh1,lrobh1ang,
        ctdipang, ctdip,

        rfppcc* 0.101972 AS rfppcc,
        rfppcw * 0.101972 AS rfppcw,
        rfh1cc * 0.101972 AS rfh1cc,
        rfh1cw * 0.101972 AS rfh1cw,
        rfh2cc * 0.101972 AS rfh2cc,
        rfh2cw * 0.101972 AS rfh2cw,
        lfppcc * 0.101972 AS lfppcc,
        lfppcw * 0.101972 AS lfppcw,
        rfh3cw * 0.101972 AS rfh3cw,
        rfh4cw * 0.101972 AS rfh4cw,
        rfh5cw * 0.101972 AS rfh5cw,
        rfh6cw * 0.101972 AS rfh6cw,
        rfh7cw * 0.101972 AS rfh7cw,
        rfh8cw * 0.101972 AS rfh8cw,
        rfh9cw * 0.101972 AS rfh9cw,
        rfh10cw * 0.101972 AS rfh10cw,
        rfh11cw * 0.101972 AS rfh11cw,
        rfh12cw * 0.101972 AS rfh12cw,
        rfh13cw * 0.101972 AS rfh13cw,
        rfh14cw * 0.101972 AS rfh14cw,
        rfh15cw * 0.101972 AS rfh15cw,
        rfh16cw * 0.101972 AS rfh16cw,
        
        rfa1cw  AS rfa1cw,
        rfa2cw  AS rfa2cw,
        rfa3cw  AS rfa3cw,
        rfa4cw  AS rfa4cw,
        rfa5cw  AS rfa5cw,
        rfa6cw  AS rfa6cw,
        rfa7cw  AS rfa7cw,
        rfa8cw  AS rfa8cw,
        rfa9cw  AS rfa9cw,
        rfa10cw  AS rfa10cw,
        rfa11cw  AS rfa11cw,
        rfa12cw  AS rfa12cw,
        rfa13cw  AS rfa13cw,
        rfa14cw  AS rfa14cw,
        rfa15cw  AS rfa15cw,
        rfa16cw  AS rfa16cw,
        
        lfh1cw  AS lfh1cw,
        lfh2cw  AS lfh2cw,
        lfh3cw  AS lfh3cw,
        lfh4cw  AS lfh4cw,
        lfh5cw  AS lfh5cw,
        lfh6cw  AS lfh6cw,
        lfh7cw  AS lfh7cw,
        lfh8cw  AS lfh8cw,
        lfh9cw  AS lfh9cw,
        lfh10cw  AS lfh10cw,
        lfh11cw  AS lfh11cw,
        lfh12cw  AS lfh12cw,
        lfh13cw  AS lfh13cw,
        lfh14cw  AS lfh14cw,
        lfh15cw  AS lfh15cw,
        lfh16cw  AS lfh16cw,
        
        lfa1cw  AS lfa1cw,
        lfa2cw  AS lfa2cw,
        lfa3cw  AS lfa3cw,
        lfa4cw  AS lfa4cw,
        lfa5cw  AS lfa5cw,
        lfa6cw  AS lfa6cw,
        lfa7cw  AS lfa7cw,
        lfa8cw  AS lfa8cw,
        lfa9cw  AS lfa9cw,
        lfa10cw  AS lfa10cw,
        lfa11cw  AS lfa11cw,
        lfa12cw  AS lfa12cw,
        lfa13cw  AS lfa13cw,
        lfa14cw  AS lfa14cw,
        lfa15cw  AS lfa15cw,
        lfa16cw  AS lfa16cw,
        
        cony * 0.101972 AS cony,
        plys * 0.101972 AS plys,

        sbal, sbalang, couple, coupleang, upper, upperang, lower, lowerang, 
        lrot, lrob, crro, trro, brro,
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
        vrl_vhmc_mes_pa_spot_position,
        vrl_vhmc_mes_td_spot_position,
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
SELECT DISTINCT
    mt.tire_barcode,
    mt.gt_weighting_time,
    mt.vi_result,
    mt.stt_id,
    mt.loc_name,
    mt.ft_code,
    mt.ct_code AS sku,
    mt.last_change_time,
    mt.tbm_name AS tbm,
    mt.cp_name AS equ,
    mt.cp_curing_time,
    sur.sur_stt_id,
    sur.sur_loc_id,
    sur.sur_repair_time,
    sur.sur_repair_code AS re_code,
    sur.sur_repair_comment AS re_comment
FROM sc_wms_apollo_pcr.mv_tire mt
LEFT JOIN sc_apollo_mom.t_zf_test_completed tz
    ON tz.barcode = mt.tire_barcode
INNER JOIN sc_wms_apollo_pcr.stock_unit_repair sur
    ON mt.stt_id = sur.sur_stt_id
WHERE mt.ct_code IN ({modelno_sql})
AND mt.last_change_time >= '{start_timestamp}'
AND mt.last_change_time <  '{end_timestamp}'
"""
print("#########################")

df_cimcorp = inst.execute_query(q2)

# =================================================
# QUERY 3 — Tracking TBM CURING / REPAIR DATA (data_merged_tracking_historical_data)
# =================================================
q3 = f"""
SELECT DISTINCT
    mt.tire_barcode,
    mt.ct_code AS sku,
    mt.last_change_time,
    tbm.equipmentid AS PTB_ID,
    tbm.producedon AS PTB_time,
    tbm.vst_vhmc_mes_gt_code AS gt_code,
    tbm.vrl_vhmc_mes_br1_spot_position, 
    tbm.vrl_vhmc_mes_br2_spot_position, 
    tbm.vrl_vhmc_mes_cwap_spot_pos, 
    tbm.vrl_vhmc_mes_td_spot_position, 
    tbm.vrl_vhmc_mes_pa_spot_position, 
    tbm.vrl_vhmc_mes_barcode_spot_pos,
    tbm.vrl_vhmc_mes_bp1_spot_pos,
    tbm.vrl_vhmc_mes_bp2_spot_pos,
    curing.curingpressid AS CUR_ID,
    curing.curingtimestamp AS Cur_time,
    curing.curingmouldid AS cur_mouldid,
    sur.sur_repair_time,
    sur.sur_repair_code AS re_code,
    sur.sur_repair_comment AS re_comment
FROM sc_wms_apollo_pcr.mv_tire mt
LEFT JOIN sc_mhshistory.t_tbm_pcr_productiondata tbm
    ON tbm.vst_vhmc_mes_gt_barcode = mt.tire_barcode
LEFT JOIN sc_mhshistory.t_curing_history curing
    ON curing.barcodeid = mt.tire_barcode
INNER JOIN sc_wms_apollo_pcr.stock_unit_repair sur
    ON mt.stt_id = sur.sur_stt_id
WHERE mt.ct_code IN ({modelno_sql})
AND mt.last_change_time >= '{start_timestamp}'
AND mt.last_change_time <  '{end_timestamp}'
AND sur.sur_repair_code = '999'
"""
print("#########################")

df_tracking = inst.execute_query(q3)

											  

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

    hour = dt.hour

    # Shift A: 06:00 – 17:59
    if 6 <= hour <= 17:
        return "A"
    # Shift B: 18:00 – 23:59  OR  00:00 – 05:59 (overnight)
    elif hour >= 18 or hour < 6:
        return "B"
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
    # No-op: intermediate DBs (Data_Clean_TUB_CIMCORP_HISTORICAL_DATA, VISUAL_HISTORICAL_DATA,
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
    No intermediate DBs (TUB_CIMCORP_HISTORICAL_DATA, Data_Clean_TUB_CIMCORP_HISTORICAL_DATA,
    all_combined_data_home) are written to — data flows straight to the
    final merged DB via copy_to_single_collection_api.
    """
    output_messages = []
    cleaned_df = pd.DataFrame()
    logging.info("process_data_for_api function started")
    try:
        df = pd.DataFrame(list(source_data))
        if database_name == "TUB_CIMCORP_HISTORICAL_DATA":
            cleaned_df, invalid_df, tub_messages = clean_tub_data(df.copy())
            output_messages.extend(tub_messages)
            if not invalid_df.empty:
                invalid_df = invalid_df.drop('_id', axis=1, errors='ignore')
                invalid_collection_name = "invalid_data_TUB_" + filename.replace(".csv", "").replace(".xlsx", "")
                db_client[INVALID_ROWS_DB][invalid_collection_name].insert_many(df_to_mongo_records(invalid_df))
                output_messages.append(f"Invalid TUB data saved to {INVALID_ROWS_DB}.{invalid_collection_name}")
            cleaned_df = cleaned_df.drop('_id', axis=1, errors='ignore')

        elif database_name == "VISUAL_HISTORICAL_DATA":
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
    """Fastest bulk write: chunked delete+insert_many."""
    if not records:
        return
    CHUNK = 10000
    keys = [rec[key_field] for rec in records if rec.get(key_field) is not None]
    # Delete in chunks to avoid huge $in queries
    for i in range(0, len(keys), CHUNK):
        collection.delete_many({key_field: {"$in": keys[i:i+CHUNK]}})
    # Insert all at once
    for i in range(0, len(records), CHUNK):
        collection.insert_many(records[i:i+CHUNK], ordered=False)


def _upsert_scrap_records(collection, records, key_cols):
    """Fastest scrap bulk write: chunked delete+insert_many."""
    if not records or not key_cols:
        return
    CHUNK = 10000
    # Use only tire_barcode for fast delete if available
    if 'tire_barcode' in key_cols:
        barcodes = [rec.get('tire_barcode') for rec in records if rec.get('tire_barcode')]
        for i in range(0, len(barcodes), CHUNK):
            collection.delete_many({"tire_barcode": {"$in": barcodes[i:i+CHUNK]}})
    else:
        or_clauses = [{col: rec.get(col) for col in key_cols} for rec in records]
        for i in range(0, len(or_clauses), CHUNK):
            collection.delete_many({"$or": or_clauses[i:i+CHUNK]})
    for i in range(0, len(records), CHUNK):
        collection.insert_many(records[i:i+CHUNK], ordered=False)


def copy_to_single_collection_api(df_summary=None, df_scrap=None, df_tracking=None):
    """
    Writes cleaned data directly to the final merged DB.
    Accepts in-memory DataFrames — no intermediate DB reads needed.
    """
    output_messages = []
    logging.info("copy_to_single_collection_api function started")
    try:
        merged_db = client[TUB_CIMCORP_VISUAL_MERGED_DB_hisrory]

        # Ensure indexes for fast upsert lookups
        merged_db[DATA_MERGED_SUMMARY_COLLECTION_history].create_index([("barcode", 1)], background=True)
        merged_db[DATA_MERGED_SCRAP_COLLECTION_history].create_index([("tire_barcode", 1)], background=True)
        merged_db[DATA_MERGED_SCRAP_COLLECTION_history].create_index(
            [("tire_barcode", 1), ("gt_weighting_time", 1), ("sur_repair_time", 1), ("last_change_time", 1)],
            background=True
        )

        # ── 1. Summary (TUB/UFM) data ────────────────────────────────────────
        if df_summary is not None and not df_summary.empty:
            # Use the shift value directly from Redshift as-is — do not recompute

            # ── Compute 'last' and 'rerun' vectorised ──
            if 'uf_time' in df_summary.columns and 'barcode' in df_summary.columns:
                df_summary['_uf_time_sort'] = pd.to_datetime(df_summary['uf_time'], errors='coerce')
                df_summary['_rank_desc'] = df_summary.groupby('barcode')['_uf_time_sort'].rank(method='first', ascending=False).astype(int)
                df_summary['_rank_asc']  = df_summary.groupby('barcode')['_uf_time_sort'].rank(method='first', ascending=True).astype(int)
                df_summary['_count']     = df_summary.groupby('barcode')['barcode'].transform('count')
                df_summary['last'] = -1
                df_summary.loc[df_summary['_rank_desc'] == 1, 'last'] = 1
                df_summary.loc[(df_summary['_rank_asc'] == 1) & (df_summary['_count'] > 1), 'last'] = 0
                df_summary.loc[df_summary['_count'] == 1, 'last'] = 1
                df_summary['rerun'] = (df_summary['_rank_asc'] > 1).astype(int)
                df_summary.drop(columns=['_uf_time_sort', '_rank_desc', '_rank_asc', '_count'], inplace=True)

            # uf_yield vectorised
            _uf_cols = ['overalltugrade', 'overallbalgrade', 'overallsolgrade']
            _present = [c for c in _uf_cols if c in df_summary.columns]
            if _present:
                df_summary['uf_yield'] = (
                    df_summary[_present].apply(pd.to_numeric, errors='coerce').sum(axis=1) == 3
                ).astype(int)

            # sku_type vectorised
            try:
                qr_docs = list(client[config_databse][channel_no_collect].find({}, {"_id": 0, "sku": 1, "QR_Status": 1}))
                if qr_docs and 'sku' in df_summary.columns:
                    qr_map = {str(doc["sku"]).strip(): doc.get("QR_Status", "") for doc in qr_docs}
                    df_summary['sku_type'] = df_summary['sku'].astype(str).str.strip().map(qr_map).fillna("")
            except Exception as _qr_err:
                logging.warning(f"sku_type enrichment failed: {_qr_err}")

            # Normalise re_code vectorised
            if 're_code' in df_summary.columns:
                rc_series = df_summary['re_code'].astype(str).str.strip()
                empty_mask = rc_series.isin(['', 'None', 'nan', 'NaN', 'null'])
                df_summary.loc[empty_mask, 're_code'] = '0'
                df_summary.loc[empty_mask, 're_comment'] = None
            else:
                df_summary['re_code'] = '0'
                df_summary['re_comment'] = None

            # vi_yield vectorised
            _vi_codes = {'1100', '102', '2', '0'}
            df_summary['vi_yield'] = df_summary['re_code'].astype(str).str.strip().isin(_vi_codes).astype(int)

            records_summary = df_to_mongo_records(df_summary)
            # Exclude re_code = "99" / "999" records from data_merged_summary
            records_summary = [r for r in records_summary if str(r.get('re_code') or '').strip() not in ('99', '999')]
            _upsert_records(merged_db[DATA_MERGED_SUMMARY_COLLECTION_history], records_summary, 'barcode')
            output_messages.append(f"Upserted {len(records_summary)} records into {TUB_CIMCORP_VISUAL_MERGED_DB_hisrory}.{DATA_MERGED_SUMMARY_COLLECTION_history}")
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
            # Store ONLY re_code = "999" records in data_merged_scrap
            records_scrap = [r for r in records_scrap if str(r.get('re_code') or '').strip() == '999']
            # Keep only the latest record per tire_barcode (by sur_repair_time, then cp_curing_time)
            if records_scrap:
                def _parse_dt(val):
                    try:
                        return pd.to_datetime(str(val), errors='coerce')
                    except Exception:
                        return pd.NaT
                scrap_dedup_map = {}
                for rec in records_scrap:
                    bc = rec.get('tire_barcode')
                    if not bc:
                        continue
                    if bc not in scrap_dedup_map:
                        scrap_dedup_map[bc] = rec
                    else:
                        existing = scrap_dedup_map[bc]
                        # Compare sur_repair_time first, then cp_curing_time as tiebreaker
                        new_t1 = _parse_dt(rec.get('sur_repair_time'))
                        old_t1 = _parse_dt(existing.get('sur_repair_time'))
                        new_t2 = _parse_dt(rec.get('cp_curing_time'))
                        old_t2 = _parse_dt(existing.get('cp_curing_time'))
                        new_t1 = new_t1 if not pd.isna(new_t1) else pd.Timestamp.min
                        old_t1 = old_t1 if not pd.isna(old_t1) else pd.Timestamp.min
                        new_t2 = new_t2 if not pd.isna(new_t2) else pd.Timestamp.min
                        old_t2 = old_t2 if not pd.isna(old_t2) else pd.Timestamp.min
                        if (new_t1, new_t2) > (old_t1, old_t2):
                            scrap_dedup_map[bc] = rec
                records_scrap = list(scrap_dedup_map.values())
            # ── Enrich sku from gyo_qr_status when sku is empty and ft_code is present ──
            if records_scrap:
                try:
                    qr_docs = list(client[config_databse]["gyo_qr_status"].find(
                        {}, {"_id": 0, "sku": 1, "ft_code": 1}
                    ))
                    gv_sku_lookup = {str(d.get('sku') or '').strip(): str(d.get('sku') or '').strip()
                                     for d in qr_docs if d.get('sku')}
                    gf_ft_lookup  = {str(d.get('ft_code') or '').strip(): str(d.get('sku') or '').strip()
                                     for d in qr_docs if d.get('ft_code')}
                    for rec in records_scrap:
                        sku_val = str(rec.get('sku') or '').strip()
                        ft_val  = str(rec.get('ft_code') or '').strip()
                        if not sku_val and ft_val:
                            if ft_val.upper().startswith('GV'):
                                matched = gv_sku_lookup.get(ft_val)
                                if matched:
                                    rec['sku'] = matched
                            elif ft_val.upper().startswith('GF'):
                                matched = gf_ft_lookup.get(ft_val)
                                if matched:
                                    rec['sku'] = matched
                except Exception as _qr_err:
                    logging.warning(f"Scrap sku enrichment from gyo_qr_status failed: {_qr_err}")
            if records_scrap:
                _upsert_scrap_records(merged_db[DATA_MERGED_SCRAP_COLLECTION_history], records_scrap, _present_keys)
                output_messages.append(f"Upserted {len(records_scrap)} re_code=999 records into {TUB_CIMCORP_VISUAL_MERGED_DB_hisrory}.{DATA_MERGED_SCRAP_COLLECTION_history}")
            else:
                output_messages.append("No re_code=999 scrap records to store.")

            # ── 3. Enrich summary with RE_CODE/RE_COMMENT via barcode join ───
            if records_summary:
                barcode_col_scrap = next((c for c in ['tire_barcode', 'barcode'] if c in df_scrap.columns), None)
                if barcode_col_scrap:
                    scrap_cols = [c for c in ['re_code', 're_comment', 're_last_change_time',
                                              'RE_CODE', 'RE_COMMENT', 'RE_LAST_CHANGE_TIME'] if c in df_scrap.columns]
                    scrap_dedup = df_scrap[[barcode_col_scrap] + scrap_cols].drop_duplicates(
                        subset=[barcode_col_scrap], keep='last'
                    )
                    # Build lookup — exclude re_code=999 (scrap-only, must not enrich summary)
                    scrap_lookup = {}
                    for k, v in scrap_dedup.set_index(barcode_col_scrap)[scrap_cols].to_dict('index').items():
                        rc = str(v.get('re_code') or '').strip()
                        if rc in ('999', '99'):
                            continue  # re_code=999/99 stays in scrap only, not in summary
                        key_orig = str(k).strip()
                        key_stripped = key_orig.lstrip('0')
                        scrap_lookup[key_orig] = v
                        if key_stripped and key_stripped != key_orig:
                            scrap_lookup[key_stripped] = v

                    enriched = 0
                    unmatched_barcodes = []
                    # Build barcode→rec index for fast update
                    bc_to_recs = {}
                    for rec in records_summary:
                        bc = rec.get('barcode')
                        if bc:
                            bc_str = str(bc).strip()
                            bc_to_recs.setdefault(bc_str, []).append(rec)
                            bc_to_recs.setdefault(bc_str.lstrip('0'), []).append(rec)

                    for bc_key, val in scrap_lookup.items():
                        for rec in bc_to_recs.get(bc_key, []):
                            rec.update({k: _safe_val(v) for k, v in val.items()})
                            rc = str(rec.get('re_code') or '').strip()
                            if rc in ('', 'None', 'nan', 'NaN', 'null'):
                                rec['re_code'] = '0'
                                rec['re_comment'] = None
                            enriched += 1

                    matched_bcs = set(bc_to_recs.keys()) & set(scrap_lookup.keys())
                    for rec in records_summary:
                        bc = rec.get('barcode')
                        bc_str = str(bc).strip() if bc else None
                        if bc_str not in matched_bcs and bc_str.lstrip('0') not in matched_bcs:
                            unmatched_barcodes.append(rec)

                    # ── Fallback: batch lookup in data_merged_scrap for unmatched barcodes ──
                    if unmatched_barcodes:
                        scrap_coll = merged_db[DATA_MERGED_SCRAP_COLLECTION_history]
                        bc_variants = set()
                        for rec in unmatched_barcodes:
                            bc = rec.get('barcode')
                            if bc:
                                bc_str = str(bc).strip()
                                bc_variants.add(bc_str)
                                bc_variants.add(bc_str.lstrip('0'))
                        mongo_lookup = {}
                        for doc in scrap_coll.find(
                            {"tire_barcode": {"$in": list(bc_variants)}},
                            {"tire_barcode": 1, "re_code": 1, "re_comment": 1}
                        ):
                            tb = str(doc.get('tire_barcode', '')).strip()
                            rc = str(doc.get('re_code') or '').strip()
                            # re_code=999/99 stays in scrap only — do not enrich summary
                            if rc and rc not in ('', 'None', 'nan', 'NaN', 'null', '999', '99'):
                                mongo_lookup[tb] = doc
                                mongo_lookup[tb.lstrip('0')] = doc
                        for rec in unmatched_barcodes:
                            bc = rec.get('barcode')
                            bc_str = str(bc).strip() if bc else None
                            mongo_doc = mongo_lookup.get(bc_str) or mongo_lookup.get(bc_str.lstrip('0') if bc_str else None)
                            if mongo_doc:
                                rc = str(mongo_doc.get('re_code') or '').strip()
                                if rc not in ('', 'None', 'nan', 'NaN', 'null'):
                                    rec['re_code'] = rc
                                    rec['re_comment'] = mongo_doc.get('re_comment')
                                    enriched += 1
                                    continue
                            rec['re_code'] = '0'
                            rec['re_comment'] = None

                    if enriched:
                        # Re-stamp vi_yield vectorised on the updated records
                        _vi_codes = {'1100', '102', '2', '0'}
                        for rec in records_summary:
                            rec['vi_yield'] = 1 if str(rec.get('re_code') or '').strip() in _vi_codes else 0
                        _upsert_records(merged_db[DATA_MERGED_SUMMARY_COLLECTION_history], records_summary, 'barcode')
                        output_messages.append(f"Enriched {enriched} summary records with scrap/repair codes.")
        else:
            output_messages.append("No scrap data to write.")

        # ── 4. Tracking data (data_merged_tracking_historical_data) ──────────
        if df_tracking is not None and not df_tracking.empty:
            try:
                # Convert datetime and re_code columns to string
                _dt_cols = ['last_change_time', 'PTB_time', 'Cur_time', 'sur_repair_time',
                            'ptb_time', 'cur_time']  # cover both original and lowercased names
                for _col in _dt_cols:
                    if _col in df_tracking.columns:
                        df_tracking[_col] = df_tracking[_col].apply(
                            lambda x: None if x is pd.NaT or (isinstance(x, float) and pd.isna(x))
                            else str(x).strip()
                        )
                # Convert re_code to string — strip decimal (999.0 → "999")
                if 're_code' in df_tracking.columns:
                    df_tracking['re_code'] = df_tracking['re_code'].apply(
                        lambda x: None if x is pd.NaT or (isinstance(x, float) and pd.isna(x))
                        else str(int(float(x))) if str(x).replace('.', '', 1).isdigit()
                        else str(x).strip()
                    )
                records_tracking = df_to_mongo_records(df_tracking)
                # Deduplicate: keep latest per tire_barcode by sur_repair_time
                tracking_map = {}
                for rec in records_tracking:
                    bc = rec.get('tire_barcode')
                    if not bc:
                        continue
                    if bc not in tracking_map:
                        tracking_map[bc] = rec
                    else:
                        existing = tracking_map[bc]
                        try:
                            new_t = pd.to_datetime(str(rec.get('sur_repair_time') or ''), errors='coerce')
                            old_t = pd.to_datetime(str(existing.get('sur_repair_time') or ''), errors='coerce')
                            new_t = new_t if not pd.isna(new_t) else pd.Timestamp.min
                            old_t = old_t if not pd.isna(old_t) else pd.Timestamp.min
                            if new_t > old_t:
                                tracking_map[bc] = rec
                        except Exception:
                            pass
                records_tracking = list(tracking_map.values())

                # ── Enrich tracking records with condition_code, description,
                #    scrap_category, qr_status from data_merged_scrap_historical_data ──
                if records_tracking:
                    scrap_coll = merged_db[DATA_MERGED_SCRAP_COLLECTION_history]
                    # Collect all barcode variants (with and without leading zeros)
                    bc_variants = set()
                    for rec in records_tracking:
                        bc = rec.get('tire_barcode')
                        if bc:
                            bc_str = str(bc).strip()
                            bc_variants.add(bc_str)
                            bc_stripped = bc_str.lstrip('0')
                            if bc_stripped:
                                bc_variants.add(bc_stripped)
                    # Single batch query from scrap collection
                    scrap_enrich_lookup = {}
                    for doc in scrap_coll.find(
                        {"tire_barcode": {"$in": list(bc_variants)}},
                        {"tire_barcode": 1, "condition_code": 1, "description": 1,
                         "scrap_category": 1}
                    ):
                        tb = str(doc.get('tire_barcode', '')).strip()
                        scrap_enrich_lookup[tb] = doc
                        scrap_enrich_lookup[tb.lstrip('0')] = doc
                    # Apply enrichment to each tracking record
                    _enrich_cols = ['condition_code', 'description', 'scrap_category']
                    for rec in records_tracking:
                        bc = rec.get('tire_barcode')
                        bc_str = str(bc).strip() if bc else None
                        bc_stripped = bc_str.lstrip('0') if bc_str else None
                        scrap_doc = scrap_enrich_lookup.get(bc_str) or scrap_enrich_lookup.get(bc_stripped)
                        for col in _enrich_cols:
                            val = scrap_doc.get(col) if scrap_doc else None
                            # Convert empty string to None
                            rec[col] = val if val not in ('', None) else None

                if records_tracking:
                    # ── Enrich with QR_Status from config_databse.gyo_qr_status ──
                    try:
                        qr_docs = list(client[config_databse]["gyo_qr_status"].find(
                            {}, {"_id": 0, "sku": 1, "gt_code": 1, "QR_Status": 1}
                        ))
                        # Build lookup keyed by (sku, gt_code)
                        qr_lookup = {}
                        for doc in qr_docs:
                            sku_key = str(doc.get('sku') or '').strip()
                            gt_key  = str(doc.get('gt_code') or '').strip()
                            if sku_key and gt_key:
                                qr_lookup[(sku_key, gt_key)] = doc.get('QR_Status')
                        # Apply QR_Status to each tracking record
                        for rec in records_tracking:
                            sku_val = str(rec.get('sku') or '').strip()
                            gt_val  = str(rec.get('gt_code') or '').strip()
                            qr_val  = qr_lookup.get((sku_val, gt_val))
                            rec['qr_status'] = qr_val if qr_val not in ('', None) else None
                    except Exception as qr_err:
                        logging.warning(f"QR_Status enrichment for tracking failed: {qr_err}")
                        for rec in records_tracking:
                            rec.setdefault('qr_status', None)

                    tracking_coll = merged_db[DATA_MERGED_TRACKING_COLLECTION_history]
                    tracking_coll.create_index([("tire_barcode", 1)], background=True)
                    CHUNK = 10000
                    barcodes = [r.get('tire_barcode') for r in records_tracking if r.get('tire_barcode')]
                    for i in range(0, len(barcodes), CHUNK):
                        tracking_coll.delete_many({"tire_barcode": {"$in": barcodes[i:i+CHUNK]}})
                    for i in range(0, len(records_tracking), CHUNK):
                        tracking_coll.insert_many(records_tracking[i:i+CHUNK], ordered=False)
                    output_messages.append(f"Upserted {len(records_tracking)} records into {TUB_CIMCORP_VISUAL_MERGED_DB_hisrory}.{DATA_MERGED_TRACKING_COLLECTION_history}")
                else:
                    output_messages.append("No tracking records to insert after deduplication.")
            except Exception as te:
                output_messages.append(f"Error inserting tracking data: {te}")
                logging.error(f"Tracking insert error: {te}")
        else:
            output_messages.append("No tracking data to write.")

        # ── 5. TBM spot-position data (data_merged_tracking_tbm) ─────────────
        # Columns: barcode, sku, ptb_id, ptb_time, gt_code,
        #          vrl_vhmc_mes_br1_spot_position, vrl_vhmc_mes_br2_spot_position,
        #          vrl_vhmc_mes_cwap_spot_pos, vrl_vhmc_mes_td_spot_position,
        #          vrl_vhmc_mes_pa_spot_position, vrl_vhmc_mes_barcode_spot_pos,
        #          vrl_vhmc_mes_bp1_spot_pos, vrl_vhmc_mes_bp2_spot_pos
        # Upsert key: barcode + ptb_time
        if df_summary is not None and not df_summary.empty:
            try:
                _tbm_cols = [
                    'barcode', 'sku', 'ptb_id', 'ptb_time', 'gt_code',
                    'vrl_vhmc_mes_br1_spot_position', 'vrl_vhmc_mes_br2_spot_position',
                    'vrl_vhmc_mes_cwap_spot_pos', 'vrl_vhmc_mes_td_spot_position',
                    'vrl_vhmc_mes_pa_spot_position', 'vrl_vhmc_mes_barcode_spot_pos',
                    'vrl_vhmc_mes_bp1_spot_pos', 'vrl_vhmc_mes_bp2_spot_pos'
                ]
                # Keep only the columns that actually exist in the DataFrame
                _present_tbm_cols = [c for c in _tbm_cols if c in df_summary.columns]
                df_tbm = df_summary[_present_tbm_cols].copy()

                # Drop rows where both barcode and ptb_time are null
                df_tbm = df_tbm.dropna(subset=['barcode'], how='all')

                # Normalise ptb_time to string for consistent upsert key comparison
                if 'ptb_time' in df_tbm.columns:
                    df_tbm['ptb_time'] = df_tbm['ptb_time'].apply(
                        lambda x: None if x is pd.NaT or (isinstance(x, float) and pd.isna(x))
                        else str(x).strip()
                    )

                records_tbm = df_to_mongo_records(df_tbm)

                if records_tbm:
                    # ── Deduplicate: keep only the latest ptb_time per barcode ──
                    tbm_dedup_map = {}
                    for rec in records_tbm:
                        bc = rec.get('barcode')
                        if not bc:
                            continue
                        if bc not in tbm_dedup_map:
                            tbm_dedup_map[bc] = rec
                        else:
                            existing = tbm_dedup_map[bc]
                            try:
                                new_t = pd.to_datetime(str(rec.get('ptb_time') or ''), errors='coerce')
                                old_t = pd.to_datetime(str(existing.get('ptb_time') or ''), errors='coerce')
                                new_t = new_t if not pd.isna(new_t) else pd.Timestamp.min
                                old_t = old_t if not pd.isna(old_t) else pd.Timestamp.min
                                if new_t > old_t:
                                    tbm_dedup_map[bc] = rec
                            except Exception:
                                pass
                    records_tbm = list(tbm_dedup_map.values())

                    tbm_coll = merged_db[DATA_MERGED_TRACKING_TBM_COLLECTION_history]
                    # Index on barcode only — one document per barcode
                    tbm_coll.create_index([("barcode", 1)], background=True)

                    # Upsert: update-or-insert based on barcode alone
                    from pymongo import UpdateOne
                    bulk_ops = []
                    for rec in records_tbm:
                        bc = rec.get('barcode')
                        if not bc:
                            continue
                        bulk_ops.append(
                            UpdateOne({"barcode": bc}, {"$set": rec}, upsert=True)
                        )
                    if bulk_ops:
                        CHUNK = 10000
                        for i in range(0, len(bulk_ops), CHUNK):
                            tbm_coll.bulk_write(bulk_ops[i:i + CHUNK], ordered=False)
                        output_messages.append(
                            f"Upserted {len(bulk_ops)} records into "
                            f"{TUB_CIMCORP_VISUAL_MERGED_DB_hisrory}.{DATA_MERGED_TRACKING_TBM_COLLECTION_history}"
                        )
                    else:
                        output_messages.append("No valid TBM spot-position records to upsert (missing barcode).")
                else:
                    output_messages.append("No TBM spot-position records found in summary data.")
            except Exception as tbm_err:
                output_messages.append(f"Error inserting data_merged_tracking_tbm data: {tbm_err}")
                logging.error(f"data_merged_tracking_tbm insert error: {tbm_err}")
        else:
            output_messages.append("No summary data available for data_merged_tracking_tbm.")

    except Exception as e:
        error_msg = f"Error in copy_to_single_collection_api: {e}"
        output_messages.append(error_msg)
        logging.error(error_msg)

    logging.info("copy_to_single_collection_api function completed")
    return output_messages

def run_data_cleaning(df_tub_input, df_cimcorp_input, df_tracking_input=None):
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
        # Proceed with TUB data even if scrap/visual data is empty or None
        if df_cimcorp_input is None or not isinstance(df_cimcorp_input, pd.DataFrame) or df_cimcorp_input.empty:
            logging.info("Scrap/repair data is empty or None — proceeding with TUB data only.")
            cleaned_scrap = pd.DataFrame()
        else:
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
        for _col in ['re_last_change_time', 'sur_repair_time', 'gt_weighting_time', 'last_change_time', 'cp_curing_time']:
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
                _uf_keywords = {"force", "runout", "rfv-grinding", "bulge/dep"}
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

        # Write directly to final merged DB
        copy_msgs = copy_to_single_collection_api(
            df_summary=cleaned_summary,
            df_scrap=cleaned_scrap,
            df_tracking=df_tracking_input
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
                "target_collection": DATA_MERGED_SUMMARY_COLLECTION_history,
                "database_name": TUB_CIMCORP_VISUAL_MERGED_DB_hisrory,
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
                "target_collection": DATA_MERGED_SCRAP_COLLECTION_history,
                "database_name": TUB_CIMCORP_VISUAL_MERGED_DB_hisrory,
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
        result = run_data_cleaning(df_tub, df_cimcorp, df_tracking_input=df_tracking)
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
