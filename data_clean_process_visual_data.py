from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
import pandas as pd
import re
from datetime import datetime, timezone, time as datetime_time
from datetime import datetime, date
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
from collections import defaultdict

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
from config import (
    client,
    FILE_TRACKING_DB,
    FILE_TRACKING_COLLECTION,
    CIMCORP_DB_visual,
    CIMCORP_CLEANED_PREFIX_visual,
    CIMCORP_INVALID_PREFIX,
    INVALID_ROWS_DB,
    visual_data,
    inspection_data,
    All_SLOBS_DATA,
    active_sku_list
)

app = Flask(__name__)
app.secret_key = "supersecretkey"

# ── Logging ───────────────────────────────────────────────────────────────────

def configure_logging():
    if not os.path.exists('logs'):
        os.makedirs('logs')
    current_date = datetime.now().strftime('%Y-%m-%d')
    log_filename = f'logs/log_visual_data_{current_date}.txt'
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.FileHandler(log_filename, mode='a', encoding='utf-8')]
    )
    warnings.simplefilter("always")
    logging.captureWarnings(True)

# ── Helpers ───────────────────────────────────────────────────────────────────

db_tracking      = client[FILE_TRACKING_DB]
tracking_collection = db_tracking[FILE_TRACKING_COLLECTION]
file_tracking_db = client['file_tracking']
cimcorp_db       = client['CIMCORP_VISUAL']

def some_function():
    return datetime.now(pytz.timezone('Asia/Kolkata'))

DEFAULT_DATETIME = datetime(1900, 1, 1, 12, 30, 0)
DEFAULT_DATE     = datetime(1900, 1, 1)

def insert_with_timestamp(df, collection_name_prefix, db_name):
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Expected a pandas DataFrame as input")
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            try:
                df[col] = df[col].dt.tz_localize(None)
            except TypeError:
                df[col] = df[col].dt.tz_convert(None)
    for col in df.select_dtypes(include=['object']).columns:
        if any(isinstance(x, date) and not isinstance(x, datetime) for x in df[col] if x is not None):
            df[col] = df[col].apply(
                lambda x: x.strftime('%Y-%m-%d') if isinstance(x, date) and not isinstance(x, datetime) and x is not None else x
            )
    timestamp = some_function().strftime("%Y%m%d_%H%M%S")
    if db_name == 'CIMCORP_VISUAL':
        db = cimcorp_db
    else:
        raise ValueError("Invalid database name")
    collection_name = f"{collection_name_prefix}_{timestamp}"

    def sanitize(val):
        if val is pd.NaT or (isinstance(val, float) and np.isnan(val)):
            return None
        if isinstance(val, datetime) and val.tzinfo is not None:
            return val.replace(tzinfo=None)
        return val

    records = [{k: sanitize(v) for k, v in row.items()} for row in df.to_dict('records')]
    db[collection_name].insert_many(records)
    track_file_in_file_tracking(collection_name_prefix, timestamp, len(df), db_name)

def track_file_in_file_tracking(file_prefix, timestamp, num_rows, db_name):
    file_tracking_db['file_metadata'].insert_one({
        "filename":            f"{file_prefix}_{timestamp}",
        "inserted_timestamp":  some_function(),
        "num_rows":            num_rows,
        "database_name":       db_name,
        "processed":           False
    })

# ── Redshift ──────────────────────────────────────────────────────────────────

class RedshiftDataConnectorr:
    def __init__(self):
        self.conn = redshift_connector.connect(
            host='atl-dl-prd-01.cuvxusm4qify.ap-south-1.redshift.amazonaws.com',
            database='db_prd_plant_che',
            port=5439,
            user='usr_thangadurai_a',
            password='QtHJO%1^m^dQEVFc',
            sslmode='disable'
        )
    def execute_query(self, q):
        return pd.read_sql(q, self.conn)

inst     = RedshiftDataConnectorr()

########### working updated code start 02-Jun-2026 START ######################
from datetime import datetime, timedelta
import pytz

# ---------------------------------------------------------
# Timezone
# ---------------------------------------------------------
local_tz = pytz.timezone("Asia/Kolkata")
now = datetime.now(local_tz)

print("Current IST Time:", now.strftime("%Y-%m-%d %H:%M:%S"))

# ---------------------------------------------------------
# Run every hour at 26th and 56th minute
# Example for 26th minute:
# 11:26 -> 09:00 to 11:00
# 12:26 -> 10:00 to 12:00
# 13:26 -> 11:00 to 13:00
#
# Example for 56th minute:
# 11:56 -> 09:00 to 11:00
# 12:56 -> 10:00 to 12:00
# 13:56 -> 11:00 to 13:00
# ---------------------------------------------------------
if now.minute in [26, 56]:  # <-- Updated condition

    # Current hour rounded to HH:00:00
    end_dt = now.replace(
        minute=0,
        second=0,
        microsecond=0
    )

    # Previous 8 hours
    start_dt = end_dt - timedelta(hours=8)

    # Format values
    start_date = start_dt.strftime("%Y-%m-%d")
    end_date   = end_dt.strftime("%Y-%m-%d")

    start_time = start_dt.strftime("%H:%M:%S")
    end_time   = end_dt.strftime("%H:%M:%S")

    print("\nExecution Window:")
    print(f"From : {start_date} {start_time}")
    print(f"To   : {end_date} {end_time}")
    
########### working updated code start 02-Jun-2026 END ######################

       #--------------------# 
       
# from datetime import datetime, timedelta
# import pytz

# # ---------------------------------------------------------
# # Timezone
# # ---------------------------------------------------------
# local_tz = pytz.timezone("Asia/Kolkata")
# now = datetime.now(local_tz)

# print("Current IST Time:", now.strftime("%Y-%m-%d %H:%M:%S"))

# # ---------------------------------------------------------
# # Run every hour at 17th minute
# # Example:
# # 11:17 -> 09:00 to 11:00
# # 12:17 -> 10:00 to 12:00
# # 13:17 -> 11:00 to 13:00
# # ---------------------------------------------------------
# if now.minute == 25:

#     # Current hour rounded to HH:00:00
#     end_dt = now.replace(
#         minute=0,
#         second=0,
#         microsecond=0
#     )

#     # Previous 2 hours
#     start_dt = end_dt - timedelta(hours=2)

#     # Format values
#     start_date = start_dt.strftime("%Y-%m-%d")
#     end_date   = end_dt.strftime("%Y-%m-%d")

#     start_time = start_dt.strftime("%H:%M:%S")
#     end_time   = end_dt.strftime("%H:%M:%S")

#     print("\nExecution Window:")
#     print(f"From : {start_date} {start_time}")
#     print(f"To   : {end_date} {end_time}")

       #--------------------#


########### Manual Trigger Start ################################
# local_tz = pytz.timezone("Asia/Kolkata")

# # ------------------ INPUT ------------------
# manual_start_date = '2026-06-30'
# manual_end_date   = '2026-07-16'
# manual_start_time = '07:00:00'
# manual_end_time   = '14:00:00'
# # -------------------------------------------

# if manual_start_date and manual_end_date and manual_start_time and manual_end_time:
#     start_dt = local_tz.localize(datetime.strptime(f"{manual_start_date} {manual_start_time}", "%Y-%m-%d %H:%M:%S"))
#     end_dt   = local_tz.localize(datetime.strptime(f"{manual_end_date} {manual_end_time}", "%Y-%m-%d %H:%M:%S"))
# else:
#     now      = datetime.now(pytz.utc).astimezone(local_tz)
#     prev_hour = now - timedelta(hours=1)
#     start_dt = prev_hour.replace(minute=0, second=0, microsecond=0)
#     end_dt   = start_dt + timedelta(hours=1)

# start_date = start_dt.strftime("%Y-%m-%d")
# end_date   = end_dt.strftime("%Y-%m-%d")
# start_time = start_dt.strftime("%H:%M:%S")
# end_time   = end_dt.strftime("%H:%M:%S")

# print(start_date, start_time)
# print(end_date, end_time)

############ Manual Trigger End ################################

q1 = f"""
SELECT
    t.barcode,
    CONVERT_TIMEZONE('UTC','Asia/Kolkata', tbm.timestamp) AS timestamp_x, 
    tbm.deviceid AS deviceid_x,
    tbm.gttypeid,
    curing.machinenumber,
    curing.mouldid,
    CONVERT_TIMEZONE('UTC','Asia/Kolkata', curing.timestamp) AS timestamp_y,
    repair.operation,
    CONVERT_TIMEZONE('UTC','Asia/Kolkata', repair.timestamp) AS timestamp_z,
    CONVERT_TIMEZONE('UTC','Asia/Kolkata', visualinspection.timestamp) AS timestamp_z1
FROM sc_cimcorp_pcr.tbm tbm
LEFT JOIN sc_cimcorp_pcr.transportationunit t
    ON tbm.transportationunitid = t.transportationunitid
LEFT JOIN sc_cimcorp_pcr.curing curing
    ON tbm.transportationunitid = curing.transportationunitid
LEFT JOIN sc_cimcorp_pcr.repair repair
    ON tbm.transportationunitid = repair.transportationunitid
LEFT JOIN sc_cimcorp_pcr.visualinspection visualinspection
    ON tbm.transportationunitid = visualinspection.transportationunitid
WHERE 
    CONVERT_TIMEZONE('UTC','Asia/Kolkata', tbm."timestamp") > '{start_date} {start_time}'
AND CONVERT_TIMEZONE('UTC','Asia/Kolkata', tbm."timestamp") < '{end_date} {end_time}'
AND repair.operation IS NOT NULL;
"""
print("#########################")

df_cimcorp = inst.execute_query(q1)



# ── Step 1: Clean data ────────────────────────────────────────────────────────

def clean_cimcorp_data(df):
    output_messages = []
    invalid_barcode_rows = pd.DataFrame()
    duplicates_df = pd.DataFrame()
    logging.info("clean_cimcorp_data function is just started")
    try:
        df = df.map(str)
        df.columns = [re.sub(r'[.\s]+', '_', col.strip()).lower() for col in df.columns]
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
        df = df.replace({"nan": "None", "NaT": "None", "none": "None", "<NA>": "None", "": "None"})
        for col in df.select_dtypes(include=['object']):
            df[col] = df[col].astype(str).str.strip()
        rename_columns_mapping = {
            'deviceid_x':   'cimcorp_tbm_tub',
            'timestamp_x':  'tbm_buildtime_tub',
            'mouldid':      'curing_mouldid_tub',
            'machinenumber':'curing_press_tub',
            'timestamp_y':  'curing_infeedtime_tub',
            'gttypeid':     'tbm_gtcode_tub',
            'timestamp_z':  'operation_time_tub',
            'timestamp_z1': 'visualinspection_time_tub',
            'operation': 'operation_tub'
        }
        for old_name, new_name in rename_columns_mapping.items():
            if old_name in df.columns:
                df = df.rename(columns={old_name: new_name}, inplace=False)
        # for ts_col in ['tbm_buildtime_tub', 'curing_infeedtime_tub', 'operation_time_tub', 'visualinspection_time_tub']:
        #     if ts_col in df.columns:
        #         parsed = pd.to_datetime(df[ts_col], errors='coerce', utc=True)
        #         df[ts_col] = (parsed + pd.Timedelta(hours=5, minutes=30)).dt.strftime("%Y-%m-%d %H:%M:%S")
        #         df[ts_col] = df[ts_col].fillna("None")
        if 'cimcorp_tbm_tub' in df.columns:
            df['cimcorp_tbm_tub'] = 'TBM' + df['cimcorp_tbm_tub'].astype(str)
        if 'curing_press_tub' in df.columns:
            df['curing_press_tub'] = 'M' + df['curing_press_tub'].astype(str)
            df['curing_press_tub'] = df['curing_press_tub'].replace({"MNone": "None", "Mnone": "None"})
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
            df['barcode'] = (df['barcode'].astype(str)
                             .apply(lambda x: x[:-2] if x.endswith('.0') else x)
                             .apply(lambda x: x.lstrip('0')))
        output_messages.append("CIMCORP Cleaning successful")
        logging.info("clean_cimcorp_data function is completed")
        return df, invalid_barcode_rows, duplicates_df, output_messages
    except Exception as e:
        output_messages.append(f"Error during cleaning: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), output_messages

# ── Step 2: Insert into visual_data.inspection_data ──────────────────────────

def fetch_clean_and_insert_visual_data():
    print("fetch_clean_and_insert_visual_data started")
    logging.info("fetch_clean_and_insert_visual_data started")
    try:
        if df_cimcorp is None or df_cimcorp.empty:
            print("No data fetched from Redshift.")
            logging.info("No data fetched from Redshift.")
            return

        print(f"df_cimcorp shape: {df_cimcorp.shape}")
        cleaned_df, invalid_rows, duplicates_df, messages = clean_cimcorp_data(df_cimcorp.copy())
        for msg in messages:
            print(msg)
            logging.info(msg)

        print(f"cleaned_df shape: {cleaned_df.shape}")
        if cleaned_df.empty:
            print("No cleaned data to insert into visual_data.inspection_data")
            logging.info("No cleaned data to insert into visual_data.inspection_data")
            return

        inspection_collection = client["visual_data"]["inspection_data"]

        # ── Duplicate check: skip records where barcode + tbm_buildtime_tub already exists ──
        # Fetch all existing (barcode, tbm_buildtime_tub) pairs in ONE query — handles 1 lakh+ records fast
        incoming_barcodes = cleaned_df["barcode"].dropna().unique().tolist()
        existing_docs = inspection_collection.find(
            {"barcode": {"$in": incoming_barcodes}},
            {"_id": 0, "barcode": 1, "tbm_buildtime_tub": 1}
        )
        existing_keys = set(
            (doc["barcode"], doc.get("tbm_buildtime_tub"))
            for doc in existing_docs
        )

        records_all = cleaned_df.to_dict(orient="records")
        records = []
        skipped = 0
        for rec in records_all:
            key = (rec.get("barcode"), rec.get("tbm_buildtime_tub"))
            if key in existing_keys:
                skipped += 1
                # logging.info(f"Skipped duplicate barcode={key[0]}, tbm_buildtime_tub={key[1]}")
            else:
                records.append(rec)
        print(f"Duplicate records skipped: {skipped}")
        logging.info(f"Duplicate records skipped: {skipped}")
        # ── End duplicate check ──────────────────────────────────────────────────────────────

        if records:
            inspection_collection.insert_many(records, ordered=False)
            print(f"Inserted {len(records)} records into visual_data.inspection_data")
            logging.info(f"Inserted {len(records)} records into visual_data.inspection_data")

    except Exception as e:
        print(f"fetch_clean_and_insert_visual_data failed: {e}")
        logging.error(f"fetch_clean_and_insert_visual_data failed: {e}")
        logging.error(traceback.format_exc())

# ── Step 3: Build nested and insert into All_SLOBS_DATA.active_sku_list ───────

def fix_curing_press(val):
    return "None" if val in ("MNone", "Mnone", "mnone") else val

def build_nested(records):
    groups = defaultdict(list)
    for r in records:
        date_str = r.get("tbm_buildtime_tub", "None")[:10] if r.get("tbm_buildtime_tub", "None") != "None" else "None"
        gt_code  = r.get("tbm_gtcode_tub", "None")
        groups[(date_str, gt_code)].append(r)
    return [
        {
            "tbm_buildtime_tub": date_str,
            "tbm_gtcode_tub": gt_code,
            "barcodes": [{"index": i, **r} for i, r in enumerate(recs)]
        }
        for (date_str, gt_code), recs in groups.items()
    ]

def build_and_insert_slobs_data():
    logging.info("build_and_insert_slobs_data started")
    try:
        raw = list(client[visual_data][inspection_data].find({}, {"_id": 0}))
        logging.info(f"Fetched {len(raw)} records from visual_data.inspection_data")

        for doc in raw:
            doc["curing_press_tub"] = fix_curing_press(doc.get("curing_press_tub", "None"))

        nested = build_nested(raw)
        logging.info(f"Built {len(nested)} grouped documents")

        col = client[All_SLOBS_DATA][active_sku_list]
        col.delete_many({})
        if nested:
            col.insert_many(nested, ordered=False)

        print(f"Inserted {len(nested)} documents into All_SLOBS_DATA.active_sku_list")
        logging.info(f"Inserted {len(nested)} documents into All_SLOBS_DATA.active_sku_list")

    except Exception as e:
        print(f"build_and_insert_slobs_data failed: {e}")
        logging.error(f"build_and_insert_slobs_data failed: {e}")
        logging.error(traceback.format_exc())

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    try:
        configure_logging()
        warnings.filterwarnings("ignore")
        logging.info("Starting visual_data pipeline...")

        # Step 1: Store raw data into CIMCORP_VISUAL
        insert_with_timestamp(df_cimcorp, CIMCORP_CLEANED_PREFIX_visual, "CIMCORP_VISUAL")
        logging.info("Raw data inserted into CIMCORP_VISUAL.")

        # Step 2: Clean and insert into visual_data.inspection_data
        fetch_clean_and_insert_visual_data()

        # Step 3: Build nested structure and insert into All_SLOBS_DATA.active_sku_list
        build_and_insert_slobs_data()

        logging.info("visual_data pipeline completed.")

    except Exception as e:
        logging.error(f"Error in main: {str(e)}")
        logging.error(traceback.format_exc())
        print(f"Error in main: {str(e)}")

if __name__ == "__main__":
    main()


# from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
# import pandas as pd
# import re
# from datetime import datetime, timezone, time as datetime_time
# from datetime import datetime, date
# import traceback
# import os
# import numpy as np
# import time
# import pytz
# import pymongo
# from pymongo import MongoClient
# from datetime import datetime, timedelta
# from pytz import timezone
# import logging
# from datetime import datetime
# from passlib.context import CryptContext
# import redshift_connector
# import csv
# import warnings
# import bcrypt
# from collections import defaultdict

# pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
# from config import (
#     client,
#     FILE_TRACKING_DB,
#     FILE_TRACKING_COLLECTION,
#     CIMCORP_DB_visual,
#     CIMCORP_CLEANED_PREFIX_visual,
#     CIMCORP_INVALID_PREFIX,
#     INVALID_ROWS_DB,
#     visual_data,
#     inspection_data,
#     All_SLOBS_DATA,
#     active_sku_list
# )

# app = Flask(__name__)
# app.secret_key = "supersecretkey"

# # ── Logging ───────────────────────────────────────────────────────────────────

# def configure_logging():
#     if not os.path.exists('logs'):
#         os.makedirs('logs')
#     current_date = datetime.now().strftime('%Y-%m-%d')
#     log_filename = f'logs/log_visual_data_{current_date}.txt'
#     for handler in logging.root.handlers[:]:
#         logging.root.removeHandler(handler)
#     logging.basicConfig(
#         level=logging.INFO,
#         format='%(asctime)s - %(levelname)s - %(message)s',
#         handlers=[logging.FileHandler(log_filename, mode='a', encoding='utf-8')]
#     )
#     warnings.simplefilter("always")
#     logging.captureWarnings(True)

# # ── Helpers ───────────────────────────────────────────────────────────────────

# db_tracking      = client[FILE_TRACKING_DB]
# tracking_collection = db_tracking[FILE_TRACKING_COLLECTION]
# file_tracking_db = client['file_tracking']
# cimcorp_db       = client['CIMCORP_VISUAL']

# def some_function():
#     return datetime.now(pytz.timezone('Asia/Kolkata'))

# DEFAULT_DATETIME = datetime(1900, 1, 1, 12, 30, 0)
# DEFAULT_DATE     = datetime(1900, 1, 1)

# def insert_with_timestamp(df, collection_name_prefix, db_name):
#     if not isinstance(df, pd.DataFrame):
#         raise TypeError("Expected a pandas DataFrame as input")
#     df = df.copy()
#     for col in df.columns:
#         if pd.api.types.is_datetime64_any_dtype(df[col]):
#             try:
#                 df[col] = df[col].dt.tz_localize(None)
#             except TypeError:
#                 df[col] = df[col].dt.tz_convert(None)
#     for col in df.select_dtypes(include=['object']).columns:
#         if any(isinstance(x, date) and not isinstance(x, datetime) for x in df[col] if x is not None):
#             df[col] = df[col].apply(
#                 lambda x: x.strftime('%Y-%m-%d') if isinstance(x, date) and not isinstance(x, datetime) and x is not None else x
#             )
#     timestamp = some_function().strftime("%Y%m%d_%H%M%S")
#     if db_name == 'CIMCORP_VISUAL':
#         db = cimcorp_db
#     else:
#         raise ValueError("Invalid database name")
#     collection_name = f"{collection_name_prefix}_{timestamp}"

#     def sanitize(val):
#         if val is pd.NaT or (isinstance(val, float) and np.isnan(val)):
#             return None
#         if isinstance(val, datetime) and val.tzinfo is not None:
#             return val.replace(tzinfo=None)
#         return val

#     records = [{k: sanitize(v) for k, v in row.items()} for row in df.to_dict('records')]
#     db[collection_name].insert_many(records)
#     track_file_in_file_tracking(collection_name_prefix, timestamp, len(df), db_name)

# def track_file_in_file_tracking(file_prefix, timestamp, num_rows, db_name):
#     file_tracking_db['file_metadata'].insert_one({
#         "filename":            f"{file_prefix}_{timestamp}",
#         "inserted_timestamp":  some_function(),
#         "num_rows":            num_rows,
#         "database_name":       db_name,
#         "processed":           False
#     })

# # ── Redshift ──────────────────────────────────────────────────────────────────

# class RedshiftDataConnectorr:
#     def __init__(self):
#         self.conn = redshift_connector.connect(
#             host='atl-dl-prd-01.cuvxusm4qify.ap-south-1.redshift.amazonaws.com',
#             database='db_prd_plant_che',
#             port=5439,
#             user='usr_thangadurai_a',
#             password='QtHJO%1^m^dQEVFc',
#             sslmode='disable'
#         )
#     def execute_query(self, q):
#         return pd.read_sql(q, self.conn)

# inst     = RedshiftDataConnectorr()

# ########### working updated code start 02-Jun-2026 START ######################
# from datetime import datetime, timedelta
# import pytz

# # ---------------------------------------------------------
# # Timezone
# # ---------------------------------------------------------
# local_tz = pytz.timezone("Asia/Kolkata")
# now = datetime.now(local_tz)

# print("Current IST Time:", now.strftime("%Y-%m-%d %H:%M:%S"))

# # ---------------------------------------------------------
# # Run every hour at 26th and 56th minute
# # Example for 26th minute:
# # 11:26 -> 09:00 to 11:00
# # 12:26 -> 10:00 to 12:00
# # 13:26 -> 11:00 to 13:00
# #
# # Example for 56th minute:
# # 11:56 -> 09:00 to 11:00
# # 12:56 -> 10:00 to 12:00
# # 13:56 -> 11:00 to 13:00
# # ---------------------------------------------------------
# if now.minute in [26, 56]:  # <-- Updated condition

#     # Current hour rounded to HH:00:00
#     end_dt = now.replace(
#         minute=0,
#         second=0,
#         microsecond=0
#     )

#     # Previous 8 hours
#     start_dt = end_dt - timedelta(hours=8)

#     # Format values
#     start_date = start_dt.strftime("%Y-%m-%d")
#     end_date   = end_dt.strftime("%Y-%m-%d")

#     start_time = start_dt.strftime("%H:%M:%S")
#     end_time   = end_dt.strftime("%H:%M:%S")

#     print("\nExecution Window:")
#     print(f"From : {start_date} {start_time}")
#     print(f"To   : {end_date} {end_time}")
    
# ########### working updated code start 02-Jun-2026 END ######################

#        #--------------------# 
       
# # from datetime import datetime, timedelta
# # import pytz

# # # ---------------------------------------------------------
# # # Timezone
# # # ---------------------------------------------------------
# # local_tz = pytz.timezone("Asia/Kolkata")
# # now = datetime.now(local_tz)

# # print("Current IST Time:", now.strftime("%Y-%m-%d %H:%M:%S"))

# # # ---------------------------------------------------------
# # # Run every hour at 17th minute
# # # Example:
# # # 11:17 -> 09:00 to 11:00
# # # 12:17 -> 10:00 to 12:00
# # # 13:17 -> 11:00 to 13:00
# # # ---------------------------------------------------------
# # if now.minute == 25:

# #     # Current hour rounded to HH:00:00
# #     end_dt = now.replace(
# #         minute=0,
# #         second=0,
# #         microsecond=0
# #     )

# #     # Previous 2 hours
# #     start_dt = end_dt - timedelta(hours=2)

# #     # Format values
# #     start_date = start_dt.strftime("%Y-%m-%d")
# #     end_date   = end_dt.strftime("%Y-%m-%d")

# #     start_time = start_dt.strftime("%H:%M:%S")
# #     end_time   = end_dt.strftime("%H:%M:%S")

# #     print("\nExecution Window:")
# #     print(f"From : {start_date} {start_time}")
# #     print(f"To   : {end_date} {end_time}")

#        #--------------------#


# ########### Manual Trigger Start ################################
# # local_tz = pytz.timezone("Asia/Kolkata")

# # # ------------------ INPUT ------------------
# # manual_start_date = '2026-05-01'
# # manual_end_date   = '2026-05-10'
# # manual_start_time = '07:00:00'
# # manual_end_time   = '07:00:00'
# # # -------------------------------------------

# # if manual_start_date and manual_end_date and manual_start_time and manual_end_time:
# #     start_dt = local_tz.localize(datetime.strptime(f"{manual_start_date} {manual_start_time}", "%Y-%m-%d %H:%M:%S"))
# #     end_dt   = local_tz.localize(datetime.strptime(f"{manual_end_date} {manual_end_time}", "%Y-%m-%d %H:%M:%S"))
# # else:
# #     now      = datetime.now(pytz.utc).astimezone(local_tz)
# #     prev_hour = now - timedelta(hours=1)
# #     start_dt = prev_hour.replace(minute=0, second=0, microsecond=0)
# #     end_dt   = start_dt + timedelta(hours=1)

# # start_date = start_dt.strftime("%Y-%m-%d")
# # end_date   = end_dt.strftime("%Y-%m-%d")
# # start_time = start_dt.strftime("%H:%M:%S")
# # end_time   = end_dt.strftime("%H:%M:%S")

# # print(start_date, start_time)
# # print(end_date, end_time)

# ############ Manual Trigger End ################################

# q1 = f"""
# SELECT
#     t.barcode,
#     CONVERT_TIMEZONE('UTC','Asia/Kolkata', tbm.timestamp) AS timestamp_x, 
#     tbm.deviceid AS deviceid_x,
#     tbm.gttypeid,
#     curing.machinenumber,
#     curing.mouldid,
#     CONVERT_TIMEZONE('UTC','Asia/Kolkata', curing.timestamp) AS timestamp_y,
#     repair.operation,
#     CONVERT_TIMEZONE('UTC','Asia/Kolkata', repair.timestamp) AS timestamp_z,
#     CONVERT_TIMEZONE('UTC','Asia/Kolkata', visualinspection.timestamp) AS timestamp_z1
# FROM sc_cimcorp_pcr.tbm tbm
# LEFT JOIN sc_cimcorp_pcr.transportationunit t
#     ON tbm.transportationunitid = t.transportationunitid
# LEFT JOIN sc_cimcorp_pcr.curing curing
#     ON tbm.transportationunitid = curing.transportationunitid
# LEFT JOIN sc_cimcorp_pcr.repair repair
#     ON tbm.transportationunitid = repair.transportationunitid
# LEFT JOIN sc_cimcorp_pcr.visualinspection visualinspection
#     ON tbm.transportationunitid = visualinspection.transportationunitid
# WHERE 
#     CONVERT_TIMEZONE('UTC','Asia/Kolkata', tbm."timestamp") > '{start_date} {start_time}'
# AND CONVERT_TIMEZONE('UTC','Asia/Kolkata', tbm."timestamp") < '{end_date} {end_time}'
# AND repair.operation IS NOT NULL;
# """
# print("#########################")

# df_cimcorp = inst.execute_query(q1)



# # ── Step 1: Clean data ────────────────────────────────────────────────────────

# def clean_cimcorp_data(df):
#     output_messages = []
#     invalid_barcode_rows = pd.DataFrame()
#     duplicates_df = pd.DataFrame()
#     logging.info("clean_cimcorp_data function is just started")
#     try:
#         df = df.map(str)
#         df.columns = [re.sub(r'[.\s]+', '_', col.strip()).lower() for col in df.columns]
#         df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
#         df = df.replace({"nan": "None", "NaT": "None", "none": "None", "<NA>": "None", "": "None"})
#         for col in df.select_dtypes(include=['object']):
#             df[col] = df[col].astype(str).str.strip()
#         rename_columns_mapping = {
#             'deviceid_x':   'cimcorp_tbm_tub',
#             'timestamp_x':  'tbm_buildtime_tub',
#             'mouldid':      'curing_mouldid_tub',
#             'machinenumber':'curing_press_tub',
#             'timestamp_y':  'curing_infeedtime_tub',
#             'gttypeid':     'tbm_gtcode_tub',
#             'timestamp_z':  'operation_time_tub',
#             'timestamp_z1': 'visualinspection_time_tub',
#             'operation': 'operation_tub'
#         }
#         for old_name, new_name in rename_columns_mapping.items():
#             if old_name in df.columns:
#                 df = df.rename(columns={old_name: new_name}, inplace=False)
#         # for ts_col in ['tbm_buildtime_tub', 'curing_infeedtime_tub', 'operation_time_tub', 'visualinspection_time_tub']:
#         #     if ts_col in df.columns:
#         #         parsed = pd.to_datetime(df[ts_col], errors='coerce', utc=True)
#         #         df[ts_col] = (parsed + pd.Timedelta(hours=5, minutes=30)).dt.strftime("%Y-%m-%d %H:%M:%S")
#         #         df[ts_col] = df[ts_col].fillna("None")
#         if 'cimcorp_tbm_tub' in df.columns:
#             df['cimcorp_tbm_tub'] = 'TBM' + df['cimcorp_tbm_tub'].astype(str)
#         if 'curing_press_tub' in df.columns:
#             df['curing_press_tub'] = 'M' + df['curing_press_tub'].astype(str)
#             df['curing_press_tub'] = df['curing_press_tub'].replace({"MNone": "None", "Mnone": "None"})
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
#         output_messages.append("CIMCORP Cleaning successful")
#         logging.info("clean_cimcorp_data function is completed")
#         return df, invalid_barcode_rows, duplicates_df, output_messages
#     except Exception as e:
#         output_messages.append(f"Error during cleaning: {e}")
#         return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), output_messages

# # ── Step 2: Insert into visual_data.inspection_data ──────────────────────────

# def fetch_clean_and_insert_visual_data():
#     print("fetch_clean_and_insert_visual_data started")
#     logging.info("fetch_clean_and_insert_visual_data started")
#     try:
#         if df_cimcorp is None or df_cimcorp.empty:
#             print("No data fetched from Redshift.")
#             logging.info("No data fetched from Redshift.")
#             return

#         print(f"df_cimcorp shape: {df_cimcorp.shape}")
#         cleaned_df, invalid_rows, duplicates_df, messages = clean_cimcorp_data(df_cimcorp.copy())
#         for msg in messages:
#             print(msg)
#             logging.info(msg)

#         print(f"cleaned_df shape: {cleaned_df.shape}")
#         if cleaned_df.empty:
#             print("No cleaned data to insert into visual_data.inspection_data")
#             logging.info("No cleaned data to insert into visual_data.inspection_data")
#             return

#         inspection_collection = client["visual_data"]["inspection_data"]
#         records = cleaned_df.to_dict(orient="records")
#         if records:
#             inspection_collection.insert_many(records, ordered=False)
#             print(f"Inserted {len(records)} records into visual_data.inspection_data")
#             logging.info(f"Inserted {len(records)} records into visual_data.inspection_data")

#     except Exception as e:
#         print(f"fetch_clean_and_insert_visual_data failed: {e}")
#         logging.error(f"fetch_clean_and_insert_visual_data failed: {e}")
#         logging.error(traceback.format_exc())

# # ── Step 3: Build nested and insert into All_SLOBS_DATA.active_sku_list ───────

# def fix_curing_press(val):
#     return "None" if val in ("MNone", "Mnone", "mnone") else val

# def build_nested(records):
#     groups = defaultdict(list)
#     for r in records:
#         date_str = r.get("tbm_buildtime_tub", "None")[:10] if r.get("tbm_buildtime_tub", "None") != "None" else "None"
#         gt_code  = r.get("tbm_gtcode_tub", "None")
#         groups[(date_str, gt_code)].append(r)
#     return [
#         {
#             "tbm_buildtime_tub": date_str,
#             "tbm_gtcode_tub": gt_code,
#             "barcodes": [{"index": i, **r} for i, r in enumerate(recs)]
#         }
#         for (date_str, gt_code), recs in groups.items()
#     ]

# def build_and_insert_slobs_data():
#     logging.info("build_and_insert_slobs_data started")
#     try:
#         raw = list(client[visual_data][inspection_data].find({}, {"_id": 0}))
#         logging.info(f"Fetched {len(raw)} records from visual_data.inspection_data")

#         for doc in raw:
#             doc["curing_press_tub"] = fix_curing_press(doc.get("curing_press_tub", "None"))

#         nested = build_nested(raw)
#         logging.info(f"Built {len(nested)} grouped documents")

#         col = client[All_SLOBS_DATA][active_sku_list]
#         col.delete_many({})
#         if nested:
#             col.insert_many(nested, ordered=False)

#         print(f"Inserted {len(nested)} documents into All_SLOBS_DATA.active_sku_list")
#         logging.info(f"Inserted {len(nested)} documents into All_SLOBS_DATA.active_sku_list")

#     except Exception as e:
#         print(f"build_and_insert_slobs_data failed: {e}")
#         logging.error(f"build_and_insert_slobs_data failed: {e}")
#         logging.error(traceback.format_exc())

# # ── Main ──────────────────────────────────────────────────────────────────────

# def main():
#     try:
#         configure_logging()
#         warnings.filterwarnings("ignore")
#         logging.info("Starting visual_data pipeline...")

#         # Step 1: Store raw data into CIMCORP_VISUAL
#         insert_with_timestamp(df_cimcorp, CIMCORP_CLEANED_PREFIX_visual, "CIMCORP_VISUAL")
#         logging.info("Raw data inserted into CIMCORP_VISUAL.")

#         # Step 2: Clean and insert into visual_data.inspection_data
#         fetch_clean_and_insert_visual_data()

#         # Step 3: Build nested structure and insert into All_SLOBS_DATA.active_sku_list
#         build_and_insert_slobs_data()

#         logging.info("visual_data pipeline completed.")

#     except Exception as e:
#         logging.error(f"Error in main: {str(e)}")
#         logging.error(traceback.format_exc())
#         print(f"Error in main: {str(e)}")

# if __name__ == "__main__":
#     main()