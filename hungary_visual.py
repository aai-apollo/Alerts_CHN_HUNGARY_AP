# import pandas as pd
# import re
# from datetime import datetime, timezone, time as datetime_time
# from datetime import datetime, date
# import traceback
# import os
# import numpy as np
# import time
# import redshift_connector
# import pytz
# import pymongo
# from pymongo import MongoClient
# from datetime import datetime, timedelta
# from pytz import timezone
# import logging
# from datetime import datetime
# from passlib.context import CryptContext
# import warnings
# pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
# from config_hungary import (
#     client,
#     INVALID_ROWS_DB,
#     channel_no_collect,
#     re_codes,
#     alert_mail_db,
#     admin_table,
#     alert_users,
#     config_databse,
#     visual_data,
#     inspection_data,
#     All_SLOBS_DATA,
#     active_sku_list
# )


# def configure_logging():
#     if not os.path.exists('logs'):
#         os.makedirs('logs')

#     current_date = datetime.now().strftime('%Y-%m-%d')
#     log_filename = f'logs/log_home_{current_date}.txt'

#     for handler in logging.root.handlers[:]:
#         logging.root.removeHandler(handler)

#     logging.basicConfig(
#         level=logging.INFO,
#         format='%(asctime)s - %(levelname)s - %(message)s',
#         handlers=[
#             logging.FileHandler(log_filename, mode='a', encoding='utf-8')
#         ]
#     )
#     warnings.simplefilter("always")
#     logging.captureWarnings(True)


# def some_function():
#     india_time = datetime.now(pytz.timezone('Asia/Kolkata'))
#     return india_time


# logging.info(some_function())

# DEFAULT_DATETIME = datetime(1900, 1, 1, 12, 30, 0)
# DEFAULT_DATE = datetime(1900, 1, 1)


# class RedshiftDataConnectorr:
#     def __init__(self):
#         self.conn = redshift_connector.connect(
#             host='atl-dl-prd-01.cuvxusm4qify.ap-south-1.redshift.amazonaws.com',
#             database='db_prd_plant_gyo',
#             port=5439,
#             user='usr_gobinath_b',
#             password='nl2k#Z#u74FZ#2uP', sslmode='disable'
#         )

#     def execute_query(self, q):
#         data = pd.read_sql(q, self.conn)
#         # Strip timezone from all datetime columns — NaT with tz causes
#         # "NaTType does not support utcoffset" when serialising to MongoDB
#         for col in data.select_dtypes(include=['datetimetz', 'datetime64[ns, UTC]',
#                                                'datetime64[ns]']).columns:
#             try:
#                 data[col] = data[col].dt.tz_localize(None) if data[col].dt.tz is None \
#                     else data[col].dt.tz_convert(None)
#             except Exception:
#                 data[col] = pd.to_datetime(data[col], errors='coerce').dt.tz_localize(None)
#         return data


# inst = RedshiftDataConnectorr()


# local_tz = pytz.timezone("Asia/Kolkata")
# # ---------------- MANUAL INPUT ----------------
# manual_start_date = '2025-12-31'
# manual_end_date   = '2026-01-31'
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

# # # -------- Fetch model numbers --------
# # # Only fetch SKUs where oe_replacement_slob == "OE"
# # mongo_db = client["production_db_hungary"]
# # channel_coll = mongo_db["channel_no_ufm"]

# # modelno_list = channel_coll.distinct("sku", {"oe_replacement_slob": "OE"})
# # modelno_list = [str(m) for m in modelno_list if m]

# # if not modelno_list:
# #     print("Warning: No model numbers found in MongoDB — continuing with hardcoded SKU list in queries.")
# # else:
# #     print("Model numbers from MongoDB (OE only):", modelno_list)

# # modelno_sql = ",".join(f"'{m}'" for m in modelno_list)

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


# # =================================================
# # QUERY 1 — UFM + TBM + CIMCORP + SCRAP (merged)
# # Filter: sur.sur_repair_comment IS NOT NULL
# # =================================================
# q1 = f"""
# SELECT 
#     d.*,
#     prod.*,
#     ch.*,
#     mt.tire_barcode,
#     mt.stt_id,
#     mt.ct_code AS mv_sku,
#     mt.last_change_time,
#     sur.sur_stt_id,
#     sur.sur_repair_time,
#     sur.sur_repair_code AS re_code,
#     sur.sur_repair_comment AS re_comment
# FROM (
#     SELECT 
#         ztc.barcode,
#         ztc.sku,
#         ztc.equipmentid AS UF_ID,
#         ztc.timestamp AS UF_time,
#         shift,
#         overallufmgrade
#     FROM sc_apollo_mom.t_zf_test_completed ztc
#     WHERE ztc.timestamp BETWEEN '{start_timestamp}' AND '{end_timestamp}'
#       AND ztc.sku IN ({modelno_sql})
# ) d

# LEFT JOIN (
#     SELECT 
#         equipmentid AS PTB_ID,
#         producedon AS PTB_time,
#         vst_vhmc_mes_gt_barcode,
#         vst_vhmc_mes_gt_code AS gt_code
#     FROM sc_mhshistory.t_tbm_pcr_productiondata
# ) prod
# ON prod.vst_vhmc_mes_gt_barcode = d.barcode

# LEFT JOIN (
#     SELECT 
#         curingpressid AS CUR_ID,
#         curingtimestamp AS Cur_time,
#         barcodeid,
#         curingmouldid AS cur_mouldid
#     FROM sc_mhshistory.t_curing_history
# ) ch
# ON ch.barcodeid = d.barcode

# LEFT JOIN sc_wms_apollo_pcr.mv_tire mt
#     ON mt.tire_barcode = d.barcode
#     AND mt.ct_code IN ({modelno_sql})
#     AND mt.last_change_time >= '{start_timestamp}'
#     AND mt.last_change_time <  '{end_timestamp}'

# LEFT JOIN sc_wms_apollo_pcr.stock_unit_repair sur
#     ON mt.stt_id = sur.sur_stt_id

# WHERE sur.sur_repair_comment IS NOT NULL
# """

# print("#########################")
# df_tub = inst.execute_query(q1)

# # ── Convert all datetime columns to "YYYY-MM-DD HH:MM:SS" string format ──────
# def _fmt_dt(x):
#     """Convert any datetime/timestamp to 'YYYY-MM-DD HH:MM:SS' string. None if null."""
#     if x is None or x is pd.NaT:
#         return None
#     if isinstance(x, float):
#         try:
#             if pd.isna(x):
#                 return None
#         except Exception:
#             pass
#     try:
#         ts = pd.to_datetime(x, errors='coerce', utc=True)
#         if pd.isna(ts):
#             ts = pd.to_datetime(x, errors='coerce')
#         if pd.isna(ts):
#             return None
#         if ts.tzinfo is not None:
#             ts = ts.tz_convert('UTC').tz_localize(None)
#         return ts.strftime('%Y-%m-%d %H:%M:%S')
#     except Exception:
#         return str(x).strip() if x else None

# _DT_COLS = ['uf_time', 'ptb_time', 'cur_time', 'last_change_time', 'sur_repair_time']
# for _col in _DT_COLS:
#     if _col in df_tub.columns:
#         df_tub[_col] = df_tub[_col].apply(_fmt_dt)

# print(f"[INFO] Datetime columns converted: {[c for c in _DT_COLS if c in df_tub.columns]}")


# def _safe_val(x):
#     """
#     Convert a single value to something PyMongo/BSON can serialize.
#     NaT and NaN → None.  tz-aware datetime/Timestamp → strip tz.
#     Called AFTER .to_dict('records') so x is already a Python object.
#     """
#     if x is pd.NaT:
#         return None
#     if isinstance(x, pd.Timestamp):
#         try:
#             if pd.isnull(x):
#                 return None
#         except Exception:
#             return None
#         return x.replace(tzinfo=None).to_pydatetime()
#     if isinstance(x, datetime):
#         return x.replace(tzinfo=None)
#     if isinstance(x, float):
#         try:
#             if pd.isna(x):
#                 return None
#         except Exception:
#             pass
#     return x


# def df_to_mongo_records(df):
#     """
#     Convert a DataFrame to a list of dicts safe for MongoDB.
#     Operates on dict values AFTER .to_dict() so pandas cannot sneak NaT
#     back in as a typed object.
#     """
#     raw = df.to_dict('records')
#     return [{k: _safe_val(v) for k, v in row.items()} for row in raw]


# def sanitize_df_for_mongo(df):
#     # No-op passthrough — real sanitization happens inside df_to_mongo_records.
#     return df


# def sanitize_collection_name(filename):
#     name = filename.rsplit(".", 1)[0]
#     name = re.sub(r'[\/\.\$]', '_', name)
#     return name[:120]


# def insert_inspection_data(df):
#     """Insert merged query results into visual_data.inspection_data."""
#     if df is None or df.empty:
#         print("[INFO] No inspection data to insert.")
#         return
#     try:
#         vis_db   = client[visual_data]
#         vis_coll = vis_db[inspection_data]
#         records  = df_to_mongo_records(df)
#         if records:
#             # Upsert by barcode to avoid duplicates on re-run
#             for i in range(0, len(records), 5000):
#                 chunk = records[i:i + 5000]
#                 barcodes = [r.get('barcode') for r in chunk if r.get('barcode')]
#                 if barcodes:
#                     vis_coll.delete_many({"barcode": {"$in": barcodes}})
#                 vis_coll.insert_many(chunk, ordered=False)
#             print(f"[INFO] Inserted {len(records)} records into {visual_data}.{inspection_data}")
#         else:
#             print("[INFO] No records to insert into inspection_data.")
#     except Exception as e:
#         print(f"[ERROR] Failed to insert into inspection_data: {e}")


# insert_inspection_data(df_tub)

# # df_cimcorp is no longer a separate query — set to empty DataFrame
# df_cimcorp = pd.DataFrame()


# # ── Unique index for deduplication ──────────────────────────────────────────
# UNIQUE_INDEX_FIELDS = [
#     ("barcode", 1), ("uf_time", 1), ("ptb_time", 1), ("cur_time", 1),
#     ("gt_weighting_time", 1), ("last_change_time", 1),
#     ("sur_repair_time", 1), ("tire_barcode", 1)
# ]
# UNIQUE_INDEX_NAME = "idx_unique_duplicate_detection"


# def ensure_unique_index(collection):
#     """Create the deduplication index if it doesn't already exist."""
#     existing = collection.index_information()
#     if UNIQUE_INDEX_NAME not in existing:
#         try:
#             collection.create_index(
#                 UNIQUE_INDEX_FIELDS,
#                 name=UNIQUE_INDEX_NAME,
#                 unique=True,
#                 background=True
#             )
#             logging.info(f"Created unique index on {collection.name}")
#         except Exception as e:
#             logging.warning(f"Index creation failed: {e}")
#     else:
#         logging.info(f"Unique index already exists on {collection.name}")


# def chunked_insert(collection, records, chunk_size=5000):
#     """Insert with ordered=False — MongoDB skips duplicates automatically."""
#     total    = len(records)
#     inserted = 0
#     for i in range(0, total, chunk_size):
#         chunk = records[i:i + chunk_size]
#         try:
#             result    = collection.insert_many(chunk, ordered=False)
#             inserted += len(result.inserted_ids)
#         except pymongo.errors.BulkWriteError as bwe:
#             inserted += bwe.details['nInserted']
#     return inserted


# def has_special_chars(value):
#     if isinstance(value, str):
#         return bool(re.search(r'[^0-9]', value))
#     return False


# def get_shift(dt):
#     if dt is None or dt == "No Date":
#         return "No Date"
#     if isinstance(dt, str):
#         try:
#             dt = pd.to_datetime(dt, errors='coerce')
#             if pd.isna(dt):
#                 return "No Date"
#         except Exception as e:
#             logging.info("Error parsing date: %s", e)
#             return "No Date"
#     if not isinstance(dt, (datetime, pd.Timestamp)):
#         return "No Date"
#     if pd.isna(dt):
#         return "No Date"
#     if dt == DEFAULT_DATETIME:
#         return "No Date"
#     if dt.tzinfo is not None:
#         dt = dt.replace(tzinfo=None)

#     hour = dt.hour

#     # Shift A: 06:00 – 17:59
#     if 6 <= hour <= 17:
#         return "A"
#     # Shift B: 18:00 – 23:59  OR  00:00 – 05:59 (overnight)
#     elif hour >= 18 or hour < 6:
#         return "B"
#     else:
#         return "No Shift"


# def clean_tub_data(df):
#     output_messages = []
#     invalid_rows = pd.DataFrame()
#     logging.info("clean_tub_data function is just started")
#     if not isinstance(df, pd.DataFrame):
#         raise TypeError("Expected a pandas DataFrame as input")
#     df_clean = df.copy()

#     if 'barcode' in df_clean.columns:
#         df_clean['barcode'] = df_clean['barcode'].astype(str)

#     rename_columns_mapping = {
#         'ufm': 'ubno',
#         'timestamp': 'date_and_time',
#         'equipmentid': 'tbm',
#         'producedon': 'tbm_buildtime',
#         'ctcode': 'sku',
#         'curingmouldid': 'curing_mouldid',
#         'curingpressid': 'curing_press',
#         'curingtimestamp': 'curing_infeedtime',
#         'vst_vhmc_mes_gt_code': 'tbm_gtcode',
#         'barcodeid': 'barcode',
#         'vst_vhmc_mes_gt_barcode': 'barcode_tbm'
#     }
#     existing_rename_mapping = {k: v for k, v in rename_columns_mapping.items() if k in df_clean.columns}
#     df_clean.rename(columns=existing_rename_mapping, inplace=True)

#     duplicates = df_clean.columns[df_clean.columns.duplicated(keep=False)].unique()
#     if len(duplicates) > 0:
#         logging.warning(f"Duplicate column names in input: {list(duplicates)}. Renaming duplicates.")
#         cols = pd.Series(df_clean.columns)
#         for dup in duplicates:
#             cols[cols[cols == dup].index.values.tolist()] = [
#                 f"{dup}_{i}" if i != 0 else dup for i in range(sum(cols == dup))
#             ]
#         df_clean.columns = cols

#     mask = df_clean['barcode'].isnull() | df_clean['barcode'].apply(has_special_chars)
#     invalid_rows = df_clean[mask]
#     df_clean = df_clean[~mask].drop_duplicates()
#     df_clean.drop(columns=['rn', 'Unnamed: 226', 'Unnamed: 225'], inplace=True, errors='ignore')
#     df_clean.columns = df_clean.columns.to_series().str.strip().str.lower().str.replace(' ', '_')

#     return df_clean, invalid_rows, output_messages


# def clean_visual_data(df):
#     output_messages = []
#     invalid_barcode_rows = pd.DataFrame()
#     duplicates_df = pd.DataFrame()
#     logging.info("clean_visual_data function is just started")

#     try:
#         df.columns = [re.sub(r'[.\s]+', '_', col.strip()).lower() for col in df.columns]

#         rename_columns_mapping = {
#             'equipmentid': 'tbm',
#             'producedon': 'tbm_buildtime',
#             'ctcode': 'sku',
#             'curingmouldid': 'curing_mouldid',
#             'curingpressid': 'curing_press',
#             'curingtimestamp': 'curing_infeedtime',
#             'vst_vhmc_mes_gt_code': 'tbm_gtcode',
#             'barcodeid': 'barcode',
#             'vst_vhmc_mes_gt_barcode': 'barcode_tbm'
#         }
#         for old_name, new_name in rename_columns_mapping.items():
#             if old_name in df.columns:
#                 df = df.rename(columns={old_name: new_name}, inplace=False)

#         df['barcode'] = df['barcode'].astype(str).str.strip()
#         df['curing_infeedtime'] = df['curing_infeedtime'].astype(str)

#         special_char_regex = re.compile(r'[^0-9]')

#         rows_with_null_barcode = df[df['barcode'].isnull() | (df['barcode'] == 'nan')]
#         rows_with_special_char_barcode = df[
#             df['barcode'].apply(lambda x: bool(special_char_regex.search(x)))
#         ]
#         invalid_barcode_rows = pd.concat(
#             [rows_with_null_barcode, rows_with_special_char_barcode]
#         ).drop_duplicates()

#         df = df[~df.index.isin(invalid_barcode_rows.index)]
#         df = df.dropna(axis=1, how='all')
#         duplicates_df = df[df.duplicated(subset=['barcode'], keep=False)]
#         df = df.drop_duplicates(subset=['barcode'])

#         output_messages.append("CIMCORP_HISTORICAL_DATA Cleaning successful")
#         logging.info("clean_visual_data function is completed")
#         return df, invalid_barcode_rows, duplicates_df, output_messages

#     except Exception as e:
#         error_message = f"Error during cleaning: {e}"
#         output_messages.append(error_message)
#         return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), output_messages


# def _upsert_records(collection, records, key_field):
#     """Fastest bulk write: chunked delete+insert_many."""
#     if not records:
#         return
#     CHUNK = 10000
#     keys = [rec[key_field] for rec in records if rec.get(key_field) is not None]
#     for i in range(0, len(keys), CHUNK):
#         collection.delete_many({key_field: {"$in": keys[i:i+CHUNK]}})
#     for i in range(0, len(records), CHUNK):
#         collection.insert_many(records[i:i+CHUNK], ordered=False)


# def _upsert_scrap_records(collection, records, key_cols):
#     """Fastest scrap bulk write: chunked delete+insert_many."""
#     if not records or not key_cols:
#         return
#     CHUNK = 10000
#     if 'tire_barcode' in key_cols:
#         barcodes = [rec.get('tire_barcode') for rec in records if rec.get('tire_barcode')]
#         for i in range(0, len(barcodes), CHUNK):
#             collection.delete_many({"tire_barcode": {"$in": barcodes[i:i+CHUNK]}})
#     else:
#         or_clauses = [{col: rec.get(col) for col in key_cols} for rec in records]
#         for i in range(0, len(or_clauses), CHUNK):
#             collection.delete_many({"$or": or_clauses[i:i+CHUNK]})
#     for i in range(0, len(records), CHUNK):
#         collection.insert_many(records[i:i+CHUNK], ordered=False)


# def copy_to_single_collection_api(df_summary=None, df_scrap=None):
#     """No longer writes to TUB_CIMCORP_VISUAL_MERGED_DB — kept as no-op for compatibility."""
#     return []


# # =====================================================
# # AGGREGATION FUNCTION: Group by uf_time date with all SKUs
# # =====================================================
# def aggregate_and_store_by_date_sku():
#     """
#     Read data from visual_data.inspection_data, aggregate by uf_time date only,
#     and store in All_SLOBS_DATA.active_sku_list.
#     Each document represents a unique uf_time date with all SKUs and their respective barcodes.
#     """
#     logging.info("Starting aggregation by uf_time date with all SKUs...")
    
#     try:
#         # Connect to the source database
#         source_db = client[visual_data]
#         source_collection = source_db[inspection_data]
        
#         # Connect to the target database
#         target_db = client[All_SLOBS_DATA]
#         target_collection = target_db[active_sku_list]
        
#         # Step 1: Aggregate by uf_time date and sku first
#         pipeline = [
#             {
#                 "$match": {
#                     "uf_time": {"$exists": True, "$ne": None, "$ne": ""},
#                     "sku": {"$exists": True, "$ne": None, "$ne": ""}
#                 }
#             },
#             {
#                 "$addFields": {
#                     "uf_date": {
#                         "$dateToString": {
#                             "format": "%Y-%m-%d",
#                             "date": {"$toDate": "$uf_time"}
#                         }
#                     }
#                 }
#             },
#             {
#                 "$group": {
#                     "_id": {
#                         "uf_date": "$uf_date",
#                         "sku": "$sku"
#                     },
#                     "barcodes": {
#                         "$push": {
#                             "barcode": "$barcode",
#                             "ptb_time": "$ptb_time",
#                             "ptb_id": "$ptb_id",
#                             "gt_code": "$gt_code",
#                             "cur_id": "$cur_id",
#                             "cur_mouldid": "$cur_mouldid",
#                             "cur_time": "$cur_time",
#                             "re_comment": "$re_comment",
#                             "sur_repair_time": "$sur_repair_time",
#                             "visualinspection_time_tub": "$visualinspection_time_tub",
#                             "last_change_time": "$last_change_time",
#                             "re_code": "$re_code",
#                             "uf_id": "$uf_id",
#                             "uf_time": "$uf_time"
#                         }
#                     }
#                 }
#             },
#             {
#                 "$group": {
#                     "_id": "$_id.uf_date",
#                     "skus": {
#                         "$push": {
#                             "sku": "$_id.sku",
#                             "barcodes": "$barcodes"
#                         }
#                     }
#                 }
#             },
#             {
#                 "$project": {
#                     "_id": 0,
#                     "uf_time": "$_id",
#                     "skus": 1
#                 }
#             },
#             {
#                 "$sort": {
#                     "uf_time": 1
#                 }
#             }
#         ]
        
#         # Execute aggregation
#         logging.info("Executing aggregation pipeline...")
#         aggregated_results = list(source_collection.aggregate(pipeline, allowDiskUse=True))
        
#         if not aggregated_results:
#             logging.info("No data found to aggregate.")
#             return {
#                 "status": "success",
#                 "message": "No data found to aggregate.",
#                 "records_processed": 0,
#                 "records_inserted": 0
#             }
        
#         logging.info(f"Found {len(aggregated_results)} unique uf_time dates to process.")
        
#         # Get unique dates from results for deletion
#         dates = list(set([doc['uf_time'] for doc in aggregated_results]))
        
#         # Delete existing data for the dates being inserted
#         if dates:
#             delete_result = target_collection.delete_many({
#                 "uf_time": {"$in": dates}
#             })
#             logging.info(f"Deleted {delete_result.deleted_count} existing records for dates: {dates}")
        
#         # Insert new aggregated data in chunks
#         total_inserted = 0
#         chunk_size = 1000
        
#         for i in range(0, len(aggregated_results), chunk_size):
#             chunk = aggregated_results[i:i+chunk_size]
#             try:
#                 result = target_collection.insert_many(chunk, ordered=False)
#                 total_inserted += len(result.inserted_ids)
#                 logging.info(f"Inserted chunk {i//chunk_size + 1} with {len(result.inserted_ids)} records")
#             except pymongo.errors.BulkWriteError as bwe:
#                 inserted_count = bwe.details.get('nInserted', 0)
#                 total_inserted += inserted_count
#                 logging.warning(f"Bulk write error in chunk {i//chunk_size + 1}: {bwe}")
#                 logging.warning(f"Inserted {inserted_count} out of {len(chunk)} documents in this chunk")
        
#         logging.info(f"Successfully inserted {total_inserted} aggregated records into {All_SLOBS_DATA}.{active_sku_list}")
        
#         return {
#             "status": "success",
#             "message": f"Successfully aggregated {len(aggregated_results)} uf_time dates.",
#             "records_processed": len(aggregated_results),
#             "records_inserted": total_inserted,
#             "dates": dates,
#             "date_count": len(dates)
#         }
        
#     except Exception as e:
#         error_msg = f"Error in aggregate_and_store_by_date_sku: {str(e)}"
#         logging.error(error_msg)
#         logging.error(traceback.format_exc())
#         return {
#             "status": "error",
#             "message": error_msg,
#             "records_processed": 0,
#             "records_inserted": 0
#         }


# # =====================================================
# # ALTERNATIVE: Aggregate by uf_time month (YYYY-MM)
# # =====================================================
# def aggregate_and_store_by_uf_month():
#     """
#     Aggregate by uf_time month (YYYY-MM) instead of date.
#     Useful for monthly reporting.
#     """
#     logging.info("Starting aggregation by uf_time month with all SKUs...")
    
#     try:
#         # Connect to the source database
#         source_db = client[visual_data]
#         source_collection = source_db[inspection_data]
        
#         # Connect to the target database
#         target_db = client[All_SLOBS_DATA]
#         target_collection = target_db[active_sku_list]
        
#         pipeline = [
#             {
#                 "$match": {
#                     "uf_time": {"$exists": True, "$ne": None, "$ne": ""},
#                     "sku": {"$exists": True, "$ne": None, "$ne": ""}
#                 }
#             },
#             {
#                 "$addFields": {
#                     "uf_month": {
#                         "$dateToString": {
#                             "format": "%Y-%m",
#                             "date": {"$toDate": "$uf_time"}
#                         }
#                     }
#                 }
#             },
#             {
#                 "$group": {
#                     "_id": {
#                         "uf_month": "$uf_month",
#                         "sku": "$sku"
#                     },
#                     "barcodes": {
#                         "$push": {
#                             "barcode": "$barcode",
#                             "ptb_time": "$ptb_time",
#                             "ptb_id": "$ptb_id",
#                             "gt_code": "$gt_code",
#                             "cur_id": "$cur_id",
#                             "cur_mouldid": "$cur_mouldid",
#                             "cur_time": "$cur_time",
#                             "re_comment": "$re_comment",
#                             "sur_repair_time": "$sur_repair_time",
#                             "visualinspection_time_tub": "$visualinspection_time_tub",
#                             "last_change_time": "$last_change_time",
#                             "re_code": "$re_code",
#                             "uf_id": "$uf_id",
#                             "uf_time": "$uf_time"
#                         }
#                     }
#                 }
#             },
#             {
#                 "$group": {
#                     "_id": "$_id.uf_month",
#                     "skus": {
#                         "$push": {
#                             "sku": "$_id.sku",
#                             "barcodes": "$barcodes"
#                         }
#                     }
#                 }
#             },
#             {
#                 "$project": {
#                     "_id": 0,
#                     "uf_time": "$_id",
#                     "skus": 1
#                 }
#             },
#             {
#                 "$sort": {
#                     "uf_time": 1
#                 }
#             }
#         ]
        
#         logging.info("Executing monthly aggregation pipeline...")
#         aggregated_results = list(source_collection.aggregate(pipeline, allowDiskUse=True))
        
#         if not aggregated_results:
#             logging.info("No data found to aggregate by month.")
#             return {
#                 "status": "success",
#                 "message": "No data found to aggregate.",
#                 "records_processed": 0,
#                 "records_inserted": 0
#             }
        
#         logging.info(f"Found {len(aggregated_results)} unique uf_time months to process.")
        
#         # Get unique months from results for deletion
#         months = list(set([doc['uf_time'] for doc in aggregated_results]))
        
#         # Delete existing data for the months being inserted
#         if months:
#             delete_result = target_collection.delete_many({
#                 "uf_time": {"$in": months}
#             })
#             logging.info(f"Deleted {delete_result.deleted_count} existing records for months: {months}")
        
#         # Insert new aggregated data
#         total_inserted = 0
#         chunk_size = 1000
        
#         for i in range(0, len(aggregated_results), chunk_size):
#             chunk = aggregated_results[i:i+chunk_size]
#             try:
#                 result = target_collection.insert_many(chunk, ordered=False)
#                 total_inserted += len(result.inserted_ids)
#             except pymongo.errors.BulkWriteError as bwe:
#                 inserted_count = bwe.details.get('nInserted', 0)
#                 total_inserted += inserted_count
#                 logging.warning(f"Bulk write error: {bwe}")
        
#         logging.info(f"Successfully inserted {total_inserted} aggregated monthly records")
        
#         return {
#             "status": "success",
#             "message": f"Successfully aggregated {len(aggregated_results)} months.",
#             "records_processed": len(aggregated_results),
#             "records_inserted": total_inserted,
#             "months": months,
#             "month_count": len(months)
#         }
        
#     except Exception as e:
#         error_msg = f"Error in aggregate_and_store_by_uf_month: {str(e)}"
#         logging.error(error_msg)
#         logging.error(traceback.format_exc())
#         return {
#             "status": "error",
#             "message": error_msg,
#             "records_processed": 0,
#             "records_inserted": 0
#         }


# # =====================================================
# # CREATE INDEXES FOR TARGET COLLECTION
# # =====================================================
# def create_indexes_for_aggregation():
#     """
#     Create indexes on the target collection for better query performance
#     """
#     try:
#         target_db = client[All_SLOBS_DATA]
#         target_collection = target_db[active_sku_list]
        
#         # Create indexes
#         target_collection.create_index([("uf_time", 1)], unique=True, background=True)
        
#         logging.info(f"Indexes created on {All_SLOBS_DATA}.{active_sku_list}")
#         return True
#     except Exception as e:
#         logging.error(f"Error creating indexes: {e}")
#         return False


# # =====================================================
# # UPDATE run_data_cleaning function to use uf_time
# # =====================================================
# def run_data_cleaning(df_tub_input, df_cimcorp_input):
#     """
#     Cleans df_tub_input and df_cimcorp_input in-memory and writes
#     directly to the final merged DB. No intermediate DBs are used.
#     """
#     start_time = time.time()
#     output_messages = []
#     logging.info("run_data_cleaning function started")
#     try:
#         cleaned_summary, invalid_df, tub_msgs = clean_tub_data(df_tub_input.copy())
#         output_messages.extend(tub_msgs)
#         if not invalid_df.empty:
#             invalid_df = invalid_df.drop('_id', axis=1, errors='ignore')
#             client[INVALID_ROWS_DB]["invalid_data_TUB_home"].insert_many(df_to_mongo_records(invalid_df))
#             output_messages.append(f"Saved {len(invalid_df)} invalid TUB rows to INVALID_ROWS.invalid_data_TUB_home")

#         # Convert datetime columns to "YYYY-MM-DD HH:MM:SS" string format
#         def _fmt_dt(x):
#             """Convert any datetime/timestamp to 'YYYY-MM-DD HH:MM:SS' string. None if null."""
#             if x is None or x is pd.NaT:
#                 return None
#             if isinstance(x, float) and pd.isna(x):
#                 return None
#             try:
#                 ts = pd.to_datetime(x, errors='coerce', utc=True)
#                 if pd.isna(ts):
#                     ts = pd.to_datetime(x, errors='coerce')
#                 if pd.isna(ts):
#                     return None
#                 # Convert to UTC-naive then format
#                 if ts.tzinfo is not None:
#                     ts = ts.tz_convert('UTC').tz_localize(None)
#                 return ts.strftime('%Y-%m-%d %H:%M:%S')
#             except Exception:
#                 return str(x).strip() if x else None

#         for _col in ['uf_time', 'ptb_time', 'cur_time', 'last_change_time', 'sur_repair_time']:
#             if _col in cleaned_summary.columns:
#                 cleaned_summary[_col] = cleaned_summary[_col].apply(_fmt_dt)

#         if df_cimcorp_input is None or not isinstance(df_cimcorp_input, pd.DataFrame) or df_cimcorp_input.empty:
#             logging.info("Scrap/repair data is empty or None — proceeding with TUB data only.")
#             cleaned_scrap = pd.DataFrame()
#         else:
#             cleaned_scrap = df_cimcorp_input.copy()
#             cleaned_scrap.columns = [c.lower() for c in cleaned_scrap.columns]

#         def _to_str_val(x, strip_decimal=False):
#             if x is pd.NaT or (isinstance(x, float) and pd.isna(x)):
#                 return None
#             if strip_decimal:
#                 try:
#                     return str(int(float(x)))
#                 except (ValueError, TypeError):
#                     pass
#             return str(x).strip()

#         if 're_code' in cleaned_scrap.columns:
#             cleaned_scrap['re_code'] = cleaned_scrap['re_code'].apply(lambda x: _to_str_val(x, strip_decimal=True))
#             cleaned_scrap['re_code'] = cleaned_scrap['re_code'].apply(
#                 lambda x: '0' if (x is None or str(x).strip() in ('', 'None', 'nan', 'NaN')) else x
#             )
#         for _col in ['re_last_change_time', 'sur_repair_time', 'gt_weighting_time', 'last_change_time', 'cp_curing_time']:
#             if _col in cleaned_scrap.columns:
#                 cleaned_scrap[_col] = cleaned_scrap[_col].apply(_fmt_dt)


#         def _extract_comment_code(comment):
#             """Extract the first standalone number from re_comment."""
#             if not comment or not isinstance(comment, str):
#                 return None
#             m = re.search(r'\b(\d+)\b', comment)
#             return m.group(1) if m else None

#         try:
#             re_code_docs = list(client[config_databse]["re_codes"].find({}, {"_id": 0, "condition_code": 1, "description": 1}))
#             if re_code_docs:
#                 re_code_map = {
#                     str(doc["condition_code"]).strip(): doc.get("description", "")
#                     for doc in re_code_docs
#                 }
#                 re_desc_map = {
#                     desc.lower(): (code, desc)
#                     for code, desc in re_code_map.items()
#                 }

#                 def _resolve(row):
#                     comment = str(row.get("re_comment", "") or "").strip()
#                     key = str(row.get("re_code", "") or "").strip()
#                     if key and key in re_code_map:
#                         return key, re_code_map[key]
#                     comment_key = _extract_comment_code(comment)
#                     if comment_key and comment_key in re_code_map:
#                         return comment_key, re_code_map[comment_key]
#                     if comment:
#                         comment_lower = comment.lower()
#                         if comment_lower in re_desc_map:
#                             return re_desc_map[comment_lower]
#                         for desc_lower, (code, desc) in re_desc_map.items():
#                             if desc_lower in comment_lower or comment_lower in desc_lower:
#                                 return code, desc
#                     return "", ""

#                 resolved = cleaned_scrap.apply(_resolve, axis=1)
#                 cleaned_scrap["condition_code"] = resolved.apply(lambda x: x[0])
#                 cleaned_scrap["description"]    = resolved.apply(lambda x: x[1])

#                 _uf_keywords = {"force", "runout", "rfv-grinding"}
#                 cleaned_scrap["scrap_category"] = cleaned_scrap["description"].apply(
#                     lambda x: "UF" if str(x).strip().lower() in _uf_keywords else "VISUAL"
#                 )
#                 output_messages.append(f"Enriched scrap data with condition_code + description from {len(re_code_map)} re_codes.")
#             else:
#                 output_messages.append("No re_codes found in config DB — skipping enrichment.")
#         except Exception as re_err:
#             output_messages.append(f"Warning: could not enrich re_codes: {re_err}")
#             logging.warning(f"re_codes enrichment failed: {re_err}")

#         try:
#             qr_docs = list(client[config_databse][channel_no_collect].find(
#                 {}, {"_id": 0, "sku": 1, "QR_Status": 1}
#             ))
#             if qr_docs:
#                 qr_map = {str(doc["sku"]).strip(): doc.get("QR_Status", "") for doc in qr_docs}
#                 barcode_col = next((c for c in ['barcode'] if c in cleaned_summary.columns), None)
#                 sku_col     = next((c for c in ['sku'] if c in cleaned_summary.columns), None)
#                 if barcode_col and sku_col:
#                     barcode_sku_map = dict(zip(
#                         cleaned_summary[barcode_col].astype(str),
#                         cleaned_summary[sku_col].astype(str)
#                     ))
#                     scrap_bc_col = next((c for c in ['tire_barcode', 'barcode'] if c in cleaned_scrap.columns), None)
#                     if scrap_bc_col:
#                         cleaned_scrap["sku"] = cleaned_scrap[scrap_bc_col].apply(
#                             lambda bc: barcode_sku_map.get(str(bc).strip(), "")
#                         )
#                         cleaned_scrap["qr_status"] = cleaned_scrap["sku"].apply(
#                             lambda s: qr_map.get(str(s).strip(), "")
#                         )
#                         output_messages.append("Added qr_status to scrap data via sku lookup.")
#                     else:
#                         output_messages.append("Warning: no barcode column found in scrap data for QR_Status join.")
#                 else:
#                     output_messages.append("Warning: barcode/sku columns missing in summary for QR_Status join.")
#             else:
#                 output_messages.append("No QR_Status data found in channel_no_ufm — skipping.")
#         except Exception as qr_err:
#             output_messages.append(f"Warning: could not add QR_Status: {qr_err}")
#             logging.warning(f"QR_Status enrichment failed: {qr_err}")

#         copy_msgs = copy_to_single_collection_api(
#             df_summary=cleaned_summary,
#             df_scrap=cleaned_scrap
#         )
#         output_messages.extend(copy_msgs)

#         run_timestamp = some_function()
#         client["file_tracking"]["file_metadata"].insert_one({
#             "query":              "Q1_TUB_TBM_CIMCORP",
#             "description":        "UFM + TBM + Curing + Repair merged data",
#             "filename":           f"Q1_{run_timestamp.strftime('%Y%m%d_%H%M%S')}",
#             "inserted_timestamp": run_timestamp,
#             "start_timestamp":    start_timestamp,
#             "end_timestamp":      end_timestamp,
#             "num_rows_raw":       len(df_tub_input),
#             "num_rows_cleaned":   len(cleaned_summary),
#             "num_rows_invalid":   len(invalid_df) if not invalid_df.empty else 0,
#             "target_db":          visual_data,
#             "target_collection":  inspection_data,
#             "processed":          True,
#             "processed_timestamp": run_timestamp
#         })
#         output_messages.append("Run tracked in file_tracking.file_metadata.")

#         # Create indexes on the target collection
#         create_indexes_for_aggregation()
        
#         # AFTER cleaning, run the aggregation by uf_time date
#         aggregation_result = aggregate_and_store_by_date_sku()
#         output_messages.append(f"Aggregation result: {aggregation_result.get('message', '')}")
        
#         execution_time = time.time() - start_time
#         output_messages.append(f"Completed in {execution_time:.2f} seconds.")
#         logging.info("run_data_cleaning function completed")
#         return {
#             "status": "success",
#             "message": "Data cleaning and processing completed successfully.",
#             "output_log": output_messages,
#             "execution_time": execution_time,
#             "aggregation_result": aggregation_result
#         }
#     except Exception as e:
#         error_message = f"An error occurred in run_data_cleaning: {e}"
#         output_messages.append(error_message)
#         logging.error(error_message)
#         return {
#             "status": "error",
#             "message": error_message,
#             "output_log": output_messages,
#             "execution_time": 0
#         }

# date_formats = ["%d-%m-%Y", "%Y-%m-%d"]


# def ensure_admin_collection_exists():
#     logging.info("ensure_admin_collection_exists function is just started")
#     try:
#         db = client[alert_mail_db]
#         collection = db[admin_table]
#         alert_users_collection = db[alert_users]
#         if admin_table not in db.list_collection_names() or collection.count_documents({}) == 0:
#             logging.info(f"Collection '{admin_table}' is missing or empty. Inserting default admin user...")
#             hashed_password = pwd_context.hash("1234")
#             default_admin = {
#                 "user_id": 1,
#                 "email": "manager@apollotyres.com",
#                 "fname": "Admin",
#                 "lname": "User",
#                 "password": hashed_password,
#                 "role": "manager",
#                 "created_by_id": 1,
#                 "created_at": datetime.utcnow()
#             }
#             collection.insert_one(default_admin)
#             alert_users_collection.insert_one(default_admin)
#             logging.info("Default manager admin inserted.")
#             return
#         if collection.count_documents({"role": "manager"}) == 0:
#             logging.info(f"No user with role 'manager' found. Inserting default manager...")
#             hashed_password = pwd_context.hash("1234")
#             default_admin = {
#                 "user_id": 1,
#                 "email": "manager@apollotyres.com",
#                 "password": hashed_password,
#                 "role": "manager",
#                 "created_by_id": 1,
#                 "created_at": datetime.utcnow()
#             }
#             collection.insert_one(default_admin)
#             alert_users_collection.insert_one(default_admin)
#             logging.info("Default manager admin inserted into both admin_table and alert_users.")
#         else:
#             logging.info("At least one 'manager' user already exists in admin_table. No insert needed.")
#     except Exception as e:
#         logging.error(f"Error in ensure_admin_collection_exists: {str(e)}")


# def main():
#     try:
#         configure_logging()
#         warnings.filterwarnings("ignore")
#         logging.info("Starting the data cleaning process...")

#         ensure_admin_collection_exists()
#         logging.info("Admin collection check completed.")

#         result = run_data_cleaning(df_tub, df_cimcorp)
#         logging.info(f"Data cleaning result: {result['message']}")
#         logging.info(f"Execution time: {result['execution_time']:.2f} seconds")
#         logging.info("Output log:\n" + "\n".join(result['output_log']))
#         print(result["message"])
#         print(f"Execution time: {result['execution_time']:.2f} seconds")
#         print("Output log:", result["output_log"])
        
#         # Print aggregation results if available
#         if 'aggregation_result' in result:
#             print("\nAggregation Results:")
#             print(f"  Status: {result['aggregation_result'].get('status', 'N/A')}")
#             print(f"  Message: {result['aggregation_result'].get('message', 'N/A')}")
#             print(f"  Records Processed: {result['aggregation_result'].get('records_processed', 0)}")
#             print(f"  Records Inserted: {result['aggregation_result'].get('records_inserted', 0)}")
#             if 'dates' in result['aggregation_result']:
#                 print(f"  Dates: {result['aggregation_result']['dates']}")
            
#     except Exception as e:
#         error_message = f"Error in main function: {str(e)}"
#         logging.error(error_message)
#         logging.error(traceback.format_exc())
#         print(error_message)
#         print(traceback.format_exc())


# if __name__ == "__main__":
#     main()


import pandas as pd
import re
from datetime import datetime, timezone, time as datetime_time
from datetime import datetime, date
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
import warnings
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
from config_hungary_uat import (
    client,
    INVALID_ROWS_DB,
    channel_no_collect,
    re_codes,
    alert_mail_db,
    admin_table,
    alert_users,
    config_databse,
    visual_data,
    inspection_data,
    All_SLOBS_DATA,
    active_sku_list
)


def configure_logging():
    if not os.path.exists('logs'):
        os.makedirs('logs')

    current_date = datetime.now().strftime('%Y-%m-%d')
    log_filename = f'logs/log_visual_{current_date}.txt'

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


def some_function():
    india_time = datetime.now(pytz.timezone('Asia/Kolkata'))
    return india_time


logging.info(some_function())

DEFAULT_DATETIME = datetime(1900, 1, 1, 12, 30, 0)
DEFAULT_DATE = datetime(1900, 1, 1)


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


############## cronjob START #######################
from datetime import datetime, timedelta, timezone 
 
# --- CURRENT UTC TIME --- 
now_utc = datetime.now(timezone.utc) 
 
# --- ALIGN TO CURRENT HOUR :25 --- 
end_dt = now_utc.replace(minute=25, second=0, microsecond=0) 
 
# --- START TIME: 11 HOURS BACK --- 
start_dt = end_dt - timedelta(hours=11) 
 
# --- FORMAT --- 
start_timestamp = start_dt.strftime("%Y-%m-%d %H:%M:%S") 
end_timestamp = end_dt.strftime("%Y-%m-%d %H:%M:%S") 
 
# --- OUTPUT --- 
print("Current UTC :", now_utc) 
print("Start Time  :", start_timestamp) 
print("End Time    :", end_timestamp)

############## cronjob END #######################

############## Manual Trigger START #######################
# local_tz = pytz.timezone("Asia/Kolkata")
# # ---------------- MANUAL INPUT ----------------
# manual_start_date = '2026-08-23'
# manual_end_date   = '2026-08-25'
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

############# Manual Trigger END #######################

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


# =================================================
# QUERY 1 — UFM + TBM + CIMCORP + SCRAP (merged)
# Filter: sur.sur_repair_comment IS NOT NULL
# =================================================
q1 = f"""
SELECT 
    d.*,
    prod.*,
    ch.*,
    mt.tire_barcode,
    mt.stt_id,
    mt.ct_code AS mv_sku,
    mt.last_change_time,
    sur.sur_stt_id,
    sur.sur_repair_time,
    sur.sur_repair_code AS re_code,
    sur.sur_repair_comment AS re_comment
FROM (
    SELECT 
        ztc.barcode,
        ztc.sku,
        ztc.equipmentid AS UF_ID,
        ztc.timestamp AS UF_time,
        shift,
        overallufmgrade
    FROM sc_apollo_mom.t_zf_test_completed ztc
    WHERE ztc.timestamp BETWEEN '{start_timestamp}' AND '{end_timestamp}'
      AND ztc.sku IN ({modelno_sql})
) d

LEFT JOIN (
    SELECT 
        equipmentid AS PTB_ID,
        producedon AS PTB_time,
        vst_vhmc_mes_gt_barcode,
        vst_vhmc_mes_gt_code AS gt_code
    FROM sc_mhshistory.t_tbm_pcr_productiondata
) prod
ON prod.vst_vhmc_mes_gt_barcode = d.barcode

LEFT JOIN (
    SELECT 
        curingpressid AS CUR_ID,
        curingtimestamp AS Cur_time,
        barcodeid,
        curingmouldid AS cur_mouldid
    FROM sc_mhshistory.t_curing_history
) ch
ON ch.barcodeid = d.barcode

LEFT JOIN sc_wms_apollo_pcr.mv_tire mt
    ON mt.tire_barcode = d.barcode
    AND mt.ct_code IN ({modelno_sql})
    AND mt.last_change_time >= '{start_timestamp}'
    AND mt.last_change_time <  '{end_timestamp}'

LEFT JOIN sc_wms_apollo_pcr.stock_unit_repair sur
    ON mt.stt_id = sur.sur_stt_id

WHERE sur.sur_repair_comment IS NOT NULL
"""

print("#########################")
df_tub = inst.execute_query(q1)

# ── Convert all datetime columns to "YYYY-MM-DD HH:MM:SS" string format ──────
def _fmt_dt(x):
    """Convert any datetime/timestamp to 'YYYY-MM-DD HH:MM:SS' string. None if null."""
    if x is None or x is pd.NaT:
        return None
    if isinstance(x, float):
        try:
            if pd.isna(x):
                return None
        except Exception:
            pass
    try:
        ts = pd.to_datetime(x, errors='coerce', utc=True)
        if pd.isna(ts):
            ts = pd.to_datetime(x, errors='coerce')
        if pd.isna(ts):
            return None
        if ts.tzinfo is not None:
            ts = ts.tz_convert('UTC').tz_localize(None)
        return ts.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return str(x).strip() if x else None

_DT_COLS = ['uf_time', 'ptb_time', 'cur_time', 'last_change_time', 'sur_repair_time']
for _col in _DT_COLS:
    if _col in df_tub.columns:
        df_tub[_col] = df_tub[_col].apply(_fmt_dt)

print(f"[INFO] Datetime columns converted: {[c for c in _DT_COLS if c in df_tub.columns]}")


def _safe_val(x):
    """
    Convert a single value to something PyMongo/BSON can serialize.
    NaT and NaN → None.  tz-aware datetime/Timestamp → strip tz.
    Called AFTER .to_dict('records') so x is already a Python object.
    """
    if x is pd.NaT:
        return None
    if isinstance(x, pd.Timestamp):
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
    Convert a DataFrame to a list of dicts safe for MongoDB.
    Operates on dict values AFTER .to_dict() so pandas cannot sneak NaT
    back in as a typed object.
    """
    raw = df.to_dict('records')
    return [{k: _safe_val(v) for k, v in row.items()} for row in raw]


def sanitize_df_for_mongo(df):
    # No-op passthrough — real sanitization happens inside df_to_mongo_records.
    return df


def sanitize_collection_name(filename):
    name = filename.rsplit(".", 1)[0]
    name = re.sub(r'[\/\.\$]', '_', name)
    return name[:120]


def insert_inspection_data(df):
    """Insert merged query results into visual_data.inspection_data."""
    if df is None or df.empty:
        print("[INFO] No inspection data to insert.")
        return
    try:
        vis_db   = client[visual_data]
        vis_coll = vis_db[inspection_data]
        records  = df_to_mongo_records(df)
        if records:
            # Upsert by barcode to avoid duplicates on re-run
            for i in range(0, len(records), 5000):
                chunk = records[i:i + 5000]
                barcodes = [r.get('barcode') for r in chunk if r.get('barcode')]
                if barcodes:
                    vis_coll.delete_many({"barcode": {"$in": barcodes}})
                vis_coll.insert_many(chunk, ordered=False)
            print(f"[INFO] Inserted {len(records)} records into {visual_data}.{inspection_data}")
        else:
            print("[INFO] No records to insert into inspection_data.")
    except Exception as e:
        print(f"[ERROR] Failed to insert into inspection_data: {e}")


insert_inspection_data(df_tub)

# df_cimcorp is no longer a separate query — set to empty DataFrame
df_cimcorp = pd.DataFrame()


# ── Unique index for deduplication ──────────────────────────────────────────
UNIQUE_INDEX_FIELDS = [
    ("barcode", 1), ("uf_time", 1), ("ptb_time", 1), ("cur_time", 1),
    ("gt_weighting_time", 1), ("last_change_time", 1),
    ("sur_repair_time", 1), ("tire_barcode", 1)
]
UNIQUE_INDEX_NAME = "idx_unique_duplicate_detection"


def ensure_unique_index(collection):
    """Create the deduplication index if it doesn't already exist."""
    existing = collection.index_information()
    if UNIQUE_INDEX_NAME not in existing:
        try:
            collection.create_index(
                UNIQUE_INDEX_FIELDS,
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
    """Insert with ordered=False — MongoDB skips duplicates automatically."""
    total    = len(records)
    inserted = 0
    for i in range(0, total, chunk_size):
        chunk = records[i:i + chunk_size]
        try:
            result    = collection.insert_many(chunk, ordered=False)
            inserted += len(result.inserted_ids)
        except pymongo.errors.BulkWriteError as bwe:
            inserted += bwe.details['nInserted']
    return inserted


def has_special_chars(value):
    if isinstance(value, str):
        return bool(re.search(r'[^0-9]', value))
    return False


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


# =====================================================
# CHANGED: mark_rerun_and_last now uses ptb_time
# =====================================================
def mark_rerun_and_last(df):
    """
    Mark rerun and last for each barcode within the same day based on ptb_time.
    
    Rules:
    - 1 occurrence: rerun=0, last=1
    - 2 occurrences: 
        - First: rerun=0, last=0
        - Last: rerun=1, last=1
    - 3+ occurrences:
        - First: rerun=0, last=-1
        - Middle: rerun=1, last=0
        - Last: rerun=1, last=1
    
    Args:
        df: DataFrame with 'barcode' and 'ptb_time' columns
        
    Returns:
        DataFrame with 'rerun' and 'last' columns added
        
    Examples:
        # 1 occurrence
        ptb_time     barcode    rerun    last
        2:00        123        0        1
        
        # 2 occurrences
        ptb_time     barcode    rerun    last
        2:00        123        0        0
        2:30        123        1        1
        
        # 3 occurrences
        ptb_time     barcode    rerun    last
        2:00        123        0        -1
        2:30        123        1        0
        3:00        123        1        1
        
        # 4 occurrences
        ptb_time     barcode    rerun    last
        2:00        123        0        -1
        2:30        123        1        0
        3:00        123        1        0
        3:30        123        1        1
    """
    if df.empty:
        df['rerun'] = 0
        df['last'] = 1
        return df
    
    # Extract date from ptb_time
    def extract_date(ptb_time):
        if pd.isna(ptb_time):
            return None
        try:
            dt = pd.to_datetime(ptb_time)
            return dt.strftime('%Y-%m-%d')
        except:
            return None
    
    df_copy = df.copy()
    df_copy['ptb_date'] = df_copy['ptb_time'].apply(extract_date)
    
    # Initialize columns
    df_copy['rerun'] = 0
    df_copy['last'] = 1  # Default for single occurrences
    
    # Group by barcode and date
    grouped = df_copy.groupby(['barcode', 'ptb_date'])
    
    for (barcode, date), group_indices in grouped.groups.items():
        if date is None or barcode is None:
            continue
            
        # Get sorted indices by ptb_time (chronological order)
        group_df = df_copy.loc[group_indices].sort_values('ptb_time')
        indices = group_df.index.tolist()
        count = len(indices)
        
        if count == 1:
            # Only one occurrence - set rerun=0, last=1
            df_copy.loc[indices[0], 'rerun'] = 0
            df_copy.loc[indices[0], 'last'] = 1
        elif count == 2:
            # Two occurrences
            for i, idx in enumerate(indices):
                if i == 0:
                    # First occurrence
                    df_copy.loc[idx, 'rerun'] = 0
                    df_copy.loc[idx, 'last'] = 0
                else:
                    # Last occurrence
                    df_copy.loc[idx, 'rerun'] = 1
                    df_copy.loc[idx, 'last'] = 1
        else:
            # 3 or more occurrences
            for i, idx in enumerate(indices):
                if i == 0:
                    # First occurrence
                    df_copy.loc[idx, 'rerun'] = 0
                    df_copy.loc[idx, 'last'] = -1
                elif i == count - 1:
                    # Last occurrence (most recent)
                    df_copy.loc[idx, 'rerun'] = 1
                    df_copy.loc[idx, 'last'] = 1
                else:
                    # Middle occurrences
                    df_copy.loc[idx, 'rerun'] = 1
                    df_copy.loc[idx, 'last'] = 0
    
    # Drop temporary column
    df_copy = df_copy.drop('ptb_date', axis=1)
    
    return df_copy


def clean_tub_data(df):
    output_messages = []
    invalid_rows = pd.DataFrame()
    logging.info("clean_tub_data function is just started")
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Expected a pandas DataFrame as input")
    df_clean = df.copy()

    if 'barcode' in df_clean.columns:
        df_clean['barcode'] = df_clean['barcode'].astype(str)

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
    existing_rename_mapping = {k: v for k, v in rename_columns_mapping.items() if k in df_clean.columns}
    df_clean.rename(columns=existing_rename_mapping, inplace=True)

    duplicates = df_clean.columns[df_clean.columns.duplicated(keep=False)].unique()
    if len(duplicates) > 0:
        logging.warning(f"Duplicate column names in input: {list(duplicates)}. Renaming duplicates.")
        cols = pd.Series(df_clean.columns)
        for dup in duplicates:
            cols[cols[cols == dup].index.values.tolist()] = [
                f"{dup}_{i}" if i != 0 else dup for i in range(sum(cols == dup))
            ]
        df_clean.columns = cols

    mask = df_clean['barcode'].isnull() | df_clean['barcode'].apply(has_special_chars)
    invalid_rows = df_clean[mask]
    df_clean = df_clean[~mask].drop_duplicates()
    df_clean.drop(columns=['rn', 'Unnamed: 226', 'Unnamed: 225'], inplace=True, errors='ignore')
    df_clean.columns = df_clean.columns.to_series().str.strip().str.lower().str.replace(' ', '_')

    return df_clean, invalid_rows, output_messages


def clean_visual_data(df):
    output_messages = []
    invalid_barcode_rows = pd.DataFrame()
    duplicates_df = pd.DataFrame()
    logging.info("clean_visual_data function is just started")

    try:
        df.columns = [re.sub(r'[.\s]+', '_', col.strip()).lower() for col in df.columns]

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
                df = df.rename(columns={old_name: new_name}, inplace=False)

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

        df = df[~df.index.isin(invalid_barcode_rows.index)]
        df = df.dropna(axis=1, how='all')
        duplicates_df = df[df.duplicated(subset=['barcode'], keep=False)]
        df = df.drop_duplicates(subset=['barcode'])

        output_messages.append("CIMCORP_HISTORICAL_DATA Cleaning successful")
        logging.info("clean_visual_data function is completed")
        return df, invalid_barcode_rows, duplicates_df, output_messages

    except Exception as e:
        error_message = f"Error during cleaning: {e}"
        output_messages.append(error_message)
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), output_messages


def _upsert_records(collection, records, key_field):
    """Fastest bulk write: chunked delete+insert_many."""
    if not records:
        return
    CHUNK = 10000
    keys = [rec[key_field] for rec in records if rec.get(key_field) is not None]
    for i in range(0, len(keys), CHUNK):
        collection.delete_many({key_field: {"$in": keys[i:i+CHUNK]}})
    for i in range(0, len(records), CHUNK):
        collection.insert_many(records[i:i+CHUNK], ordered=False)


def _upsert_scrap_records(collection, records, key_cols):
    """Fastest scrap bulk write: chunked delete+insert_many."""
    if not records or not key_cols:
        return
    CHUNK = 10000
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


# # =====================================================
# # CHANGED: copy_to_single_collection_api now handles null ptb_time
# # =====================================================
# def copy_to_single_collection_api(df_summary=None, df_scrap=None):
#     """Write cleaned data to inspection_data collection with rerun and last columns.
#        Also stores records with null ptb_time to All_SLOBS_DATA.tbm_time_null_data
#     """
#     messages = []
    
#     # Separate records with null ptb_time
#     null_ptb_records = pd.DataFrame()
#     has_ptb_records = pd.DataFrame()
    
#     if df_summary is not None and not df_summary.empty:
#         # Check if ptb_time column exists
#         if 'ptb_time' in df_summary.columns:
#             # Split into null and non-null ptb_time
#             null_ptb_mask = df_summary['ptb_time'].isnull() | (df_summary['ptb_time'] == '')
#             null_ptb_records = df_summary[null_ptb_mask].copy()
#             has_ptb_records = df_summary[~null_ptb_mask].copy()
            
#             # Log the split
#             logging.info(f"Records with null ptb_time: {len(null_ptb_records)}")
#             logging.info(f"Records with ptb_time: {len(has_ptb_records)}")
#             print(f"Records with null ptb_time: {len(null_ptb_records)}")
#             print(f"Records with ptb_time: {len(has_ptb_records)}")
#         else:
#             # If ptb_time doesn't exist, treat all as having ptb_time
#             has_ptb_records = df_summary.copy()
#             logging.warning("ptb_time column not found in dataframe")
    
#     # Write summary data for records WITH ptb_time
#     if not has_ptb_records.empty:
#         try:
#             # Apply rerun and last marking using ptb_time
#             has_ptb_records = mark_rerun_and_last(has_ptb_records)
            
#             # Log the rerun/last distribution for debugging
#             if 'rerun' in has_ptb_records.columns and 'last' in has_ptb_records.columns:
#                 rerun_counts = has_ptb_records['rerun'].value_counts().to_dict()
#                 last_counts = has_ptb_records['last'].value_counts().to_dict()
#                 logging.info(f"Rerun distribution (with ptb_time): {rerun_counts}")
#                 logging.info(f"Last distribution (with ptb_time): {last_counts}")
#                 print(f"Rerun distribution (with ptb_time): {rerun_counts}")
#                 print(f"Last distribution (with ptb_time): {last_counts}")
            
#             vis_db = client[visual_data]
#             vis_coll = vis_db[inspection_data]
#             records = df_to_mongo_records(has_ptb_records)
            
#             if records:
#                 # Delete existing records with same barcode in time range
#                 barcodes = [r.get('barcode') for r in records if r.get('barcode')]
#                 if barcodes:
#                     vis_coll.delete_many({"barcode": {"$in": barcodes}})
                
#                 # Insert in chunks
#                 for i in range(0, len(records), 5000):
#                     chunk = records[i:i+5000]
#                     vis_coll.insert_many(chunk, ordered=False)
                
#                 messages.append(f"Inserted {len(records)} records into {visual_data}.{inspection_data} with rerun/last columns")
#                 logging.info(f"Inserted {len(records)} records into inspection_data with rerun/last columns")
#                 print(f"Inserted {len(records)} records into inspection_data with rerun/last columns")
#             else:
#                 messages.append("No records to insert into inspection_data")
#         except Exception as e:
#             error_msg = f"Error inserting summary data: {e}"
#             messages.append(error_msg)
#             logging.error(error_msg)
#             print(error_msg)
    
#     # =====================================================
#     # NEW: Store records with null ptb_time in tbm_time_null_data
#     # =====================================================
#     if not null_ptb_records.empty:
#         try:
#             # Connect to All_SLOBS_DATA database and tbm_time_null_data collection
#             null_db = client[All_SLOBS_DATA]
#             null_coll = null_db["tbm_time_null_data"]
            
#             # Convert to records
#             records = df_to_mongo_records(null_ptb_records)
            
#             if records:
#                 # Delete existing records with same barcode
#                 barcodes = [r.get('barcode') for r in records if r.get('barcode')]
#                 if barcodes:
#                     null_coll.delete_many({"barcode": {"$in": barcodes}})
                
#                 # Insert in chunks
#                 for i in range(0, len(records), 5000):
#                     chunk = records[i:i+5000]
#                     null_coll.insert_many(chunk, ordered=False)
                
#                 messages.append(f"Inserted {len(records)} records into All_SLOBS_DATA.tbm_time_null_data (null ptb_time)")
#                 logging.info(f"Inserted {len(records)} records into tbm_time_null_data (null ptb_time)")
#                 print(f"Inserted {len(records)} records into tbm_time_null_data (null ptb_time)")
#             else:
#                 messages.append("No records to insert into tbm_time_null_data")
#         except Exception as e:
#             error_msg = f"Error inserting null ptb_time data: {e}"
#             messages.append(error_msg)
#             logging.error(error_msg)
#             print(error_msg)
    
#     # Write scrap data if available (unchanged)
#     if df_scrap is not None and not df_scrap.empty:
#         try:
#             scrap_db = client["All_SLOBS_DATA"]
#             scrap_coll = scrap_db["scrap"]
            
#             # Ensure tire_barcode exists as key
#             if 'tire_barcode' not in df_scrap.columns and 'barcode' in df_scrap.columns:
#                 df_scrap['tire_barcode'] = df_scrap['barcode']
            
#             records = df_to_mongo_records(df_scrap)
#             if records:
#                 # Delete existing scrap records
#                 barcodes = [r.get('tire_barcode') for r in records if r.get('tire_barcode')]
#                 if barcodes:
#                     scrap_coll.delete_many({"tire_barcode": {"$in": barcodes}})
                
#                 # Insert in chunks
#                 for i in range(0, len(records), 5000):
#                     chunk = records[i:i+5000]
#                     scrap_coll.insert_many(chunk, ordered=False)
                
#                 messages.append(f"Inserted {len(records)} records into All_SLOBS_DATA.scrap")
#                 logging.info(f"Inserted {len(records)} records into scrap collection")
#         except Exception as e:
#             error_msg = f"Error inserting scrap data: {e}"
#             messages.append(error_msg)
#             logging.error(error_msg)
    
#     return messages

def copy_to_single_collection_api(df_summary=None, df_scrap=None):
    """
    Write cleaned data to inspection_data collection with rerun and last columns.
    Records with null ptb_time are stored separately in All_SLOBS_DATA.tbm_time_null_data
    """
    messages = []
    
    # Separate records with null ptb_time
    null_ptb_records = pd.DataFrame()
    has_ptb_records = pd.DataFrame()
    
    if df_summary is not None and not df_summary.empty:
        # Check if ptb_time column exists
        if 'ptb_time' in df_summary.columns:
            # Split into null and non-null ptb_time
            # Check for null, empty string, or 'None' values
            null_ptb_mask = (
                df_summary['ptb_time'].isnull() | 
                (df_summary['ptb_time'] == '') |
                (df_summary['ptb_time'].astype(str).str.lower() == 'none')
            )
            null_ptb_records = df_summary[null_ptb_mask].copy()
            has_ptb_records = df_summary[~null_ptb_mask].copy()
            
            # Log the split
            logging.info(f"Records with null ptb_time: {len(null_ptb_records)}")
            logging.info(f"Records with ptb_time: {len(has_ptb_records)}")
            print(f"Records with null ptb_time: {len(null_ptb_records)}")
            print(f"Records with ptb_time: {len(has_ptb_records)}")
            
            # Log sample of null ptb_time records for debugging
            if len(null_ptb_records) > 0:
                sample_barcodes = null_ptb_records['barcode'].head(5).tolist()
                logging.info(f"Sample barcodes with null ptb_time: {sample_barcodes}")
                print(f"Sample barcodes with null ptb_time: {sample_barcodes}")
        else:
            # If ptb_time doesn't exist, treat all as having ptb_time
            has_ptb_records = df_summary.copy()
            logging.warning("ptb_time column not found in dataframe")
            print("Warning: ptb_time column not found in dataframe")
    
    # =====================================================
    # Store records with null ptb_time in tbm_time_null_data
    # =====================================================
    if not null_ptb_records.empty:
        try:
            # Connect to All_SLOBS_DATA database and tbm_time_null_data collection
            null_db = client[All_SLOBS_DATA]
            null_coll = null_db["tbm_time_null_data"]
            
            # Convert to records
            records = df_to_mongo_records(null_ptb_records)
            
            if records:
                # Delete existing records with same barcode (to avoid duplicates on re-run)
                barcodes = [r.get('barcode') for r in records if r.get('barcode')]
                if barcodes:
                    delete_result = null_coll.delete_many({"barcode": {"$in": barcodes}})
                    logging.info(f"Deleted {delete_result.deleted_count} existing records from tbm_time_null_data")
                    print(f"Deleted {delete_result.deleted_count} existing records from tbm_time_null_data")
                
                # Insert in chunks
                total_inserted = 0
                for i in range(0, len(records), 5000):
                    chunk = records[i:i+5000]
                    try:
                        result = null_coll.insert_many(chunk, ordered=False)
                        total_inserted += len(result.inserted_ids)
                    except pymongo.errors.BulkWriteError as bwe:
                        inserted_count = bwe.details.get('nInserted', 0)
                        total_inserted += inserted_count
                        logging.warning(f"Bulk write error in tbm_time_null_data: {bwe}")
                
                messages.append(f"Inserted {total_inserted} records into All_SLOBS_DATA.tbm_time_null_data (null ptb_time)")
                logging.info(f"Inserted {total_inserted} records into tbm_time_null_data (null ptb_time)")
                print(f"Inserted {total_inserted} records into tbm_time_null_data (null ptb_time)")
                
                # Note: These records are EXCLUDED from the main inspection_data collection
                messages.append(f"Records with null ptb_time are EXCLUDED from {visual_data}.{inspection_data}")
                logging.info(f"Records with null ptb_time are EXCLUDED from inspection_data")
                print(f"Records with null ptb_time are EXCLUDED from inspection_data")
            else:
                messages.append("No records to insert into tbm_time_null_data")
        except Exception as e:
            error_msg = f"Error inserting null ptb_time data: {e}"
            messages.append(error_msg)
            logging.error(error_msg)
            print(error_msg)
    
    # =====================================================
    # Write summary data for records WITH ptb_time ONLY
    # =====================================================
    if not has_ptb_records.empty:
        try:
            # Apply rerun and last marking using ptb_time
            has_ptb_records = mark_rerun_and_last(has_ptb_records)
            
            # Log the rerun/last distribution for debugging
            if 'rerun' in has_ptb_records.columns and 'last' in has_ptb_records.columns:
                rerun_counts = has_ptb_records['rerun'].value_counts().to_dict()
                last_counts = has_ptb_records['last'].value_counts().to_dict()
                logging.info(f"Rerun distribution (with ptb_time): {rerun_counts}")
                logging.info(f"Last distribution (with ptb_time): {last_counts}")
                print(f"Rerun distribution (with ptb_time): {rerun_counts}")
                print(f"Last distribution (with ptb_time): {last_counts}")
            
            vis_db = client[visual_data]
            vis_coll = vis_db[inspection_data]
            records = df_to_mongo_records(has_ptb_records)
            
            if records:
                # Delete existing records with same barcode in time range
                barcodes = [r.get('barcode') for r in records if r.get('barcode')]
                if barcodes:
                    delete_result = vis_coll.delete_many({"barcode": {"$in": barcodes}})
                    logging.info(f"Deleted {delete_result.deleted_count} existing records from inspection_data")
                
                # Insert in chunks
                total_inserted = 0
                for i in range(0, len(records), 5000):
                    chunk = records[i:i+5000]
                    try:
                        result = vis_coll.insert_many(chunk, ordered=False)
                        total_inserted += len(result.inserted_ids)
                    except pymongo.errors.BulkWriteError as bwe:
                        inserted_count = bwe.details.get('nInserted', 0)
                        total_inserted += inserted_count
                        logging.warning(f"Bulk write error in inspection_data: {bwe}")
                
                messages.append(f"Inserted {total_inserted} records into {visual_data}.{inspection_data} with rerun/last columns")
                logging.info(f"Inserted {total_inserted} records into inspection_data with rerun/last columns")
                print(f"Inserted {total_inserted} records into inspection_data with rerun/last columns")
            else:
                messages.append("No records with ptb_time to insert into inspection_data")
        except Exception as e:
            error_msg = f"Error inserting summary data: {e}"
            messages.append(error_msg)
            logging.error(error_msg)
            print(error_msg)
    
    # Write scrap data if available (unchanged)
    if df_scrap is not None and not df_scrap.empty:
        try:
            scrap_db = client["All_SLOBS_DATA"]
            scrap_coll = scrap_db["scrap"]
            
            # Ensure tire_barcode exists as key
            if 'tire_barcode' not in df_scrap.columns and 'barcode' in df_scrap.columns:
                df_scrap['tire_barcode'] = df_scrap['barcode']
            
            records = df_to_mongo_records(df_scrap)
            if records:
                # Delete existing scrap records
                barcodes = [r.get('tire_barcode') for r in records if r.get('tire_barcode')]
                if barcodes:
                    scrap_coll.delete_many({"tire_barcode": {"$in": barcodes}})
                
                # Insert in chunks
                total_inserted = 0
                for i in range(0, len(records), 5000):
                    chunk = records[i:i+5000]
                    try:
                        result = scrap_coll.insert_many(chunk, ordered=False)
                        total_inserted += len(result.inserted_ids)
                    except pymongo.errors.BulkWriteError as bwe:
                        inserted_count = bwe.details.get('nInserted', 0)
                        total_inserted += inserted_count
                
                messages.append(f"Inserted {total_inserted} records into All_SLOBS_DATA.scrap")
                logging.info(f"Inserted {total_inserted} records into scrap collection")
        except Exception as e:
            error_msg = f"Error inserting scrap data: {e}"
            messages.append(error_msg)
            logging.error(error_msg)
    
    return messages

# =====================================================
# CHANGED: AGGREGATION FUNCTION: Group by ptb_time date
# =====================================================
# def aggregate_and_store_by_date_sku():
#     """
#     Read data from visual_data.inspection_data, aggregate by ptb_time date only,
#     and store in All_SLOBS_DATA.active_sku_list.
#     Each document represents a unique ptb_time date with all SKUs and their respective barcodes.
#     """
#     logging.info("Starting aggregation by ptb_time date with all SKUs...")
    
#     try:
#         # Connect to the source database
#         source_db = client[visual_data]
#         source_collection = source_db[inspection_data]
        
#         # Connect to the target database
#         target_db = client[All_SLOBS_DATA]
#         target_collection = target_db[active_sku_list]
        
#         # Step 1: Aggregate by ptb_time date and sku first
#         pipeline = [
#             {
#                 "$match": {
#                     "ptb_time": {"$exists": True, "$ne": None, "$ne": ""},
#                     "sku": {"$exists": True, "$ne": None, "$ne": ""}
#                 }
#             },
#             {
#                 "$addFields": {
#                     "ptb_date": {
#                         "$dateToString": {
#                             "format": "%Y-%m-%d",
#                             "date": {"$toDate": "$ptb_time"}
#                         }
#                     }
#                 }
#             },
#             {
#                 "$group": {
#                     "_id": {
#                         "ptb_date": "$ptb_date",
#                         "sku": "$sku"
#                     },
#                     "barcodes": {
#                         "$push": {
#                             "barcode": "$barcode",
#                             "ptb_time": "$ptb_time",
#                             "ptb_id": "$ptb_id",
#                             "gt_code": "$gt_code",
#                             "cur_id": "$cur_id",
#                             "cur_mouldid": "$cur_mouldid",
#                             "cur_time": "$cur_time",
#                             "re_comment": "$re_comment",
#                             "sur_repair_time": "$sur_repair_time",
#                             "visualinspection_time_tub": "$visualinspection_time_tub",
#                             "last_change_time": "$last_change_time",
#                             "re_code": "$re_code",
#                             "uf_id": "$uf_id",
#                             "uf_time": "$uf_time",
#                             "rerun": "$rerun",
#                             "last": "$last"
#                         }
#                     }
#                 }
#             },
#             {
#                 "$group": {
#                     "_id": "$_id.ptb_date",
#                     "skus": {
#                         "$push": {
#                             "sku": "$_id.sku",
#                             "barcodes": "$barcodes"
#                         }
#                     }
#                 }
#             },
#             {
#                 "$project": {
#                     "_id": 0,
#                     "ptb_time": "$_id",
#                     "skus": 1
#                 }
#             },
#             {
#                 "$sort": {
#                     "ptb_time": 1
#                 }
#             }
#         ]
        
#         # Execute aggregation
#         logging.info("Executing aggregation pipeline...")
#         aggregated_results = list(source_collection.aggregate(pipeline, allowDiskUse=True))
        
#         if not aggregated_results:
#             logging.info("No data found to aggregate.")
#             return {
#                 "status": "success",
#                 "message": "No data found to aggregate.",
#                 "records_processed": 0,
#                 "records_inserted": 0
#             }
        
#         logging.info(f"Found {len(aggregated_results)} unique ptb_time dates to process.")
        
#         # Get unique dates from results for deletion
#         dates = list(set([doc['ptb_time'] for doc in aggregated_results]))
        
#         # Delete existing data for the dates being inserted
#         if dates:
#             delete_result = target_collection.delete_many({
#                 "ptb_time": {"$in": dates}
#             })
#             logging.info(f"Deleted {delete_result.deleted_count} existing records for dates: {dates}")
        
#         # Insert new aggregated data in chunks
#         total_inserted = 0
#         chunk_size = 1000
        
#         for i in range(0, len(aggregated_results), chunk_size):
#             chunk = aggregated_results[i:i+chunk_size]
#             try:
#                 result = target_collection.insert_many(chunk, ordered=False)
#                 total_inserted += len(result.inserted_ids)
#                 logging.info(f"Inserted chunk {i//chunk_size + 1} with {len(result.inserted_ids)} records")
#             except pymongo.errors.BulkWriteError as bwe:
#                 inserted_count = bwe.details.get('nInserted', 0)
#                 total_inserted += inserted_count
#                 logging.warning(f"Bulk write error in chunk {i//chunk_size + 1}: {bwe}")
#                 logging.warning(f"Inserted {inserted_count} out of {len(chunk)} documents in this chunk")
        
#         logging.info(f"Successfully inserted {total_inserted} aggregated records into {All_SLOBS_DATA}.{active_sku_list}")
        
#         return {
#             "status": "success",
#             "message": f"Successfully aggregated {len(aggregated_results)} ptb_time dates.",
#             "records_processed": len(aggregated_results),
#             "records_inserted": total_inserted,
#             "dates": dates,
#             "date_count": len(dates)
#         }
        
#     except Exception as e:
#         error_msg = f"Error in aggregate_and_store_by_date_sku: {str(e)}"
#         logging.error(error_msg)
#         logging.error(traceback.format_exc())
#         return {
#             "status": "error",
#             "message": error_msg,
#             "records_processed": 0,
#             "records_inserted": 0
#         }

def aggregate_and_store_by_date_sku():
    """
    Read data from visual_data.inspection_data, aggregate by ptb_time date only,
    and store in All_SLOBS_DATA.active_sku_list.
    NOTE: Only records with ptb_time are included in this aggregation.
    Records with null ptb_time are stored separately in tbm_time_null_data.
    """
    logging.info("Starting aggregation by ptb_time date with all SKUs...")
    
    try:
        # Connect to the source database
        source_db = client[visual_data]
        source_collection = source_db[inspection_data]
        
        # Connect to the target database
        target_db = client[All_SLOBS_DATA]
        target_collection = target_db[active_sku_list]
        
        # Step 1: Aggregate by ptb_time date and sku first
        # This automatically excludes records with null ptb_time
        pipeline = [
            {
                "$match": {
                    "ptb_time": {"$exists": True, "$ne": None, "$ne": "", "$type": "string"},
                    "sku": {"$exists": True, "$ne": None, "$ne": ""}
                }
            },
            {
                "$addFields": {
                    "ptb_date": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": {"$toDate": "$ptb_time"}
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "ptb_date": "$ptb_date",
                        "sku": "$sku"
                    },
                    "barcodes": {
                        "$push": {
                            "barcode": "$barcode",
                            "ptb_time": "$ptb_time",
                            "ptb_id": "$ptb_id",
                            "gt_code": "$gt_code",
                            "cur_id": "$cur_id",
                            "cur_mouldid": "$cur_mouldid",
                            "cur_time": "$cur_time",
                            "re_comment": "$re_comment",
                            "sur_repair_time": "$sur_repair_time",
                            "visualinspection_time_tub": "$visualinspection_time_tub",
                            "last_change_time": "$last_change_time",
                            "re_code": "$re_code",
                            "uf_id": "$uf_id",
                            "uf_time": "$uf_time",
                            "rerun": "$rerun",
                            "last": "$last"
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": "$_id.ptb_date",
                    "skus": {
                        "$push": {
                            "sku": "$_id.sku",
                            "barcodes": "$barcodes"
                        }
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "ptb_time": "$_id",
                    "skus": 1
                }
            },
            {
                "$sort": {
                    "ptb_time": 1
                }
            }
        ]
        
        # Execute aggregation
        logging.info("Executing aggregation pipeline (excluding null ptb_time)...")
        aggregated_results = list(source_collection.aggregate(pipeline, allowDiskUse=True))
        
        if not aggregated_results:
            logging.info("No data found to aggregate (all records have null ptb_time).")
            return {
                "status": "success",
                "message": "No data found to aggregate (all records have null ptb_time).",
                "records_processed": 0,
                "records_inserted": 0
            }
        
        logging.info(f"Found {len(aggregated_results)} unique ptb_time dates to process.")
        
        # Get unique dates from results for deletion
        dates = list(set([doc['ptb_time'] for doc in aggregated_results]))
        
        # Delete existing data for the dates being inserted
        if dates:
            delete_result = target_collection.delete_many({
                "ptb_time": {"$in": dates}
            })
            logging.info(f"Deleted {delete_result.deleted_count} existing records for dates: {dates}")
        
        # Insert new aggregated data in chunks
        total_inserted = 0
        chunk_size = 1000
        
        for i in range(0, len(aggregated_results), chunk_size):
            chunk = aggregated_results[i:i+chunk_size]
            try:
                result = target_collection.insert_many(chunk, ordered=False)
                total_inserted += len(result.inserted_ids)
                logging.info(f"Inserted chunk {i//chunk_size + 1} with {len(result.inserted_ids)} records")
            except pymongo.errors.BulkWriteError as bwe:
                inserted_count = bwe.details.get('nInserted', 0)
                total_inserted += inserted_count
                logging.warning(f"Bulk write error in chunk {i//chunk_size + 1}: {bwe}")
                logging.warning(f"Inserted {inserted_count} out of {len(chunk)} documents in this chunk")
        
        logging.info(f"Successfully inserted {total_inserted} aggregated records into {All_SLOBS_DATA}.{active_sku_list}")
        
        return {
            "status": "success",
            "message": f"Successfully aggregated {len(aggregated_results)} ptb_time dates.",
            "records_processed": len(aggregated_results),
            "records_inserted": total_inserted,
            "dates": dates,
            "date_count": len(dates)
        }
        
    except Exception as e:
        error_msg = f"Error in aggregate_and_store_by_date_sku: {str(e)}"
        logging.error(error_msg)
        logging.error(traceback.format_exc())
        return {
            "status": "error",
            "message": error_msg,
            "records_processed": 0,
            "records_inserted": 0
        }

# =====================================================
# CHANGED: AGGREGATE BY ptb_time MONTH (YYYY-MM)
# =====================================================
def aggregate_and_store_by_ptb_month():
    """
    Aggregate by ptb_time month (YYYY-MM) instead of date.
    Useful for monthly reporting.
    """
    logging.info("Starting aggregation by ptb_time month with all SKUs...")
    
    try:
        # Connect to the source database
        source_db = client[visual_data]
        source_collection = source_db[inspection_data]
        
        # Connect to the target database
        target_db = client[All_SLOBS_DATA]
        target_collection = target_db[active_sku_list]
        
        pipeline = [
            {
                "$match": {
                    "ptb_time": {"$exists": True, "$ne": None, "$ne": ""},
                    "sku": {"$exists": True, "$ne": None, "$ne": ""}
                }
            },
            {
                "$addFields": {
                    "ptb_month": {
                        "$dateToString": {
                            "format": "%Y-%m",
                            "date": {"$toDate": "$ptb_time"}
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "ptb_month": "$ptb_month",
                        "sku": "$sku"
                    },
                    "barcodes": {
                        "$push": {
                            "barcode": "$barcode",
                            "ptb_time": "$ptb_time",
                            "ptb_id": "$ptb_id",
                            "gt_code": "$gt_code",
                            "cur_id": "$cur_id",
                            "cur_mouldid": "$cur_mouldid",
                            "cur_time": "$cur_time",
                            "re_comment": "$re_comment",
                            "sur_repair_time": "$sur_repair_time",
                            "visualinspection_time_tub": "$visualinspection_time_tub",
                            "last_change_time": "$last_change_time",
                            "re_code": "$re_code",
                            "uf_id": "$uf_id",
                            "uf_time": "$uf_time",
                            "rerun": "$rerun",
                            "last": "$last"
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": "$_id.ptb_month",
                    "skus": {
                        "$push": {
                            "sku": "$_id.sku",
                            "barcodes": "$barcodes"
                        }
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "ptb_time": "$_id",
                    "skus": 1
                }
            },
            {
                "$sort": {
                    "ptb_time": 1
                }
            }
        ]
        
        logging.info("Executing monthly aggregation pipeline...")
        aggregated_results = list(source_collection.aggregate(pipeline, allowDiskUse=True))
        
        if not aggregated_results:
            logging.info("No data found to aggregate by month.")
            return {
                "status": "success",
                "message": "No data found to aggregate.",
                "records_processed": 0,
                "records_inserted": 0
            }
        
        logging.info(f"Found {len(aggregated_results)} unique ptb_time months to process.")
        
        # Get unique months from results for deletion
        months = list(set([doc['ptb_time'] for doc in aggregated_results]))
        
        # Delete existing data for the months being inserted
        if months:
            delete_result = target_collection.delete_many({
                "ptb_time": {"$in": months}
            })
            logging.info(f"Deleted {delete_result.deleted_count} existing records for months: {months}")
        
        # Insert new aggregated data
        total_inserted = 0
        chunk_size = 1000
        
        for i in range(0, len(aggregated_results), chunk_size):
            chunk = aggregated_results[i:i+chunk_size]
            try:
                result = target_collection.insert_many(chunk, ordered=False)
                total_inserted += len(result.inserted_ids)
            except pymongo.errors.BulkWriteError as bwe:
                inserted_count = bwe.details.get('nInserted', 0)
                total_inserted += inserted_count
                logging.warning(f"Bulk write error: {bwe}")
        
        logging.info(f"Successfully inserted {total_inserted} aggregated monthly records")
        
        return {
            "status": "success",
            "message": f"Successfully aggregated {len(aggregated_results)} months.",
            "records_processed": len(aggregated_results),
            "records_inserted": total_inserted,
            "months": months,
            "month_count": len(months)
        }
        
    except Exception as e:
        error_msg = f"Error in aggregate_and_store_by_ptb_month: {str(e)}"
        logging.error(error_msg)
        logging.error(traceback.format_exc())
        return {
            "status": "error",
            "message": error_msg,
            "records_processed": 0,
            "records_inserted": 0
        }


# =====================================================
# CREATE INDEXES FOR TARGET COLLECTION
# =====================================================
# def create_indexes_for_aggregation():
#     """
#     Create indexes on the target collection for better query performance
#     """
#     try:
#         target_db = client[All_SLOBS_DATA]
#         target_collection = target_db[active_sku_list]
        
#         # Create indexes
#         target_collection.create_index([("ptb_time", 1)], unique=True, background=True)
        
#         # Also create index for null ptb_time collection
#         null_collection = target_db["tbm_time_null_data"]
#         null_collection.create_index([("barcode", 1)], unique=True, background=True)
#         null_collection.create_index([("ptb_time", 1)], background=True)
        
#         logging.info(f"Indexes created on {All_SLOBS_DATA}.{active_sku_list} and tbm_time_null_data")
#         return True
#     except Exception as e:
#         logging.error(f"Error creating indexes: {e}")
#         return False

def create_indexes_for_aggregation():
    """
    Create indexes on the target collections for better query performance
    """
    try:
        target_db = client[All_SLOBS_DATA]
        
        # Index for active_sku_list (on ptb_time)
        target_collection = target_db[active_sku_list]
        target_collection.create_index([("ptb_time", 1)], unique=True, background=True)
        logging.info(f"Index created on {All_SLOBS_DATA}.{active_sku_list}.ptb_time")
        
        # Index for tbm_time_null_data (on barcode and ptb_time)
        null_collection = target_db["tbm_time_null_data"]
        null_collection.create_index([("barcode", 1)], unique=True, background=True)
        null_collection.create_index([("ptb_time", 1)], background=True)
        null_collection.create_index([("uf_time", 1)], background=True)
        logging.info(f"Indexes created on All_SLOBS_DATA.tbm_time_null_data")
        
        return True
    except Exception as e:
        logging.error(f"Error creating indexes: {e}")
        return False

# =====================================================
# UPDATE run_data_cleaning function to use ptb_time
# =====================================================
def run_data_cleaning(df_tub_input, df_cimcorp_input):
    """
    Cleans df_tub_input and df_cimcorp_input in-memory and writes
    directly to the final merged DB. No intermediate DBs are used.
    """
    start_time = time.time()
    output_messages = []
    logging.info("run_data_cleaning function started")
    try:
        cleaned_summary, invalid_df, tub_msgs = clean_tub_data(df_tub_input.copy())
        output_messages.extend(tub_msgs)
        if not invalid_df.empty:
            invalid_df = invalid_df.drop('_id', axis=1, errors='ignore')
            client[INVALID_ROWS_DB]["invalid_data_TUB_home"].insert_many(df_to_mongo_records(invalid_df))
            output_messages.append(f"Saved {len(invalid_df)} invalid TUB rows to INVALID_ROWS.invalid_data_TUB_home")

        # Convert datetime columns to "YYYY-MM-DD HH:MM:SS" string format
        def _fmt_dt(x):
            """Convert any datetime/timestamp to 'YYYY-MM-DD HH:MM:SS' string. None if null."""
            if x is None or x is pd.NaT:
                return None
            if isinstance(x, float) and pd.isna(x):
                return None
            try:
                ts = pd.to_datetime(x, errors='coerce', utc=True)
                if pd.isna(ts):
                    ts = pd.to_datetime(x, errors='coerce')
                if pd.isna(ts):
                    return None
                # Convert to UTC-naive then format
                if ts.tzinfo is not None:
                    ts = ts.tz_convert('UTC').tz_localize(None)
                return ts.strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                return str(x).strip() if x else None

        for _col in ['uf_time', 'ptb_time', 'cur_time', 'last_change_time', 'sur_repair_time']:
            if _col in cleaned_summary.columns:
                cleaned_summary[_col] = cleaned_summary[_col].apply(_fmt_dt)

        if df_cimcorp_input is None or not isinstance(df_cimcorp_input, pd.DataFrame) or df_cimcorp_input.empty:
            logging.info("Scrap/repair data is empty or None — proceeding with TUB data only.")
            cleaned_scrap = pd.DataFrame()
        else:
            cleaned_scrap = df_cimcorp_input.copy()
            cleaned_scrap.columns = [c.lower() for c in cleaned_scrap.columns]

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
            cleaned_scrap['re_code'] = cleaned_scrap['re_code'].apply(
                lambda x: '0' if (x is None or str(x).strip() in ('', 'None', 'nan', 'NaN')) else x
            )
        for _col in ['re_last_change_time', 'sur_repair_time', 'gt_weighting_time', 'last_change_time', 'cp_curing_time']:
            if _col in cleaned_scrap.columns:
                cleaned_scrap[_col] = cleaned_scrap[_col].apply(_fmt_dt)


        def _extract_comment_code(comment):
            """Extract the first standalone number from re_comment."""
            if not comment or not isinstance(comment, str):
                return None
            m = re.search(r'\b(\d+)\b', comment)
            return m.group(1) if m else None

        try:
            re_code_docs = list(client[config_databse]["re_codes"].find({}, {"_id": 0, "condition_code": 1, "description": 1}))
            if re_code_docs:
                re_code_map = {
                    str(doc["condition_code"]).strip(): doc.get("description", "")
                    for doc in re_code_docs
                }
                re_desc_map = {
                    desc.lower(): (code, desc)
                    for code, desc in re_code_map.items()
                }

                def _resolve(row):
                    comment = str(row.get("re_comment", "") or "").strip()
                    key = str(row.get("re_code", "") or "").strip()
                    if key and key in re_code_map:
                        return key, re_code_map[key]
                    comment_key = _extract_comment_code(comment)
                    if comment_key and comment_key in re_code_map:
                        return comment_key, re_code_map[comment_key]
                    if comment:
                        comment_lower = comment.lower()
                        if comment_lower in re_desc_map:
                            return re_desc_map[comment_lower]
                        for desc_lower, (code, desc) in re_desc_map.items():
                            if desc_lower in comment_lower or comment_lower in desc_lower:
                                return code, desc
                    return "", ""

                resolved = cleaned_scrap.apply(_resolve, axis=1)
                cleaned_scrap["condition_code"] = resolved.apply(lambda x: x[0])
                cleaned_scrap["description"]    = resolved.apply(lambda x: x[1])

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

        try:
            qr_docs = list(client[config_databse][channel_no_collect].find(
                {}, {"_id": 0, "sku": 1, "QR_Status": 1}
            ))
            if qr_docs:
                qr_map = {str(doc["sku"]).strip(): doc.get("QR_Status", "") for doc in qr_docs}
                barcode_col = next((c for c in ['barcode'] if c in cleaned_summary.columns), None)
                sku_col     = next((c for c in ['sku'] if c in cleaned_summary.columns), None)
                if barcode_col and sku_col:
                    barcode_sku_map = dict(zip(
                        cleaned_summary[barcode_col].astype(str),
                        cleaned_summary[sku_col].astype(str)
                    ))
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

        copy_msgs = copy_to_single_collection_api(
            df_summary=cleaned_summary,
            df_scrap=cleaned_scrap
        )
        output_messages.extend(copy_msgs)

        run_timestamp = some_function()
        client["file_tracking"]["file_metadata"].insert_one({
            "query":              "Q1_TUB_TBM_CIMCORP",
            "description":        "UFM + TBM + Curing + Repair merged data",
            "filename":           f"Q1_{run_timestamp.strftime('%Y%m%d_%H%M%S')}",
            "inserted_timestamp": run_timestamp,
            "start_timestamp":    start_timestamp,
            "end_timestamp":      end_timestamp,
            "num_rows_raw":       len(df_tub_input),
            "num_rows_cleaned":   len(cleaned_summary),
            "num_rows_invalid":   len(invalid_df) if not invalid_df.empty else 0,
            "target_db":          visual_data,
            "target_collection":  inspection_data,
            "processed":          True,
            "processed_timestamp": run_timestamp
        })
        output_messages.append("Run tracked in file_tracking.file_metadata.")

        # Create indexes on the target collection
        create_indexes_for_aggregation()
        
        # AFTER cleaning, run the aggregation by ptb_time date
        aggregation_result = aggregate_and_store_by_date_sku()
        output_messages.append(f"Aggregation result: {aggregation_result.get('message', '')}")
        
        execution_time = time.time() - start_time
        output_messages.append(f"Completed in {execution_time:.2f} seconds.")
        logging.info("run_data_cleaning function completed")
        return {
            "status": "success",
            "message": "Data cleaning and processing completed successfully.",
            "output_log": output_messages,
            "execution_time": execution_time,
            "aggregation_result": aggregation_result
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

        result = run_data_cleaning(df_tub, df_cimcorp)
        logging.info(f"Data cleaning result: {result['message']}")
        logging.info(f"Execution time: {result['execution_time']:.2f} seconds")
        logging.info("Output log:\n" + "\n".join(result['output_log']))
        print(result["message"])
        print(f"Execution time: {result['execution_time']:.2f} seconds")
        print("Output log:", result["output_log"])
        
        # Print aggregation results if available
        if 'aggregation_result' in result:
            print("\nAggregation Results:")
            print(f"  Status: {result['aggregation_result'].get('status', 'N/A')}")
            print(f"  Message: {result['aggregation_result'].get('message', 'N/A')}")
            print(f"  Records Processed: {result['aggregation_result'].get('records_processed', 0)}")
            print(f"  Records Inserted: {result['aggregation_result'].get('records_inserted', 0)}")
            if 'dates' in result['aggregation_result']:
                print(f"  Dates: {result['aggregation_result']['dates']}")
            
    except Exception as e:
        error_message = f"Error in main function: {str(e)}"
        logging.error(error_message)
        logging.error(traceback.format_exc())
        print(error_message)
        print(traceback.format_exc())


if __name__ == "__main__":
    main()


# import pandas as pd
# import re
# from datetime import datetime, timezone, time as datetime_time
# from datetime import datetime, date
# import traceback
# import os
# import numpy as np
# import time
# import redshift_connector
# import pytz
# import pymongo
# from pymongo import MongoClient
# from datetime import datetime, timedelta
# from pytz import timezone
# import logging
# from datetime import datetime
# from passlib.context import CryptContext
# import warnings
# pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
# from config_hungary import (
#     client,
#     INVALID_ROWS_DB,
#     channel_no_collect,
#     re_codes,
#     alert_mail_db,
#     admin_table,
#     alert_users,
#     config_databse,
#     visual_data,
#     inspection_data,
#     All_SLOBS_DATA,
#     active_sku_list
# )


# def configure_logging():
#     if not os.path.exists('logs'):
#         os.makedirs('logs')

#     current_date = datetime.now().strftime('%Y-%m-%d')
#     log_filename = f'logs/log_home_{current_date}.txt'

#     for handler in logging.root.handlers[:]:
#         logging.root.removeHandler(handler)

#     logging.basicConfig(
#         level=logging.INFO,
#         format='%(asctime)s - %(levelname)s - %(message)s',
#         handlers=[
#             logging.FileHandler(log_filename, mode='a', encoding='utf-8')
#         ]
#     )
#     warnings.simplefilter("always")
#     logging.captureWarnings(True)


# def some_function():
#     india_time = datetime.now(pytz.timezone('Asia/Kolkata'))
#     return india_time


# logging.info(some_function())

# DEFAULT_DATETIME = datetime(1900, 1, 1, 12, 30, 0)
# DEFAULT_DATE = datetime(1900, 1, 1)


# class RedshiftDataConnectorr:
#     def __init__(self):
#         self.conn = redshift_connector.connect(
#             host='atl-dl-prd-01.cuvxusm4qify.ap-south-1.redshift.amazonaws.com',
#             database='db_prd_plant_gyo',
#             port=5439,
#             user='usr_gobinath_b',
#             password='nl2k#Z#u74FZ#2uP', sslmode='disable'
#         )

#     def execute_query(self, q):
#         data = pd.read_sql(q, self.conn)
#         # Strip timezone from all datetime columns — NaT with tz causes
#         # "NaTType does not support utcoffset" when serialising to MongoDB
#         for col in data.select_dtypes(include=['datetimetz', 'datetime64[ns, UTC]',
#                                                'datetime64[ns]']).columns:
#             try:
#                 data[col] = data[col].dt.tz_localize(None) if data[col].dt.tz is None \
#                     else data[col].dt.tz_convert(None)
#             except Exception:
#                 data[col] = pd.to_datetime(data[col], errors='coerce').dt.tz_localize(None)
#         return data


# inst = RedshiftDataConnectorr()


# local_tz = pytz.timezone("Asia/Kolkata")
# # ---------------- MANUAL INPUT ----------------
# manual_start_date = '2026-01-31'
# manual_end_date   = '2026-02-28'
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


# # =================================================
# # QUERY 1 — UFM + TBM + CIMCORP + SCRAP (merged)
# # Filter: sur.sur_repair_comment IS NOT NULL
# # =================================================
# q1 = f"""
# SELECT 
#     d.*,
#     prod.*,
#     ch.*,
#     mt.tire_barcode,
#     mt.stt_id,
#     mt.ct_code AS mv_sku,
#     mt.last_change_time,
#     sur.sur_stt_id,
#     sur.sur_repair_time,
#     sur.sur_repair_code AS re_code,
#     sur.sur_repair_comment AS re_comment
# FROM (
#     SELECT 
#         ztc.barcode,
#         ztc.sku,
#         ztc.equipmentid AS UF_ID,
#         ztc.timestamp AS UF_time,
#         shift,
#         overallufmgrade
#     FROM sc_apollo_mom.t_zf_test_completed ztc
#     WHERE ztc.timestamp BETWEEN '{start_timestamp}' AND '{end_timestamp}'
#       AND ztc.sku IN ({modelno_sql})
# ) d

# LEFT JOIN (
#     SELECT 
#         equipmentid AS PTB_ID,
#         producedon AS PTB_time,
#         vst_vhmc_mes_gt_barcode,
#         vst_vhmc_mes_gt_code AS gt_code
#     FROM sc_mhshistory.t_tbm_pcr_productiondata
# ) prod
# ON prod.vst_vhmc_mes_gt_barcode = d.barcode

# LEFT JOIN (
#     SELECT 
#         curingpressid AS CUR_ID,
#         curingtimestamp AS Cur_time,
#         barcodeid,
#         curingmouldid AS cur_mouldid
#     FROM sc_mhshistory.t_curing_history
# ) ch
# ON ch.barcodeid = d.barcode

# LEFT JOIN sc_wms_apollo_pcr.mv_tire mt
#     ON mt.tire_barcode = d.barcode
#     AND mt.ct_code IN ({modelno_sql})
#     AND mt.last_change_time >= '{start_timestamp}'
#     AND mt.last_change_time <  '{end_timestamp}'

# LEFT JOIN sc_wms_apollo_pcr.stock_unit_repair sur
#     ON mt.stt_id = sur.sur_stt_id

# WHERE sur.sur_repair_comment IS NOT NULL
# """

# print("#########################")
# df_tub = inst.execute_query(q1)

# # ── Convert all datetime columns to "YYYY-MM-DD HH:MM:SS" string format ──────
# def _fmt_dt(x):
#     """Convert any datetime/timestamp to 'YYYY-MM-DD HH:MM:SS' string. None if null."""
#     if x is None or x is pd.NaT:
#         return None
#     if isinstance(x, float):
#         try:
#             if pd.isna(x):
#                 return None
#         except Exception:
#             pass
#     try:
#         ts = pd.to_datetime(x, errors='coerce', utc=True)
#         if pd.isna(ts):
#             ts = pd.to_datetime(x, errors='coerce')
#         if pd.isna(ts):
#             return None
#         if ts.tzinfo is not None:
#             ts = ts.tz_convert('UTC').tz_localize(None)
#         return ts.strftime('%Y-%m-%d %H:%M:%S')
#     except Exception:
#         return str(x).strip() if x else None

# _DT_COLS = ['uf_time', 'ptb_time', 'cur_time', 'last_change_time', 'sur_repair_time']
# for _col in _DT_COLS:
#     if _col in df_tub.columns:
#         df_tub[_col] = df_tub[_col].apply(_fmt_dt)

# print(f"[INFO] Datetime columns converted: {[c for c in _DT_COLS if c in df_tub.columns]}")


# def _safe_val(x):
#     """
#     Convert a single value to something PyMongo/BSON can serialize.
#     NaT and NaN → None.  tz-aware datetime/Timestamp → strip tz.
#     Called AFTER .to_dict('records') so x is already a Python object.
#     """
#     if x is pd.NaT:
#         return None
#     if isinstance(x, pd.Timestamp):
#         try:
#             if pd.isnull(x):
#                 return None
#         except Exception:
#             return None
#         return x.replace(tzinfo=None).to_pydatetime()
#     if isinstance(x, datetime):
#         return x.replace(tzinfo=None)
#     if isinstance(x, float):
#         try:
#             if pd.isna(x):
#                 return None
#         except Exception:
#             pass
#     return x


# def df_to_mongo_records(df):
#     """
#     Convert a DataFrame to a list of dicts safe for MongoDB.
#     Operates on dict values AFTER .to_dict() so pandas cannot sneak NaT
#     back in as a typed object.
#     """
#     raw = df.to_dict('records')
#     return [{k: _safe_val(v) for k, v in row.items()} for row in raw]


# def sanitize_df_for_mongo(df):
#     # No-op passthrough — real sanitization happens inside df_to_mongo_records.
#     return df


# def sanitize_collection_name(filename):
#     name = filename.rsplit(".", 1)[0]
#     name = re.sub(r'[\/\.\$]', '_', name)
#     return name[:120]


# def insert_inspection_data(df):
#     """Insert merged query results into visual_data.inspection_data."""
#     if df is None or df.empty:
#         print("[INFO] No inspection data to insert.")
#         return
#     try:
#         vis_db   = client[visual_data]
#         vis_coll = vis_db[inspection_data]
#         records  = df_to_mongo_records(df)
#         if records:
#             # Upsert by barcode to avoid duplicates on re-run
#             for i in range(0, len(records), 5000):
#                 chunk = records[i:i + 5000]
#                 barcodes = [r.get('barcode') for r in chunk if r.get('barcode')]
#                 if barcodes:
#                     vis_coll.delete_many({"barcode": {"$in": barcodes}})
#                 vis_coll.insert_many(chunk, ordered=False)
#             print(f"[INFO] Inserted {len(records)} records into {visual_data}.{inspection_data}")
#         else:
#             print("[INFO] No records to insert into inspection_data.")
#     except Exception as e:
#         print(f"[ERROR] Failed to insert into inspection_data: {e}")


# insert_inspection_data(df_tub)

# # df_cimcorp is no longer a separate query — set to empty DataFrame
# df_cimcorp = pd.DataFrame()


# # ── Unique index for deduplication ──────────────────────────────────────────
# UNIQUE_INDEX_FIELDS = [
#     ("barcode", 1), ("uf_time", 1), ("ptb_time", 1), ("cur_time", 1),
#     ("gt_weighting_time", 1), ("last_change_time", 1),
#     ("sur_repair_time", 1), ("tire_barcode", 1)
# ]
# UNIQUE_INDEX_NAME = "idx_unique_duplicate_detection"


# def ensure_unique_index(collection):
#     """Create the deduplication index if it doesn't already exist."""
#     existing = collection.index_information()
#     if UNIQUE_INDEX_NAME not in existing:
#         try:
#             collection.create_index(
#                 UNIQUE_INDEX_FIELDS,
#                 name=UNIQUE_INDEX_NAME,
#                 unique=True,
#                 background=True
#             )
#             logging.info(f"Created unique index on {collection.name}")
#         except Exception as e:
#             logging.warning(f"Index creation failed: {e}")
#     else:
#         logging.info(f"Unique index already exists on {collection.name}")


# def chunked_insert(collection, records, chunk_size=5000):
#     """Insert with ordered=False — MongoDB skips duplicates automatically."""
#     total    = len(records)
#     inserted = 0
#     for i in range(0, total, chunk_size):
#         chunk = records[i:i + chunk_size]
#         try:
#             result    = collection.insert_many(chunk, ordered=False)
#             inserted += len(result.inserted_ids)
#         except pymongo.errors.BulkWriteError as bwe:
#             inserted += bwe.details['nInserted']
#     return inserted


# def has_special_chars(value):
#     if isinstance(value, str):
#         return bool(re.search(r'[^0-9]', value))
#     return False


# def get_shift(dt):
#     if dt is None or dt == "No Date":
#         return "No Date"
#     if isinstance(dt, str):
#         try:
#             dt = pd.to_datetime(dt, errors='coerce')
#             if pd.isna(dt):
#                 return "No Date"
#         except Exception as e:
#             logging.info("Error parsing date: %s", e)
#             return "No Date"
#     if not isinstance(dt, (datetime, pd.Timestamp)):
#         return "No Date"
#     if pd.isna(dt):
#         return "No Date"
#     if dt == DEFAULT_DATETIME:
#         return "No Date"
#     if dt.tzinfo is not None:
#         dt = dt.replace(tzinfo=None)

#     hour = dt.hour

#     # Shift A: 06:00 – 17:59
#     if 6 <= hour <= 17:
#         return "A"
#     # Shift B: 18:00 – 23:59  OR  00:00 – 05:59 (overnight)
#     elif hour >= 18 or hour < 6:
#         return "B"
#     else:
#         return "No Shift"


# def mark_rerun_and_last(df):
#     """
#     Mark rerun and last for each barcode within the same day based on uf_time.
    
#     Rules:
#     - 1 occurrence: rerun=0, last=1
#     - 2 occurrences: 
#         - First: rerun=0, last=0
#         - Last: rerun=1, last=1
#     - 3+ occurrences:
#         - First: rerun=0, last=-1
#         - Middle: rerun=1, last=0
#         - Last: rerun=1, last=1
    
#     Args:
#         df: DataFrame with 'barcode' and 'uf_time' columns
        
#     Returns:
#         DataFrame with 'rerun' and 'last' columns added
        
#     Examples:
#         # 1 occurrence
#         uf_time     barcode    rerun    last
#         2:00        123        0        1
        
#         # 2 occurrences
#         uf_time     barcode    rerun    last
#         2:00        123        0        0
#         2:30        123        1        1
        
#         # 3 occurrences
#         uf_time     barcode    rerun    last
#         2:00        123        0        -1
#         2:30        123        1        0
#         3:00        123        1        1
        
#         # 4 occurrences
#         uf_time     barcode    rerun    last
#         2:00        123        0        -1
#         2:30        123        1        0
#         3:00        123        1        0
#         3:30        123        1        1
#     """
#     if df.empty:
#         df['rerun'] = 0
#         df['last'] = 1
#         return df
    
#     # Extract date from uf_time
#     def extract_date(uf_time):
#         if pd.isna(uf_time):
#             return None
#         try:
#             dt = pd.to_datetime(uf_time)
#             return dt.strftime('%Y-%m-%d')
#         except:
#             return None
    
#     df_copy = df.copy()
#     df_copy['uf_date'] = df_copy['uf_time'].apply(extract_date)
    
#     # Initialize columns
#     df_copy['rerun'] = 0
#     df_copy['last'] = 1  # Default for single occurrences
    
#     # Group by barcode and date
#     grouped = df_copy.groupby(['barcode', 'uf_date'])
    
#     for (barcode, date), group_indices in grouped.groups.items():
#         if date is None or barcode is None:
#             continue
            
#         # Get sorted indices by uf_time (chronological order)
#         group_df = df_copy.loc[group_indices].sort_values('uf_time')
#         indices = group_df.index.tolist()
#         count = len(indices)
        
#         if count == 1:
#             # Only one occurrence - set rerun=0, last=1
#             df_copy.loc[indices[0], 'rerun'] = 0
#             df_copy.loc[indices[0], 'last'] = 1
#         elif count == 2:
#             # Two occurrences
#             for i, idx in enumerate(indices):
#                 if i == 0:
#                     # First occurrence
#                     df_copy.loc[idx, 'rerun'] = 0
#                     df_copy.loc[idx, 'last'] = 0
#                 else:
#                     # Last occurrence
#                     df_copy.loc[idx, 'rerun'] = 1
#                     df_copy.loc[idx, 'last'] = 1
#         else:
#             # 3 or more occurrences
#             for i, idx in enumerate(indices):
#                 if i == 0:
#                     # First occurrence
#                     df_copy.loc[idx, 'rerun'] = 0
#                     df_copy.loc[idx, 'last'] = -1
#                 elif i == count - 1:
#                     # Last occurrence (most recent)
#                     df_copy.loc[idx, 'rerun'] = 1
#                     df_copy.loc[idx, 'last'] = 1
#                 else:
#                     # Middle occurrences
#                     df_copy.loc[idx, 'rerun'] = 1
#                     df_copy.loc[idx, 'last'] = 0
    
#     # Drop temporary column
#     df_copy = df_copy.drop('uf_date', axis=1)
    
#     return df_copy


# def clean_tub_data(df):
#     output_messages = []
#     invalid_rows = pd.DataFrame()
#     logging.info("clean_tub_data function is just started")
#     if not isinstance(df, pd.DataFrame):
#         raise TypeError("Expected a pandas DataFrame as input")
#     df_clean = df.copy()

#     if 'barcode' in df_clean.columns:
#         df_clean['barcode'] = df_clean['barcode'].astype(str)

#     rename_columns_mapping = {
#         'ufm': 'ubno',
#         'timestamp': 'date_and_time',
#         'equipmentid': 'tbm',
#         'producedon': 'tbm_buildtime',
#         'ctcode': 'sku',
#         'curingmouldid': 'curing_mouldid',
#         'curingpressid': 'curing_press',
#         'curingtimestamp': 'curing_infeedtime',
#         'vst_vhmc_mes_gt_code': 'tbm_gtcode',
#         'barcodeid': 'barcode',
#         'vst_vhmc_mes_gt_barcode': 'barcode_tbm'
#     }
#     existing_rename_mapping = {k: v for k, v in rename_columns_mapping.items() if k in df_clean.columns}
#     df_clean.rename(columns=existing_rename_mapping, inplace=True)

#     duplicates = df_clean.columns[df_clean.columns.duplicated(keep=False)].unique()
#     if len(duplicates) > 0:
#         logging.warning(f"Duplicate column names in input: {list(duplicates)}. Renaming duplicates.")
#         cols = pd.Series(df_clean.columns)
#         for dup in duplicates:
#             cols[cols[cols == dup].index.values.tolist()] = [
#                 f"{dup}_{i}" if i != 0 else dup for i in range(sum(cols == dup))
#             ]
#         df_clean.columns = cols

#     mask = df_clean['barcode'].isnull() | df_clean['barcode'].apply(has_special_chars)
#     invalid_rows = df_clean[mask]
#     df_clean = df_clean[~mask].drop_duplicates()
#     df_clean.drop(columns=['rn', 'Unnamed: 226', 'Unnamed: 225'], inplace=True, errors='ignore')
#     df_clean.columns = df_clean.columns.to_series().str.strip().str.lower().str.replace(' ', '_')

#     return df_clean, invalid_rows, output_messages


# def clean_visual_data(df):
#     output_messages = []
#     invalid_barcode_rows = pd.DataFrame()
#     duplicates_df = pd.DataFrame()
#     logging.info("clean_visual_data function is just started")

#     try:
#         df.columns = [re.sub(r'[.\s]+', '_', col.strip()).lower() for col in df.columns]

#         rename_columns_mapping = {
#             'equipmentid': 'tbm',
#             'producedon': 'tbm_buildtime',
#             'ctcode': 'sku',
#             'curingmouldid': 'curing_mouldid',
#             'curingpressid': 'curing_press',
#             'curingtimestamp': 'curing_infeedtime',
#             'vst_vhmc_mes_gt_code': 'tbm_gtcode',
#             'barcodeid': 'barcode',
#             'vst_vhmc_mes_gt_barcode': 'barcode_tbm'
#         }
#         for old_name, new_name in rename_columns_mapping.items():
#             if old_name in df.columns:
#                 df = df.rename(columns={old_name: new_name}, inplace=False)

#         df['barcode'] = df['barcode'].astype(str).str.strip()
#         df['curing_infeedtime'] = df['curing_infeedtime'].astype(str)

#         special_char_regex = re.compile(r'[^0-9]')

#         rows_with_null_barcode = df[df['barcode'].isnull() | (df['barcode'] == 'nan')]
#         rows_with_special_char_barcode = df[
#             df['barcode'].apply(lambda x: bool(special_char_regex.search(x)))
#         ]
#         invalid_barcode_rows = pd.concat(
#             [rows_with_null_barcode, rows_with_special_char_barcode]
#         ).drop_duplicates()

#         df = df[~df.index.isin(invalid_barcode_rows.index)]
#         df = df.dropna(axis=1, how='all')
#         duplicates_df = df[df.duplicated(subset=['barcode'], keep=False)]
#         df = df.drop_duplicates(subset=['barcode'])

#         output_messages.append("CIMCORP_HISTORICAL_DATA Cleaning successful")
#         logging.info("clean_visual_data function is completed")
#         return df, invalid_barcode_rows, duplicates_df, output_messages

#     except Exception as e:
#         error_message = f"Error during cleaning: {e}"
#         output_messages.append(error_message)
#         return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), output_messages


# def _upsert_records(collection, records, key_field):
#     """Fastest bulk write: chunked delete+insert_many."""
#     if not records:
#         return
#     CHUNK = 10000
#     keys = [rec[key_field] for rec in records if rec.get(key_field) is not None]
#     for i in range(0, len(keys), CHUNK):
#         collection.delete_many({key_field: {"$in": keys[i:i+CHUNK]}})
#     for i in range(0, len(records), CHUNK):
#         collection.insert_many(records[i:i+CHUNK], ordered=False)


# def _upsert_scrap_records(collection, records, key_cols):
#     """Fastest scrap bulk write: chunked delete+insert_many."""
#     if not records or not key_cols:
#         return
#     CHUNK = 10000
#     if 'tire_barcode' in key_cols:
#         barcodes = [rec.get('tire_barcode') for rec in records if rec.get('tire_barcode')]
#         for i in range(0, len(barcodes), CHUNK):
#             collection.delete_many({"tire_barcode": {"$in": barcodes[i:i+CHUNK]}})
#     else:
#         or_clauses = [{col: rec.get(col) for col in key_cols} for rec in records]
#         for i in range(0, len(or_clauses), CHUNK):
#             collection.delete_many({"$or": or_clauses[i:i+CHUNK]})
#     for i in range(0, len(records), CHUNK):
#         collection.insert_many(records[i:i+CHUNK], ordered=False)


# def copy_to_single_collection_api(df_summary=None, df_scrap=None):
#     """Write cleaned data to inspection_data collection with rerun and last columns."""
#     messages = []
    
#     # Write summary data
#     if df_summary is not None and not df_summary.empty:
#         try:
#             # Apply rerun and last marking BEFORE storing
#             df_summary = mark_rerun_and_last(df_summary)
            
#             # Log the rerun/last distribution for debugging
#             if 'rerun' in df_summary.columns and 'last' in df_summary.columns:
#                 rerun_counts = df_summary['rerun'].value_counts().to_dict()
#                 last_counts = df_summary['last'].value_counts().to_dict()
#                 logging.info(f"Rerun distribution: {rerun_counts}")
#                 logging.info(f"Last distribution: {last_counts}")
#                 print(f"Rerun distribution: {rerun_counts}")
#                 print(f"Last distribution: {last_counts}")
            
#             vis_db = client[visual_data]
#             vis_coll = vis_db[inspection_data]
#             records = df_to_mongo_records(df_summary)
            
#             if records:
#                 # Delete existing records with same barcode in time range
#                 barcodes = [r.get('barcode') for r in records if r.get('barcode')]
#                 if barcodes:
#                     vis_coll.delete_many({"barcode": {"$in": barcodes}})
                
#                 # Insert in chunks
#                 for i in range(0, len(records), 5000):
#                     chunk = records[i:i+5000]
#                     vis_coll.insert_many(chunk, ordered=False)
                
#                 messages.append(f"Inserted {len(records)} records into {visual_data}.{inspection_data} with rerun/last columns")
#                 logging.info(f"Inserted {len(records)} records into inspection_data with rerun/last columns")
#                 print(f"Inserted {len(records)} records into inspection_data with rerun/last columns")
#             else:
#                 messages.append("No records to insert into inspection_data")
#         except Exception as e:
#             error_msg = f"Error inserting summary data: {e}"
#             messages.append(error_msg)
#             logging.error(error_msg)
#             print(error_msg)
    
#     # Write scrap data if available
#     if df_scrap is not None and not df_scrap.empty:
#         try:
#             scrap_db = client["All_SLOBS_DATA"]
#             scrap_coll = scrap_db["scrap"]
            
#             # Ensure tire_barcode exists as key
#             if 'tire_barcode' not in df_scrap.columns and 'barcode' in df_scrap.columns:
#                 df_scrap['tire_barcode'] = df_scrap['barcode']
            
#             records = df_to_mongo_records(df_scrap)
#             if records:
#                 # Delete existing scrap records
#                 barcodes = [r.get('tire_barcode') for r in records if r.get('tire_barcode')]
#                 if barcodes:
#                     scrap_coll.delete_many({"tire_barcode": {"$in": barcodes}})
                
#                 # Insert in chunks
#                 for i in range(0, len(records), 5000):
#                     chunk = records[i:i+5000]
#                     scrap_coll.insert_many(chunk, ordered=False)
                
#                 messages.append(f"Inserted {len(records)} records into All_SLOBS_DATA.scrap")
#                 logging.info(f"Inserted {len(records)} records into scrap collection")
#         except Exception as e:
#             error_msg = f"Error inserting scrap data: {e}"
#             messages.append(error_msg)
#             logging.error(error_msg)
    
#     return messages


# # =====================================================
# # AGGREGATION FUNCTION: Group by uf_time date with all SKUs
# # =====================================================
# def aggregate_and_store_by_date_sku():
#     """
#     Read data from visual_data.inspection_data, aggregate by uf_time date only,
#     and store in All_SLOBS_DATA.active_sku_list.
#     Each document represents a unique uf_time date with all SKUs and their respective barcodes.
#     """
#     logging.info("Starting aggregation by uf_time date with all SKUs...")
    
#     try:
#         # Connect to the source database
#         source_db = client[visual_data]
#         source_collection = source_db[inspection_data]
        
#         # Connect to the target database
#         target_db = client[All_SLOBS_DATA]
#         target_collection = target_db[active_sku_list]
        
#         # Step 1: Aggregate by uf_time date and sku first
#         pipeline = [
#             {
#                 "$match": {
#                     "uf_time": {"$exists": True, "$ne": None, "$ne": ""},
#                     "sku": {"$exists": True, "$ne": None, "$ne": ""}
#                 }
#             },
#             {
#                 "$addFields": {
#                     "uf_date": {
#                         "$dateToString": {
#                             "format": "%Y-%m-%d",
#                             "date": {"$toDate": "$uf_time"}
#                         }
#                     }
#                 }
#             },
#             {
#                 "$group": {
#                     "_id": {
#                         "uf_date": "$uf_date",
#                         "sku": "$sku"
#                     },
#                     "barcodes": {
#                         "$push": {
#                             "barcode": "$barcode",
#                             "ptb_time": "$ptb_time",
#                             "ptb_id": "$ptb_id",
#                             "gt_code": "$gt_code",
#                             "cur_id": "$cur_id",
#                             "cur_mouldid": "$cur_mouldid",
#                             "cur_time": "$cur_time",
#                             "re_comment": "$re_comment",
#                             "sur_repair_time": "$sur_repair_time",
#                             "visualinspection_time_tub": "$visualinspection_time_tub",
#                             "last_change_time": "$last_change_time",
#                             "re_code": "$re_code",
#                             "uf_id": "$uf_id",
#                             "uf_time": "$uf_time",
#                             "rerun": "$rerun",
#                             "last": "$last"
#                         }
#                     }
#                 }
#             },
#             {
#                 "$group": {
#                     "_id": "$_id.uf_date",
#                     "skus": {
#                         "$push": {
#                             "sku": "$_id.sku",
#                             "barcodes": "$barcodes"
#                         }
#                     }
#                 }
#             },
#             {
#                 "$project": {
#                     "_id": 0,
#                     "uf_time": "$_id",
#                     "skus": 1
#                 }
#             },
#             {
#                 "$sort": {
#                     "uf_time": 1
#                 }
#             }
#         ]
        
#         # Execute aggregation
#         logging.info("Executing aggregation pipeline...")
#         aggregated_results = list(source_collection.aggregate(pipeline, allowDiskUse=True))
        
#         if not aggregated_results:
#             logging.info("No data found to aggregate.")
#             return {
#                 "status": "success",
#                 "message": "No data found to aggregate.",
#                 "records_processed": 0,
#                 "records_inserted": 0
#             }
        
#         logging.info(f"Found {len(aggregated_results)} unique uf_time dates to process.")
        
#         # Get unique dates from results for deletion
#         dates = list(set([doc['uf_time'] for doc in aggregated_results]))
        
#         # Delete existing data for the dates being inserted
#         if dates:
#             delete_result = target_collection.delete_many({
#                 "uf_time": {"$in": dates}
#             })
#             logging.info(f"Deleted {delete_result.deleted_count} existing records for dates: {dates}")
        
#         # Insert new aggregated data in chunks
#         total_inserted = 0
#         chunk_size = 1000
        
#         for i in range(0, len(aggregated_results), chunk_size):
#             chunk = aggregated_results[i:i+chunk_size]
#             try:
#                 result = target_collection.insert_many(chunk, ordered=False)
#                 total_inserted += len(result.inserted_ids)
#                 logging.info(f"Inserted chunk {i//chunk_size + 1} with {len(result.inserted_ids)} records")
#             except pymongo.errors.BulkWriteError as bwe:
#                 inserted_count = bwe.details.get('nInserted', 0)
#                 total_inserted += inserted_count
#                 logging.warning(f"Bulk write error in chunk {i//chunk_size + 1}: {bwe}")
#                 logging.warning(f"Inserted {inserted_count} out of {len(chunk)} documents in this chunk")
        
#         logging.info(f"Successfully inserted {total_inserted} aggregated records into {All_SLOBS_DATA}.{active_sku_list}")
        
#         return {
#             "status": "success",
#             "message": f"Successfully aggregated {len(aggregated_results)} uf_time dates.",
#             "records_processed": len(aggregated_results),
#             "records_inserted": total_inserted,
#             "dates": dates,
#             "date_count": len(dates)
#         }
        
#     except Exception as e:
#         error_msg = f"Error in aggregate_and_store_by_date_sku: {str(e)}"
#         logging.error(error_msg)
#         logging.error(traceback.format_exc())
#         return {
#             "status": "error",
#             "message": error_msg,
#             "records_processed": 0,
#             "records_inserted": 0
#         }


# # =====================================================
# # ALTERNATIVE: Aggregate by uf_time month (YYYY-MM)
# # =====================================================
# def aggregate_and_store_by_uf_month():
#     """
#     Aggregate by uf_time month (YYYY-MM) instead of date.
#     Useful for monthly reporting.
#     """
#     logging.info("Starting aggregation by uf_time month with all SKUs...")
    
#     try:
#         # Connect to the source database
#         source_db = client[visual_data]
#         source_collection = source_db[inspection_data]
        
#         # Connect to the target database
#         target_db = client[All_SLOBS_DATA]
#         target_collection = target_db[active_sku_list]
        
#         pipeline = [
#             {
#                 "$match": {
#                     "uf_time": {"$exists": True, "$ne": None, "$ne": ""},
#                     "sku": {"$exists": True, "$ne": None, "$ne": ""}
#                 }
#             },
#             {
#                 "$addFields": {
#                     "uf_month": {
#                         "$dateToString": {
#                             "format": "%Y-%m",
#                             "date": {"$toDate": "$uf_time"}
#                         }
#                     }
#                 }
#             },
#             {
#                 "$group": {
#                     "_id": {
#                         "uf_month": "$uf_month",
#                         "sku": "$sku"
#                     },
#                     "barcodes": {
#                         "$push": {
#                             "barcode": "$barcode",
#                             "ptb_time": "$ptb_time",
#                             "ptb_id": "$ptb_id",
#                             "gt_code": "$gt_code",
#                             "cur_id": "$cur_id",
#                             "cur_mouldid": "$cur_mouldid",
#                             "cur_time": "$cur_time",
#                             "re_comment": "$re_comment",
#                             "sur_repair_time": "$sur_repair_time",
#                             "visualinspection_time_tub": "$visualinspection_time_tub",
#                             "last_change_time": "$last_change_time",
#                             "re_code": "$re_code",
#                             "uf_id": "$uf_id",
#                             "uf_time": "$uf_time",
#                             "rerun": "$rerun",
#                             "last": "$last"
#                         }
#                     }
#                 }
#             },
#             {
#                 "$group": {
#                     "_id": "$_id.uf_month",
#                     "skus": {
#                         "$push": {
#                             "sku": "$_id.sku",
#                             "barcodes": "$barcodes"
#                         }
#                     }
#                 }
#             },
#             {
#                 "$project": {
#                     "_id": 0,
#                     "uf_time": "$_id",
#                     "skus": 1
#                 }
#             },
#             {
#                 "$sort": {
#                     "uf_time": 1
#                 }
#             }
#         ]
        
#         logging.info("Executing monthly aggregation pipeline...")
#         aggregated_results = list(source_collection.aggregate(pipeline, allowDiskUse=True))
        
#         if not aggregated_results:
#             logging.info("No data found to aggregate by month.")
#             return {
#                 "status": "success",
#                 "message": "No data found to aggregate.",
#                 "records_processed": 0,
#                 "records_inserted": 0
#             }
        
#         logging.info(f"Found {len(aggregated_results)} unique uf_time months to process.")
        
#         # Get unique months from results for deletion
#         months = list(set([doc['uf_time'] for doc in aggregated_results]))
        
#         # Delete existing data for the months being inserted
#         if months:
#             delete_result = target_collection.delete_many({
#                 "uf_time": {"$in": months}
#             })
#             logging.info(f"Deleted {delete_result.deleted_count} existing records for months: {months}")
        
#         # Insert new aggregated data
#         total_inserted = 0
#         chunk_size = 1000
        
#         for i in range(0, len(aggregated_results), chunk_size):
#             chunk = aggregated_results[i:i+chunk_size]
#             try:
#                 result = target_collection.insert_many(chunk, ordered=False)
#                 total_inserted += len(result.inserted_ids)
#             except pymongo.errors.BulkWriteError as bwe:
#                 inserted_count = bwe.details.get('nInserted', 0)
#                 total_inserted += inserted_count
#                 logging.warning(f"Bulk write error: {bwe}")
        
#         logging.info(f"Successfully inserted {total_inserted} aggregated monthly records")
        
#         return {
#             "status": "success",
#             "message": f"Successfully aggregated {len(aggregated_results)} months.",
#             "records_processed": len(aggregated_results),
#             "records_inserted": total_inserted,
#             "months": months,
#             "month_count": len(months)
#         }
        
#     except Exception as e:
#         error_msg = f"Error in aggregate_and_store_by_uf_month: {str(e)}"
#         logging.error(error_msg)
#         logging.error(traceback.format_exc())
#         return {
#             "status": "error",
#             "message": error_msg,
#             "records_processed": 0,
#             "records_inserted": 0
#         }


# # =====================================================
# # CREATE INDEXES FOR TARGET COLLECTION
# # =====================================================
# def create_indexes_for_aggregation():
#     """
#     Create indexes on the target collection for better query performance
#     """
#     try:
#         target_db = client[All_SLOBS_DATA]
#         target_collection = target_db[active_sku_list]
        
#         # Create indexes
#         target_collection.create_index([("uf_time", 1)], unique=True, background=True)
        
#         logging.info(f"Indexes created on {All_SLOBS_DATA}.{active_sku_list}")
#         return True
#     except Exception as e:
#         logging.error(f"Error creating indexes: {e}")
#         return False


# # =====================================================
# # UPDATE run_data_cleaning function to use uf_time
# # =====================================================
# def run_data_cleaning(df_tub_input, df_cimcorp_input):
#     """
#     Cleans df_tub_input and df_cimcorp_input in-memory and writes
#     directly to the final merged DB. No intermediate DBs are used.
#     """
#     start_time = time.time()
#     output_messages = []
#     logging.info("run_data_cleaning function started")
#     try:
#         cleaned_summary, invalid_df, tub_msgs = clean_tub_data(df_tub_input.copy())
#         output_messages.extend(tub_msgs)
#         if not invalid_df.empty:
#             invalid_df = invalid_df.drop('_id', axis=1, errors='ignore')
#             client[INVALID_ROWS_DB]["invalid_data_TUB_home"].insert_many(df_to_mongo_records(invalid_df))
#             output_messages.append(f"Saved {len(invalid_df)} invalid TUB rows to INVALID_ROWS.invalid_data_TUB_home")

#         # Convert datetime columns to "YYYY-MM-DD HH:MM:SS" string format
#         def _fmt_dt(x):
#             """Convert any datetime/timestamp to 'YYYY-MM-DD HH:MM:SS' string. None if null."""
#             if x is None or x is pd.NaT:
#                 return None
#             if isinstance(x, float) and pd.isna(x):
#                 return None
#             try:
#                 ts = pd.to_datetime(x, errors='coerce', utc=True)
#                 if pd.isna(ts):
#                     ts = pd.to_datetime(x, errors='coerce')
#                 if pd.isna(ts):
#                     return None
#                 # Convert to UTC-naive then format
#                 if ts.tzinfo is not None:
#                     ts = ts.tz_convert('UTC').tz_localize(None)
#                 return ts.strftime('%Y-%m-%d %H:%M:%S')
#             except Exception:
#                 return str(x).strip() if x else None

#         for _col in ['uf_time', 'ptb_time', 'cur_time', 'last_change_time', 'sur_repair_time']:
#             if _col in cleaned_summary.columns:
#                 cleaned_summary[_col] = cleaned_summary[_col].apply(_fmt_dt)

#         if df_cimcorp_input is None or not isinstance(df_cimcorp_input, pd.DataFrame) or df_cimcorp_input.empty:
#             logging.info("Scrap/repair data is empty or None — proceeding with TUB data only.")
#             cleaned_scrap = pd.DataFrame()
#         else:
#             cleaned_scrap = df_cimcorp_input.copy()
#             cleaned_scrap.columns = [c.lower() for c in cleaned_scrap.columns]

#         def _to_str_val(x, strip_decimal=False):
#             if x is pd.NaT or (isinstance(x, float) and pd.isna(x)):
#                 return None
#             if strip_decimal:
#                 try:
#                     return str(int(float(x)))
#                 except (ValueError, TypeError):
#                     pass
#             return str(x).strip()

#         if 're_code' in cleaned_scrap.columns:
#             cleaned_scrap['re_code'] = cleaned_scrap['re_code'].apply(lambda x: _to_str_val(x, strip_decimal=True))
#             cleaned_scrap['re_code'] = cleaned_scrap['re_code'].apply(
#                 lambda x: '0' if (x is None or str(x).strip() in ('', 'None', 'nan', 'NaN')) else x
#             )
#         for _col in ['re_last_change_time', 'sur_repair_time', 'gt_weighting_time', 'last_change_time', 'cp_curing_time']:
#             if _col in cleaned_scrap.columns:
#                 cleaned_scrap[_col] = cleaned_scrap[_col].apply(_fmt_dt)


#         def _extract_comment_code(comment):
#             """Extract the first standalone number from re_comment."""
#             if not comment or not isinstance(comment, str):
#                 return None
#             m = re.search(r'\b(\d+)\b', comment)
#             return m.group(1) if m else None

#         try:
#             re_code_docs = list(client[config_databse]["re_codes"].find({}, {"_id": 0, "condition_code": 1, "description": 1}))
#             if re_code_docs:
#                 re_code_map = {
#                     str(doc["condition_code"]).strip(): doc.get("description", "")
#                     for doc in re_code_docs
#                 }
#                 re_desc_map = {
#                     desc.lower(): (code, desc)
#                     for code, desc in re_code_map.items()
#                 }

#                 def _resolve(row):
#                     comment = str(row.get("re_comment", "") or "").strip()
#                     key = str(row.get("re_code", "") or "").strip()
#                     if key and key in re_code_map:
#                         return key, re_code_map[key]
#                     comment_key = _extract_comment_code(comment)
#                     if comment_key and comment_key in re_code_map:
#                         return comment_key, re_code_map[comment_key]
#                     if comment:
#                         comment_lower = comment.lower()
#                         if comment_lower in re_desc_map:
#                             return re_desc_map[comment_lower]
#                         for desc_lower, (code, desc) in re_desc_map.items():
#                             if desc_lower in comment_lower or comment_lower in desc_lower:
#                                 return code, desc
#                     return "", ""

#                 resolved = cleaned_scrap.apply(_resolve, axis=1)
#                 cleaned_scrap["condition_code"] = resolved.apply(lambda x: x[0])
#                 cleaned_scrap["description"]    = resolved.apply(lambda x: x[1])

#                 _uf_keywords = {"force", "runout", "rfv-grinding"}
#                 cleaned_scrap["scrap_category"] = cleaned_scrap["description"].apply(
#                     lambda x: "UF" if str(x).strip().lower() in _uf_keywords else "VISUAL"
#                 )
#                 output_messages.append(f"Enriched scrap data with condition_code + description from {len(re_code_map)} re_codes.")
#             else:
#                 output_messages.append("No re_codes found in config DB — skipping enrichment.")
#         except Exception as re_err:
#             output_messages.append(f"Warning: could not enrich re_codes: {re_err}")
#             logging.warning(f"re_codes enrichment failed: {re_err}")

#         try:
#             qr_docs = list(client[config_databse][channel_no_collect].find(
#                 {}, {"_id": 0, "sku": 1, "QR_Status": 1}
#             ))
#             if qr_docs:
#                 qr_map = {str(doc["sku"]).strip(): doc.get("QR_Status", "") for doc in qr_docs}
#                 barcode_col = next((c for c in ['barcode'] if c in cleaned_summary.columns), None)
#                 sku_col     = next((c for c in ['sku'] if c in cleaned_summary.columns), None)
#                 if barcode_col and sku_col:
#                     barcode_sku_map = dict(zip(
#                         cleaned_summary[barcode_col].astype(str),
#                         cleaned_summary[sku_col].astype(str)
#                     ))
#                     scrap_bc_col = next((c for c in ['tire_barcode', 'barcode'] if c in cleaned_scrap.columns), None)
#                     if scrap_bc_col:
#                         cleaned_scrap["sku"] = cleaned_scrap[scrap_bc_col].apply(
#                             lambda bc: barcode_sku_map.get(str(bc).strip(), "")
#                         )
#                         cleaned_scrap["qr_status"] = cleaned_scrap["sku"].apply(
#                             lambda s: qr_map.get(str(s).strip(), "")
#                         )
#                         output_messages.append("Added qr_status to scrap data via sku lookup.")
#                     else:
#                         output_messages.append("Warning: no barcode column found in scrap data for QR_Status join.")
#                 else:
#                     output_messages.append("Warning: barcode/sku columns missing in summary for QR_Status join.")
#             else:
#                 output_messages.append("No QR_Status data found in channel_no_ufm — skipping.")
#         except Exception as qr_err:
#             output_messages.append(f"Warning: could not add QR_Status: {qr_err}")
#             logging.warning(f"QR_Status enrichment failed: {qr_err}")

#         copy_msgs = copy_to_single_collection_api(
#             df_summary=cleaned_summary,
#             df_scrap=cleaned_scrap
#         )
#         output_messages.extend(copy_msgs)

#         run_timestamp = some_function()
#         client["file_tracking"]["file_metadata"].insert_one({
#             "query":              "Q1_TUB_TBM_CIMCORP",
#             "description":        "UFM + TBM + Curing + Repair merged data",
#             "filename":           f"Q1_{run_timestamp.strftime('%Y%m%d_%H%M%S')}",
#             "inserted_timestamp": run_timestamp,
#             "start_timestamp":    start_timestamp,
#             "end_timestamp":      end_timestamp,
#             "num_rows_raw":       len(df_tub_input),
#             "num_rows_cleaned":   len(cleaned_summary),
#             "num_rows_invalid":   len(invalid_df) if not invalid_df.empty else 0,
#             "target_db":          visual_data,
#             "target_collection":  inspection_data,
#             "processed":          True,
#             "processed_timestamp": run_timestamp
#         })
#         output_messages.append("Run tracked in file_tracking.file_metadata.")

#         # Create indexes on the target collection
#         create_indexes_for_aggregation()
        
#         # AFTER cleaning, run the aggregation by uf_time date
#         aggregation_result = aggregate_and_store_by_date_sku()
#         output_messages.append(f"Aggregation result: {aggregation_result.get('message', '')}")
        
#         execution_time = time.time() - start_time
#         output_messages.append(f"Completed in {execution_time:.2f} seconds.")
#         logging.info("run_data_cleaning function completed")
#         return {
#             "status": "success",
#             "message": "Data cleaning and processing completed successfully.",
#             "output_log": output_messages,
#             "execution_time": execution_time,
#             "aggregation_result": aggregation_result
#         }
#     except Exception as e:
#         error_message = f"An error occurred in run_data_cleaning: {e}"
#         output_messages.append(error_message)
#         logging.error(error_message)
#         return {
#             "status": "error",
#             "message": error_message,
#             "output_log": output_messages,
#             "execution_time": 0
#         }

# date_formats = ["%d-%m-%Y", "%Y-%m-%d"]


# def ensure_admin_collection_exists():
#     logging.info("ensure_admin_collection_exists function is just started")
#     try:
#         db = client[alert_mail_db]
#         collection = db[admin_table]
#         alert_users_collection = db[alert_users]
#         if admin_table not in db.list_collection_names() or collection.count_documents({}) == 0:
#             logging.info(f"Collection '{admin_table}' is missing or empty. Inserting default admin user...")
#             hashed_password = pwd_context.hash("1234")
#             default_admin = {
#                 "user_id": 1,
#                 "email": "manager@apollotyres.com",
#                 "fname": "Admin",
#                 "lname": "User",
#                 "password": hashed_password,
#                 "role": "manager",
#                 "created_by_id": 1,
#                 "created_at": datetime.utcnow()
#             }
#             collection.insert_one(default_admin)
#             alert_users_collection.insert_one(default_admin)
#             logging.info("Default manager admin inserted.")
#             return
#         if collection.count_documents({"role": "manager"}) == 0:
#             logging.info(f"No user with role 'manager' found. Inserting default manager...")
#             hashed_password = pwd_context.hash("1234")
#             default_admin = {
#                 "user_id": 1,
#                 "email": "manager@apollotyres.com",
#                 "password": hashed_password,
#                 "role": "manager",
#                 "created_by_id": 1,
#                 "created_at": datetime.utcnow()
#             }
#             collection.insert_one(default_admin)
#             alert_users_collection.insert_one(default_admin)
#             logging.info("Default manager admin inserted into both admin_table and alert_users.")
#         else:
#             logging.info("At least one 'manager' user already exists in admin_table. No insert needed.")
#     except Exception as e:
#         logging.error(f"Error in ensure_admin_collection_exists: {str(e)}")


# def main():
#     try:
#         configure_logging()
#         warnings.filterwarnings("ignore")
#         logging.info("Starting the data cleaning process...")

#         ensure_admin_collection_exists()
#         logging.info("Admin collection check completed.")

#         result = run_data_cleaning(df_tub, df_cimcorp)
#         logging.info(f"Data cleaning result: {result['message']}")
#         logging.info(f"Execution time: {result['execution_time']:.2f} seconds")
#         logging.info("Output log:\n" + "\n".join(result['output_log']))
#         print(result["message"])
#         print(f"Execution time: {result['execution_time']:.2f} seconds")
#         print("Output log:", result["output_log"])
        
#         # Print aggregation results if available
#         if 'aggregation_result' in result:
#             print("\nAggregation Results:")
#             print(f"  Status: {result['aggregation_result'].get('status', 'N/A')}")
#             print(f"  Message: {result['aggregation_result'].get('message', 'N/A')}")
#             print(f"  Records Processed: {result['aggregation_result'].get('records_processed', 0)}")
#             print(f"  Records Inserted: {result['aggregation_result'].get('records_inserted', 0)}")
#             if 'dates' in result['aggregation_result']:
#                 print(f"  Dates: {result['aggregation_result']['dates']}")
            
#     except Exception as e:
#         error_message = f"Error in main function: {str(e)}"
#         logging.error(error_message)
#         logging.error(traceback.format_exc())
#         print(error_message)
#         print(traceback.format_exc())


# if __name__ == "__main__":
#     main()