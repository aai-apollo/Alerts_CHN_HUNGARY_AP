# import os
# import logging
# import warnings
# from datetime import datetime, timedelta
# import pandas as pd
# import redshift_connector
# import requests
# import pytz

# # ====================== LOGGING ======================
# def configure_logging():
#     if not os.path.exists('logs'):
#         os.makedirs('logs')
    
#     hungary_tz = pytz.timezone('Europe/Budapest')
#     current_date_hu = datetime.now(hungary_tz).strftime('%Y-%m-%d')
#     log_filename = f'logs/log_repl_shift_wise_report_{current_date_hu}.txt'
    
#     for handler in logging.root.handlers[:]:
#         logging.root.removeHandler(handler)
    
#     logging.basicConfig(
#         level=logging.INFO,
#         format='%(asctime)s - %(levelname)s - %(message)s',
#         handlers=[logging.FileHandler(log_filename, mode='a', encoding='utf-8')]
#     )
    
#     warnings.simplefilter("always")
#     logging.captureWarnings(True)
#     logging.info(f"Logging initialized for Hungary date: {current_date_hu}")
#     return log_filename

# log_filename = configure_logging()

# # ====================== MAIN SCRIPT ======================
# class RedshiftDataConnectorr:
#     def __init__(self):
#         self.conn = redshift_connector.connect(
#             host='atl-dl-prd-01.cuvxusm4qify.ap-south-1.redshift.amazonaws.com',
#             database='db_prd_plant_gyo',
#             port=5439,
#             user='usr_digital_twin_readonly',
#             password='UXY0%BSJa9dD#vP', sslmode='disable'
#         )
    
#     def execute_query(self, q):
#         try:
#             data = pd.read_sql(q, self.conn)
#             logging.info(f"Query executed successfully. Rows: {len(data)}")
#             return data
#         except Exception as e:
#             logging.error(f"Query failed: {str(e)}")
#             raise

# inst = RedshiftDataConnectorr()

# hungary_tz = pytz.timezone('Europe/Budapest')
# utc = pytz.utc

# now_hu = datetime.now(hungary_tz)
# logging.info(f"Script started at Hungary time: {now_hu.strftime('%Y-%m-%d %H:%M:%S %Z')}")

# # ====================== NEW SHIFT LOGIC AS PER YOUR REQUIREMENT ======================
# if now_hu.hour < 12:   # Morning run (around 6:30 AM)
#     shift = "Shift B"
#     # Previous evening to this morning
#     prev_date = (now_hu - timedelta(days=1)).strftime('%Y-%m-%d')
#     curr_date = now_hu.strftime('%Y-%m-%d')
#     start_time_hu = "18:00:00"
#     end_time_hu = "06:00:00"
#     query_date_hu = prev_date  # For display purpose
#     logging.info(f"Morning run → Reporting previous Shift B ({prev_date} 18:00 to {curr_date} 06:00)")
# else:                  # Evening run (around 6:30 PM)
#     shift = "Shift A"
#     curr_date = now_hu.strftime('%Y-%m-%d')
#     start_time_hu = "06:00:00"
#     end_time_hu = "18:00:00"
#     query_date_hu = curr_date
#     logging.info(f"Evening run → Reporting Shift A ({curr_date} 06:00 to 18:00)")

# # ====================== BUILD UTC TIMESTAMPS ======================
# if shift == "Shift A":
#     start_hu = hungary_tz.localize(datetime.strptime(f"{curr_date} {start_time_hu}", "%Y-%m-%d %H:%M:%S"))
#     end_hu = hungary_tz.localize(datetime.strptime(f"{curr_date} {end_time_hu}", "%Y-%m-%d %H:%M:%S"))
# else:  # Shift B
#     start_hu = hungary_tz.localize(datetime.strptime(f"{prev_date} 18:00:00", "%Y-%m-%d %H:%M:%S"))
#     end_hu = hungary_tz.localize(datetime.strptime(f"{curr_date} 06:00:00", "%Y-%m-%d %H:%M:%S"))

# start_ts_utc = start_hu.astimezone(utc).strftime('%Y-%m-%d %H:%M:%S')
# end_ts_utc = end_hu.astimezone(utc).strftime('%Y-%m-%d %H:%M:%S')

# logging.info(f"Querying UTC: {start_ts_utc} to {end_ts_utc}")

# # ====================== MONGO + REDSHIFT + PROCESSING (Same as before) ======================
# from pymongo import MongoClient

# MONGO_URI = "mongodb://10.100.192.93:27017/"
# # MONGO_URI = "mongodb://localhost:27017/"
# client = MongoClient(MONGO_URI)
# mongo_db = client["production_db_hungary"]
# channel_coll = mongo_db["channel_no_ufm"]

# # to check the sku's ---
# # Only fetch SKUs where oe_replacement_slob == "REPL"
# oe_modelno_list = channel_coll.distinct("sku", {"oe_replacement_slob": "REPL"})

# # Keep all SKU strings as-is (SKUs are alphanumeric e.g. "GV2554520ULAWB")
# oe_modelno_list = [str(m) for m in oe_modelno_list if m]

# if not oe_modelno_list:
#     print("Warning: No REPL model numbers found in MongoDB — continuing with hardcoded SKU list in queries.")
# else:
#     print("Model numbers from MongoDB (REPL only):", oe_modelno_list)

# oe_modelno_set = set(oe_modelno_list)

# model_workcenter_db = client["model_workcenter_db_hungary"]
# tbm_gtcodes_coll = model_workcenter_db["model_no_status"]

# active_records = list(tbm_gtcodes_coll.find({"status": "active"}))
# if not active_records:
#     logging.error("No active records found")
#     raise ValueError("No active records found")

# modelno_list = []
# gt_code_set = set()
# for record in active_records:
#     if "sku" in record and record["sku"]:
#         modelno_str = str(record["sku"]).strip()
#         if modelno_str:
#             modelno_list.append(modelno_str)
#     if "gt_code" in record:
#         gt_codes = record["gt_code"]
#         if isinstance(gt_codes, list):
#             gt_code_set.update(gt_codes)
#         elif isinstance(gt_codes, str):
#             gt_code_set.add(gt_codes)

# modelno_list = list(set(modelno_list))

# # Keep only REPL SKUs (oe_replacement_slob == "REPL" from channel_no_ufm). If no REPL
# # SKUs were found, fall back to the full active list.
# if oe_modelno_set:
#     modelno_list = [m for m in modelno_list if m in oe_modelno_set]

# model_numbers = ",".join(f"'{m}'" for m in modelno_list)
# modelno_sql = ",".join(f"'{m}'" for m in modelno_list)
# gt_numbers = ",".join(f"'{gt}'" for gt in sorted(gt_code_set))

# # ... [Mongo data loading code remains same] ...
# channel_data = list(channel_coll.find({"sku": {"$in": modelno_list}}))

# channel_data_list = []
# for record in channel_data:
#     modelno = record.get("sku")
#     gt_code = record.get("gt_code", [])
#     sizecode = record.get("sizecode", "")
#     spec_yield = record.get("spec_overall_uniformity_yield", "")
#     if isinstance(gt_code, list):
#         for gt in gt_code:
#             channel_data_list.append({"sku": modelno, "gt_code": gt, "sizecode": sizecode, "spec_overall_uniformity_yield": spec_yield})
#     else:
#         channel_data_list.append({"sku": modelno, "gt_code": gt_code, "sizecode": sizecode, "spec_overall_uniformity_yield": spec_yield})

# model_nos_selected = pd.DataFrame(channel_data_list)
# model_nos_selected = model_nos_selected[model_nos_selected["gt_code"].isin(gt_code_set)]

# # Redshift Query
# q = f"""
#     SELECT ub.overallufmgrade, ub.sku, tbm.vst_vhmc_mes_gt_code
#     FROM db_prd_plant_gyo.sc_apollo_mom.t_zf_test_completed ub
#     LEFT JOIN db_prd_plant_gyo.sc_mhshistory.t_tbm_pcr_productiondata tbm
#         ON ub.barcode = tbm.vst_vhmc_mes_gt_barcode
#     WHERE ub.timestamp >= '{start_ts_utc}'
#       AND ub.timestamp < '{end_ts_utc}'
#       AND ub.sku IN ({model_numbers})
#       AND tbm.vst_vhmc_mes_gt_code IN ({gt_numbers})
# """

# df = inst.execute_query(q)

# # [Rest of the data processing, merging, yield calculation remains exactly same as previous version]

# if df.empty:
#     logging.warning("No data found for this shift")
# else:
#     tyre_counts = df.groupby(['sku', 'vst_vhmc_mes_gt_code']).size().reset_index(name='Total Tyres Produced')
#     df = df.merge(tyre_counts, on=['sku', 'vst_vhmc_mes_gt_code'], how='left')
#     df = df.rename(columns={'sku': 'SKU', "vst_vhmc_mes_gt_code": "GT Code"})

#     df['SKU'] = df['SKU'].astype(str).str.strip()
#     df['GT Code'] = df['GT Code'].astype(str).str.strip()
#     model_nos_selected['sku'] = model_nos_selected['sku'].astype(str).str.strip()
#     model_nos_selected['gt_code'] = model_nos_selected['gt_code'].astype(str).str.strip()

#     merged_df = df.merge(model_nos_selected, left_on=['SKU', 'GT Code'], right_on=['sku', 'gt_code'], how='left')

#     merged_df['overallufmgrade'] = merged_df['overallufmgrade'].astype(int)
#     # REPL yield formula = count(grades 1,2,3) / count(grades 1,2,3,4,5) * 100.
#     # Numerator = passing grades (1,2,3); denominator = MEASURED grades (1..5),
#     # NOT the raw Total Tyres Produced (which may include grade 0/null).
#     oe_yield_df = merged_df[merged_df['overallufmgrade'].isin([1, 2, 3])].groupby(['SKU','GT Code']).size().reset_index(name="Count_1")
#     measured_df = merged_df[merged_df['overallufmgrade'].isin([1, 2, 3, 4, 5])].groupby(['SKU','GT Code']).size().reset_index(name="Measured_Count")
#     # LEFT join (not inner) so SKU/GT combinations that produced tyres but have
#     # NO passing tyres still appear in the report (yield 0%), including their
#     # grade 3/4/5 counts. Inner join would drop them -> empty table.
#     merged_df = merged_df.merge(oe_yield_df, on=['SKU','GT Code'], how='left')
#     merged_df = merged_df.merge(measured_df, on=['SKU','GT Code'], how='left')
#     merged_df["Count_1"] = merged_df["Count_1"].fillna(0).astype(int)
#     merged_df["Measured_Count"] = merged_df["Measured_Count"].fillna(0).astype(int)

#     # Per-(SKU, GT Code) counts of overallufmgrade 3, 4 and 5 (individual grades).
#     for _grade in (3, 4, 5):
#         _grade_df = (
#             merged_df[merged_df['overallufmgrade'] == _grade]
#             .groupby(['SKU', 'GT Code'])
#             .size()
#             .reset_index(name=f"Grade_{_grade}_Count")
#         )
#         merged_df = merged_df.merge(_grade_df, on=['SKU', 'GT Code'], how='left')
#         merged_df[f"Grade_{_grade}_Count"] = merged_df[f"Grade_{_grade}_Count"].fillna(0).astype(int)

#     # yield = count(1,2,3) / count(1,2,3,4,5) * 100  (0 when no measured tyres)
#     merged_df['Actual REPL Yield (%) (Overallufmgrade)'] = (
#         (merged_df["Count_1"] / merged_df["Measured_Count"].replace(0, pd.NA)) * 100
#     ).round(2).fillna(0.0)
#     merged_df.rename(columns={"spec_overall_uniformity_yield": "REPL Yield Threshold (%)"}, inplace=True)

#     merged_df['REPL Yield Threshold (%)'] = merged_df['REPL Yield Threshold (%)'].apply(
#         lambda x: float(str(x).rstrip('%')) if pd.notna(x) and str(x).strip() != '' else None
#     )

#     merged_df["REPL Dropped (Y/N)"] = merged_df["Actual REPL Yield (%) (Overallufmgrade)"] < merged_df["REPL Yield Threshold (%)"]
#     merged_df["REPL Dropped (Y/N)"] = merged_df["REPL Dropped (Y/N)"].map({True: "Yes", False: "No"})

#     merged_df.drop_duplicates(subset=['SKU','GT Code'], inplace=True)
#     merged_df.drop(columns=['overallufmgrade','Count_1'], inplace=True)

#     final_df = merged_df[[
#         "SKU", "GT Code", "sizecode", "Total Tyres Produced",
#         "Actual REPL Yield (%) (Overallufmgrade)", "REPL Yield Threshold (%)", "REPL Dropped (Y/N)",
#         "Grade_3_Count", "Grade_4_Count", "Grade_5_Count"
#     ]].copy()

#     final_df.rename(columns={"sizecode": "Tyre Description"}, inplace=True)

#     # ====================== TEAMS MESSAGE ======================
#     WEBHOOK_URL = "https://defaultf792002939f941b79c94ec73395d18.4d.environment.api.powerplatform.com:443/powerautomate/automations/direct/cu/18/workflows/a4d8106834a84b589ce56e18fd52f826/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=VM1hm5QYFQA1r3z8X_5hRFYhuzRdxQPTx4aSHP48Qnk"
#     # WEBHOOK_URL = 'https://defaultf792002939f941b79c94ec73395d18.4d.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/67fc14590d7f465e95e10d0d4d516677/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=RZrtfWzs6Dvy19O1W7uAJ1i-5JFRxzBrXX3PKH--6Jk'

#     message = f"**SHIFT-WISE REPL YIELD REPORT {shift} ({start_time_hu} to {end_time_hu} HU) {query_date_hu}**\n\n"
#     message += "| **SKU** | **GT Code** | **Tyre Description** | **Total Tyres Produced** | **Actual REPL Yield (%)** | **REPL Yield Threshold (%)** | **REPL Dropped (Y/N)** | **Overall UFM Grade (3)** | **Overall UFM Grade (4)** | **Overall UFM Grade (5)** |\n"
#     message += "|------------|------------|----------------------|-------------------------|------------------------------------|--------------------------|-------------------|------------------|------------------|------------------|\n"

#     for _, row in final_df.iterrows():
#         oe_dropped_status = "🔴 **Y**" if str(row["REPL Dropped (Y/N)"]).strip().upper() == "YES" else "🟢 **N**"
#         message += (
#             f"| {row['SKU']} | {row['GT Code']} | {row.get('Tyre Description', '')} | "
#             f"{int(row['Total Tyres Produced'])} | {row['Actual REPL Yield (%) (Overallufmgrade)']:.2f}% | "
#             f"{row['REPL Yield Threshold (%)']:.2f}% | {oe_dropped_status} | "
#             f"{int(row['Grade_3_Count'])} | {int(row['Grade_4_Count'])} | {int(row['Grade_5_Count'])} |\n"
#         )

#     payload = {"text": message}
#     response = requests.post(WEBHOOK_URL, json=payload)

#     if response.status_code == 200:
#         logging.info("Alert sent successfully to Teams")
#         print("✅ Alert sent successfully!")
#     else:
#         logging.error(f"Failed to send alert: {response.status_code}")

# logging.info("Script completed.")






import os
import logging
import warnings
from datetime import datetime, timedelta
import pandas as pd
import redshift_connector
import requests
import pytz

# ====================== LOGGING ======================
def configure_logging():
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    hungary_tz = pytz.timezone('Europe/Budapest')
    current_date_hu = datetime.now(hungary_tz).strftime('%Y-%m-%d')
    log_filename = f'logs/log_repl_shift_wise_report_{current_date_hu}.txt'
    
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.FileHandler(log_filename, mode='a', encoding='utf-8')]
    )
    
    warnings.simplefilter("always")
    logging.captureWarnings(True)
    logging.info(f"Logging initialized for Hungary date: {current_date_hu}")
    return log_filename

log_filename = configure_logging()

# ====================== MAIN SCRIPT ======================
class RedshiftDataConnectorr:
    def __init__(self):
        self.conn = redshift_connector.connect(
            host='atl-dl-prd-01.cuvxusm4qify.ap-south-1.redshift.amazonaws.com',
            database='db_prd_plant_gyo',
            port=5439,
            user='usr_digital_twin_readonly',
            password='UXY0%BSJa9dD#vP', sslmode='disable'
        )
    
    def execute_query(self, q):
        try:
            data = pd.read_sql(q, self.conn)
            logging.info(f"Query executed successfully. Rows: {len(data)}")
            return data
        except Exception as e:
            logging.error(f"Query failed: {str(e)}")
            raise

inst = RedshiftDataConnectorr()

hungary_tz = pytz.timezone('Europe/Budapest')
utc = pytz.utc

now_hu = datetime.now(hungary_tz)
logging.info(f"Script started at Hungary time: {now_hu.strftime('%Y-%m-%d %H:%M:%S %Z')}")

# ====================== NEW SHIFT LOGIC AS PER YOUR REQUIREMENT ======================
if now_hu.hour < 12:   # Morning run (around 6:30 AM)
    shift = "Shift B"
    # Previous evening to this morning
    prev_date = (now_hu - timedelta(days=1)).strftime('%Y-%m-%d')
    curr_date = now_hu.strftime('%Y-%m-%d')
    start_time_hu = "18:00:00"
    end_time_hu = "06:00:00"
    query_date_hu = prev_date  # For display purpose
    logging.info(f"Morning run → Reporting previous Shift B ({prev_date} 18:00 to {curr_date} 06:00)")
else:                  # Evening run (around 6:30 PM)
    shift = "Shift A"
    curr_date = now_hu.strftime('%Y-%m-%d')
    start_time_hu = "06:00:00"
    end_time_hu = "18:00:00"
    query_date_hu = curr_date
    logging.info(f"Evening run → Reporting Shift A ({curr_date} 06:00 to 18:00)")

# ====================== BUILD UTC TIMESTAMPS ======================
if shift == "Shift A":
    start_hu = hungary_tz.localize(datetime.strptime(f"{curr_date} {start_time_hu}", "%Y-%m-%d %H:%M:%S"))
    end_hu = hungary_tz.localize(datetime.strptime(f"{curr_date} {end_time_hu}", "%Y-%m-%d %H:%M:%S"))
else:  # Shift B
    start_hu = hungary_tz.localize(datetime.strptime(f"{prev_date} 18:00:00", "%Y-%m-%d %H:%M:%S"))
    end_hu = hungary_tz.localize(datetime.strptime(f"{curr_date} 06:00:00", "%Y-%m-%d %H:%M:%S"))

start_ts_utc = start_hu.astimezone(utc).strftime('%Y-%m-%d %H:%M:%S')
end_ts_utc = end_hu.astimezone(utc).strftime('%Y-%m-%d %H:%M:%S')

logging.info(f"Querying UTC: {start_ts_utc} to {end_ts_utc}")

# ====================== MONGO + REDSHIFT + PROCESSING (Same as before) ======================
from pymongo import MongoClient

MONGO_URI = "mongodb://10.100.192.93:27017/"
# MONGO_URI = "mongodb://localhost:27017/"
client = MongoClient(MONGO_URI)
mongo_db = client["production_db_hungary"]
channel_coll = mongo_db["channel_no_ufm"]

# to check the sku's ---
# Only fetch SKUs where oe_replacement_slob == "REPL"
oe_modelno_list = channel_coll.distinct("sku", {"oe_replacement_slob": "REPL"})

# Keep all SKU strings as-is (SKUs are alphanumeric e.g. "GV2554520ULAWB")
oe_modelno_list = [str(m) for m in oe_modelno_list if m]

if not oe_modelno_list:
    print("Warning: No REPL model numbers found in MongoDB — continuing with hardcoded SKU list in queries.")
else:
    print("Model numbers from MongoDB (REPL only):", oe_modelno_list)

oe_modelno_set = set(oe_modelno_list)

model_workcenter_db = client["model_workcenter_db_hungary"]
tbm_gtcodes_coll = model_workcenter_db["model_no_status"]

active_records = list(tbm_gtcodes_coll.find({"status": "active"}))
if not active_records:
    logging.error("No active records found")
    raise ValueError("No active records found")

modelno_list = []
gt_code_set = set()
for record in active_records:
    if "sku" in record and record["sku"]:
        modelno_str = str(record["sku"]).strip()
        if modelno_str:
            modelno_list.append(modelno_str)
    if "gt_code" in record:
        gt_codes = record["gt_code"]
        if isinstance(gt_codes, list):
            gt_code_set.update(gt_codes)
        elif isinstance(gt_codes, str):
            gt_code_set.add(gt_codes)

modelno_list = list(set(modelno_list))

# Keep only REPL SKUs (oe_replacement_slob == "REPL" from channel_no_ufm). If no REPL
# SKUs were found, fall back to the full active list.
if oe_modelno_set:
    modelno_list = [m for m in modelno_list if m in oe_modelno_set]

model_numbers = ",".join(f"'{m}'" for m in modelno_list)
modelno_sql = ",".join(f"'{m}'" for m in modelno_list)
gt_numbers = ",".join(f"'{gt}'" for gt in sorted(gt_code_set))

# ... [Mongo data loading code remains same] ...
channel_data = list(channel_coll.find({"sku": {"$in": modelno_list}}))

channel_data_list = []
for record in channel_data:
    modelno = record.get("sku")
    gt_code = record.get("gt_code", [])
    sizecode = record.get("sizecode", "")
    spec_yield = record.get("spec_overall_uniformity_yield", "")
    if isinstance(gt_code, list):
        for gt in gt_code:
            channel_data_list.append({"sku": modelno, "gt_code": gt, "sizecode": sizecode, "spec_overall_uniformity_yield": spec_yield})
    else:
        channel_data_list.append({"sku": modelno, "gt_code": gt_code, "sizecode": sizecode, "spec_overall_uniformity_yield": spec_yield})

model_nos_selected = pd.DataFrame(channel_data_list)
model_nos_selected = model_nos_selected[model_nos_selected["gt_code"].isin(gt_code_set)]

# Redshift Query
q = f"""
    SELECT ub.overallufmgrade, ub.sku, tbm.vst_vhmc_mes_gt_code
    FROM db_prd_plant_gyo.sc_apollo_mom.t_zf_test_completed ub
    LEFT JOIN db_prd_plant_gyo.sc_mhshistory.t_tbm_pcr_productiondata tbm
        ON ub.barcode = tbm.vst_vhmc_mes_gt_barcode
    WHERE ub.timestamp >= '{start_ts_utc}'
      AND ub.timestamp < '{end_ts_utc}'
      AND ub.sku IN ({model_numbers})
      AND tbm.vst_vhmc_mes_gt_code IN ({gt_numbers})
"""

df = inst.execute_query(q)

# [Rest of the data processing, merging, yield calculation remains exactly same as previous version]

if df.empty:
    logging.warning("No data found for this shift")
else:
    tyre_counts = df.groupby(['sku', 'vst_vhmc_mes_gt_code']).size().reset_index(name='Total Tyres Produced')
    df = df.merge(tyre_counts, on=['sku', 'vst_vhmc_mes_gt_code'], how='left')
    df = df.rename(columns={'sku': 'SKU', "vst_vhmc_mes_gt_code": "GT Code"})

    df['SKU'] = df['SKU'].astype(str).str.strip()
    df['GT Code'] = df['GT Code'].astype(str).str.strip()
    model_nos_selected['sku'] = model_nos_selected['sku'].astype(str).str.strip()
    model_nos_selected['gt_code'] = model_nos_selected['gt_code'].astype(str).str.strip()

    merged_df = df.merge(model_nos_selected, left_on=['SKU', 'GT Code'], right_on=['sku', 'gt_code'], how='left')

    merged_df['overallufmgrade'] = merged_df['overallufmgrade'].astype(int)
    # REPL yield formula = count(grades 1,2,3) / count(grades 1,2,3,4,5) * 100.
    # Numerator = passing grades (1,2,3); denominator = MEASURED grades (1..5),
    # NOT the raw Total Tyres Produced (which may include grade 0/null).
    oe_yield_df = merged_df[merged_df['overallufmgrade'].isin([1, 2, 3])].groupby(['SKU','GT Code']).size().reset_index(name="Count_1")
    measured_df = merged_df[merged_df['overallufmgrade'].isin([1, 2, 3, 4, 5])].groupby(['SKU','GT Code']).size().reset_index(name="Measured_Count")
    # LEFT join (not inner) so SKU/GT combinations that produced tyres but have
    # NO passing tyres still appear in the report (yield 0%), including their
    # grade 3/4/5 counts. Inner join would drop them -> empty table.
    merged_df = merged_df.merge(oe_yield_df, on=['SKU','GT Code'], how='left')
    merged_df = merged_df.merge(measured_df, on=['SKU','GT Code'], how='left')
    merged_df["Count_1"] = merged_df["Count_1"].fillna(0).astype(int)
    merged_df["Measured_Count"] = merged_df["Measured_Count"].fillna(0).astype(int)

    # Per-(SKU, GT Code) counts of overallufmgrade 3, 4 and 5 (individual grades).
    for _grade in (3, 4, 5):
        _grade_df = (
            merged_df[merged_df['overallufmgrade'] == _grade]
            .groupby(['SKU', 'GT Code'])
            .size()
            .reset_index(name=f"Grade_{_grade}_Count")
        )
        merged_df = merged_df.merge(_grade_df, on=['SKU', 'GT Code'], how='left')
        merged_df[f"Grade_{_grade}_Count"] = merged_df[f"Grade_{_grade}_Count"].fillna(0).astype(int)

    # yield = count(1,2,3) / count(1,2,3,4,5) * 100  (0 when no measured tyres)
    merged_df['Actual REPL Yield (%) (Overallufmgrade)'] = (
        (merged_df["Count_1"] / merged_df["Measured_Count"].replace(0, pd.NA)) * 100
    ).round(2).fillna(0.0)
    merged_df.rename(columns={"spec_overall_uniformity_yield": "REPL Yield Threshold (%)"}, inplace=True)

    merged_df['REPL Yield Threshold (%)'] = merged_df['REPL Yield Threshold (%)'].apply(
        lambda x: float(str(x).rstrip('%')) if pd.notna(x) and str(x).strip() != '' else None
    )

    merged_df["REPL Dropped (Y/N)"] = merged_df["Actual REPL Yield (%) (Overallufmgrade)"] < merged_df["REPL Yield Threshold (%)"]
    merged_df["REPL Dropped (Y/N)"] = merged_df["REPL Dropped (Y/N)"].map({True: "Yes", False: "No"})

    merged_df.drop_duplicates(subset=['SKU','GT Code'], inplace=True)
    # merged_df.drop(columns=['overallufmgrade','Count_1'], inplace=True)
    merged_df.drop(columns=['overallufmgrade'], inplace=True)

    final_df = merged_df[[
        "SKU", "GT Code", "sizecode", "Total Tyres Produced",
        "Actual REPL Yield (%) (Overallufmgrade)", "REPL Yield Threshold (%)", "REPL Dropped (Y/N)",
        "Grade_3_Count", "Grade_4_Count", "Grade_5_Count",
        # Kept so the Grand Total yield can be recomputed consistently
        # (numerator = grades 1,2,3 ; denominator = grades 1,2,3,4,5).
        "Count_1", "Measured_Count"
    ]].copy()

    final_df.rename(columns={"sizecode": "Tyre Description"}, inplace=True)

    # Report order: highest total tyres tested first (High -> Low).
    final_df.sort_values(
        by="Total Tyres Produced", ascending=False, inplace=True
    )
    final_df.reset_index(drop=True, inplace=True)

    # ====================== TEAMS MESSAGE ======================
    WEBHOOK_URL = "https://defaultf792002939f941b79c94ec73395d18.4d.environment.api.powerplatform.com:443/powerautomate/automations/direct/cu/18/workflows/a4d8106834a84b589ce56e18fd52f826/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=VM1hm5QYFQA1r3z8X_5hRFYhuzRdxQPTx4aSHP48Qnk"
    # WEBHOOK_URL = 'https://defaultf792002939f941b79c94ec73395d18.4d.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/67fc14590d7f465e95e10d0d4d516677/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=RZrtfWzs6Dvy19O1W7uAJ1i-5JFRxzBrXX3PKH--6Jk'

    message = f"**SHIFT-WISE REPL YIELD REPORT {shift} ({start_time_hu} to {end_time_hu} HU) {query_date_hu}**\n\n"
    message += "| **SKU** | **GT Code** | **Tyre Description** | **Total Tyres Produced** | **Actual REPL Yield (%)** | **REPL Yield Threshold (%)** | **REPL Dropped (Y/N)** | **Overall UFM Grade (3)** | **Overall UFM Grade (4)** | **Overall UFM Grade (5)** |\n"
    message += "|------------|------------|----------------------|-------------------------|------------------------------------|--------------------------|-------------------|------------------|------------------|------------------|\n"

    # ── Grand Total row (top): totals across all SKUs. The overall REPL yield
    # uses the SAME formula as per-SKU — sum(grades 1,2,3) / sum(grades
    # 1,2,3,4,5) * 100 — not an average of the per-SKU yields.
    gt_total_produced = int(final_df["Total Tyres Produced"].sum())
    gt_count_123 = int(final_df["Count_1"].sum())
    gt_measured = int(final_df["Measured_Count"].sum())
    gt_grade_3 = int(final_df["Grade_3_Count"].sum())
    gt_grade_4 = int(final_df["Grade_4_Count"].sum())
    gt_grade_5 = int(final_df["Grade_5_Count"].sum())
    gt_yield = round((gt_count_123 / gt_measured) * 100, 2) if gt_measured else 0.0
    message += (
        f"| **GRAND TOTAL** | — | **All SKUs** | "
        f"**{gt_total_produced}** | **{gt_yield:.2f}%** | "
        f"— | — | "
        f"**{gt_grade_3}** | **{gt_grade_4}** | **{gt_grade_5}** |\n"
    )

    for _, row in final_df.iterrows():
        oe_dropped_status = "🔴 **Y**" if str(row["REPL Dropped (Y/N)"]).strip().upper() == "YES" else "🟢 **N**"
        message += (
            f"| {row['SKU']} | {row['GT Code']} | {row.get('Tyre Description', '')} | "
            f"{int(row['Total Tyres Produced'])} | {row['Actual REPL Yield (%) (Overallufmgrade)']:.2f}% | "
            f"{row['REPL Yield Threshold (%)']:.2f}% | {oe_dropped_status} | "
            f"{int(row['Grade_3_Count'])} | {int(row['Grade_4_Count'])} | {int(row['Grade_5_Count'])} |\n"
        )

    payload = {"text": message}
    response = requests.post(WEBHOOK_URL, json=payload)

    if response.status_code == 200:
        logging.info("Alert sent successfully to Teams")
        print("✅ Alert sent successfully!")
    else:
        logging.error(f"Failed to send alert: {response.status_code}")

logging.info("Script completed.")