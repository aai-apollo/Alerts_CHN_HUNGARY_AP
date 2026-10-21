# import logging
# import os
# from datetime import datetime, timedelta, timezone
# import time
# from flask import Flask
# from collections import defaultdict
# import config_h
# from collections import Counter

# app = Flask(__name__)


# # last values to iterate
# LAST_VALUES = [1, 0]

# # Trend: last N days
# TREND_DAYS = 7

# # ─────────────────────────────────────────────────────────────────────────────
# # LOGGING
# # ─────────────────────────────────────────────────────────────────────────────

# from logging.handlers import TimedRotatingFileHandler

# # Create logs folder if it does not exist
# LOG_DIR = "logs"
# os.makedirs(LOG_DIR, exist_ok=True)

# # Log file
# LOG_FILE = os.path.join(LOG_DIR, "hungary_historical_database.log")

# # Rotate log file every midnight
# file_handler = TimedRotatingFileHandler(
#     LOG_FILE,
#     when="midnight",
#     interval=1,
#     backupCount=30,
#     encoding="utf-8"
# )

# # Rename rotated files with date
# file_handler.suffix = "%Y-%m-%d"

# # Log format
# formatter = logging.Formatter(
#     "%(asctime)s - %(levelname)s - %(message)s"
# )

# file_handler.setFormatter(formatter)

# # Console logging
# console_handler = logging.StreamHandler()
# console_handler.setFormatter(formatter)

# # Configure logger
# logger = logging.getLogger("hungary_historical_database")
# logger.setLevel(logging.INFO)

# # Avoid duplicate handlers if run/reloaded
# if not logger.handlers:
#     logger.addHandler(file_handler)
#     logger.addHandler(console_handler)

# logger.propagate = False

# # ─────────────────────────────────────────────────────────────────────────────
# # MONGO
# # ─────────────────────────────────────────────────────────────────────────────

# # ─────────────────────────────────────────────────────────────────────────────
# # HELPERS
# # ─────────────────────────────────────────────────────────────────────────────
# def day_window(date_str: str):
#     """Return (start_str, end_str) for 6am-to-6am window of given date."""
#     d = datetime.strptime(date_str, "%Y-%m-%d")
#     start = d.strftime("%Y-%m-%d") + " 06:00:00"
#     end   = (d + timedelta(days=1)).strftime("%Y-%m-%d") + " 05:59:59"
#     return start, end


# def get_tyre_gradings() -> list:
#     """Fetch distinct tyre_grading values from production_db.channel_no_curing."""
#     gradings = [
#         x for x in config_h.channel_no_curing.distinct("oe_replacement_slob")
#         if x and str(x).strip() not in ("", "0", "None")
#     ]
#     if not gradings:
#         logger.warning("No tyre_grading values found in channel_no_curing.oe_replacement_slob")
#     return gradings


# def get_skus_for_grading(grading: str) -> list:
#     """Get SKUs from production_db.channel_no_curing for a given tyre_grading."""
#     import re
#     skus = config_h.channel_no_curing.distinct(
#         "sku",
#         {"oe_replacement_slob": re.compile(f"^{grading}$", re.IGNORECASE)}
#     )
#     skus = [s for s in skus if s]
#     return skus


# def get_qr_statuses_for_grading(grading: str) -> list:
#     """Get distinct QR_Status values for a given tyre_grading from production_db."""
#     import re
#     qr_list = config_h.channel_no_curing.distinct(
#         "QR_Status",
#         {"oe_replacement_slob": re.compile(f"^{grading}$", re.IGNORECASE)}
#     )
#     return [q for q in qr_list if q]


# def get_skus_for_grading_and_qr(grading: str, qr_status: str) -> list:
#     """Get SKUs filtered by both tyre_grading and QR_Status from production_db."""
#     import re
#     skus = config_h.channel_no_curing.distinct(
#         "sku",
#         {
#             "oe_replacement_slob": re.compile(f"^{grading}$", re.IGNORECASE),
#             "QR_Status": qr_status
#         }
#     )
#     return [s for s in skus if s]


# def build_filter(date_str: str, grading: str, last_val: int, skus: list,
#                  qr_status: str = None, sku_filter: str = None) -> dict:
#     start, end = day_window(date_str)

#     # first_yield → rerun=0, last IN [0,1]  (rerun=0,last=0 OR rerun=0,last=1)
#     # final_yield → last=1                  (rerun=0,last=1 OR rerun=1,last=1)
#     if last_val == 0:
#         yield_filter = {"rerun": 0, "last": {"$in": [0, 1]}}
#     else:  # last_val == 1, default: final_yield
#         yield_filter = {"last": 1}

#     flt = {
#         "uf_time": {"$gte": start, "$lte": end},
#         "sku":     {"$in": skus},
#         **yield_filter,
#     }
#     # QR status combos already restrict the SKU list via channel_no_curing.
#     # Use the SKU filter directly from the combination and avoid extra sku_type matching.
#     if sku_filter:
#         flt["sku"] = sku_filter
#     return flt


# # ─────────────────────────────────────────────────────────────────────────────
# # AGGREGATION FUNCTIONS
# # ─────────────────────────────────────────────────────────────────────────────
# def count_last(flt: dict) -> dict:
#     pipeline = [
#         {"$match": flt},
#         {"$group": {"_id": "$last", "count": {"$sum": 1}}}
#     ]
#     result = {}
#     for row in config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True):
#         key = f"last_{row['_id']}"
#         result[key] = row["count"]
#     return result


# def count_overallufmgrade(flt: dict) -> dict:
#     pipeline = [
#         {"$match": flt},
#         {"$group": {"_id": "$overallufmgrade", "count": {"$sum": 1}}}
#     ]
#     result = {}
#     for row in config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True):
#         key = f"overallufmgrade_{row['_id']}"
#         result[key] = row["count"]
#     return result


# def count_vi_yield(flt: dict) -> dict:
#     pipeline = [
#         {"$match": flt},
#         {"$group": {"_id": "$vi_yield", "count": {"$sum": 1}}}
#     ]
#     result = {}
#     for row in config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True):
#         key = f"vi_yield_{row['_id']}"
#         result[key] = row["count"]
#     return result

# def count_uf_yield(flt: dict) -> dict:
#     pipeline = [
#         {"$match": flt},
#         {"$group": {"_id": "$uf_yield", "count": {"$sum": 1}}}
#     ]
#     result = {}
#     for row in config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True):
#         key = f"uf_yield_{row['_id']}"
#         result[key] = row["count"]
#     return result

# def count_uf_downgrade_condition(flt: dict) -> dict:
#     """Return Numerator and UF_MeasuredSum counts."""

#     base_query = {
#         **flt,
#         "vi_yield": 1,
#         "$or": [
#             {"overalltugrade": {"$in": [2, 3, 4, 5]}},
#             {"overallbalgrade": {"$in": [2, 3, 4, 5]}},
#             {"overallsolgrade": {"$in": [2, 3, 4, 5]}}
#         ]
#     }

#     numerator = config_h.data_merged_summary_historical_data.count_documents(base_query)

#     uf_measuredsum = config_h.data_merged_summary_historical_data.count_documents({
#         **flt,
#         "overallufmgrade": {"$in": [2, 3, 4, 5]}
#     })

#     return {
#         "Numerator": numerator,
#         "uf_MeasuredSum": uf_measuredsum
#     }

# def card_split_counts(flt: dict) -> dict:
#     """
#     Returns actual document counts for tyre_bifer and rework_split using count_documents.
    
#     Args:
#         collection: MongoDB collection object
#         flt: Base filter dictionary
#     """
    
#     # Tyre Bifer
#     tyre_bifer = {
#         "count_vi": config_h.data_merged_summary_historical_data.count_documents({
#             **flt,
#             "overallufmgrade": {"$in": [2, 3]},
#             "vi_yield": 0
#         }),
#         "count_uf": config_h.data_merged_summary_historical_data.count_documents({
#             **flt,
#             "overallufmgrade": {"$in": [2, 3]},
#             "vi_yield": 1,
#             "$or": [
#                 {"overalltugrade": {"$in": [2, 3, 4, 5]}},
#                 {"overallbalgrade": {"$in": [2, 3, 4, 5]}},
#                 {"overallsolgrade": {"$in": [2, 3, 4, 5]}}
#             ]
#         })
#     }
    
#     # Rework Split
#     rework_split = {
#         "count_vi": config_h.data_merged_summary_historical_data.count_documents({
#             **flt,
#             "overallufmgrade": 4,
#             "vi_yield": 0
#         }),
#         "count_uf": config_h.data_merged_summary_historical_data.count_documents({
#             **flt,
#             "overallufmgrade": 4,
#             "vi_yield": 1,
#             "$or": [
#                 {"overalltugrade": {"$in": [2, 3, 4, 5]}},
#                 {"overallbalgrade": {"$in": [2, 3, 4, 5]}},
#                 {"overallsolgrade": {"$in": [2, 3, 4, 5]}}
#             ]
#         })
#     }
    
#     return {
#         "tyre_bifer": tyre_bifer,
#         "rework_split": rework_split
#     }

# def count_re_code(flt: dict) -> dict:
#     pipeline = [
#         {"$match": {**flt, "re_code": {"$ne": None, "$ne": ""}}},
#         {"$group": {"_id": "$re_code", "count": {"$sum": 1}}},
#         {"$sort": {"count": -1}}
#     ]
#     return {str(row["_id"]): row["count"] for row in config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True) if row["_id"] is not None}


# import re



# def count_re_comment(flt: dict) -> dict:

#     match_filter = {
#         **flt,
#         "vi_yield": 0,
#         "re_comment": {"$nin": [None, ""]}
#     }

#     pipeline = [
#         {"$match": match_filter},
#         {
#             "$group": {
#                 "_id": "$re_comment",
#                 "count": {"$sum": 1}
#             }
#         },
#         {"$sort": {"count": -1}}
#     ]

#     rows = list(config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True))

#     # --------------------------------------------------------
#     # Extract VI reason codes
#     # --------------------------------------------------------

#     reason_codes = set()

#     for row in rows:

#         comment = str(row["_id"])

#         match = re.search(
#             r"The VI Reason was changed:\s*(\d+)\s*-->",
#             comment
#         )

#         if match:
#             reason_codes.add(match.group(1))

#     # --------------------------------------------------------
#     # Fetch descriptions
#     # --------------------------------------------------------

#     reason_map = {
#         x["condition_code"]: x.get("description", "")
#         for x in config_h.re_codes.find(
#             {
#                 "condition_code": {
#                     "$in": list(reason_codes)
#                 }
#             },
#             {
#                 "_id": 0,
#                 "condition_code": 1,
#                 "description": 1
#             }
#         )
#     }

#     # --------------------------------------------------------
#     # Final result
#     # --------------------------------------------------------

#     final_result = {}

#     for row in rows:

#         comment = str(row["_id"])
#         count = row["count"]

#         match = re.search(
#             r"The VI Reason was changed:\s*(\d+)\s*-->",
#             comment
#         )

#         if match:

#             code = match.group(1)

#             description = reason_map.get(code)

#             if description:
#                 final_result[description] = (
#                     final_result.get(description, 0) + count
#                 )
#             else:
#                 final_result[comment] = (
#                     final_result.get(comment, 0) + count
#                 )

#         else:
#             final_result[comment] = (
#                 final_result.get(comment, 0) + count
#             )

#     return final_result

# def oe_tyre_bifer(flt: dict) -> dict:
#     pipeline = [
#         {
#             "$match": {
#                 **flt,
#                 "overallufmgrade": 1
#             }
#         },
#         {
#             "$group": {
#                 "_id": {"$ifNull": ["$re_comment", "NULL"]},
#                 "count": {"$sum": 1}
#             }
#         },
#         {"$sort": {"count": -1}}
#     ]

#     return {
#         str(row["_id"]): row["count"]
#         for row in config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True)
#     }

# # ─────────────────────────────────────────────────────────────────────────────
# # SCRAP COLLECTION
# # ─────────────────────────────────────────────────────────────────────────────
# # config_h.data_merged_scrap_historical_data is now initialized in MONGO section above


# def get_scrap_filter(start_time: str, end_time: str, skus: list) -> dict:
#     """
#     Create filter for scrap collection using last_change_time and matching SKUs.
    
#     Args:
#         start_time: Start datetime string (e.g., "2026-05-15 06:00:00")
#         end_time: End datetime string (e.g., "2026-05-16 05:59:59")
#         skus: List of SKU values to filter by
    
#     Returns:
#         Filter dict for scrap collection query
#     """
#     return {
#         "last_change_time": {"$gte": start_time, "$lte": end_time},
#         "sku": {"$in": skus}
#     }


# def count_scrap_categories(start_time: str, end_time: str, skus: list) -> tuple:
#     """
#     Returns (visual_count, uf_count) from data_merged_scrap,
#     filtered by last_change_time and matching SKUs.
    
#     Args:
#         start_time: Start datetime string (e.g., "2026-05-15 06:00:00")
#         end_time: End datetime string (e.g., "2026-05-16 05:59:59")
#         skus: List of SKU values to filter by
    
#     Returns:
#         Tuple of (visual_count, uf_count)
#     """
#     scrap_flt = get_scrap_filter(start_time, end_time, skus)
#     pipeline = [
#         {"$match": scrap_flt},
#         {"$group": {"_id": "$scrap_category", "count": {"$sum": 1}}}
#     ]
#     visual, uf = 0, 0
#     for row in config_h.data_merged_scrap_historical_data.aggregate(pipeline, allowDiskUse=True):
#         cat = (row["_id"] or "").upper()
#         if cat == "VISUAL":
#             visual = row["count"]
#         elif cat == "UF":
#             uf = row["count"]
#     return visual, uf


# def count_condition_code(start_time: str, end_time: str, skus: list) -> dict:
#     """
#     Counts per condition_code from data_merged_scrap,
#     filtered by last_change_time and matching SKUs.
    
#     Args:
#         start_time: Start datetime string
#         end_time: End datetime string
#         skus: List of SKU values
    
#     Returns:
#         Dict mapping condition_code to count
#     """
#     scrap_flt = get_scrap_filter(start_time, end_time, skus)
#     pipeline = [
#         {"$match": {**scrap_flt, "condition_code": {"$ne": None, "$ne": ""}}},
#         {"$group": {"_id": "$condition_code", "count": {"$sum": 1}}},
#         {"$sort": {"count": -1}}
#     ]
#     return {str(row["_id"]): row["count"] for row in config_h.data_merged_scrap_historical_data.aggregate(pipeline, allowDiskUse=True) if row["_id"] is not None}


# def tyre_counts_by_sku(flt: dict,  grading: str = "OE") -> dict:
#     """
#     Per-SKU counts based on overallufmgrade:
#       total_tyres  → overallufmgrade == 1
#       rework       → overallufmgrade IN [2, 3]
#       replacement  → overallufmgrade == 4
#       rejected     → overallufmgrade == 5
#     """
#     if grading == "REPL":
#         oe_grade = {"$in": ["$overallufmgrade", [1, 2, 3]]}
#     else:
#         oe_grade = {"$eq": ["$overallufmgrade", 1]}

#     pipeline = [
#         {"$match": flt},
#         {"$group": {
#             "_id":        "$sku",
#             "total":      {"$sum": {"$cond": [oe_grade, 1, 0]}},
#             "rework":     {"$sum": {"$cond": [{"$in": ["$overallufmgrade", [2, 3]]}, 1, 0]}},
#             "replacement":{"$sum": {"$cond": [{"$eq": ["$overallufmgrade", 4]}, 1, 0]}},
#             "rejected":   {"$sum": {"$cond": [{"$eq": ["$overallufmgrade", 5]}, 1, 0]}},
#         }}
#     ]
#     total_d = {}; rework_d = {}; repl_d = {}; rej_d = {}
#     for row in config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True):
#         sku = row["_id"] or "unknown"
#         total_d[sku]  = row["total"]
#         rework_d[sku] = row["rework"]
#         repl_d[sku]   = row["replacement"]
#         rej_d[sku]    = row["rejected"]
#     return {
#         "oe_tyres":  total_d,
#         "rework":       rework_d,
#         "replacement":  repl_d,
#         "rejected":     rej_d
#     }

# def count_uf_downgrade(flt: dict, grading: str) -> int:
#     """Count tyres where vi_yield == 1 and overallufmgrade != 1 using the base filter."""
    
#     query = {**flt}

#     if grading == "REPL":
#         query["overallufmgrade"] = {"$nin": [1, 2, 3]}
#     else:  # OE
#         query["overallufmgrade"] = {"$ne": 1}

#     return config_h.data_merged_summary_historical_data.count_documents(query)

# def uf_downgrade_breakdown(flt: dict) -> dict:

#         # 👉 Add VI filter locally (without mutating original filter)
#     match_filter = {**flt, "vi_yield": 1}

#     """Count tyres where each grade param > 1 (UF downgrade reason)."""
#     uf_params = [
#         ("lrograde",    "LRO"),   ("conygrade",   "CONY"),
#         ("rfppwcgrade", "RFPP"),  ("rfh1wcgrade", "RFH1"),
#         ("rfh2wcgrade", "RFH2"),  ("rfh3wcgrade", "RFH3"),
#         ("lfppwcgrade", "LFPP"),  ("crrograde",   "CRRQ"),
#         ("trrograde",   "TRRQ"),  ("brrograde",   "BRRQ"),
#         ("plysgrade",   "PLYS"),  ("sbalgrade",   "SBAL"),
#         ("bulwcgrade",  "BUL"),   ("depwcgrade",  "DEP"),
#         ("uppergrade",  "UPPER"), ("lowergrade",  "LOWER"),
#     ]
#     # add_fields = {label: {"$cond": [{"$gt": [f"${field}", 1]}, 1, 0]} for field, label in uf_params}
#     add_fields = {
#         label: {
#             "$cond": [
#                 {"$in": [f"${field}", [2, 3, 4, 5]]},
#                 1,
#                 0
#             ]
#         }
#         for field, label in uf_params
#     }
#     group_sum  = {label: {"$sum": f"${label}"} for _, label in uf_params}
#     pipeline   = [
#         {"$match": match_filter},
#         {"$addFields": add_fields},
#         {"$group": {"_id": None, **group_sum}}
#     ]
#     agg = list(config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True))
#     if not agg:
#         return {label: 0 for _, label in uf_params}
#     row = agg[0]
#     return dict(sorted({label: row.get(label, 0) for _, label in uf_params}.items(),
#                         key=lambda x: x[1], reverse=True))

# def sku_wise_warehouse_yield(flt: dict) -> dict:
#     """Per-SKU overallufmgrade value counts + scrap_cateogry_visual (SKU-wise)."""
#     # Step 1: grade counts + barcodes per SKU from summary
#     pipeline = [
#         {"$match": flt},
#         {"$group": {
#             "_id":   "$sku",
#             "g0":    {"$sum": {"$cond": [{"$eq": ["$overallufmgrade", 0]}, 1, 0]}},
#             "g1":    {"$sum": {"$cond": [{"$eq": ["$overallufmgrade", 1]}, 1, 0]}},
#             "g2":    {"$sum": {"$cond": [{"$eq": ["$overallufmgrade", 2]}, 1, 0]}},
#             "g3":    {"$sum": {"$cond": [{"$eq": ["$overallufmgrade", 3]}, 1, 0]}},
#             "g4":    {"$sum": {"$cond": [{"$eq": ["$overallufmgrade", 4]}, 1, 0]}},
#             "g5":    {"$sum": {"$cond": [{"$eq": ["$overallufmgrade", 5]}, 1, 0]}},
#             "barcodes": {"$push": "$barcode"}
#         }}
#     ]
#     rows = list(config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True))
#     # Step 2: get scrap_category == "VISUAL" per SKU by filtering scrap collection
#     # using last_change_time range and SKU list extracted from the incoming filter.
#     # This avoids joining via barcodes and follows the new requirement.
#     vi_scrap_by_sku = {}

#     # Extract time range from filter (uf_time expected)
#     start_time = None
#     end_time = None
#     if "uf_time" in flt and isinstance(flt["uf_time"], dict):
#         start_time = flt["uf_time"].get("$gte") or flt["uf_time"].get("$gt")
#         end_time = flt["uf_time"].get("$lte") or flt["uf_time"].get("$lt")

#     # Extract SKU list from filter
#     sku_filter = flt.get("sku")
#     sku_list = None
#     if isinstance(sku_filter, dict) and "$in" in sku_filter:
#         sku_list = sku_filter["$in"]
#     elif isinstance(sku_filter, list):
#         sku_list = sku_filter
#     elif isinstance(sku_filter, str):
#         sku_list = [sku_filter]

#     # Build scrap query only if we have a time range and SKU list
#     if start_time and end_time and sku_list:
#         scrap_query = {
#             "last_change_time": {"$gte": start_time, "$lte": end_time},
#             "sku": {"$in": sku_list},
#             "scrap_category": "VISUAL"
#         }
#         for row in config_h.data_merged_scrap_historical_data.aggregate([
#             {"$match": scrap_query},
#             {"$group": {"_id": "$sku", "count": {"$sum": 1}}}
#         ], allowDiskUse=True):
#             if row and row.get("_id"):
#                 vi_scrap_by_sku[row["_id"]] = row.get("count", 0)

#     # Step 3: build result
#     result = {}
#     for row in rows:
#         sku = row["_id"] or "unknown"
#         result[sku] = {
#             "overallufmgrade_0":    row["g0"],
#             "overallufmgrade_1":    row["g1"],
#             "overallufmgrade_2":    row["g2"],
#             "overallufmgrade_3":    row["g3"],
#             "overallufmgrade_4":    row["g4"],
#             "overallufmgrade_5":    row["g5"],
#             "scrap_cateogry_visual": vi_scrap_by_sku.get(sku, 0),
#         }
#     return result

# from collections import Counter

# def sku_wise_trend(date_str: str, grading: str, last_val: int, skus: list) -> dict:
#     """
#     SKU-wise 7-day trend for UFM, GEO, BAL yields.
#     Structure: SKU → category → param → date → machine_type → machine → {
#         numerator, oe_tyres, denominator,
#         overallufmgrade: {overallufmgrade_<grade>: count, ...},
#         <grade_field>: {<grade_field>_<grade>: count, ...}
#     }
#     Grades are dynamic (any integer values present in data) — no hardcoding of 1/2/3.
#     One aggregation pipeline per day (not per param/machine) for efficiency.
#     numerator   = count(grade_col meets OE/REPL condition) per machine
#     denominator = count(overallufmgrade IN 1-5) per machine
#     """
#     all_params = {
#         "UFM": [
#             ("rfv",      "rfppwcgrade"),
#             ("rfh1",     "rfh1wcgrade"),
#             ("rfh2",     "rfh2wcgrade"),
#             ("lfv",      "lfppwcgrade"),
#             ("conicity", "conygrade"),
#             ("plysteer", "plysgrade"),
#         ],
#         "GEO": [
#             ("crro",   "crrograde"),
#             ("trro",   "trrograde"),
#             ("brro",   "brrograde"),
#             ("lro",    "lrograde"),
#             ("lroh1",  "lroh1grade"),
#             ("crroh1", "crroh1grade"),
#             ("trroh1", "trroh1grade"),
#             ("brroh1", "brroh1grade"),
#             ("lrro",   "lrrograde"),
#         ],
#         "BAL": [
#             ("sbal",   "sbalgrade"),
#             ("couple", "couplegrade"),
#             ("upper",  "uppergrade"),
#             ("lower",  "lowergrade"),
#         ],
#     }
#     machine_fields = [("tbm", "ptb_id"), ("curing", "cur_id"), ("ufm", "uf_id")]

#     # === FIXED SCRAP FETCHING (No machine grouping) ===
#     start, end = day_window(date_str)
#     vi_scrap_map = {}   # Will store by sku only now
#     scrap_pipeline = [
#         {
#             "$match": {
#                 "last_change_time": {"$gte": start, "$lt": end},
#                 "sku": {"$in": skus},
#                 "scrap_category": "VISUAL"
#             }
#         },
#         {
#             "$group": {
#                 "_id": "$sku",
#                 "vi_scrap": {"$sum": 1}
#             }
#         }
#     ]
#     for row in config_h.data_merged_scrap_historical_data.aggregate(scrap_pipeline, allowDiskUse=True):
#         sku = row["_id"]
#         vi_scrap_map[sku] = row.get("vi_scrap", 0)

#     # Flat list of all (label, field) pairs
#     all_grade_fields = [
#         (label, field)
#         for params in all_params.values()
#         for label, field in params
#     ]
#     # Map label → category
#     label_to_cat = {
#         label: cat
#         for cat, params in all_params.items()
#         for label, _ in params
#     }
#     # Map label → field name (for the dynamic grade key)
#     label_to_field = {label: field for label, field in all_grade_fields}

#     # Init result structure
#     result = {
#         sku: {
#             cat: {label: {} for label, _ in params}
#             for cat, params in all_params.items()
#         }
#         for sku in skus
#     }
#     # Add yields section for overall yields
#     for sku in skus:
#         result[sku]["yields"] = {
#             "warehouse":   {},
#             "uniformity":  {},
#             "visual":      {},
#         }

#     # Build date windows — only the running date
#     start, end = day_window(date_str)
#     days = [(date_str, start, end)]

#     # first_yield → rerun=0, last IN [0,1]
#     # final_yield → last=1
#     if last_val == 0:
#         yield_filter = {"rerun": 0, "last": {"$in": [0, 1]}}
#     else:
#         yield_filter = {"last": 1}

#     # Determine the overallufmgrade condition based on tyre grading:
#     # OE  → grade == 1
#     # REPL → grade in [1, 2, 3]
#     if grading == "REPL":
#         oe_grade_cond = {"$in": ["$overallufmgrade", [1, 2, 3]]}
#     else:  # default: OE
#         oe_grade_cond = {"$eq": ["$overallufmgrade", 1]}

#     # Determine the uf_pass condition based on tyre grading
#     if grading == "REPL":
#         uf_pass_cond = {
#             "$and": [
#                 oe_grade_cond,
#                 {"$eq": ["$vi_yield", 1]}
#             ]
#         }
#     else:
#         uf_pass_cond = oe_grade_cond

#     for d, start, end in days:
#         base_flt = {
#             "uf_time":         {"$gte": start, "$lt": end},
#             "sku":             {"$in": skus},
#             "overallufmgrade": {"$in": [1, 2, 3, 4, 5]},
#             **yield_filter,
#         }
#         for mtype, mfield in machine_fields:
#             # denominator = SUM(overallufmgrade IN 1-5) per {sku, machine}
#             # numerator   = SUM(grade_field meets condition) per {sku, machine}
#             # + push full grade lists so we can build dynamic frequency maps
#             group_stage = {
#                 "_id":         {"sku": "$sku", "machine": f"${mfield}"},
#                 "denominator": {"$sum": 1},
#                 "oe_tyres":    {"$sum": {"$cond": [oe_grade_cond, 1, 0]}},
#                 # for overall yields
#                 "vi_downgrade":  {"$sum": {"$cond": [{"$eq": ["$vi_yield", 0]}, 1, 0]}},
#                 "uf_pass_as_oe": {
#                     "$sum": {
#                         "$cond": [uf_pass_cond, 1, 0]
#                     }
#                 },
#                 # Dynamic grade distributions (no hardcoded 1/2/3)
#                 "overallufmgrade_list": {"$push": "$overallufmgrade"},
#             }
#             for label, field in all_grade_fields:
#                 if grading == "REPL":
#                     param_grade_cond = {
#                         "$in": [f"${field}", [1, 2, 3]]
#                     }
#                 else:
#                     param_grade_cond = {
#                         "$eq": [f"${field}", 1]
#                     }
#                 group_stage[label] = {
#                     "$sum": {
#                         "$cond": [param_grade_cond, 1, 0]
#                     }
#                 }
#                 # Push the raw grade values for this parameter
#                 group_stage[f"{label}_list"] = {"$push": f"${field}"}

#             pipeline = [
#                 {"$match": base_flt},
#                 {"$group": group_stage}
#             ]
#             for row in config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True):
#                 sku     = row["_id"].get("sku")
#                 machine = row["_id"].get("machine")
#                 if not sku or not machine or sku not in result:
#                     continue
#                 denom    = row["denominator"]
#                 vi_dg    = row.get("vi_downgrade",  0)
#                 uf_pass  = row.get("uf_pass_as_oe", 0)
#                 oe       = row.get("oe_tyres",      0)

#                 # Build dynamic overallufmgrade frequency map (any grades present)
#                 overall_counts = Counter(row.get("overallufmgrade_list", []))
#                 overall_map = {
#                     f"overallufmgrade_{g}": cnt
#                     for g, cnt in overall_counts.items()
#                 }

#                 # UFM / GEO / BAL params
#                 for label, field in all_grade_fields:
#                     cat = label_to_cat[label]
#                     if d not in result[sku][cat][label]:
#                         result[sku][cat][label][d] = {"tbm": {}, "curing": {}, "ufm": {}}

#                     # Build dynamic parameter grade frequency map (any grades present)
#                     param_counts = Counter(row.get(f"{label}_list", []))
#                     param_map = {
#                         f"{field}_{g}": cnt
#                         for g, cnt in param_counts.items()
#                     }

#                     result[sku][cat][label][d][mtype][str(machine)] = {
#                         "numerator":   row.get(label, 0),
#                         "oe_tyres":    oe,
#                         "denominator": denom,
#                         "overallufmgrade": overall_map,
#                         field: param_map,          # e.g. "rfppwcgrade": {"rfppwcgrade_1": 18, ...}
#                     }

#                 # Overall yields — machine-wise (unchanged)
#                 machine_str = str(machine)
#                 vi_pass  = max(denom - vi_dg, 0)
#                 uf_denom = max(denom - vi_dg, 0)
#                 vi_scrap = vi_scrap_map.get(sku, 0)
#                 warehouse_denom = denom + vi_scrap
#                 uniformity_denom = max(denom - vi_dg, 0)
#                 visual_denom = denom + vi_scrap
#                 visual_num = max(denom - vi_dg, 0)

#                 # warehouse
#                 if d not in result[sku]["yields"]["warehouse"]:
#                     result[sku]["yields"]["warehouse"][d] = {
#                         "tbm": {}, "curing": {}, "ufm": {}
#                     }
#                 result[sku]["yields"]["warehouse"][d][mtype][machine_str] = {
#                     "numerator": oe,
#                     "oe_tyres": oe,
#                     "denominator": warehouse_denom,
#                 }
#                 # uniformity
#                 if d not in result[sku]["yields"]["uniformity"]:
#                     result[sku]["yields"]["uniformity"][d] = {
#                         "tbm": {}, "curing": {}, "ufm": {}
#                     }
#                 result[sku]["yields"]["uniformity"][d][mtype][machine_str] = {
#                     "numerator": uf_pass,
#                     "oe_tyres": oe,
#                     "denominator": uniformity_denom,
#                 }
#                 # visual
#                 if d not in result[sku]["yields"]["visual"]:
#                     result[sku]["yields"]["visual"][d] = {
#                         "tbm": {}, "curing": {}, "ufm": {}
#                     }
#                 result[sku]["yields"]["visual"][d][mtype][machine_str] = {
#                     "numerator": visual_num,
#                     "oe_tyres": oe,
#                     "denominator": visual_denom,
#                 }
#     return result

# def compute_yields(flt: dict, grading: str = "OE") -> dict:
#     """
#     SKU-wise counts where each grade column == 1 (OE) or in [1,2,3] (REPL).
#     overallufmgrade_sum = total docs per SKU with grade IN 1-5.

#     Last logic:
#       last == 1 → filter: last == 1
#       last == 0 → filter: rerun == 0 AND last IN [0, 1]
#     """
#     grade_fields = {
#         "ufm_yields": [
#             ("rfv",      "rfppwcgrade"),
#             ("rfh1",     "rfh1wcgrade"),
#             ("rfh2",     "rfh2wcgrade"),
#             ("lfv",      "lfppwcgrade"),
#             ("conicity", "conygrade"),
#             ("plysteer", "plysgrade"),
#         ],
#         "geo_yields": [
#             ("crro",   "crrograde"),
#             ("trro",   "trrograde"),
#             ("brro",   "brrograde"),
#             ("lro",    "lrograde"),
#             ("lroh1",  "lroh1grade"),
#             ("crroh1", "crroh1grade"),
#             ("trroh1", "trroh1grade"),
#             ("brroh1", "brroh1grade"),
#             ("lrro",   "lrrograde"),
#         ],
#         "bal_yields": [
#             ("sbal",   "sbalgrade"),
#             ("couple", "couplegrade"),
#             ("upper",  "uppergrade"),
#             ("lower",  "lowergrade"),
#         ],
#     }

#     # Apply last logic on top of existing filter
#     # The filter flt already contains the correct last/rerun MongoDB expressions
#     # from build_filter, so we use it directly without re-deriving
#     yield_flt = dict(flt)

#     # OE → grade == 1 | REPL → grade in [1, 2, 3]
#     if grading == "REPL":
#         oe_grade_cond = {"$in": ["$overallufmgrade", [1, 2, 3]]}
#         param_grade_cond = lambda field: {"$in": [f"${field}", [1, 2, 3]]}
#     else:
#         oe_grade_cond = {"$eq": ["$overallufmgrade", 1]}
#         param_grade_cond = lambda field: {"$eq": [f"${field}", 1]}

#     if grading == "REPL":
#         uf_pass_cond = {
#             "$and": [
#                 oe_grade_cond,
#                 {"$eq": ["$vi_yield", 1]}
#             ]
#         }
#     else:
#         uf_pass_cond = oe_grade_cond

#     group_stage = {
#         "_id":                 "$sku",
#         "overallufmgrade_sum": {"$sum": {"$cond": [{"$and": [{"$gte": ["$overallufmgrade", 1]}, {"$lte": ["$overallufmgrade", 5]}]}, 1, 0]}},
#         "pass_as_oe":          {"$sum": {"$cond": [oe_grade_cond, 1, 0]}},
#         "vi_downgrade":        {"$sum": {"$cond": [{"$eq": ["$vi_yield", 0]}, 1, 0]}},
#         "uf_pass_as_oe": {
#             "$sum": {
#                 "$cond": [uf_pass_cond, 1, 0]
#             }
#         },
#         "barcodes":            {"$push": "$barcode"},
#     }
#     for _, fields in grade_fields.items():
#         for label, field in fields:
#             group_stage[label] = {"$sum": {"$cond": [param_grade_cond(field), 1, 0]}}
#             group_stage[f"{field}_values"] = {"$push": f"${field}"}

#     pipeline = [
#         {"$match": yield_flt},
#         {"$group": group_stage}
#     ]

#     rows = list(config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True))

#     # scrap_category_visual per SKU + total scrap_count via last_change_time + sku
#     # Extract time range from yield_flt (uf_time expected)
#     start_time = None
#     end_time = None
#     if "uf_time" in yield_flt and isinstance(yield_flt["uf_time"], dict):
#         start_time = yield_flt["uf_time"].get("$gte") or yield_flt["uf_time"].get("$gt")
#         end_time = yield_flt["uf_time"].get("$lte") or yield_flt["uf_time"].get("$lt")

#     # Build SKU list from aggregated rows
#     sku_list = [r["_id"] for r in rows if r.get("_id")]

#     vi_scrap_by_sku = {}
#     total_scrap_by_sku = {}

#     if start_time and end_time and sku_list:
#         # visual scrap per sku
#         visual_query = {
#             "last_change_time": {"$gte": start_time, "$lte": end_time},
#             "sku": {"$in": sku_list},
#             "scrap_category": "VISUAL"
#         }
#         for sc_row in config_h.data_merged_scrap_historical_data.aggregate([
#             {"$match": visual_query},
#             {"$group": {"_id": "$sku", "count": {"$sum": 1}}}
#         ], allowDiskUse=True):
#             if sc_row and sc_row.get("_id"):
#                 vi_scrap_by_sku[sc_row["_id"]] = sc_row.get("count", 0)

#         # total scrap per sku (no category filter)
#         total_query = {
#             "last_change_time": {"$gte": start_time, "$lte": end_time},
#             "sku": {"$in": sku_list}
#         }
#         for sc_row in config_h.data_merged_scrap_historical_data.aggregate([
#             {"$match": total_query},
#             {"$group": {"_id": "$sku", "count": {"$sum": 1}}}
#         ], allowDiskUse=True):
#             if sc_row and sc_row.get("_id"):
#                 total_scrap_by_sku[sc_row["_id"]] = sc_row.get("count", 0)
#     def build_parameter_counts(row, fields):
#         parameter_counts = {}

#         for _, field in fields:
#             values = row.get(f"{field}_values", [])

#             counts = Counter(
#                 value for value in values
#                 if value is not None
#             )

#             parameter_counts[field] = {
#                 f"{field}_{grade}": count
#                 for grade, count in counts.items()
#             }

#         return parameter_counts
#     sku_results = {}
    
#     for row in rows:
#         sku = row["_id"] or "unknown"
#         sku_results[sku] = {
#             "overallufmgrade_sum":   row.get("overallufmgrade_sum", 0),
#             "pass_as_oe":            row.get("pass_as_oe", 0),
#             "vi_downgrade":          row.get("vi_downgrade", 0),
#             "uf_pass_as_oe":         row.get("uf_pass_as_oe", 0),
#             "scrap_category_visual": vi_scrap_by_sku.get(sku, 0),
#             "scrap_count":           total_scrap_by_sku.get(sku, 0),
#             "ufm_yields": {
#                 **{
#                     label: row.get(label, 0)
#                     for label, _ in grade_fields["ufm_yields"]
#                 },
#                 "parameter_counts": build_parameter_counts(
#                     row,
#                     grade_fields["ufm_yields"]
#                 )
#             },

#             "geo_yields": {
#                 **{
#                     label: row.get(label, 0)
#                     for label, _ in grade_fields["geo_yields"]
#                 },
#                 "parameter_counts": build_parameter_counts(
#                     row,
#                     grade_fields["geo_yields"]
#                 )
#             },

#             "bal_yields": {
#                 **{
#                     label: row.get(label, 0)
#                     for label, _ in grade_fields["bal_yields"]
#                 },
#                 "parameter_counts": build_parameter_counts(
#                     row,
#                     grade_fields["bal_yields"]
#                 )
#             },
#         }

#     return sku_results


# UF_PARAMETER_MAP = {
#     "LRO": "lrograde",
#     "CONY": "conygrade",
#     "RFPP": "rfppwcgrade",
#     "RFH1": "rfh1wcgrade",
#     "RFH2": "rfh2wcgrade",
#     "RFH3": "rfh3wcgrade",
#     "LFPP": "lfppwcgrade",
#     "CRRQ": "crrograde",
#     "TRRQ": "trrograde",
#     "BRRQ": "brrograde",
#     "PLYS": "plysgrade",
#     "SBAL": "sbalgrade",
#     "BUL": "bulwcgrade",
#     "DEP": "depwcgrade",
#     "UPPER": "uppergrade",
#     "LOWER": "lowergrade"
# }

# def _get_re_code_map(db):
#     """Build {condition_code_int: description} from re_codes collection."""
#     re_codes_col = db["re_codes"]
#     code_map = {}
#     for doc in re_codes_col.find({}, {"condition_code": 1, "description": 1}):
#         try:
#             code_map[int(doc["condition_code"])] = doc.get("description", str(doc["condition_code"]))
#         except (ValueError, TypeError):
#             pass
#     return code_map

# def compute_sku_count(flt: dict, start_time: str, end_time: str) -> dict:
#     """
#     SKU-wise counts for ALL flags and parameters.

#     Returns:
#     {
#         "sku_count": {
#             "uf": {
#                 "LRO": {"GV1": 10, "GV2": 5},
#                 "CONY": {"GV3": 8},
#                 ...
#             },
#             "vi": {
#                 "Bead Damage": {"GV1": 2},
#                 "Bubble": {"GV2": 7},
#                 ...
#             },
#             "scrap": {
#                 "Cut": {"GV1": 4},
#                 "Sidewall": {"GV3": 9},
#                 ...
#             }
#         }
#     }
#     """
#     summary_col = config_h.data_merged_summary_historical_data
#     scrap_col = config_h.data_merged_scrap_historical_data

#     result = {
#         "sku_count": {
#             "uf": {},
#             "vi": {},
#             "scrap": {},
#         }
#     }

#     # ------------------------------------------------------------------
#     # UF — one aggregation per parameter in UF_PARAMETER_MAP
#     # ------------------------------------------------------------------
#     for parameter, grade_field in UF_PARAMETER_MAP.items():
#         match_filter = {
#             **flt,
#             "vi_yield": 1,
#             grade_field: {"$in": [2, 3, 4, 5]},
#         }
#         pipeline = [
#             {"$match": match_filter},
#             {"$group": {"_id": "$sku", "count": {"$sum": 1}}},
#             {"$sort": {"count": -1}},
#         ]
#         sku_counts = {
#             doc["_id"]: doc["count"]
#             for doc in summary_col.aggregate(pipeline, allowDiskUse=True)
#             if doc.get("_id")
#         }
#         if sku_counts:
#             result["sku_count"]["uf"][parameter] = sku_counts

#     # ------------------------------------------------------------------
#     # VI — resolve label from re_comment / re_code, group by label → SKU
#     # ------------------------------------------------------------------
#     match_filter_vi = {**flt, "vi_yield": 0}

#     code_map = _get_re_code_map(config_h.client["production_db_hungary"])
#     re_description_map = {
#         str(doc["condition_code"]): doc.get("description", "")
#         for doc in config_h.re_codes.find(
#             {},
#             {"condition_code": 1, "description": 1},
#         )
#     }

#     cursor = summary_col.find(
#         match_filter_vi,
#         {"sku": 1, "re_code": 1, "re_comment": 1},
#     ).batch_size(5000)

#     vi_by_label = {}
#     comment_regex = re.compile(r"changed:\s*(\d+)\s*-->")

#     for doc in cursor:
#         code = doc.get("re_code")
#         comment = (doc.get("re_comment") or "").strip()
#         label = None

#         if comment.startswith("The VI Reason was changed"):
#             m = comment_regex.search(comment)
#             if m:
#                 changed_code = m.group(1)
#                 label = re_description_map.get(changed_code, changed_code)

#         if not label and code is not None:
#             try:
#                 label = code_map.get(int(float(code)))
#             except (ValueError, TypeError):
#                 pass

#         if not label:
#             label = comment if comment else (str(code) if code is not None else None)

#         if not label or label.lower() == "unknown":
#             continue

#         sku_name = doc.get("sku")
#         if not sku_name:
#             continue

#         if label not in vi_by_label:
#             vi_by_label[label] = {}
#         vi_by_label[label][sku_name] = vi_by_label[label].get(sku_name, 0) + 1

#     for label, sku_counts in vi_by_label.items():
#         result["sku_count"]["vi"][label] = dict(
#             sorted(sku_counts.items(), key=lambda x: x[1], reverse=True)
#         )

#     # ------------------------------------------------------------------
#     # SCRAP — group by description → SKU from scrap collection
#     # ------------------------------------------------------------------
#     match_filter_scrap = {
#         "last_change_time": {"$gte": start_time, "$lte": end_time},
#         "description": {"$nin": [None, ""]},
#     }
#     if "sku" in flt:
#         match_filter_scrap["sku"] = flt["sku"]

#     pipeline_scrap = [
#         {"$match": match_filter_scrap},
#         {
#             "$group": {
#                 "_id": {"description": "$description", "sku": "$sku"},
#                 "count": {"$sum": 1},
#             }
#         },
#         {"$sort": {"count": -1}},
#     ]

#     scrap_by_desc = {}
#     for doc in scrap_col.aggregate(pipeline_scrap, allowDiskUse=True):
#         _id = doc.get("_id") or {}
#         desc = _id.get("description")
#         sku = _id.get("sku")
#         if not desc or not sku:
#             continue
#         if desc not in scrap_by_desc:
#             scrap_by_desc[desc] = {}
#         scrap_by_desc[desc][sku] = doc["count"]

#     for desc, sku_counts in scrap_by_desc.items():
#         result["sku_count"]["scrap"][desc] = dict(
#             sorted(sku_counts.items(), key=lambda x: x[1], reverse=True)
#         )

#     return result

# def count_description(start_time: str, end_time: str, skus: list) -> dict:
#     """
#     Counts per description from data_merged_scrap,
#     filtered by last_change_time and matching SKUs.
    
#     Args:
#         start_time: Start datetime string
#         end_time: End datetime string
#         skus: List of SKU values
    
#     Returns:
#         Dict mapping description to count
#     """
#     # Build a scrap filter similar to `_get_scrap_breakdown` to ensure
#     # description counts are derived directly from the scrap collection.
#     scrap_filter = {
#         "last_change_time": {"$gte": start_time, "$lte": end_time},
#         "sku": {"$in": list(skus)} if skus else {"$in": []},
#         "description": {"$nin": [None, ""]}
#     }

#     pipeline = [
#         {"$match": scrap_filter},
#         {"$group": {"_id": "$description", "count": {"$sum": 1}}},
#         {"$sort": {"count": -1}}
#     ]

#     agg = list(config_h.data_merged_scrap_historical_data.aggregate(pipeline, allowDiskUse=True))

#     breakdown = {
#         item["_id"]: item["count"]
#         for item in agg
#         if item.get("_id")
#     }

#     return breakdown


# # ─────────────────────────────────────────────────────────────────────────────
# # BUILD ONE COMBINATION VALUE
# # ─────────────────────────────────────────────────────────────────────────────
# def build_combination_value(date_str: str, grading: str, last_val: int, skus: list,
#                             today_str: str = None, qr_status: str = None,
#                             sku_filter: str = None, active_skus: list = None) -> dict:
#     flt = build_filter(date_str, grading, last_val, skus,
#                        qr_status=qr_status, sku_filter=sku_filter)
#     trend_anchor = today_str if today_str else date_str
    
#     # Get time window for scrap queries
#     start_time, end_time = day_window(date_str)
    
#     # Use the current combination's matching SKUs for scrap counts.
#     # This ensures exact sku and qr_status combos only count relevant scrap records.
#     scrap_skus = [sku_filter] if sku_filter else skus

#     visual_scrap, uf_scrap = count_scrap_categories(start_time, end_time, scrap_skus)

#     # Build combination label
#     combination = ["uf_date", "tyre_grading", "last"]
#     if qr_status:
#         combination.append("qr_status")
#     if sku_filter:
#         combination.append("sku")

#     entry = {
#         "uf_date":      date_str,
#         "tyre_grading": grading,
#         "last":         last_val,
#     }
#     if qr_status:
#         entry["qr_status"] = qr_status
#     if sku_filter:
#         entry["sku"] = sku_filter

#     entry["data"] = {
#         "scrap_cateogry_visual":    visual_scrap,
#         "scrap_cateogry_uf":        uf_scrap,
#         "last":                     count_last(flt),
#         "overallufmgrade":          count_overallufmgrade(flt),
#         "vi_yield":                 count_vi_yield(flt),
#         "uf_yield":                 count_uf_yield(flt),
#         "uf_downgrade_num":         count_uf_downgrade(flt, grading),
#         "uf_downgrade_condition":   count_uf_downgrade_condition(flt),
#         "card_split_counts":        card_split_counts(flt),
#         "re_code":                  count_re_code(flt),
#         "description":              count_description(start_time, end_time, scrap_skus),
#         "re_comment":               count_re_comment(flt),
#         "condition_code":           count_condition_code(start_time, end_time, scrap_skus),
#         "tyre_counts":              tyre_counts_by_sku(flt, grading),
#         "uf_downgade":              uf_downgrade_breakdown(flt),
#         "sku_wise_warehouse_yield": sku_wise_warehouse_yield(flt),
#         "sku_wise_trend":           sku_wise_trend(trend_anchor, grading, last_val, skus),
#         "yields":                   compute_yields(flt,grading),
#         "oe_tyre_bifer":            oe_tyre_bifer(flt),
#         "compute_sku_count":        compute_sku_count(
#                                     flt,
#                                     start_time,
#                                     end_time
#                                 )
#     }
#     return entry, combination


# # ============================================================
# # IMPROVED YIELD CALCULATION FUNCTIONS - FIXED SCRAP YIELD
# # ============================================================

# def calculate_warehouse_yield_fixed(summary_data, scrap_data=None):
#     """
#     Fixed Warehouse Yield Calculation
#     Warehouse Yield = Pass as OE / (Measured sum + VI scrap)
#     """
#     measured_sum = 0
#     pass_as_oe = 0
    
#     for d in summary_data:
#         ufm_grade = d.get('overallufmgrade')
#         if ufm_grade in (1, 2, 3, 4, 5):
#             measured_sum += 1
#             if ufm_grade == 1:
#                 pass_as_oe += 1
    
#     if measured_sum == 0:
#         return 0.0
    
#     vi_scrap_sum = 0
#     if scrap_data is not None:
#         for d in scrap_data:
#             if d.get('scrap_category') == 'VISUAL':
#                 vi_scrap_sum += 1
    
#     denominator = measured_sum + vi_scrap_sum
    
#     if denominator <= 0:
#         return 0.0
    
#     return round((pass_as_oe / denominator) * 100, 2)


# def calculate_visual_yield_fixed(summary_data, scrap_data):
#     """
#     Fixed Visual Yield Calculation
#     Visual Yield = VI pass / (VI pass + VI scrap)
#     where VI pass = Measured sum - VI downgrade
#     """
#     measured_sum = 0
#     vi_downgrade = 0
    
#     for d in summary_data:
#         ufm_grade = d.get('overallufmgrade')
#         if ufm_grade in (1, 2, 3, 4, 5):
#             measured_sum += 1
#             if d.get('vi_yield') == 0:
#                 vi_downgrade += 1
    
#     vi_scrap = 0
#     for d in scrap_data:
#         if d.get('scrap_category') == 'VISUAL':
#             vi_scrap += 1
    
#     vi_pass = measured_sum - vi_downgrade
#     denominator = vi_pass + vi_scrap
    
#     if denominator <= 0:
#         return 0.0
    
#     return round((vi_pass / denominator) * 100, 2)


# def calculate_scrap_yield_fixed(summary_data, scrap_data):
#     """
#     FIXED: Proper Scrap Yield Calculation
#     Scrap Yield = (VI scrap + UF scrap) / (Measured sum + VI scrap)
#     """
#     measured_sum = 0
#     for d in summary_data:
#         if d.get('overallufmgrade') in (1, 2, 3, 4, 5):
#             measured_sum += 1
    
#     vi_scrap = 0
#     uf_scrap = 0
#     for d in scrap_data:
#         category = d.get('scrap_category')
#         if category == 'VISUAL':
#             vi_scrap += 1
#         elif category == 'UF':
#             uf_scrap += 1
    
#     denominator = measured_sum + vi_scrap
    
#     if denominator <= 0:
#         return 0.0
    
#     return round(((vi_scrap + uf_scrap) / denominator) * 100, 2)


# def calculate_uniformity_yield_fixed(data):
#     """
#     Fixed Uniformity Yield Calculation
#     Uniformity Yield = UF pass / (Measured sum - VI downgrade)
#     """
#     measured_sum = 0
#     uf_pass_as_oe = 0
#     vi_downgrade = 0
#     has_vi_yield = False
    
#     for d in data:
#         ufm_grade = d.get('overallufmgrade')
#         if ufm_grade in (1, 2, 3, 4, 5):
#             measured_sum += 1
#             if ufm_grade == 1:
#                 uf_pass_as_oe += 1
            
#             vi_yield = d.get('vi_yield')
#             if vi_yield is not None:
#                 has_vi_yield = True
#                 if vi_yield == 0:
#                     vi_downgrade += 1
    
#     if measured_sum == 0:
#         return 0.0
    
#     denominator = measured_sum - vi_downgrade if has_vi_yield else measured_sum
    
#     if denominator <= 0:
#         return 0.0
    
#     result = (uf_pass_as_oe / denominator) * 100
#     return round(min(result, 100.0), 2)


# # ============================================================
# # FIXED BUILD RESULT FUNCTION
# # ============================================================

# def build_result_fixed(records_by_sku, scrap_by_sku, grading):
#     """
#     Fixed build result with proper scrap yield calculation for GV performance
#     """
#     result = {}
    
#     # Process each SKU
#     for sku, sku_records in records_by_sku.items():
#         sku_scrap = scrap_by_sku.get(sku, [])
        
#         # Get summary data for this SKU
#         summary_data = sku_records
#         scrap_data = sku_scrap
        
#         # Calculate yields using fixed formulas
#         if grading == "OE":
#             warehouse_y = calculate_warehouse_yield_fixed(summary_data, scrap_data)
#             visual_y = calculate_visual_yield_fixed(summary_data, scrap_data)
#             scrap_y = calculate_scrap_yield_fixed(summary_data, scrap_data)
#             uniformity_y = calculate_uniformity_yield_fixed(summary_data)
#         else:  # REPL
#             warehouse_y = calculate_warehouse_yield_repl_fixed(summary_data, scrap_data)
#             visual_y = calculate_visual_yield_repl_fixed(summary_data, scrap_data)
#             scrap_y = calculate_scrap_yield_repl_fixed(summary_data, scrap_data)
#             uniformity_y = calculate_uniformity_yield_repl_fixed(summary_data)
        
#         result[sku] = {
#             "uniformity_yield": float(f"{uniformity_y:.2f}") if uniformity_y else 0.0,
#             "warehouse_yield": float(f"{warehouse_y:.2f}") if warehouse_y else 0.0,
#             "visual_yield": float(f"{visual_y:.2f}") if visual_y else 0.0,
#             "scrap_yield": float(f"{scrap_y:.2f}") if scrap_y else 0.0,
#             "tyre_count": len(sku_records)
#         }
    
#     # Calculate overall daily/monthly yields
#     if records_by_sku:
#         # Aggregate all records for overall calculation
#         all_summary = []
#         all_scrap = []
        
#         for sku, sku_records in records_by_sku.items():
#             all_summary.extend(sku_records)
#             all_scrap.extend(scrap_by_sku.get(sku, []))
        
#         if grading == "OE":
#             result["daily_yield"] = {
#                 "warehouse_yield": float(f"{calculate_warehouse_yield_fixed(all_summary, all_scrap):.2f}") or 0.0,
#                 "visual_yield": float(f"{calculate_visual_yield_fixed(all_summary, all_scrap):.2f}") or 0.0,
#                 "scrap_yield": float(f"{calculate_scrap_yield_fixed(all_summary, all_scrap):.2f}") or 0.0,
#                 "uniformity_yield": float(f"{calculate_uniformity_yield_fixed(all_summary):.2f}") or 0.0
#             }
#         else:
#             result["daily_yield"] = {
#                 "warehouse_yield": float(f"{calculate_warehouse_yield_repl_fixed(all_summary, all_scrap):.2f}") or 0.0,
#                 "visual_yield": float(f"{calculate_visual_yield_repl_fixed(all_summary, all_scrap):.2f}") or 0.0,
#                 "scrap_yield": float(f"{calculate_scrap_yield_repl_fixed(all_summary, all_scrap):.2f}") or 0.0,
#                 "uniformity_yield": float(f"{calculate_uniformity_yield_repl_fixed(all_summary):.2f}") or 0.0
#             }
    
#     return result


# # ============================================================
# # FIXED REPL YIELD CALCULATIONS
# # ============================================================

# def calculate_warehouse_yield_repl_fixed(summary_data, scrap_data=None):
#     """Fixed REPL Warehouse Yield Calculation"""
#     measured_sum = 0
#     pass_as_oe = 0
    
#     for d in summary_data:
#         ufm_grade = d.get('overallufmgrade')
#         if ufm_grade in (1, 2, 3, 4, 5):
#             measured_sum += 1
#             if ufm_grade in (1, 2, 3):
#                 pass_as_oe += 1
    
#     if measured_sum == 0:
#         return 0.0
    
#     vi_scrap_sum = 0
#     if scrap_data is not None:
#         for d in scrap_data:
#             if d.get('scrap_category') == 'VISUAL':
#                 vi_scrap_sum += 1
    
#     denominator = measured_sum + vi_scrap_sum
    
#     if denominator <= 0:
#         return 0.0
    
#     return round((pass_as_oe / denominator) * 100, 2)


# def calculate_visual_yield_repl_fixed(summary_data, scrap_data):
#     """Fixed REPL Visual Yield Calculation"""
#     measured_sum = 0
#     vi_downgrade = 0
    
#     for d in summary_data:
#         ufm_grade = d.get('overallufmgrade')
#         if ufm_grade in (1, 2, 3, 4, 5):
#             measured_sum += 1
#             if d.get('vi_yield') == 0:
#                 vi_downgrade += 1
    
#     vi_scrap = 0
#     for d in scrap_data:
#         if d.get('scrap_category') == 'VISUAL':
#             vi_scrap += 1
    
#     vi_pass = measured_sum - vi_downgrade
#     denominator = vi_pass + vi_scrap
    
#     if denominator <= 0:
#         return 0.0
    
#     return round((vi_pass / denominator) * 100, 2)


# def calculate_scrap_yield_repl_fixed(summary_data, scrap_data):
#     """Fixed REPL Scrap Yield Calculation"""
#     measured_sum = 0
#     for d in summary_data:
#         if d.get('overallufmgrade') in (1, 2, 3, 4, 5):
#             measured_sum += 1
    
#     vi_scrap = 0
#     uf_scrap = 0
#     for d in scrap_data:
#         category = d.get('scrap_category')
#         if category == 'VISUAL':
#             vi_scrap += 1
#         elif category == 'UF':
#             uf_scrap += 1
    
#     denominator = measured_sum + vi_scrap
    
#     if denominator <= 0:
#         return 0.0
    
#     return round(((vi_scrap + uf_scrap) / denominator) * 100, 2)


# def calculate_uniformity_yield_repl_fixed(data):
#     """Fixed REPL Uniformity Yield Calculation"""
#     measured_sum = 0
#     uf_pass_as_oe = 0
#     vi_downgrade = 0
#     has_vi_yield = False
    
#     for d in data:
#         ufm_grade = d.get('overallufmgrade')
#         if ufm_grade in (1, 2, 3, 4, 5):
#             measured_sum += 1
#             if ufm_grade in (1, 2, 3) and d.get('vi_yield') == 1:
#                 uf_pass_as_oe += 1
            
#             vi_yield = d.get('vi_yield')
#             if vi_yield is not None:
#                 has_vi_yield = True
#                 if vi_yield == 0:
#                     vi_downgrade += 1
    
#     if measured_sum == 0:
#         return 0.0
    
#     denominator = measured_sum - vi_downgrade if has_vi_yield else measured_sum
    
#     if denominator <= 0:
#         return 0.0
    
#     result = (uf_pass_as_oe / denominator) * 100
#     return round(min(result, 100.0), 2)


# # ============================================================
# # OPTIMIZED COMMON HELPERS
# # ============================================================

# TIME_FIELDS = {
#     "uf_time": "uf_time",
#     "cur_time": "cur_time",
#     "ptb_time": "ptb_time",
# }

# SCRAP_TIME_KEY = "scrap_time"

# # Batch size for processing
# BATCH_SIZE = 50
# SKU_BATCH_SIZE = 20


# def process_summary_records_fixed(summary_records, scrap_by_sku, grading):
#     """
#     Process summary records with fixed scrap yield calculation
#     """
#     if not summary_records:
#         return {}
    
#     records_by_sku = defaultdict(list)
    
#     for rec in summary_records:
#         sku = rec.get("sku")
#         if not sku:
#             continue
#         records_by_sku[sku].append(rec)
    
#     # Free memory
#     summary_records.clear()
    
#     return build_result_fixed(records_by_sku, scrap_by_sku, grading)


# def process_scrap_time_records_fixed(summary_records, scrap_map, grading):
#     """
#     Process scrap time records with fixed scrap yield calculation
#     """
#     if not summary_records:
#         return {}
    
#     records_by_sku = defaultdict(list)
#     scrap_by_sku = defaultdict(list)
    
#     for rec in summary_records:
#         sku = rec.get("sku")
#         barcode = rec.get("barcode")
        
#         if not sku:
#             continue
        
#         records_by_sku[sku].append(rec)
        
#         if barcode and barcode in scrap_map:
#             scrap_by_sku[sku].append(scrap_map[barcode])
    
#     # Free memory
#     summary_records.clear()
    
#     return build_result_fixed(records_by_sku, scrap_by_sku, grading)


# def fetch_summary_records_batch(query, sku_list, batch_size=SKU_BATCH_SIZE):
#     """
#     Fetch summary records in batches
#     """
#     if not sku_list:
#         return []
    
#     all_records = []
    
#     for i in range(0, len(sku_list), batch_size):
#         batch_skus = sku_list[i:i+batch_size]
#         batch_query = query.copy()
#         batch_query["sku"] = {"$in": batch_skus}
        
#         batch_records = list(
#             config_h.data_merged_summary_historical_data.find(
#                 batch_query,
#                 {
#                     "sku": 1, 
#                     "overallufmgrade": 1, 
#                     "vi_yield": 1, 
#                     "barcode": 1,
#                     "last": 1,
#                     "rerun": 1
#                 }
#             ).batch_size(min(len(batch_skus) * 2, 100))
#         )
#         all_records.extend(batch_records)
#         batch_records.clear()
    
#     return all_records


# def fetch_scrap_records_batch(start_date, end_date, active_sku_list, batch_size=SKU_BATCH_SIZE):
#     """
#     Fetch scrap records in batches
#     """
#     all_scrap_records = []
    
#     for i in range(0, len(active_sku_list), batch_size):
#         batch_skus = active_sku_list[i:i+batch_size]
#         batch_records = list(
#             config_h.data_merged_scrap_historical_data.find(
#                 {
#                     "last_change_time": {
#                         "$gte": start_date,
#                         "$lte": end_date
#                     },
#                     "sku": {
#                         "$in": batch_skus
#                     }
#                 },
#                 {
#                     "sku": 1,
#                     "scrap_category": 1,
#                     "tire_barcode": 1
#                 }
#             ).batch_size(min(len(batch_skus) * 2, 100))
#         )
#         all_scrap_records.extend(batch_records)
#         batch_records.clear()
    
#     return all_scrap_records


# # ============================================================
# # FIXED MAIN AGGREGATOR
# # ============================================================

# def aggregate_yields_fixed(
#     start_date,
#     end_date,
#     doc_filter,
#     agg_collection_name,
#     doc_key_name,
#     doc_key_value
# ):
#     """
#     Fixed aggregation engine with proper scrap yield calculation
#     """
    
#     # --------------------------------------------------------
#     # ACTIVE SKUS
#     # --------------------------------------------------------
#     active_skus = set(
#         config_h.model_no_status.distinct(
#             "sku",
#             {"status": "active"}
#         )
#     )
    
#     oe_skus = set(
#         config_h.channel_no_curing.distinct(
#             "sku",
#             {"oe_replacement_slob": "OE"}
#         )
#     )
    
#     repl_skus = set(
#         config_h.channel_no_curing.distinct(
#             "sku",
#             {"oe_replacement_slob": "REPL"}
#         )
#     )
    
#     sku_groups = {
#         "OE": list(active_skus & oe_skus),
#         "REPL": list(active_skus & repl_skus)
#     }
    
#     active_sku_list = list(active_skus)
    
#     # --------------------------------------------------------
#     # FETCH SCRAP RECORDS
#     # --------------------------------------------------------
#     scrap_records = fetch_scrap_records_batch(start_date, end_date, active_sku_list)
    
#     scrap_by_sku = defaultdict(list)
#     for s in scrap_records:
#         sku = s.get("sku")
#         if sku:
#             scrap_by_sku[sku].append(s)
    
#     scrap_map = {
#         x["tire_barcode"]: x
#         for x in scrap_records
#         if x.get("tire_barcode")
#     }
    
#     scrap_records.clear()
    
#     # --------------------------------------------------------
#     # MASTER DOC
#     # --------------------------------------------------------
#     doc = {
#         doc_key_name: doc_key_value,
#         "created_at": datetime.now(timezone.utc),
#         "OE": {"last_1": {}, "last_0": {}},
#         "REPL": {"last_1": {}, "last_0": {}}
#     }
    
#     # --------------------------------------------------------
#     # PROCESS EACH GRADING AND TIME FIELD
#     # --------------------------------------------------------
#     for grading, sku_list in sku_groups.items():
#         if not sku_list:
#             continue
            
#         last_1 = {}
#         last_0 = {}
        
#         for time_key, mongo_field in TIME_FIELDS.items():
#             # Process last_1
#             query_last_1 = {
#                 mongo_field: {"$gte": start_date, "$lte": end_date},
#                 "last": 1,
#                 "rerun": {"$in": [0, 1]},
#                 "sku": {"$in": sku_list}
#             }
            
#             summary_last_1 = fetch_summary_records_batch(query_last_1, sku_list)
#             last_1[time_key] = process_summary_records_fixed(
#                 summary_last_1,
#                 scrap_by_sku,
#                 grading
#             )
            
#             # Process last_0
#             query_last_0 = {
#                 mongo_field: {"$gte": start_date, "$lte": end_date},
#                 "rerun": 0,
#                 "last": {"$in": [0, 1]},
#                 "sku": {"$in": sku_list}
#             }
            
#             summary_last_0 = fetch_summary_records_batch(query_last_0, sku_list)
#             last_0[time_key] = process_summary_records_fixed(
#                 summary_last_0,
#                 scrap_by_sku,
#                 grading
#             )
        
#         # ---------------- SCRAP TIME ----------------
#         scrap_time_barcodes = list(scrap_map.keys())
        
#         if scrap_time_barcodes:
#             scrap_time_batch = 20
#             for batch_start in range(0, len(scrap_time_barcodes), scrap_time_batch):
#                 batch_barcodes = scrap_time_barcodes[batch_start:batch_start + scrap_time_batch]
                
#                 st_query_last_1 = {
#                     "barcode": {"$in": batch_barcodes},
#                     "last": 1,
#                     "rerun": {"$in": [0, 1]},
#                     "sku": {"$in": sku_list}
#                 }
                
#                 st_summary_last_1 = list(
#                     config_h.data_merged_summary_historical_data.find(
#                         st_query_last_1,
#                         {
#                             "sku": 1,
#                             "overallufmgrade": 1,
#                             "vi_yield": 1,
#                             "barcode": 1,
#                             "last": 1,
#                             "rerun": 1
#                         }
#                     ).batch_size(min(len(batch_barcodes), 50))
#                 )
                
#                 if batch_start == 0:
#                     last_1[SCRAP_TIME_KEY] = process_scrap_time_records_fixed(
#                         st_summary_last_1,
#                         scrap_map,
#                         grading
#                     )
#                 else:
#                     batch_result = process_scrap_time_records_fixed(
#                         st_summary_last_1,
#                         scrap_map,
#                         grading
#                     )
#                     for sku, yields in batch_result.items():
#                         if sku not in last_1[SCRAP_TIME_KEY]:
#                             last_1[SCRAP_TIME_KEY][sku] = yields
                
#                 st_query_last_0 = {
#                     "barcode": {"$in": batch_barcodes},
#                     "rerun": 0,
#                     "last": {"$in": [0, 1]},
#                     "sku": {"$in": sku_list}
#                 }
                
#                 st_summary_last_0 = list(
#                     config_h.data_merged_summary_historical_data.find(
#                         st_query_last_0,
#                         {
#                             "sku": 1,
#                             "overallufmgrade": 1,
#                             "vi_yield": 1,
#                             "barcode": 1,
#                             "last": 1,
#                             "rerun": 1
#                         }
#                     ).batch_size(min(len(batch_barcodes), 50))
#                 )
                
#                 if batch_start == 0:
#                     last_0[SCRAP_TIME_KEY] = process_scrap_time_records_fixed(
#                         st_summary_last_0,
#                         scrap_map,
#                         grading
#                     )
#                 else:
#                     batch_result = process_scrap_time_records_fixed(
#                         st_summary_last_0,
#                         scrap_map,
#                         grading
#                     )
#                     for sku, yields in batch_result.items():
#                         if sku not in last_0[SCRAP_TIME_KEY]:
#                             last_0[SCRAP_TIME_KEY][sku] = yields
        
#         last_1["date_range_start"] = start_date
#         last_1["date_range_end"] = end_date
#         last_0["date_range_start"] = start_date
#         last_0["date_range_end"] = end_date
        
#         doc[grading]["last_1"] = last_1
#         doc[grading]["last_0"] = last_0
    
#     # --------------------------------------------------------
#     # SAVE
#     # --------------------------------------------------------
#     agg_collection_name.replace_one(
#         doc_filter,
#         doc,
#         upsert=True
#     )
    
#     return 1


# # ============================================================
# # DAILY AND MONTHLY AGGREGATION FUNCTIONS
# # ============================================================

# def day_range_str(date_str: str) -> tuple[str, str]:
#     """Return (start, end) strings for a 6am-to-6am production day."""
#     d = datetime.strptime(date_str[:10], "%Y-%m-%d")
#     return (
#         d.strftime("%Y-%m-%d") + " 06:00:00",
#         (d + timedelta(days=1)).strftime("%Y-%m-%d") + " 05:59:59",
#     )


# def aggregate_daily_yields_fixed(production_date=None):
#     """Fixed daily aggregation with proper scrap yield"""
#     if production_date is None:
#         raise ValueError("production_date is required")
    
#     date_start, date_end = day_range_str(production_date)
    
#     return aggregate_yields_fixed(
#         start_date=date_start,
#         end_date=date_end,
#         doc_filter={"production_date": production_date},
#         agg_collection_name=config_h.oe_yield_daily,
#         doc_key_name="production_date",
#         doc_key_value=production_date
#     )


# def aggregate_monthly_yields_fixed(current_date=None):
#     """Fixed monthly aggregation with proper scrap yield"""
    
#     if current_date is None:
#         raise ValueError("current_date is required")
    
#     if isinstance(current_date, str):
#         current_date = datetime.strptime(current_date, "%Y-%m-%d").date()
    
#     if current_date.day == 1:
#         production_month_date = current_date - timedelta(days=1)
#     else:
#         production_month_date = current_date
    
#     year_month = production_month_date.strftime("%Y-%m")
    
#     month_start = production_month_date.replace(day=1)
    
#     date_start = month_start.strftime("%Y-%m-%d") + " 06:00:00"
#     date_end = current_date.strftime("%Y-%m-%d") + " 05:59:59"
    
#     return aggregate_yields_fixed(
#         start_date=date_start,
#         end_date=date_end,
#         doc_filter={"month": year_month},
#         agg_collection_name=config_h.oe_yield_monthly,
#         doc_key_name="month",
#         doc_key_value=year_month
#     )


# # ============================================================
# # RUN FUNCTION
# # ============================================================

# def run():
#     from concurrent.futures import ThreadPoolExecutor, as_completed
#     import os

#     today = datetime.now(tz=timezone.utc).date()

#     # Process last 3 days
#     dates_to_process = [
#         (today - timedelta(days=offset)).strftime("%Y-%m-%d")
#         for offset in range(4, 0, -1)
#     ]

#     tyre_gradings = get_tyre_gradings()
#     if not tyre_gradings:
#         logger.error("No tyre_grading values found in production_db. Exiting.")
#         return

#     active_skus_list = list(config_h.model_no_status.distinct("sku", {"status": "active"}))

#     grading_meta = {}
#     for grading in tyre_gradings:
#         skus_for_grading = get_skus_for_grading(grading)
#         if not skus_for_grading:
#             logger.warning("No SKUs for grading=%s — skipping", grading)
#             continue
#         qr_statuses = get_qr_statuses_for_grading(grading)
#         skus_per_qr = {}
#         for qr in qr_statuses:
#             skus_for_qr = get_skus_for_grading_and_qr(grading, qr)
#             if skus_for_qr:
#                 skus_per_qr[qr] = skus_for_qr
#         grading_meta[grading] = {
#             "skus":       skus_for_grading,
#             "qr_statuses": qr_statuses,
#             "skus_per_qr": skus_per_qr
#         }

#     if not grading_meta:
#         logger.error("No valid gradings with SKUs found. Exiting.")
#         return

#     def make_tasks(production_date):
#         tasks = []
#         for grading, meta in grading_meta.items():
#             skus_for_grading = meta["skus"]
#             skus_per_qr      = meta["skus_per_qr"]
#             for last_val in LAST_VALUES:
#                 tasks.append((production_date, grading, last_val, skus_for_grading,
#                             production_date, None, None))
#                 for qr, skus_for_qr in skus_per_qr.items():
#                     tasks.append((production_date, grading, last_val, skus_for_qr,
#                                 production_date, qr, None))
#                     for sku in skus_for_qr:
#                         tasks.append((production_date, grading, last_val, [sku],
#                                     production_date, qr, sku))
#         return tasks

#     def execute_task(args):
#         production_date, grading, last_val, skus, today_str, qr_status, sku_filter = args
#         return build_combination_value(
#             production_date, grading, last_val, skus,
#             today_str=today_str,
#             qr_status=qr_status,
#             sku_filter=sku_filter,
#             active_skus=active_skus_list
#         )

#     cpu_count = os.cpu_count() or 4
#     max_workers = min(cpu_count * 3, 50)
#     logger.info("Using %d worker threads for parallel processing", max_workers)

#     for production_date in dates_to_process:
#         logger.info("=== Processing production_date: %s ===", production_date)
#         try:
#             tasks = make_tasks(production_date)
#             logger.info("Total combinations to process for %s: %d", production_date, len(tasks))

#             combo_map = defaultdict(list)
#             start_time = time.time()

#             with ThreadPoolExecutor(max_workers=max_workers) as executor:
#                 future_to_task = {executor.submit(execute_task, t): t for t in tasks}
#                 completed = 0
#                 for future in as_completed(future_to_task):
#                     try:
#                         val, combo = future.result()
#                         combo_map[tuple(combo)].append(val)
#                         completed += 1
#                         if completed % 50 == 0:
#                             elapsed = time.time() - start_time
#                             rate = completed / elapsed if elapsed > 0 else 0
#                             remaining = len(tasks) - completed
#                             eta = remaining / rate if rate > 0 else 0
#                             logger.info("  [%s] %d/%d combinations done (%.1f/sec, ETA: %.1fm)",
#                                         production_date, completed, len(tasks), rate, eta / 60)
#                     except Exception as e:
#                         task = future_to_task[future]
#                         logger.exception("Combination failed for task %s: %s", task, e)

#             elapsed_total = time.time() - start_time
#             logger.info("Completed all combinations for %s in %.1f minutes", 
#                         production_date, elapsed_total / 60)

#             if not combo_map:
#                 logger.warning("No combinations built for date=%s. Nothing stored.", production_date)
#                 continue

#             filters_data = [
#                 {"combination": list(combo_keys), "values": values}
#                 for combo_keys, values in combo_map.items()
#             ]

#             total = sum(len(v["values"]) for v in filters_data)

#             doc = {
#                 "uf_date":      production_date,
#                 "created_at":   datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
#                 "filters_data": filters_data
#             }

#             result = config_h.home_data.replace_one({"uf_date": production_date}, doc, upsert=True)
#             logger.info("Stored %d total values across %d combination types for date=%s. Matched: %d, Modified: %d, Upserted: %d",
#                         total, len(filters_data), production_date, 
#                         result.matched_count, result.modified_count, result.upserted_id is not None)

#             saved_doc = config_h.home_data.find_one({"uf_date": production_date})
#             if saved_doc:
#                 logger.info(" Successfully verified data saved for date=%s", production_date)
#             else:
#                 logger.error(" Failed to verify data saved for date=%s", production_date)

#             try:
#                 aggregate_daily_yields_fixed(production_date=production_date)
#                 logger.info("=== Daily Aggregation END for date=%s ===", production_date)
#             except Exception as e:
#                 logger.exception("Daily aggregation failed for date=%s: %s", production_date, e)

#         except Exception as e:
#             logger.exception("Processing failed for date=%s — skipping to next date: %s", production_date, e)

#     # Monthly aggregation with fixed scrap yield
#     try:
#         aggregate_monthly_yields_fixed(current_date=today)
#         logger.info("=== Monthly Aggregation END (Fixed Scrap Yield) ===")
#     except Exception as e:
#         logger.exception("Monthly aggregation failed: %s", e)


# if __name__ == "__main__":
#     run()

# # ##################Working scrap data is missing ravi ##########
# # import logging
# # import os
# # from datetime import datetime, timedelta, timezone
# # import time
# # from flask import Flask
# # from collections import defaultdict
# # import config_h
# # from collections import Counter

# # app = Flask(__name__)


# # # last values to iterate
# # LAST_VALUES = [1, 0]

# # # Trend: last N days
# # TREND_DAYS = 7

# # # ─────────────────────────────────────────────────────────────────────────────
# # # LOGGING
# # # ─────────────────────────────────────────────────────────────────────────────

# # from logging.handlers import TimedRotatingFileHandler

# # # Create logs folder if it does not exist
# # LOG_DIR = "logs"
# # os.makedirs(LOG_DIR, exist_ok=True)

# # # Log file
# # LOG_FILE = os.path.join(LOG_DIR, "hungary_historical_database.log")

# # # Rotate log file every midnight
# # file_handler = TimedRotatingFileHandler(
# #     LOG_FILE,
# #     when="midnight",
# #     interval=1,
# #     backupCount=30,
# #     encoding="utf-8"
# # )

# # # Rename rotated files with date
# # file_handler.suffix = "%Y-%m-%d"

# # # Log format
# # formatter = logging.Formatter(
# #     "%(asctime)s - %(levelname)s - %(message)s"
# # )

# # file_handler.setFormatter(formatter)

# # # Console logging
# # console_handler = logging.StreamHandler()
# # console_handler.setFormatter(formatter)

# # # Configure logger
# # logger = logging.getLogger("hungary_historical_database")
# # logger.setLevel(logging.INFO)

# # # Avoid duplicate handlers if run/reloaded
# # if not logger.handlers:
# #     logger.addHandler(file_handler)
# #     logger.addHandler(console_handler)

# # logger.propagate = False

# # # ─────────────────────────────────────────────────────────────────────────────
# # # MONGO
# # # ─────────────────────────────────────────────────────────────────────────────

# # # ─────────────────────────────────────────────────────────────────────────────
# # # HELPERS
# # # ─────────────────────────────────────────────────────────────────────────────
# # def day_window(date_str: str):
# #     """Return (start_str, end_str) for 6am-to-6am window of given date."""
# #     d = datetime.strptime(date_str, "%Y-%m-%d")
# #     start = d.strftime("%Y-%m-%d") + " 06:00:00"
# #     end   = (d + timedelta(days=1)).strftime("%Y-%m-%d") + " 05:59:59"
# #     return start, end


# # def get_tyre_gradings() -> list:
# #     """Fetch distinct tyre_grading values from production_db.channel_no_curing."""
# #     gradings = [
# #         x for x in config_h.channel_no_curing.distinct("oe_replacement_slob")
# #         if x and str(x).strip() not in ("", "0", "None")
# #     ]
# #     if not gradings:
# #         logger.warning("No tyre_grading values found in channel_no_curing.oe_replacement_slob")
# #     return gradings


# # def get_skus_for_grading(grading: str) -> list:
# #     """Get SKUs from production_db.channel_no_curing for a given tyre_grading."""
# #     import re
# #     skus = config_h.channel_no_curing.distinct(
# #         "sku",
# #         {"oe_replacement_slob": re.compile(f"^{grading}$", re.IGNORECASE)}
# #     )
# #     skus = [s for s in skus if s]
# #     return skus


# # def get_qr_statuses_for_grading(grading: str) -> list:
# #     """Get distinct QR_Status values for a given tyre_grading from production_db."""
# #     import re
# #     qr_list = config_h.channel_no_curing.distinct(
# #         "QR_Status",
# #         {"oe_replacement_slob": re.compile(f"^{grading}$", re.IGNORECASE)}
# #     )
# #     return [q for q in qr_list if q]


# # def get_skus_for_grading_and_qr(grading: str, qr_status: str) -> list:
# #     """Get SKUs filtered by both tyre_grading and QR_Status from production_db."""
# #     import re
# #     skus = config_h.channel_no_curing.distinct(
# #         "sku",
# #         {
# #             "oe_replacement_slob": re.compile(f"^{grading}$", re.IGNORECASE),
# #             "QR_Status": qr_status
# #         }
# #     )
# #     return [s for s in skus if s]


# # def build_filter(date_str: str, grading: str, last_val: int, skus: list,
# #                  qr_status: str = None, sku_filter: str = None) -> dict:
# #     start, end = day_window(date_str)

# #     # first_yield → rerun=0, last IN [0,1]  (rerun=0,last=0 OR rerun=0,last=1)
# #     # final_yield → last=1                  (rerun=0,last=1 OR rerun=1,last=1)
# #     if last_val == 0:
# #         yield_filter = {"rerun": 0, "last": {"$in": [0, 1]}}
# #     else:  # last_val == 1, default: final_yield
# #         yield_filter = {"last": 1}

# #     flt = {
# #         "uf_time": {"$gte": start, "$lte": end},
# #         "sku":     {"$in": skus},
# #         **yield_filter,
# #     }
# #     # QR status combos already restrict the SKU list via channel_no_curing.
# #     # Use the SKU filter directly from the combination and avoid extra sku_type matching.
# #     if sku_filter:
# #         flt["sku"] = sku_filter
# #     return flt


# # # ─────────────────────────────────────────────────────────────────────────────
# # # AGGREGATION FUNCTIONS
# # # ─────────────────────────────────────────────────────────────────────────────
# # def count_last(flt: dict) -> dict:
# #     pipeline = [
# #         {"$match": flt},
# #         {"$group": {"_id": "$last", "count": {"$sum": 1}}}
# #     ]
# #     result = {}
# #     for row in config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True):
# #         key = f"last_{row['_id']}"
# #         result[key] = row["count"]
# #     return result


# # def count_overallufmgrade(flt: dict) -> dict:
# #     pipeline = [
# #         {"$match": flt},
# #         {"$group": {"_id": "$overallufmgrade", "count": {"$sum": 1}}}
# #     ]
# #     result = {}
# #     for row in config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True):
# #         key = f"overallufmgrade_{row['_id']}"
# #         result[key] = row["count"]
# #     return result


# # def count_vi_yield(flt: dict) -> dict:
# #     pipeline = [
# #         {"$match": flt},
# #         {"$group": {"_id": "$vi_yield", "count": {"$sum": 1}}}
# #     ]
# #     result = {}
# #     for row in config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True):
# #         key = f"vi_yield_{row['_id']}"
# #         result[key] = row["count"]
# #     return result

# # def count_uf_yield(flt: dict) -> dict:
# #     pipeline = [
# #         {"$match": flt},
# #         {"$group": {"_id": "$uf_yield", "count": {"$sum": 1}}}
# #     ]
# #     result = {}
# #     for row in config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True):
# #         key = f"uf_yield_{row['_id']}"
# #         result[key] = row["count"]
# #     return result

# # def count_uf_downgrade_condition(flt: dict) -> dict:
# #     """Return Numerator and UF_MeasuredSum counts."""

# #     base_query = {
# #         **flt,
# #         "vi_yield": 1,
# #         "$or": [
# #             {"overalltugrade": {"$in": [2, 3, 4, 5]}},
# #             {"overallbalgrade": {"$in": [2, 3, 4, 5]}},
# #             {"overallsolgrade": {"$in": [2, 3, 4, 5]}}
# #         ]
# #     }

# #     numerator = config_h.data_merged_summary_historical_data.count_documents(base_query)

# #     uf_measuredsum = config_h.data_merged_summary_historical_data.count_documents({
# #         **flt,
# #         "overallufmgrade": {"$in": [2, 3, 4, 5]}
# #     })

# #     return {
# #         "Numerator": numerator,
# #         "uf_MeasuredSum": uf_measuredsum
# #     }

# # def card_split_counts(flt: dict) -> dict:
# #     """
# #     Returns actual document counts for tyre_bifer and rework_split using count_documents.
    
# #     Args:
# #         collection: MongoDB collection object
# #         flt: Base filter dictionary
# #     """
    
# #     # Tyre Bifer
# #     tyre_bifer = {
# #         "count_vi": config_h.data_merged_summary_historical_data.count_documents({
# #             **flt,
# #             "overallufmgrade": {"$in": [2, 3]},
# #             "vi_yield": 0
# #         }),
# #         "count_uf": config_h.data_merged_summary_historical_data.count_documents({
# #             **flt,
# #             "overallufmgrade": {"$in": [2, 3]},
# #             "vi_yield": 1,
# #             "$or": [
# #                 {"overalltugrade": {"$in": [2, 3, 4, 5]}},
# #                 {"overallbalgrade": {"$in": [2, 3, 4, 5]}},
# #                 {"overallsolgrade": {"$in": [2, 3, 4, 5]}}
# #             ]
# #         })
# #     }
    
# #     # Rework Split
# #     rework_split = {
# #         "count_vi": config_h.data_merged_summary_historical_data.count_documents({
# #             **flt,
# #             "overallufmgrade": 4,
# #             "vi_yield": 0
# #         }),
# #         "count_uf": config_h.data_merged_summary_historical_data.count_documents({
# #             **flt,
# #             "overallufmgrade": 4,
# #             "vi_yield": 1,
# #             "$or": [
# #                 {"overalltugrade": {"$in": [2, 3, 4, 5]}},
# #                 {"overallbalgrade": {"$in": [2, 3, 4, 5]}},
# #                 {"overallsolgrade": {"$in": [2, 3, 4, 5]}}
# #             ]
# #         })
# #     }
    
# #     return {
# #         "tyre_bifer": tyre_bifer,
# #         "rework_split": rework_split
# #     }

# # def count_re_code(flt: dict) -> dict:
# #     pipeline = [
# #         {"$match": {**flt, "re_code": {"$ne": None, "$ne": ""}}},
# #         {"$group": {"_id": "$re_code", "count": {"$sum": 1}}},
# #         {"$sort": {"count": -1}}
# #     ]
# #     return {str(row["_id"]): row["count"] for row in config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True) if row["_id"] is not None}


# # import re



# # def count_re_comment(flt: dict) -> dict:

# #     match_filter = {
# #         **flt,
# #         "vi_yield": 0,
# #         "re_comment": {"$nin": [None, ""]}
# #     }

# #     pipeline = [
# #         {"$match": match_filter},
# #         {
# #             "$group": {
# #                 "_id": "$re_comment",
# #                 "count": {"$sum": 1}
# #             }
# #         },
# #         {"$sort": {"count": -1}}
# #     ]

# #     rows = list(config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True))

# #     # --------------------------------------------------------
# #     # Extract VI reason codes
# #     # --------------------------------------------------------

# #     reason_codes = set()

# #     for row in rows:

# #         comment = str(row["_id"])

# #         match = re.search(
# #             r"The VI Reason was changed:\s*(\d+)\s*-->",
# #             comment
# #         )

# #         if match:
# #             reason_codes.add(match.group(1))

# #     # --------------------------------------------------------
# #     # Fetch descriptions
# #     # --------------------------------------------------------

# #     reason_map = {
# #         x["condition_code"]: x.get("description", "")
# #         for x in config_h.re_codes.find(
# #             {
# #                 "condition_code": {
# #                     "$in": list(reason_codes)
# #                 }
# #             },
# #             {
# #                 "_id": 0,
# #                 "condition_code": 1,
# #                 "description": 1
# #             }
# #         )
# #     }

# #     # --------------------------------------------------------
# #     # Final result
# #     # --------------------------------------------------------

# #     final_result = {}

# #     for row in rows:

# #         comment = str(row["_id"])
# #         count = row["count"]

# #         match = re.search(
# #             r"The VI Reason was changed:\s*(\d+)\s*-->",
# #             comment
# #         )

# #         if match:

# #             code = match.group(1)

# #             description = reason_map.get(code)

# #             if description:
# #                 final_result[description] = (
# #                     final_result.get(description, 0) + count
# #                 )
# #             else:
# #                 final_result[comment] = (
# #                     final_result.get(comment, 0) + count
# #                 )

# #         else:
# #             final_result[comment] = (
# #                 final_result.get(comment, 0) + count
# #             )

# #     return final_result

# # def oe_tyre_bifer(flt: dict) -> dict:
# #     pipeline = [
# #         {
# #             "$match": {
# #                 **flt,
# #                 "overallufmgrade": 1
# #             }
# #         },
# #         {
# #             "$group": {
# #                 "_id": {"$ifNull": ["$re_comment", "NULL"]},
# #                 "count": {"$sum": 1}
# #             }
# #         },
# #         {"$sort": {"count": -1}}
# #     ]

# #     return {
# #         str(row["_id"]): row["count"]
# #         for row in config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True)
# #     }

# # # ─────────────────────────────────────────────────────────────────────────────
# # # SCRAP COLLECTION
# # # ─────────────────────────────────────────────────────────────────────────────
# # # config_h.data_merged_scrap_historical_data is now initialized in MONGO section above


# # def get_scrap_filter(start_time: str, end_time: str, skus: list) -> dict:
# #     """
# #     Create filter for scrap collection using last_change_time and matching SKUs.
    
# #     Args:
# #         start_time: Start datetime string (e.g., "2026-05-15 06:00:00")
# #         end_time: End datetime string (e.g., "2026-05-16 05:59:59")
# #         skus: List of SKU values to filter by
    
# #     Returns:
# #         Filter dict for scrap collection query
# #     """
# #     return {
# #         "last_change_time": {"$gte": start_time, "$lte": end_time},
# #         "sku": {"$in": skus}
# #     }


# # def count_scrap_categories(start_time: str, end_time: str, skus: list) -> tuple:
# #     """
# #     Returns (visual_count, uf_count) from data_merged_scrap,
# #     filtered by last_change_time and matching SKUs.
    
# #     Args:
# #         start_time: Start datetime string (e.g., "2026-05-15 06:00:00")
# #         end_time: End datetime string (e.g., "2026-05-16 05:59:59")
# #         skus: List of SKU values to filter by
    
# #     Returns:
# #         Tuple of (visual_count, uf_count)
# #     """
# #     scrap_flt = get_scrap_filter(start_time, end_time, skus)
# #     pipeline = [
# #         {"$match": scrap_flt},
# #         {"$group": {"_id": "$scrap_category", "count": {"$sum": 1}}}
# #     ]
# #     visual, uf = 0, 0
# #     for row in config_h.data_merged_scrap_historical_data.aggregate(pipeline, allowDiskUse=True):
# #         cat = (row["_id"] or "").upper()
# #         if cat == "VISUAL":
# #             visual = row["count"]
# #         elif cat == "UF":
# #             uf = row["count"]
# #     return visual, uf


# # def count_condition_code(start_time: str, end_time: str, skus: list) -> dict:
# #     """
# #     Counts per condition_code from data_merged_scrap,
# #     filtered by last_change_time and matching SKUs.
    
# #     Args:
# #         start_time: Start datetime string
# #         end_time: End datetime string
# #         skus: List of SKU values
    
# #     Returns:
# #         Dict mapping condition_code to count
# #     """
# #     scrap_flt = get_scrap_filter(start_time, end_time, skus)
# #     pipeline = [
# #         {"$match": {**scrap_flt, "condition_code": {"$ne": None, "$ne": ""}}},
# #         {"$group": {"_id": "$condition_code", "count": {"$sum": 1}}},
# #         {"$sort": {"count": -1}}
# #     ]
# #     return {str(row["_id"]): row["count"] for row in config_h.data_merged_scrap_historical_data.aggregate(pipeline, allowDiskUse=True) if row["_id"] is not None}


# # def tyre_counts_by_sku(flt: dict,  grading: str = "OE") -> dict:
# #     """
# #     Per-SKU counts based on overallufmgrade:
# #       total_tyres  → overallufmgrade == 1
# #       rework       → overallufmgrade IN [2, 3]
# #       replacement  → overallufmgrade == 4
# #       rejected     → overallufmgrade == 5
# #     """
# #     if grading == "REPL":
# #         oe_grade = {"$in": ["$overallufmgrade", [1, 2, 3]]}
# #     else:
# #         oe_grade = {"$eq": ["$overallufmgrade", 1]}

# #     pipeline = [
# #         {"$match": flt},
# #         {"$group": {
# #             "_id":        "$sku",
# #             "total":      {"$sum": {"$cond": [oe_grade, 1, 0]}},
# #             "rework":     {"$sum": {"$cond": [{"$in": ["$overallufmgrade", [2, 3]]}, 1, 0]}},
# #             "replacement":{"$sum": {"$cond": [{"$eq": ["$overallufmgrade", 4]}, 1, 0]}},
# #             "rejected":   {"$sum": {"$cond": [{"$eq": ["$overallufmgrade", 5]}, 1, 0]}},
# #         }}
# #     ]
# #     total_d = {}; rework_d = {}; repl_d = {}; rej_d = {}
# #     for row in config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True):
# #         sku = row["_id"] or "unknown"
# #         total_d[sku]  = row["total"]
# #         rework_d[sku] = row["rework"]
# #         repl_d[sku]   = row["replacement"]
# #         rej_d[sku]    = row["rejected"]
# #     return {
# #         "oe_tyres":  total_d,
# #         "rework":       rework_d,
# #         "replacement":  repl_d,
# #         "rejected":     rej_d
# #     }

# # def count_uf_downgrade(flt: dict, grading: str) -> int:
# #     """Count tyres where vi_yield == 1 and overallufmgrade != 1 using the base filter."""
    
# #     query = {**flt}

# #     if grading == "REPL":
# #         query["overallufmgrade"] = {"$nin": [1, 2, 3]}
# #     else:  # OE
# #         query["overallufmgrade"] = {"$ne": 1}

# #     return config_h.data_merged_summary_historical_data.count_documents(query)

# # def uf_downgrade_breakdown(flt: dict) -> dict:

# #         # 👉 Add VI filter locally (without mutating original filter)
# #     match_filter = {**flt, "vi_yield": 1}

# #     """Count tyres where each grade param > 1 (UF downgrade reason)."""
# #     uf_params = [
# #         ("lrograde",    "LRO"),   ("conygrade",   "CONY"),
# #         ("rfppwcgrade", "RFPP"),  ("rfh1wcgrade", "RFH1"),
# #         ("rfh2wcgrade", "RFH2"),  ("rfh3wcgrade", "RFH3"),
# #         ("lfppwcgrade", "LFPP"),  ("crrograde",   "CRRQ"),
# #         ("trrograde",   "TRRQ"),  ("brrograde",   "BRRQ"),
# #         ("plysgrade",   "PLYS"),  ("sbalgrade",   "SBAL"),
# #         ("bulwcgrade",  "BUL"),   ("depwcgrade",  "DEP"),
# #         ("uppergrade",  "UPPER"), ("lowergrade",  "LOWER"),
# #     ]
# #     # add_fields = {label: {"$cond": [{"$gt": [f"${field}", 1]}, 1, 0]} for field, label in uf_params}
# #     add_fields = {
# #         label: {
# #             "$cond": [
# #                 {"$in": [f"${field}", [2, 3, 4, 5]]},
# #                 1,
# #                 0
# #             ]
# #         }
# #         for field, label in uf_params
# #     }
# #     group_sum  = {label: {"$sum": f"${label}"} for _, label in uf_params}
# #     pipeline   = [
# #         {"$match": match_filter},
# #         {"$addFields": add_fields},
# #         {"$group": {"_id": None, **group_sum}}
# #     ]
# #     agg = list(config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True))
# #     if not agg:
# #         return {label: 0 for _, label in uf_params}
# #     row = agg[0]
# #     return dict(sorted({label: row.get(label, 0) for _, label in uf_params}.items(),
# #                         key=lambda x: x[1], reverse=True))

# # def sku_wise_warehouse_yield(flt: dict) -> dict:
# #     """Per-SKU overallufmgrade value counts + scrap_cateogry_visual (SKU-wise)."""
# #     # Step 1: grade counts + barcodes per SKU from summary
# #     pipeline = [
# #         {"$match": flt},
# #         {"$group": {
# #             "_id":   "$sku",
# #             "g0":    {"$sum": {"$cond": [{"$eq": ["$overallufmgrade", 0]}, 1, 0]}},
# #             "g1":    {"$sum": {"$cond": [{"$eq": ["$overallufmgrade", 1]}, 1, 0]}},
# #             "g2":    {"$sum": {"$cond": [{"$eq": ["$overallufmgrade", 2]}, 1, 0]}},
# #             "g3":    {"$sum": {"$cond": [{"$eq": ["$overallufmgrade", 3]}, 1, 0]}},
# #             "g4":    {"$sum": {"$cond": [{"$eq": ["$overallufmgrade", 4]}, 1, 0]}},
# #             "g5":    {"$sum": {"$cond": [{"$eq": ["$overallufmgrade", 5]}, 1, 0]}},
# #             "barcodes": {"$push": "$barcode"}
# #         }}
# #     ]
# #     rows = list(config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True))
# #     # Step 2: get scrap_category == "VISUAL" per SKU by filtering scrap collection
# #     # using last_change_time range and SKU list extracted from the incoming filter.
# #     # This avoids joining via barcodes and follows the new requirement.
# #     vi_scrap_by_sku = {}

# #     # Extract time range from filter (uf_time expected)
# #     start_time = None
# #     end_time = None
# #     if "uf_time" in flt and isinstance(flt["uf_time"], dict):
# #         start_time = flt["uf_time"].get("$gte") or flt["uf_time"].get("$gt")
# #         end_time = flt["uf_time"].get("$lte") or flt["uf_time"].get("$lt")

# #     # Extract SKU list from filter
# #     sku_filter = flt.get("sku")
# #     sku_list = None
# #     if isinstance(sku_filter, dict) and "$in" in sku_filter:
# #         sku_list = sku_filter["$in"]
# #     elif isinstance(sku_filter, list):
# #         sku_list = sku_filter
# #     elif isinstance(sku_filter, str):
# #         sku_list = [sku_filter]

# #     # Build scrap query only if we have a time range and SKU list
# #     if start_time and end_time and sku_list:
# #         scrap_query = {
# #             "last_change_time": {"$gte": start_time, "$lte": end_time},
# #             "sku": {"$in": sku_list},
# #             "scrap_category": "VISUAL"
# #         }
# #         for row in config_h.data_merged_scrap_historical_data.aggregate([
# #             {"$match": scrap_query},
# #             {"$group": {"_id": "$sku", "count": {"$sum": 1}}}
# #         ], allowDiskUse=True):
# #             if row and row.get("_id"):
# #                 vi_scrap_by_sku[row["_id"]] = row.get("count", 0)

# #     # Step 3: build result
# #     result = {}
# #     for row in rows:
# #         sku = row["_id"] or "unknown"
# #         result[sku] = {
# #             "overallufmgrade_0":    row["g0"],
# #             "overallufmgrade_1":    row["g1"],
# #             "overallufmgrade_2":    row["g2"],
# #             "overallufmgrade_3":    row["g3"],
# #             "overallufmgrade_4":    row["g4"],
# #             "overallufmgrade_5":    row["g5"],
# #             "scrap_cateogry_visual": vi_scrap_by_sku.get(sku, 0),
# #         }
# #     return result

# # from collections import Counter

# # def sku_wise_trend(date_str: str, grading: str, last_val: int, skus: list) -> dict:
# #     """
# #     SKU-wise 7-day trend for UFM, GEO, BAL yields.
# #     Structure: SKU → category → param → date → machine_type → machine → {
# #         numerator, oe_tyres, denominator,
# #         overallufmgrade: {overallufmgrade_<grade>: count, ...},
# #         <grade_field>: {<grade_field>_<grade>: count, ...}
# #     }
# #     Grades are dynamic (any integer values present in data) — no hardcoding of 1/2/3.
# #     One aggregation pipeline per day (not per param/machine) for efficiency.
# #     numerator   = count(grade_col meets OE/REPL condition) per machine
# #     denominator = count(overallufmgrade IN 1-5) per machine
# #     """
# #     all_params = {
# #         "UFM": [
# #             ("rfv",      "rfppwcgrade"),
# #             ("rfh1",     "rfh1wcgrade"),
# #             ("rfh2",     "rfh2wcgrade"),
# #             ("lfv",      "lfppwcgrade"),
# #             ("conicity", "conygrade"),
# #             ("plysteer", "plysgrade"),
# #         ],
# #         "GEO": [
# #             ("crro",   "crrograde"),
# #             ("trro",   "trrograde"),
# #             ("brro",   "brrograde"),
# #             ("lro",    "lrograde"),
# #             ("lroh1",  "lroh1grade"),
# #             ("crroh1", "crroh1grade"),
# #             ("trroh1", "trroh1grade"),
# #             ("brroh1", "brroh1grade"),
# #             ("lrro",   "lrrograde"),
# #         ],
# #         "BAL": [
# #             ("sbal",   "sbalgrade"),
# #             ("couple", "couplegrade"),
# #             ("upper",  "uppergrade"),
# #             ("lower",  "lowergrade"),
# #         ],
# #     }
# #     machine_fields = [("tbm", "ptb_id"), ("curing", "cur_id"), ("ufm", "uf_id")]

# #     # === FIXED SCRAP FETCHING (No machine grouping) ===
# #     start, end = day_window(date_str)
# #     vi_scrap_map = {}   # Will store by sku only now
# #     scrap_pipeline = [
# #         {
# #             "$match": {
# #                 "last_change_time": {"$gte": start, "$lt": end},
# #                 "sku": {"$in": skus},
# #                 "scrap_category": "VISUAL"
# #             }
# #         },
# #         {
# #             "$group": {
# #                 "_id": "$sku",
# #                 "vi_scrap": {"$sum": 1}
# #             }
# #         }
# #     ]
# #     for row in config_h.data_merged_scrap_historical_data.aggregate(scrap_pipeline, allowDiskUse=True):
# #         sku = row["_id"]
# #         vi_scrap_map[sku] = row.get("vi_scrap", 0)

# #     # Flat list of all (label, field) pairs
# #     all_grade_fields = [
# #         (label, field)
# #         for params in all_params.values()
# #         for label, field in params
# #     ]
# #     # Map label → category
# #     label_to_cat = {
# #         label: cat
# #         for cat, params in all_params.items()
# #         for label, _ in params
# #     }
# #     # Map label → field name (for the dynamic grade key)
# #     label_to_field = {label: field for label, field in all_grade_fields}

# #     # Init result structure
# #     result = {
# #         sku: {
# #             cat: {label: {} for label, _ in params}
# #             for cat, params in all_params.items()
# #         }
# #         for sku in skus
# #     }
# #     # Add yields section for overall yields
# #     for sku in skus:
# #         result[sku]["yields"] = {
# #             "warehouse":   {},
# #             "uniformity":  {},
# #             "visual":      {},
# #         }

# #     # Build date windows — only the running date
# #     start, end = day_window(date_str)
# #     days = [(date_str, start, end)]

# #     # first_yield → rerun=0, last IN [0,1]
# #     # final_yield → last=1
# #     if last_val == 0:
# #         yield_filter = {"rerun": 0, "last": {"$in": [0, 1]}}
# #     else:
# #         yield_filter = {"last": 1}

# #     # Determine the overallufmgrade condition based on tyre grading:
# #     # OE  → grade == 1
# #     # REPL → grade in [1, 2, 3]
# #     if grading == "REPL":
# #         oe_grade_cond = {"$in": ["$overallufmgrade", [1, 2, 3]]}
# #     else:  # default: OE
# #         oe_grade_cond = {"$eq": ["$overallufmgrade", 1]}

# #     # Determine the uf_pass condition based on tyre grading
# #     if grading == "REPL":
# #         uf_pass_cond = {
# #             "$and": [
# #                 oe_grade_cond,
# #                 {"$eq": ["$vi_yield", 1]}
# #             ]
# #         }
# #     else:
# #         uf_pass_cond = oe_grade_cond

# #     for d, start, end in days:
# #         base_flt = {
# #             "uf_time":         {"$gte": start, "$lt": end},
# #             "sku":             {"$in": skus},
# #             "overallufmgrade": {"$in": [1, 2, 3, 4, 5]},
# #             **yield_filter,
# #         }
# #         for mtype, mfield in machine_fields:
# #             # denominator = SUM(overallufmgrade IN 1-5) per {sku, machine}
# #             # numerator   = SUM(grade_field meets condition) per {sku, machine}
# #             # + push full grade lists so we can build dynamic frequency maps
# #             group_stage = {
# #                 "_id":         {"sku": "$sku", "machine": f"${mfield}"},
# #                 "denominator": {"$sum": 1},
# #                 "oe_tyres":    {"$sum": {"$cond": [oe_grade_cond, 1, 0]}},
# #                 # for overall yields
# #                 "vi_downgrade":  {"$sum": {"$cond": [{"$eq": ["$vi_yield", 0]}, 1, 0]}},
# #                 "uf_pass_as_oe": {
# #                     "$sum": {
# #                         "$cond": [uf_pass_cond, 1, 0]
# #                     }
# #                 },
# #                 # Dynamic grade distributions (no hardcoded 1/2/3)
# #                 "overallufmgrade_list": {"$push": "$overallufmgrade"},
# #             }
# #             for label, field in all_grade_fields:
# #                 if grading == "REPL":
# #                     param_grade_cond = {
# #                         "$in": [f"${field}", [1, 2, 3]]
# #                     }
# #                 else:
# #                     param_grade_cond = {
# #                         "$eq": [f"${field}", 1]
# #                     }
# #                 group_stage[label] = {
# #                     "$sum": {
# #                         "$cond": [param_grade_cond, 1, 0]
# #                     }
# #                 }
# #                 # Push the raw grade values for this parameter
# #                 group_stage[f"{label}_list"] = {"$push": f"${field}"}

# #             pipeline = [
# #                 {"$match": base_flt},
# #                 {"$group": group_stage}
# #             ]
# #             for row in config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True):
# #                 sku     = row["_id"].get("sku")
# #                 machine = row["_id"].get("machine")
# #                 if not sku or not machine or sku not in result:
# #                     continue
# #                 denom    = row["denominator"]
# #                 vi_dg    = row.get("vi_downgrade",  0)
# #                 uf_pass  = row.get("uf_pass_as_oe", 0)
# #                 oe       = row.get("oe_tyres",      0)

# #                 # Build dynamic overallufmgrade frequency map (any grades present)
# #                 overall_counts = Counter(row.get("overallufmgrade_list", []))
# #                 overall_map = {
# #                     f"overallufmgrade_{g}": cnt
# #                     for g, cnt in overall_counts.items()
# #                 }

# #                 # UFM / GEO / BAL params
# #                 for label, field in all_grade_fields:
# #                     cat = label_to_cat[label]
# #                     if d not in result[sku][cat][label]:
# #                         result[sku][cat][label][d] = {"tbm": {}, "curing": {}, "ufm": {}}

# #                     # Build dynamic parameter grade frequency map (any grades present)
# #                     param_counts = Counter(row.get(f"{label}_list", []))
# #                     param_map = {
# #                         f"{field}_{g}": cnt
# #                         for g, cnt in param_counts.items()
# #                     }

# #                     result[sku][cat][label][d][mtype][str(machine)] = {
# #                         "numerator":   row.get(label, 0),
# #                         "oe_tyres":    oe,
# #                         "denominator": denom,
# #                         "overallufmgrade": overall_map,
# #                         field: param_map,          # e.g. "rfppwcgrade": {"rfppwcgrade_1": 18, ...}
# #                     }

# #                 # Overall yields — machine-wise (unchanged)
# #                 machine_str = str(machine)
# #                 vi_pass  = max(denom - vi_dg, 0)
# #                 uf_denom = max(denom - vi_dg, 0)
# #                 vi_scrap = vi_scrap_map.get(sku, 0)
# #                 warehouse_denom = denom + vi_scrap
# #                 uniformity_denom = max(denom - vi_dg, 0)
# #                 visual_denom = denom + vi_scrap
# #                 visual_num = max(denom - vi_dg, 0)

# #                 # warehouse
# #                 if d not in result[sku]["yields"]["warehouse"]:
# #                     result[sku]["yields"]["warehouse"][d] = {
# #                         "tbm": {}, "curing": {}, "ufm": {}
# #                     }
# #                 result[sku]["yields"]["warehouse"][d][mtype][machine_str] = {
# #                     "numerator": oe,
# #                     "oe_tyres": oe,
# #                     "denominator": warehouse_denom,
# #                 }
# #                 # uniformity
# #                 if d not in result[sku]["yields"]["uniformity"]:
# #                     result[sku]["yields"]["uniformity"][d] = {
# #                         "tbm": {}, "curing": {}, "ufm": {}
# #                     }
# #                 result[sku]["yields"]["uniformity"][d][mtype][machine_str] = {
# #                     "numerator": uf_pass,
# #                     "oe_tyres": oe,
# #                     "denominator": uniformity_denom,
# #                 }
# #                 # visual
# #                 if d not in result[sku]["yields"]["visual"]:
# #                     result[sku]["yields"]["visual"][d] = {
# #                         "tbm": {}, "curing": {}, "ufm": {}
# #                     }
# #                 result[sku]["yields"]["visual"][d][mtype][machine_str] = {
# #                     "numerator": visual_num,
# #                     "oe_tyres": oe,
# #                     "denominator": visual_denom,
# #                 }
# #     return result

# # def compute_yields(flt: dict, grading: str = "OE") -> dict:
# #     """
# #     SKU-wise counts where each grade column == 1 (OE) or in [1,2,3] (REPL).
# #     overallufmgrade_sum = total docs per SKU with grade IN 1-5.

# #     Last logic:
# #       last == 1 → filter: last == 1
# #       last == 0 → filter: rerun == 0 AND last IN [0, 1]
# #     """
# #     grade_fields = {
# #         "ufm_yields": [
# #             ("rfv",      "rfppwcgrade"),
# #             ("rfh1",     "rfh1wcgrade"),
# #             ("rfh2",     "rfh2wcgrade"),
# #             ("lfv",      "lfppwcgrade"),
# #             ("conicity", "conygrade"),
# #             ("plysteer", "plysgrade"),
# #         ],
# #         "geo_yields": [
# #             ("crro",   "crrograde"),
# #             ("trro",   "trrograde"),
# #             ("brro",   "brrograde"),
# #             ("lro",    "lrograde"),
# #             ("lroh1",  "lroh1grade"),
# #             ("crroh1", "crroh1grade"),
# #             ("trroh1", "trroh1grade"),
# #             ("brroh1", "brroh1grade"),
# #             ("lrro",   "lrrograde"),
# #         ],
# #         "bal_yields": [
# #             ("sbal",   "sbalgrade"),
# #             ("couple", "couplegrade"),
# #             ("upper",  "uppergrade"),
# #             ("lower",  "lowergrade"),
# #         ],
# #     }

# #     # Apply last logic on top of existing filter
# #     # The filter flt already contains the correct last/rerun MongoDB expressions
# #     # from build_filter, so we use it directly without re-deriving
# #     yield_flt = dict(flt)

# #     # OE → grade == 1 | REPL → grade in [1, 2, 3]
# #     if grading == "REPL":
# #         oe_grade_cond = {"$in": ["$overallufmgrade", [1, 2, 3]]}
# #         param_grade_cond = lambda field: {"$in": [f"${field}", [1, 2, 3]]}
# #     else:
# #         oe_grade_cond = {"$eq": ["$overallufmgrade", 1]}
# #         param_grade_cond = lambda field: {"$eq": [f"${field}", 1]}

# #     if grading == "REPL":
# #         uf_pass_cond = {
# #             "$and": [
# #                 oe_grade_cond,
# #                 {"$eq": ["$vi_yield", 1]}
# #             ]
# #         }
# #     else:
# #         uf_pass_cond = oe_grade_cond

# #     group_stage = {
# #         "_id":                 "$sku",
# #         "overallufmgrade_sum": {"$sum": {"$cond": [{"$and": [{"$gte": ["$overallufmgrade", 1]}, {"$lte": ["$overallufmgrade", 5]}]}, 1, 0]}},
# #         "pass_as_oe":          {"$sum": {"$cond": [oe_grade_cond, 1, 0]}},
# #         "vi_downgrade":        {"$sum": {"$cond": [{"$eq": ["$vi_yield", 0]}, 1, 0]}},
# #         "uf_pass_as_oe": {
# #             "$sum": {
# #                 "$cond": [uf_pass_cond, 1, 0]
# #             }
# #         },
# #         "barcodes":            {"$push": "$barcode"},
# #     }
# #     for _, fields in grade_fields.items():
# #         for label, field in fields:
# #             group_stage[label] = {"$sum": {"$cond": [param_grade_cond(field), 1, 0]}}
# #             group_stage[f"{field}_values"] = {"$push": f"${field}"}

# #     pipeline = [
# #         {"$match": yield_flt},
# #         {"$group": group_stage}
# #     ]

# #     rows = list(config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True))

# #     # scrap_category_visual per SKU + total scrap_count via last_change_time + sku
# #     # Extract time range from yield_flt (uf_time expected)
# #     start_time = None
# #     end_time = None
# #     if "uf_time" in yield_flt and isinstance(yield_flt["uf_time"], dict):
# #         start_time = yield_flt["uf_time"].get("$gte") or yield_flt["uf_time"].get("$gt")
# #         end_time = yield_flt["uf_time"].get("$lte") or yield_flt["uf_time"].get("$lt")

# #     # Build SKU list from aggregated rows
# #     sku_list = [r["_id"] for r in rows if r.get("_id")]

# #     vi_scrap_by_sku = {}
# #     total_scrap_by_sku = {}

# #     if start_time and end_time and sku_list:
# #         # visual scrap per sku
# #         visual_query = {
# #             "last_change_time": {"$gte": start_time, "$lte": end_time},
# #             "sku": {"$in": sku_list},
# #             "scrap_category": "VISUAL"
# #         }
# #         for sc_row in config_h.data_merged_scrap_historical_data.aggregate([
# #             {"$match": visual_query},
# #             {"$group": {"_id": "$sku", "count": {"$sum": 1}}}
# #         ], allowDiskUse=True):
# #             if sc_row and sc_row.get("_id"):
# #                 vi_scrap_by_sku[sc_row["_id"]] = sc_row.get("count", 0)

# #         # total scrap per sku (no category filter)
# #         total_query = {
# #             "last_change_time": {"$gte": start_time, "$lte": end_time},
# #             "sku": {"$in": sku_list}
# #         }
# #         for sc_row in config_h.data_merged_scrap_historical_data.aggregate([
# #             {"$match": total_query},
# #             {"$group": {"_id": "$sku", "count": {"$sum": 1}}}
# #         ], allowDiskUse=True):
# #             if sc_row and sc_row.get("_id"):
# #                 total_scrap_by_sku[sc_row["_id"]] = sc_row.get("count", 0)
# #     def build_parameter_counts(row, fields):
# #         parameter_counts = {}

# #         for _, field in fields:
# #             values = row.get(f"{field}_values", [])

# #             counts = Counter(
# #                 value for value in values
# #                 if value is not None
# #             )

# #             parameter_counts[field] = {
# #                 f"{field}_{grade}": count
# #                 for grade, count in counts.items()
# #             }

# #         return parameter_counts
# #     sku_results = {}
    
# #     for row in rows:
# #         sku = row["_id"] or "unknown"
# #         sku_results[sku] = {
# #             "overallufmgrade_sum":   row.get("overallufmgrade_sum", 0),
# #             "pass_as_oe":            row.get("pass_as_oe", 0),
# #             "vi_downgrade":          row.get("vi_downgrade", 0),
# #             "uf_pass_as_oe":         row.get("uf_pass_as_oe", 0),
# #             "scrap_category_visual": vi_scrap_by_sku.get(sku, 0),
# #             "scrap_count":           total_scrap_by_sku.get(sku, 0),
# #             # "ufm_yields":  {label: row.get(label, 0) for label, _ in grade_fields["ufm_yields"]},
# #             # "geo_yields":  {label: row.get(label, 0) for label, _ in grade_fields["geo_yields"]},
# #             # "bal_yields":  {label: row.get(label, 0) for label, _ in grade_fields["bal_yields"]},
# #             "ufm_yields": {
# #                 **{
# #                     label: row.get(label, 0)
# #                     for label, _ in grade_fields["ufm_yields"]
# #                 },
# #                 "parameter_counts": build_parameter_counts(
# #                     row,
# #                     grade_fields["ufm_yields"]
# #                 )
# #             },

# #             "geo_yields": {
# #                 **{
# #                     label: row.get(label, 0)
# #                     for label, _ in grade_fields["geo_yields"]
# #                 },
# #                 "parameter_counts": build_parameter_counts(
# #                     row,
# #                     grade_fields["geo_yields"]
# #                 )
# #             },

# #             "bal_yields": {
# #                 **{
# #                     label: row.get(label, 0)
# #                     for label, _ in grade_fields["bal_yields"]
# #                 },
# #                 "parameter_counts": build_parameter_counts(
# #                     row,
# #                     grade_fields["bal_yields"]
# #                 )
# #             },
# #         }

# #     return sku_results


# # UF_PARAMETER_MAP = {
# #     "LRO": "lrograde",
# #     "CONY": "conygrade",
# #     "RFPP": "rfppwcgrade",
# #     "RFH1": "rfh1wcgrade",
# #     "RFH2": "rfh2wcgrade",
# #     "RFH3": "rfh3wcgrade",
# #     "LFPP": "lfppwcgrade",
# #     "CRRQ": "crrograde",
# #     "TRRQ": "trrograde",
# #     "BRRQ": "brrograde",
# #     "PLYS": "plysgrade",
# #     "SBAL": "sbalgrade",
# #     "BUL": "bulwcgrade",
# #     "DEP": "depwcgrade",
# #     "UPPER": "uppergrade",
# #     "LOWER": "lowergrade"
# # }

# # def _get_re_code_map(db):
# #     """Build {condition_code_int: description} from re_codes collection."""
# #     re_codes_col = db["re_codes"]
# #     code_map = {}
# #     for doc in re_codes_col.find({}, {"condition_code": 1, "description": 1}):
# #         try:
# #             code_map[int(doc["condition_code"])] = doc.get("description", str(doc["condition_code"]))
# #         except (ValueError, TypeError):
# #             pass
# #     return code_map

# # def compute_sku_count(flt: dict, start_time: str, end_time: str) -> dict:
# #     """
# #     SKU-wise counts for ALL flags and parameters.

# #     Returns:
# #     {
# #         "sku_count": {
# #             "uf": {
# #                 "LRO": {"GV1": 10, "GV2": 5},
# #                 "CONY": {"GV3": 8},
# #                 ...
# #             },
# #             "vi": {
# #                 "Bead Damage": {"GV1": 2},
# #                 "Bubble": {"GV2": 7},
# #                 ...
# #             },
# #             "scrap": {
# #                 "Cut": {"GV1": 4},
# #                 "Sidewall": {"GV3": 9},
# #                 ...
# #             }
# #         }
# #     }
# #     """
# #     summary_col = config_h.data_merged_summary_historical_data
# #     scrap_col = config_h.data_merged_scrap_historical_data

# #     result = {
# #         "sku_count": {
# #             "uf": {},
# #             "vi": {},
# #             "scrap": {},
# #         }
# #     }

# #     # ------------------------------------------------------------------
# #     # UF — one aggregation per parameter in UF_PARAMETER_MAP
# #     # ------------------------------------------------------------------
# #     for parameter, grade_field in UF_PARAMETER_MAP.items():
# #         match_filter = {
# #             **flt,
# #             "vi_yield": 1,
# #             grade_field: {"$in": [2, 3, 4, 5]},
# #         }
# #         pipeline = [
# #             {"$match": match_filter},
# #             {"$group": {"_id": "$sku", "count": {"$sum": 1}}},
# #             {"$sort": {"count": -1}},
# #         ]
# #         sku_counts = {
# #             doc["_id"]: doc["count"]
# #             for doc in summary_col.aggregate(pipeline, allowDiskUse=True)
# #             if doc.get("_id")
# #         }
# #         if sku_counts:
# #             result["sku_count"]["uf"][parameter] = sku_counts

# #     # ------------------------------------------------------------------
# #     # VI — resolve label from re_comment / re_code, group by label → SKU
# #     # ------------------------------------------------------------------
# #     match_filter_vi = {**flt, "vi_yield": 0}

# #     code_map = _get_re_code_map(config_h.client["production_db_hungary"])
# #     re_description_map = {
# #         str(doc["condition_code"]): doc.get("description", "")
# #         for doc in config_h.re_codes.find(
# #             {},
# #             {"condition_code": 1, "description": 1},
# #         )
# #     }

# #     cursor = summary_col.find(
# #         match_filter_vi,
# #         {"sku": 1, "re_code": 1, "re_comment": 1},
# #     ).batch_size(5000)

# #     vi_by_label = {}
# #     comment_regex = re.compile(r"changed:\s*(\d+)\s*-->")

# #     for doc in cursor:
# #         code = doc.get("re_code")
# #         comment = (doc.get("re_comment") or "").strip()
# #         label = None

# #         if comment.startswith("The VI Reason was changed"):
# #             m = comment_regex.search(comment)
# #             if m:
# #                 changed_code = m.group(1)
# #                 label = re_description_map.get(changed_code, changed_code)

# #         if not label and code is not None:
# #             try:
# #                 label = code_map.get(int(float(code)))
# #             except (ValueError, TypeError):
# #                 pass

# #         if not label:
# #             label = comment if comment else (str(code) if code is not None else None)

# #         if not label or label.lower() == "unknown":
# #             continue

# #         sku_name = doc.get("sku")
# #         if not sku_name:
# #             continue

# #         if label not in vi_by_label:
# #             vi_by_label[label] = {}
# #         vi_by_label[label][sku_name] = vi_by_label[label].get(sku_name, 0) + 1

# #     for label, sku_counts in vi_by_label.items():
# #         result["sku_count"]["vi"][label] = dict(
# #             sorted(sku_counts.items(), key=lambda x: x[1], reverse=True)
# #         )

# #     # ------------------------------------------------------------------
# #     # SCRAP — group by description → SKU from scrap collection
# #     # ------------------------------------------------------------------
# #     match_filter_scrap = {
# #         "last_change_time": {"$gte": start_time, "$lte": end_time},
# #         "description": {"$nin": [None, ""]},
# #     }
# #     if "sku" in flt:
# #         match_filter_scrap["sku"] = flt["sku"]

# #     pipeline_scrap = [
# #         {"$match": match_filter_scrap},
# #         {
# #             "$group": {
# #                 "_id": {"description": "$description", "sku": "$sku"},
# #                 "count": {"$sum": 1},
# #             }
# #         },
# #         {"$sort": {"count": -1}},
# #     ]

# #     scrap_by_desc = {}
# #     for doc in scrap_col.aggregate(pipeline_scrap, allowDiskUse=True):
# #         _id = doc.get("_id") or {}
# #         desc = _id.get("description")
# #         sku = _id.get("sku")
# #         if not desc or not sku:
# #             continue
# #         if desc not in scrap_by_desc:
# #             scrap_by_desc[desc] = {}
# #         scrap_by_desc[desc][sku] = doc["count"]

# #     for desc, sku_counts in scrap_by_desc.items():
# #         result["sku_count"]["scrap"][desc] = dict(
# #             sorted(sku_counts.items(), key=lambda x: x[1], reverse=True)
# #         )

# #     return result

# # def count_description(start_time: str, end_time: str, skus: list) -> dict:
# #     """
# #     Counts per description from data_merged_scrap,
# #     filtered by last_change_time and matching SKUs.
    
# #     Args:
# #         start_time: Start datetime string
# #         end_time: End datetime string
# #         skus: List of SKU values
    
# #     Returns:
# #         Dict mapping description to count
# #     """
# #     # Build a scrap filter similar to `_get_scrap_breakdown` to ensure
# #     # description counts are derived directly from the scrap collection.
# #     scrap_filter = {
# #         "last_change_time": {"$gte": start_time, "$lte": end_time},
# #         "sku": {"$in": list(skus)} if skus else {"$in": []},
# #         "description": {"$nin": [None, ""]}
# #     }

# #     pipeline = [
# #         {"$match": scrap_filter},
# #         {"$group": {"_id": "$description", "count": {"$sum": 1}}},
# #         {"$sort": {"count": -1}}
# #     ]

# #     agg = list(config_h.data_merged_scrap_historical_data.aggregate(pipeline, allowDiskUse=True))

# #     breakdown = {
# #         item["_id"]: item["count"]
# #         for item in agg
# #         if item.get("_id")
# #     }

# #     return breakdown


# # # ─────────────────────────────────────────────────────────────────────────────
# # # BUILD ONE COMBINATION VALUE
# # # ─────────────────────────────────────────────────────────────────────────────
# # def build_combination_value(date_str: str, grading: str, last_val: int, skus: list,
# #                             today_str: str = None, qr_status: str = None,
# #                             sku_filter: str = None, active_skus: list = None) -> dict:
# #     flt = build_filter(date_str, grading, last_val, skus,
# #                        qr_status=qr_status, sku_filter=sku_filter)
# #     trend_anchor = today_str if today_str else date_str
    
# #     # Get time window for scrap queries
# #     start_time, end_time = day_window(date_str)
    
# #     # Use the current combination's matching SKUs for scrap counts.
# #     # This ensures exact sku and qr_status combos only count relevant scrap records.
# #     scrap_skus = [sku_filter] if sku_filter else skus

# #     visual_scrap, uf_scrap = count_scrap_categories(start_time, end_time, scrap_skus)

# #     # Build combination label
# #     combination = ["uf_date", "tyre_grading", "last"]
# #     if qr_status:
# #         combination.append("qr_status")
# #     if sku_filter:
# #         combination.append("sku")

# #     entry = {
# #         "uf_date":      date_str,
# #         "tyre_grading": grading,
# #         "last":         last_val,
# #     }
# #     if qr_status:
# #         entry["qr_status"] = qr_status
# #     if sku_filter:
# #         entry["sku"] = sku_filter

# #     entry["data"] = {
# #         "scrap_cateogry_visual":    visual_scrap,
# #         "scrap_cateogry_uf":        uf_scrap,
# #         "last":                     count_last(flt),
# #         "overallufmgrade":          count_overallufmgrade(flt),
# #         "vi_yield":                 count_vi_yield(flt),
# #         "uf_yield":                 count_uf_yield(flt),
# #         "uf_downgrade_num":         count_uf_downgrade(flt, grading),
# #         "uf_downgrade_condition":   count_uf_downgrade_condition(flt),
# #         "card_split_counts":        card_split_counts(flt),
# #         "re_code":                  count_re_code(flt),
# #         "description":              count_description(start_time, end_time, scrap_skus),
# #         "re_comment":               count_re_comment(flt),
# #         "condition_code":           count_condition_code(start_time, end_time, scrap_skus),
# #         "tyre_counts":              tyre_counts_by_sku(flt, grading),
# #         "uf_downgade":              uf_downgrade_breakdown(flt),
# #         "sku_wise_warehouse_yield": sku_wise_warehouse_yield(flt),
# #         "sku_wise_trend":           sku_wise_trend(trend_anchor, grading, last_val, skus),
# #         "yields":                   compute_yields(flt,grading),
# #         "oe_tyre_bifer":            oe_tyre_bifer(flt),
# #         "compute_sku_count":        compute_sku_count(
# #                                     flt,
# #                                     start_time,
# #                                     end_time
# #                                 )
# #     }
# #     return entry, combination

# # # ============================================================
# # # HEAVILY OPTIMIZED YIELD CALCULATION HELPERS FOR 2CR RECORDS
# # # ============================================================

# # def calculate_warehouse_yield_optimized(summary_data, scrap_data=None):
# #     """Heavily optimized warehouse yield calculation - single pass"""
# #     measured_sum = 0
# #     pass_as_oe = 0
    
# #     # Local variable references for speed
# #     for d in summary_data:
# #         ufm_grade = d.get('overallufmgrade')
# #         if ufm_grade in (1, 2, 3, 4, 5):  # Using tuple for faster lookup
# #             measured_sum += 1
# #             if ufm_grade == 1:
# #                 pass_as_oe += 1
    
# #     if measured_sum == 0:
# #         return 0.0
    
# #     if scrap_data is not None:
# #         vi_scrap_sum = 0
# #         for d in scrap_data:
# #             if d.get('scrap_category') == 'VISUAL':
# #                 vi_scrap_sum += 1
# #         denominator = measured_sum + vi_scrap_sum
# #     else:
# #         denominator = measured_sum
    
# #     if denominator <= 0:
# #         return 0.0
    
# #     return round((pass_as_oe / denominator) * 100, 2)


# # def calculate_overall_visual_yield_optimized(summary_data, scrap_data):
# #     """Heavily optimized visual yield calculation"""
# #     measured_sum = 0
# #     vi_downgrade = 0
    
# #     for d in summary_data:
# #         ufm_grade = d.get('overallufmgrade')
# #         if ufm_grade in (1, 2, 3, 4, 5):
# #             measured_sum += 1
# #             if d.get('vi_yield') == 0:
# #                 vi_downgrade += 1
    
# #     vi_scrap = 0
# #     for d in scrap_data:
# #         if d.get('scrap_category') == 'VISUAL':
# #             vi_scrap += 1
    
# #     possible_oe_count = measured_sum + vi_scrap
# #     vi_pass = measured_sum - vi_downgrade
    
# #     if possible_oe_count <= 0:
# #         return 0.0
    
# #     return round((vi_pass / possible_oe_count) * 100, 2)


# # def calculate_overall_scrap_optimized(summary_data, scrap_data):
# #     """Heavily optimized scrap calculation"""
# #     measured_sum = 0
# #     for d in summary_data:
# #         if d.get('overallufmgrade') in (1, 2, 3, 4, 5):
# #             measured_sum += 1
    
# #     vi_scrap = 0
# #     uf_scrap = 0
# #     for d in scrap_data:
# #         category = d.get('scrap_category')
# #         if category == 'VISUAL':
# #             vi_scrap += 1
# #         elif category == 'UF':
# #             uf_scrap += 1
    
# #     denominator = measured_sum + vi_scrap
    
# #     if denominator <= 0:
# #         return 0.0
    
# #     return round(((vi_scrap + uf_scrap) / denominator) * 100, 2)


# # def calculate_overall_uniformity_yield_optimized(data):
# #     """Heavily optimized uniformity yield calculation"""
# #     measured_sum = 0
# #     uf_pass_as_oe = 0
# #     vi_downgrade = 0
# #     has_vi_yield = False
    
# #     for d in data:
# #         ufm_grade = d.get('overallufmgrade')
# #         if ufm_grade in (1, 2, 3, 4, 5):
# #             measured_sum += 1
# #             if ufm_grade == 1:
# #                 uf_pass_as_oe += 1
            
# #             vi_yield = d.get('vi_yield')
# #             if vi_yield is not None:
# #                 has_vi_yield = True
# #                 if vi_yield == 0:
# #                     vi_downgrade += 1
    
# #     if measured_sum == 0:
# #         return 0.0
    
# #     if has_vi_yield:
# #         denominator = measured_sum - vi_downgrade
# #     else:
# #         denominator = measured_sum
    
# #     if denominator <= 0:
# #         return 0.0
    
# #     result = (uf_pass_as_oe / denominator) * 100
# #     return round(min(result, 100.0), 2)


# # def calculate_warehouse_yield_repl_optimized(summary_data, scrap_data=None):
# #     """Heavily optimized REPL warehouse yield calculation"""
# #     measured_sum = 0
# #     pass_as_oe = 0
    
# #     for d in summary_data:
# #         ufm_grade = d.get('overallufmgrade')
# #         if ufm_grade in (1, 2, 3, 4, 5):
# #             measured_sum += 1
# #             if ufm_grade in (1, 2, 3):
# #                 pass_as_oe += 1
    
# #     if measured_sum == 0:
# #         return 0.0
    
# #     if scrap_data is not None:
# #         vi_scrap_sum = 0
# #         for d in scrap_data:
# #             if d.get('scrap_category') == 'VISUAL':
# #                 vi_scrap_sum += 1
# #         denominator = measured_sum + vi_scrap_sum
# #     else:
# #         denominator = measured_sum
    
# #     if denominator <= 0:
# #         return 0.0
    
# #     return round((pass_as_oe / denominator) * 100, 2)


# # def calculate_overall_visual_yield_repl_optimized(summary_data, scrap_data):
# #     """Heavily optimized REPL visual yield calculation"""
# #     measured_sum = 0
# #     vi_downgrade = 0
    
# #     for d in summary_data:
# #         ufm_grade = d.get('overallufmgrade')
# #         if ufm_grade in (1, 2, 3, 4, 5):
# #             measured_sum += 1
# #             if d.get('vi_yield') == 0:
# #                 vi_downgrade += 1
    
# #     vi_scrap = 0
# #     for d in scrap_data:
# #         if d.get('scrap_category') == 'VISUAL':
# #             vi_scrap += 1
    
# #     possible_oe_count = measured_sum + vi_scrap
# #     vi_pass = measured_sum - vi_downgrade
    
# #     if possible_oe_count <= 0:
# #         return 0.0
    
# #     return round((vi_pass / possible_oe_count) * 100, 2)


# # def calculate_overall_scrap_repl_optimized(summary_data, scrap_data):
# #     """Heavily optimized REPL scrap calculation"""
# #     measured_sum = 0
# #     for d in summary_data:
# #         if d.get('overallufmgrade') in (1, 2, 3, 4, 5):
# #             measured_sum += 1
    
# #     vi_scrap = 0
# #     uf_scrap = 0
# #     for d in scrap_data:
# #         category = d.get('scrap_category')
# #         if category == 'VISUAL':
# #             vi_scrap += 1
# #         elif category == 'UF':
# #             uf_scrap += 1
    
# #     denominator = measured_sum + vi_scrap
    
# #     if denominator <= 0:
# #         return 0.0
    
# #     return round(((vi_scrap + uf_scrap) / denominator) * 100, 2)


# # def calculate_overall_uniformity_yield_repl_optimized(data):
# #     """Heavily optimized REPL uniformity yield calculation"""
# #     measured_sum = 0
# #     uf_pass_as_oe = 0
# #     vi_downgrade = 0
# #     has_vi_yield = False
    
# #     for d in data:
# #         ufm_grade = d.get('overallufmgrade')
# #         if ufm_grade in (1, 2, 3, 4, 5):
# #             measured_sum += 1
# #             if ufm_grade in (1, 2, 3) and d.get('vi_yield') == 1:
# #                 uf_pass_as_oe += 1
            
# #             vi_yield = d.get('vi_yield')
# #             if vi_yield is not None:
# #                 has_vi_yield = True
# #                 if vi_yield == 0:
# #                     vi_downgrade += 1
    
# #     if measured_sum == 0:
# #         return 0.0
    
# #     if has_vi_yield:
# #         denominator = measured_sum - vi_downgrade
# #     else:
# #         denominator = measured_sum
    
# #     if denominator <= 0:
# #         return 0.0
    
# #     result = (uf_pass_as_oe / denominator) * 100
# #     return round(min(result, 100.0), 2)


# # # ============================================================
# # # HEAVILY OPTIMIZED COMMON HELPERS FOR 2CR RECORDS
# # # ============================================================

# # TIME_FIELDS = {
# #     "uf_time": "uf_time",
# #     "cur_time": "cur_time",
# #     "ptb_time": "ptb_time",
# # }

# # SCRAP_TIME_KEY = "scrap_time"

# # # Much smaller batch size for 2 crore records
# # BATCH_SIZE = 50
# # SKU_BATCH_SIZE = 20


# # def build_result_optimized(records_by_sku, scrap_by_sku, grading):
# #     """
# #     Build yield response per SKU + overall daily/monthly yield
# #     Heavily optimized for 2 crore records - processes incrementally
# #     """
# #     result = {}
    
# #     # Process SKUs one at a time to avoid memory buildup
# #     sku_items = list(records_by_sku.items())
    
# #     # Initialize accumulators
# #     total_measured_sum = 0
# #     total_pass_as_oe = 0
# #     total_vi_downgrade = 0
# #     total_vi_scrap = 0
# #     total_uf_scrap = 0
# #     total_uf_pass_as_oe = 0
# #     total_tyre_count = 0
# #     has_vi_yield_global = False
    
# #     for sku, sku_records in sku_items:
# #         sku_scrap = scrap_by_sku.get(sku, [])
        
# #         # Calculate SKU-level metrics in single pass
# #         sku_measured_sum = 0
# #         sku_pass_as_oe = 0
# #         sku_vi_downgrade = 0
# #         sku_uf_pass_as_oe = 0
# #         sku_has_vi_yield = False
        
# #         # Pass 1: Summary data
# #         for d in sku_records:
# #             ufm_grade = d.get('overallufmgrade')
# #             if ufm_grade in (1, 2, 3, 4, 5):
# #                 sku_measured_sum += 1
# #                 if grading == "OE":
# #                     if ufm_grade == 1:
# #                         sku_pass_as_oe += 1
# #                         sku_uf_pass_as_oe += 1
# #                 else:  # REPL
# #                     if ufm_grade in (1, 2, 3):
# #                         sku_pass_as_oe += 1
# #                         if d.get('vi_yield') == 1:
# #                             sku_uf_pass_as_oe += 1
            
# #             vi_yield = d.get('vi_yield')
# #             if vi_yield is not None:
# #                 sku_has_vi_yield = True
# #                 if vi_yield == 0:
# #                     sku_vi_downgrade += 1
        
# #         # Pass 2: Scrap data
# #         sku_vi_scrap = 0
# #         sku_uf_scrap = 0
# #         for d in sku_scrap:
# #             category = d.get('scrap_category')
# #             if category == 'VISUAL':
# #                 sku_vi_scrap += 1
# #             elif category == 'UF':
# #                 sku_uf_scrap += 1
        
# #         # Calculate yields for this SKU
# #         if grading == "OE":
# #             # Warehouse Yield
# #             warehouse_denom = sku_measured_sum + sku_vi_scrap
# #             warehouse_y = round((sku_pass_as_oe / warehouse_denom * 100), 2) if warehouse_denom > 0 else 0.0
            
# #             # Visual Yield
# #             possible_oe = sku_measured_sum + sku_vi_scrap
# #             vi_pass = sku_measured_sum - sku_vi_downgrade
# #             visual_y = round((vi_pass / possible_oe * 100), 2) if possible_oe > 0 else 0.0
            
# #             # Scrap Yield
# #             scrap_denom = sku_measured_sum + sku_vi_scrap
# #             scrap_y = round(((sku_vi_scrap + sku_uf_scrap) / scrap_denom * 100), 2) if scrap_denom > 0 else 0.0
            
# #             # Uniformity Yield
# #             uniform_denom = sku_measured_sum - sku_vi_downgrade if sku_has_vi_yield else sku_measured_sum
# #             uniformity_y = round((sku_uf_pass_as_oe / uniform_denom * 100), 2) if uniform_denom > 0 else 0.0
# #             uniformity_y = min(uniformity_y, 100.0) if uniformity_y > 0 else 0.0
            
# #             # Accumulate for global
# #             total_measured_sum += sku_measured_sum
# #             total_pass_as_oe += sku_pass_as_oe
# #             total_vi_downgrade += sku_vi_downgrade
# #             total_vi_scrap += sku_vi_scrap
# #             total_uf_scrap += sku_uf_scrap
# #             total_uf_pass_as_oe += sku_uf_pass_as_oe
# #             if sku_has_vi_yield:
# #                 has_vi_yield_global = True
# #         else:  # REPL
# #             # Same calculations but with REPL logic
# #             warehouse_denom = sku_measured_sum + sku_vi_scrap
# #             warehouse_y = round((sku_pass_as_oe / warehouse_denom * 100), 2) if warehouse_denom > 0 else 0.0
            
# #             possible_oe = sku_measured_sum + sku_vi_scrap
# #             vi_pass = sku_measured_sum - sku_vi_downgrade
# #             visual_y = round((vi_pass / possible_oe * 100), 2) if possible_oe > 0 else 0.0
            
# #             scrap_denom = sku_measured_sum + sku_vi_scrap
# #             scrap_y = round(((sku_vi_scrap + sku_uf_scrap) / scrap_denom * 100), 2) if scrap_denom > 0 else 0.0
            
# #             uniform_denom = sku_measured_sum - sku_vi_downgrade if sku_has_vi_yield else sku_measured_sum
# #             uniformity_y = round((sku_uf_pass_as_oe / uniform_denom * 100), 2) if uniform_denom > 0 else 0.0
# #             uniformity_y = min(uniformity_y, 100.0) if uniformity_y > 0 else 0.0
            
# #             total_measured_sum += sku_measured_sum
# #             total_pass_as_oe += sku_pass_as_oe
# #             total_vi_downgrade += sku_vi_downgrade
# #             total_vi_scrap += sku_vi_scrap
# #             total_uf_scrap += sku_uf_scrap
# #             total_uf_pass_as_oe += sku_uf_pass_as_oe
# #             if sku_has_vi_yield:
# #                 has_vi_yield_global = True
        
# #         result[sku] = {
# #             "uniformity_yield": round(uniformity_y, 2),
# #             "warehouse_yield": round(warehouse_y, 2),
# #             "visual_yield": round(visual_y, 2),
# #             "scrap_yield": round(scrap_y, 2),
# #             "tyre_count": len(sku_records)
# #         }
        
# #         # Clear processed records to free memory
# #         sku_records.clear()
# #         sku_scrap.clear()
    
# #     # Calculate overall daily/monthly yields using accumulated totals
# #     if grading == "OE":
# #         warehouse_denom = total_measured_sum + total_vi_scrap
# #         daily_warehouse = round((total_pass_as_oe / warehouse_denom * 100), 2) if warehouse_denom > 0 else 0.0
        
# #         possible_oe = total_measured_sum + total_vi_scrap
# #         vi_pass = total_measured_sum - total_vi_downgrade
# #         daily_visual = round((vi_pass / possible_oe * 100), 2) if possible_oe > 0 else 0.0
        
# #         scrap_denom = total_measured_sum + total_vi_scrap
# #         daily_scrap = round(((total_vi_scrap + total_uf_scrap) / scrap_denom * 100), 2) if scrap_denom > 0 else 0.0
        
# #         uniform_denom = total_measured_sum - total_vi_downgrade if has_vi_yield_global else total_measured_sum
# #         daily_uniformity = round((total_uf_pass_as_oe / uniform_denom * 100), 2) if uniform_denom > 0 else 0.0
# #         daily_uniformity = min(daily_uniformity, 100.0) if daily_uniformity > 0 else 0.0
# #     else:
# #         warehouse_denom = total_measured_sum + total_vi_scrap
# #         daily_warehouse = round((total_pass_as_oe / warehouse_denom * 100), 2) if warehouse_denom > 0 else 0.0
        
# #         possible_oe = total_measured_sum + total_vi_scrap
# #         vi_pass = total_measured_sum - total_vi_downgrade
# #         daily_visual = round((vi_pass / possible_oe * 100), 2) if possible_oe > 0 else 0.0
        
# #         scrap_denom = total_measured_sum + total_vi_scrap
# #         daily_scrap = round(((total_vi_scrap + total_uf_scrap) / scrap_denom * 100), 2) if scrap_denom > 0 else 0.0
        
# #         uniform_denom = total_measured_sum - total_vi_downgrade if has_vi_yield_global else total_measured_sum
# #         daily_uniformity = round((total_uf_pass_as_oe / uniform_denom * 100), 2) if uniform_denom > 0 else 0.0
# #         daily_uniformity = min(daily_uniformity, 100.0) if daily_uniformity > 0 else 0.0
    
# #     result["daily_yield"] = {
# #         "warehouse_yield": daily_warehouse or 0.0,
# #         "visual_yield": daily_visual or 0.0,
# #         "scrap_yield": daily_scrap or 0.0,
# #         "uniformity_yield": daily_uniformity or 0.0
# #     }
    
# #     return result


# # def process_summary_records_optimized(summary_records, scrap_by_sku, grading):
# #     """
# #     Generic processor for normal time fields - heavily optimized for 2cr records
# #     """
# #     if not summary_records:
# #         return {}
    
# #     records_by_sku = defaultdict(list)
    
# #     # Process records and group by SKU
# #     for rec in summary_records:
# #         sku = rec.get("sku")
# #         if not sku:
# #             continue
# #         records_by_sku[sku].append(rec)
    
# #     # Free the original list memory
# #     summary_records.clear()
    
# #     return build_result_optimized(records_by_sku, scrap_by_sku, grading)


# # def process_scrap_time_records_optimized(summary_records, scrap_map, grading):
# #     """
# #     Exact barcode-based scrap_time processing - heavily optimized
# #     """
# #     if not summary_records:
# #         return {}
    
# #     records_by_sku = defaultdict(list)
# #     scrap_by_sku = defaultdict(list)
    
# #     # Process records and group by SKU
# #     for rec in summary_records:
# #         sku = rec.get("sku")
# #         barcode = rec.get("barcode")
        
# #         if not sku:
# #             continue
        
# #         records_by_sku[sku].append(rec)
        
# #         if barcode and barcode in scrap_map:
# #             scrap_by_sku[sku].append(scrap_map[barcode])
    
# #     # Free memory
# #     summary_records.clear()
    
# #     return build_result_optimized(records_by_sku, scrap_by_sku, grading)


# # def fetch_summary_records_batch(query, sku_list, batch_size=SKU_BATCH_SIZE):
# #     """
# #     Fetch summary records in very small batches for 2cr records
# #     """
# #     if not sku_list:
# #         return []
    
# #     all_records = []
    
# #     # Process SKUs in tiny batches to minimize memory
# #     for i in range(0, len(sku_list), batch_size):
# #         batch_skus = sku_list[i:i+batch_size]
# #         batch_query = query.copy()
# #         batch_query["sku"] = {"$in": batch_skus}
        
# #         # Use projection to fetch only needed fields
# #         batch_records = list(
# #             config_h.data_merged_summary_historical_data.find(
# #                 batch_query,
# #                 {
# #                     "sku": 1, 
# #                     "overallufmgrade": 1, 
# #                     "vi_yield": 1, 
# #                     "barcode": 1,
# #                     "last": 1,
# #                     "rerun": 1
# #                 }
# #             ).batch_size(min(len(batch_skus) * 2, 100))
# #         )
# #         all_records.extend(batch_records)
# #         # Free batch memory
# #         batch_records.clear()
    
# #     return all_records


# # def fetch_scrap_records_batch(start_date, end_date, active_sku_list, batch_size=SKU_BATCH_SIZE):
# #     """
# #     Fetch scrap records in very small batches for 2cr records
# #     """
# #     all_scrap_records = []
    
# #     for i in range(0, len(active_sku_list), batch_size):
# #         batch_skus = active_sku_list[i:i+batch_size]
# #         batch_records = list(
# #             config_h.data_merged_scrap_historical_data.find(
# #                 {
# #                     "last_change_time": {
# #                         "$gte": start_date,
# #                         "$lte": end_date
# #                     },
# #                     "sku": {
# #                         "$in": batch_skus
# #                     }
# #                 },
# #                 {
# #                     "sku": 1,
# #                     "scrap_category": 1,
# #                     "tire_barcode": 1
# #                 }
# #             ).batch_size(min(len(batch_skus) * 2, 100))
# #         )
# #         all_scrap_records.extend(batch_records)
# #         batch_records.clear()
    
# #     return all_scrap_records


# # # ============================================================
# # # HEAVILY OPTIMIZED MAIN AGGREGATOR FOR 2CR RECORDS
# # # ============================================================

# # def aggregate_yields_optimized(
# #     start_date,
# #     end_date,
# #     doc_filter,
# #     agg_collection_name,
# #     doc_key_name,
# #     doc_key_value
# # ):
# #     """
# #     Heavily optimized aggregation engine for 2 crore records - minimal memory usage
# #     """
    
# #     # --------------------------------------------------------
# #     # ACTIVE SKUS - Use set for faster lookups
# #     # --------------------------------------------------------
# #     active_skus = set(
# #         config_h.model_no_status.distinct(
# #             "sku",
# #             {"status": "active"}
# #         )
# #     )
    
# #     # Get OE and REPL SKUs - use set operations
# #     oe_skus = set(
# #         config_h.channel_no_curing.distinct(
# #             "sku",
# #             {"oe_replacement_slob": "OE"}
# #         )
# #     )
    
# #     repl_skus = set(
# #         config_h.channel_no_curing.distinct(
# #             "sku",
# #             {"oe_replacement_slob": "REPL"}
# #         )
# #     )
    
# #     sku_groups = {
# #         "OE": list(active_skus & oe_skus),
# #         "REPL": list(active_skus & repl_skus)
# #     }
    
# #     active_sku_list = list(active_skus)
    
# #     # --------------------------------------------------------
# #     # FETCH SCRAP RECORDS IN BATCHES - Minimize memory
# #     # --------------------------------------------------------
# #     scrap_records = fetch_scrap_records_batch(start_date, end_date, active_sku_list)
    
# #     # --------------------------------------------------------
# #     # SCRAP MAPS - Using dict comprehension for speed
# #     # --------------------------------------------------------
# #     scrap_by_sku = defaultdict(list)
# #     for s in scrap_records:
# #         sku = s.get("sku")
# #         if sku:
# #             scrap_by_sku[sku].append(s)
    
# #     scrap_map = {
# #         x["tire_barcode"]: x
# #         for x in scrap_records
# #         if x.get("tire_barcode")
# #     }
    
# #     # Free scrap_records memory immediately
# #     scrap_records.clear()
    
# #     # --------------------------------------------------------
# #     # MASTER DOC
# #     # --------------------------------------------------------
# #     doc = {
# #         doc_key_name: doc_key_value,
# #         "created_at": datetime.now(timezone.utc),
# #         "OE": {"last_1": {}, "last_0": {}},
# #         "REPL": {"last_1": {}, "last_0": {}}
# #     }
    
# #     # --------------------------------------------------------
# #     # NORMAL TIME FIELDS - Process each field individually
# #     # --------------------------------------------------------
# #     for grading, sku_list in sku_groups.items():
# #         # Skip empty SKU lists
# #         if not sku_list:
# #             continue
            
# #         last_1 = {}
# #         last_0 = {}
        
# #         # Process each time field
# #         for time_key, mongo_field in TIME_FIELDS.items():
# #             # Process last_1
# #             query_last_1 = {
# #                 mongo_field: {"$gte": start_date, "$lte": end_date},
# #                 "last": 1,
# #                 "rerun": {"$in": [0, 1]},
# #                 "sku": {"$in": sku_list}
# #             }
            
# #             summary_last_1 = fetch_summary_records_batch(query_last_1, sku_list)
# #             last_1[time_key] = process_summary_records_optimized(
# #                 summary_last_1,
# #                 scrap_by_sku,
# #                 grading
# #             )
            
# #             # Process last_0
# #             query_last_0 = {
# #                 mongo_field: {"$gte": start_date, "$lte": end_date},
# #                 "rerun": 0,
# #                 "last": {"$in": [0, 1]},
# #                 "sku": {"$in": sku_list}
# #             }
            
# #             summary_last_0 = fetch_summary_records_batch(query_last_0, sku_list)
# #             last_0[time_key] = process_summary_records_optimized(
# #                 summary_last_0,
# #                 scrap_by_sku,
# #                 grading
# #             )
        
# #         # ---------------- SCRAP TIME - Heavily Optimized ----------------
# #         scrap_time_barcodes = list(scrap_map.keys())
        
# #         if scrap_time_barcodes:
# #             # Process in tiny batches for scrap time
# #             scrap_time_batch = 20
# #             for batch_start in range(0, len(scrap_time_barcodes), scrap_time_batch):
# #                 batch_barcodes = scrap_time_barcodes[batch_start:batch_start + scrap_time_batch]
                
# #                 # last_1
# #                 st_query_last_1 = {
# #                     "barcode": {"$in": batch_barcodes},
# #                     "last": 1,
# #                     "rerun": {"$in": [0, 1]},
# #                     "sku": {"$in": sku_list}
# #                 }
                
# #                 st_summary_last_1 = list(
# #                     config_h.data_merged_summary_historical_data.find(
# #                         st_query_last_1,
# #                         {
# #                             "sku": 1,
# #                             "overallufmgrade": 1,
# #                             "vi_yield": 1,
# #                             "barcode": 1,
# #                             "last": 1,
# #                             "rerun": 1
# #                         }
# #                     ).batch_size(min(len(batch_barcodes), 50))
# #                 )
                
# #                 # Store scrap_time results only for the first batch
# #                 if batch_start == 0:
# #                     last_1[SCRAP_TIME_KEY] = process_scrap_time_records_optimized(
# #                         st_summary_last_1,
# #                         scrap_map,
# #                         grading
# #                     )
# #                 else:
# #                     # Merge results from subsequent batches
# #                     batch_result = process_scrap_time_records_optimized(
# #                         st_summary_last_1,
# #                         scrap_map,
# #                         grading
# #                     )
# #                     # Merge batch results into main result
# #                     for sku, yields in batch_result.items():
# #                         if sku not in last_1[SCRAP_TIME_KEY]:
# #                             last_1[SCRAP_TIME_KEY][sku] = yields
                
# #                 # last_0 - similar processing
# #                 st_query_last_0 = {
# #                     "barcode": {"$in": batch_barcodes},
# #                     "rerun": 0,
# #                     "last": {"$in": [0, 1]},
# #                     "sku": {"$in": sku_list}
# #                 }
                
# #                 st_summary_last_0 = list(
# #                     config_h.data_merged_summary_historical_data.find(
# #                         st_query_last_0,
# #                         {
# #                             "sku": 1,
# #                             "overallufmgrade": 1,
# #                             "vi_yield": 1,
# #                             "barcode": 1,
# #                             "last": 1,
# #                             "rerun": 1
# #                         }
# #                     ).batch_size(min(len(batch_barcodes), 50))
# #                 )
                
# #                 if batch_start == 0:
# #                     last_0[SCRAP_TIME_KEY] = process_scrap_time_records_optimized(
# #                         st_summary_last_0,
# #                         scrap_map,
# #                         grading
# #                     )
# #                 else:
# #                     batch_result = process_scrap_time_records_optimized(
# #                         st_summary_last_0,
# #                         scrap_map,
# #                         grading
# #                     )
# #                     for sku, yields in batch_result.items():
# #                         if sku not in last_0[SCRAP_TIME_KEY]:
# #                             last_0[SCRAP_TIME_KEY][sku] = yields
        
# #         # Add date range info
# #         last_1["date_range_start"] = start_date
# #         last_1["date_range_end"] = end_date
# #         last_0["date_range_start"] = start_date
# #         last_0["date_range_end"] = end_date
        
# #         doc[grading]["last_1"] = last_1
# #         doc[grading]["last_0"] = last_0
    
# #     # --------------------------------------------------------
# #     # SAVE - Use replace_one with upsert
# #     # --------------------------------------------------------
# #     agg_collection_name.replace_one(
# #         doc_filter,
# #         doc,
# #         upsert=True
# #     )
    
# #     return 1


# # # ============================================================
# # # HEAVILY OPTIMIZED DAILY AGGREGATION
# # # ============================================================

# # def aggregate_daily_yields_optimized(production_date=None):
# #     """Heavily optimized daily aggregation for 2cr records"""
# #     if production_date is None:
# #         raise ValueError("production_date is required")
    
# #     date_start, date_end = day_range_str(production_date)
    
# #     return aggregate_yields_optimized(
# #         start_date=date_start,
# #         end_date=date_end,
# #         doc_filter={"production_date": production_date},
# #         agg_collection_name=config_h.oe_yield_daily,
# #         doc_key_name="production_date",
# #         doc_key_value=production_date
# #     )


# # # ============================================================
# # # HEAVILY OPTIMIZED MONTHLY AGGREGATION
# # # ============================================================

# # def aggregate_monthly_yields_optimized(current_date=None):
# #     """Heavily optimized monthly aggregation with minimal memory usage for 2cr records"""
    
# #     if current_date is None:
# #         raise ValueError("current_date is required")
    
# #     if isinstance(current_date, str):
# #         current_date = datetime.strptime(current_date, "%Y-%m-%d").date()
    
# #     if current_date.day == 1:
# #         production_month_date = current_date - timedelta(days=1)
# #     else:
# #         production_month_date = current_date
    
# #     year_month = production_month_date.strftime("%Y-%m")
    
# #     month_start = production_month_date.replace(day=1)
    
# #     date_start = month_start.strftime("%Y-%m-%d") + " 06:00:00"
# #     date_end = current_date.strftime("%Y-%m-%d") + " 05:59:59"
    
# #     return aggregate_yields_optimized(
# #         start_date=date_start,
# #         end_date=date_end,
# #         doc_filter={"month": year_month},
# #         agg_collection_name=config_h.oe_yield_monthly,
# #         doc_key_name="month",
# #         doc_key_value=year_month
# #     )


# # # ─────────────────────────────────────────────────────────────────────────────
# # # ORIGINAL (NON-OPTIMIZED) FUNCTIONS - KEPT FOR BACKWARD COMPATIBILITY
# # # ─────────────────────────────────────────────────────────────────────────────

# # def day_range_str(date_str: str) -> tuple[str, str]:
# #     """
# #     Return (start, end) strings for a 6am-to-6am production day in UTC.
    
# #     Example:
# #         Input: "2026-04-23"
# #         Output: ("2026-04-23 06:00:00", "2026-04-24 05:59:59")
    
# #     Note: All times are in UTC to ensure consistency across the application.
# #     """
# #     d = datetime.strptime(date_str[:10], "%Y-%m-%d")
# #     return (
# #         d.strftime("%Y-%m-%d") + " 06:00:00",
# #         (d + timedelta(days=1)).strftime("%Y-%m-%d") + " 05:59:59",
# #     )


# # def calculate_overall_uniformity_yield(data):
# #     """
# #     Overall Uniformity Yield = (UF pass as OE / (Measured sum - VI downgrade))
    
# #     Simplified interpretation:
# #     - UF pass as OE = overallufmgrade = 1
# #     - Measured sum = overallufmgrade IN (1,2,3,4,5)
# #     - VI downgrade = 0 (if vi_yield field doesn't exist)
    
# #     This gives: overallufmgrade=1 / total measured
# #     """
# #     # Measured sum
# #     measured_sum = sum(1 for d in data if d.get('overallufmgrade') in [1, 2, 3, 4, 5])
    
# #     if measured_sum == 0:
# #         return 0.0
    
# #     # Check if vi_yield field exists in data
# #     has_vi_yield = any(d.get('vi_yield') is not None for d in data)
    
# #     if has_vi_yield:
# #         # VI downgrade = vi_yield = 0 AND last = 1
# #         vi_downgrade = sum(1 for d in data 
# #                           if d.get('vi_yield') == 0 )
        
# #         # UF pass as OE = overallufmgrade = 1 AND last = 1 (excluding VI downgrade)
# #         uf_pass_as_oe = sum(1 for d in data 
# #                            if d.get('overallufmgrade') == 1 )
        
# #         denominator = measured_sum - vi_downgrade
# #         # measured_sum 1798 - vi_downgrade 1  1635
# #     else:
# #         # Simplified: if vi_yield doesn't exist, use standard calculation
# #         # UF pass = overallufmgrade IN (1,2,3,4) / total
# #         uf_pass_as_oe = sum(1 for d in data if d.get('overallufmgrade') in [1, 2, 3, 4])
# #         denominator = measured_sum
    
# #     if denominator <= 0:
# #         return 0.0
    
# #     result = (uf_pass_as_oe / denominator) * 100
    
# #     # Cap at 100% to prevent invalid values
# #     return round(min(result, 100.0), 2)


# # def calculate_overall_scrap(summary_data, scrap_data):
# #     """
# #     Overall Scrap = (VI scrap + UF scrap) / (Measured sum + VI scrap sum)
    
# #     VI scrap = count of tyres where scrap_category = "VISUAL"
# #     UF scrap = count of tyres where scrap_category = "UF"
# #     Measured sum = overallufmgrade IN (1,2,3,4,5)
# #     """
# #     # Measured sum
# #     measured_sum = sum(1 for d in summary_data if d.get('overallufmgrade') in [1, 2, 3, 4, 5])
    
# #     # VI scrap
# #     vi_scrap = sum(1 for d in scrap_data if d.get('scrap_category') == 'VISUAL')
    
# #     # UF scrap
# #     uf_scrap = sum(1 for d in scrap_data if d.get('scrap_category') == 'UF')
    
# #     denominator = measured_sum + vi_scrap
    
# #     if denominator <= 0:
# #         return 0.0
    
# #     return round(((vi_scrap + uf_scrap) / denominator) * 100, 2)


# # def calculate_overall_visual_yield(summary_data, scrap_data):
# #     """
# #     Overall Visual Yield = VI pass as OE / (VI pass + VI scrap)
    
# #     Possible OE count = (overallufmgrade IN (1,2,3,4,5) + VI scrap) - VI downgrade
# #     VI pass = (overallufmgrade IN (1,2,3,4,5) + VI scrap) - VI downgrade
# #     VI scrap = count of tyres where scrap_category = "VISUAL"
# #     VI downgrade = vi_yield = 0 and last = 1
# #     """
# #     # Measured sum
# #     measured_sum = sum(1 for d in summary_data if d.get('overallufmgrade') in [1, 2, 3, 4, 5])
    
# #     # VI scrap
# #     vi_scrap = sum(1 for d in scrap_data if d.get('scrap_category') == 'VISUAL')
    
# #     # VI downgrade
# #     vi_downgrade = sum(1 for d in summary_data 
# #                       if d.get('vi_yield') == 0 )
    
# #     # Possible OE count
# #     possible_oe_count = measured_sum + vi_scrap
    
# #     # VI pass
# #     vi_pass = measured_sum - vi_downgrade  
    
# #     if possible_oe_count <= 0:
# #         return 0.0
# #     return round((vi_pass / possible_oe_count) * 100, 2)


# # def calculate_warehouse_yield(summary_data, scrap_data=None):
# #     """
# #     Overall Warehouse Yield (FF) = Pass as OE / (Measured sum + VI scrap sum)
    
# #     Pass as OE = SUM(overallufmgrade = 1)
# #     Measured sum = SUM(overallufmgrade IN (1,2,3,4,5))
# #     VI scrap sum = count of tyres where scrap_category = "VISUAL"
    
# #     If scrap_data is not provided, uses simplified calculation without VI scrap.
# #     """
    
# #     # Measured sum = SUM(overallufmgrade IN (1,2,3,4,5))
# #     measured_sum = sum(1 for d in summary_data if d.get('overallufmgrade') in [1, 2, 3, 4, 5])
    
# #     if measured_sum == 0:
# #         return 0.0
    
# #     # Pass as OE = SUM(overallufmgrade = 1)
# #     pass_as_oe = sum(1 for d in summary_data if d.get('overallufmgrade') == 1)
    
# #     if scrap_data is not None:
# #         # VI scrap sum = COUNT(scrap_category = 'VISUAL')
# #         vi_scrap_sum = sum(1 for d in scrap_data if d.get('scrap_category') == 'VISUAL')
# #         denominator = measured_sum + vi_scrap_sum
# #     else:
# #         # Simplified calculation without scrap data
# #         denominator = measured_sum
    
# #     if denominator <= 0:
# #         return 0.0

# #     # Warehouse Yield = Pass as OE / (Measured sum + VI scrap sum) * 100
# #     return round((pass_as_oe / denominator) * 100, 2)


# # def calculate_warehouse_yield_repl(summary_data, scrap_data=None):
# #     """
# #     Overall Warehouse Yield (FF) = Pass as OE / (Measured sum + VI scrap sum)
    
# #     Pass as OE = SUM(overallufmgrade = 1)
# #     Measured sum = SUM(overallufmgrade IN (3,4,5))
# #     VI scrap sum = count of tyres where scrap_category = "VISUAL"
    
# #     If scrap_data is not provided, uses simplified calculation without VI scrap.
# #     """
    
# #     # Measured sum = SUM(overallufmgrade IN (3,4,5))
# #     measured_sum = sum(1 for d in summary_data if d.get('overallufmgrade') in [1,2,3, 4, 5])
    
# #     if measured_sum == 0:
# #         return 0.0
    
# #     # Pass as OE = SUM(overallufmgrade = 1)
# #     pass_as_oe = sum(1 for d in summary_data if d.get('overallufmgrade') in [1,2,3])
    
# #     if scrap_data is not None:
# #         # VI scrap sum = COUNT(scrap_category = 'VISUAL')
# #         vi_scrap_sum = sum(1 for d in scrap_data if d.get('scrap_category') == 'VISUAL')
# #         denominator = measured_sum + vi_scrap_sum
# #     else:
# #         # Simplified calculation without scrap data
# #         denominator = measured_sum
    
# #     if denominator <= 0:
# #         return 0.0

# #     # Warehouse Yield = Pass as OE / (Measured sum + VI scrap sum) * 100
# #     return round((pass_as_oe / denominator) * 100, 2)


# # def calculate_overall_scrap_repl(summary_data, scrap_data):
# #     """
# #     Overall Scrap = (VI scrap + UF scrap) / (Measured sum + VI scrap sum)
    
# #     VI scrap = count of tyres where scrap_category = "VISUAL"
# #     UF scrap = count of tyres where scrap_category = "UF"
# #     Measured sum = overallufmgrade IN (3,4,5)
# #     """
# #     # Measured sum
# #     measured_sum = sum(1 for d in summary_data if d.get('overallufmgrade') in [1,2,3, 4, 5])
    
# #     # VI scrap
# #     vi_scrap = sum(1 for d in scrap_data if d.get('scrap_category') == 'VISUAL')
    
# #     # UF scrap
# #     uf_scrap = sum(1 for d in scrap_data if d.get('scrap_category') == 'UF')
    
# #     denominator = measured_sum + vi_scrap
    
# #     if denominator <= 0:
# #         return 0.0
    
# #     return round(((vi_scrap + uf_scrap) / denominator) * 100, 2)


# # def calculate_overall_visual_yield_repl(summary_data, scrap_data):
# #     """
# #     Overall Visual Yield = VI pass as OE / (VI pass + VI scrap)
    
# #     Possible OE count = (overallufmgrade IN (3,4,5) + VI scrap) - VI downgrade
# #     VI pass = (overallufmgrade IN (3,4,5) + VI scrap) - VI downgrade
# #     VI scrap = count of tyres where scrap_category = "VISUAL"
# #     VI downgrade = vi_yield = 0 and last = 1
# #     """
# #     # Measured sum
# #     measured_sum = sum(1 for d in summary_data if d.get('overallufmgrade') in [1,2,3, 4, 5])
    
# #     # VI scrap
# #     vi_scrap = sum(1 for d in scrap_data if d.get('scrap_category') == 'VISUAL')
    
# #     # VI downgrade
# #     vi_downgrade = sum(1 for d in summary_data 
# #                       if d.get('vi_yield') == 0 )
    
# #     # Possible OE count
# #     possible_oe_count = measured_sum + vi_scrap
    
# #     # VI pass
# #     vi_pass = measured_sum - vi_downgrade  
    
# #     if possible_oe_count <= 0:
# #         return 0.0
# #     return round((vi_pass / possible_oe_count) * 100, 2)


# # def calculate_overall_uniformity_yield_repl(data):
# #     """
# #     Overall Uniformity Yield = (UF pass as OE / (Measured sum - VI downgrade))
    
# #     Simplified interpretation:
# #     - UF pass as OE = overallufmgrade = 3
# #     - Measured sum = overallufmgrade IN (3,4,5)
# #     - VI downgrade = 0 (if vi_yield field doesn't exist)
    
# #     This gives: overallufmgrade=3 / total measured
# #     """
# #     # Measured sum
# #     measured_sum = sum(1 for d in data if d.get('overallufmgrade') in [1,2,3, 4, 5])
    
# #     if measured_sum == 0:
# #         return 0.0
    
# #     # Check if vi_yield field exists in data
# #     has_vi_yield = any(d.get('vi_yield') is not None for d in data)
    
# #     if has_vi_yield:
# #         # VI downgrade = vi_yield = 0 AND last = 1
# #         vi_downgrade = sum(1 for d in data 
# #                           if d.get('vi_yield') == 0 )
        
# #         # UF pass as OE = overallufmgrade = 3 AND last = 1 (excluding VI downgrade)
# #         uf_pass_as_oe = sum(
# #             1 for d in data
# #             if d.get('overallufmgrade') in [1, 2, 3]
# #             and d.get('vi_yield') == 1
# #         )
                    
# #         denominator = measured_sum - vi_downgrade
# #         # measured_sum 1798 - vi_downgrade 1  1635
# #     else:
# #         # Simplified: if vi_yield doesn't exist, use standard calculation
# #         # UF pass = overallufmgrade IN (3,4,5) / total
# #         uf_pass_as_oe = sum(1 for d in data if d.get('overallufmgrade') in [1,2,3])
# #         denominator = measured_sum
    
# #     if denominator <= 0:
# #         return 0.0
    
# #     result = (uf_pass_as_oe / denominator) * 100
    
# #     # Cap at 100% to prevent invalid values
# #     return round(min(result, 100.0), 2)


# # def build_result(records_by_sku, scrap_by_sku, grading):
# #     """
# #     Build yield response per SKU + overall daily/monthly yield
# #     """
# #     result = {}
# #     total_records = []
# #     total_scrap = []

# #     for sku, sku_records in records_by_sku.items():
# #         sku_scrap = scrap_by_sku.get(sku, [])
# #         total_records.extend(sku_records)
# #         total_scrap.extend(sku_scrap)

# #         if grading == "OE":
# #             warehouse_y = calculate_warehouse_yield(sku_records, sku_scrap)
# #             visual_y = calculate_overall_visual_yield(sku_records, sku_scrap)
# #             scrap_y = calculate_overall_scrap(sku_records, sku_scrap)
# #             uniformity_y = calculate_overall_uniformity_yield(sku_records)
# #         else:
# #             warehouse_y = calculate_warehouse_yield_repl(sku_records, sku_scrap)
# #             visual_y = calculate_overall_visual_yield_repl(sku_records, sku_scrap)
# #             scrap_y = calculate_overall_scrap_repl(sku_records, sku_scrap)
# #             uniformity_y = calculate_overall_uniformity_yield_repl(sku_records)

# #         result[sku] = {
# #             "uniformity_yield": uniformity_y or 0.0,
# #             "warehouse_yield": warehouse_y or 0.0,
# #             "visual_yield": visual_y or 0.0,
# #             "scrap_yield": scrap_y or 0.0,
# #             "tyre_count": len(sku_records)
# #         }

# #     if grading == "OE":
# #         result["daily_yield"] = {
# #             "warehouse_yield": calculate_warehouse_yield(total_records, total_scrap) or 0.0,
# #             "visual_yield": calculate_overall_visual_yield(total_records, total_scrap) or 0.0,
# #             "scrap_yield": calculate_overall_scrap(total_records, total_scrap) or 0.0,
# #             "uniformity_yield": calculate_overall_uniformity_yield(total_records) or 0.0
# #         }
# #     else:
# #         result["daily_yield"] = {
# #             "warehouse_yield": calculate_warehouse_yield_repl(total_records, total_scrap) or 0.0,
# #             "visual_yield": calculate_overall_visual_yield_repl(total_records, total_scrap) or 0.0,
# #             "scrap_yield": calculate_overall_scrap_repl(total_records, total_scrap) or 0.0,
# #             "uniformity_yield": calculate_overall_uniformity_yield_repl(total_records) or 0.0
# #         }
# #     return result


# # def process_summary_records(summary_records, scrap_by_sku, grading):
# #     """
# #     Generic processor for normal time fields
# #     """
# #     if not summary_records:
# #         return {}

# #     records_by_sku = defaultdict(list)

# #     for rec in summary_records:
# #         sku = rec.get("sku")
# #         if not sku:
# #             continue
# #         records_by_sku[sku].append(rec)

# #     return build_result(records_by_sku, scrap_by_sku, grading)


# # def process_scrap_time_records(summary_records, scrap_map, grading):
# #     """
# #     Exact barcode-based scrap_time processing
# #     """
# #     if not summary_records:
# #         return {}

# #     records_by_sku = defaultdict(list)
# #     scrap_by_sku = defaultdict(list)

# #     for rec in summary_records:
# #         sku = rec.get("sku")
# #         barcode = rec.get("barcode")

# #         if not sku:
# #             continue

# #         records_by_sku[sku].append(rec)

# #         if barcode and barcode in scrap_map:
# #             scrap_by_sku[sku].append(scrap_map[barcode])

# #     return build_result(records_by_sku, scrap_by_sku, grading)


# # # ============================================================
# # # ORIGINAL AGGREGATOR (NON-OPTIMIZED)
# # # ============================================================

# # def aggregate_yields(
# #     start_date,
# #     end_date,
# #     doc_filter,
# #     agg_collection_name,
# #     doc_key_name,
# #     doc_key_value
# # ):
# #     """
# #     Generic aggregation engine (original version)
# #     """
# #     # --------------------------------------------------------
# #     # COLLECTIONS
# #     # --------------------------------------------------------
# #     agg_col = agg_collection_name

# #     # --------------------------------------------------------
# #     # ACTIVE SKUS
# #     # --------------------------------------------------------
# #     active_skus = set(
# #         config_h.model_no_status.distinct(
# #             "sku",
# #             {"status": "active"}
# #         )
# #     )

# #     # Get OE SKUs
# #     oe_skus = set(
# #         config_h.channel_no_curing.distinct(
# #             "sku",
# #             {
# #                 "oe_replacement_slob": "OE"
# #             }
# #         )
# #     )

# #     # Get REPL SKUs
# #     repl_skus = set(
# #         config_h.channel_no_curing.distinct(
# #             "sku",
# #             {
# #                 "oe_replacement_slob": "REPL"
# #             }
# #         )
# #     )

# #     sku_groups = {
# #         "OE": list(active_skus & oe_skus),
# #         "REPL": list(active_skus & repl_skus)
# #     }

# #     active_sku_list = list(active_skus)

# #     # --------------------------------------------------------
# #     # FETCH SCRAP RECORDS ONLY ONCE
# #     # --------------------------------------------------------
# #     scrap_records = list(
# #         config_h.data_merged_scrap_historical_data.find(
# #             {
# #                 "last_change_time": {
# #                     "$gte": start_date,
# #                     "$lte": end_date
# #                 },
# #                 "sku": {
# #                     "$in": list(active_skus)
# #                 }
# #             }
# #         )
# #     )

# #     # --------------------------------------------------------
# #     # SCRAP MAPS
# #     # --------------------------------------------------------
# #     scrap_by_sku = defaultdict(list)

# #     for s in scrap_records:
# #         sku = s.get("sku")
# #         if sku:
# #             scrap_by_sku[sku].append(s)

# #     scrap_map = {
# #         x["tire_barcode"]: x
# #         for x in scrap_records
# #         if x.get("tire_barcode")
# #     }

# #     # --------------------------------------------------------
# #     # MASTER DOC
# #     # --------------------------------------------------------
# #     doc = {
# #         doc_key_name: doc_key_value,
# #         "created_at": datetime.now(timezone.utc),
# #         "OE": {"last_1": {}, "last_0": {}},
# #         "REPL": {"last_1": {}, "last_0": {}}
# #     }

# #     # --------------------------------------------------------
# #     # NORMAL TIME FIELDS
# #     # --------------------------------------------------------
# #     for grading, sku_list in sku_groups.items():
# #         last_1 = {}
# #         last_0 = {}

# #         # ---------------- NORMAL TIME ----------------
# #         for time_key, mongo_field in TIME_FIELDS.items():
# #             query_last_1 = {
# #                 mongo_field: {
# #                     "$gte": start_date,
# #                     "$lte": end_date
# #                 },
# #                 "last": 1,
# #                 "rerun": {"$in": [0, 1]},
# #                 "sku": {"$in": sku_list}
# #             }

# #             query_last_0 = {
# #                 mongo_field: {
# #                     "$gte": start_date,
# #                     "$lte": end_date
# #                 },
# #                 "rerun": 0,
# #                 "last": {"$in": [0, 1]},
# #                 "sku": {"$in": sku_list}
# #             }

# #             summary_last_1 = list(
# #                 config_h.data_merged_summary_historical_data.find(query_last_1).batch_size(500)
# #             )

# #             summary_last_0 = list(
# #                 config_h.data_merged_summary_historical_data.find(query_last_0).batch_size(500)
# #             )

# #             last_1[time_key] = process_summary_records(
# #                 summary_last_1,
# #                 scrap_by_sku,
# #                 grading
# #             )

# #             last_0[time_key] = process_summary_records(
# #                 summary_last_0,
# #                 scrap_by_sku,
# #                 grading
# #             )

# #         # ---------------- SCRAP TIME ----------------
# #         scrap_time_barcodes = list(scrap_map.keys())

# #         if scrap_time_barcodes:
# #             st_query_last_1 = {
# #                 "barcode": {"$in": scrap_time_barcodes},
# #                 "last": 1,
# #                 "rerun": {"$in": [0, 1]},
# #                 "sku": {"$in": sku_list}
# #             }

# #             st_query_last_0 = {
# #                 "barcode": {"$in": scrap_time_barcodes},
# #                 "rerun": 0,
# #                 "last": {"$in": [0, 1]},
# #                 "sku": {"$in": sku_list}
# #             }

# #             st_summary_last_1 = list(
# #                 config_h.data_merged_summary_historical_data.find(st_query_last_1).batch_size(500)
# #             )

# #             st_summary_last_0 = list(
# #                 config_h.data_merged_summary_historical_data.find(st_query_last_0).batch_size(500)
# #             )

# #             last_1[SCRAP_TIME_KEY] = process_scrap_time_records(
# #                 st_summary_last_1,
# #                 scrap_map,
# #                 grading
# #             )

# #             last_0[SCRAP_TIME_KEY] = process_scrap_time_records(
# #                 st_summary_last_0,
# #                 scrap_map,
# #                 grading
# #             )

# #         last_1["date_range_start"] = start_date
# #         last_1["date_range_end"] = end_date

# #         last_0["date_range_start"] = start_date
# #         last_0["date_range_end"] = end_date

# #         doc[grading]["last_1"] = last_1
# #         doc[grading]["last_0"] = last_0

# #     # --------------------------------------------------------
# #     # SAVE
# #     # --------------------------------------------------------
# #     agg_col.replace_one(
# #         doc_filter,
# #         doc,
# #         upsert=True
# #     )

# #     return 1


# # def aggregate_daily_yields(production_date=None):
# #     if production_date is None:
# #         raise ValueError("production_date is required")

# #     date_start, date_end = day_range_str(production_date)

# #     return aggregate_yields(
# #         start_date=date_start,
# #         end_date=date_end,
# #         doc_filter={"production_date": production_date},
# #         agg_collection_name=config_h.oe_yield_daily,
# #         doc_key_name="production_date",
# #         doc_key_value=production_date
# #     )


# # def aggregate_monthly_yields(current_date=None):
# #     if current_date is None:
# #         raise ValueError("current_date is required")

# #     if isinstance(current_date, str):
# #         current_date = datetime.strptime(current_date, "%Y-%m-%d").date()

# #     if current_date.day == 1:
# #         production_month_date = current_date - timedelta(days=1)
# #     else:
# #         production_month_date = current_date

# #     year_month = production_month_date.strftime("%Y-%m")

# #     month_start = production_month_date.replace(day=1)

# #     date_start = month_start.strftime("%Y-%m-%d") + " 06:00:00"
# #     date_end = current_date.strftime("%Y-%m-%d") + " 05:59:59"

# #     return aggregate_yields(
# #         start_date=date_start,
# #         end_date=date_end,
# #         doc_filter={"month": year_month},
# #         agg_collection_name=config_h.oe_yield_monthly,
# #         doc_key_name="month",
# #         doc_key_value=year_month
# #     )


# # # ============================================================
# # # RUN FUNCTION - UPDATED TO USE HEAVILY OPTIMIZED MONTHLY AGGREGATION
# # # ============================================================

# # def run():
# #     from concurrent.futures import ThreadPoolExecutor, as_completed
# #     import os

# #     today = datetime.now(tz=timezone.utc).date()
# #     # # today = datetime(2026,5, 3, tzinfo=timezone.utc).date()

# #     # Process last 3 days in chronological order: T-3 first, then T-2, then T-1
# #     dates_to_process = [
# #         (today - timedelta(days=offset)).strftime("%Y-%m-%d")
# #         for offset in range(3, 0, -1)
# #     ]

# #     tyre_gradings = get_tyre_gradings()
# #     if not tyre_gradings:
# #         logger.error("No tyre_grading values found in production_db. Exiting.")
# #         return

# #     # Get active SKU list once for all date iterations
# #     active_skus_list = list(config_h.model_no_status.distinct("sku", {"status": "active"}))

# #     # ── Pre-fetch grading metadata once (shared across both dates) ───────────
# #     grading_meta = {}  # grading → {skus, qr_statuses, skus_per_qr}
# #     for grading in tyre_gradings:
# #         skus_for_grading = get_skus_for_grading(grading)
# #         if not skus_for_grading:
# #             logger.warning("No SKUs for grading=%s — skipping", grading)
# #             continue
# #         qr_statuses = get_qr_statuses_for_grading(grading)
# #         skus_per_qr = {}
# #         for qr in qr_statuses:
# #             skus_for_qr = get_skus_for_grading_and_qr(grading, qr)
# #             if skus_for_qr:
# #                 skus_per_qr[qr] = skus_for_qr
# #         grading_meta[grading] = {
# #             "skus":       skus_for_grading,
# #             "qr_statuses": qr_statuses,
# #             "skus_per_qr": skus_per_qr
# #         }

# #     if not grading_meta:
# #         logger.error("No valid gradings with SKUs found. Exiting.")
# #         return

# #     # ── Build all (date, combination_args) tasks upfront ─────────────────────
# #     def make_tasks(production_date):
# #         tasks = []
# #         for grading, meta in grading_meta.items():
# #             skus_for_grading = meta["skus"]
# #             skus_per_qr      = meta["skus_per_qr"]
# #             for last_val in LAST_VALUES:
# #                 # Combination 1
# #                 tasks.append((production_date, grading, last_val, skus_for_grading,
# #                             production_date, None, None))
# #                 for qr, skus_for_qr in skus_per_qr.items():
# #                     # Combination 2
# #                     tasks.append((production_date, grading, last_val, skus_for_qr,
# #                                 production_date, qr, None))
# #                     # Combination 3
# #                     for sku in skus_for_qr:
# #                         tasks.append((production_date, grading, last_val, [sku],
# #                                     production_date, qr, sku))
# #         return tasks

# #     def execute_task(args):
# #         production_date, grading, last_val, skus, today_str, qr_status, sku_filter = args
# #         return build_combination_value(
# #             production_date, grading, last_val, skus,
# #             today_str=today_str,
# #             qr_status=qr_status,
# #             sku_filter=sku_filter,
# #             active_skus=active_skus_list
# #         )

# #     # ── Dynamically determine optimal worker count ───────────────────────────
# #     cpu_count = os.cpu_count() or 4
# #     max_workers = min(cpu_count * 3, 50)
# #     logger.info("Using %d worker threads for parallel processing", max_workers)

# #     # ── Process each date ─────────────────────────────────────────────────────
# #     for production_date in dates_to_process:
# #         logger.info("=== Processing production_date: %s ===", production_date)
# #         try:
# #             tasks = make_tasks(production_date)
# #             logger.info("Total combinations to process for %s: %d", production_date, len(tasks))

# #             combo_map = defaultdict(list)
# #             start_time = time.time()

# #             with ThreadPoolExecutor(max_workers=max_workers) as executor:
# #                 future_to_task = {executor.submit(execute_task, t): t for t in tasks}
# #                 completed = 0
# #                 for future in as_completed(future_to_task):
# #                     try:
# #                         val, combo = future.result()
# #                         combo_map[tuple(combo)].append(val)
# #                         completed += 1
# #                         if completed % 50 == 0:
# #                             elapsed = time.time() - start_time
# #                             rate = completed / elapsed if elapsed > 0 else 0
# #                             remaining = len(tasks) - completed
# #                             eta = remaining / rate if rate > 0 else 0
# #                             logger.info("  [%s] %d/%d combinations done (%.1f/sec, ETA: %.1fm)",
# #                                         production_date, completed, len(tasks), rate, eta / 60)
# #                     except Exception as e:
# #                         task = future_to_task[future]
# #                         logger.exception("Combination failed for task %s: %s", task, e)

# #             elapsed_total = time.time() - start_time
# #             logger.info("Completed all combinations for %s in %.1f minutes", 
# #                         production_date, elapsed_total / 60)

# #             if not combo_map:
# #                 logger.warning("No combinations built for date=%s. Nothing stored.", production_date)
# #                 continue

# #             # Build filters_data — one entry per combination type
# #             filters_data = [
# #                 {"combination": list(combo_keys), "values": values}
# #                 for combo_keys, values in combo_map.items()
# #             ]

# #             total = sum(len(v["values"]) for v in filters_data)

# #             doc = {
# #                 "uf_date":      production_date,
# #                 "created_at":   datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
# #                 "filters_data": filters_data
# #             }

# #             # Save to home_data collection
# #             result = config_h.home_data.replace_one({"uf_date": production_date}, doc, upsert=True)
# #             logger.info("Stored %d total values across %d combination types for date=%s. Matched: %d, Modified: %d, Upserted: %d",
# #                         total, len(filters_data), production_date, 
# #                         result.matched_count, result.modified_count, result.upserted_id is not None)

# #             # Verify the data was saved
# #             saved_doc = config_h.home_data.find_one({"uf_date": production_date})
# #             if saved_doc:
# #                 logger.info(" Successfully verified data saved for date=%s", production_date)
# #             else:
# #                 logger.error(" Failed to verify data saved for date=%s", production_date)

# #             try:
# #                 aggregate_daily_yields_optimized(production_date=production_date)
# #                 logger.info("=== Daily Aggregation END for date=%s ===", production_date)
# #             except Exception as e:
# #                 logger.exception("Daily aggregation failed for date=%s: %s", production_date, e)

# #         except Exception as e:
# #             logger.exception("Processing failed for date=%s — skipping to next date: %s", production_date, e)

# #     # Monthly aggregation runs once after all dates are processed - USING HEAVILY OPTIMIZED VERSION
# #     try:
# #         aggregate_monthly_yields_optimized(current_date=today)
# #         logger.info("=== Monthly Aggregation END (Heavily Optimized for 2Cr Records) ===")
# #     except Exception as e:
# #         logger.exception("Monthly aggregation failed: %s", e)


# # if __name__ == "__main__":
# #     run()


# # # jan 2026 ujwala
# # # feb 2026 megha
# # # march vindhya
# # # april ravi
# # # may harshad
# # # june harshad
# # # july avanti
# # # aug avanti




import logging
import os
from datetime import datetime, timedelta, timezone
import time
from flask import Flask
from collections import defaultdict
import config_h
from collections import Counter

app = Flask(__name__)


# last values to iterate
LAST_VALUES = [1, 0]

# Trend: last N days
TREND_DAYS = 7

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

from logging.handlers import TimedRotatingFileHandler

# Create logs folder if it does not exist
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Log file
LOG_FILE = os.path.join(LOG_DIR, "hungary_historical_database.log")

# Rotate log file every midnight
file_handler = TimedRotatingFileHandler(
    LOG_FILE,
    when="midnight",
    interval=1,
    backupCount=30,
    encoding="utf-8"
)

# Rename rotated files with date
file_handler.suffix = "%Y-%m-%d"

# Log format
formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s"
)

file_handler.setFormatter(formatter)

# Console logging
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# Configure logger
logger = logging.getLogger("hungary_historical_database")
logger.setLevel(logging.INFO)

# Avoid duplicate handlers if run/reloaded
if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

logger.propagate = False

# ─────────────────────────────────────────────────────────────────────────────
# MONGO
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def day_window(date_str: str):
    """Return (start_str, end_str) for 6am-to-6am window of given date."""
    d = datetime.strptime(date_str, "%Y-%m-%d")
    start = d.strftime("%Y-%m-%d") + " 06:00:00"
    end   = (d + timedelta(days=1)).strftime("%Y-%m-%d") + " 05:59:59"
    return start, end


def get_tyre_gradings() -> list:
    """Fetch distinct tyre_grading values from production_db.channel_no_curing."""
    gradings = [
        x for x in config_h.channel_no_curing.distinct("oe_replacement_slob")
        if x and str(x).strip() not in ("", "0", "None")
    ]
    if not gradings:
        logger.warning("No tyre_grading values found in channel_no_curing.oe_replacement_slob")
    return gradings


def get_skus_for_grading(grading: str) -> list:
    """Get SKUs from production_db.channel_no_curing for a given tyre_grading."""
    import re
    skus = config_h.channel_no_curing.distinct(
        "sku",
        {"oe_replacement_slob": re.compile(f"^{grading}$", re.IGNORECASE)}
    )
    skus = [s for s in skus if s]
    return skus


def get_qr_statuses_for_grading(grading: str) -> list:
    """Get distinct QR_Status values for a given tyre_grading from production_db."""
    import re
    qr_list = config_h.channel_no_curing.distinct(
        "QR_Status",
        {"oe_replacement_slob": re.compile(f"^{grading}$", re.IGNORECASE)}
    )
    return [q for q in qr_list if q]


def get_skus_for_grading_and_qr(grading: str, qr_status: str) -> list:
    """Get SKUs filtered by both tyre_grading and QR_Status from production_db."""
    import re
    skus = config_h.channel_no_curing.distinct(
        "sku",
        {
            "oe_replacement_slob": re.compile(f"^{grading}$", re.IGNORECASE),
            "QR_Status": qr_status
        }
    )
    return [s for s in skus if s]


def build_filter(date_str: str, grading: str, last_val: int, skus: list,
                 qr_status: str = None, sku_filter: str = None) -> dict:
    start, end = day_window(date_str)

    # first_yield → rerun=0, last IN [0,1]  (rerun=0,last=0 OR rerun=0,last=1)
    # final_yield → last=1                  (rerun=0,last=1 OR rerun=1,last=1)
    if last_val == 0:
        yield_filter = {"rerun": 0, "last": {"$in": [0, 1]}}
    else:  # last_val == 1, default: final_yield
        yield_filter = {"last": 1}

    flt = {
        "uf_time": {"$gte": start, "$lte": end},
        "sku":     {"$in": skus},
        **yield_filter,
    }
    # QR status combos already restrict the SKU list via channel_no_curing.
    # Use the SKU filter directly from the combination and avoid extra sku_type matching.
    if sku_filter:
        flt["sku"] = sku_filter
    return flt


# ─────────────────────────────────────────────────────────────────────────────
# AGGREGATION FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────
def count_last(flt: dict) -> dict:
    pipeline = [
        {"$match": flt},
        {"$group": {"_id": "$last", "count": {"$sum": 1}}}
    ]
    result = {}
    for row in config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True):
        key = f"last_{row['_id']}"
        result[key] = row["count"]
    return result


def count_overallufmgrade(flt: dict) -> dict:
    pipeline = [
        {"$match": flt},
        {"$group": {"_id": "$overallufmgrade", "count": {"$sum": 1}}}
    ]
    result = {}
    for row in config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True):
        key = f"overallufmgrade_{row['_id']}"
        result[key] = row["count"]
    return result


def count_vi_yield(flt: dict) -> dict:
    pipeline = [
        {"$match": flt},
        {"$group": {"_id": "$vi_yield", "count": {"$sum": 1}}}
    ]
    result = {}
    for row in config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True):
        key = f"vi_yield_{row['_id']}"
        result[key] = row["count"]
    return result

def count_uf_yield(flt: dict) -> dict:
    pipeline = [
        {"$match": flt},
        {"$group": {"_id": "$uf_yield", "count": {"$sum": 1}}}
    ]
    result = {}
    for row in config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True):
        key = f"uf_yield_{row['_id']}"
        result[key] = row["count"]
    return result

def count_uf_downgrade_condition(flt: dict) -> dict:
    """Return Numerator and UF_MeasuredSum counts."""

    base_query = {
        **flt,
        "vi_yield": 1,
        "$or": [
            {"overalltugrade": {"$in": [2, 3, 4, 5]}},
            {"overallbalgrade": {"$in": [2, 3, 4, 5]}},
            {"overallsolgrade": {"$in": [2, 3, 4, 5]}}
        ]
    }

    numerator = config_h.data_merged_summary_historical_data.count_documents(base_query)

    uf_measuredsum = config_h.data_merged_summary_historical_data.count_documents({
        **flt,
        "overallufmgrade": {"$in": [2, 3, 4, 5]}
    })

    return {
        "Numerator": numerator,
        "uf_MeasuredSum": uf_measuredsum
    }

def card_split_counts(flt: dict) -> dict:
    """
    Returns actual document counts for tyre_bifer and rework_split using count_documents.
    
    Args:
        collection: MongoDB collection object
        flt: Base filter dictionary
    """
    
    # Tyre Bifer
    tyre_bifer = {
        "count_vi": config_h.data_merged_summary_historical_data.count_documents({
            **flt,
            "overallufmgrade": {"$in": [2, 3]},
            "vi_yield": 0
        }),
        "count_uf": config_h.data_merged_summary_historical_data.count_documents({
            **flt,
            "overallufmgrade": {"$in": [2, 3]},
            "vi_yield": 1,
            "$or": [
                {"overalltugrade": {"$in": [2, 3, 4, 5]}},
                {"overallbalgrade": {"$in": [2, 3, 4, 5]}},
                {"overallsolgrade": {"$in": [2, 3, 4, 5]}}
            ]
        })
    }
    
    # Rework Split
    rework_split = {
        "count_vi": config_h.data_merged_summary_historical_data.count_documents({
            **flt,
            "overallufmgrade": 4,
            "vi_yield": 0
        }),
        "count_uf": config_h.data_merged_summary_historical_data.count_documents({
            **flt,
            "overallufmgrade": 4,
            "vi_yield": 1,
            "$or": [
                {"overalltugrade": {"$in": [2, 3, 4, 5]}},
                {"overallbalgrade": {"$in": [2, 3, 4, 5]}},
                {"overallsolgrade": {"$in": [2, 3, 4, 5]}}
            ]
        })
    }
    
    return {
        "tyre_bifer": tyre_bifer,
        "rework_split": rework_split
    }

def count_re_code(flt: dict) -> dict:
    pipeline = [
        {"$match": {**flt, "re_code": {"$ne": None, "$ne": ""}}},
        {"$group": {"_id": "$re_code", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    return {str(row["_id"]): row["count"] for row in config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True) if row["_id"] is not None}


import re



def count_re_comment(flt: dict) -> dict:

    match_filter = {
        **flt,
        "vi_yield": 0,
        "re_comment": {"$nin": [None, ""]}
    }

    pipeline = [
        {"$match": match_filter},
        {
            "$group": {
                "_id": "$re_comment",
                "count": {"$sum": 1}
            }
        },
        {"$sort": {"count": -1}}
    ]

    rows = list(config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True))

    # --------------------------------------------------------
    # Extract VI reason codes
    # --------------------------------------------------------

    reason_codes = set()

    for row in rows:

        comment = str(row["_id"])

        match = re.search(
            r"The VI Reason was changed:\s*(\d+)\s*-->",
            comment
        )

        if match:
            reason_codes.add(match.group(1))

    # --------------------------------------------------------
    # Fetch descriptions
    # --------------------------------------------------------

    reason_map = {
        x["condition_code"]: x.get("description", "")
        for x in config_h.re_codes.find(
            {
                "condition_code": {
                    "$in": list(reason_codes)
                }
            },
            {
                "_id": 0,
                "condition_code": 1,
                "description": 1
            }
        )
    }

    # --------------------------------------------------------
    # Final result
    # --------------------------------------------------------

    final_result = {}

    for row in rows:

        comment = str(row["_id"])
        count = row["count"]

        match = re.search(
            r"The VI Reason was changed:\s*(\d+)\s*-->",
            comment
        )

        if match:

            code = match.group(1)

            description = reason_map.get(code)

            if description:
                final_result[description] = (
                    final_result.get(description, 0) + count
                )
            else:
                final_result[comment] = (
                    final_result.get(comment, 0) + count
                )

        else:
            final_result[comment] = (
                final_result.get(comment, 0) + count
            )

    return final_result

def oe_tyre_bifer(flt: dict) -> dict:
    pipeline = [
        {
            "$match": {
                **flt,
                "overallufmgrade": 1
            }
        },
        {
            "$group": {
                "_id": {"$ifNull": ["$re_comment", "NULL"]},
                "count": {"$sum": 1}
            }
        },
        {"$sort": {"count": -1}}
    ]

    return {
        str(row["_id"]): row["count"]
        for row in config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True)
    }

# ─────────────────────────────────────────────────────────────────────────────
# SCRAP COLLECTION
# ─────────────────────────────────────────────────────────────────────────────
# config_h.data_merged_scrap_historical_data is now initialized in MONGO section above


def get_scrap_filter(start_time: str, end_time: str, skus: list) -> dict:
    """
    Create filter for scrap collection using last_change_time and matching SKUs.
    
    Args:
        start_time: Start datetime string (e.g., "2026-05-15 06:00:00")
        end_time: End datetime string (e.g., "2026-05-16 05:59:59")
        skus: List of SKU values to filter by
    
    Returns:
        Filter dict for scrap collection query
    """
    return {
        "last_change_time": {"$gte": start_time, "$lte": end_time},
        "sku": {"$in": skus}
    }


def count_scrap_categories(start_time: str, end_time: str, skus: list) -> tuple:
    """
    Returns (visual_count, uf_count) from data_merged_scrap,
    filtered by last_change_time and matching SKUs.
    
    Args:
        start_time: Start datetime string (e.g., "2026-05-15 06:00:00")
        end_time: End datetime string (e.g., "2026-05-16 05:59:59")
        skus: List of SKU values to filter by
    
    Returns:
        Tuple of (visual_count, uf_count)
    """
    scrap_flt = get_scrap_filter(start_time, end_time, skus)
    pipeline = [
        {"$match": scrap_flt},
        {"$group": {"_id": "$scrap_category", "count": {"$sum": 1}}}
    ]
    visual, uf = 0, 0
    for row in config_h.data_merged_scrap_historical_data.aggregate(pipeline, allowDiskUse=True):
        cat = (row["_id"] or "").upper()
        if cat == "VISUAL":
            visual = row["count"]
        elif cat == "UF":
            uf = row["count"]
    return visual, uf


def count_condition_code(start_time: str, end_time: str, skus: list) -> dict:
    """
    Counts per condition_code from data_merged_scrap,
    filtered by last_change_time and matching SKUs.
    
    Args:
        start_time: Start datetime string
        end_time: End datetime string
        skus: List of SKU values
    
    Returns:
        Dict mapping condition_code to count
    """
    scrap_flt = get_scrap_filter(start_time, end_time, skus)
    pipeline = [
        {"$match": {**scrap_flt, "condition_code": {"$ne": None, "$ne": ""}}},
        {"$group": {"_id": "$condition_code", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    return {str(row["_id"]): row["count"] for row in config_h.data_merged_scrap_historical_data.aggregate(pipeline, allowDiskUse=True) if row["_id"] is not None}


def tyre_counts_by_sku(flt: dict,  grading: str = "OE") -> dict:
    """
    Per-SKU counts based on overallufmgrade:
      total_tyres  → overallufmgrade == 1
      rework       → overallufmgrade IN [2, 3]
      replacement  → overallufmgrade == 4
      rejected     → overallufmgrade == 5
    """
    if grading == "REPL":
        oe_grade = {"$in": ["$overallufmgrade", [1, 2, 3]]}
    else:
        oe_grade = {"$eq": ["$overallufmgrade", 1]}

    pipeline = [
        {"$match": flt},
        {"$group": {
            "_id":        "$sku",
            "total":      {"$sum": {"$cond": [oe_grade, 1, 0]}},
            "rework":     {"$sum": {"$cond": [{"$in": ["$overallufmgrade", [2, 3]]}, 1, 0]}},
            "replacement":{"$sum": {"$cond": [{"$eq": ["$overallufmgrade", 4]}, 1, 0]}},
            "rejected":   {"$sum": {"$cond": [{"$eq": ["$overallufmgrade", 5]}, 1, 0]}},
        }}
    ]
    total_d = {}; rework_d = {}; repl_d = {}; rej_d = {}
    for row in config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True):
        sku = row["_id"] or "unknown"
        total_d[sku]  = row["total"]
        rework_d[sku] = row["rework"]
        repl_d[sku]   = row["replacement"]
        rej_d[sku]    = row["rejected"]
    return {
        "oe_tyres":  total_d,
        "rework":       rework_d,
        "replacement":  repl_d,
        "rejected":     rej_d
    }

def count_uf_downgrade(flt: dict, grading: str) -> int:
    """Count tyres where vi_yield == 1 and overallufmgrade != 1 using the base filter."""
    
    query = {**flt}

    if grading == "REPL":
        query["overallufmgrade"] = {"$nin": [1, 2, 3]}
    else:  # OE
        query["overallufmgrade"] = {"$ne": 1}

    return config_h.data_merged_summary_historical_data.count_documents(query)

def uf_downgrade_breakdown(flt: dict) -> dict:

        # 👉 Add VI filter locally (without mutating original filter)
    match_filter = {**flt, "vi_yield": 1}

    """Count tyres where each grade param > 1 (UF downgrade reason)."""
    uf_params = [
        ("lrograde",    "LRO"),   ("conygrade",   "CONY"),
        ("rfppwcgrade", "RFPP"),  ("rfh1wcgrade", "RFH1"),
        ("rfh2wcgrade", "RFH2"),  ("rfh3wcgrade", "RFH3"),
        ("lfppwcgrade", "LFPP"),  ("crrograde",   "CRRQ"),
        ("trrograde",   "TRRQ"),  ("brrograde",   "BRRQ"),
        ("plysgrade",   "PLYS"),  ("sbalgrade",   "SBAL"),
        ("bulwcgrade",  "BUL"),   ("depwcgrade",  "DEP"),
        ("uppergrade",  "UPPER"), ("lowergrade",  "LOWER"),
    ]
    # add_fields = {label: {"$cond": [{"$gt": [f"${field}", 1]}, 1, 0]} for field, label in uf_params}
    add_fields = {
        label: {
            "$cond": [
                {"$in": [f"${field}", [2, 3, 4, 5]]},
                1,
                0
            ]
        }
        for field, label in uf_params
    }
    group_sum  = {label: {"$sum": f"${label}"} for _, label in uf_params}
    pipeline   = [
        {"$match": match_filter},
        {"$addFields": add_fields},
        {"$group": {"_id": None, **group_sum}}
    ]
    agg = list(config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True))
    if not agg:
        return {label: 0 for _, label in uf_params}
    row = agg[0]
    return dict(sorted({label: row.get(label, 0) for _, label in uf_params}.items(),
                        key=lambda x: x[1], reverse=True))

def sku_wise_warehouse_yield(flt: dict) -> dict:
    """Per-SKU overallufmgrade value counts + scrap_cateogry_visual (SKU-wise)."""
    # Step 1: grade counts + barcodes per SKU from summary
    pipeline = [
        {"$match": flt},
        {"$group": {
            "_id":   "$sku",
            "g0":    {"$sum": {"$cond": [{"$eq": ["$overallufmgrade", 0]}, 1, 0]}},
            "g1":    {"$sum": {"$cond": [{"$eq": ["$overallufmgrade", 1]}, 1, 0]}},
            "g2":    {"$sum": {"$cond": [{"$eq": ["$overallufmgrade", 2]}, 1, 0]}},
            "g3":    {"$sum": {"$cond": [{"$eq": ["$overallufmgrade", 3]}, 1, 0]}},
            "g4":    {"$sum": {"$cond": [{"$eq": ["$overallufmgrade", 4]}, 1, 0]}},
            "g5":    {"$sum": {"$cond": [{"$eq": ["$overallufmgrade", 5]}, 1, 0]}},
            "barcodes": {"$push": "$barcode"}
        }}
    ]
    rows = list(config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True))
    # Step 2: get scrap_category == "VISUAL" per SKU by filtering scrap collection
    # using last_change_time range and SKU list extracted from the incoming filter.
    # This avoids joining via barcodes and follows the new requirement.
    vi_scrap_by_sku = {}

    # Extract time range from filter (uf_time expected)
    start_time = None
    end_time = None
    if "uf_time" in flt and isinstance(flt["uf_time"], dict):
        start_time = flt["uf_time"].get("$gte") or flt["uf_time"].get("$gt")
        end_time = flt["uf_time"].get("$lte") or flt["uf_time"].get("$lt")

    # Extract SKU list from filter
    sku_filter = flt.get("sku")
    sku_list = None
    if isinstance(sku_filter, dict) and "$in" in sku_filter:
        sku_list = sku_filter["$in"]
    elif isinstance(sku_filter, list):
        sku_list = sku_filter
    elif isinstance(sku_filter, str):
        sku_list = [sku_filter]

    # Build scrap query only if we have a time range and SKU list
    if start_time and end_time and sku_list:
        scrap_query = {
            "last_change_time": {"$gte": start_time, "$lte": end_time},
            "sku": {"$in": sku_list},
            "scrap_category": "VISUAL"
        }
        for row in config_h.data_merged_scrap_historical_data.aggregate([
            {"$match": scrap_query},
            {"$group": {"_id": "$sku", "count": {"$sum": 1}}}
        ], allowDiskUse=True):
            if row and row.get("_id"):
                vi_scrap_by_sku[row["_id"]] = row.get("count", 0)

    # Step 3: build result
    result = {}
    for row in rows:
        sku = row["_id"] or "unknown"
        result[sku] = {
            "overallufmgrade_0":    row["g0"],
            "overallufmgrade_1":    row["g1"],
            "overallufmgrade_2":    row["g2"],
            "overallufmgrade_3":    row["g3"],
            "overallufmgrade_4":    row["g4"],
            "overallufmgrade_5":    row["g5"],
            "scrap_cateogry_visual": vi_scrap_by_sku.get(sku, 0),
        }
    return result

from collections import Counter

def sku_wise_trend(date_str: str, grading: str, last_val: int, skus: list) -> dict:
    """
    SKU-wise 7-day trend for UFM, GEO, BAL yields.
    Structure: SKU → category → param → date → machine_type → machine → {
        numerator, oe_tyres, denominator,
        overallufmgrade: {overallufmgrade_<grade>: count, ...},
        <grade_field>: {<grade_field>_<grade>: count, ...}
    }
    Grades are dynamic (any integer values present in data) — no hardcoding of 1/2/3.
    One aggregation pipeline per day (not per param/machine) for efficiency.
    numerator   = count(grade_col meets OE/REPL condition) per machine
    denominator = count(overallufmgrade IN 1-5) per machine
    """
    all_params = {
        "UFM": [
            ("rfv",      "rfppwcgrade"),
            ("rfh1",     "rfh1wcgrade"),
            ("rfh2",     "rfh2wcgrade"),
            ("lfv",      "lfppwcgrade"),
            ("conicity", "conygrade"),
            ("plysteer", "plysgrade"),
        ],
        "GEO": [
            ("crro",   "crrograde"),
            ("trro",   "trrograde"),
            ("brro",   "brrograde"),
            ("lro",    "lrograde"),
            ("lroh1",  "lroh1grade"),
            ("crroh1", "crroh1grade"),
            ("trroh1", "trroh1grade"),
            ("brroh1", "brroh1grade"),
            ("lrro",   "lrrograde"),
            ("bulge", "bulwcgrade"),
            ("dent", "depwcgrade"),
        ],
        "BAL": [
            ("sbal",   "sbalgrade"),
            ("couple", "couplegrade"),
            ("upper",  "uppergrade"),
            ("lower",  "lowergrade"),
        ],
    }
    machine_fields = [("tbm", "ptb_id"), ("curing", "cur_id"), ("ufm", "uf_id")]

    # === FIXED SCRAP FETCHING (No machine grouping) ===
    start, end = day_window(date_str)
    vi_scrap_map = {}   # Will store by sku only now
    scrap_pipeline = [
        {
            "$match": {
                "last_change_time": {"$gte": start, "$lt": end},
                "sku": {"$in": skus},
                "scrap_category": "VISUAL"
            }
        },
        {
            "$group": {
                "_id": "$sku",
                "vi_scrap": {"$sum": 1}
            }
        }
    ]
    for row in config_h.data_merged_scrap_historical_data.aggregate(scrap_pipeline, allowDiskUse=True):
        sku = row["_id"]
        vi_scrap_map[sku] = row.get("vi_scrap", 0)

    # Flat list of all (label, field) pairs
    all_grade_fields = [
        (label, field)
        for params in all_params.values()
        for label, field in params
    ]
    # Map label → category
    label_to_cat = {
        label: cat
        for cat, params in all_params.items()
        for label, _ in params
    }
    # Map label → field name (for the dynamic grade key)
    label_to_field = {label: field for label, field in all_grade_fields}

    # Init result structure
    result = {
        sku: {
            cat: {label: {} for label, _ in params}
            for cat, params in all_params.items()
        }
        for sku in skus
    }
    # Add yields section for overall yields
    for sku in skus:
        result[sku]["yields"] = {
            "warehouse":   {},
            "uniformity":  {},
            "visual":      {},
        }

    # Build date windows — only the running date
    start, end = day_window(date_str)
    days = [(date_str, start, end)]

    # first_yield → rerun=0, last IN [0,1]
    # final_yield → last=1
    if last_val == 0:
        yield_filter = {"rerun": 0, "last": {"$in": [0, 1]}}
    else:
        yield_filter = {"last": 1}

    # Determine the overallufmgrade condition based on tyre grading:
    # OE  → grade == 1
    # REPL → grade in [1, 2, 3]
    if grading == "REPL":
        oe_grade_cond = {"$in": ["$overallufmgrade", [1, 2, 3]]}
    else:  # default: OE
        oe_grade_cond = {"$eq": ["$overallufmgrade", 1]}

    # Determine the uf_pass condition based on tyre grading
    if grading == "REPL":
        uf_pass_cond = {
            "$and": [
                oe_grade_cond,
                {"$eq": ["$vi_yield", 1]}
            ]
        }
    else:
        uf_pass_cond = oe_grade_cond

    for d, start, end in days:
        base_flt = {
            "uf_time":         {"$gte": start, "$lt": end},
            "sku":             {"$in": skus},
            "overallufmgrade": {"$in": [1, 2, 3, 4, 5]},
            **yield_filter,
        }
        for mtype, mfield in machine_fields:
            # denominator = SUM(overallufmgrade IN 1-5) per {sku, machine}
            # numerator   = SUM(grade_field meets condition) per {sku, machine}
            # + push full grade lists so we can build dynamic frequency maps
            group_stage = {
                "_id":         {"sku": "$sku", "machine": f"${mfield}"},
                "denominator": {"$sum": 1},
                "oe_tyres":    {"$sum": {"$cond": [oe_grade_cond, 1, 0]}},
                # for overall yields
                "vi_downgrade":  {"$sum": {"$cond": [{"$eq": ["$vi_yield", 0]}, 1, 0]}},
                "uf_pass_as_oe": {
                    "$sum": {
                        "$cond": [uf_pass_cond, 1, 0]
                    }
                },
                # Dynamic grade distributions (no hardcoded 1/2/3)
                "overallufmgrade_list": {"$push": "$overallufmgrade"},
            }
            for label, field in all_grade_fields:
                if grading == "REPL":
                    param_grade_cond = {
                        "$in": [f"${field}", [1, 2, 3]]
                    }
                else:
                    param_grade_cond = {
                        "$eq": [f"${field}", 1]
                    }
                group_stage[label] = {
                    "$sum": {
                        "$cond": [param_grade_cond, 1, 0]
                    }
                }
                # Push the raw grade values for this parameter
                group_stage[f"{label}_list"] = {"$push": f"${field}"}

            pipeline = [
                {"$match": base_flt},
                {"$group": group_stage}
            ]
            for row in config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True):
                sku     = row["_id"].get("sku")
                machine = row["_id"].get("machine")
                if not sku or not machine or sku not in result:
                    continue
                denom    = row["denominator"]
                vi_dg    = row.get("vi_downgrade",  0)
                uf_pass  = row.get("uf_pass_as_oe", 0)
                oe       = row.get("oe_tyres",      0)

                # Build dynamic overallufmgrade frequency map (any grades present)
                overall_counts = Counter(row.get("overallufmgrade_list", []))
                overall_map = {
                    f"overallufmgrade_{g}": cnt
                    for g, cnt in overall_counts.items()
                }

                # UFM / GEO / BAL params
                for label, field in all_grade_fields:
                    cat = label_to_cat[label]
                    if d not in result[sku][cat][label]:
                        result[sku][cat][label][d] = {"tbm": {}, "curing": {}, "ufm": {}}

                    # Build dynamic parameter grade frequency map (any grades present)
                    param_counts = Counter(row.get(f"{label}_list", []))
                    param_map = {
                        f"{field}_{g}": cnt
                        for g, cnt in param_counts.items()
                    }

                    result[sku][cat][label][d][mtype][str(machine)] = {
                        "numerator":   row.get(label, 0),
                        "oe_tyres":    oe,
                        "denominator": denom,
                        "overallufmgrade": overall_map,
                        field: param_map,          # e.g. "rfppwcgrade": {"rfppwcgrade_1": 18, ...}
                    }

                # Overall yields — machine-wise (unchanged)
                machine_str = str(machine)
                vi_pass  = max(denom - vi_dg, 0)
                uf_denom = max(denom - vi_dg, 0)
                vi_scrap = vi_scrap_map.get(sku, 0)
                warehouse_denom = denom + vi_scrap
                uniformity_denom = max(denom - vi_dg, 0)
                visual_denom = denom + vi_scrap
                visual_num = max(denom - vi_dg, 0)

                # warehouse
                if d not in result[sku]["yields"]["warehouse"]:
                    result[sku]["yields"]["warehouse"][d] = {
                        "tbm": {}, "curing": {}, "ufm": {}
                    }
                result[sku]["yields"]["warehouse"][d][mtype][machine_str] = {
                    "numerator": oe,
                    "oe_tyres": oe,
                    "denominator": warehouse_denom,
                }
                # uniformity
                if d not in result[sku]["yields"]["uniformity"]:
                    result[sku]["yields"]["uniformity"][d] = {
                        "tbm": {}, "curing": {}, "ufm": {}
                    }
                result[sku]["yields"]["uniformity"][d][mtype][machine_str] = {
                    "numerator": uf_pass,
                    "oe_tyres": oe,
                    "denominator": uniformity_denom,
                }
                # visual
                if d not in result[sku]["yields"]["visual"]:
                    result[sku]["yields"]["visual"][d] = {
                        "tbm": {}, "curing": {}, "ufm": {}
                    }
                result[sku]["yields"]["visual"][d][mtype][machine_str] = {
                    "numerator": visual_num,
                    "oe_tyres": oe,
                    "denominator": visual_denom,
                }
    return result

def compute_yields(flt: dict, grading: str = "OE") -> dict:
    """
    SKU-wise counts where each grade column == 1 (OE) or in [1,2,3] (REPL).
    overallufmgrade_sum = total docs per SKU with grade IN 1-5.

    Last logic:
      last == 1 → filter: last == 1
      last == 0 → filter: rerun == 0 AND last IN [0, 1]
    """
    grade_fields = {
        "ufm_yields": [
            ("rfv",      "rfppwcgrade"),
            ("rfh1",     "rfh1wcgrade"),
            ("rfh2",     "rfh2wcgrade"),
            ("lfv",      "lfppwcgrade"),
            ("conicity", "conygrade"),
            ("plysteer", "plysgrade"),
        ],
        "geo_yields": [
            ("crro",   "crrograde"),
            ("trro",   "trrograde"),
            ("brro",   "brrograde"),
            ("lro",    "lrograde"),
            ("lroh1",  "lroh1grade"),
            ("crroh1", "crroh1grade"),
            ("trroh1", "trroh1grade"),
            ("brroh1", "brroh1grade"),
            ("lrro",   "lrrograde"),
            ("bulge", "bulwcgrade"),
            ("dent", "depwcgrade"),
        ],
        "bal_yields": [
            ("sbal",   "sbalgrade"),
            ("couple", "couplegrade"),
            ("upper",  "uppergrade"),
            ("lower",  "lowergrade"),
        ],
    }

    # Apply last logic on top of existing filter
    # The filter flt already contains the correct last/rerun MongoDB expressions
    # from build_filter, so we use it directly without re-deriving
    yield_flt = dict(flt)

    # OE → grade == 1 | REPL → grade in [1, 2, 3]
    if grading == "REPL":
        oe_grade_cond = {"$in": ["$overallufmgrade", [1, 2, 3]]}
        param_grade_cond = lambda field: {"$in": [f"${field}", [1, 2, 3]]}
    else:
        oe_grade_cond = {"$eq": ["$overallufmgrade", 1]}
        param_grade_cond = lambda field: {"$eq": [f"${field}", 1]}

    if grading == "REPL":
        uf_pass_cond = {
            "$and": [
                oe_grade_cond,
                {"$eq": ["$vi_yield", 1]}
            ]
        }
    else:
        uf_pass_cond = oe_grade_cond

    group_stage = {
        "_id":                 "$sku",
        "overallufmgrade_sum": {"$sum": {"$cond": [{"$and": [{"$gte": ["$overallufmgrade", 1]}, {"$lte": ["$overallufmgrade", 5]}]}, 1, 0]}},
        "pass_as_oe":          {"$sum": {"$cond": [oe_grade_cond, 1, 0]}},
        "vi_downgrade":        {"$sum": {"$cond": [{"$eq": ["$vi_yield", 0]}, 1, 0]}},
        "uf_pass_as_oe": {
            "$sum": {
                "$cond": [uf_pass_cond, 1, 0]
            }
        },
        "barcodes":            {"$push": "$barcode"},
    }
    for _, fields in grade_fields.items():
        for label, field in fields:
            group_stage[label] = {"$sum": {"$cond": [param_grade_cond(field), 1, 0]}}
            group_stage[f"{field}_values"] = {"$push": f"${field}"}

    pipeline = [
        {"$match": yield_flt},
        {"$group": group_stage}
    ]

    rows = list(config_h.data_merged_summary_historical_data.aggregate(pipeline, allowDiskUse=True))

    # scrap_category_visual per SKU + total scrap_count via last_change_time + sku
    # Extract time range from yield_flt (uf_time expected)
    start_time = None
    end_time = None
    if "uf_time" in yield_flt and isinstance(yield_flt["uf_time"], dict):
        start_time = yield_flt["uf_time"].get("$gte") or yield_flt["uf_time"].get("$gt")
        end_time = yield_flt["uf_time"].get("$lte") or yield_flt["uf_time"].get("$lt")

    # Build SKU list from aggregated rows
    sku_list = [r["_id"] for r in rows if r.get("_id")]

    vi_scrap_by_sku = {}
    total_scrap_by_sku = {}

    if start_time and end_time and sku_list:
        # visual scrap per sku
        visual_query = {
            "last_change_time": {"$gte": start_time, "$lte": end_time},
            "sku": {"$in": sku_list},
            "scrap_category": "VISUAL"
        }
        for sc_row in config_h.data_merged_scrap_historical_data.aggregate([
            {"$match": visual_query},
            {"$group": {"_id": "$sku", "count": {"$sum": 1}}}
        ], allowDiskUse=True):
            if sc_row and sc_row.get("_id"):
                vi_scrap_by_sku[sc_row["_id"]] = sc_row.get("count", 0)

        # total scrap per sku (no category filter)
        total_query = {
            "last_change_time": {"$gte": start_time, "$lte": end_time},
            "sku": {"$in": sku_list}
        }
        for sc_row in config_h.data_merged_scrap_historical_data.aggregate([
            {"$match": total_query},
            {"$group": {"_id": "$sku", "count": {"$sum": 1}}}
        ], allowDiskUse=True):
            if sc_row and sc_row.get("_id"):
                total_scrap_by_sku[sc_row["_id"]] = sc_row.get("count", 0)
    def build_parameter_counts(row, fields):
        parameter_counts = {}

        for _, field in fields:
            values = row.get(f"{field}_values", [])

            counts = Counter(
                value for value in values
                if value is not None
            )

            parameter_counts[field] = {
                f"{field}_{grade}": count
                for grade, count in counts.items()
            }

        return parameter_counts
    sku_results = {}
    
    for row in rows:
        sku = row["_id"] or "unknown"
        sku_results[sku] = {
            "overallufmgrade_sum":   row.get("overallufmgrade_sum", 0),
            "pass_as_oe":            row.get("pass_as_oe", 0),
            "vi_downgrade":          row.get("vi_downgrade", 0),
            "uf_pass_as_oe":         row.get("uf_pass_as_oe", 0),
            "scrap_category_visual": vi_scrap_by_sku.get(sku, 0),
            "scrap_count":           total_scrap_by_sku.get(sku, 0),
            "ufm_yields": {
                **{
                    label: row.get(label, 0)
                    for label, _ in grade_fields["ufm_yields"]
                },
                "parameter_counts": build_parameter_counts(
                    row,
                    grade_fields["ufm_yields"]
                )
            },

            "geo_yields": {
                **{
                    label: row.get(label, 0)
                    for label, _ in grade_fields["geo_yields"]
                },
                "parameter_counts": build_parameter_counts(
                    row,
                    grade_fields["geo_yields"]
                )
            },

            "bal_yields": {
                **{
                    label: row.get(label, 0)
                    for label, _ in grade_fields["bal_yields"]
                },
                "parameter_counts": build_parameter_counts(
                    row,
                    grade_fields["bal_yields"]
                )
            },
        }

    return sku_results


UF_PARAMETER_MAP = {
    "LRO": "lrograde",
    "CONY": "conygrade",
    "RFPP": "rfppwcgrade",
    "RFH1": "rfh1wcgrade",
    "RFH2": "rfh2wcgrade",
    "RFH3": "rfh3wcgrade",
    "LFPP": "lfppwcgrade",
    "CRRQ": "crrograde",
    "TRRQ": "trrograde",
    "BRRQ": "brrograde",
    "PLYS": "plysgrade",
    "SBAL": "sbalgrade",
    "BUL": "bulwcgrade",
    "DEP": "depwcgrade",
    "UPPER": "uppergrade",
    "LOWER": "lowergrade"
}

def _get_re_code_map(db):
    """Build {condition_code_int: description} from re_codes collection."""
    re_codes_col = db["re_codes"]
    code_map = {}
    for doc in re_codes_col.find({}, {"condition_code": 1, "description": 1}):
        try:
            code_map[int(doc["condition_code"])] = doc.get("description", str(doc["condition_code"]))
        except (ValueError, TypeError):
            pass
    return code_map

def compute_sku_count(flt: dict, start_time: str, end_time: str) -> dict:
    """
    SKU-wise counts for ALL flags and parameters.

    Returns:
    {
        "sku_count": {
            "uf": {
                "LRO": {"GV1": 10, "GV2": 5},
                "CONY": {"GV3": 8},
                ...
            },
            "vi": {
                "Bead Damage": {"GV1": 2},
                "Bubble": {"GV2": 7},
                ...
            },
            "scrap": {
                "Cut": {"GV1": 4},
                "Sidewall": {"GV3": 9},
                ...
            }
        }
    }
    """
    summary_col = config_h.data_merged_summary_historical_data
    scrap_col = config_h.data_merged_scrap_historical_data

    result = {
        "sku_count": {
            "uf": {},
            "vi": {},
            "scrap": {},
        }
    }

    # ------------------------------------------------------------------
    # UF — one aggregation per parameter in UF_PARAMETER_MAP
    # ------------------------------------------------------------------
    for parameter, grade_field in UF_PARAMETER_MAP.items():
        match_filter = {
            **flt,
            "vi_yield": 1,
            grade_field: {"$in": [2, 3, 4, 5]},
        }
        pipeline = [
            {"$match": match_filter},
            {"$group": {"_id": "$sku", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
        ]
        sku_counts = {
            doc["_id"]: doc["count"]
            for doc in summary_col.aggregate(pipeline, allowDiskUse=True)
            if doc.get("_id")
        }
        if sku_counts:
            result["sku_count"]["uf"][parameter] = sku_counts

    # ------------------------------------------------------------------
    # VI — resolve label from re_comment / re_code, group by label → SKU
    # ------------------------------------------------------------------
    match_filter_vi = {**flt, "vi_yield": 0}

    code_map = _get_re_code_map(config_h.client["production_db_hungary"])
    re_description_map = {
        str(doc["condition_code"]): doc.get("description", "")
        for doc in config_h.re_codes.find(
            {},
            {"condition_code": 1, "description": 1},
        )
    }

    cursor = summary_col.find(
        match_filter_vi,
        {"sku": 1, "re_code": 1, "re_comment": 1},
    ).batch_size(5000)

    vi_by_label = {}
    comment_regex = re.compile(r"changed:\s*(\d+)\s*-->")

    for doc in cursor:
        code = doc.get("re_code")
        comment = (doc.get("re_comment") or "").strip()
        label = None

        if comment.startswith("The VI Reason was changed"):
            m = comment_regex.search(comment)
            if m:
                changed_code = m.group(1)
                label = re_description_map.get(changed_code, changed_code)

        if not label and code is not None:
            try:
                label = code_map.get(int(float(code)))
            except (ValueError, TypeError):
                pass

        if not label:
            label = comment if comment else (str(code) if code is not None else None)

        if not label or label.lower() == "unknown":
            continue

        sku_name = doc.get("sku")
        if not sku_name:
            continue

        if label not in vi_by_label:
            vi_by_label[label] = {}
        vi_by_label[label][sku_name] = vi_by_label[label].get(sku_name, 0) + 1

    for label, sku_counts in vi_by_label.items():
        result["sku_count"]["vi"][label] = dict(
            sorted(sku_counts.items(), key=lambda x: x[1], reverse=True)
        )

    # ------------------------------------------------------------------
    # SCRAP — group by description → SKU from scrap collection
    # ------------------------------------------------------------------
    match_filter_scrap = {
        "last_change_time": {"$gte": start_time, "$lte": end_time},
        "description": {"$nin": [None, ""]},
    }
    if "sku" in flt:
        match_filter_scrap["sku"] = flt["sku"]

    pipeline_scrap = [
        {"$match": match_filter_scrap},
        {
            "$group": {
                "_id": {"description": "$description", "sku": "$sku"},
                "count": {"$sum": 1},
            }
        },
        {"$sort": {"count": -1}},
    ]

    scrap_by_desc = {}
    for doc in scrap_col.aggregate(pipeline_scrap, allowDiskUse=True):
        _id = doc.get("_id") or {}
        desc = _id.get("description")
        sku = _id.get("sku")
        if not desc or not sku:
            continue
        if desc not in scrap_by_desc:
            scrap_by_desc[desc] = {}
        scrap_by_desc[desc][sku] = doc["count"]

    for desc, sku_counts in scrap_by_desc.items():
        result["sku_count"]["scrap"][desc] = dict(
            sorted(sku_counts.items(), key=lambda x: x[1], reverse=True)
        )

    return result

def count_description(start_time: str, end_time: str, skus: list) -> dict:
    """
    Counts per description from data_merged_scrap,
    filtered by last_change_time and matching SKUs.
    
    Args:
        start_time: Start datetime string
        end_time: End datetime string
        skus: List of SKU values
    
    Returns:
        Dict mapping description to count
    """
    # Build a scrap filter similar to `_get_scrap_breakdown` to ensure
    # description counts are derived directly from the scrap collection.
    scrap_filter = {
        "last_change_time": {"$gte": start_time, "$lte": end_time},
        "sku": {"$in": list(skus)} if skus else {"$in": []},
        "description": {"$nin": [None, ""]}
    }

    pipeline = [
        {"$match": scrap_filter},
        {"$group": {"_id": "$description", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]

    agg = list(config_h.data_merged_scrap_historical_data.aggregate(pipeline, allowDiskUse=True))

    breakdown = {
        item["_id"]: item["count"]
        for item in agg
        if item.get("_id")
    }

    return breakdown


# ─────────────────────────────────────────────────────────────────────────────
# BUILD ONE COMBINATION VALUE
# ─────────────────────────────────────────────────────────────────────────────
def build_combination_value(date_str: str, grading: str, last_val: int, skus: list,
                            today_str: str = None, qr_status: str = None,
                            sku_filter: str = None, active_skus: list = None) -> dict:
    flt = build_filter(date_str, grading, last_val, skus,
                       qr_status=qr_status, sku_filter=sku_filter)
    trend_anchor = today_str if today_str else date_str
    
    # Get time window for scrap queries
    start_time, end_time = day_window(date_str)
    
    # Use the current combination's matching SKUs for scrap counts.
    # This ensures exact sku and qr_status combos only count relevant scrap records.
    scrap_skus = [sku_filter] if sku_filter else skus

    visual_scrap, uf_scrap = count_scrap_categories(start_time, end_time, scrap_skus)

    # Build combination label
    combination = ["uf_date", "tyre_grading", "last"]
    if qr_status:
        combination.append("qr_status")
    if sku_filter:
        combination.append("sku")

    entry = {
        "uf_date":      date_str,
        "tyre_grading": grading,
        "last":         last_val,
    }
    if qr_status:
        entry["qr_status"] = qr_status
    if sku_filter:
        entry["sku"] = sku_filter

    entry["data"] = {
        "scrap_cateogry_visual":    visual_scrap,
        "scrap_cateogry_uf":        uf_scrap,
        "last":                     count_last(flt),
        "overallufmgrade":          count_overallufmgrade(flt),
        "vi_yield":                 count_vi_yield(flt),
        "uf_yield":                 count_uf_yield(flt),
        "uf_downgrade_num":         count_uf_downgrade(flt, grading),
        "uf_downgrade_condition":   count_uf_downgrade_condition(flt),
        "card_split_counts":        card_split_counts(flt),
        "re_code":                  count_re_code(flt),
        "description":              count_description(start_time, end_time, scrap_skus),
        "re_comment":               count_re_comment(flt),
        "condition_code":           count_condition_code(start_time, end_time, scrap_skus),
        "tyre_counts":              tyre_counts_by_sku(flt, grading),
        "uf_downgade":              uf_downgrade_breakdown(flt),
        "sku_wise_warehouse_yield": sku_wise_warehouse_yield(flt),
        "sku_wise_trend":           sku_wise_trend(trend_anchor, grading, last_val, skus),
        "yields":                   compute_yields(flt,grading),
        "oe_tyre_bifer":            oe_tyre_bifer(flt),
        "compute_sku_count":        compute_sku_count(
                                    flt,
                                    start_time,
                                    end_time
                                )
    }
    return entry, combination


# ============================================================
# IMPROVED YIELD CALCULATION FUNCTIONS - FIXED SCRAP YIELD
# ============================================================

def calculate_warehouse_yield_fixed(summary_data, scrap_data=None):
    """
    Fixed Warehouse Yield Calculation
    Warehouse Yield = Pass as OE / (Measured sum + VI scrap)
    """
    measured_sum = 0
    pass_as_oe = 0
    
    for d in summary_data:
        ufm_grade = d.get('overallufmgrade')
        if ufm_grade in (1, 2, 3, 4, 5):
            measured_sum += 1
            if ufm_grade == 1:
                pass_as_oe += 1
    
    if measured_sum == 0:
        return 0.0
    
    vi_scrap_sum = 0
    if scrap_data is not None:
        for d in scrap_data:
            if d.get('scrap_category') == 'VISUAL':
                vi_scrap_sum += 1
    
    denominator = measured_sum + vi_scrap_sum
    
    if denominator <= 0:
        return 0.0
    
    return round((pass_as_oe / denominator) * 100, 2)


def calculate_visual_yield_fixed(summary_data, scrap_data):
    """
    Fixed Visual Yield Calculation
    Visual Yield = VI pass / (VI pass + VI scrap)
    where VI pass = Measured sum - VI downgrade
    """
    measured_sum = 0
    vi_downgrade = 0
    
    for d in summary_data:
        ufm_grade = d.get('overallufmgrade')
        if ufm_grade in (1, 2, 3, 4, 5):
            measured_sum += 1
            if d.get('vi_yield') == 0:
                vi_downgrade += 1
    
    vi_scrap = 0
    for d in scrap_data:
        if d.get('scrap_category') == 'VISUAL':
            vi_scrap += 1
    
    vi_pass = measured_sum - vi_downgrade
    denominator = vi_pass + vi_scrap
    
    if denominator <= 0:
        return 0.0
    
    return round((vi_pass / denominator) * 100, 2)


def calculate_scrap_yield_fixed(summary_data, scrap_data):
    """
    FIXED: Proper Scrap Yield Calculation
    Scrap Yield = (VI scrap + UF scrap) / (Measured sum + VI scrap)
    """
    measured_sum = 0
    for d in summary_data:
        if d.get('overallufmgrade') in (1, 2, 3, 4, 5):
            measured_sum += 1
    
    vi_scrap = 0
    uf_scrap = 0
    for d in scrap_data:
        category = d.get('scrap_category')
        if category == 'VISUAL':
            vi_scrap += 1
        elif category == 'UF':
            uf_scrap += 1
    
    denominator = measured_sum + vi_scrap
    
    if denominator <= 0:
        return 0.0
    
    return round(((vi_scrap + uf_scrap) / denominator) * 100, 2)


def calculate_uniformity_yield_fixed(data):
    """
    Fixed Uniformity Yield Calculation
    Uniformity Yield = UF pass / (Measured sum - VI downgrade)
    """
    measured_sum = 0
    uf_pass_as_oe = 0
    vi_downgrade = 0
    has_vi_yield = False
    
    for d in data:
        ufm_grade = d.get('overallufmgrade')
        if ufm_grade in (1, 2, 3, 4, 5):
            measured_sum += 1
            if ufm_grade == 1:
                uf_pass_as_oe += 1
            
            vi_yield = d.get('vi_yield')
            if vi_yield is not None:
                has_vi_yield = True
                if vi_yield == 0:
                    vi_downgrade += 1
    
    if measured_sum == 0:
        return 0.0
    
    denominator = measured_sum - vi_downgrade if has_vi_yield else measured_sum
    
    if denominator <= 0:
        return 0.0
    
    result = (uf_pass_as_oe / denominator) * 100
    return round(min(result, 100.0), 2)


# ============================================================
# FIXED BUILD RESULT FUNCTION
# ============================================================

def build_result_fixed(records_by_sku, scrap_by_sku, grading):
    """
    Fixed build result with proper scrap yield calculation for GV performance
    """
    result = {}
    
    # Process each SKU
    for sku, sku_records in records_by_sku.items():
        sku_scrap = scrap_by_sku.get(sku, [])
        
        # Get summary data for this SKU
        summary_data = sku_records
        scrap_data = sku_scrap
        
        # Calculate yields using fixed formulas
        if grading == "OE":
            warehouse_y = calculate_warehouse_yield_fixed(summary_data, scrap_data)
            visual_y = calculate_visual_yield_fixed(summary_data, scrap_data)
            scrap_y = calculate_scrap_yield_fixed(summary_data, scrap_data)
            uniformity_y = calculate_uniformity_yield_fixed(summary_data)
        else:  # REPL
            warehouse_y = calculate_warehouse_yield_repl_fixed(summary_data, scrap_data)
            visual_y = calculate_visual_yield_repl_fixed(summary_data, scrap_data)
            scrap_y = calculate_scrap_yield_repl_fixed(summary_data, scrap_data)
            uniformity_y = calculate_uniformity_yield_repl_fixed(summary_data)
        
        result[sku] = {
            "uniformity_yield": float(f"{uniformity_y:.2f}") if uniformity_y else 0.0,
            "warehouse_yield": float(f"{warehouse_y:.2f}") if warehouse_y else 0.0,
            "visual_yield": float(f"{visual_y:.2f}") if visual_y else 0.0,
            "scrap_yield": float(f"{scrap_y:.2f}") if scrap_y else 0.0,
            "tyre_count": len(sku_records)
        }
    
    # Calculate overall daily/monthly yields
    if records_by_sku:
        # Aggregate all records for overall calculation
        all_summary = []
        all_scrap = []
        
        for sku, sku_records in records_by_sku.items():
            all_summary.extend(sku_records)
            all_scrap.extend(scrap_by_sku.get(sku, []))
        
        if grading == "OE":
            result["daily_yield"] = {
                "warehouse_yield": float(f"{calculate_warehouse_yield_fixed(all_summary, all_scrap):.2f}") or 0.0,
                "visual_yield": float(f"{calculate_visual_yield_fixed(all_summary, all_scrap):.2f}") or 0.0,
                "scrap_yield": float(f"{calculate_scrap_yield_fixed(all_summary, all_scrap):.2f}") or 0.0,
                "uniformity_yield": float(f"{calculate_uniformity_yield_fixed(all_summary):.2f}") or 0.0
            }
        else:
            result["daily_yield"] = {
                "warehouse_yield": float(f"{calculate_warehouse_yield_repl_fixed(all_summary, all_scrap):.2f}") or 0.0,
                "visual_yield": float(f"{calculate_visual_yield_repl_fixed(all_summary, all_scrap):.2f}") or 0.0,
                "scrap_yield": float(f"{calculate_scrap_yield_repl_fixed(all_summary, all_scrap):.2f}") or 0.0,
                "uniformity_yield": float(f"{calculate_uniformity_yield_repl_fixed(all_summary):.2f}") or 0.0
            }
    
    return result


# ============================================================
# FIXED REPL YIELD CALCULATIONS
# ============================================================

def calculate_warehouse_yield_repl_fixed(summary_data, scrap_data=None):
    """Fixed REPL Warehouse Yield Calculation"""
    measured_sum = 0
    pass_as_oe = 0
    
    for d in summary_data:
        ufm_grade = d.get('overallufmgrade')
        if ufm_grade in (1, 2, 3, 4, 5):
            measured_sum += 1
            if ufm_grade in (1, 2, 3):
                pass_as_oe += 1
    
    if measured_sum == 0:
        return 0.0
    
    vi_scrap_sum = 0
    if scrap_data is not None:
        for d in scrap_data:
            if d.get('scrap_category') == 'VISUAL':
                vi_scrap_sum += 1
    
    denominator = measured_sum + vi_scrap_sum
    
    if denominator <= 0:
        return 0.0
    
    return round((pass_as_oe / denominator) * 100, 2)


def calculate_visual_yield_repl_fixed(summary_data, scrap_data):
    """Fixed REPL Visual Yield Calculation"""
    measured_sum = 0
    vi_downgrade = 0
    
    for d in summary_data:
        ufm_grade = d.get('overallufmgrade')
        if ufm_grade in (1, 2, 3, 4, 5):
            measured_sum += 1
            if d.get('vi_yield') == 0:
                vi_downgrade += 1
    
    vi_scrap = 0
    for d in scrap_data:
        if d.get('scrap_category') == 'VISUAL':
            vi_scrap += 1
    
    vi_pass = measured_sum - vi_downgrade
    denominator = vi_pass + vi_scrap
    
    if denominator <= 0:
        return 0.0
    
    return round((vi_pass / denominator) * 100, 2)


def calculate_scrap_yield_repl_fixed(summary_data, scrap_data):
    """Fixed REPL Scrap Yield Calculation"""
    measured_sum = 0
    for d in summary_data:
        if d.get('overallufmgrade') in (1, 2, 3, 4, 5):
            measured_sum += 1
    
    vi_scrap = 0
    uf_scrap = 0
    for d in scrap_data:
        category = d.get('scrap_category')
        if category == 'VISUAL':
            vi_scrap += 1
        elif category == 'UF':
            uf_scrap += 1
    
    denominator = measured_sum + vi_scrap
    
    if denominator <= 0:
        return 0.0
    
    return round(((vi_scrap + uf_scrap) / denominator) * 100, 2)


def calculate_uniformity_yield_repl_fixed(data):
    """Fixed REPL Uniformity Yield Calculation"""
    measured_sum = 0
    uf_pass_as_oe = 0
    vi_downgrade = 0
    has_vi_yield = False
    
    for d in data:
        ufm_grade = d.get('overallufmgrade')
        if ufm_grade in (1, 2, 3, 4, 5):
            measured_sum += 1
            if ufm_grade in (1, 2, 3) and d.get('vi_yield') == 1:
                uf_pass_as_oe += 1
            
            vi_yield = d.get('vi_yield')
            if vi_yield is not None:
                has_vi_yield = True
                if vi_yield == 0:
                    vi_downgrade += 1
    
    if measured_sum == 0:
        return 0.0
    
    denominator = measured_sum - vi_downgrade if has_vi_yield else measured_sum
    
    if denominator <= 0:
        return 0.0
    
    result = (uf_pass_as_oe / denominator) * 100
    return round(min(result, 100.0), 2)


# ============================================================
# OPTIMIZED COMMON HELPERS
# ============================================================

TIME_FIELDS = {
    "uf_time": "uf_time",
    "cur_time": "cur_time",
    "ptb_time": "ptb_time",
}

SCRAP_TIME_KEY = "scrap_time"

# Batch size for processing
BATCH_SIZE = 50
SKU_BATCH_SIZE = 20


def process_summary_records_fixed(summary_records, scrap_by_sku, grading):
    """
    Process summary records with fixed scrap yield calculation
    """
    if not summary_records:
        return {}
    
    records_by_sku = defaultdict(list)
    
    for rec in summary_records:
        sku = rec.get("sku")
        if not sku:
            continue
        records_by_sku[sku].append(rec)
    
    # Free memory
    summary_records.clear()
    
    return build_result_fixed(records_by_sku, scrap_by_sku, grading)


def process_scrap_time_records_fixed(summary_records, scrap_map, grading):
    """
    Process scrap time records with fixed scrap yield calculation
    """
    if not summary_records:
        return {}
    
    records_by_sku = defaultdict(list)
    scrap_by_sku = defaultdict(list)
    
    for rec in summary_records:
        sku = rec.get("sku")
        barcode = rec.get("barcode")
        
        if not sku:
            continue
        
        records_by_sku[sku].append(rec)
        
        if barcode and barcode in scrap_map:
            scrap_by_sku[sku].append(scrap_map[barcode])
    
    # Free memory
    summary_records.clear()
    
    return build_result_fixed(records_by_sku, scrap_by_sku, grading)


def fetch_summary_records_batch(query, sku_list, batch_size=SKU_BATCH_SIZE):
    """
    Fetch summary records in batches
    """
    if not sku_list:
        return []
    
    all_records = []
    
    for i in range(0, len(sku_list), batch_size):
        batch_skus = sku_list[i:i+batch_size]
        batch_query = query.copy()
        batch_query["sku"] = {"$in": batch_skus}
        
        batch_records = list(
            config_h.data_merged_summary_historical_data.find(
                batch_query,
                {
                    "sku": 1, 
                    "overallufmgrade": 1, 
                    "vi_yield": 1, 
                    "barcode": 1,
                    "last": 1,
                    "rerun": 1
                }
            ).batch_size(min(len(batch_skus) * 2, 100))
        )
        all_records.extend(batch_records)
        batch_records.clear()
    
    return all_records


def fetch_scrap_records_batch(start_date, end_date, active_sku_list, batch_size=SKU_BATCH_SIZE):
    """
    Fetch scrap records in batches
    """
    all_scrap_records = []
    
    for i in range(0, len(active_sku_list), batch_size):
        batch_skus = active_sku_list[i:i+batch_size]
        batch_records = list(
            config_h.data_merged_scrap_historical_data.find(
                {
                    "last_change_time": {
                        "$gte": start_date,
                        "$lte": end_date
                    },
                    "sku": {
                        "$in": batch_skus
                    }
                },
                {
                    "sku": 1,
                    "scrap_category": 1,
                    "tire_barcode": 1
                }
            ).batch_size(min(len(batch_skus) * 2, 100))
        )
        all_scrap_records.extend(batch_records)
        batch_records.clear()
    
    return all_scrap_records


# ============================================================
# FIXED MAIN AGGREGATOR
# ============================================================

def aggregate_yields_fixed(
    start_date,
    end_date,
    doc_filter,
    agg_collection_name,
    doc_key_name,
    doc_key_value
):
    """
    Fixed aggregation engine with proper scrap yield calculation
    """
    
    # --------------------------------------------------------
    # ACTIVE SKUS
    # --------------------------------------------------------
    active_skus = set(
        config_h.model_no_status.distinct(
            "sku",
            {"status": "active"}
        )
    )
    
    oe_skus = set(
        config_h.channel_no_curing.distinct(
            "sku",
            {"oe_replacement_slob": "OE"}
        )
    )
    
    repl_skus = set(
        config_h.channel_no_curing.distinct(
            "sku",
            {"oe_replacement_slob": "REPL"}
        )
    )
    
    sku_groups = {
        "OE": list(active_skus & oe_skus),
        "REPL": list(active_skus & repl_skus)
    }
    
    active_sku_list = list(active_skus)
    
    # --------------------------------------------------------
    # FETCH SCRAP RECORDS
    # --------------------------------------------------------
    scrap_records = fetch_scrap_records_batch(start_date, end_date, active_sku_list)
    
    scrap_by_sku = defaultdict(list)
    for s in scrap_records:
        sku = s.get("sku")
        if sku:
            scrap_by_sku[sku].append(s)
    
    scrap_map = {
        x["tire_barcode"]: x
        for x in scrap_records
        if x.get("tire_barcode")
    }
    
    scrap_records.clear()
    
    # --------------------------------------------------------
    # MASTER DOC
    # --------------------------------------------------------
    doc = {
        doc_key_name: doc_key_value,
        "created_at": datetime.now(timezone.utc),
        "OE": {"last_1": {}, "last_0": {}},
        "REPL": {"last_1": {}, "last_0": {}}
    }
    
    # --------------------------------------------------------
    # PROCESS EACH GRADING AND TIME FIELD
    # --------------------------------------------------------
    for grading, sku_list in sku_groups.items():
        if not sku_list:
            continue
            
        last_1 = {}
        last_0 = {}
        
        for time_key, mongo_field in TIME_FIELDS.items():
            # Process last_1
            query_last_1 = {
                mongo_field: {"$gte": start_date, "$lte": end_date},
                "last": 1,
                "rerun": {"$in": [0, 1]},
                "sku": {"$in": sku_list}
            }
            
            summary_last_1 = fetch_summary_records_batch(query_last_1, sku_list)
            last_1[time_key] = process_summary_records_fixed(
                summary_last_1,
                scrap_by_sku,
                grading
            )
            
            # Process last_0
            query_last_0 = {
                mongo_field: {"$gte": start_date, "$lte": end_date},
                "rerun": 0,
                "last": {"$in": [0, 1]},
                "sku": {"$in": sku_list}
            }
            
            summary_last_0 = fetch_summary_records_batch(query_last_0, sku_list)
            last_0[time_key] = process_summary_records_fixed(
                summary_last_0,
                scrap_by_sku,
                grading
            )
        
        # ---------------- SCRAP TIME ----------------
        scrap_time_barcodes = list(scrap_map.keys())
        
        if scrap_time_barcodes:
            scrap_time_batch = 20
            for batch_start in range(0, len(scrap_time_barcodes), scrap_time_batch):
                batch_barcodes = scrap_time_barcodes[batch_start:batch_start + scrap_time_batch]
                
                st_query_last_1 = {
                    "barcode": {"$in": batch_barcodes},
                    "last": 1,
                    "rerun": {"$in": [0, 1]},
                    "sku": {"$in": sku_list}
                }
                
                st_summary_last_1 = list(
                    config_h.data_merged_summary_historical_data.find(
                        st_query_last_1,
                        {
                            "sku": 1,
                            "overallufmgrade": 1,
                            "vi_yield": 1,
                            "barcode": 1,
                            "last": 1,
                            "rerun": 1
                        }
                    ).batch_size(min(len(batch_barcodes), 50))
                )
                
                if batch_start == 0:
                    last_1[SCRAP_TIME_KEY] = process_scrap_time_records_fixed(
                        st_summary_last_1,
                        scrap_map,
                        grading
                    )
                else:
                    batch_result = process_scrap_time_records_fixed(
                        st_summary_last_1,
                        scrap_map,
                        grading
                    )
                    for sku, yields in batch_result.items():
                        if sku not in last_1[SCRAP_TIME_KEY]:
                            last_1[SCRAP_TIME_KEY][sku] = yields
                
                st_query_last_0 = {
                    "barcode": {"$in": batch_barcodes},
                    "rerun": 0,
                    "last": {"$in": [0, 1]},
                    "sku": {"$in": sku_list}
                }
                
                st_summary_last_0 = list(
                    config_h.data_merged_summary_historical_data.find(
                        st_query_last_0,
                        {
                            "sku": 1,
                            "overallufmgrade": 1,
                            "vi_yield": 1,
                            "barcode": 1,
                            "last": 1,
                            "rerun": 1
                        }
                    ).batch_size(min(len(batch_barcodes), 50))
                )
                
                if batch_start == 0:
                    last_0[SCRAP_TIME_KEY] = process_scrap_time_records_fixed(
                        st_summary_last_0,
                        scrap_map,
                        grading
                    )
                else:
                    batch_result = process_scrap_time_records_fixed(
                        st_summary_last_0,
                        scrap_map,
                        grading
                    )
                    for sku, yields in batch_result.items():
                        if sku not in last_0[SCRAP_TIME_KEY]:
                            last_0[SCRAP_TIME_KEY][sku] = yields
        
        last_1["date_range_start"] = start_date
        last_1["date_range_end"] = end_date
        last_0["date_range_start"] = start_date
        last_0["date_range_end"] = end_date
        
        doc[grading]["last_1"] = last_1
        doc[grading]["last_0"] = last_0
    
    # --------------------------------------------------------
    # SAVE
    # --------------------------------------------------------
    agg_collection_name.replace_one(
        doc_filter,
        doc,
        upsert=True
    )
    
    return 1


# ============================================================
# DAILY AND MONTHLY AGGREGATION FUNCTIONS
# ============================================================

def day_range_str(date_str: str) -> tuple[str, str]:
    """Return (start, end) strings for a 6am-to-6am production day."""
    d = datetime.strptime(date_str[:10], "%Y-%m-%d")
    return (
        d.strftime("%Y-%m-%d") + " 06:00:00",
        (d + timedelta(days=1)).strftime("%Y-%m-%d") + " 05:59:59",
    )


def aggregate_daily_yields_fixed(production_date=None):
    """Fixed daily aggregation with proper scrap yield"""
    if production_date is None:
        raise ValueError("production_date is required")
    
    date_start, date_end = day_range_str(production_date)
    
    return aggregate_yields_fixed(
        start_date=date_start,
        end_date=date_end,
        doc_filter={"production_date": production_date},
        agg_collection_name=config_h.oe_yield_daily,
        doc_key_name="production_date",
        doc_key_value=production_date
    )


def aggregate_monthly_yields_fixed(current_date=None):
    """Fixed monthly aggregation with proper scrap yield"""
    
    if current_date is None:
        raise ValueError("current_date is required")
    
    if isinstance(current_date, str):
        current_date = datetime.strptime(current_date, "%Y-%m-%d").date()
    
    if current_date.day == 1:
        production_month_date = current_date - timedelta(days=1)
    else:
        production_month_date = current_date
    
    year_month = production_month_date.strftime("%Y-%m")
    
    month_start = production_month_date.replace(day=1)
    
    date_start = month_start.strftime("%Y-%m-%d") + " 06:00:00"
    date_end = current_date.strftime("%Y-%m-%d") + " 05:59:59"
    
    return aggregate_yields_fixed(
        start_date=date_start,
        end_date=date_end,
        doc_filter={"month": year_month},
        agg_collection_name=config_h.oe_yield_monthly,
        doc_key_name="month",
        doc_key_value=year_month
    )


# ============================================================
# RUN FUNCTION
# ============================================================

def run():
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import os

    today = datetime.now(tz=timezone.utc).date()

    # Process last 3 days
    dates_to_process = [
        (today - timedelta(days=offset)).strftime("%Y-%m-%d")
        for offset in range(4, 0, -1)
    ]

    tyre_gradings = get_tyre_gradings()
    if not tyre_gradings:
        logger.error("No tyre_grading values found in production_db. Exiting.")
        return

    active_skus_list = list(config_h.model_no_status.distinct("sku", {"status": "active"}))

    grading_meta = {}
    for grading in tyre_gradings:
        skus_for_grading = get_skus_for_grading(grading)
        if not skus_for_grading:
            logger.warning("No SKUs for grading=%s — skipping", grading)
            continue
        qr_statuses = get_qr_statuses_for_grading(grading)
        skus_per_qr = {}
        for qr in qr_statuses:
            skus_for_qr = get_skus_for_grading_and_qr(grading, qr)
            if skus_for_qr:
                skus_per_qr[qr] = skus_for_qr
        grading_meta[grading] = {
            "skus":       skus_for_grading,
            "qr_statuses": qr_statuses,
            "skus_per_qr": skus_per_qr
        }

    if not grading_meta:
        logger.error("No valid gradings with SKUs found. Exiting.")
        return

    def make_tasks(production_date):
        tasks = []
        for grading, meta in grading_meta.items():
            skus_for_grading = meta["skus"]
            skus_per_qr      = meta["skus_per_qr"]
            for last_val in LAST_VALUES:
                tasks.append((production_date, grading, last_val, skus_for_grading,
                            production_date, None, None))
                for qr, skus_for_qr in skus_per_qr.items():
                    tasks.append((production_date, grading, last_val, skus_for_qr,
                                production_date, qr, None))
                    for sku in skus_for_qr:
                        tasks.append((production_date, grading, last_val, [sku],
                                    production_date, qr, sku))
        return tasks

    def execute_task(args):
        production_date, grading, last_val, skus, today_str, qr_status, sku_filter = args
        return build_combination_value(
            production_date, grading, last_val, skus,
            today_str=today_str,
            qr_status=qr_status,
            sku_filter=sku_filter,
            active_skus=active_skus_list
        )

    cpu_count = os.cpu_count() or 4
    max_workers = min(cpu_count * 3, 50)
    logger.info("Using %d worker threads for parallel processing", max_workers)

    for production_date in dates_to_process:
        logger.info("=== Processing production_date: %s ===", production_date)
        try:
            tasks = make_tasks(production_date)
            logger.info("Total combinations to process for %s: %d", production_date, len(tasks))

            combo_map = defaultdict(list)
            start_time = time.time()

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_task = {executor.submit(execute_task, t): t for t in tasks}
                completed = 0
                for future in as_completed(future_to_task):
                    try:
                        val, combo = future.result()
                        combo_map[tuple(combo)].append(val)
                        completed += 1
                        if completed % 50 == 0:
                            elapsed = time.time() - start_time
                            rate = completed / elapsed if elapsed > 0 else 0
                            remaining = len(tasks) - completed
                            eta = remaining / rate if rate > 0 else 0
                            logger.info("  [%s] %d/%d combinations done (%.1f/sec, ETA: %.1fm)",
                                        production_date, completed, len(tasks), rate, eta / 60)
                    except Exception as e:
                        task = future_to_task[future]
                        logger.exception("Combination failed for task %s: %s", task, e)

            elapsed_total = time.time() - start_time
            logger.info("Completed all combinations for %s in %.1f minutes", 
                        production_date, elapsed_total / 60)

            if not combo_map:
                logger.warning("No combinations built for date=%s. Nothing stored.", production_date)
                continue

            filters_data = [
                {"combination": list(combo_keys), "values": values}
                for combo_keys, values in combo_map.items()
            ]

            total = sum(len(v["values"]) for v in filters_data)

            doc = {
                "uf_date":      production_date,
                "created_at":   datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "filters_data": filters_data
            }

            result = config_h.home_data.replace_one({"uf_date": production_date}, doc, upsert=True)
            logger.info("Stored %d total values across %d combination types for date=%s. Matched: %d, Modified: %d, Upserted: %d",
                        total, len(filters_data), production_date, 
                        result.matched_count, result.modified_count, result.upserted_id is not None)

            saved_doc = config_h.home_data.find_one({"uf_date": production_date})
            if saved_doc:
                logger.info(" Successfully verified data saved for date=%s", production_date)
            else:
                logger.error(" Failed to verify data saved for date=%s", production_date)

            try:
                aggregate_daily_yields_fixed(production_date=production_date)
                logger.info("=== Daily Aggregation END for date=%s ===", production_date)
            except Exception as e:
                logger.exception("Daily aggregation failed for date=%s: %s", production_date, e)

        except Exception as e:
            logger.exception("Processing failed for date=%s — skipping to next date: %s", production_date, e)

    # Monthly aggregation with fixed scrap yield
    try:
        aggregate_monthly_yields_fixed(current_date=today)
        logger.info("=== Monthly Aggregation END (Fixed Scrap Yield) ===")
    except Exception as e:
        logger.exception("Monthly aggregation failed: %s", e)


if __name__ == "__main__":
    run()