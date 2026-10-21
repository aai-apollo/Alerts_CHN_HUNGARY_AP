# import os
# import traceback
# from datetime import datetime
# from dateutil.parser import parse
# from io import BytesIO
# import requests
# import json
# import pandas as pd
# import smtplib
# from bs4 import BeautifulSoup
# from email.mime.application import MIMEApplication
# from email.mime.multipart import MIMEMultipart
# from email.mime.text import MIMEText
# from flask import Flask, current_app
# from pymongo import MongoClient
# from fpdf import FPDF
# import base64
# import mail_config
# from config_hungary_repl import (
#     alert_triggering_ubno_collect_name,
#     alert_triggering_collect_name,
#     alert_triggering_curing_collect_name,
#     alert_db_virgin_yield,
#     alert_mail_db,
#     alert_users,
#     config_databse,
#     channel_no_collect,
#     MONGO_URI,
#     SENDER_EMAIL,
#     SENDER_PASSWORD,
#     TEAMS_WEBHOOK_URL,
#     TUB_CIMCORP_VISUAL_MERGED_DB_alert,
#     DATA_MERGED_SUMMARY_COLLECTION_alert,
# )

# # --- Configuration ---
# MONGO_URI = MONGO_URI
# SENDER_EMAIL = SENDER_EMAIL
# SENDER_PASSWORD = SENDER_PASSWORD
# ALERT_DB_NAME = alert_db_virgin_yield
# CONFIG_DB_NAME = config_databse  # NEW: Database for OE Replacement lookup
# CHANNEL_NO_COLLECT = channel_no_collect  # NEW: Collection for OE Replacement
# BASE_PDF_DIR = "generated_pdfs"
# MAX_EMAIL_SIZE = 20 * 1024 * 1024  # 20 MB

# # Full list of possible keys
# PARAM_KEYS = [
#     "rfppcc","rfppcw","rfh1cc","rfh1cw","lfppcc","lfppcw","cony","trro","brro","lrot","lrob","tbul","bbul","tdep","bdep",
#     "upper","lower","sbal""rfv_yield", "rfh1_yield", "lfv_yield", "conicity_yield", 
#     "lateral_run_out_yield", "bulge_yield", "dent_yield", "static_yield","upper_yield", "lower_yield", 
#     "overall_uniformity_yield", "overall_visual_yield", "overall_warehouse_yield", "overall_scrap_yield"
# ]

# ALERT_TYPES = [
#     {
#         "name": "UFM",
#         "coll": alert_triggering_ubno_collect_name,
#         "banner": "UFM ALERT",
#         "filename_prefix": "UFM",
#         "start_key": "machine_lot_start_ufm_time",
#         "end_key": "machine_lot_end_ufm_time",
#         "alert_time_key": "timestamp_alert_generated",
#         "use_grouped": False,
#         "shift_data_key": "shifts_data_tbm",
#         "shift_start_time_key": "machine_shift_tbm_start_time"
#     },
#     {
#         "name": "TBM",
#         "coll": alert_triggering_collect_name,
#         "banner": "TBM ALERT",
#         "filename_prefix": "TBM",
#         "start_key": "machine_lot_start_build_time",
#         "end_key": "machine_lot_end_build_time",
#         "alert_time_key": "timestamp_alert_generated",
#         "use_grouped": True,
#         "shift_data_key": "shifts_data",
#         "shift_start_time_key": "machine_shift_tbm_start_time"
#     },
#     {
#         "name": "CURING",
#         "coll": alert_triggering_curing_collect_name,
#         "banner": "CURING ALERT",
#         "filename_prefix": "CURING",
#         "start_key": "machine_lot_start_curing_time",
#         "end_key": "machine_lot_end_curing_time",
#         "alert_time_key": "timestamp_alert_generated",
#         "use_grouped": True,
#         "shift_data_key": "shifts_data_tbm",
#         "shift_start_time_key": "machine_shift_tbm_start_time"
#     }
# ]

# # -------------------------------------------------
# # NEW: Get OE Replacement from config database
# # -------------------------------------------------
# def get_oe_replacement(client, gt_code):
#     """
#     Looks up the OE replacement for a given gt_code in
#     config_databse.channel_no_collect.
#     Returns the value of field `oe_replacement` or "N/A".
#     """
#     try:
#         cfg_db = client[CONFIG_DB_NAME]
#         coll = cfg_db[CHANNEL_NO_COLLECT]
#         doc = coll.find_one({"gt_code": {"$in": [gt_code]}})
#         if doc:
#             return sanitize_text(doc.get("oe_replacement", "N/A"))
#         else:
#             return "N/A"
#     except Exception as e:
#         print(f"[WARN] OE replacement lookup failed for {gt_code}: {e}")
#         return "N/A"

# # Utility functions
# def sanitize_text(text):
#     """
#     Sanitizes text to ensure compatibility with PDF encoding by replacing problematic characters.
#     """
#     if text is None:
#         return "N/A"
#     text = str(text)
#     return text.encode('ascii', 'replace').decode('ascii').replace('\ufffd', '?')

# def html_table_to_markdown(html_table):
#     """
#     Converts HTML table to Markdown format for better Teams compatibility
#     """
#     soup = BeautifulSoup(html_table, 'html.parser')
#     markdown_table = ""
#     headers = [th.get_text() for th in soup.find_all('th')]
#     markdown_table += "| " + " | ".join(headers) + " |\n"
#     markdown_table += "|" + "|".join(["---"] * len(headers)) + "|\n"
#     for row in soup.find_all('tr')[1:]:
#         cells = [td.get_text() for td in row.find_all('td')]
#         markdown_table += "| " + " | ".join(cells) + " |\n"
#     return markdown_table

# # -------------------------------------------------
# # UPDATED: generate_combined_summary_table with OE Replacement
# # -------------------------------------------------
# def generate_combined_summary_table(alerts, alert_type, client=None):
#     """
#     Generates properly formatted HTML table for all alerts.
#     Now includes "OE Replacement" column.
#     """
#     if not alerts:
#         return "<table><tr><td>No alert data available</td></tr></table>"
    
#     html = "<table>"
    
#     # Header with OE Replacement column
#     if alert_type.upper() == "TBM":
#         html += """
#         <tr>
#             <th>S.No</th>
#             <th>SKU</th>
#             <th>Gt Code</th>
#             <th>Size Code</th>
#             <th>TBM</th>
#             <th>Total Tyre Produced</th>
#             <th>Overall WAREHOUSE Yield (%)</th>
#             <th>Overall UNIFORMITY Yield (%)</th>
#             <th>Overall VISUAL Yield (%)</th>
#             <th>Overall SCRAP Yield (%)</th>
#         </tr>
#         """
#     elif alert_type.upper() == "CURING":
#         html += """
#         <tr>
#             <th>S.No</th>
#             <th>SKU</th>
#             <th>Gt Code</th>
#             <th>Size Code</th>
#             <th>CURING</th>
#             <th>Total Tyre Produced</th>
#             <th>Overall WAREHOUSE Yield (%)</th>
#             <th>Overall UNIFORMITY Yield (%)</th>
#             <th>Overall VISUAL Yield (%)</th>
#             <th>Overall SCRAP Yield (%)</th>
#         </tr>
#         """
#     elif alert_type.upper() == "UFM":
#         html += """
#         <tr>
#             <th>S.No</th>
#             <th>SKU</th>
#             <th>Size Code</th>
#             <th>GT Code</th>
#             <th>UFM</th>
#             <th>Total Tyres Produced</th>
#             <th>Overall WAREHOUSE Yield (%)</th>
#             <th>Overall UNIFORMITY Yield (%)</th>
#             <th>Overall VISUAL Yield (%)</th>
#             <th>Overall SCRAP Yield (%)</th>
#             <th>Overall UFM Grade (3)</th>
#             <th>Overall UFM Grade (4)</th>
#             <th>Overall UFM Grade (5)</th>
#         </tr>
#         """
    
#     # Handle different alert types
#     if alert_type.upper() == "TBM":
#         for i, alert in enumerate(alerts):
#             for machine in alert.get("machine_alerts", []):
#                 try:
#                     warehouse = f"{float(machine.get('overall_warehouse_yield', 0)):.2f}" if machine.get("overall_warehouse_yield") not in [None, "N/A"] else "N/A"
#                     uniformity = f"{float(machine.get('overall_uniformity_yield', 0)):.2f}" if machine.get("overall_uniformity_yield") not in [None, "N/A"] else "N/A"
#                     visual = f"{float(machine.get('overall_visual_yield', 0)):.2f}" if machine.get("overall_visual_yield") not in [None, "N/A"] else "N/A"
#                     scrap = f"{float(machine.get('overall_scrap_yield', 0)):.2f}" if machine.get("overall_scrap_yield") not in [None, "N/A"] else "N/A"
#                 except (ValueError, TypeError):
#                     warehouse = uniformity = visual = scrap ="N/A"

#                 html += f"""
#                 <tr>
#                     <td>{i+1}</td>
#                     <td>{sanitize_text(alert.get('sku', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('gt_code', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('sizecode', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('machine', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('processed_machine_barcodes', 'N/A'))}</td>
#                     <td>{warehouse}</td>
#                     <td>{uniformity}</td>
#                     <td>{visual}</td>
#                     <td>{scrap}</td>
#                 </tr>
#                 """
    
#     elif alert_type.upper() == "CURING":
#         for i, alert in enumerate(alerts):
#             for machine in alert.get("machine_alerts", []):
#                 try:
#                     warehouse = f"{float(machine.get('overall_warehouse_yield', 0)):.2f}" if machine.get("overall_warehouse_yield") not in [None, "N/A"] else "N/A"
#                     uniformity = f"{float(machine.get('overall_uniformity_yield', 0)):.2f}" if machine.get("overall_uniformity_yield") not in [None, "N/A"] else "N/A"
#                     visual = f"{float(machine.get('overall_visual_yield', 0)):.2f}" if machine.get("overall_visual_yield") not in [None, "N/A"] else "N/A"
#                     scrap = f"{float(machine.get('overall_scrap_yield', 0)):.2f}" if machine.get("overall_scrap_yield") not in [None, "N/A"] else "N/A"
#                 except (ValueError, TypeError):
#                     warehouse = uniformity = visual = scrap ="N/A"

#                 html += f"""
#                 <tr>
#                     <td>{i+1}</td>
#                     <td>{sanitize_text(alert.get('sku', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('gt_code', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('sizecode', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('machine', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('processed_machine_barcodes', 'N/A'))}</td>
#                     <td>{warehouse}</td>
#                     <td>{uniformity}</td>
#                     <td>{visual}</td>
#                     <td>{scrap}</td>
#                 </tr>
#                 """
    
#     elif alert_type.upper() == "UFM":
#         for i, alert in enumerate(alerts):
#             sku_val = alert.get('sku', 'N/A')
#             for machine in alert.get("machine_alerts", []):
#                 try:
#                     # Fetch overallufmgrade counts from TUB_CIMCORP_VISUAL_MERGED_DB_alert
#                     grade3 = grade4 = grade5 = 0
#                     try:
#                         _mc2 = MongoClient(MONGO_URI)
#                         _summary_coll = _mc2[TUB_CIMCORP_VISUAL_MERGED_DB_alert][DATA_MERGED_SUMMARY_COLLECTION_alert]
#                         # Get barcodes for this machine from the alert doc
#                         machine_barcodes = []
#                         for shift in machine.get('shifts_data_tbm', []):
#                             machine_barcodes.extend(shift.get('barcodes', []))
#                         for shift in machine.get('shifts_data', []):
#                             machine_barcodes.extend(shift.get('barcodes', []))
#                         # Deduplicate
#                         machine_barcodes = list(set(machine_barcodes))
#                         if not machine_barcodes:
#                             query_filter = {"sku": sku_val, "overallufmgrade": {"$in": [3, 4, 5]}}
#                         else:
#                             query_filter = {"barcode": {"$in": machine_barcodes}, "overallufmgrade": {"$in": [3, 4, 5]}}
#                         grade3 = _summary_coll.count_documents({**query_filter, "overallufmgrade": 3})
#                         grade4 = _summary_coll.count_documents({**query_filter, "overallufmgrade": 4})
#                         grade5 = _summary_coll.count_documents({**query_filter, "overallufmgrade": 5})
#                         _mc2.close()
#                     except Exception as _ge:
#                         print(f"[WARN] overallufmgrade fetch failed: {_ge}")

#                     warehouse  = f"{float(machine.get('overall_warehouse_yield',  0)):.2f}" if machine.get("overall_warehouse_yield")  not in [None, "N/A"] else "N/A"
#                     uniformity = f"{float(machine.get('overall_uniformity_yield', 0)):.2f}" if machine.get("overall_uniformity_yield") not in [None, "N/A"] else "N/A"
#                     visual     = f"{float(machine.get('overall_visual_yield',     0)):.2f}" if machine.get("overall_visual_yield")     not in [None, "N/A"] else "N/A"
#                     scrap      = f"{float(machine.get('overall_scrap_yield',      0)):.2f}" if machine.get("overall_scrap_yield")      not in [None, "N/A"] else "N/A"
#                 except (ValueError, TypeError):
#                     grade3 = grade4 = grade5 = 0
#                     warehouse = uniformity = visual = scrap = "N/A"

#                 html += f"""
#                 <tr>
#                     <td>{i+1}</td>
#                     <td>{sanitize_text(sku_val)}</td>
#                     <td>{sanitize_text(machine.get('sizecode', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('gt_code', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('machine', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('processed_machine_barcodes', 'N/A'))}</td>
#                     <td>{warehouse}</td>
#                     <td>{uniformity}</td>
#                     <td>{visual}</td>
#                     <td>{scrap}</td>
#                     <td>{grade3}</td>
#                     <td>{grade4}</td>
#                     <td>{grade5}</td>
#                 </tr>
#                 """
    
#     # Close the table
#     html += "</table>"
#     return html

# # def send_teams_alert(summary_html, alert_type, docs):
# #     """
# #     Sends a Microsoft Teams alert message with properly formatted table.
# #     Args:
# #         summary_html (str): HTML string of the summary table.
# #         alert_type (str): Type of alert (UFM, TBM, CURING).
# #         docs (list): List of alert documents from MongoDB.
# #     """   
# #     teams_webhook_url = TEAMS_WEBHOOK_URL
# #     if not teams_webhook_url:
# #         print("[ERROR] TEAMS_WEBHOOK_URL not configured.")
# #         return False

# #     # ── Fetch yield spec thresholds from channel_no_collect (keyed by sku) ──
# #     yield_spec_lookup = {}
# #     try:
# #         _mc = MongoClient(MONGO_URI)
# #         _cfg_coll = _mc[CONFIG_DB_NAME][CHANNEL_NO_COLLECT]
# #         for doc in _cfg_coll.find(
# #             {},
# #             {"_id": 0, "sku": 1,
# #              "spec_overall_warehouse_yield": 1,
# #              "spec_overall_uniformity_yield": 1,
# #              "spec_overall_visual_yield": 1,
# #              "spec_rfppwc_yield": 1,
# #              "spec_rfh1wc_yield": 1,
# #              "spec_plys_yield": 1,
# #              "spec_lfppwc_yield": 1,
# #              "spec_cony_yield": 1,
# #              "spec_lro_yield": 1,
# #              "spec_bulge_yield": 1,
# #              "spec_dent_yield": 1,
# #              "spec_upper_yield": 1,
# #              "spec_lower_yield": 1,
# #              "spec_sbal_yield": 1}
# #         ):
# #             sku_key = str(doc.get("sku", "")).strip()
# #             if sku_key:
# #                 yield_spec_lookup[sku_key] = {
# #                     "spec_overall_warehouse_yield": doc.get("spec_overall_warehouse_yield"),
# #                     "spec_overall_uniformity_yield": doc.get("spec_overall_uniformity_yield"),
# #                     "spec_overall_visual_yield":    doc.get("spec_overall_visual_yield"),
# #                     "spec_rfv_yield":               doc.get("spec_rfppwc_yield"),
# #                     "spec_rfh1_yield":              doc.get("spec_rfh1wc_yield"),
# #                     "spec_plysteer_yield":          doc.get("spec_plys_yield"),
# #                     "spec_lfv_yield":               doc.get("spec_lfppwc_yield"),
# #                     "spec_conicity_yield":          doc.get("spec_cony_yield"),
# #                     "spec_lateral_run_out_yield":   doc.get("spec_lro_yield"),
# #                     "spec_bulge_yield":             doc.get("spec_bulge_yield"),
# #                     "spec_dent_yield":              doc.get("spec_dent_yield"),
# #                     "spec_upper_yield":             doc.get("spec_upper_yield"),
# #                     "spec_lower_yield":             doc.get("spec_lower_yield"),
# #                     "spec_static_yield":            doc.get("spec_sbal_yield"),
# #                 }
# #         _mc.close()
# #     except Exception as _e:
# #         print(f"[WARN] Could not fetch yield specs from channel_no_collect: {_e}")

# #     # ── Yield calculation functions with updated formulas ──
# #     def yield_percentage(numerator, denominator):
# #         try:
# #             if denominator is None or denominator == 0:
# #                 return 0.0
# #             return round((numerator / denominator) * 100, 2)
# #         except (TypeError, ValueError, ZeroDivisionError):
# #             return 0.0

# #     def rfv_yield(batch_data):
# #         try:
# #             if batch_data is None or batch_data.empty:
# #                 return 0.0, 0, 0
# #             denominator_data = batch_data[
# #                 ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
# #                 (batch_data['barcode'].notnull())
# #             ]
# #             denominator = len(denominator_data)
# #             if denominator == 0:
# #                 return 0.0, 0, 0
# #             numerator_data = denominator_data[
# #                 (denominator_data['rfppwcgrade'].isin([1, 2, 3]))
# #             ]
# #             numerator = len(numerator_data)
# #             return yield_percentage(numerator, denominator), numerator, denominator
# #         except Exception as e:
# #             print(f"[ERROR] rfv_yield: {e}")
# #             return 0.0, 0, 0

# #     def rfh1_yield(batch_data):
# #         try:
# #             if batch_data is None or batch_data.empty:
# #                 return 0.0, 0, 0
# #             denominator_data = batch_data[
# #                 ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
# #                 (batch_data['barcode'].notnull())
# #             ]
# #             denominator = len(denominator_data)
# #             if denominator == 0:
# #                 return 0.0, 0, 0
# #             numerator_data = denominator_data[
# #                 (denominator_data['rfh1wcgrade'].isin([1, 2, 3]))
# #             ]
# #             numerator = len(numerator_data)
# #             return yield_percentage(numerator, denominator), numerator, denominator
# #         except Exception as e:
# #             print(f"[ERROR] rfh1_yield: {e}")
# #             return 0.0, 0, 0

# #     def lfv_yield(batch_data):
# #         try:
# #             if batch_data is None or batch_data.empty:
# #                 return 0.0, 0, 0
# #             denominator_data = batch_data[
# #                 ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
# #                 (batch_data['barcode'].notnull())
# #             ]
# #             denominator = len(denominator_data)
# #             if denominator == 0:
# #                 return 0.0, 0, 0
# #             numerator_data = denominator_data[
# #                 (denominator_data['lfppwcgrade'].isin([1, 2, 3]))
# #             ]
# #             numerator = len(numerator_data)
# #             return yield_percentage(numerator, denominator), numerator, denominator
# #         except Exception as e:
# #             print(f"[ERROR] lfv_yield: {e}")
# #             return 0.0, 0, 0

# #     def conicity_yield(batch_data):
# #         try:
# #             if batch_data is None or batch_data.empty:
# #                 return 0.0, 0, 0
# #             denominator_data = batch_data[
# #                 ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
# #                 (batch_data['barcode'].notnull())
# #             ]
# #             denominator = len(denominator_data)
# #             if denominator == 0:
# #                 return 0.0, 0, 0
# #             numerator_data = denominator_data[
# #                 (denominator_data['conygrade'].isin([1, 2, 3]))
# #             ]
# #             numerator = len(numerator_data)
# #             return yield_percentage(numerator, denominator), numerator, denominator
# #         except Exception as e:
# #             print(f"[ERROR] conicity_yield: {e}")
# #             return 0.0, 0, 0

# #     def static_yield(batch_data):
# #         try:
# #             if batch_data is None or batch_data.empty:
# #                 return 0.0, 0, 0
# #             denominator_data = batch_data[
# #                 ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
# #                 (batch_data['barcode'].notnull())
# #             ]
# #             denominator = len(denominator_data)
# #             if denominator == 0:
# #                 return 0.0, 0, 0
# #             numerator_data = denominator_data[
# #                 (denominator_data['sbalgrade'].isin([1, 2, 3]))
# #             ]
# #             numerator = len(numerator_data)
# #             return yield_percentage(numerator, denominator), numerator, denominator
# #         except Exception as e:
# #             print(f"[ERROR] static_yield: {e}")
# #             return 0.0, 0, 0

# #     def upper_yield(batch_data):
# #         try:
# #             if batch_data is None or batch_data.empty:
# #                 return 0.0, 0, 0
# #             denominator_data = batch_data[
# #                 ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
# #                 (batch_data['barcode'].notnull())
# #             ]
# #             denominator = len(denominator_data)
# #             if denominator == 0:
# #                 return 0.0, 0, 0
# #             numerator_data = denominator_data[
# #                 (denominator_data['uppergrade'].isin([1, 2, 3]))
# #             ]
# #             numerator = len(numerator_data)
# #             return yield_percentage(numerator, denominator), numerator, denominator
# #         except Exception as e:
# #             print(f"[ERROR] upper_yield: {e}")
# #             return 0.0, 0, 0

# #     def lower_yield(batch_data):
# #         try:
# #             if batch_data is None or batch_data.empty:
# #                 return 0.0, 0, 0
# #             denominator_data = batch_data[
# #                 ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
# #                 (batch_data['barcode'].notnull())
# #             ]
# #             denominator = len(denominator_data)
# #             if denominator == 0:
# #                 return 0.0, 0, 0
# #             numerator_data = denominator_data[
# #                 (denominator_data['lowergrade'].isin([1, 2, 3]))
# #             ]
# #             numerator = len(numerator_data)
# #             return yield_percentage(numerator, denominator), numerator, denominator
# #         except Exception as e:
# #             print(f"[ERROR] lower_yield: {e}")
# #             return 0.0, 0, 0

# #     def overall_uniformity_yield(batch_data):
# #         try:
# #             if batch_data is None or batch_data.empty:
# #                 return 0.0, 0, 0
# #             measured_data = batch_data[
# #                 (batch_data['overallufmgrade'].isin([1, 2, 3, 4, 5])) &
# #                 (batch_data['barcode'].notnull())
# #             ]
# #             measured = len(measured_data)
# #             if measured == 0:
# #                 return 0.0, 0, 0

# #             vi_downgrade_data = batch_data[
# #                 (batch_data['vi_yield'] == 0) & (batch_data["vi_yield"].notna())
# #             ]
# #             vi_downgrade = len(vi_downgrade_data)

# #             denominator = measured - vi_downgrade
# #             if denominator <= 0:
# #                 denominator = measured

# #             numerator_data = batch_data[
# #                 (batch_data['overallufmgrade'].isin([1, 2, 3])) & (batch_data['vi_yield'] == 1)
# #             ]
# #             numerator = len(numerator_data)

# #             return yield_percentage(numerator, denominator), numerator, denominator
# #         except Exception as e:
# #             print(f"[ERROR] overall_uniformity_yield: {e}")
# #             return 0.0, 0, 0

# #     def overall_visual_yield(batch_data):
# #         try:
# #             if batch_data is None or batch_data.empty:
# #                 return 0.0, 0, 0
# #             vi_downgrade_data = batch_data[
# #                 (batch_data['vi_yield'] == 0)
# #             ]
# #             vi_downgrade = len(vi_downgrade_data)

# #             vi_scrap_data = batch_data[
# #                 batch_data['scrap_category_tub'] == "VISUAL"
# #             ]
# #             vi_scrap = len(vi_scrap_data)

# #             ufm_data = batch_data[
# #                 batch_data['overallufmgrade'].isin([1, 2, 3, 4, 5])
# #             ]
# #             ufm_count = len(ufm_data)

# #             possible_oe = (ufm_count + vi_scrap)
# #             vi_pass = possible_oe
# #             denominator = vi_pass if vi_pass > 0 else 1
# #             numerator = (ufm_count - vi_downgrade)

# #             return yield_percentage(numerator, denominator), numerator, denominator
# #         except Exception as e:
# #             print(f"[ERROR] overall_visual_yield: {e}")
# #             return 0.0, 0, 0

# #     def overall_warehouse_yield(batch_data):
# #         try:
# #             if batch_data is None or batch_data.empty:
# #                 return 0.0, 0, 0
# #             measured_data = batch_data[
# #                 (batch_data['overallufmgrade'].isin([1, 2, 3, 4, 5])) &
# #                 (batch_data['barcode'].notnull())
# #             ]
# #             measured = len(measured_data)

# #             vi_scrap_data = batch_data[
# #                 (batch_data['scrap_category_tub'] == "VISUAL") & (batch_data["scrap_category_tub"].notna())
# #             ]
# #             vi_scrap = len(vi_scrap_data)

# #             denominator = measured + vi_scrap
# #             if denominator <= 0:
# #                 denominator = measured if measured > 0 else 1

# #             numerator_data = batch_data[
# #                 (batch_data['overallufmgrade'].isin([1, 2, 3]))
# #             ]
# #             numerator = len(numerator_data)

# #             return yield_percentage(numerator, denominator), numerator, denominator
# #         except Exception as e:
# #             print(f"[ERROR] overall_warehouse_yield: {e}")
# #             return 0.0, 0, 0

# #     # ── Function to calculate yields for a specific machine ──
# #     def calculate_yields_for_machine(batch_df, total_bc):
# #         """Calculate all yields for a specific machine's batch data"""
# #         # Initialize with default values - use total_bc as denominator
# #         wh_percent, wh_num, wh_den = 0.0, 0, total_bc if total_bc > 0 else 1
# #         uni_percent, uni_num, uni_den = 0.0, 0, total_bc if total_bc > 0 else 1
# #         vis_percent, vis_num, vis_den = 0.0, 0, total_bc if total_bc > 0 else 1
# #         rfv_percent, rfv_num, rfv_den = 0.0, 0, total_bc if total_bc > 0 else 1
# #         rfh1_percent, rfh1_num, rfh1_den = 0.0, 0, total_bc if total_bc > 0 else 1
# #         lfv_percent, lfv_num, lfv_den = 0.0, 0, total_bc if total_bc > 0 else 1
# #         conicity_percent, conicity_num, conicity_den = 0.0, 0, total_bc if total_bc > 0 else 1
# #         static_percent, static_num, static_den = 0.0, 0, total_bc if total_bc > 0 else 1
# #         upper_percent, upper_num, upper_den = 0.0, 0, total_bc if total_bc > 0 else 1
# #         lower_percent, lower_num, lower_den = 0.0, 0, total_bc if total_bc > 0 else 1
        
# #         # If batch data is available, use it to calculate yields per machine
# #         if batch_df is not None and not batch_df.empty:
# #             try:
# #                 # Use the length of the dataframe as the actual count
# #                 actual_count = len(batch_df)
# #                 print(f"[DEBUG] Batch data count: {actual_count}, expected: {total_bc}")
                
# #                 # Calculate each yield using the machine-specific data
# #                 wh_percent, wh_num, wh_den = overall_warehouse_yield(batch_df)
# #                 uni_percent, uni_num, uni_den = overall_uniformity_yield(batch_df)
# #                 vis_percent, vis_num, vis_den = overall_visual_yield(batch_df)
# #                 rfv_percent, rfv_num, rfv_den = rfv_yield(batch_df)
# #                 rfh1_percent, rfh1_num, rfh1_den = rfh1_yield(batch_df)
# #                 lfv_percent, lfv_num, lfv_den = lfv_yield(batch_df)
# #                 conicity_percent, conicity_num, conicity_den = conicity_yield(batch_df)
# #                 static_percent, static_num, static_den = static_yield(batch_df)
# #                 upper_percent, upper_num, upper_den = upper_yield(batch_df)
# #                 lower_percent, lower_num, lower_den = lower_yield(batch_df)
                
# #                 print(f"[DEBUG] Calculated yields:")
# #                 print(f"  WH: {wh_percent}% ({wh_num}/{wh_den})")
# #                 print(f"  UNI: {uni_percent}% ({uni_num}/{uni_den})")
# #                 print(f"  VIS: {vis_percent}% ({vis_num}/{vis_den})")
                
# #             except Exception as e:
# #                 print(f"[ERROR] Error calculating yields from batch data: {e}")
# #                 import traceback
# #                 traceback.print_exc()
        
# #         return {
# #             'wh': (wh_percent, wh_num, wh_den),
# #             'uni': (uni_percent, uni_num, uni_den),
# #             'vis': (vis_percent, vis_num, vis_den),
# #             'rfv': (rfv_percent, rfv_num, rfv_den),
# #             'rfh1': (rfh1_percent, rfh1_num, rfh1_den),
# #             'lfv': (lfv_percent, lfv_num, lfv_den),
# #             'conicity': (conicity_percent, conicity_num, conicity_den),
# #             'static': (static_percent, static_num, static_den),
# #             'upper': (upper_percent, upper_num, upper_den),
# #             'lower': (lower_percent, lower_num, lower_den)
# #         }

# #     # ── Fetch batch data from MongoDB ──
# #     def fetch_batch_data_by_machine(sku, machine_name, barcodes_list, start_time, end_time):
# #         """Fetch batch data for a specific SKU and machine within time range using barcodes"""
# #         try:
# #             client = MongoClient(MONGO_URI)
# #             db = client["TUB_CIMCORP_VISUAL_merged_cleaned_data_alerts_data"]
            
# #             # Use the summary collection which has all the grade fields
# #             collection = db["data_merged_summary_alerts_data"]
            
# #             if barcodes_list and len(barcodes_list) > 0:
# #                 # Query by barcodes - use unique barcodes only
# #                 unique_barcodes = list(set(barcodes_list))
# #                 print(f"[DEBUG] Querying by {len(unique_barcodes)} unique barcodes (from {len(barcodes_list)} total)")
# #                 query = {
# #                     "barcode": {"$in": unique_barcodes}
# #                 }
# #             else:
# #                 # Fallback: query by sku and machine with time range
# #                 query = {
# #                     "sku": sku,
# #                     "machine": machine_name,
# #                     "barcode_time": {"$gte": start_time, "$lte": end_time}
# #                 }
# #                 print(f"[DEBUG] Querying by sku and machine: {machine_name}")
            
# #             cursor = collection.find(query)
# #             data = list(cursor)
# #             print(f"[DEBUG] Found {len(data)} records in data_merged_summary_alerts_data")
            
# #             if data and len(data) > 0:
# #                 df = pd.DataFrame(data)
# #                 print(f"[DEBUG] DataFrame shape: {df.shape}")
# #                 if 'overallufmgrade' in df.columns:
# #                     print(f"[DEBUG] overallufmgrade distribution: {df['overallufmgrade'].value_counts().to_dict()}")
# #                 return df
# #             else:
# #                 print(f"[WARN] No batch data found for sku={sku}, machine={machine_name}")
# #                 return None
                    
# #         except Exception as e:
# #             print(f"[ERROR] Error fetching batch data for machine {machine_name}: {e}")
# #             import traceback
# #             traceback.print_exc()
# #             return None
# #         finally:
# #             try:
# #                 client.close()
# #             except:
# #                 pass

# #     def _fmt(val):
# #         try:
# #             return f"{float(val):.2f}%" if val not in (None, 'N/A') else "N/A"
# #         except (TypeError, ValueError):
# #             return "N/A"

# #     def _color(actual, spec):
# #         try:
# #             if actual is None or spec is None:
# #                 return "none"
# #             return "red" if float(actual) < float(spec) else "green"
# #         except (TypeError, ValueError):
# #             return "none"

# #     def _ind(color_tag):
# #         if color_tag == "green": return "✅"
# #         if color_tag == "red":   return "🔴"
# #         return ""

# #     def _fmt_count(count):
# #         if count is None:
# #             return "N/A"
# #         try:
# #             return str(int(count))
# #         except (TypeError, ValueError):
# #             return "N/A"

# #     # Initialize list to collect one entry per (alert_doc, machine) pair
# #     alert_machine_entries = []

# #     for alert in docs:
# #         for machine in alert.get("machine_alerts", []):
# #             raw_sizecode = machine.get('sizecode', 'N/A')
# #             sizecode = raw_sizecode if raw_sizecode != 'N/A' else sanitize_text(raw_sizecode)
# #             raw_gt_code = machine.get('gt_code', 'N/A')
# #             gt_code = raw_gt_code if raw_gt_code != 'N/A' else sanitize_text(raw_gt_code)
# #             param_alerts = machine.get('parameter_alerts', [])

# #             entry = {
# #                 'sku': sanitize_text(alert.get('sku', 'N/A')),
# #                 'machine_name': sanitize_text(machine.get('machine', 'N/A')),
# #                 'sizecode': sizecode,
# #                 'gt_code': gt_code,
# #                 'params': [],
# #                 'machine_obj': machine,
# #                 'alert_obj': alert,
# #             }

# #             for param in param_alerts:
# #                 param_name = param.get('parameter')
# #                 current_val = param.get('current_value')
# #                 shift_threshold = param.get('shift_occurred_thershold', False)
# #                 shift_spec = param.get('shift_occurred_spec', False)
# #                 rank_counts = param.get('rank_counts', {})

# #                 if shift_threshold or shift_spec:
# #                     if param_name.endswith('_yield'):
# #                         display_name = param_name.replace('_yield', ' yield')
# #                     else:
# #                         display_name = param_name

# #                     if current_val == 0 or (isinstance(current_val, (int, float)) and current_val == 0):
# #                         rank_str = ", ".join([
# #                             f"{k.rsplit('_', 1)[0]}->{k.rsplit('_', 1)[1]}:{v}(count)"
# #                             if '_' in k else f"{k}->{v}"
# #                             for k, v in rank_counts.items()
# #                         ]) if rank_counts else "No ranks"
# #                         deviated_param = f"{display_name}({rank_str})"
# #                     else:
# #                         deviated_param = display_name

# #                     entry['params'].append(deviated_param)

# #             alert_machine_entries.append(entry)

# #     # Create payload title
# #     if alert_type.upper() == "TBM":
# #         title_str = "Alert: TBM Alert Summary"
# #     elif alert_type.upper() == "CURING":
# #         title_str = "Alert: CURING Alert Summary"
# #     elif alert_type.upper() == "UFM":
# #         title_str = "Alert: UFM Alert Summary"
# #     else:
# #         title_str = "Alert Summary"

# #     markdown_table = html_table_to_markdown(summary_html)

# #     # ── Machine Breakdown ──
# #     breakdown_blocks = []

# #     for entry in alert_machine_entries:
# #         sku         = entry['sku']
# #         machine_obj = entry['machine_obj']
# #         alert_obj   = entry['alert_obj']
# #         _at         = alert_type.upper()

# #         total_bc  = machine_obj.get('processed_machine_barcodes', 0)
# #         sku_specs = yield_spec_lookup.get(str(sku).strip(), {})
# #         spec_wh   = sku_specs.get('spec_overall_warehouse_yield')
# #         spec_uni  = sku_specs.get('spec_overall_uniformity_yield')
# #         spec_vis  = sku_specs.get('spec_overall_visual_yield')
# #         spec_rfv  = sku_specs.get('spec_rfv_yield')
# #         spec_rfh1 = sku_specs.get('spec_rfh1_yield')
# #         spec_lfv  = sku_specs.get('spec_lfv_yield')
# #         spec_conicity = sku_specs.get('spec_conicity_yield')
# #         spec_static   = sku_specs.get('spec_static_yield')
# #         spec_upper    = sku_specs.get('spec_upper_yield')
# #         spec_lower    = sku_specs.get('spec_lower_yield')

# #         # ── Get the start and end times for this alert ──
# #         start_time = alert_obj.get('machine_lot_start_build_time' if _at == "TBM" else 
# #                                    'machine_lot_start_curing_time' if _at == "CURING" else 
# #                                    'machine_lot_start_ufm_time')
# #         end_time = alert_obj.get('machine_lot_end_build_time' if _at == "TBM" else 
# #                                  'machine_lot_end_curing_time' if _at == "CURING" else 
# #                                  'machine_lot_end_ufm_time')

# #         # ── Collect all barcodes for this machine ──
# #         machine_barcodes = []
        
# #         # For UFM alerts, use barcodes from shifts_data (UFM shifts)
# #         if _at == "UFM":
# #             for shift in machine_obj.get('shifts_data', []):
# #                 machine_barcodes.extend(shift.get('barcodes', []))
# #             for shift in machine_obj.get('shifts_data_tbm', []):
# #                 machine_barcodes.extend(shift.get('barcodes', []))
# #         else:
# #             # For TBM and CURING alerts
# #             for shift in machine_obj.get('shifts_data', []):
# #                 machine_barcodes.extend(shift.get('barcodes', []))
# #             for shift in machine_obj.get('shifts_data_tbm', []):
# #                 machine_barcodes.extend(shift.get('barcodes', []))
        
# #         # Deduplicate - this is important to avoid counting duplicates
# #         machine_barcodes = list(set(machine_barcodes))
# #         print(f"[DEBUG] Unique barcodes collected: {len(machine_barcodes)}")

# #         # ── Rows for this individual breakdown ──
# #         entry_rows = []

# #         def _add_row(mname, mtype, bc):
# #             # Fetch batch data for this specific machine using barcodes
# #             # Use the machine_barcodes that were already collected
# #             batch_df = fetch_batch_data_by_machine(sku, mname, machine_barcodes, start_time, end_time)
            
# #             # Calculate yields for this machine
# #             yields = calculate_yields_for_machine(batch_df, bc)
            
# #             # Get yield values from the calculated yields
# #             wh_percent, wh_num, wh_den = yields['wh']
# #             uni_percent, uni_num, uni_den = yields['uni']
# #             vis_percent, vis_num, vis_den = yields['vis']
# #             rfv_percent, rfv_num, rfv_den = yields['rfv']
# #             rfh1_percent, rfh1_num, rfh1_den = yields['rfh1']
# #             lfv_percent, lfv_num, lfv_den = yields['lfv']
# #             conicity_percent, conicity_num, conicity_den = yields['conicity']
# #             static_percent, static_num, static_den = yields['static']
# #             upper_percent, upper_num, upper_den = yields['upper']
# #             lower_percent, lower_num, lower_den = yields['lower']
            
# #             entry_rows.append({
# #                 'mname': mname,
# #                 'mtype': mtype,
# #                 'bc': bc,
# #                 'wh': (wh_percent, wh_num, wh_den),
# #                 'uni': (uni_percent, uni_num, uni_den),
# #                 'vis': (vis_percent, vis_num, vis_den),
# #                 'rfv': (rfv_percent, rfv_num, rfv_den),
# #                 'rfh1': (rfh1_percent, rfh1_num, rfh1_den),
# #                 'lfv': (lfv_percent, lfv_num, lfv_den),
# #                 'conicity': (conicity_percent, conicity_num, conicity_den),
# #                 'static': (static_percent, static_num, static_den),
# #                 'upper': (upper_percent, upper_num, upper_den),
# #                 'lower': (lower_percent, lower_num, lower_den),
# #                 'wh_color': _color(wh_percent, spec_wh),
# #                 'uni_color': _color(uni_percent, spec_uni),
# #                 'vis_color': _color(vis_percent, spec_vis),
# #                 'rfv_color': _color(rfv_percent, spec_rfv),
# #                 'rfh1_color': _color(rfh1_percent, spec_rfh1),
# #                 'lfv_color': _color(lfv_percent, spec_lfv),
# #                 'conicity_color': _color(conicity_percent, spec_conicity),
# #                 'static_color': _color(static_percent, spec_static),
# #                 'upper_color': _color(upper_percent, spec_upper),
# #                 'lower_color': _color(lower_percent, spec_lower),
# #             })

# #         # Build rows based on alert type
# #         if _at == "UFM":
# #             _add_row(
# #                 sanitize_text(machine_obj.get('machine', 'N/A')), 
# #                 "UFM", 
# #                 int(total_bc) if total_bc else 0
# #             )
# #             for c in machine_obj.get('cur_ids', []):
# #                 bc = c.get('barcode', 0)
# #                 _add_row(
# #                     sanitize_text(str(c.get('cur_id', 'N/A'))), 
# #                     "Curing", 
# #                     int(bc)
# #                 )
# #             tbm_ids_raw = machine_obj.get('tbm_ids', [])
# #             tbm_machine = sanitize_text(machine_obj.get('tbm_machine', 'N/A'))
# #             if tbm_ids_raw:
# #                 for t in tbm_ids_raw:
# #                     bc = t.get('barcode', 0)
# #                     _add_row(
# #                         sanitize_text(str(t.get('tbm_id', 'N/A'))), 
# #                         "TBM", 
# #                         int(bc)
# #                     )
# #             elif tbm_machine and tbm_machine != 'N/A':
# #                 _add_row(
# #                     tbm_machine, 
# #                     "TBM", 
# #                     int(total_bc) if total_bc else 0
# #                 )

# #         elif _at == "TBM":
# #             _add_row(
# #                 sanitize_text(machine_obj.get('machine', 'N/A')), 
# #                 "TBM", 
# #                 int(total_bc) if total_bc else 0
# #             )
# #             seen_ubno = set()
# #             for ma in alert_obj.get("machine_alerts", []):
# #                 for rec in ma.get("grouped_averages", {}).get("uf_id", []):
# #                     uf = sanitize_text(rec.get('uf_id', ''))
# #                     bc = rec.get('barcode_count', 0)
# #                     if uf and uf not in seen_ubno:
# #                         seen_ubno.add(uf)
# #                         _add_row(
# #                             uf, 
# #                             "UFM", 
# #                             int(bc)
# #                         )
# #             for c in machine_obj.get('cur_ids', []):
# #                 bc = c.get('barcode', 0)
# #                 _add_row(
# #                     sanitize_text(str(c.get('cur_id', 'N/A'))), 
# #                     "Curing", 
# #                     int(bc)
# #                 )

# #         elif _at == "CURING":
# #             _add_row(
# #                 sanitize_text(machine_obj.get('machine', 'N/A')), 
# #                 "Curing", 
# #                 int(total_bc) if total_bc else 0
# #             )
# #             seen_ubno = set()
# #             for ma in alert_obj.get("machine_alerts", []):
# #                 for rec in ma.get("grouped_averages", {}).get("uf_id", []):
# #                     uf = sanitize_text(rec.get('uf_id', ''))
# #                     bc = rec.get('barcode_count', 0)
# #                     if uf and uf not in seen_ubno:
# #                         seen_ubno.add(uf)
# #                         _add_row(
# #                             uf, 
# #                             "UFM", 
# #                             int(bc)
# #                         )
# #             for p in machine_obj.get('ptb_ids', []):
# #                 bc = p.get('barcode', 0)
# #                 _add_row(
# #                     sanitize_text(str(p.get('ptb_id', 'N/A'))), 
# #                     "TBM", 
# #                     int(bc)
# #                 )

# #         if not entry_rows:
# #             continue

# #         # Build HTML table for this entry
# #         primary_machine = sanitize_text(machine_obj.get('machine', 'N/A'))

# #         html_table = f"<p><strong>SKU: {sku} | Machine: {primary_machine}</strong></p>"
# #         html_table += "<table border='1' cellpadding='5' cellspacing='0' style='border-collapse: collapse; width: 100%;'>"
# #         html_table += "<thead><tr style='background-color: #4A90E2; color: white;'>"
        
# #         # Base columns: Machine, Machine Type, Total Tyres
# #         html_table += "<th>Machine</th><th>Machine Type</th><th>Total Tyres</th>"
        
# #         # Warehouse Yield columns: Num, Den, %
# #         html_table += "<th>WH Num</th><th>WH Den</th><th>WAREHOUSE Yield %</th>"
        
# #         # Uniformity Yield columns: Num, Den, %
# #         html_table += "<th>UNI Num</th><th>UNI Den</th><th>UNIFORMITY Yield %</th>"
        
# #         # Visual Yield columns: Num, Den, %
# #         html_table += "<th>VIS Num</th><th>VIS Den</th><th>VISUAL Yield %</th>"
        
# #         # RFV Yield columns: Num, Den, %
# #         html_table += "<th>RFV Num</th><th>RFV Den</th><th>RFV Yield %</th>"
        
# #         # RFH1 Yield columns: Num, Den, %
# #         html_table += "<th>RFH1 Num</th><th>RFH1 Den</th><th>RFH1 Yield %</th>"
        
# #         # LFV Yield columns: Num, Den, %
# #         html_table += "<th>LFV Num</th><th>LFV Den</th><th>LFV Yield %</th>"
        
# #         # Conicity Yield columns: Num, Den, %
# #         html_table += "<th>CON Num</th><th>CON Den</th><th>Conicity Yield %</th>"
        
# #         # Static Yield columns: Num, Den, %
# #         html_table += "<th>STA Num</th><th>STA Den</th><th>Static Yield %</th>"
        
# #         # Upper Yield columns: Num, Den, %
# #         html_table += "<th>UPP Num</th><th>UPP Den</th><th>Upper Yield %</th>"
        
# #         # Lower Yield columns: Num, Den, %
# #         html_table += "<th>LOW Num</th><th>LOW Den</th><th>Lower Yield %</th>"
        
# #         html_table += "</tr></thead><tbody>"

# #         def _format_yield_cell(percent, num, den, spec_val):
# #             """Format yield with color indicator"""
# #             color = _color(percent, spec_val)
# #             if percent is None or percent == 0:
# #                 return f"0.00% {_ind(color)}"
# #             return f"{_fmt(percent)} {_ind(color)}"

# #         for row in entry_rows:
# #             mname = row['mname']
# #             mtype = row['mtype']
# #             bc = row['bc']
            
# #             wh_percent, wh_num, wh_den = row['wh']
# #             uni_percent, uni_num, uni_den = row['uni']
# #             vis_percent, vis_num, vis_den = row['vis']
# #             rfv_percent, rfv_num, rfv_den = row['rfv']
# #             rfh1_percent, rfh1_num, rfh1_den = row['rfh1']
# #             lfv_percent, lfv_num, lfv_den = row['lfv']
# #             conicity_percent, conicity_num, conicity_den = row['conicity']
# #             static_percent, static_num, static_den = row['static']
# #             upper_percent, upper_num, upper_den = row['upper']
# #             lower_percent, lower_num, lower_den = row['lower']

# #             html_table += "<tr>"
            
# #             # Machine and Total Tyres
# #             html_table += f"<td>{mname}</td><td>{mtype}</td><td>{bc}</td>"
            
# #             # WH: Num, Den, %
# #             html_table += f"<td>{_fmt_count(wh_num)}</td><td>{_fmt_count(wh_den)}</td>"
# #             html_table += f"<td>{_format_yield_cell(wh_percent, wh_num, wh_den, spec_wh)}</td>"
            
# #             # UNI: Num, Den, %
# #             html_table += f"<td>{_fmt_count(uni_num)}</td><td>{_fmt_count(uni_den)}</td>"
# #             html_table += f"<td>{_format_yield_cell(uni_percent, uni_num, uni_den, spec_uni)}</td>"
            
# #             # VIS: Num, Den, %
# #             html_table += f"<td>{_fmt_count(vis_num)}</td><td>{_fmt_count(vis_den)}</td>"
# #             html_table += f"<td>{_format_yield_cell(vis_percent, vis_num, vis_den, spec_vis)}</td>"
            
# #             # RFV: Num, Den, %
# #             html_table += f"<td>{_fmt_count(rfv_num)}</td><td>{_fmt_count(rfv_den)}</td>"
# #             html_table += f"<td>{_format_yield_cell(rfv_percent, rfv_num, rfv_den, spec_rfv)}</td>"
            
# #             # RFH1: Num, Den, %
# #             html_table += f"<td>{_fmt_count(rfh1_num)}</td><td>{_fmt_count(rfh1_den)}</td>"
# #             html_table += f"<td>{_format_yield_cell(rfh1_percent, rfh1_num, rfh1_den, spec_rfh1)}</td>"
            
# #             # LFV: Num, Den, %
# #             html_table += f"<td>{_fmt_count(lfv_num)}</td><td>{_fmt_count(lfv_den)}</td>"
# #             html_table += f"<td>{_format_yield_cell(lfv_percent, lfv_num, lfv_den, spec_lfv)}</td>"
            
# #             # Conicity: Num, Den, %
# #             html_table += f"<td>{_fmt_count(conicity_num)}</td><td>{_fmt_count(conicity_den)}</td>"
# #             html_table += f"<td>{_format_yield_cell(conicity_percent, conicity_num, conicity_den, spec_conicity)}</td>"
            
# #             # Static: Num, Den, %
# #             html_table += f"<td>{_fmt_count(static_num)}</td><td>{_fmt_count(static_den)}</td>"
# #             html_table += f"<td>{_format_yield_cell(static_percent, static_num, static_den, spec_static)}</td>"
            
# #             # Upper: Num, Den, %
# #             html_table += f"<td>{_fmt_count(upper_num)}</td><td>{_fmt_count(upper_den)}</td>"
# #             html_table += f"<td>{_format_yield_cell(upper_percent, upper_num, upper_den, spec_upper)}</td>"
            
# #             # Lower: Num, Den, %
# #             html_table += f"<td>{_fmt_count(lower_num)}</td><td>{_fmt_count(lower_den)}</td>"
# #             html_table += f"<td>{_format_yield_cell(lower_percent, lower_num, lower_den, spec_lower)}</td>"
            
# #             html_table += "</tr>"

# #         html_table += "</tbody></table>"
# #         breakdown_blocks.append(html_table)

# #     machine_breakdown_html = "<br><br>".join(breakdown_blocks) if breakdown_blocks else "<p>No data available.</p>"

# #     # ── Send combined payload ──
# #     payload = {
# #         "title": title_str,
# #         "text": (
# #             f"**{title_str}**\n\n"
# #             f"{markdown_table}\n\n"
# #             f"---\n\n"
# #             f"**Machine Breakdown**\n\n"
# #             f"{machine_breakdown_html}"
# #         )
# #     }

# #     try:
# #         response = requests.post(
# #             teams_webhook_url,
# #             json=payload,
# #             headers={"Content-Type": "application/json"}
# #         )
# #         if response.status_code in (200, 202):
# #             print(f"[INFO] Teams alert sent successfully for {alert_type} alerts")
# #             return True
# #         else:
# #             print(f"[ERROR] Failed to send Teams alert: {response.status_code} {response.text}")
# #             return False
# #     except Exception as e:
# #         print(f"[ERROR] Exception sending Teams alert: {e}")
# #         return False

# def send_teams_alert(summary_html, alert_type, docs):
#     teams_webhook_url = TEAMS_WEBHOOK_URL
#     if not teams_webhook_url:
#         print("[ERROR] TEAMS_WEBHOOK_URL not configured.")
#         return False

#     # Fetch yield spec thresholds from channel_no_collect (keyed by sku)
#     yield_spec_lookup = {}
#     try:
#         _mc = MongoClient(MONGO_URI)
#         _cfg_coll = _mc[CONFIG_DB_NAME][CHANNEL_NO_COLLECT]
#         for doc in _cfg_coll.find(
#             {},
#             {"_id": 0, "sku": 1,
#              "spec_overall_warehouse_yield": 1,
#              "spec_overall_uniformity_yield": 1,
#              "spec_overall_visual_yield": 1,
#              "spec_rfppwc_yield": 1,
#              "spec_rfh1wc_yield": 1,
#              "spec_plys_yield": 1,
#              "spec_lfppwc_yield": 1,
#              "spec_cony_yield": 1,
#              "spec_lro_yield": 1,
#              "spec_bulge_yield": 1,
#              "spec_dent_yield": 1,
#              "spec_upper_yield": 1,
#              "spec_lower_yield": 1,
#              "spec_sbal_yield": 1}
#         ):
#             sku_key = str(doc.get("sku", "")).strip()
#             if sku_key:
#                 yield_spec_lookup[sku_key] = {
#                     "spec_overall_warehouse_yield": doc.get("spec_overall_warehouse_yield"),
#                     "spec_overall_uniformity_yield": doc.get("spec_overall_uniformity_yield"),
#                     "spec_overall_visual_yield":    doc.get("spec_overall_visual_yield"),
#                     "spec_rfv_yield":               doc.get("spec_rfppwc_yield"),
#                     "spec_rfh1_yield":              doc.get("spec_rfh1wc_yield"),
#                     "spec_plysteer_yield":          doc.get("spec_plys_yield"),
#                     "spec_lfv_yield":               doc.get("spec_lfppwc_yield"),
#                     "spec_conicity_yield":          doc.get("spec_cony_yield"),
#                     "spec_lateral_run_out_yield":   doc.get("spec_lro_yield"),
#                     "spec_bulge_yield":             doc.get("spec_bulge_yield"),
#                     "spec_dent_yield":              doc.get("spec_dent_yield"),
#                     "spec_upper_yield":             doc.get("spec_upper_yield"),
#                     "spec_lower_yield":             doc.get("spec_lower_yield"),
#                     "spec_static_yield":            doc.get("spec_sbal_yield"),
#                 }
#         _mc.close()
#     except Exception as _e:
#         print(f"[WARN] Could not fetch yield specs from channel_no_collect: {_e}")

#     # Yield calculation functions
#     def yield_percentage(numerator, denominator):
#         try:
#             if denominator is None or denominator == 0:
#                 return 0.0
#             return round((numerator / denominator) * 100, 2)
#         except (TypeError, ValueError, ZeroDivisionError):
#             return 0.0

#     def get_yield_from_rank_counts(rank_counts, total_bc):
#         """
#         Extract numerator and denominator from rank_counts.
#         rank_counts format: {'grade_0': count, 'grade_3': count}
#         Pass grades are 1, 2, 3
#         """
#         if not rank_counts:
#             return 0, total_bc if total_bc > 0 else 1
        
#         numerator = 0
#         denominator = 0
        
#         for key, count in rank_counts.items():
#             # Extract grade number from key like 'rfppwcgrade_3' -> 3
#             try:
#                 if '_' in key:
#                     grade = int(key.split('_')[-1])
#                 else:
#                     grade = int(key)
                
#                 denominator += count
                
#                 # Grades 1, 2, 3 are PASS
#                 if grade in [1, 2, 3]:
#                     numerator += count
#             except (ValueError, IndexError):
#                 # If we can't parse the grade, skip
#                 continue
        
#         # If no denominator found, use total_bc
#         if denominator == 0:
#             denominator = total_bc if total_bc > 0 else 1
        
#         return numerator, denominator

#     def calculate_yields_from_machine_obj(machine_obj, total_bc, specific_barcodes=None):
#         """Calculate yields from machine_obj, optionally for specific barcodes"""
#         # If specific barcodes are provided, we need to filter the data
#         if specific_barcodes and len(specific_barcodes) > 0:
#             return calculate_yields_from_barcodes(specific_barcodes, total_bc, machine_obj)
#         else:
#             # Use the aggregated data from machine_obj
#             return calculate_yields_from_aggregated_data(machine_obj, total_bc)

#     def _grade_yield_counts(df, grade_col):
#         """Numerator/denominator using the canonical *_yield formula:
#         denominator = rows where (grade > 0) & (grade <= 5) & barcode notnull
#         numerator   = subset of denominator where grade in [1, 2, 3] & barcode notnull
#         """
#         try:
#             if df is None or df.empty or grade_col not in df.columns or 'barcode' not in df.columns:
#                 return 0, 0
#             grade = pd.to_numeric(df[grade_col], errors='coerce')
#             den_mask = (grade > 0) & (grade <= 5) & df['barcode'].notnull()
#             denominator = int(den_mask.sum())
#             num_mask = den_mask & grade.isin([1, 2, 3])
#             numerator = int(num_mask.sum())
#             return numerator, denominator
#         except Exception:
#             return 0, 0

#     def _overall_uniformity_counts(df):
#         """overall_uniformity_yield: den = measured - vi_downgrade, num = ufm pass & vi_yield==1"""
#         try:
#             grade = pd.to_numeric(df['overallufmgrade'], errors='coerce')
#             measured = int(((grade.isin([1, 2, 3, 4, 5])) & df['barcode'].notnull()).sum())
#             vi = pd.to_numeric(df['vi_yield'], errors='coerce') if 'vi_yield' in df.columns else pd.Series([], dtype=float)
#             vi_downgrade = int(((vi == 0) & vi.notna()).sum()) if len(vi) else 0
#             denominator = measured - vi_downgrade
#             if 'vi_yield' in df.columns:
#                 numerator = int(((grade.isin([1, 2, 3])) & (pd.to_numeric(df['vi_yield'], errors='coerce') == 1)).sum())
#             else:
#                 numerator = int(grade.isin([1, 2, 3]).sum())
#             return numerator, denominator
#         except Exception:
#             return 0, 0

#     def _overall_visual_counts(df):
#         """overall_visual_yield: den = ufm_count + vi_scrap, num = ufm_count - vi_downgrade"""
#         try:
#             grade = pd.to_numeric(df['overallufmgrade'], errors='coerce')
#             vi = pd.to_numeric(df['vi_yield'], errors='coerce') if 'vi_yield' in df.columns else pd.Series([], dtype=float)
#             vi_downgrade = int((vi == 0).sum()) if len(vi) else 0
#             if 'scrap_category_tub' in df.columns:
#                 vi_scrap = int((df['scrap_category_tub'] == "VISUAL").sum())
#             else:
#                 vi_scrap = 0
#             ufm_count = int(grade.isin([1, 2, 3, 4, 5]).sum())
#             denominator = ufm_count + vi_scrap
#             numerator = ufm_count - vi_downgrade
#             return numerator, denominator
#         except Exception:
#             return 0, 0

#     def _overall_warehouse_counts(df):
#         """overall_warehouse_yield: den = measured + vi_scrap, num = ufm grade in [1,2,3]"""
#         try:
#             grade = pd.to_numeric(df['overallufmgrade'], errors='coerce')
#             measured = int(((grade.isin([1, 2, 3, 4, 5])) & df['barcode'].notnull()).sum())
#             if 'scrap_category_tub' in df.columns:
#                 vi_scrap = int(((df['scrap_category_tub'] == "VISUAL") & df['scrap_category_tub'].notna()).sum())
#             else:
#                 vi_scrap = 0
#             denominator = measured + vi_scrap
#             numerator = int(grade.isin([1, 2, 3]).sum())
#             return numerator, denominator
#         except Exception:
#             return 0, 0

#     def calculate_yields_from_barcodes(barcodes, total_bc, machine_obj):
#         """Calculate yields for the given machine using ONLY this lot's barcodes.

#         The barcodes passed in are intersected with the lot barcodes (machine_obj['barcodes'])
#         so a machine never pulls tyres from other lots/SKUs. Every yield is computed with the
#         exact canonical formula (grade>0 & grade<=5 for denominator, grade in [1,2,3] for
#         numerator; overall_* use vi_yield / scrap_category_tub logic).
#         """
#         # Restrict to this lot's barcodes only
#         lot_barcodes = [str(b) for b in (machine_obj.get('barcodes') or []) if b]
#         machine_barcodes = [str(b) for b in (barcodes or []) if b]
#         if lot_barcodes:
#             lot_set = set(lot_barcodes)
#             scoped = [b for b in machine_barcodes if b in lot_set]
#             # if the machine-specific list didn't intersect, fall back to the lot barcodes
#             query_barcodes = scoped if scoped else lot_barcodes
#         else:
#             query_barcodes = machine_barcodes

#         if not query_barcodes:
#             return calculate_yields_from_aggregated_data(machine_obj, total_bc)

#         try:
#             _mc = MongoClient(MONGO_URI)
#             _db = _mc[TUB_CIMCORP_VISUAL_MERGED_DB_alert]
#             _coll = _db[DATA_MERGED_SUMMARY_COLLECTION_alert]

#             projection = {
#                 "_id": 0, "barcode": 1,
#                 "rfppwcgrade": 1, "rfh1wcgrade": 1, "lfppwcgrade": 1,
#                 "conygrade": 1, "lrograde": 1, "sbalgrade": 1,
#                 "uppergrade": 1, "lowergrade": 1, "bulwcgrade": 1, "depwcgrade": 1,
#                 "overallufmgrade": 1, "vi_yield": 1, "scrap_category_tub": 1,
#             }
#             docs = list(_coll.find({"barcode": {"$in": query_barcodes}}, projection))
#             _mc.close()
#         except Exception as e:
#             print(f"[ERROR] Failed to get specific barcode data: {e}")
#             return calculate_yields_from_aggregated_data(machine_obj, total_bc)

#         if not docs:
#             return calculate_yields_from_aggregated_data(machine_obj, total_bc)

#         df = pd.DataFrame(docs)

#         # Per-parameter yields (canonical grade formula)
#         rfv_num, rfv_den = _grade_yield_counts(df, 'rfppwcgrade')
#         rfh1_num, rfh1_den = _grade_yield_counts(df, 'rfh1wcgrade')
#         lfv_num, lfv_den = _grade_yield_counts(df, 'lfppwcgrade')
#         conicity_num, conicity_den = _grade_yield_counts(df, 'conygrade')
#         static_num, static_den = _grade_yield_counts(df, 'sbalgrade')
#         upper_num, upper_den = _grade_yield_counts(df, 'uppergrade')
#         lower_num, lower_den = _grade_yield_counts(df, 'lowergrade')
#         bulge_num, bulge_den = _grade_yield_counts(df, 'bulwcgrade')
#         dent_num, dent_den = _grade_yield_counts(df, 'depwcgrade')

#         # Overall yields (canonical overall_* formulas)
#         wh_num, wh_den = _overall_warehouse_counts(df)
#         uni_num, uni_den = _overall_uniformity_counts(df)
#         vis_num, vis_den = _overall_visual_counts(df)

#         return {
#             'wh': (yield_percentage(wh_num, wh_den), wh_num, wh_den),
#             'uni': (yield_percentage(uni_num, uni_den), uni_num, uni_den),
#             'vis': (yield_percentage(vis_num, vis_den), vis_num, vis_den),
#             'rfv': (yield_percentage(rfv_num, rfv_den), rfv_num, rfv_den),
#             'rfh1': (yield_percentage(rfh1_num, rfh1_den), rfh1_num, rfh1_den),
#             'lfv': (yield_percentage(lfv_num, lfv_den), lfv_num, lfv_den),
#             'conicity': (yield_percentage(conicity_num, conicity_den), conicity_num, conicity_den),
#             'static': (yield_percentage(static_num, static_den), static_num, static_den),
#             'upper': (yield_percentage(upper_num, upper_den), upper_num, upper_den),
#             'lower': (yield_percentage(lower_num, lower_den), lower_num, lower_den),
#             'bulge': (yield_percentage(bulge_num, bulge_den), bulge_num, bulge_den),
#             'dent': (yield_percentage(dent_num, dent_den), dent_num, dent_den),
#         }

#     def calculate_yields_from_aggregated_data(machine_obj, total_bc):
#         """Calculate yields from aggregated machine_obj data"""
#         # Initialize with zeros
#         wh_percent, wh_num, wh_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         uni_percent, uni_num, uni_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         vis_percent, vis_num, vis_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         rfv_percent, rfv_num, rfv_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         rfh1_percent, rfh1_num, rfh1_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         lfv_percent, lfv_num, lfv_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         conicity_percent, conicity_num, conicity_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         static_percent, static_num, static_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         upper_percent, upper_num, upper_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         lower_percent, lower_num, lower_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         bulge_percent, bulge_num, bulge_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         dent_percent, dent_num, dent_den = 0.0, 0, total_bc if total_bc > 0 else 1
        
#         # Get parameter_alerts from machine_obj
#         param_alerts = machine_obj.get('parameter_alerts', [])
        
#         # Also check lot_averages for yields
#         lot_averages = machine_obj.get('lot_averages', {})
        
#         for param in param_alerts:
#             param_name = param.get('parameter')
#             current_value = param.get('current_value')
#             rank_counts = param.get('rank_counts', {})
            
#             # If rank_counts exists, use it to calculate numerator and denominator
#             if rank_counts:
#                 num, den = get_yield_from_rank_counts(rank_counts, total_bc)
                
#                 if param_name == 'rfv_yield':
#                     rfv_num, rfv_den = num, den
#                     rfv_percent = yield_percentage(rfv_num, rfv_den)
#                 elif param_name == 'rfh1_yield':
#                     rfh1_num, rfh1_den = num, den
#                     rfh1_percent = yield_percentage(rfh1_num, rfh1_den)
#                 elif param_name == 'lfv_yield':
#                     lfv_num, lfv_den = num, den
#                     lfv_percent = yield_percentage(lfv_num, lfv_den)
#                 elif param_name == 'conicity_yield':
#                     conicity_num, conicity_den = num, den
#                     conicity_percent = yield_percentage(conicity_num, conicity_den)
#                 elif param_name == 'static_yield':
#                     static_num, static_den = num, den
#                     static_percent = yield_percentage(static_num, static_den)
#                 elif param_name == 'upper_yield':
#                     upper_num, upper_den = num, den
#                     upper_percent = yield_percentage(upper_num, upper_den)
#                 elif param_name == 'lower_yield':
#                     lower_num, lower_den = num, den
#                     lower_percent = yield_percentage(lower_num, lower_den)
#                 elif param_name == 'bulge_yield':
#                     bulge_num, bulge_den = num, den
#                     bulge_percent = yield_percentage(bulge_num, bulge_den)
#                 elif param_name == 'dent_yield':
#                     dent_num, dent_den = num, den
#                     dent_percent = yield_percentage(dent_num, dent_den)
#                 elif param_name == 'overall_warehouse_yield':
#                     wh_num, wh_den = num, den
#                     wh_percent = yield_percentage(wh_num, wh_den)
#                 elif param_name == 'overall_uniformity_yield':
#                     uni_num, uni_den = num, den
#                     uni_percent = yield_percentage(uni_num, uni_den)
#                 elif param_name == 'overall_visual_yield':
#                     vis_num, vis_den = num, den
#                     vis_percent = yield_percentage(vis_num, vis_den)
#             else:
#                 # If no rank_counts, try to use current_value from lot_averages
#                 if param_name in lot_averages:
#                     value = lot_averages.get(param_name)
#                     if value is not None:
#                         # We only have percentage, not numerator/denominator
#                         # Use total_bc as denominator and calculate numerator
#                         den = total_bc if total_bc > 0 else 1
#                         num = int(round((value / 100) * den)) if value else 0
                        
#                         if param_name == 'rfv_yield':
#                             rfv_percent = float(value) if value else 0.0
#                             rfv_num, rfv_den = num, den
#                         elif param_name == 'rfh1_yield':
#                             rfh1_percent = float(value) if value else 0.0
#                             rfh1_num, rfh1_den = num, den
#                         elif param_name == 'lfv_yield':
#                             lfv_percent = float(value) if value else 0.0
#                             lfv_num, lfv_den = num, den
#                         elif param_name == 'conicity_yield':
#                             conicity_percent = float(value) if value else 0.0
#                             conicity_num, conicity_den = num, den
#                         elif param_name == 'static_yield':
#                             static_percent = float(value) if value else 0.0
#                             static_num, static_den = num, den
#                         elif param_name == 'upper_yield':
#                             upper_percent = float(value) if value else 0.0
#                             upper_num, upper_den = num, den
#                         elif param_name == 'lower_yield':
#                             lower_percent = float(value) if value else 0.0
#                             lower_num, lower_den = num, den
#                         elif param_name == 'bulge_yield':
#                             bulge_percent = float(value) if value else 0.0
#                             bulge_num, bulge_den = num, den
#                         elif param_name == 'dent_yield':
#                             dent_percent = float(value) if value else 0.0
#                             dent_num, dent_den = num, den
#                         elif param_name == 'overall_warehouse_yield':
#                             wh_percent = float(value) if value else 0.0
#                             wh_num, wh_den = num, den
#                         elif param_name == 'overall_uniformity_yield':
#                             uni_percent = float(value) if value else 0.0
#                             uni_num, uni_den = num, den
#                         elif param_name == 'overall_visual_yield':
#                             vis_percent = float(value) if value else 0.0
#                             vis_num, vis_den = num, den
        
#         # If we still don't have values, try lot_averages directly
#         if rfv_num == 0 and rfv_den == total_bc:
#             for key, value in lot_averages.items():
#                 if key == 'rfv_yield' and value is not None:
#                     rfv_percent = float(value)
#                     rfv_num = int(round((rfv_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     rfv_den = total_bc if total_bc > 0 else 1
#                 elif key == 'rfh1_yield' and value is not None:
#                     rfh1_percent = float(value)
#                     rfh1_num = int(round((rfh1_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     rfh1_den = total_bc if total_bc > 0 else 1
#                 elif key == 'lfv_yield' and value is not None:
#                     lfv_percent = float(value)
#                     lfv_num = int(round((lfv_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     lfv_den = total_bc if total_bc > 0 else 1
#                 elif key == 'conicity_yield' and value is not None:
#                     conicity_percent = float(value)
#                     conicity_num = int(round((conicity_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     conicity_den = total_bc if total_bc > 0 else 1
#                 elif key == 'static_yield' and value is not None:
#                     static_percent = float(value)
#                     static_num = int(round((static_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     static_den = total_bc if total_bc > 0 else 1
#                 elif key == 'upper_yield' and value is not None:
#                     upper_percent = float(value)
#                     upper_num = int(round((upper_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     upper_den = total_bc if total_bc > 0 else 1
#                 elif key == 'lower_yield' and value is not None:
#                     lower_percent = float(value)
#                     lower_num = int(round((lower_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     lower_den = total_bc if total_bc > 0 else 1
#                 elif key == 'bulge_yield' and value is not None:
#                     bulge_percent = float(value)
#                     bulge_num = int(round((bulge_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     bulge_den = total_bc if total_bc > 0 else 1
#                 elif key == 'dent_yield' and value is not None:
#                     dent_percent = float(value)
#                     dent_num = int(round((dent_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     dent_den = total_bc if total_bc > 0 else 1
#                 elif key == 'overall_warehouse_yield' and value is not None:
#                     wh_percent = float(value)
#                     wh_num = int(round((wh_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     wh_den = total_bc if total_bc > 0 else 1
#                 elif key == 'overall_uniformity_yield' and value is not None:
#                     uni_percent = float(value)
#                     uni_num = int(round((uni_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     uni_den = total_bc if total_bc > 0 else 1
#                 elif key == 'overall_visual_yield' and value is not None:
#                     vis_percent = float(value)
#                     vis_num = int(round((vis_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     vis_den = total_bc if total_bc > 0 else 1
        
#         return {
#             'wh': (wh_percent, wh_num, wh_den),
#             'uni': (uni_percent, uni_num, uni_den),
#             'vis': (vis_percent, vis_num, vis_den),
#             'rfv': (rfv_percent, rfv_num, rfv_den),
#             'rfh1': (rfh1_percent, rfh1_num, rfh1_den),
#             'lfv': (lfv_percent, lfv_num, lfv_den),
#             'conicity': (conicity_percent, conicity_num, conicity_den),
#             'static': (static_percent, static_num, static_den),
#             'upper': (upper_percent, upper_num, upper_den),
#             'lower': (lower_percent, lower_num, lower_den),
#             'bulge': (bulge_percent, bulge_num, bulge_den),
#             'dent': (dent_percent, dent_num, dent_den)
#         }

#     def _fmt(val):
#         try:
#             return f"{float(val):.2f}%" if val not in (None, 'N/A') else "N/A"
#         except (TypeError, ValueError):
#             return "N/A"

#     def _color(actual, spec):
#         try:
#             if actual is None or spec is None:
#                 return "none"
#             return "red" if float(actual) < float(spec) else "green"
#         except (TypeError, ValueError):
#             return "none"

#     def _ind(color_tag):
#         if color_tag == "green": return "✅"
#         if color_tag == "red":   return "🔴"
#         return ""

#     def _fmt_count(count):
#         if count is None:
#             return "N/A"
#         try:
#             return str(int(count))
#         except (TypeError, ValueError):
#             return "N/A"

#     alert_machine_entries = []

#     for alert in docs:
#         for machine in alert.get("machine_alerts", []):
#             raw_sizecode = machine.get('sizecode', 'N/A')
#             sizecode = raw_sizecode if raw_sizecode != 'N/A' else sanitize_text(raw_sizecode)
#             raw_gt_code = machine.get('gt_code', 'N/A')
#             gt_code = raw_gt_code if raw_gt_code != 'N/A' else sanitize_text(raw_gt_code)
#             param_alerts = machine.get('parameter_alerts', [])

#             entry = {
#                 'sku': sanitize_text(alert.get('sku', 'N/A')),
#                 'machine_name': sanitize_text(machine.get('machine', 'N/A')),
#                 'sizecode': sizecode,
#                 'gt_code': gt_code,
#                 'params': [],
#                 'machine_obj': machine,
#                 'alert_obj': alert,
#             }

#             for param in param_alerts:
#                 param_name = param.get('parameter')
#                 current_val = param.get('current_value')
#                 shift_threshold = param.get('shift_occurred_thershold', False)
#                 shift_spec = param.get('shift_occurred_spec', False)
#                 rank_counts = param.get('rank_counts', {})

#                 if shift_threshold or shift_spec:
#                     if param_name.endswith('_yield'):
#                         display_name = param_name.replace('_yield', ' yield')
#                     else:
#                         display_name = param_name

#                     if current_val == 0 or (isinstance(current_val, (int, float)) and current_val == 0):
#                         rank_str = ", ".join([
#                             f"{k.rsplit('_', 1)[0]}->{k.rsplit('_', 1)[1]}:{v}(count)"
#                             if '_' in k else f"{k}->{v}"
#                             for k, v in rank_counts.items()
#                         ]) if rank_counts else "No ranks"
#                         deviated_param = f"{display_name}({rank_str})"
#                     else:
#                         deviated_param = display_name

#                     entry['params'].append(deviated_param)

#             alert_machine_entries.append(entry)

#     if alert_type.upper() == "TBM":
#         title_str = "Alert: TBM Alert Summary"
#     elif alert_type.upper() == "CURING":
#         title_str = "Alert: CURING Alert Summary"
#     elif alert_type.upper() == "UFM":
#         title_str = "Alert: UFM Alert Summary"
#     else:
#         title_str = "Alert Summary"

#     markdown_table = html_table_to_markdown(summary_html)

#     breakdown_blocks = []

#     # Helper function to get barcodes for a specific curing machine
#     def get_barcodes_for_curing_machine(machine_obj, cur_id):
#         """
#         Get the list of barcodes that went through a specific curing machine.
#         Queries data_merged_summary_alerts_data by cur_id field — gives exact per-press barcodes.
#         """
#         try:
#             sku = machine_obj.get('sku') or machine_obj.get('gt_code', '')
#             _mc = MongoClient(MONGO_URI)
#             _db = _mc[TUB_CIMCORP_VISUAL_MERGED_DB_alert]
#             _coll = _db[DATA_MERGED_SUMMARY_COLLECTION_alert]

#             lot_barcodes = [str(b) for b in (machine_obj.get('barcodes') or []) if b]
#             query = {"cur_id": cur_id}
#             # Restrict to this lot's barcodes so we never pull other lots/SKUs
#             if lot_barcodes:
#                 query["barcode"] = {"$in": lot_barcodes}

#             docs = list(_coll.find(query, {"barcode": 1, "_id": 0}))
#             _mc.close()

#             barcodes = [str(d["barcode"]) for d in docs if d.get("barcode")]
#             if barcodes:
#                 return barcodes
#         except Exception as e:
#             print(f"[WARN] get_barcodes_for_curing_machine failed for {cur_id}: {e}")

#         # Fallback: use the main barcodes list (less accurate)
#         return machine_obj.get('barcodes', [])

#     # Helper function to get barcodes for a specific TBM machine
#     def get_barcodes_for_tbm_machine(machine_obj, tbm_id):
#         """
#         Get the list of barcodes that went through a specific TBM machine.
#         Queries data_merged_summary_alerts_data by ptb_id field.
#         """
#         try:
#             _mc = MongoClient(MONGO_URI)
#             _db = _mc[TUB_CIMCORP_VISUAL_MERGED_DB_alert]
#             _coll = _db[DATA_MERGED_SUMMARY_COLLECTION_alert]

#             lot_barcodes = [str(b) for b in (machine_obj.get('barcodes') or []) if b]
#             query = {"ptb_id": tbm_id}
#             # Restrict to this lot's barcodes so we never pull other lots/SKUs
#             if lot_barcodes:
#                 query["barcode"] = {"$in": lot_barcodes}

#             docs = list(_coll.find(query, {"barcode": 1, "_id": 0}))
#             _mc.close()

#             barcodes = [str(d["barcode"]) for d in docs if d.get("barcode")]
#             if barcodes:
#                 return barcodes
#         except Exception as e:
#             print(f"[WARN] get_barcodes_for_tbm_machine failed for {tbm_id}: {e}")

#         # Fallback: use the main barcodes list
#         return machine_obj.get('barcodes', [])

#     for entry in alert_machine_entries:
#         sku         = entry['sku']
#         machine_obj = entry['machine_obj']
#         alert_obj   = entry['alert_obj']
#         _at         = alert_type.upper()

#         total_bc  = machine_obj.get('processed_machine_barcodes', 0)
#         sku_specs = yield_spec_lookup.get(str(sku).strip(), {})
#         spec_wh   = sku_specs.get('spec_overall_warehouse_yield')
#         spec_uni  = sku_specs.get('spec_overall_uniformity_yield')
#         spec_vis  = sku_specs.get('spec_overall_visual_yield')
#         spec_rfv  = sku_specs.get('spec_rfv_yield')
#         spec_rfh1 = sku_specs.get('spec_rfh1_yield')
#         spec_lfv  = sku_specs.get('spec_lfv_yield')
#         spec_conicity = sku_specs.get('spec_conicity_yield')
#         spec_static   = sku_specs.get('spec_static_yield')
#         spec_upper    = sku_specs.get('spec_upper_yield')
#         spec_lower    = sku_specs.get('spec_lower_yield')
#         spec_bulge    = sku_specs.get('spec_bulge_yield')
#         spec_dent     = sku_specs.get('spec_dent_yield')

#         entry_rows = []

#         def _add_row(mname, mtype, bc, mach_obj, specific_barcodes=None):
#             """Add a row with yields calculated from machine_obj"""
#             # Calculate yields directly from machine_obj
#             yields = calculate_yields_from_machine_obj(mach_obj, bc, specific_barcodes)
            
#             wh_percent, wh_num, wh_den = yields['wh']
#             uni_percent, uni_num, uni_den = yields['uni']
#             vis_percent, vis_num, vis_den = yields['vis']
#             rfv_percent, rfv_num, rfv_den = yields['rfv']
#             rfh1_percent, rfh1_num, rfh1_den = yields['rfh1']
#             lfv_percent, lfv_num, lfv_den = yields['lfv']
#             conicity_percent, conicity_num, conicity_den = yields['conicity']
#             static_percent, static_num, static_den = yields['static']
#             upper_percent, upper_num, upper_den = yields['upper']
#             lower_percent, lower_num, lower_den = yields['lower']
#             bulge_percent, bulge_num, bulge_den = yields['bulge']
#             dent_percent, dent_num, dent_den = yields['dent']
            
#             entry_rows.append({
#                 'mname': mname,
#                 'mtype': mtype,
#                 'bc': bc,
#                 'wh': (wh_percent, wh_num, wh_den),
#                 'uni': (uni_percent, uni_num, uni_den),
#                 'vis': (vis_percent, vis_num, vis_den),
#                 'rfv': (rfv_percent, rfv_num, rfv_den),
#                 'rfh1': (rfh1_percent, rfh1_num, rfh1_den),
#                 'lfv': (lfv_percent, lfv_num, lfv_den),
#                 'conicity': (conicity_percent, conicity_num, conicity_den),
#                 'static': (static_percent, static_num, static_den),
#                 'upper': (upper_percent, upper_num, upper_den),
#                 'lower': (lower_percent, lower_num, lower_den),
#                 'bulge': (bulge_percent, bulge_num, bulge_den),
#                 'dent': (dent_percent, dent_num, dent_den),
#                 'wh_color': _color(wh_percent, spec_wh),
#                 'uni_color': _color(uni_percent, spec_uni),
#                 'vis_color': _color(vis_percent, spec_vis),
#                 'rfv_color': _color(rfv_percent, spec_rfv),
#                 'rfh1_color': _color(rfh1_percent, spec_rfh1),
#                 'lfv_color': _color(lfv_percent, spec_lfv),
#                 'conicity_color': _color(conicity_percent, spec_conicity),
#                 'static_color': _color(static_percent, spec_static),
#                 'upper_color': _color(upper_percent, spec_upper),
#                 'lower_color': _color(lower_percent, spec_lower),
#                 'bulge_color': _color(bulge_percent, spec_bulge),
#                 'dent_color': _color(dent_percent, spec_dent),
#             })

#         # Build rows based on alert type
#         if _at == "UFM":
#             # UFM row - compute from this lot's own barcodes so num/den are real
#             # counts (consistent with the machine breakdown rows) using the
#             # canonical yield formulas.
#             _add_row(
#                 sanitize_text(machine_obj.get('machine', 'N/A')), 
#                 "UFM", 
#                 int(total_bc) if total_bc else 0,
#                 machine_obj,
#                 machine_obj.get('barcodes', [])  # this lot's barcodes only
#             )
            
#             # Curing rows - get specific barcodes for each curing machine
#             for c in machine_obj.get('cur_ids', []):
#                 bc = c.get('barcode', 0)
#                 cur_id = c.get('cur_id', 'N/A')
                
#                 # Get the barcodes that went through this curing machine
#                 cur_barcodes = get_barcodes_for_curing_machine(machine_obj, cur_id)
                
#                 # If we couldn't get specific barcodes, use the main barcodes list
#                 if not cur_barcodes:
#                     cur_barcodes = machine_obj.get('barcodes', [])
                
#                 curing_obj = machine_obj.copy()
#                 curing_obj['processed_machine_barcodes'] = bc
#                 _add_row(
#                     sanitize_text(str(cur_id)), 
#                     "Curing", 
#                     int(bc),
#                     curing_obj,
#                     cur_barcodes  # Pass specific barcodes for this curing machine
#                 )
            
#             # TBM rows - get specific barcodes for each TBM machine
#             tbm_ids_raw = machine_obj.get('tbm_ids', [])
#             tbm_machine = sanitize_text(machine_obj.get('tbm_machine', 'N/A'))
#             if tbm_ids_raw:
#                 for t in tbm_ids_raw:
#                     bc = t.get('barcode', 0)
#                     tbm_id = t.get('tbm_id', 'N/A')
                    
#                     # Get the barcodes that went through this TBM machine
#                     tbm_barcodes = get_barcodes_for_tbm_machine(machine_obj, tbm_id)
                    
#                     # If we couldn't get specific barcodes, use the main barcodes list
#                     if not tbm_barcodes:
#                         tbm_barcodes = machine_obj.get('barcodes', [])
                    
#                     tbm_obj = machine_obj.copy()
#                     tbm_obj['processed_machine_barcodes'] = bc
#                     _add_row(
#                         sanitize_text(str(tbm_id)), 
#                         "TBM", 
#                         int(bc),
#                         tbm_obj,
#                         tbm_barcodes  # Pass specific barcodes for this TBM machine
#                     )
#             elif tbm_machine and tbm_machine != 'N/A':
#                 # If no specific TBM IDs, use the main TBM machine with all barcodes
#                 tbm_barcodes = machine_obj.get('barcodes', [])
#                 tbm_obj = machine_obj.copy()
#                 tbm_obj['processed_machine_barcodes'] = total_bc
#                 _add_row(
#                     tbm_machine, 
#                     "TBM", 
#                     int(total_bc) if total_bc else 0,
#                     tbm_obj,
#                     tbm_barcodes
#                 )

#         elif _at == "TBM":
#             # TBM row - use aggregated data
#             _add_row(
#                 sanitize_text(machine_obj.get('machine', 'N/A')), 
#                 "TBM", 
#                 int(total_bc) if total_bc else 0,
#                 machine_obj,
#                 None
#             )
            
#             # UFM rows
#             seen_ubno = set()
#             for ma in alert_obj.get("machine_alerts", []):
#                 for rec in ma.get("grouped_averages", {}).get("uf_id", []):
#                     uf = sanitize_text(rec.get('uf_id', ''))
#                     bc = rec.get('barcode_count', 0)
#                     if uf and uf not in seen_ubno:
#                         seen_ubno.add(uf)
#                         # Get barcodes for this UFM machine
#                         ufm_barcodes = machine_obj.get('barcodes', [])
#                         ufm_obj = machine_obj.copy()
#                         ufm_obj['processed_machine_barcodes'] = bc
#                         _add_row(
#                             uf, 
#                             "UFM", 
#                             int(bc),
#                             ufm_obj,
#                             ufm_barcodes
#                         )
            
#             # Curing rows
#             for c in machine_obj.get('cur_ids', []):
#                 bc = c.get('barcode', 0)
#                 cur_id = c.get('cur_id', 'N/A')
#                 cur_barcodes = get_barcodes_for_curing_machine(machine_obj, cur_id)
#                 if not cur_barcodes:
#                     cur_barcodes = machine_obj.get('barcodes', [])
#                 cur_obj = machine_obj.copy()
#                 cur_obj['processed_machine_barcodes'] = bc
#                 _add_row(
#                     sanitize_text(str(cur_id)), 
#                     "Curing", 
#                     int(bc),
#                     cur_obj,
#                     cur_barcodes
#                 )

#         elif _at == "CURING":
#             # Curing row - use aggregated data
#             _add_row(
#                 sanitize_text(machine_obj.get('machine', 'N/A')), 
#                 "Curing", 
#                 int(total_bc) if total_bc else 0,
#                 machine_obj,
#                 None
#             )
            
#             # UFM rows
#             seen_ubno = set()
#             for ma in alert_obj.get("machine_alerts", []):
#                 for rec in ma.get("grouped_averages", {}).get("uf_id", []):
#                     uf = sanitize_text(rec.get('uf_id', ''))
#                     bc = rec.get('barcode_count', 0)
#                     if uf and uf not in seen_ubno:
#                         seen_ubno.add(uf)
#                         ufm_barcodes = machine_obj.get('barcodes', [])
#                         ufm_obj = machine_obj.copy()
#                         ufm_obj['processed_machine_barcodes'] = bc
#                         _add_row(
#                             uf, 
#                             "UFM", 
#                             int(bc),
#                             ufm_obj,
#                             ufm_barcodes
#                         )
            
#             # TBM rows
#             for p in machine_obj.get('ptb_ids', []):
#                 bc = p.get('barcode', 0)
#                 ptb_id = p.get('ptb_id', 'N/A')
#                 tbm_barcodes = machine_obj.get('barcodes', [])
#                 tbm_obj = machine_obj.copy()
#                 tbm_obj['processed_machine_barcodes'] = bc
#                 _add_row(
#                     sanitize_text(str(ptb_id)), 
#                     "TBM", 
#                     int(bc),
#                     tbm_obj,
#                     tbm_barcodes
#                 )

#         if not entry_rows:
#             continue

#         primary_machine = sanitize_text(machine_obj.get('machine', 'N/A'))

#         html_table = f"<p><strong>SKU: {sku} | Machine: {primary_machine}</strong></p>"
#         html_table += "<table border='1' cellpadding='5' cellspacing='0' style='border-collapse: collapse; width: 100%;'>"
#         html_table += "<thead><tr style='background-color: #4A90E2; color: white;'>"
#         html_table += "<th>Machine</th><th>Machine Type</th><th>Total Tyres</th>"
#         html_table += "<th>WH Num</th><th>WH Den</th><th>WAREHOUSE Yield %</th>"
#         html_table += "<th>UNI Num</th><th>UNI Den</th><th>UNIFORMITY Yield %</th>"
#         html_table += "<th>VIS Num</th><th>VIS Den</th><th>VISUAL Yield %</th>"
#         html_table += "<th>RFV Num</th><th>RFV Den</th><th>RFV Yield %</th>"
#         html_table += "<th>RFH1 Num</th><th>RFH1 Den</th><th>RFH1 Yield %</th>"
#         html_table += "<th>LFV Num</th><th>LFV Den</th><th>LFV Yield %</th>"
#         html_table += "<th>CON Num</th><th>CON Den</th><th>Conicity Yield %</th>"
#         html_table += "<th>STA Num</th><th>STA Den</th><th>Static Yield %</th>"
#         html_table += "<th>UPP Num</th><th>UPP Den</th><th>Upper Yield %</th>"
#         html_table += "<th>LOW Num</th><th>LOW Den</th><th>Lower Yield %</th>"
#         html_table += "<th>BUL Num</th><th>BUL Den</th><th>Bulge Yield %</th>"
#         html_table += "<th>DEN Num</th><th>DEN Den</th><th>Dent Yield %</th>"
#         html_table += "</tr></thead><tbody>"

#         def _format_yield_cell(percent, num, den, spec_val):
#             color = _color(percent, spec_val)
#             if percent is None or percent == 0:
#                 return f"0.00% {_ind(color)}"
#             return f"{_fmt(percent)} {_ind(color)}"

#         for row in entry_rows:
#             mname = row['mname']
#             mtype = row['mtype']
#             bc = row['bc']
            
#             wh_percent, wh_num, wh_den = row['wh']
#             uni_percent, uni_num, uni_den = row['uni']
#             vis_percent, vis_num, vis_den = row['vis']
#             rfv_percent, rfv_num, rfv_den = row['rfv']
#             rfh1_percent, rfh1_num, rfh1_den = row['rfh1']
#             lfv_percent, lfv_num, lfv_den = row['lfv']
#             conicity_percent, conicity_num, conicity_den = row['conicity']
#             static_percent, static_num, static_den = row['static']
#             upper_percent, upper_num, upper_den = row['upper']
#             lower_percent, lower_num, lower_den = row['lower']
#             bulge_percent, bulge_num, bulge_den = row['bulge']
#             dent_percent, dent_num, dent_den = row['dent']

#             html_table += "<tr>"
#             html_table += f"<td>{mname}</td><td>{mtype}</td><td>{bc}</td>"
            
#             html_table += f"<td>{_fmt_count(wh_num)}</td><td>{_fmt_count(wh_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(wh_percent, wh_num, wh_den, spec_wh)}</td>"
            
#             html_table += f"<td>{_fmt_count(uni_num)}</td><td>{_fmt_count(uni_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(uni_percent, uni_num, uni_den, spec_uni)}</td>"
            
#             html_table += f"<td>{_fmt_count(vis_num)}</td><td>{_fmt_count(vis_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(vis_percent, vis_num, vis_den, spec_vis)}</td>"
            
#             html_table += f"<td>{_fmt_count(rfv_num)}</td><td>{_fmt_count(rfv_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(rfv_percent, rfv_num, rfv_den, spec_rfv)}</td>"
            
#             html_table += f"<td>{_fmt_count(rfh1_num)}</td><td>{_fmt_count(rfh1_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(rfh1_percent, rfh1_num, rfh1_den, spec_rfh1)}</td>"
            
#             html_table += f"<td>{_fmt_count(lfv_num)}</td><td>{_fmt_count(lfv_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(lfv_percent, lfv_num, lfv_den, spec_lfv)}</td>"
            
#             html_table += f"<td>{_fmt_count(conicity_num)}</td><td>{_fmt_count(conicity_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(conicity_percent, conicity_num, conicity_den, spec_conicity)}</td>"
            
#             html_table += f"<td>{_fmt_count(static_num)}</td><td>{_fmt_count(static_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(static_percent, static_num, static_den, spec_static)}</td>"
            
#             html_table += f"<td>{_fmt_count(upper_num)}</td><td>{_fmt_count(upper_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(upper_percent, upper_num, upper_den, spec_upper)}</td>"
            
#             html_table += f"<td>{_fmt_count(lower_num)}</td><td>{_fmt_count(lower_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(lower_percent, lower_num, lower_den, spec_lower)}</td>"
            
#             html_table += f"<td>{_fmt_count(bulge_num)}</td><td>{_fmt_count(bulge_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(bulge_percent, bulge_num, bulge_den, spec_bulge)}</td>"
            
#             html_table += f"<td>{_fmt_count(dent_num)}</td><td>{_fmt_count(dent_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(dent_percent, dent_num, dent_den, spec_dent)}</td>"
            
#             html_table += "</tr>"

#         html_table += "</tbody></table>"
#         breakdown_blocks.append(html_table)

#     machine_breakdown_html = "<br><br>".join(breakdown_blocks) if breakdown_blocks else "<p>No data available.</p>"

#     payload = {
#         "title": title_str,
#         "text": (
#             f"**{title_str}**\n\n"
#             f"{markdown_table}\n\n"
#             f"---\n\n"
#             f"**Machine Breakdown**\n\n"
#             f"{machine_breakdown_html}"
#         )
#     }

#     try:
#         response = requests.post(
#             teams_webhook_url,
#             json=payload,
#             headers={"Content-Type": "application/json"}
#         )
#         if response.status_code in (200, 202):
#             print(f"[INFO] Teams alert sent successfully for {alert_type} alerts")
#             return True
#         else:
#             print(f"[ERROR] Failed to send Teams alert: {response.status_code} {response.text}")
#             return False
#     except Exception as e:
#         print(f"[ERROR] Exception sending Teams alert: {e}")
#         return False

# def format_value(v, decimals=2):
#     if v is None or pd.isna(v):
#         return "N/A"
#     try:
#         return f"{float(v):.{decimals}f}"
#     except:
#         return "N/A"

# def format_datetime(dt, fmt="%d-%m-%Y %I:%M:%S%p"):
#     if not dt or dt == "":
#         return None

#     if isinstance(dt, dict) and "$date" in dt:
#         dt = dt["$date"]

#     try:
#         dt_parsed = pd.to_datetime(dt, errors='coerce')
#         if pd.isnull(dt_parsed):
#             return None
#         return dt_parsed.strftime(fmt)
#     except Exception:
#         return None
    
# def format_shift_date(dt):
#     """Format datetime to YYYY-Month-DD format for shift data."""
#     if not dt or dt == "":
#         return None
#     if isinstance(dt, dict) and "$date" in dt:
#         dt = dt["$date"]
#     try:
#         dt_parsed = pd.to_datetime(dt, errors='coerce')
#         if pd.isnull(dt_parsed):
#             return None
#         return dt_parsed.strftime("%Y-%B-%d")
#     except Exception:
#         return None
                     
# # PDF class
# class PDF(FPDF):
#     def __init__(self):
#         super().__init__('L','mm',(300,250))
#         self.set_auto_page_break(True,15)
#         self.set_margins(10,10,10)

#     def footer(self):
#         self.set_y(-15)
#         self.set_font('Arial','I',8)
#         self.cell(0,10,f'Page {self.page_no()}',0,0,'C')

#     def section_banner(self, text):
#         self.set_fill_color(238,174,193)
#         self.set_text_color(0)
#         self.set_font('Arial','B',10)
#         self.cell(0,8,sanitize_text(text),0,1,'C',fill=True)
#         self.ln(4)

#     def grouped_parameter_table(self, headers, widths, group_defs, data, cols):
#         # Header with Group column
#         self.set_fill_color(106, 13, 173)
#         self.set_text_color(255)
#         self.set_font('Arial', 'B', 9)
        
#         # Header row
#         def print_header():
#             # Header row (keep all borders)
#             self.cell(widths[0], 7, "Stage", 1, 0, 'C', fill=True)
#             for w, h in zip(widths[1:], headers):
#                 self.cell(w, 7, sanitize_text(h), 1, 0, 'C', fill=True)
#             self.ln()

#         print_header()
#         self.set_text_color(0)
                 

#         row_h = 6
#         for group_label, keys in group_defs:
#             total_rows = len(keys)
#             total_height = row_h * total_rows

#             # Check if the group fits on the current page
#             if self.get_y() + total_height + 10 > self.h - self.b_margin:
#                 self.add_page()
#                 print_header()
#                 self.set_text_color(0)
            
#             # Calculate vertical position
#             y_start = self.get_y()
            
#             # Draw rows with special border handling
#             for i, key in enumerate(keys):
#                 # Stage cell borders:
#                 # - Always left border (L)
#                 # - Top border for first row (T)
#                 # - Bottom border for last row (B)
#                 border = 'L'  # Always keep left border
#                 if i == 0:
#                     border += 'T'  # Add top border for first row
#                 if i == len(keys) - 1:
#                     border += 'B'  # Add bottom border for last row
                
#                 self.cell(widths[0], row_h, "", border)
                
#                 # Parameter cell - keep all borders
#                 param_name = sanitize_text(key.replace('_', ' ').upper())
#                 self.cell(widths[1], row_h, param_name, 1)
                
#                 # Data cells - keep all borders
#                 for col_name, w in zip(cols, widths[2:]):
#                     val = data.get(key, {}).get(col_name)
#                     vs = format_value(val)
#                     hl = ((col_name == 'difference' and data.get(key, {}).get('shift_occurred_thershold')) or \
#                         (col_name == 'spec_diff' and data.get(key, {}).get('shift_occurred_spec')))
#                     if hl:
#                         self.set_fill_color(255, 0, 0)
#                         self.set_text_color(255)
#                         self.cell(w, row_h, vs, 1, 0, 'C', fill=True)
#                         self.set_fill_color(255)
#                         self.set_text_color(0)
#                     else:
#                         self.cell(w, row_h, vs, 1, 0, 'C')
#                 self.ln()
            
#             # Draw the merged group label with left and right borders
#             self.set_xy(self.l_margin, y_start)
#             self.set_font('Arial', 'B', 9)
#             self.cell(widths[0], total_height, sanitize_text(group_label), 'LR', 0, 'C')
            
#             # Move to next position
#             self.set_xy(self.l_margin, y_start + total_height)
#             self.ln(2)
    
#     def grouped_summary(self, machines):
#         usable = self.w - self.l_margin - self.r_margin
#         first  = usable * 0.40
#         each = (usable - first) / (2 * len(machines))
#         widths = [first] + [each, each] * len(machines)
#         h = 6

#         self.set_font('Arial','B',9)
#         self.cell(widths[0],h,'UBNO Machine',1)
#         for m in machines:
#             self.cell(widths[1] + widths[2], h, sanitize_text(m['uf_id']), 1, 0, 'C')
#         self.ln()

#         self.set_font('Arial','',9)
#         self.cell(widths[0], h, 'Start Time', 1)
#         for m in machines:
#             # self.cell(widths[1] + widths[2], h, format_datetime(m['start']), 1, 0, 'C')
#             self.cell(widths[1] + widths[2], h, str(format_datetime(m['start'])), 1, 0, 'C')
#         self.ln()

#         self.cell(widths[0],h,'End Time',1)
#         for m in machines:
#             # self.cell(widths[1] + widths[2], h, format_datetime(m['end']), 1, 0, 'C')
#             self.cell(widths[1] + widths[2], h, str(format_datetime(m['end'])), 1, 0, 'C')
#         self.ln()

#         self.cell(widths[0], h, 'Barcode Count', 1)
#         for m in machines:
#             self.cell(widths[1] + widths[2], h, str(m['barcode_count']), 1, 0, 'C')
#         self.ln(5)

#         self.set_fill_color(106,13,173)
#         self.set_text_color(255)
#         self.set_font('Arial','B',9)
#         self.cell(widths[0], h, 'Parameters', 1, 0, 'C', fill=True)
#         for _ in machines:
#             self.cell(widths[1], h, 'SPEC', 1, 0, 'C', fill=True)
#             self.cell(widths[2], h, 'AVG', 1, 0, 'C', fill=True)
#         self.ln()
#         self.set_text_color(0)

#         for key in PARAM_KEYS:
#             self.cell(widths[0], h, sanitize_text(key.replace('_',' ').upper()), 1)
#             for m in machines:
#                 avg = format_value(m['averages'].get(f'avg_{key}') if f'avg_{key}' in m['averages'] else m['averages'].get(key))
#                 spec = format_value(m['specs'].get(key))
#                 self.cell(widths[1], h, spec, 1, 0, 'C')
#                 if m['parameter_map'].get(key):
#                     self.set_fill_color(255,0,0)
#                     self.set_text_color(255)
#                     self.cell(widths[2], h, avg, 1, 0, 'C', fill=True)
#                     self.set_fill_color(255)
#                     self.set_text_color(0)
#                 else:
#                     self.cell(widths[2], h, avg, 1, 0, 'C')
#             self.ln()

#         # ── overallufmgrade 3, 4, 5 rows ─────────────────────────────────
#         for grade in [3, 4, 5]:
#             grade_key = f"overallufmgrade_{grade}"
#             label     = f"OVERALL UFM GRADE {grade}"
#             self.cell(widths[0], h, label, 1)
#             for m in machines:
#                 avg  = format_value(m['averages'].get(grade_key))
#                 spec = format_value(m['specs'].get(grade_key))
#                 self.cell(widths[1], h, spec, 1, 0, 'C')
#                 if m['parameter_map'].get(grade_key):
#                     self.set_fill_color(255,0,0)
#                     self.set_text_color(255)
#                     self.cell(widths[2], h, avg, 1, 0, 'C', fill=True)
#                     self.set_fill_color(255)
#                     self.set_text_color(0)
#                 else:
#                     self.cell(widths[2], h, avg, 1, 0, 'C')
#             self.ln()
#         self.ln(5)

#         # Barcodes section at the bottom
#         if len(machines) > 1:
#             # Side-by-side layout for multiple machines
#             barcode_width = usable / len(machines)
#             self.set_fill_color(106, 13, 173)
#             self.set_text_color(255)
#             self.set_font('Arial', 'B', 9)
#             for m in machines:
#                 self.cell(barcode_width, h, f"{sanitize_text(m['uf_id'])} Barcodes", 1, 0, 'C', fill=True)
#             self.ln()
#             self.set_text_color(0)
#             self.set_font('Arial', '', 9)
            
#             max_rows = max(len(m['barcodes']) // 2 + (1 if len(m['barcodes']) % 2 else 0) for m in machines)
#             for row in range(max_rows):
#                 x_start = self.l_margin
#                 for m in machines:
#                     self.set_x(x_start)
#                     chunk_size = 2
#                     start_idx = row * chunk_size
#                     chunk = m['barcodes'][start_idx:start_idx + chunk_size]
#                     barcode_str = ", ".join(sanitize_text(str(b)) for b in chunk if b is not None) if chunk else ""
#                     self.cell(barcode_width, h, barcode_str, 1, 0, 'L')
#                     x_start += barcode_width
#                 self.ln()
#         else:
#             # Single machine layout
#             m = machines[0]
#             self.set_fill_color(106, 13, 173)
#             self.set_text_color(255)
#             self.set_font('Arial', 'B', 9)
#             self.cell(usable, h, f"{sanitize_text(m['uf_id'])} Barcodes", 1, 0, 'C', fill=True)
#             self.ln()
#             self.set_text_color(0)
#             self.set_font('Arial', '', 9)
            
#             # Custom layout: 5 barcodes per row
#             barcodes = m['barcodes']
#             chunk_size = 5
#             barcode_width = usable / chunk_size
#             total_rows = len(barcodes) // chunk_size + (1 if len(barcodes) % chunk_size else 0)

#             for row in range(total_rows):
#                 start_idx = row * chunk_size
#                 chunk = barcodes[start_idx:start_idx + chunk_size]
#                 for b in chunk:
#                     self.cell(barcode_width, h, sanitize_text(str(b)) if b is not None else "", 1, 0, 'L')
#                 # Fill remaining cells if the row has fewer than 5 barcodes
#                 for _ in range(chunk_size - len(chunk)):
#                     self.cell(barcode_width, h, "", 1, 0, 'L')
#                 self.ln()
    
#     def barcodes_table(self, barcodes, width):
#         """
#         Generates a table displaying the barcodes in chunks.
#         """
#         self.set_fill_color(106, 13, 173)
#         self.set_text_color(255)
#         self.set_font('Arial', 'B', 9)
#         self.cell(width, 7, "Barcodes", 1, 0, 'C', fill=True)
#         self.ln()
#         self.set_text_color(0)
#         self.set_font('Arial', '', 9)

#         row_h = 6
#         if not barcodes or not isinstance(barcodes, list):
#             self.cell(width, row_h, "No barcodes available", 1, 0, 'C')
#             self.ln()
#             return

#         # Chunk barcodes into groups of 5 for readability
#         chunk_size = 5
#         max_barcodes = 100  # Limit to 100 barcodes
#         for i in range(0, min(len(barcodes), max_barcodes), chunk_size):
#             chunk = barcodes[i:i + chunk_size]
#             barcode_str = ", ".join(sanitize_text(str(b)) for b in chunk if b is not None)
#             self.cell(width, row_h, barcode_str, 1, 0, 'L')
#             self.ln()

#     def shift_data_table(self, shifts_data, machine_name, alert_type, width):
#         """
#         Generates a table displaying shift data with shift names and barcodes.
#         """
#         self.set_fill_color(238, 174, 193)
#         self.set_text_color(0)
#         self.set_font('Arial', 'B', 10)
#         self.cell(0, 8, "TBM SHIFT DATA", 0, 1, 'C', fill=True)
#         self.ln(4)

#         self.set_fill_color(106, 13, 173)
#         self.set_text_color(255)
#         self.set_font('Arial', 'B', 9)
#         self.cell(width, 7, "TBM Machine", 1, 0, 'C', fill=True)
#         self.ln()
#         self.set_text_color(0)
#         self.set_font('Arial', '', 9)
#         self.cell(width, 6, sanitize_text(machine_name), 1, 0, 'C')
#         self.ln(5)

#         if not shifts_data or not isinstance(shifts_data, list):
#             self.set_font('Arial', 'B', 9)
#             self.cell(width, 6, "No shift data available", 1, 0, 'C')
#             self.ln()
#             return
                  
#         # Prepare shift data
#         shift_info = []
#         for shift in shifts_data:
#             shift_name = shift.get('shift', 'N/A')
#             start_time = shift.get('machine_shift_tbm_start_time', None)
#             barcodes = shift.get('barcodes', [])
#             formatted_date = format_shift_date(start_time)
#             if formatted_date and shift_name != 'N/A':
#                 shift_display = f"{formatted_date}/{shift_name}"
#             else:
#                 shift_display = shift_name
#             shift_info.append({
#                 'shift_display': shift_display,
#                 'barcodes': barcodes
#             })

#         # Limit to 3 shifts for table layout
#         shift_info = shift_info[:3]
#         num_shifts = len(shift_info)
#         if num_shifts == 0:
#             self.set_font('Arial', 'B', 9)
#             self.cell(width, 6, "No valid shift data available", 1, 0, 'C')
#             self.ln()
#             return

#         # Calculate column widths
#         col_width = width / max(num_shifts, 1)
#         row_h = 6
#         # Print shift headers
#         self.set_fill_color(106, 13, 173)
#         self.set_text_color(255)
#         self.set_font('Arial', 'B', 9)
#         for shift in shift_info:
#             self.cell(col_width, 7, "Shift", 1, 0, 'C', fill=True)
#         self.ln()
#         self.set_text_color(0)
#         self.set_font('Arial', '', 9)
#         for shift in shift_info:
#             self.cell(col_width, row_h, sanitize_text(shift['shift_display']), 1, 0, 'C')
#         self.ln(5)

#         # Print barcodes
#         self.set_fill_color(106, 13, 173)
#         self.set_text_color(255)
#         self.set_font('Arial', 'B', 9)
#         for _ in shift_info:
#             self.cell(col_width, 7, "Barcodes", 1, 0, 'C', fill=True)
#         self.ln()
#         self.set_text_color(0)
#         self.set_font('Arial', '', 9)

#         # Find max number of barcodes to determine rows
#         max_barcodes = max(len(shift['barcodes']) for shift in shift_info) if shift_info else 0
#         for row in range(max_barcodes):
#             for shift in shift_info:
#                 barcode = shift['barcodes'][row] if row < len(shift['barcodes']) else ""
#                 self.cell(col_width, row_h, sanitize_text(str(barcode)), 1, 0, 'L')
#             self.ln()

# # PDF generation
# def generate_alert_pdf(doc, atype):
#     entry = doc['machine_alerts'][0]
#     alert = {
#         **entry,
#         'sku': doc.get('sku', entry.get('sku', 'N/A')),
#         'target_machine_lot_size': doc.get('target_machine_lot_size'),
#         atype['alert_time_key']: doc.get(atype['alert_time_key'])
#     }

#     raw  = alert['parameter_alerts']
#     data = {p['parameter']: p for p in raw}
#     for p in data.values():
#         c, s = p.get('current_value'), p.get('spec_value')
#         p['spec_diff'] = float(c)-float(s) if c is not None and s is not None else None

#     # filter keys present
#     all_ufm      = PARAM_KEYS[:7]
#     all_geometery = PARAM_KEYS[7:15]
#     all_bal      = PARAM_KEYS[15:18]
#     all_yields   = PARAM_KEYS[18:]
#     ufm_keys      = [k for k in all_ufm if k in data]
#     geometry_keys = [k for k in all_geometery if k in data]
#     bal_keys      = [k for k in all_bal if k in data]
#     yields_keys   = [k for k in all_yields if k in data]

#     pdf = PDF()
#     pdf.add_page()

#     # 1) Compute our twoâ€column split
#     usable = pdf.w - pdf.l_margin - pdf.r_margin
#     half   = usable / 2
#     x1     = pdf.l_margin
#     x2     = pdf.l_margin + half
#     label_1=55
#     value_1=half-label_1
#     line_h = 6

#     # 2) Draw the banner on the left half
#     pdf.set_xy(x1, pdf.get_y())
#     pdf.set_font('Arial','B',11)
#     pdf.cell(half, line_h, f"{sanitize_text(atype['name'])} ALERT", 0, 0, 'L')



#     # 3) Compute identifier string		   
#     # Extract timestamp
#     raw_ts = alert.get(atype['alert_time_key'])

#     # Handle dict-based timestamp
#     if isinstance(raw_ts, dict) and '$date' in raw_ts:
#         raw_ts = raw_ts['$date']

#     # Try parsing the timestamp safely
#     ts_str = None
#     if raw_ts:
#         try:
#             # Parse the timestamp
#             ts_parsed = pd.to_datetime(raw_ts, errors='coerce', utc=True)
#             if not pd.isna(ts_parsed):  # Use pd.isna instead of pd.isnull for consistency
#                 ts_str = ts_parsed.strftime("%Y%m%d_%H%M%S")
#             else:
#                 print(f"[DEBUG] Timestamp parsing resulted in NaT for raw_ts: {raw_ts}")
#         except Exception as e:
#             print(f"[DEBUG] Error parsing timestamp {raw_ts}: {e}")

#     # Safely fetch values with defaults
#     atype_name = sanitize_text(str(atype.get('name', 'UNKNOWN')))
#     machine = sanitize_text(str(alert.get('machine', 'UNKNOWN')))
#     gt_code = sanitize_text(str(alert.get('sku', 'UNKNOWN')))
#     identifier = f":{atype_name}_{machine}_{gt_code}_{ts_str if ts_str else 'None'}"

#     # Build identifier
#     # identifier = f":{atype_name}_{machine}_{modelno}_{ts_str if ts_str else 'None'}"

										
#     pdf.set_font('Arial','B',11)
#     pdf.set_xy(x2, pdf.get_y())
#     pdf.cell(value_1, line_h, sanitize_text(identifier), 0, 1, 'R')

#     pdf.ln(2)


#     # Details
#     label_w, h = 40, 6
#     val_w = half - label_w
#     y = pdf.get_y()


#     pdf.set_xy(x1,y)
#     pdf.set_font('Arial','B',11); pdf.cell(label_w,h,"SKU",0,0)
#     pdf.set_font('Arial','',11);  pdf.cell(val_w,h,f": {sanitize_text(alert['sku'])}",0,0)
#     pdf.set_xy(x2,y)
#     pdf.set_font('Arial','B',11); pdf.cell(label_w,h,"Lot Size",0,0)
#     pdf.set_font('Arial','',11);  pdf.cell(val_w,h,f": {sanitize_text(alert['processed_machine_barcodes'])}",0,1)

#     y = pdf.get_y()
#     pdf.set_xy(x1,y)
#     pdf.set_font('Arial','B',11); pdf.cell(label_w,h,"Machine No",0,0)
#     pdf.set_font('Arial','',11);  pdf.cell(val_w,h,f": {sanitize_text(alert['machine'])}",0,1)
#     pdf.set_xy(x2,y)
#     pdf.set_font('Arial','B',11); pdf.cell(label_w,h,"Size Code",0,0)
#     pdf.set_font('Arial','',11);  pdf.cell(val_w,h,f": {sanitize_text(alert['sizecode'])}",0,1)

#     y = pdf.get_y()
#     pdf.set_xy(x1,y)
#     pdf.set_font('Arial','B',11); pdf.cell(label_w,h,"Gt Code",0,0)
#     pdf.set_font('Arial','',11);  pdf.cell(val_w,h,f": {sanitize_text(alert['gt_code'])}",0,1)

#     y = pdf.get_y()
#     pdf.set_xy(x1,y)
#     pdf.set_font('Arial','B',11); pdf.cell(label_w,h,f"{sanitize_text(atype['name'])} Start Time",0,0)
#     pdf.set_font('Arial','',11);  pdf.cell(val_w,h,f": {format_datetime(alert[atype['start_key']])}",0,0)
#     pdf.set_xy(x2,y)
#     pdf.set_font('Arial','B',11); pdf.cell(label_w,h,f"{sanitize_text(atype['name'])} End Time",0,0)
#     pdf.set_font('Arial','',11);  pdf.cell(val_w,h,f": {format_datetime(alert[atype['end_key']])}",0,1)
#     pdf.ln(5)

#     # UFM/BAL tables
#     # UFM/BAL parameter tables
							  
#     pdf.section_banner(f"{sanitize_text(atype['name'])} STAGE")
#     usable = pdf.w - pdf.l_margin - pdf.r_margin

												
#     group_w = 25
#     param_w = usable * 0.45 - group_w
#     val_w = (usable - group_w - param_w) / 3
#     widths = [group_w, param_w, val_w, val_w, val_w]

																					   
#     ufm_keys      = [k for k in PARAM_KEYS[:10] if k in data]
#     geometry_keys = [k for k in PARAM_KEYS[10:20] if k in data]
#     bal_keys      = [k for k in PARAM_KEYS[20:23] if k in data]
#     yields_keys   = [k for k in PARAM_KEYS[23:] if k in data]
#     group_defs = [("UFM", ufm_keys), ("GEOMETRY", geometry_keys), ("BALANCING", bal_keys), ("YIELDS", yields_keys)]
																										
                  
#     # PREV / CURRENT / DIFF
#     pdf.grouped_parameter_table(
#         headers=["Parameters", "PREV", "CURRENT", "DIFF"],
#         widths=widths,
#         group_defs=group_defs,
#         data=data,
#         cols=['previous_value', 'current_value', 'difference']
#     )

#     # separate second table
#     pdf.ln(5)
#     pdf.section_banner("SPECIFICATIONS")

							
#     pdf.grouped_parameter_table(
#         headers=["Parameters","SPEC","CURRENT","SPEC DIFF"],
#         widths=widths,
#         group_defs=[("UFM", ufm_keys), ("GEOMETRY", geometry_keys), ("BALANCING", bal_keys), ("YIELDS", yields_keys)],
                                                                                   
#         data=data,
#         cols=['spec_value','current_value','spec_diff']
							  
#     )

#     if atype['name'].upper() == "UFM":
#         pdf.ln(10)
#         pdf.section_banner("BARCODES")
#         barcodes = alert.get('barcodes', [])
#         pdf.barcodes_table(barcodes, usable)

#     # UBNO summary
#     if atype['use_grouped']:
#         raw_group = entry['grouped_averages']['uf_id']
#         machines = []
#         for rec in raw_group:
#             param_map = {
#                 p['parameter']: p['spec_threshold_exceeded']
#                 for p in rec.get('parameter_list', [])
#             }
#             spec_map = {
#                 p['parameter']: p['spec_value']
#                 for p in rec.get('parameter_list', [])
#             }
                                                           
#             averages = {
#                 p['parameter']: p['avg_value']
#                 for p in rec.get('parameter_list', [])
#             }
#             machines.append({
#                 'ubno': rec['uf_id'],
#                 'uf_id': rec['uf_id'],
#                 'start': rec['tub_date_start_time'],
#                 'end':   rec['tub_date_end_time'],
#                 'barcode_count': rec.get('barcode_count', 0),
#                 'averages': averages,
#                 'parameter_map': param_map,
#                 'specs':         spec_map,
#                 'barcodes': rec.get('barcodes', []),
#             })

                  
#         pdf.section_banner(f"UFM STAGE")
#         pdf.grouped_summary(machines)
#         pdf.ln(10)
    
#     # Add shift data section
#     pdf.ln(10)
#     shift_data = alert.get(atype['shift_data_key'], [])
#     machine_name = alert.get('tbm_machine' if atype['name'].upper() in ['CURING', 'UFM'] else 'machine', 'N/A')
#     pdf.shift_data_table(shift_data, machine_name, atype['name'], usable)
								 
#     return pdf.output(dest='S')

# def get_admin_emails(client):
#     cols = client[alert_mail_db][alert_users]
#     users = cols.find(
#         {"is_alert_user": True},
#         {"_id": 0, "email": 1}
#     )
#     emails = [u["email"] for u in users if u.get("email")]
#     return emails

# def send_alerts():
#     """
#     Sends alerts by grouping PDFs by modelno across all work centers (TBM, CURING, UFM).
#     Uses Microsoft Graph API for email sending (like the first code).
#     """
#     client = MongoClient(current_app.config['MONGO_URI'])
#     db = client[ALERT_DB_NAME]
#     emails = get_admin_emails(client)
    
#     if not emails:
#         print("[ERROR] No admin emails")
#         return

#     ts = datetime.now().strftime("%Y%m%d_%H%M%S")
#     total_pdfs_processed = 0

#     # Collect all alerts across all collections
#     all_alerts = []
#     for at in ALERT_TYPES:
#         coll = db[at['coll']]
#         docs = list(coll.find({'alert_flag_repl_repl': False}))
#         if docs:
#             # Send Teams alert for each work center (unchanged)
#             summary_html = generate_combined_summary_table(docs, at['name'])
#             send_teams_alert(summary_html, at['name'], docs)
#             # Add alert type to each document for PDF generation
#             for doc in docs:
#                 doc['alert_type'] = at
#                 all_alerts.append(doc)

#     if not all_alerts:
#         print("[INFO] No new alerts.")
#         return

#     # Group alerts by modelno
#     alerts_by_modelno = {}
#     for doc in all_alerts:
#         modelno = sanitize_text(doc.get('sku', 'N/A'))
#         if modelno not in alerts_by_modelno:
#             alerts_by_modelno[modelno] = []
#         alerts_by_modelno[modelno].append(doc)

#     # Process each modelno group
#     for modelno, docs in alerts_by_modelno.items():
#         # Collect size codes for this modelno
#         size_codes = sorted(set(sanitize_text(m.get('sizecode', 'N/A')) for d in docs for m in d.get('machine_alerts', [])))
#         sizecode_str = ", ".join(size_codes) if size_codes else "N/A"

#         # Generate PDFs for all alerts in this modelno
#         pdf_list = []
#         for doc in docs:
#             at = doc['alert_type']
#             pdf_bytes = generate_alert_pdf(doc, at)
#             fn = f"{at['filename_prefix']}_{sanitize_text(doc['machine_alerts'][0]['machine'])}_{modelno}_{ts}.pdf"
#             pdf_list.append({
#                 'bytes': pdf_bytes,
#                 'filename': fn,
#                 'doc_id': doc['_id'],
#                 'collection': at['coll'],
#                 'alert_type': at['name']  # Store alert type name for email body
#             })

#         # Batch PDFs based on size
#         current_batch = []
#         current_size = 0
#         batch_number = 1
#         batched_doc_ids = []

#         for pdf in pdf_list:
#             pdf_size = len(pdf['bytes'])
#             pdf_size_encoded = pdf_size * 1.33
#             email_overhead = len(json.dumps({
#                 "message": {
#                     "subject": f"PCR GYO - Yield Alert: SKU - {modelno} | Size Code - {sizecode_str} (Batch {batch_number})",
#                     "body": {"contentType": "Text", "content": f"Attached: Reports for {', '.join(sorted(set(p['alert_type'] for p in current_batch + [pdf])))}.\n\n"},
#                     "toRecipients": [{"emailAddress": {"address": e}} for e in emails],
#                     "attachments": []
#                 },
#                 "saveToSentItems": True
#             }).encode('utf-8')) * 1.33

#             if current_size + pdf_size_encoded + email_overhead > MAX_EMAIL_SIZE or len(current_batch) >= 180:
#                 if current_batch:
#                     alert_types = sorted(set(p['alert_type'] for p in current_batch))
#                     alert_types_str = ", ".join(alert_types)
#                     endpoint = f"https://graph.microsoft.com/v1.0/users/{mail_config.shared_mailbox}/sendMail"
#                     email_data = {
#                         "message": {
#                             "subject": f"PCR GYO - Yield Alert: SKU - {modelno} | Size Code - {sizecode_str} (Batch {batch_number})",
#                             "body": {"contentType": "Text", "content": f"Attached: Reports for {alert_types_str}.\n\n"},
#                             "toRecipients": [{"emailAddress": {"address": e}} for e in emails],
#                             "attachments": [
#                                 {
#                                     '@odata.type': '#microsoft.graph.fileAttachment',
#                                     'Name': pdf_item['filename'],
#                                     'ContentBytes': base64.b64encode(pdf_item['bytes'].encode('latin-1')).decode('utf-8')
#                                 } for pdf_item in current_batch
#                             ]
#                         },
#                         "saveToSentItems": True
#                     }
#                     headers = {
#                         "Authorization": f"Bearer {mail_config.access_token}",
#                         "Content-Type": "application/json"
#                     }
#                     try:
#                         resp = requests.post(endpoint, headers=headers, json=email_data)
#                         if resp.status_code == 202:
#                             print(f"[INFO] Batch {batch_number} emailed via Microsoft Graph successfully for SKU {modelno}.")
#                             # Mark alerts sent in MongoDB for this batch
#                             for pdf_item in current_batch:
#                                 db[pdf_item['collection']].update_one(
#                                     {'_id': pdf_item['doc_id']},
#                                     {'$set': {'alert_flag_repl_repl': True, doc['alert_type']['alert_time_key']: datetime.now()}}
#                                 )
#                             batched_doc_ids.extend([pdf_item['doc_id'] for pdf_item in current_batch])
#                             total_pdfs_processed += len(current_batch)
#                         else:
#                             print(f"[ERROR] Graph API sendMail failed for batch {batch_number} (SKU {modelno}): {resp.status_code} {resp.text}")
#                     except Exception as e:
#                         print(f"[ERROR] Exception while sending batch {batch_number} (SKU {modelno}): {e}")
#                         traceback.print_exc()
#                     batch_number += 1
#                     current_batch = []
#                     current_size = 0
#                 if pdf_size_encoded + email_overhead > MAX_EMAIL_SIZE:
#                     print(f"[WARN] Single PDF ({pdf['filename']}) exceeds size limit, skipping.")
#                     continue
#             current_batch.append(pdf)
#             current_size += pdf_size_encoded
        
#         # Send remaining batch
#         if current_batch:
#             alert_types = sorted(set(p['alert_type'] for p in current_batch))
#             alert_types_str = ", ".join(alert_types)
#             endpoint = f"https://graph.microsoft.com/v1.0/users/{mail_config.shared_mailbox}/sendMail"
#             email_data = {
#                 "message": {
#                     "subject": f"PCR GYO - Yield Alert: SKU - {modelno} | Size Code - {sizecode_str} (Batch {batch_number})",
#                     "body": {"contentType": "Text", "content": f"Attached: Reports for {alert_types_str}.\n\n"},
#                     "toRecipients": [{"emailAddress": {"address": e}} for e in emails],
#                     "attachments": [
#                         {
#                             '@odata.type': '#microsoft.graph.fileAttachment',
#                             'Name': pdf_item['filename'],
#                             'ContentBytes': base64.b64encode(pdf_item['bytes'].encode('latin-1')).decode('utf-8')
#                         } for pdf_item in current_batch
#                     ]
#                 },
#                 "saveToSentItems": True
#             }
#             headers = {
#                 "Authorization": f"Bearer {mail_config.access_token}",
#                 "Content-Type": "application/json"
#             }
#             try:
#                 resp = requests.post(endpoint, headers=headers, json=email_data)
#                 if resp.status_code == 202:
#                     print(f"[INFO] Final batch {batch_number} emailed via Microsoft Graph successfully for SKU {modelno}.")
#                     for pdf_item in current_batch:
#                         db[pdf_item['collection']].update_one(
#                             {'_id': pdf_item['doc_id']},
#                             {'$set': {'alert_flag_repl_repl': True, doc['alert_type']['alert_time_key']: datetime.now()}}
#                         )
#                     batched_doc_ids.extend([pdf_item['doc_id'] for pdf_item in current_batch])
#                     total_pdfs_processed += len(current_batch)
#                 else:
#                     print(f"[ERROR] Graph API sendMail failed for final batch {batch_number} (SKU {modelno}): {resp.status_code} {resp.text}")
#             except Exception as e:
#                 print(f"[ERROR] Exception while sending final batch {batch_number} (SKU {modelno}): {e}")
#                 traceback.print_exc()
        
#         print(f"[INFO] Processed {total_pdfs_processed} PDFs for SKU {modelno} in {batch_number} batches.")
    
#     if total_pdfs_processed == 0:
#         print("[INFO] No new alerts.")

# if __name__ == "__main__":
#     app = Flask(__name__)
#     app.config.update({
#         'MONGO_URI': MONGO_URI,
#         'SENDER_EMAIL': SENDER_EMAIL,
#         'SENDER_PASSWORD': SENDER_PASSWORD
#     })
#     with app.app_context():
#         send_alerts()



import os
import traceback
from datetime import datetime
from dateutil.parser import parse
from io import BytesIO
import requests
import json
import pandas as pd
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from flask import Flask, current_app
from pymongo import MongoClient
from fpdf import FPDF
import base64
import mail_config
from config_hungary_repl import (
    alert_triggering_ubno_collect_name,
    alert_triggering_collect_name,
    alert_triggering_curing_collect_name,
    alert_db_virgin_yield,
    alert_mail_db,
    alert_users,
    config_databse,
    channel_no_collect,
    MONGO_URI,
    SENDER_EMAIL,
    SENDER_PASSWORD,
    TEAMS_WEBHOOK_URL,
    TUB_CIMCORP_VISUAL_MERGED_DB_alert,
    DATA_MERGED_SUMMARY_COLLECTION_alert,
)

# --- Configuration ---
MONGO_URI = MONGO_URI
SENDER_EMAIL = SENDER_EMAIL
SENDER_PASSWORD = SENDER_PASSWORD
ALERT_DB_NAME = alert_db_virgin_yield
CONFIG_DB_NAME = config_databse
CHANNEL_NO_COLLECT = channel_no_collect
BASE_PDF_DIR = "generated_pdfs"
MAX_EMAIL_SIZE = 20 * 1024 * 1024  # 20 MB

# Full list of possible keys
PARAM_KEYS = [
    "rfppcc","rfppcw","rfh1cc","rfh1cw","lfppcc","lfppcw","cony","trro","brro","lrot","lrob","tbul","bbul","tdep","bdep",
    "upper","lower","sbal""rfv_yield", "rfh1_yield", "lfv_yield", "conicity_yield", 
    "lateral_run_out_yield", "bulge_yield", "dent_yield", "static_yield","upper_yield", "lower_yield", 
    "overall_uniformity_yield", "overall_visual_yield", "overall_warehouse_yield", "overall_scrap_yield"
]

ALERT_TYPES = [
    {
        "name": "UFM",
        "coll": alert_triggering_ubno_collect_name,
        "banner": "UFM ALERT",
        "filename_prefix": "UFM",
        "start_key": "machine_lot_start_ufm_time",
        "end_key": "machine_lot_end_ufm_time",
        "alert_time_key": "timestamp_alert_generated",
        "use_grouped": False,
        "shift_data_key": "shifts_data_tbm",
        "shift_start_time_key": "machine_shift_tbm_start_time"
    },
    {
        "name": "TBM",
        "coll": alert_triggering_collect_name,
        "banner": "TBM ALERT",
        "filename_prefix": "TBM",
        "start_key": "machine_lot_start_build_time",
        "end_key": "machine_lot_end_build_time",
        "alert_time_key": "timestamp_alert_generated",
        "use_grouped": True,
        "shift_data_key": "shifts_data",
        "shift_start_time_key": "machine_shift_tbm_start_time"
    },
    {
        "name": "CURING",
        "coll": alert_triggering_curing_collect_name,
        "banner": "CURING ALERT",
        "filename_prefix": "CURING",
        "start_key": "machine_lot_start_curing_time",
        "end_key": "machine_lot_end_curing_time",
        "alert_time_key": "timestamp_alert_generated",
        "use_grouped": True,
        "shift_data_key": "shifts_data_tbm",
        "shift_start_time_key": "machine_shift_tbm_start_time"
    }
]

# -------------------------------------------------
# Get OE Replacement from config database
# -------------------------------------------------
def get_oe_replacement(client, gt_code):
    try:
        cfg_db = client[CONFIG_DB_NAME]
        coll = cfg_db[CHANNEL_NO_COLLECT]
        doc = coll.find_one({"gt_code": {"$in": [gt_code]}})
        if doc:
            return sanitize_text(doc.get("oe_replacement", "N/A"))
        else:
            return "N/A"
    except Exception as e:
        print(f"[WARN] OE replacement lookup failed for {gt_code}: {e}")
        return "N/A"

# Utility functions
def sanitize_text(text):
    if text is None:
        return "N/A"
    text = str(text)
    return text.encode('ascii', 'replace').decode('ascii').replace('\ufffd', '?')

def html_table_to_markdown(html_table):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html_table, 'html.parser')
    markdown_table = ""
    headers = [th.get_text() for th in soup.find_all('th')]
    markdown_table += "| " + " | ".join(headers) + " |\n"
    markdown_table += "|" + "|".join(["---"] * len(headers)) + "|\n"
    for row in soup.find_all('tr')[1:]:
        cells = [td.get_text() for td in row.find_all('td')]
        markdown_table += "| " + " | ".join(cells) + " |\n"
    return markdown_table

def generate_combined_summary_table(alerts, alert_type, client=None):
    if not alerts:
        return "<table><tr><td>No alert data available</td></tr></table>"
    
    html = "<table>"
    
    if alert_type.upper() == "TBM":
        html += """
        <tr>
            <th>S.No</th>
            <th>SKU</th>
            <th>Gt Code</th>
            <th>Size Code</th>
            <th>TBM</th>
            <th>Total Tyre Produced</th>
            <th>Overall WAREHOUSE Yield (%)</th>
            <th>Overall UNIFORMITY Yield (%)</th>
            <th>Overall VISUAL Yield (%)</th>
            <th>Overall SCRAP Yield (%)</th>
        </tr>
        """
    elif alert_type.upper() == "CURING":
        html += """
        <tr>
            <th>S.No</th>
            <th>SKU</th>
            <th>Gt Code</th>
            <th>Size Code</th>
            <th>CURING</th>
            <th>Total Tyre Produced</th>
            <th>Overall WAREHOUSE Yield (%)</th>
            <th>Overall UNIFORMITY Yield (%)</th>
            <th>Overall VISUAL Yield (%)</th>
            <th>Overall SCRAP Yield (%)</th>
        </tr>
        """
    elif alert_type.upper() == "UFM":
        html += """
        <tr>
            <th>S.No</th>
            <th>SKU</th>
            <th>Size Code</th>
            <th>GT Code</th>
            <th>UFM</th>
            <th>Total Tyres Produced</th>
            <th>Overall WAREHOUSE Yield (%)</th>
            <th>Overall UNIFORMITY Yield (%)</th>
            <th>Overall VISUAL Yield (%)</th>
            <th>Overall SCRAP Yield (%)</th>
            <th>Overall UFM Grade (3)</th>
            <th>Overall UFM Grade (4)</th>
            <th>Overall UFM Grade (5)</th>
        </tr>
        """
    
    if alert_type.upper() == "TBM":
        for i, alert in enumerate(alerts):
            for machine in alert.get("machine_alerts", []):
                try:
                    warehouse = f"{float(machine.get('overall_warehouse_yield', 0)):.2f}" if machine.get("overall_warehouse_yield") not in [None, "N/A"] else "N/A"
                    uniformity = f"{float(machine.get('overall_uniformity_yield', 0)):.2f}" if machine.get("overall_uniformity_yield") not in [None, "N/A"] else "N/A"
                    visual = f"{float(machine.get('overall_visual_yield', 0)):.2f}" if machine.get("overall_visual_yield") not in [None, "N/A"] else "N/A"
                    scrap = f"{float(machine.get('overall_scrap_yield', 0)):.2f}" if machine.get("overall_scrap_yield") not in [None, "N/A"] else "N/A"
                except (ValueError, TypeError):
                    warehouse = uniformity = visual = scrap ="N/A"

                html += f"""
                <tr>
                    <td>{i+1}</td>
                    <td>{sanitize_text(alert.get('sku', 'N/A'))}</td>
                    <td>{sanitize_text(machine.get('gt_code', 'N/A'))}</td>
                    <td>{sanitize_text(machine.get('sizecode', 'N/A'))}</td>
                    <td>{sanitize_text(machine.get('machine', 'N/A'))}</td>
                    <td>{sanitize_text(machine.get('processed_machine_barcodes', 'N/A'))}</td>
                    <td>{warehouse}</td>
                    <td>{uniformity}</td>
                    <td>{visual}</td>
                    <td>{scrap}</td>
                </tr>
                """
    
    elif alert_type.upper() == "CURING":
        for i, alert in enumerate(alerts):
            for machine in alert.get("machine_alerts", []):
                try:
                    warehouse = f"{float(machine.get('overall_warehouse_yield', 0)):.2f}" if machine.get("overall_warehouse_yield") not in [None, "N/A"] else "N/A"
                    uniformity = f"{float(machine.get('overall_uniformity_yield', 0)):.2f}" if machine.get("overall_uniformity_yield") not in [None, "N/A"] else "N/A"
                    visual = f"{float(machine.get('overall_visual_yield', 0)):.2f}" if machine.get("overall_visual_yield") not in [None, "N/A"] else "N/A"
                    scrap = f"{float(machine.get('overall_scrap_yield', 0)):.2f}" if machine.get("overall_scrap_yield") not in [None, "N/A"] else "N/A"
                except (ValueError, TypeError):
                    warehouse = uniformity = visual = scrap ="N/A"

                html += f"""
                <tr>
                    <td>{i+1}</td>
                    <td>{sanitize_text(alert.get('sku', 'N/A'))}</td>
                    <td>{sanitize_text(machine.get('gt_code', 'N/A'))}</td>
                    <td>{sanitize_text(machine.get('sizecode', 'N/A'))}</td>
                    <td>{sanitize_text(machine.get('machine', 'N/A'))}</td>
                    <td>{sanitize_text(machine.get('processed_machine_barcodes', 'N/A'))}</td>
                    <td>{warehouse}</td>
                    <td>{uniformity}</td>
                    <td>{visual}</td>
                    <td>{scrap}</td>
                </tr>
                """
    
    elif alert_type.upper() == "UFM":
        for i, alert in enumerate(alerts):
            sku_val = alert.get('sku', 'N/A')
            for machine in alert.get("machine_alerts", []):
                try:
                    grade3 = grade4 = grade5 = 0
                    try:
                        _mc2 = MongoClient(MONGO_URI)
                        _summary_coll = _mc2[TUB_CIMCORP_VISUAL_MERGED_DB_alert][DATA_MERGED_SUMMARY_COLLECTION_alert]
                        machine_barcodes = []
                        for shift in machine.get('shifts_data_tbm', []):
                            machine_barcodes.extend(shift.get('barcodes', []))
                        for shift in machine.get('shifts_data', []):
                            machine_barcodes.extend(shift.get('barcodes', []))
                        machine_barcodes = list(set(machine_barcodes))
                        # A barcode can have multiple UFM test records (re-runs).
                        # Only the latest test per barcode is marked last == 1, so we
                        # restrict to it — otherwise counts exceed the lot size.
                        if not machine_barcodes:
                            query_filter = {"sku": sku_val, "last": 1, "overallufmgrade": {"$in": [3, 4, 5]}}
                        else:
                            query_filter = {"barcode": {"$in": machine_barcodes}, "last": 1, "overallufmgrade": {"$in": [3, 4, 5]}}
                        grade3 = _summary_coll.count_documents({**query_filter, "overallufmgrade": 3})
                        grade4 = _summary_coll.count_documents({**query_filter, "overallufmgrade": 4})
                        grade5 = _summary_coll.count_documents({**query_filter, "overallufmgrade": 5})
                        _mc2.close()
                    except Exception as _ge:
                        print(f"[WARN] overallufmgrade fetch failed: {_ge}")

                    warehouse  = f"{float(machine.get('overall_warehouse_yield',  0)):.2f}" if machine.get("overall_warehouse_yield")  not in [None, "N/A"] else "N/A"
                    uniformity = f"{float(machine.get('overall_uniformity_yield', 0)):.2f}" if machine.get("overall_uniformity_yield") not in [None, "N/A"] else "N/A"
                    visual     = f"{float(machine.get('overall_visual_yield',     0)):.2f}" if machine.get("overall_visual_yield")     not in [None, "N/A"] else "N/A"
                    scrap      = f"{float(machine.get('overall_scrap_yield',      0)):.2f}" if machine.get("overall_scrap_yield")      not in [None, "N/A"] else "N/A"
                except (ValueError, TypeError):
                    grade3 = grade4 = grade5 = 0
                    warehouse = uniformity = visual = scrap = "N/A"

                html += f"""
                <tr>
                    <td>{i+1}</td>
                    <td>{sanitize_text(sku_val)}</td>
                    <td>{sanitize_text(machine.get('sizecode', 'N/A'))}</td>
                    <td>{sanitize_text(machine.get('gt_code', 'N/A'))}</td>
                    <td>{sanitize_text(machine.get('machine', 'N/A'))}</td>
                    <td>{sanitize_text(machine.get('processed_machine_barcodes', 'N/A'))}</td>
                    <td>{warehouse}</td>
                    <td>{uniformity}</td>
                    <td>{visual}</td>
                    <td>{scrap}</td>
                    <td>{grade3}</td>
                    <td>{grade4}</td>
                    <td>{grade5}</td>
                </tr>
                """
    
    html += "</table>"
    return html

# def send_teams_alert(summary_html, alert_type, docs):
#     teams_webhook_url = TEAMS_WEBHOOK_URL
#     if not teams_webhook_url:
#         print("[ERROR] TEAMS_WEBHOOK_URL not configured.")
#         return False

#     # ── Only alert on documents with alert_flag_repl == False ──
#     # These come from alert_db_virgin_yield; a Teams alert must be sent only for
#     # not-yet-alerted lots. Filter defensively here so this holds even if the
#     # caller passes a broader set of docs.
#     docs = [d for d in (docs or []) if d.get("alert_flag_repl", False) is False]
#     if not docs:
#         print(f"[INFO] No alert_flag_repl=False documents to send for {alert_type}.")
#         return True

#     # Fetch yield spec thresholds from channel_no_collect (keyed by sku)
#     yield_spec_lookup = {}
#     try:
#         _mc = MongoClient(MONGO_URI)
#         _cfg_coll = _mc[CONFIG_DB_NAME][CHANNEL_NO_COLLECT]
#         for doc in _cfg_coll.find(
#             {},
#             {"_id": 0, "sku": 1,
#              "spec_overall_warehouse_yield": 1,
#              "spec_overall_uniformity_yield": 1,
#              "spec_overall_visual_yield": 1,
#              "spec_rfppwc_yield": 1,
#              "spec_rfh1wc_yield": 1,
#              "spec_plys_yield": 1,
#              "spec_lfppwc_yield": 1,
#              "spec_cony_yield": 1,
#              "spec_lro_yield": 1,
#              "spec_bulge_yield": 1,
#              "spec_dent_yield": 1,
#              "spec_upper_yield": 1,
#              "spec_lower_yield": 1,
#              "spec_sbal_yield": 1}
#         ):
#             sku_key = str(doc.get("sku", "")).strip()
#             if sku_key:
#                 yield_spec_lookup[sku_key] = {
#                     "spec_overall_warehouse_yield": doc.get("spec_overall_warehouse_yield"),
#                     "spec_overall_uniformity_yield": doc.get("spec_overall_uniformity_yield"),
#                     "spec_overall_visual_yield":    doc.get("spec_overall_visual_yield"),
#                     "spec_rfv_yield":               doc.get("spec_rfppwc_yield"),
#                     "spec_rfh1_yield":              doc.get("spec_rfh1wc_yield"),
#                     "spec_plysteer_yield":          doc.get("spec_plys_yield"),
#                     "spec_lfv_yield":               doc.get("spec_lfppwc_yield"),
#                     "spec_conicity_yield":          doc.get("spec_cony_yield"),
#                     "spec_lateral_run_out_yield":   doc.get("spec_lro_yield"),
#                     "spec_bulge_yield":             doc.get("spec_bulge_yield"),
#                     "spec_dent_yield":              doc.get("spec_dent_yield"),
#                     "spec_upper_yield":             doc.get("spec_upper_yield"),
#                     "spec_lower_yield":             doc.get("spec_lower_yield"),
#                     "spec_static_yield":            doc.get("spec_sbal_yield"),
#                 }
#         _mc.close()
#     except Exception as _e:
#         print(f"[WARN] Could not fetch yield specs from channel_no_collect: {_e}")

#     # Yield calculation functions
#     def yield_percentage(numerator, denominator):
#         try:
#             if denominator is None or denominator == 0:
#                 return 0.0
#             return round((numerator / denominator) * 100, 2)
#         except (TypeError, ValueError, ZeroDivisionError):
#             return 0.0

#     def get_yield_from_rank_counts(rank_counts, total_bc):
#         """
#         Extract numerator and denominator from rank_counts.
#         rank_counts format: {'grade_0': count, 'grade_3': count}
#         Pass grades are 1, 2, 3
#         """
#         if not rank_counts:
#             return 0, total_bc if total_bc > 0 else 1
        
#         numerator = 0
#         denominator = 0
        
#         for key, count in rank_counts.items():
#             # Extract grade number from key like 'rfppwcgrade_3' -> 3
#             try:
#                 if '_' in key:
#                     grade = int(key.split('_')[-1])
#                 else:
#                     grade = int(key)
                
#                 denominator += count
                
#                 # Grades 1, 2, 3 are PASS
#                 if grade in [1, 2, 3]:
#                     numerator += count
#             except (ValueError, IndexError):
#                 # If we can't parse the grade, skip
#                 continue
        
#         # If no denominator found, use total_bc
#         if denominator == 0:
#             denominator = total_bc if total_bc > 0 else 1
        
#         return numerator, denominator

#     def calculate_yields_from_machine_obj(machine_obj, total_bc, specific_barcodes=None):
#         """Calculate yields from machine_obj, optionally for specific barcodes"""
#         # If specific barcodes are provided, we need to filter the data
#         if specific_barcodes and len(specific_barcodes) > 0:
#             return calculate_yields_from_barcodes(specific_barcodes, total_bc, machine_obj)
#         else:
#             # Use the aggregated data from machine_obj
#             return calculate_yields_from_aggregated_data(machine_obj, total_bc)

#     def _grade_yield_counts(df, grade_col):
#         """Numerator/denominator using the canonical *_yield formula:
#         denominator = rows where (grade > 0) & (grade <= 5) & barcode notnull
#         numerator   = subset of denominator where grade in [1, 2, 3] & barcode notnull
#         """
#         try:
#             if df is None or df.empty or grade_col not in df.columns or 'barcode' not in df.columns:
#                 return 0, 0
#             grade = pd.to_numeric(df[grade_col], errors='coerce')
#             den_mask = (grade > 0) & (grade <= 5) & df['barcode'].notnull()
#             denominator = int(den_mask.sum())
#             num_mask = den_mask & grade.isin([1, 2, 3])
#             numerator = int(num_mask.sum())
#             return numerator, denominator
#         except Exception:
#             return 0, 0

#     def _overall_uniformity_counts(df):
#         """overall_uniformity_yield: den = measured - vi_downgrade, num = ufm pass & vi_yield==1"""
#         try:
#             grade = pd.to_numeric(df['overallufmgrade'], errors='coerce')
#             measured = int(((grade.isin([1, 2, 3, 4, 5])) & df['barcode'].notnull()).sum())
#             vi = pd.to_numeric(df['vi_yield'], errors='coerce') if 'vi_yield' in df.columns else pd.Series([], dtype=float)
#             vi_downgrade = int(((vi == 0) & vi.notna()).sum()) if len(vi) else 0
#             denominator = measured - vi_downgrade
#             if 'vi_yield' in df.columns:
#                 numerator = int(((grade.isin([1, 2, 3])) & (pd.to_numeric(df['vi_yield'], errors='coerce') == 1)).sum())
#             else:
#                 numerator = int(grade.isin([1, 2, 3]).sum())
#             return numerator, denominator
#         except Exception:
#             return 0, 0

#     def _overall_visual_counts(df):
#         """overall_visual_yield: den = ufm_count + vi_scrap, num = ufm_count - vi_downgrade"""
#         try:
#             grade = pd.to_numeric(df['overallufmgrade'], errors='coerce')
#             vi = pd.to_numeric(df['vi_yield'], errors='coerce') if 'vi_yield' in df.columns else pd.Series([], dtype=float)
#             vi_downgrade = int((vi == 0).sum()) if len(vi) else 0
#             if 'scrap_category_tub' in df.columns:
#                 vi_scrap = int((df['scrap_category_tub'] == "VISUAL").sum())
#             else:
#                 vi_scrap = 0
#             ufm_count = int(grade.isin([1, 2, 3, 4, 5]).sum())
#             denominator = ufm_count + vi_scrap
#             numerator = ufm_count - vi_downgrade
#             return numerator, denominator
#         except Exception:
#             return 0, 0

#     def _overall_warehouse_counts(df):
#         """overall_warehouse_yield: den = measured + vi_scrap, num = ufm grade in [1,2,3]"""
#         try:
#             grade = pd.to_numeric(df['overallufmgrade'], errors='coerce')
#             measured = int(((grade.isin([1, 2, 3, 4, 5])) & df['barcode'].notnull()).sum())
#             if 'scrap_category_tub' in df.columns:
#                 vi_scrap = int(((df['scrap_category_tub'] == "VISUAL") & df['scrap_category_tub'].notna()).sum())
#             else:
#                 vi_scrap = 0
#             denominator = measured + vi_scrap
#             numerator = int(grade.isin([1, 2, 3]).sum())
#             return numerator, denominator
#         except Exception:
#             return 0, 0

#     def calculate_yields_from_barcodes(barcodes, total_bc, machine_obj):
#         """Calculate yields for the given machine using ONLY this lot's barcodes.

#         The barcodes passed in are intersected with the lot barcodes (machine_obj['barcodes'])
#         so a machine never pulls tyres from other lots/SKUs. Every yield is computed with the
#         exact canonical formula (grade>0 & grade<=5 for denominator, grade in [1,2,3] for
#         numerator; overall_* use vi_yield / scrap_category_tub logic).
#         """
#         # Restrict to this lot's barcodes only
#         lot_barcodes = [str(b) for b in (machine_obj.get('barcodes') or []) if b]
#         machine_barcodes = [str(b) for b in (barcodes or []) if b]
#         if lot_barcodes:
#             lot_set = set(lot_barcodes)
#             scoped = [b for b in machine_barcodes if b in lot_set]
#             # if the machine-specific list didn't intersect, fall back to the lot barcodes
#             query_barcodes = scoped if scoped else lot_barcodes
#         else:
#             query_barcodes = machine_barcodes

#         if not query_barcodes:
#             return calculate_yields_from_aggregated_data(machine_obj, total_bc)

#         try:
#             _mc = MongoClient(MONGO_URI)
#             _db = _mc[TUB_CIMCORP_VISUAL_MERGED_DB_alert]
#             _coll = _db[DATA_MERGED_SUMMARY_COLLECTION_alert]

#             projection = {
#                 "_id": 0, "barcode": 1, "last": 1,
#                 "rfppwcgrade": 1, "rfh1wcgrade": 1, "lfppwcgrade": 1,
#                 "conygrade": 1, "lrograde": 1, "sbalgrade": 1,
#                 "uppergrade": 1, "lowergrade": 1, "bulwcgrade": 1, "depwcgrade": 1,
#                 "overallufmgrade": 1, "vi_yield": 1, "scrap_category_tub": 1,
#             }
#             # A barcode can have multiple UFM test records (re-runs). Only the
#             # latest test per barcode is marked last == 1. Restricting to it keeps
#             # one row per barcode so denominators never exceed the lot size.
#             docs = list(_coll.find(
#                 {"barcode": {"$in": query_barcodes}, "last": 1},
#                 projection
#             ))
#             _mc.close()
#         except Exception as e:
#             print(f"[ERROR] Failed to get specific barcode data: {e}")
#             return calculate_yields_from_aggregated_data(machine_obj, total_bc)

#         # Fallback: if the last==1 filter returned nothing (e.g. older records
#         # written before the 'last' flag existed), re-query without it and
#         # deduplicate to one row per barcode below.
#         if not docs:
#             try:
#                 _mc = MongoClient(MONGO_URI)
#                 _coll = _mc[TUB_CIMCORP_VISUAL_MERGED_DB_alert][DATA_MERGED_SUMMARY_COLLECTION_alert]
#                 docs = list(_coll.find({"barcode": {"$in": query_barcodes}}, projection))
#                 _mc.close()
#             except Exception as e:
#                 print(f"[ERROR] Fallback barcode fetch failed: {e}")

#         if not docs:
#             return calculate_yields_from_aggregated_data(machine_obj, total_bc)

#         df = pd.DataFrame(docs)

#         # Safety net: guarantee one row per barcode. Prefer the record flagged
#         # last == 1; otherwise keep the first occurrence.
#         if 'barcode' in df.columns and df['barcode'].duplicated().any():
#             if 'last' in df.columns:
#                 df['_last_pref'] = (pd.to_numeric(df['last'], errors='coerce') == 1).astype(int)
#                 df = (df.sort_values('_last_pref', ascending=False)
#                         .drop_duplicates(subset='barcode', keep='first')
#                         .drop(columns=['_last_pref']))
#             else:
#                 df = df.drop_duplicates(subset='barcode', keep='first')

#         # Per-parameter yields (canonical grade formula)
#         rfv_num, rfv_den = _grade_yield_counts(df, 'rfppwcgrade')
#         rfh1_num, rfh1_den = _grade_yield_counts(df, 'rfh1wcgrade')
#         lfv_num, lfv_den = _grade_yield_counts(df, 'lfppwcgrade')
#         conicity_num, conicity_den = _grade_yield_counts(df, 'conygrade')
#         static_num, static_den = _grade_yield_counts(df, 'sbalgrade')
#         upper_num, upper_den = _grade_yield_counts(df, 'uppergrade')
#         lower_num, lower_den = _grade_yield_counts(df, 'lowergrade')
#         bulge_num, bulge_den = _grade_yield_counts(df, 'bulwcgrade')
#         dent_num, dent_den = _grade_yield_counts(df, 'depwcgrade')

#         # Overall yields (canonical overall_* formulas)
#         wh_num, wh_den = _overall_warehouse_counts(df)
#         uni_num, uni_den = _overall_uniformity_counts(df)
#         vis_num, vis_den = _overall_visual_counts(df)

#         return {
#             'wh': (yield_percentage(wh_num, wh_den), wh_num, wh_den),
#             'uni': (yield_percentage(uni_num, uni_den), uni_num, uni_den),
#             'vis': (yield_percentage(vis_num, vis_den), vis_num, vis_den),
#             'rfv': (yield_percentage(rfv_num, rfv_den), rfv_num, rfv_den),
#             'rfh1': (yield_percentage(rfh1_num, rfh1_den), rfh1_num, rfh1_den),
#             'lfv': (yield_percentage(lfv_num, lfv_den), lfv_num, lfv_den),
#             'conicity': (yield_percentage(conicity_num, conicity_den), conicity_num, conicity_den),
#             'static': (yield_percentage(static_num, static_den), static_num, static_den),
#             'upper': (yield_percentage(upper_num, upper_den), upper_num, upper_den),
#             'lower': (yield_percentage(lower_num, lower_den), lower_num, lower_den),
#             'bulge': (yield_percentage(bulge_num, bulge_den), bulge_num, bulge_den),
#             'dent': (yield_percentage(dent_num, dent_den), dent_num, dent_den),
#         }

#     def calculate_yields_from_aggregated_data(machine_obj, total_bc):
#         """Calculate yields from aggregated machine_obj data"""
#         # Initialize with zeros
#         wh_percent, wh_num, wh_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         uni_percent, uni_num, uni_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         vis_percent, vis_num, vis_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         rfv_percent, rfv_num, rfv_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         rfh1_percent, rfh1_num, rfh1_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         lfv_percent, lfv_num, lfv_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         conicity_percent, conicity_num, conicity_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         static_percent, static_num, static_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         upper_percent, upper_num, upper_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         lower_percent, lower_num, lower_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         bulge_percent, bulge_num, bulge_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         dent_percent, dent_num, dent_den = 0.0, 0, total_bc if total_bc > 0 else 1
        
#         # Get parameter_alerts from machine_obj
#         param_alerts = machine_obj.get('parameter_alerts', [])
        
#         # Also check lot_averages for yields
#         lot_averages = machine_obj.get('lot_averages', {})
        
#         for param in param_alerts:
#             param_name = param.get('parameter')
#             current_value = param.get('current_value')
#             rank_counts = param.get('rank_counts', {})
            
#             # If rank_counts exists, use it to calculate numerator and denominator
#             if rank_counts:
#                 num, den = get_yield_from_rank_counts(rank_counts, total_bc)
                
#                 if param_name == 'rfv_yield':
#                     rfv_num, rfv_den = num, den
#                     rfv_percent = yield_percentage(rfv_num, rfv_den)
#                 elif param_name == 'rfh1_yield':
#                     rfh1_num, rfh1_den = num, den
#                     rfh1_percent = yield_percentage(rfh1_num, rfh1_den)
#                 elif param_name == 'lfv_yield':
#                     lfv_num, lfv_den = num, den
#                     lfv_percent = yield_percentage(lfv_num, lfv_den)
#                 elif param_name == 'conicity_yield':
#                     conicity_num, conicity_den = num, den
#                     conicity_percent = yield_percentage(conicity_num, conicity_den)
#                 elif param_name == 'static_yield':
#                     static_num, static_den = num, den
#                     static_percent = yield_percentage(static_num, static_den)
#                 elif param_name == 'upper_yield':
#                     upper_num, upper_den = num, den
#                     upper_percent = yield_percentage(upper_num, upper_den)
#                 elif param_name == 'lower_yield':
#                     lower_num, lower_den = num, den
#                     lower_percent = yield_percentage(lower_num, lower_den)
#                 elif param_name == 'bulge_yield':
#                     bulge_num, bulge_den = num, den
#                     bulge_percent = yield_percentage(bulge_num, bulge_den)
#                 elif param_name == 'dent_yield':
#                     dent_num, dent_den = num, den
#                     dent_percent = yield_percentage(dent_num, dent_den)
#                 elif param_name == 'overall_warehouse_yield':
#                     wh_num, wh_den = num, den
#                     wh_percent = yield_percentage(wh_num, wh_den)
#                 elif param_name == 'overall_uniformity_yield':
#                     uni_num, uni_den = num, den
#                     uni_percent = yield_percentage(uni_num, uni_den)
#                 elif param_name == 'overall_visual_yield':
#                     vis_num, vis_den = num, den
#                     vis_percent = yield_percentage(vis_num, vis_den)
#             else:
#                 # If no rank_counts, try to use current_value from lot_averages
#                 if param_name in lot_averages:
#                     value = lot_averages.get(param_name)
#                     if value is not None:
#                         # We only have percentage, not numerator/denominator
#                         # Use total_bc as denominator and calculate numerator
#                         den = total_bc if total_bc > 0 else 1
#                         num = int(round((value / 100) * den)) if value else 0
                        
#                         if param_name == 'rfv_yield':
#                             rfv_percent = float(value) if value else 0.0
#                             rfv_num, rfv_den = num, den
#                         elif param_name == 'rfh1_yield':
#                             rfh1_percent = float(value) if value else 0.0
#                             rfh1_num, rfh1_den = num, den
#                         elif param_name == 'lfv_yield':
#                             lfv_percent = float(value) if value else 0.0
#                             lfv_num, lfv_den = num, den
#                         elif param_name == 'conicity_yield':
#                             conicity_percent = float(value) if value else 0.0
#                             conicity_num, conicity_den = num, den
#                         elif param_name == 'static_yield':
#                             static_percent = float(value) if value else 0.0
#                             static_num, static_den = num, den
#                         elif param_name == 'upper_yield':
#                             upper_percent = float(value) if value else 0.0
#                             upper_num, upper_den = num, den
#                         elif param_name == 'lower_yield':
#                             lower_percent = float(value) if value else 0.0
#                             lower_num, lower_den = num, den
#                         elif param_name == 'bulge_yield':
#                             bulge_percent = float(value) if value else 0.0
#                             bulge_num, bulge_den = num, den
#                         elif param_name == 'dent_yield':
#                             dent_percent = float(value) if value else 0.0
#                             dent_num, dent_den = num, den
#                         elif param_name == 'overall_warehouse_yield':
#                             wh_percent = float(value) if value else 0.0
#                             wh_num, wh_den = num, den
#                         elif param_name == 'overall_uniformity_yield':
#                             uni_percent = float(value) if value else 0.0
#                             uni_num, uni_den = num, den
#                         elif param_name == 'overall_visual_yield':
#                             vis_percent = float(value) if value else 0.0
#                             vis_num, vis_den = num, den
        
#         # If we still don't have values, try lot_averages directly
#         if rfv_num == 0 and rfv_den == total_bc:
#             for key, value in lot_averages.items():
#                 if key == 'rfv_yield' and value is not None:
#                     rfv_percent = float(value)
#                     rfv_num = int(round((rfv_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     rfv_den = total_bc if total_bc > 0 else 1
#                 elif key == 'rfh1_yield' and value is not None:
#                     rfh1_percent = float(value)
#                     rfh1_num = int(round((rfh1_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     rfh1_den = total_bc if total_bc > 0 else 1
#                 elif key == 'lfv_yield' and value is not None:
#                     lfv_percent = float(value)
#                     lfv_num = int(round((lfv_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     lfv_den = total_bc if total_bc > 0 else 1
#                 elif key == 'conicity_yield' and value is not None:
#                     conicity_percent = float(value)
#                     conicity_num = int(round((conicity_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     conicity_den = total_bc if total_bc > 0 else 1
#                 elif key == 'static_yield' and value is not None:
#                     static_percent = float(value)
#                     static_num = int(round((static_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     static_den = total_bc if total_bc > 0 else 1
#                 elif key == 'upper_yield' and value is not None:
#                     upper_percent = float(value)
#                     upper_num = int(round((upper_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     upper_den = total_bc if total_bc > 0 else 1
#                 elif key == 'lower_yield' and value is not None:
#                     lower_percent = float(value)
#                     lower_num = int(round((lower_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     lower_den = total_bc if total_bc > 0 else 1
#                 elif key == 'bulge_yield' and value is not None:
#                     bulge_percent = float(value)
#                     bulge_num = int(round((bulge_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     bulge_den = total_bc if total_bc > 0 else 1
#                 elif key == 'dent_yield' and value is not None:
#                     dent_percent = float(value)
#                     dent_num = int(round((dent_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     dent_den = total_bc if total_bc > 0 else 1
#                 elif key == 'overall_warehouse_yield' and value is not None:
#                     wh_percent = float(value)
#                     wh_num = int(round((wh_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     wh_den = total_bc if total_bc > 0 else 1
#                 elif key == 'overall_uniformity_yield' and value is not None:
#                     uni_percent = float(value)
#                     uni_num = int(round((uni_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     uni_den = total_bc if total_bc > 0 else 1
#                 elif key == 'overall_visual_yield' and value is not None:
#                     vis_percent = float(value)
#                     vis_num = int(round((vis_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     vis_den = total_bc if total_bc > 0 else 1
        
#         return {
#             'wh': (wh_percent, wh_num, wh_den),
#             'uni': (uni_percent, uni_num, uni_den),
#             'vis': (vis_percent, vis_num, vis_den),
#             'rfv': (rfv_percent, rfv_num, rfv_den),
#             'rfh1': (rfh1_percent, rfh1_num, rfh1_den),
#             'lfv': (lfv_percent, lfv_num, lfv_den),
#             'conicity': (conicity_percent, conicity_num, conicity_den),
#             'static': (static_percent, static_num, static_den),
#             'upper': (upper_percent, upper_num, upper_den),
#             'lower': (lower_percent, lower_num, lower_den),
#             'bulge': (bulge_percent, bulge_num, bulge_den),
#             'dent': (dent_percent, dent_num, dent_den)
#         }

#     def _fmt(val):
#         try:
#             return f"{float(val):.2f}%" if val not in (None, 'N/A') else "N/A"
#         except (TypeError, ValueError):
#             return "N/A"

#     def _color(actual, spec):
#         try:
#             if actual is None or spec is None:
#                 return "none"
#             return "red" if float(actual) < float(spec) else "green"
#         except (TypeError, ValueError):
#             return "none"

#     def _ind(color_tag):
#         if color_tag == "green": return "✅"
#         if color_tag == "red":   return "🔴"
#         return ""

#     def _fmt_count(count):
#         if count is None:
#             return "N/A"
#         try:
#             return str(int(count))
#         except (TypeError, ValueError):
#             return "N/A"

#     alert_machine_entries = []

#     for alert in docs:
#         for machine in alert.get("machine_alerts", []):
#             raw_sizecode = machine.get('sizecode', 'N/A')
#             sizecode = raw_sizecode if raw_sizecode != 'N/A' else sanitize_text(raw_sizecode)
#             raw_gt_code = machine.get('gt_code', 'N/A')
#             gt_code = raw_gt_code if raw_gt_code != 'N/A' else sanitize_text(raw_gt_code)
#             param_alerts = machine.get('parameter_alerts', [])

#             entry = {
#                 'sku': sanitize_text(alert.get('sku', 'N/A')),
#                 'machine_name': sanitize_text(machine.get('machine', 'N/A')),
#                 'sizecode': sizecode,
#                 'gt_code': gt_code,
#                 'params': [],
#                 'machine_obj': machine,
#                 'alert_obj': alert,
#             }

#             for param in param_alerts:
#                 param_name = param.get('parameter')
#                 current_val = param.get('current_value')
#                 shift_threshold = param.get('shift_occurred_thershold', False)
#                 shift_spec = param.get('shift_occurred_spec', False)
#                 rank_counts = param.get('rank_counts', {})

#                 if shift_threshold or shift_spec:
#                     if param_name.endswith('_yield'):
#                         display_name = param_name.replace('_yield', ' yield')
#                     else:
#                         display_name = param_name

#                     if current_val == 0 or (isinstance(current_val, (int, float)) and current_val == 0):
#                         rank_str = ", ".join([
#                             f"{k.rsplit('_', 1)[0]}->{k.rsplit('_', 1)[1]}:{v}(count)"
#                             if '_' in k else f"{k}->{v}"
#                             for k, v in rank_counts.items()
#                         ]) if rank_counts else "No ranks"
#                         deviated_param = f"{display_name}({rank_str})"
#                     else:
#                         deviated_param = display_name

#                     entry['params'].append(deviated_param)

#             alert_machine_entries.append(entry)

#     if alert_type.upper() == "TBM":
#         title_str = "Alert: TBM Alert Summary"
#     elif alert_type.upper() == "CURING":
#         title_str = "Alert: CURING Alert Summary"
#     elif alert_type.upper() == "UFM":
#         title_str = "Alert: UFM Alert Summary"
#     else:
#         title_str = "Alert Summary"

#     markdown_table = html_table_to_markdown(summary_html)

#     breakdown_blocks = []

#     # Helper function to get barcodes for a specific curing machine
#     def get_barcodes_for_curing_machine(machine_obj, cur_id):
#         """
#         Get the list of barcodes that went through a specific curing machine.
#         Queries data_merged_summary_alerts_data by cur_id field — gives exact per-press barcodes.
#         """
#         try:
#             sku = machine_obj.get('sku') or machine_obj.get('gt_code', '')
#             _mc = MongoClient(MONGO_URI)
#             _db = _mc[TUB_CIMCORP_VISUAL_MERGED_DB_alert]
#             _coll = _db[DATA_MERGED_SUMMARY_COLLECTION_alert]

#             lot_barcodes = [str(b) for b in (machine_obj.get('barcodes') or []) if b]
#             query = {"cur_id": cur_id}
#             # Restrict to this lot's barcodes so we never pull other lots/SKUs
#             if lot_barcodes:
#                 query["barcode"] = {"$in": lot_barcodes}

#             docs = list(_coll.find(query, {"barcode": 1, "_id": 0}))
#             _mc.close()

#             barcodes = [str(d["barcode"]) for d in docs if d.get("barcode")]
#             if barcodes:
#                 return barcodes
#         except Exception as e:
#             print(f"[WARN] get_barcodes_for_curing_machine failed for {cur_id}: {e}")

#         # Fallback: use the main barcodes list (less accurate)
#         return machine_obj.get('barcodes', [])

#     # Helper function to get barcodes for a specific TBM machine
#     def get_barcodes_for_tbm_machine(machine_obj, tbm_id):
#         """
#         Get the list of barcodes that went through a specific TBM machine.
#         Queries data_merged_summary_alerts_data by ptb_id field.
#         """
#         try:
#             _mc = MongoClient(MONGO_URI)
#             _db = _mc[TUB_CIMCORP_VISUAL_MERGED_DB_alert]
#             _coll = _db[DATA_MERGED_SUMMARY_COLLECTION_alert]

#             lot_barcodes = [str(b) for b in (machine_obj.get('barcodes') or []) if b]
#             query = {"ptb_id": tbm_id}
#             # Restrict to this lot's barcodes so we never pull other lots/SKUs
#             if lot_barcodes:
#                 query["barcode"] = {"$in": lot_barcodes}

#             docs = list(_coll.find(query, {"barcode": 1, "_id": 0}))
#             _mc.close()

#             barcodes = [str(d["barcode"]) for d in docs if d.get("barcode")]
#             if barcodes:
#                 return barcodes
#         except Exception as e:
#             print(f"[WARN] get_barcodes_for_tbm_machine failed for {tbm_id}: {e}")

#         # Fallback: use the main barcodes list
#         return machine_obj.get('barcodes', [])

#     for entry in alert_machine_entries:
#         sku         = entry['sku']
#         machine_obj = entry['machine_obj']
#         alert_obj   = entry['alert_obj']
#         _at         = alert_type.upper()

#         total_bc  = machine_obj.get('processed_machine_barcodes', 0)
#         sku_specs = yield_spec_lookup.get(str(sku).strip(), {})
#         spec_wh   = sku_specs.get('spec_overall_warehouse_yield')
#         spec_uni  = sku_specs.get('spec_overall_uniformity_yield')
#         spec_vis  = sku_specs.get('spec_overall_visual_yield')
#         spec_rfv  = sku_specs.get('spec_rfv_yield')
#         spec_rfh1 = sku_specs.get('spec_rfh1_yield')
#         spec_lfv  = sku_specs.get('spec_lfv_yield')
#         spec_conicity = sku_specs.get('spec_conicity_yield')
#         spec_static   = sku_specs.get('spec_static_yield')
#         spec_upper    = sku_specs.get('spec_upper_yield')
#         spec_lower    = sku_specs.get('spec_lower_yield')
#         spec_bulge    = sku_specs.get('spec_bulge_yield')
#         spec_dent     = sku_specs.get('spec_dent_yield')

#         entry_rows = []

#         def _add_row(mname, mtype, bc, mach_obj, specific_barcodes=None):
#             """Add a row with yields calculated from machine_obj"""
#             # Calculate yields directly from machine_obj
#             yields = calculate_yields_from_machine_obj(mach_obj, bc, specific_barcodes)
            
#             wh_percent, wh_num, wh_den = yields['wh']
#             uni_percent, uni_num, uni_den = yields['uni']
#             vis_percent, vis_num, vis_den = yields['vis']
#             rfv_percent, rfv_num, rfv_den = yields['rfv']
#             rfh1_percent, rfh1_num, rfh1_den = yields['rfh1']
#             lfv_percent, lfv_num, lfv_den = yields['lfv']
#             conicity_percent, conicity_num, conicity_den = yields['conicity']
#             static_percent, static_num, static_den = yields['static']
#             upper_percent, upper_num, upper_den = yields['upper']
#             lower_percent, lower_num, lower_den = yields['lower']
#             bulge_percent, bulge_num, bulge_den = yields['bulge']
#             dent_percent, dent_num, dent_den = yields['dent']
            
#             entry_rows.append({
#                 'mname': mname,
#                 'mtype': mtype,
#                 'bc': bc,
#                 'wh': (wh_percent, wh_num, wh_den),
#                 'uni': (uni_percent, uni_num, uni_den),
#                 'vis': (vis_percent, vis_num, vis_den),
#                 'rfv': (rfv_percent, rfv_num, rfv_den),
#                 'rfh1': (rfh1_percent, rfh1_num, rfh1_den),
#                 'lfv': (lfv_percent, lfv_num, lfv_den),
#                 'conicity': (conicity_percent, conicity_num, conicity_den),
#                 'static': (static_percent, static_num, static_den),
#                 'upper': (upper_percent, upper_num, upper_den),
#                 'lower': (lower_percent, lower_num, lower_den),
#                 'bulge': (bulge_percent, bulge_num, bulge_den),
#                 'dent': (dent_percent, dent_num, dent_den),
#                 'wh_color': _color(wh_percent, spec_wh),
#                 'uni_color': _color(uni_percent, spec_uni),
#                 'vis_color': _color(vis_percent, spec_vis),
#                 'rfv_color': _color(rfv_percent, spec_rfv),
#                 'rfh1_color': _color(rfh1_percent, spec_rfh1),
#                 'lfv_color': _color(lfv_percent, spec_lfv),
#                 'conicity_color': _color(conicity_percent, spec_conicity),
#                 'static_color': _color(static_percent, spec_static),
#                 'upper_color': _color(upper_percent, spec_upper),
#                 'lower_color': _color(lower_percent, spec_lower),
#                 'bulge_color': _color(bulge_percent, spec_bulge),
#                 'dent_color': _color(dent_percent, spec_dent),
#             })

#         # Build rows based on alert type
#         if _at == "UFM":
#             # UFM row - compute from this lot's own barcodes so num/den are real
#             # counts (consistent with the machine breakdown rows) using the
#             # canonical yield formulas.
#             _add_row(
#                 sanitize_text(machine_obj.get('machine', 'N/A')), 
#                 "UFM", 
#                 int(total_bc) if total_bc else 0,
#                 machine_obj,
#                 machine_obj.get('barcodes', [])  # this lot's barcodes only
#             )
            
#             # Curing rows - get specific barcodes for each curing machine
#             for c in machine_obj.get('cur_ids', []):
#                 bc = c.get('barcode', 0)
#                 cur_id = c.get('cur_id', 'N/A')
                
#                 # Get the barcodes that went through this curing machine
#                 cur_barcodes = get_barcodes_for_curing_machine(machine_obj, cur_id)
                
#                 # If we couldn't get specific barcodes, use the main barcodes list
#                 if not cur_barcodes:
#                     cur_barcodes = machine_obj.get('barcodes', [])
                
#                 curing_obj = machine_obj.copy()
#                 curing_obj['processed_machine_barcodes'] = bc
#                 _add_row(
#                     sanitize_text(str(cur_id)), 
#                     "Curing", 
#                     int(bc),
#                     curing_obj,
#                     cur_barcodes  # Pass specific barcodes for this curing machine
#                 )
            
#             # TBM rows - get specific barcodes for each TBM machine
#             tbm_ids_raw = machine_obj.get('tbm_ids', [])
#             tbm_machine = sanitize_text(machine_obj.get('tbm_machine', 'N/A'))
#             if tbm_ids_raw:
#                 for t in tbm_ids_raw:
#                     bc = t.get('barcode', 0)
#                     tbm_id = t.get('tbm_id', 'N/A')
                    
#                     # Get the barcodes that went through this TBM machine
#                     tbm_barcodes = get_barcodes_for_tbm_machine(machine_obj, tbm_id)
                    
#                     # If we couldn't get specific barcodes, use the main barcodes list
#                     if not tbm_barcodes:
#                         tbm_barcodes = machine_obj.get('barcodes', [])
                    
#                     tbm_obj = machine_obj.copy()
#                     tbm_obj['processed_machine_barcodes'] = bc
#                     _add_row(
#                         sanitize_text(str(tbm_id)), 
#                         "TBM", 
#                         int(bc),
#                         tbm_obj,
#                         tbm_barcodes  # Pass specific barcodes for this TBM machine
#                     )
#             elif tbm_machine and tbm_machine != 'N/A':
#                 # If no specific TBM IDs, use the main TBM machine with all barcodes
#                 tbm_barcodes = machine_obj.get('barcodes', [])
#                 tbm_obj = machine_obj.copy()
#                 tbm_obj['processed_machine_barcodes'] = total_bc
#                 _add_row(
#                     tbm_machine, 
#                     "TBM", 
#                     int(total_bc) if total_bc else 0,
#                     tbm_obj,
#                     tbm_barcodes
#                 )

#         elif _at == "TBM":
#             # TBM row - use aggregated data
#             _add_row(
#                 sanitize_text(machine_obj.get('machine', 'N/A')), 
#                 "TBM", 
#                 int(total_bc) if total_bc else 0,
#                 machine_obj,
#                 None
#             )
            
#             # UFM rows
#             seen_ubno = set()
#             for ma in alert_obj.get("machine_alerts", []):
#                 for rec in ma.get("grouped_averages", {}).get("uf_id", []):
#                     uf = sanitize_text(rec.get('uf_id', ''))
#                     bc = rec.get('barcode_count', 0)
#                     if uf and uf not in seen_ubno:
#                         seen_ubno.add(uf)
#                         # Get barcodes for this UFM machine
#                         ufm_barcodes = machine_obj.get('barcodes', [])
#                         ufm_obj = machine_obj.copy()
#                         ufm_obj['processed_machine_barcodes'] = bc
#                         _add_row(
#                             uf, 
#                             "UFM", 
#                             int(bc),
#                             ufm_obj,
#                             ufm_barcodes
#                         )
            
#             # Curing rows
#             for c in machine_obj.get('cur_ids', []):
#                 bc = c.get('barcode', 0)
#                 cur_id = c.get('cur_id', 'N/A')
#                 cur_barcodes = get_barcodes_for_curing_machine(machine_obj, cur_id)
#                 if not cur_barcodes:
#                     cur_barcodes = machine_obj.get('barcodes', [])
#                 cur_obj = machine_obj.copy()
#                 cur_obj['processed_machine_barcodes'] = bc
#                 _add_row(
#                     sanitize_text(str(cur_id)), 
#                     "Curing", 
#                     int(bc),
#                     cur_obj,
#                     cur_barcodes
#                 )

#         elif _at == "CURING":
#             # Curing row - use aggregated data
#             _add_row(
#                 sanitize_text(machine_obj.get('machine', 'N/A')), 
#                 "Curing", 
#                 int(total_bc) if total_bc else 0,
#                 machine_obj,
#                 None
#             )
            
#             # UFM rows
#             seen_ubno = set()
#             for ma in alert_obj.get("machine_alerts", []):
#                 for rec in ma.get("grouped_averages", {}).get("uf_id", []):
#                     uf = sanitize_text(rec.get('uf_id', ''))
#                     bc = rec.get('barcode_count', 0)
#                     if uf and uf not in seen_ubno:
#                         seen_ubno.add(uf)
#                         ufm_barcodes = machine_obj.get('barcodes', [])
#                         ufm_obj = machine_obj.copy()
#                         ufm_obj['processed_machine_barcodes'] = bc
#                         _add_row(
#                             uf, 
#                             "UFM", 
#                             int(bc),
#                             ufm_obj,
#                             ufm_barcodes
#                         )
            
#             # TBM rows
#             for p in machine_obj.get('ptb_ids', []):
#                 bc = p.get('barcode', 0)
#                 ptb_id = p.get('ptb_id', 'N/A')
#                 tbm_barcodes = machine_obj.get('barcodes', [])
#                 tbm_obj = machine_obj.copy()
#                 tbm_obj['processed_machine_barcodes'] = bc
#                 _add_row(
#                     sanitize_text(str(ptb_id)), 
#                     "TBM", 
#                     int(bc),
#                     tbm_obj,
#                     tbm_barcodes
#                 )

#         if not entry_rows:
#             continue

#         primary_machine = sanitize_text(machine_obj.get('machine', 'N/A'))

#         html_table = f"<p><strong>SKU: {sku} | Machine: {primary_machine}</strong></p>"
#         html_table += "<table border='1' cellpadding='5' cellspacing='0' style='border-collapse: collapse; width: 100%;'>"
#         html_table += "<thead><tr style='background-color: #4A90E2; color: white;'>"
#         html_table += "<th>Machine</th><th>Machine Type</th><th>Total Tyres</th>"
#         html_table += "<th>WH Num</th><th>WH Den</th><th>WAREHOUSE Yield %</th>"
#         html_table += "<th>UNI Num</th><th>UNI Den</th><th>UNIFORMITY Yield %</th>"
#         html_table += "<th>VIS Num</th><th>VIS Den</th><th>VISUAL Yield %</th>"
#         html_table += "<th>RFV Num</th><th>RFV Den</th><th>RFV Yield %</th>"
#         html_table += "<th>RFH1 Num</th><th>RFH1 Den</th><th>RFH1 Yield %</th>"
#         html_table += "<th>LFV Num</th><th>LFV Den</th><th>LFV Yield %</th>"
#         html_table += "<th>CON Num</th><th>CON Den</th><th>Conicity Yield %</th>"
#         html_table += "<th>STA Num</th><th>STA Den</th><th>Static Yield %</th>"
#         html_table += "<th>UPP Num</th><th>UPP Den</th><th>Upper Yield %</th>"
#         html_table += "<th>LOW Num</th><th>LOW Den</th><th>Lower Yield %</th>"
#         html_table += "<th>BUL Num</th><th>BUL Den</th><th>Bulge Yield %</th>"
#         html_table += "<th>DEN Num</th><th>DEN Den</th><th>Dent Yield %</th>"
#         html_table += "</tr></thead><tbody>"

#         def _format_yield_cell(percent, num, den, spec_val):
#             color = _color(percent, spec_val)
#             if percent is None or percent == 0:
#                 return f"0.00% {_ind(color)}"
#             return f"{_fmt(percent)} {_ind(color)}"

#         for row in entry_rows:
#             mname = row['mname']
#             mtype = row['mtype']
#             bc = row['bc']
            
#             wh_percent, wh_num, wh_den = row['wh']
#             uni_percent, uni_num, uni_den = row['uni']
#             vis_percent, vis_num, vis_den = row['vis']
#             rfv_percent, rfv_num, rfv_den = row['rfv']
#             rfh1_percent, rfh1_num, rfh1_den = row['rfh1']
#             lfv_percent, lfv_num, lfv_den = row['lfv']
#             conicity_percent, conicity_num, conicity_den = row['conicity']
#             static_percent, static_num, static_den = row['static']
#             upper_percent, upper_num, upper_den = row['upper']
#             lower_percent, lower_num, lower_den = row['lower']
#             bulge_percent, bulge_num, bulge_den = row['bulge']
#             dent_percent, dent_num, dent_den = row['dent']

#             html_table += "<tr>"
#             html_table += f"<td>{mname}</td><td>{mtype}</td><td>{bc}</td>"
            
#             html_table += f"<td>{_fmt_count(wh_num)}</td><td>{_fmt_count(wh_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(wh_percent, wh_num, wh_den, spec_wh)}</td>"
            
#             html_table += f"<td>{_fmt_count(uni_num)}</td><td>{_fmt_count(uni_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(uni_percent, uni_num, uni_den, spec_uni)}</td>"
            
#             html_table += f"<td>{_fmt_count(vis_num)}</td><td>{_fmt_count(vis_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(vis_percent, vis_num, vis_den, spec_vis)}</td>"
            
#             html_table += f"<td>{_fmt_count(rfv_num)}</td><td>{_fmt_count(rfv_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(rfv_percent, rfv_num, rfv_den, spec_rfv)}</td>"
            
#             html_table += f"<td>{_fmt_count(rfh1_num)}</td><td>{_fmt_count(rfh1_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(rfh1_percent, rfh1_num, rfh1_den, spec_rfh1)}</td>"
            
#             html_table += f"<td>{_fmt_count(lfv_num)}</td><td>{_fmt_count(lfv_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(lfv_percent, lfv_num, lfv_den, spec_lfv)}</td>"
            
#             html_table += f"<td>{_fmt_count(conicity_num)}</td><td>{_fmt_count(conicity_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(conicity_percent, conicity_num, conicity_den, spec_conicity)}</td>"
            
#             html_table += f"<td>{_fmt_count(static_num)}</td><td>{_fmt_count(static_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(static_percent, static_num, static_den, spec_static)}</td>"
            
#             html_table += f"<td>{_fmt_count(upper_num)}</td><td>{_fmt_count(upper_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(upper_percent, upper_num, upper_den, spec_upper)}</td>"
            
#             html_table += f"<td>{_fmt_count(lower_num)}</td><td>{_fmt_count(lower_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(lower_percent, lower_num, lower_den, spec_lower)}</td>"
            
#             html_table += f"<td>{_fmt_count(bulge_num)}</td><td>{_fmt_count(bulge_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(bulge_percent, bulge_num, bulge_den, spec_bulge)}</td>"
            
#             html_table += f"<td>{_fmt_count(dent_num)}</td><td>{_fmt_count(dent_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(dent_percent, dent_num, dent_den, spec_dent)}</td>"
            
#             html_table += "</tr>"

#         html_table += "</tbody></table>"
#         breakdown_blocks.append(html_table)

#     # Microsoft Teams incoming webhooks reject a message once the WHOLE JSON
#     # payload exceeds ~28 KB (not just the visible text). The previous logic
#     # measured only the HTML character length, so payloads still went over the
#     # byte limit (JSON escaping + wrapper fields + multi-byte chars all add up),
#     # and the SUMMARY table itself was never split. Fix: measure the real
#     # serialized payload byte size, and split BOTH the summary and the machine
#     # breakdown on row boundaries so every message stays safely under 28 KB.
#     TEAMS_PAYLOAD_LIMIT = 28000                       # hard Teams limit (bytes)
#     TEAMS_SAFE_BYTES = 24000                          # safety budget for the text body

#     def _payload_bytes(text_body):
#         """Actual serialized JSON size of the message we would POST."""
#         payload = {
#             "@type": "MessageCard",
#             "@context": "http://schema.org/extensions",
#             "summary": title_str,
#             "themeColor": "0076D7",
#             "title": title_str,
#             "text": text_body,
#         }
#         return len(json.dumps(payload).encode("utf-8"))

#     def _post_teams(text_body, label):
#         """Post one Teams message. Returns True only on a genuine success."""
#         payload = {
#             "@type": "MessageCard",
#             "@context": "http://schema.org/extensions",
#             "summary": title_str,
#             "themeColor": "0076D7",
#             "title": title_str,
#             "text": text_body,
#         }
#         try:
#             resp = requests.post(
#                 teams_webhook_url,
#                 json=payload,
#                 headers={"Content-Type": "application/json"},
#                 timeout=30,
#             )
#         except Exception as e:
#             print(f"[ERROR] Exception sending Teams {label}: {e}")
#             return False

#         body = (resp.text or "").strip()
#         # Teams returns "1" on success. Any HTTP error, or an error string in the
#         # body (e.g. "payload is too large"), is a failure.
#         body_indicates_error = ("error" in body.lower()) or ("too large" in body.lower())
#         if resp.status_code in (200, 202) and not body_indicates_error:
#             print(f"[INFO] Teams {label} sent successfully for {alert_type}")
#             return True

#         print(f"[ERROR] Failed to send Teams {label}: {resp.status_code} {body[:300]}")
#         return False

#     def _split_on_rows(html, header=""):
#         """Split HTML into chunks whose FULL payload stays under the byte limit.
#         Splits on </tr> boundaries so no table cell is broken. Falls back to a
#         hard character split if a single row is still too large."""
#         pieces = html.split("</tr>")
#         chunks, current = [], ""
#         for part in pieces:
#             piece = part + "</tr>" if part.strip() else part
#             candidate = current + piece
#             if current and _payload_bytes(header + candidate) > TEAMS_SAFE_BYTES:
#                 chunks.append(current)
#                 current = piece
#             else:
#                 current = candidate
#         if current.strip():
#             chunks.append(current)

#         # Safety net: if any single chunk is still over the limit (e.g. one giant
#         # row), hard-split it by characters.
#         safe_chunks = []
#         for ch in chunks:
#             if _payload_bytes(header + ch) <= TEAMS_SAFE_BYTES:
#                 safe_chunks.append(ch)
#             else:
#                 step = 8000
#                 for i in range(0, len(ch), step):
#                     safe_chunks.append(ch[i:i + step])
#         return safe_chunks

#     def _batch_blocks(blocks, header_for_measure=""):
#         """Group breakdown blocks into batches whose full payload stays under the
#         byte limit. Multiple SKU/Machine blocks are combined into one message so
#         we send a handful of batch messages instead of one per machine. A single
#         block that is itself too large is split on row boundaries."""
#         batches, current = [], []
#         separator = "<br><br>"
#         for block in blocks:
#             # A single oversized block: flush current batch, then split it out.
#             if _payload_bytes(header_for_measure + block) > TEAMS_SAFE_BYTES:
#                 if current:
#                     batches.append(current)
#                     current = []
#                 for chunk in _split_on_rows(block, header_for_measure):
#                     batches.append([chunk])
#                 continue

#             candidate = current + [block]
#             candidate_text = separator.join(candidate)
#             if current and _payload_bytes(header_for_measure + candidate_text) > TEAMS_SAFE_BYTES:
#                 batches.append(current)
#                 current = [block]
#             else:
#                 current.append(block)
#         if current:
#             batches.append(current)
#         return batches

#     overall_ok = True

#     # ── Summary table (split into parts if it is itself too large) ────────────
#     summary_header = f"**{title_str}**\n\n"
#     summary_chunks = _split_on_rows(markdown_table, summary_header) if _payload_bytes(summary_header + markdown_table) > TEAMS_SAFE_BYTES else [markdown_table]
#     for s_idx, chunk in enumerate(summary_chunks, 1):
#         label = "summary" if len(summary_chunks) == 1 else f"summary (part {s_idx}/{len(summary_chunks)})"
#         ok = _post_teams(summary_header + chunk, label)
#         overall_ok = overall_ok and ok

#     # ── Batch-wise machine breakdown messages ─────────────────────────────────
#     if breakdown_blocks:
#         # Use a representative header for size measurement.
#         batches = _batch_blocks(breakdown_blocks, header_for_measure="**Machine Breakdown — Batch 00/00**<br><br>")
#         total_batches = len(batches)
#         for b_idx, batch in enumerate(batches, 1):
#             header = f"**Machine Breakdown — Batch {b_idx}/{total_batches}**<br><br>"
#             body = "<br><br>".join(batch)
#             ok = _post_teams(header + body, f"breakdown batch {b_idx}/{total_batches}")
#             overall_ok = overall_ok and ok
#     else:
#         overall_ok = overall_ok and _post_teams(
#             f"**Machine Breakdown**\n\n<p>No data available.</p>", "breakdown"
#         )

#     return overall_ok

def send_teams_alert(summary_html, alert_type, docs):
    teams_webhook_url = TEAMS_WEBHOOK_URL
    if not teams_webhook_url:
        print("[ERROR] TEAMS_WEBHOOK_URL not configured.")
        return False

    # ── Only alert on documents with alert_flag_repl == False ──
    # These come from alert_db_virgin_yield; a Teams alert must be sent only for
    # not-yet-alerted lots. Filter defensively here so this holds even if the
    # caller passes a broader set of docs.
    docs = [d for d in (docs or []) if d.get("alert_flag_repl", False) is False]
    if not docs:
        print(f"[INFO] No alert_flag_repl=False documents to send for {alert_type}.")
        return True

    # ── Only send a Teams alert when the overall uniformity yield is < 99% ──
    # A lot qualifies if ANY of its machines has overall_uniformity_yield < 99.
    def _lot_uniformity_below_99(doc):
        for m in doc.get("machine_alerts", []):
            uni = m.get("overall_uniformity_yield")
            if uni is None:
                uni = (m.get("lot_averages", {}) or {}).get("overall_uniformity_yield")
            try:
                if uni is not None and float(uni) < 99:
                    return True
            except (TypeError, ValueError):
                continue
        return False

    docs = [d for d in docs if _lot_uniformity_below_99(d)]
    if not docs:
        print(f"[INFO] No lots with overall_uniformity_yield < 99% to send for {alert_type}.")
        return True

    # Fetch yield spec thresholds from channel_no_collect (keyed by sku)
    yield_spec_lookup = {}
    try:
        _mc = MongoClient(MONGO_URI)
        _cfg_coll = _mc[CONFIG_DB_NAME][CHANNEL_NO_COLLECT]
        for doc in _cfg_coll.find(
            {},
            {"_id": 0, "sku": 1,
             "spec_overall_warehouse_yield": 1,
             "spec_overall_uniformity_yield": 1,
             "spec_overall_visual_yield": 1,
             "spec_rfppwc_yield": 1,
             "spec_rfh1wc_yield": 1,
             "spec_plys_yield": 1,
             "spec_lfppwc_yield": 1,
             "spec_cony_yield": 1,
             "spec_lro_yield": 1,
             "spec_bulge_yield": 1,
             "spec_dent_yield": 1,
             "spec_upper_yield": 1,
             "spec_lower_yield": 1,
             "spec_sbal_yield": 1}
        ):
            sku_key = str(doc.get("sku", "")).strip()
            if sku_key:
                yield_spec_lookup[sku_key] = {
                    "spec_overall_warehouse_yield": doc.get("spec_overall_warehouse_yield"),
                    "spec_overall_uniformity_yield": doc.get("spec_overall_uniformity_yield"),
                    "spec_overall_visual_yield":    doc.get("spec_overall_visual_yield"),
                    "spec_rfv_yield":               doc.get("spec_rfppwc_yield"),
                    "spec_rfh1_yield":              doc.get("spec_rfh1wc_yield"),
                    "spec_plysteer_yield":          doc.get("spec_plys_yield"),
                    "spec_lfv_yield":               doc.get("spec_lfppwc_yield"),
                    "spec_conicity_yield":          doc.get("spec_cony_yield"),
                    "spec_lateral_run_out_yield":   doc.get("spec_lro_yield"),
                    "spec_bulge_yield":             doc.get("spec_bulge_yield"),
                    "spec_dent_yield":              doc.get("spec_dent_yield"),
                    "spec_upper_yield":             doc.get("spec_upper_yield"),
                    "spec_lower_yield":             doc.get("spec_lower_yield"),
                    "spec_static_yield":            doc.get("spec_sbal_yield"),
                }
        _mc.close()
    except Exception as _e:
        print(f"[WARN] Could not fetch yield specs from channel_no_collect: {_e}")

    # Yield calculation functions
    def yield_percentage(numerator, denominator):
        try:
            if denominator is None or denominator == 0:
                return 0.0
            return round((numerator / denominator) * 100, 2)
        except (TypeError, ValueError, ZeroDivisionError):
            return 0.0

    def get_yield_from_rank_counts(rank_counts, total_bc):
        """
        Extract numerator and denominator from rank_counts.
        rank_counts format: {'grade_0': count, 'grade_3': count}
        Pass grades are 1, 2, 3
        """
        if not rank_counts:
            return 0, total_bc if total_bc > 0 else 1
        
        numerator = 0
        denominator = 0
        
        for key, count in rank_counts.items():
            # Extract grade number from key like 'rfppwcgrade_3' -> 3
            try:
                if '_' in key:
                    grade = int(key.split('_')[-1])
                else:
                    grade = int(key)
                
                denominator += count
                
                # Grades 1, 2, 3 are PASS
                if grade in [1, 2, 3]:
                    numerator += count
            except (ValueError, IndexError):
                # If we can't parse the grade, skip
                continue
        
        # If no denominator found, use total_bc
        if denominator == 0:
            denominator = total_bc if total_bc > 0 else 1
        
        return numerator, denominator

    def calculate_yields_from_machine_obj(machine_obj, total_bc, specific_barcodes=None):
        """Calculate yields from machine_obj, optionally for specific barcodes"""
        # If specific barcodes are provided, we need to filter the data
        if specific_barcodes and len(specific_barcodes) > 0:
            return calculate_yields_from_barcodes(specific_barcodes, total_bc, machine_obj)
        else:
            # Use the aggregated data from machine_obj
            return calculate_yields_from_aggregated_data(machine_obj, total_bc)

    def _grade_yield_counts(df, grade_col):
        """Numerator/denominator using the canonical *_yield formula:
        denominator = rows where (grade > 0) & (grade <= 5) & barcode notnull
        numerator   = subset of denominator where grade in [1, 2, 3] & barcode notnull
        """
        try:
            if df is None or df.empty or grade_col not in df.columns or 'barcode' not in df.columns:
                return 0, 0
            grade = pd.to_numeric(df[grade_col], errors='coerce')
            den_mask = (grade > 0) & (grade <= 5) & df['barcode'].notnull()
            denominator = int(den_mask.sum())
            num_mask = den_mask & grade.isin([1, 2, 3])
            numerator = int(num_mask.sum())
            return numerator, denominator
        except Exception:
            return 0, 0

    def _overall_uniformity_counts(df):
        """overall_uniformity_yield: den = measured - vi_downgrade, num = ufm pass & vi_yield==1"""
        try:
            grade = pd.to_numeric(df['overallufmgrade'], errors='coerce')
            measured = int(((grade.isin([1, 2, 3, 4, 5])) & df['barcode'].notnull()).sum())
            vi = pd.to_numeric(df['vi_yield'], errors='coerce') if 'vi_yield' in df.columns else pd.Series([], dtype=float)
            vi_downgrade = int(((vi == 0) & vi.notna()).sum()) if len(vi) else 0
            denominator = measured - vi_downgrade
            if 'vi_yield' in df.columns:
                numerator = int(((grade.isin([1, 2, 3])) & (pd.to_numeric(df['vi_yield'], errors='coerce') == 1)).sum())
            else:
                numerator = int(grade.isin([1, 2, 3]).sum())
            return numerator, denominator
        except Exception:
            return 0, 0

    def _overall_visual_counts(df):
        """overall_visual_yield: den = ufm_count + vi_scrap, num = ufm_count - vi_downgrade"""
        try:
            grade = pd.to_numeric(df['overallufmgrade'], errors='coerce')
            vi = pd.to_numeric(df['vi_yield'], errors='coerce') if 'vi_yield' in df.columns else pd.Series([], dtype=float)
            vi_downgrade = int((vi == 0).sum()) if len(vi) else 0
            if 'scrap_category_tub' in df.columns:
                vi_scrap = int((df['scrap_category_tub'] == "VISUAL").sum())
            else:
                vi_scrap = 0
            ufm_count = int(grade.isin([1, 2, 3, 4, 5]).sum())
            denominator = ufm_count + vi_scrap
            numerator = ufm_count - vi_downgrade
            return numerator, denominator
        except Exception:
            return 0, 0

    def _overall_warehouse_counts(df):
        """overall_warehouse_yield: den = measured + vi_scrap, num = ufm grade in [1,2,3]"""
        try:
            grade = pd.to_numeric(df['overallufmgrade'], errors='coerce')
            measured = int(((grade.isin([1, 2, 3, 4, 5])) & df['barcode'].notnull()).sum())
            if 'scrap_category_tub' in df.columns:
                vi_scrap = int(((df['scrap_category_tub'] == "VISUAL") & df['scrap_category_tub'].notna()).sum())
            else:
                vi_scrap = 0
            denominator = measured + vi_scrap
            numerator = int(grade.isin([1, 2, 3]).sum())
            return numerator, denominator
        except Exception:
            return 0, 0

    def calculate_yields_from_barcodes(barcodes, total_bc, machine_obj):
        """Calculate yields for the given machine using ONLY this lot's barcodes.

        The barcodes passed in are intersected with the lot barcodes (machine_obj['barcodes'])
        so a machine never pulls tyres from other lots/SKUs. Every yield is computed with the
        exact canonical formula (grade>0 & grade<=5 for denominator, grade in [1,2,3] for
        numerator; overall_* use vi_yield / scrap_category_tub logic).
        """
        # Restrict to this lot's barcodes only
        lot_barcodes = [str(b) for b in (machine_obj.get('barcodes') or []) if b]
        machine_barcodes = [str(b) for b in (barcodes or []) if b]
        if lot_barcodes:
            lot_set = set(lot_barcodes)
            scoped = [b for b in machine_barcodes if b in lot_set]
            # if the machine-specific list didn't intersect, fall back to the lot barcodes
            query_barcodes = scoped if scoped else lot_barcodes
        else:
            query_barcodes = machine_barcodes

        if not query_barcodes:
            return calculate_yields_from_aggregated_data(machine_obj, total_bc)

        try:
            _mc = MongoClient(MONGO_URI)
            _db = _mc[TUB_CIMCORP_VISUAL_MERGED_DB_alert]
            _coll = _db[DATA_MERGED_SUMMARY_COLLECTION_alert]

            projection = {
                "_id": 0, "barcode": 1, "last": 1,
                "rfppwcgrade": 1, "rfh1wcgrade": 1, "lfppwcgrade": 1,
                "conygrade": 1, "lrograde": 1, "sbalgrade": 1,
                "uppergrade": 1, "lowergrade": 1, "bulwcgrade": 1, "depwcgrade": 1,
                "overallufmgrade": 1, "vi_yield": 1, "scrap_category_tub": 1,
            }
            # A barcode can have multiple UFM test records (re-runs). Only the
            # latest test per barcode is marked last == 1. Restricting to it keeps
            # one row per barcode so denominators never exceed the lot size.
            docs = list(_coll.find(
                {"barcode": {"$in": query_barcodes}, "last": 1},
                projection
            ))
            _mc.close()
        except Exception as e:
            print(f"[ERROR] Failed to get specific barcode data: {e}")
            return calculate_yields_from_aggregated_data(machine_obj, total_bc)

        # Fallback: if the last==1 filter returned nothing (e.g. older records
        # written before the 'last' flag existed), re-query without it and
        # deduplicate to one row per barcode below.
        if not docs:
            try:
                _mc = MongoClient(MONGO_URI)
                _coll = _mc[TUB_CIMCORP_VISUAL_MERGED_DB_alert][DATA_MERGED_SUMMARY_COLLECTION_alert]
                docs = list(_coll.find({"barcode": {"$in": query_barcodes}}, projection))
                _mc.close()
            except Exception as e:
                print(f"[ERROR] Fallback barcode fetch failed: {e}")

        if not docs:
            return calculate_yields_from_aggregated_data(machine_obj, total_bc)

        df = pd.DataFrame(docs)

        # Safety net: guarantee one row per barcode. Prefer the record flagged
        # last == 1; otherwise keep the first occurrence.
        if 'barcode' in df.columns and df['barcode'].duplicated().any():
            if 'last' in df.columns:
                df['_last_pref'] = (pd.to_numeric(df['last'], errors='coerce') == 1).astype(int)
                df = (df.sort_values('_last_pref', ascending=False)
                        .drop_duplicates(subset='barcode', keep='first')
                        .drop(columns=['_last_pref']))
            else:
                df = df.drop_duplicates(subset='barcode', keep='first')

        # Per-parameter yields (canonical grade formula)
        rfv_num, rfv_den = _grade_yield_counts(df, 'rfppwcgrade')
        rfh1_num, rfh1_den = _grade_yield_counts(df, 'rfh1wcgrade')
        lfv_num, lfv_den = _grade_yield_counts(df, 'lfppwcgrade')
        conicity_num, conicity_den = _grade_yield_counts(df, 'conygrade')
        static_num, static_den = _grade_yield_counts(df, 'sbalgrade')
        upper_num, upper_den = _grade_yield_counts(df, 'uppergrade')
        lower_num, lower_den = _grade_yield_counts(df, 'lowergrade')
        bulge_num, bulge_den = _grade_yield_counts(df, 'bulwcgrade')
        dent_num, dent_den = _grade_yield_counts(df, 'depwcgrade')

        # Overall yields (canonical overall_* formulas)
        wh_num, wh_den = _overall_warehouse_counts(df)
        uni_num, uni_den = _overall_uniformity_counts(df)
        vis_num, vis_den = _overall_visual_counts(df)

        return {
            'wh': (yield_percentage(wh_num, wh_den), wh_num, wh_den),
            'uni': (yield_percentage(uni_num, uni_den), uni_num, uni_den),
            'vis': (yield_percentage(vis_num, vis_den), vis_num, vis_den),
            'rfv': (yield_percentage(rfv_num, rfv_den), rfv_num, rfv_den),
            'rfh1': (yield_percentage(rfh1_num, rfh1_den), rfh1_num, rfh1_den),
            'lfv': (yield_percentage(lfv_num, lfv_den), lfv_num, lfv_den),
            'conicity': (yield_percentage(conicity_num, conicity_den), conicity_num, conicity_den),
            'static': (yield_percentage(static_num, static_den), static_num, static_den),
            'upper': (yield_percentage(upper_num, upper_den), upper_num, upper_den),
            'lower': (yield_percentage(lower_num, lower_den), lower_num, lower_den),
            'bulge': (yield_percentage(bulge_num, bulge_den), bulge_num, bulge_den),
            'dent': (yield_percentage(dent_num, dent_den), dent_num, dent_den),
        }

    def calculate_yields_from_aggregated_data(machine_obj, total_bc):
        """Calculate yields from aggregated machine_obj data"""
        # Initialize with zeros
        wh_percent, wh_num, wh_den = 0.0, 0, total_bc if total_bc > 0 else 1
        uni_percent, uni_num, uni_den = 0.0, 0, total_bc if total_bc > 0 else 1
        vis_percent, vis_num, vis_den = 0.0, 0, total_bc if total_bc > 0 else 1
        rfv_percent, rfv_num, rfv_den = 0.0, 0, total_bc if total_bc > 0 else 1
        rfh1_percent, rfh1_num, rfh1_den = 0.0, 0, total_bc if total_bc > 0 else 1
        lfv_percent, lfv_num, lfv_den = 0.0, 0, total_bc if total_bc > 0 else 1
        conicity_percent, conicity_num, conicity_den = 0.0, 0, total_bc if total_bc > 0 else 1
        static_percent, static_num, static_den = 0.0, 0, total_bc if total_bc > 0 else 1
        upper_percent, upper_num, upper_den = 0.0, 0, total_bc if total_bc > 0 else 1
        lower_percent, lower_num, lower_den = 0.0, 0, total_bc if total_bc > 0 else 1
        bulge_percent, bulge_num, bulge_den = 0.0, 0, total_bc if total_bc > 0 else 1
        dent_percent, dent_num, dent_den = 0.0, 0, total_bc if total_bc > 0 else 1
        
        # Get parameter_alerts from machine_obj
        param_alerts = machine_obj.get('parameter_alerts', [])
        
        # Also check lot_averages for yields
        lot_averages = machine_obj.get('lot_averages', {})
        
        for param in param_alerts:
            param_name = param.get('parameter')
            current_value = param.get('current_value')
            rank_counts = param.get('rank_counts', {})
            
            # If rank_counts exists, use it to calculate numerator and denominator
            if rank_counts:
                num, den = get_yield_from_rank_counts(rank_counts, total_bc)
                
                if param_name == 'rfv_yield':
                    rfv_num, rfv_den = num, den
                    rfv_percent = yield_percentage(rfv_num, rfv_den)
                elif param_name == 'rfh1_yield':
                    rfh1_num, rfh1_den = num, den
                    rfh1_percent = yield_percentage(rfh1_num, rfh1_den)
                elif param_name == 'lfv_yield':
                    lfv_num, lfv_den = num, den
                    lfv_percent = yield_percentage(lfv_num, lfv_den)
                elif param_name == 'conicity_yield':
                    conicity_num, conicity_den = num, den
                    conicity_percent = yield_percentage(conicity_num, conicity_den)
                elif param_name == 'static_yield':
                    static_num, static_den = num, den
                    static_percent = yield_percentage(static_num, static_den)
                elif param_name == 'upper_yield':
                    upper_num, upper_den = num, den
                    upper_percent = yield_percentage(upper_num, upper_den)
                elif param_name == 'lower_yield':
                    lower_num, lower_den = num, den
                    lower_percent = yield_percentage(lower_num, lower_den)
                elif param_name == 'bulge_yield':
                    bulge_num, bulge_den = num, den
                    bulge_percent = yield_percentage(bulge_num, bulge_den)
                elif param_name == 'dent_yield':
                    dent_num, dent_den = num, den
                    dent_percent = yield_percentage(dent_num, dent_den)
                elif param_name == 'overall_warehouse_yield':
                    wh_num, wh_den = num, den
                    wh_percent = yield_percentage(wh_num, wh_den)
                elif param_name == 'overall_uniformity_yield':
                    uni_num, uni_den = num, den
                    uni_percent = yield_percentage(uni_num, uni_den)
                elif param_name == 'overall_visual_yield':
                    vis_num, vis_den = num, den
                    vis_percent = yield_percentage(vis_num, vis_den)
            else:
                # If no rank_counts, try to use current_value from lot_averages
                if param_name in lot_averages:
                    value = lot_averages.get(param_name)
                    if value is not None:
                        # We only have percentage, not numerator/denominator
                        # Use total_bc as denominator and calculate numerator
                        den = total_bc if total_bc > 0 else 1
                        num = int(round((value / 100) * den)) if value else 0
                        
                        if param_name == 'rfv_yield':
                            rfv_percent = float(value) if value else 0.0
                            rfv_num, rfv_den = num, den
                        elif param_name == 'rfh1_yield':
                            rfh1_percent = float(value) if value else 0.0
                            rfh1_num, rfh1_den = num, den
                        elif param_name == 'lfv_yield':
                            lfv_percent = float(value) if value else 0.0
                            lfv_num, lfv_den = num, den
                        elif param_name == 'conicity_yield':
                            conicity_percent = float(value) if value else 0.0
                            conicity_num, conicity_den = num, den
                        elif param_name == 'static_yield':
                            static_percent = float(value) if value else 0.0
                            static_num, static_den = num, den
                        elif param_name == 'upper_yield':
                            upper_percent = float(value) if value else 0.0
                            upper_num, upper_den = num, den
                        elif param_name == 'lower_yield':
                            lower_percent = float(value) if value else 0.0
                            lower_num, lower_den = num, den
                        elif param_name == 'bulge_yield':
                            bulge_percent = float(value) if value else 0.0
                            bulge_num, bulge_den = num, den
                        elif param_name == 'dent_yield':
                            dent_percent = float(value) if value else 0.0
                            dent_num, dent_den = num, den
                        elif param_name == 'overall_warehouse_yield':
                            wh_percent = float(value) if value else 0.0
                            wh_num, wh_den = num, den
                        elif param_name == 'overall_uniformity_yield':
                            uni_percent = float(value) if value else 0.0
                            uni_num, uni_den = num, den
                        elif param_name == 'overall_visual_yield':
                            vis_percent = float(value) if value else 0.0
                            vis_num, vis_den = num, den
        
        # If we still don't have values, try lot_averages directly
        if rfv_num == 0 and rfv_den == total_bc:
            for key, value in lot_averages.items():
                if key == 'rfv_yield' and value is not None:
                    rfv_percent = float(value)
                    rfv_num = int(round((rfv_percent / 100) * total_bc)) if total_bc > 0 else 0
                    rfv_den = total_bc if total_bc > 0 else 1
                elif key == 'rfh1_yield' and value is not None:
                    rfh1_percent = float(value)
                    rfh1_num = int(round((rfh1_percent / 100) * total_bc)) if total_bc > 0 else 0
                    rfh1_den = total_bc if total_bc > 0 else 1
                elif key == 'lfv_yield' and value is not None:
                    lfv_percent = float(value)
                    lfv_num = int(round((lfv_percent / 100) * total_bc)) if total_bc > 0 else 0
                    lfv_den = total_bc if total_bc > 0 else 1
                elif key == 'conicity_yield' and value is not None:
                    conicity_percent = float(value)
                    conicity_num = int(round((conicity_percent / 100) * total_bc)) if total_bc > 0 else 0
                    conicity_den = total_bc if total_bc > 0 else 1
                elif key == 'static_yield' and value is not None:
                    static_percent = float(value)
                    static_num = int(round((static_percent / 100) * total_bc)) if total_bc > 0 else 0
                    static_den = total_bc if total_bc > 0 else 1
                elif key == 'upper_yield' and value is not None:
                    upper_percent = float(value)
                    upper_num = int(round((upper_percent / 100) * total_bc)) if total_bc > 0 else 0
                    upper_den = total_bc if total_bc > 0 else 1
                elif key == 'lower_yield' and value is not None:
                    lower_percent = float(value)
                    lower_num = int(round((lower_percent / 100) * total_bc)) if total_bc > 0 else 0
                    lower_den = total_bc if total_bc > 0 else 1
                elif key == 'bulge_yield' and value is not None:
                    bulge_percent = float(value)
                    bulge_num = int(round((bulge_percent / 100) * total_bc)) if total_bc > 0 else 0
                    bulge_den = total_bc if total_bc > 0 else 1
                elif key == 'dent_yield' and value is not None:
                    dent_percent = float(value)
                    dent_num = int(round((dent_percent / 100) * total_bc)) if total_bc > 0 else 0
                    dent_den = total_bc if total_bc > 0 else 1
                elif key == 'overall_warehouse_yield' and value is not None:
                    wh_percent = float(value)
                    wh_num = int(round((wh_percent / 100) * total_bc)) if total_bc > 0 else 0
                    wh_den = total_bc if total_bc > 0 else 1
                elif key == 'overall_uniformity_yield' and value is not None:
                    uni_percent = float(value)
                    uni_num = int(round((uni_percent / 100) * total_bc)) if total_bc > 0 else 0
                    uni_den = total_bc if total_bc > 0 else 1
                elif key == 'overall_visual_yield' and value is not None:
                    vis_percent = float(value)
                    vis_num = int(round((vis_percent / 100) * total_bc)) if total_bc > 0 else 0
                    vis_den = total_bc if total_bc > 0 else 1
        
        return {
            'wh': (wh_percent, wh_num, wh_den),
            'uni': (uni_percent, uni_num, uni_den),
            'vis': (vis_percent, vis_num, vis_den),
            'rfv': (rfv_percent, rfv_num, rfv_den),
            'rfh1': (rfh1_percent, rfh1_num, rfh1_den),
            'lfv': (lfv_percent, lfv_num, lfv_den),
            'conicity': (conicity_percent, conicity_num, conicity_den),
            'static': (static_percent, static_num, static_den),
            'upper': (upper_percent, upper_num, upper_den),
            'lower': (lower_percent, lower_num, lower_den),
            'bulge': (bulge_percent, bulge_num, bulge_den),
            'dent': (dent_percent, dent_num, dent_den)
        }

    def _fmt(val):
        try:
            return f"{float(val):.2f}%" if val not in (None, 'N/A') else "N/A"
        except (TypeError, ValueError):
            return "N/A"

    def _color(actual, spec):
        try:
            if actual is None or spec is None:
                return "none"
            return "red" if float(actual) < float(spec) else "green"
        except (TypeError, ValueError):
            return "none"

    def _ind(color_tag):
        if color_tag == "green": return "✅"
        if color_tag == "red":   return "🔴"
        return ""

    def _fmt_count(count):
        if count is None:
            return "N/A"
        try:
            return str(int(count))
        except (TypeError, ValueError):
            return "N/A"

    alert_machine_entries = []

    for alert in docs:
        for machine in alert.get("machine_alerts", []):
            raw_sizecode = machine.get('sizecode', 'N/A')
            sizecode = raw_sizecode if raw_sizecode != 'N/A' else sanitize_text(raw_sizecode)
            raw_gt_code = machine.get('gt_code', 'N/A')
            gt_code = raw_gt_code if raw_gt_code != 'N/A' else sanitize_text(raw_gt_code)
            param_alerts = machine.get('parameter_alerts', [])

            entry = {
                'sku': sanitize_text(alert.get('sku', 'N/A')),
                'machine_name': sanitize_text(machine.get('machine', 'N/A')),
                'sizecode': sizecode,
                'gt_code': gt_code,
                'params': [],
                'machine_obj': machine,
                'alert_obj': alert,
            }

            for param in param_alerts:
                param_name = param.get('parameter')
                current_val = param.get('current_value')
                shift_threshold = param.get('shift_occurred_thershold', False)
                shift_spec = param.get('shift_occurred_spec', False)
                rank_counts = param.get('rank_counts', {})

                if shift_threshold or shift_spec:
                    if param_name.endswith('_yield'):
                        display_name = param_name.replace('_yield', ' yield')
                    else:
                        display_name = param_name

                    if current_val == 0 or (isinstance(current_val, (int, float)) and current_val == 0):
                        rank_str = ", ".join([
                            f"{k.rsplit('_', 1)[0]}->{k.rsplit('_', 1)[1]}:{v}(count)"
                            if '_' in k else f"{k}->{v}"
                            for k, v in rank_counts.items()
                        ]) if rank_counts else "No ranks"
                        deviated_param = f"{display_name}({rank_str})"
                    else:
                        deviated_param = display_name

                    entry['params'].append(deviated_param)

            alert_machine_entries.append(entry)

    if alert_type.upper() == "TBM":
        title_str = "Alert: TBM Alert Summary"
    elif alert_type.upper() == "CURING":
        title_str = "Alert: CURING Alert Summary"
    elif alert_type.upper() == "UFM":
        title_str = "Alert: UFM Alert Summary"
    else:
        title_str = "Alert Summary"

    markdown_table = html_table_to_markdown(summary_html)

    breakdown_blocks = []

    # Helper function to get barcodes for a specific curing machine
    def get_barcodes_for_curing_machine(machine_obj, cur_id):
        """
        Get the list of barcodes that went through a specific curing machine.
        Queries data_merged_summary_alerts_data by cur_id field — gives exact per-press barcodes.
        """
        try:
            sku = machine_obj.get('sku') or machine_obj.get('gt_code', '')
            _mc = MongoClient(MONGO_URI)
            _db = _mc[TUB_CIMCORP_VISUAL_MERGED_DB_alert]
            _coll = _db[DATA_MERGED_SUMMARY_COLLECTION_alert]

            lot_barcodes = [str(b) for b in (machine_obj.get('barcodes') or []) if b]
            query = {"cur_id": cur_id}
            # Restrict to this lot's barcodes so we never pull other lots/SKUs
            if lot_barcodes:
                query["barcode"] = {"$in": lot_barcodes}

            docs = list(_coll.find(query, {"barcode": 1, "_id": 0}))
            _mc.close()

            barcodes = [str(d["barcode"]) for d in docs if d.get("barcode")]
            if barcodes:
                return barcodes
        except Exception as e:
            print(f"[WARN] get_barcodes_for_curing_machine failed for {cur_id}: {e}")

        # Fallback: use the main barcodes list (less accurate)
        return machine_obj.get('barcodes', [])

    # Helper function to get barcodes for a specific TBM machine
    def get_barcodes_for_tbm_machine(machine_obj, tbm_id):
        """
        Get the list of barcodes that went through a specific TBM machine.
        Queries data_merged_summary_alerts_data by ptb_id field.
        """
        try:
            _mc = MongoClient(MONGO_URI)
            _db = _mc[TUB_CIMCORP_VISUAL_MERGED_DB_alert]
            _coll = _db[DATA_MERGED_SUMMARY_COLLECTION_alert]

            lot_barcodes = [str(b) for b in (machine_obj.get('barcodes') or []) if b]
            query = {"ptb_id": tbm_id}
            # Restrict to this lot's barcodes so we never pull other lots/SKUs
            if lot_barcodes:
                query["barcode"] = {"$in": lot_barcodes}

            docs = list(_coll.find(query, {"barcode": 1, "_id": 0}))
            _mc.close()

            barcodes = [str(d["barcode"]) for d in docs if d.get("barcode")]
            if barcodes:
                return barcodes
        except Exception as e:
            print(f"[WARN] get_barcodes_for_tbm_machine failed for {tbm_id}: {e}")

        # Fallback: use the main barcodes list
        return machine_obj.get('barcodes', [])

    for entry in alert_machine_entries:
        sku         = entry['sku']
        machine_obj = entry['machine_obj']
        alert_obj   = entry['alert_obj']
        _at         = alert_type.upper()

        total_bc  = machine_obj.get('processed_machine_barcodes', 0)
        sku_specs = yield_spec_lookup.get(str(sku).strip(), {})
        spec_wh   = sku_specs.get('spec_overall_warehouse_yield')
        spec_uni  = sku_specs.get('spec_overall_uniformity_yield')
        spec_vis  = sku_specs.get('spec_overall_visual_yield')
        spec_rfv  = sku_specs.get('spec_rfv_yield')
        spec_rfh1 = sku_specs.get('spec_rfh1_yield')
        spec_lfv  = sku_specs.get('spec_lfv_yield')
        spec_conicity = sku_specs.get('spec_conicity_yield')
        spec_static   = sku_specs.get('spec_static_yield')
        spec_upper    = sku_specs.get('spec_upper_yield')
        spec_lower    = sku_specs.get('spec_lower_yield')
        spec_bulge    = sku_specs.get('spec_bulge_yield')
        spec_dent     = sku_specs.get('spec_dent_yield')

        entry_rows = []

        def _add_row(mname, mtype, bc, mach_obj, specific_barcodes=None):
            """Add a row with yields calculated from machine_obj"""
            # Calculate yields directly from machine_obj
            yields = calculate_yields_from_machine_obj(mach_obj, bc, specific_barcodes)
            
            wh_percent, wh_num, wh_den = yields['wh']
            uni_percent, uni_num, uni_den = yields['uni']
            vis_percent, vis_num, vis_den = yields['vis']
            rfv_percent, rfv_num, rfv_den = yields['rfv']
            rfh1_percent, rfh1_num, rfh1_den = yields['rfh1']
            lfv_percent, lfv_num, lfv_den = yields['lfv']
            conicity_percent, conicity_num, conicity_den = yields['conicity']
            static_percent, static_num, static_den = yields['static']
            upper_percent, upper_num, upper_den = yields['upper']
            lower_percent, lower_num, lower_den = yields['lower']
            bulge_percent, bulge_num, bulge_den = yields['bulge']
            dent_percent, dent_num, dent_den = yields['dent']
            
            entry_rows.append({
                'mname': mname,
                'mtype': mtype,
                'bc': bc,
                'wh': (wh_percent, wh_num, wh_den),
                'uni': (uni_percent, uni_num, uni_den),
                'vis': (vis_percent, vis_num, vis_den),
                'rfv': (rfv_percent, rfv_num, rfv_den),
                'rfh1': (rfh1_percent, rfh1_num, rfh1_den),
                'lfv': (lfv_percent, lfv_num, lfv_den),
                'conicity': (conicity_percent, conicity_num, conicity_den),
                'static': (static_percent, static_num, static_den),
                'upper': (upper_percent, upper_num, upper_den),
                'lower': (lower_percent, lower_num, lower_den),
                'bulge': (bulge_percent, bulge_num, bulge_den),
                'dent': (dent_percent, dent_num, dent_den),
                'wh_color': _color(wh_percent, spec_wh),
                'uni_color': _color(uni_percent, spec_uni),
                'vis_color': _color(vis_percent, spec_vis),
                'rfv_color': _color(rfv_percent, spec_rfv),
                'rfh1_color': _color(rfh1_percent, spec_rfh1),
                'lfv_color': _color(lfv_percent, spec_lfv),
                'conicity_color': _color(conicity_percent, spec_conicity),
                'static_color': _color(static_percent, spec_static),
                'upper_color': _color(upper_percent, spec_upper),
                'lower_color': _color(lower_percent, spec_lower),
                'bulge_color': _color(bulge_percent, spec_bulge),
                'dent_color': _color(dent_percent, spec_dent),
            })

        # Build rows based on alert type
        if _at == "UFM":
            # UFM row - compute from this lot's own barcodes so num/den are real
            # counts (consistent with the machine breakdown rows) using the
            # canonical yield formulas.
            _add_row(
                sanitize_text(machine_obj.get('machine', 'N/A')), 
                "UFM", 
                int(total_bc) if total_bc else 0,
                machine_obj,
                machine_obj.get('barcodes', [])  # this lot's barcodes only
            )
            
            # Curing rows - get specific barcodes for each curing machine
            for c in machine_obj.get('cur_ids', []):
                bc = c.get('barcode', 0)
                cur_id = c.get('cur_id', 'N/A')
                
                # Get the barcodes that went through this curing machine
                cur_barcodes = get_barcodes_for_curing_machine(machine_obj, cur_id)
                
                # If we couldn't get specific barcodes, use the main barcodes list
                if not cur_barcodes:
                    cur_barcodes = machine_obj.get('barcodes', [])
                
                curing_obj = machine_obj.copy()
                curing_obj['processed_machine_barcodes'] = bc
                _add_row(
                    sanitize_text(str(cur_id)), 
                    "Curing", 
                    int(bc),
                    curing_obj,
                    cur_barcodes  # Pass specific barcodes for this curing machine
                )
            
            # TBM rows - get specific barcodes for each TBM machine
            tbm_ids_raw = machine_obj.get('tbm_ids', [])
            tbm_machine = sanitize_text(machine_obj.get('tbm_machine', 'N/A'))
            if tbm_ids_raw:
                for t in tbm_ids_raw:
                    bc = t.get('barcode', 0)
                    tbm_id = t.get('tbm_id', 'N/A')
                    
                    # Get the barcodes that went through this TBM machine
                    tbm_barcodes = get_barcodes_for_tbm_machine(machine_obj, tbm_id)
                    
                    # If we couldn't get specific barcodes, use the main barcodes list
                    if not tbm_barcodes:
                        tbm_barcodes = machine_obj.get('barcodes', [])
                    
                    tbm_obj = machine_obj.copy()
                    tbm_obj['processed_machine_barcodes'] = bc
                    _add_row(
                        sanitize_text(str(tbm_id)), 
                        "TBM", 
                        int(bc),
                        tbm_obj,
                        tbm_barcodes  # Pass specific barcodes for this TBM machine
                    )
            elif tbm_machine and tbm_machine != 'N/A':
                # If no specific TBM IDs, use the main TBM machine with all barcodes
                tbm_barcodes = machine_obj.get('barcodes', [])
                tbm_obj = machine_obj.copy()
                tbm_obj['processed_machine_barcodes'] = total_bc
                _add_row(
                    tbm_machine, 
                    "TBM", 
                    int(total_bc) if total_bc else 0,
                    tbm_obj,
                    tbm_barcodes
                )

        elif _at == "TBM":
            # TBM row - use aggregated data
            _add_row(
                sanitize_text(machine_obj.get('machine', 'N/A')), 
                "TBM", 
                int(total_bc) if total_bc else 0,
                machine_obj,
                None
            )
            
            # UFM rows
            seen_ubno = set()
            for ma in alert_obj.get("machine_alerts", []):
                for rec in ma.get("grouped_averages", {}).get("uf_id", []):
                    uf = sanitize_text(rec.get('uf_id', ''))
                    bc = rec.get('barcode_count', 0)
                    if uf and uf not in seen_ubno:
                        seen_ubno.add(uf)
                        # Get barcodes for this UFM machine
                        ufm_barcodes = machine_obj.get('barcodes', [])
                        ufm_obj = machine_obj.copy()
                        ufm_obj['processed_machine_barcodes'] = bc
                        _add_row(
                            uf, 
                            "UFM", 
                            int(bc),
                            ufm_obj,
                            ufm_barcodes
                        )
            
            # Curing rows
            for c in machine_obj.get('cur_ids', []):
                bc = c.get('barcode', 0)
                cur_id = c.get('cur_id', 'N/A')
                cur_barcodes = get_barcodes_for_curing_machine(machine_obj, cur_id)
                if not cur_barcodes:
                    cur_barcodes = machine_obj.get('barcodes', [])
                cur_obj = machine_obj.copy()
                cur_obj['processed_machine_barcodes'] = bc
                _add_row(
                    sanitize_text(str(cur_id)), 
                    "Curing", 
                    int(bc),
                    cur_obj,
                    cur_barcodes
                )

        elif _at == "CURING":
            # Curing row - use aggregated data
            _add_row(
                sanitize_text(machine_obj.get('machine', 'N/A')), 
                "Curing", 
                int(total_bc) if total_bc else 0,
                machine_obj,
                None
            )
            
            # UFM rows
            seen_ubno = set()
            for ma in alert_obj.get("machine_alerts", []):
                for rec in ma.get("grouped_averages", {}).get("uf_id", []):
                    uf = sanitize_text(rec.get('uf_id', ''))
                    bc = rec.get('barcode_count', 0)
                    if uf and uf not in seen_ubno:
                        seen_ubno.add(uf)
                        ufm_barcodes = machine_obj.get('barcodes', [])
                        ufm_obj = machine_obj.copy()
                        ufm_obj['processed_machine_barcodes'] = bc
                        _add_row(
                            uf, 
                            "UFM", 
                            int(bc),
                            ufm_obj,
                            ufm_barcodes
                        )
            
            # TBM rows
            for p in machine_obj.get('ptb_ids', []):
                bc = p.get('barcode', 0)
                ptb_id = p.get('ptb_id', 'N/A')
                tbm_barcodes = machine_obj.get('barcodes', [])
                tbm_obj = machine_obj.copy()
                tbm_obj['processed_machine_barcodes'] = bc
                _add_row(
                    sanitize_text(str(ptb_id)), 
                    "TBM", 
                    int(bc),
                    tbm_obj,
                    tbm_barcodes
                )

        if not entry_rows:
            continue

        primary_machine = sanitize_text(machine_obj.get('machine', 'N/A'))

        html_table = f"<p><strong>SKU: {sku} | Machine: {primary_machine}</strong></p>"
        html_table += "<table border='1' cellpadding='5' cellspacing='0' style='border-collapse: collapse; width: 100%;'>"
        html_table += "<thead><tr style='background-color: #4A90E2; color: white;'>"
        html_table += "<th>Machine</th><th>Machine Type</th><th>Total Tyres</th>"
        html_table += "<th>WH Num</th><th>WH Den</th><th>WAREHOUSE Yield %</th>"
        html_table += "<th>UNI Num</th><th>UNI Den</th><th>UNIFORMITY Yield %</th>"
        html_table += "<th>VIS Num</th><th>VIS Den</th><th>VISUAL Yield %</th>"
        html_table += "<th>RFV Num</th><th>RFV Den</th><th>RFV Yield %</th>"
        html_table += "<th>RFH1 Num</th><th>RFH1 Den</th><th>RFH1 Yield %</th>"
        html_table += "<th>LFV Num</th><th>LFV Den</th><th>LFV Yield %</th>"
        html_table += "<th>CON Num</th><th>CON Den</th><th>Conicity Yield %</th>"
        html_table += "<th>STA Num</th><th>STA Den</th><th>Static Yield %</th>"
        html_table += "<th>UPP Num</th><th>UPP Den</th><th>Upper Yield %</th>"
        html_table += "<th>LOW Num</th><th>LOW Den</th><th>Lower Yield %</th>"
        html_table += "<th>BUL Num</th><th>BUL Den</th><th>Bulge Yield %</th>"
        html_table += "<th>DEN Num</th><th>DEN Den</th><th>Dent Yield %</th>"
        html_table += "</tr></thead><tbody>"

        def _format_yield_cell(percent, num, den, spec_val):
            color = _color(percent, spec_val)
            if percent is None or percent == 0:
                return f"0.00% {_ind(color)}"
            return f"{_fmt(percent)} {_ind(color)}"

        for row in entry_rows:
            mname = row['mname']
            mtype = row['mtype']
            bc = row['bc']
            
            wh_percent, wh_num, wh_den = row['wh']
            uni_percent, uni_num, uni_den = row['uni']
            vis_percent, vis_num, vis_den = row['vis']
            rfv_percent, rfv_num, rfv_den = row['rfv']
            rfh1_percent, rfh1_num, rfh1_den = row['rfh1']
            lfv_percent, lfv_num, lfv_den = row['lfv']
            conicity_percent, conicity_num, conicity_den = row['conicity']
            static_percent, static_num, static_den = row['static']
            upper_percent, upper_num, upper_den = row['upper']
            lower_percent, lower_num, lower_den = row['lower']
            bulge_percent, bulge_num, bulge_den = row['bulge']
            dent_percent, dent_num, dent_den = row['dent']

            html_table += "<tr>"
            html_table += f"<td>{mname}</td><td>{mtype}</td><td>{bc}</td>"
            
            html_table += f"<td>{_fmt_count(wh_num)}</td><td>{_fmt_count(wh_den)}</td>"
            html_table += f"<td>{_format_yield_cell(wh_percent, wh_num, wh_den, spec_wh)}</td>"
            
            html_table += f"<td>{_fmt_count(uni_num)}</td><td>{_fmt_count(uni_den)}</td>"
            html_table += f"<td>{_format_yield_cell(uni_percent, uni_num, uni_den, spec_uni)}</td>"
            
            html_table += f"<td>{_fmt_count(vis_num)}</td><td>{_fmt_count(vis_den)}</td>"
            html_table += f"<td>{_format_yield_cell(vis_percent, vis_num, vis_den, spec_vis)}</td>"
            
            html_table += f"<td>{_fmt_count(rfv_num)}</td><td>{_fmt_count(rfv_den)}</td>"
            html_table += f"<td>{_format_yield_cell(rfv_percent, rfv_num, rfv_den, spec_rfv)}</td>"
            
            html_table += f"<td>{_fmt_count(rfh1_num)}</td><td>{_fmt_count(rfh1_den)}</td>"
            html_table += f"<td>{_format_yield_cell(rfh1_percent, rfh1_num, rfh1_den, spec_rfh1)}</td>"
            
            html_table += f"<td>{_fmt_count(lfv_num)}</td><td>{_fmt_count(lfv_den)}</td>"
            html_table += f"<td>{_format_yield_cell(lfv_percent, lfv_num, lfv_den, spec_lfv)}</td>"
            
            html_table += f"<td>{_fmt_count(conicity_num)}</td><td>{_fmt_count(conicity_den)}</td>"
            html_table += f"<td>{_format_yield_cell(conicity_percent, conicity_num, conicity_den, spec_conicity)}</td>"
            
            html_table += f"<td>{_fmt_count(static_num)}</td><td>{_fmt_count(static_den)}</td>"
            html_table += f"<td>{_format_yield_cell(static_percent, static_num, static_den, spec_static)}</td>"
            
            html_table += f"<td>{_fmt_count(upper_num)}</td><td>{_fmt_count(upper_den)}</td>"
            html_table += f"<td>{_format_yield_cell(upper_percent, upper_num, upper_den, spec_upper)}</td>"
            
            html_table += f"<td>{_fmt_count(lower_num)}</td><td>{_fmt_count(lower_den)}</td>"
            html_table += f"<td>{_format_yield_cell(lower_percent, lower_num, lower_den, spec_lower)}</td>"
            
            html_table += f"<td>{_fmt_count(bulge_num)}</td><td>{_fmt_count(bulge_den)}</td>"
            html_table += f"<td>{_format_yield_cell(bulge_percent, bulge_num, bulge_den, spec_bulge)}</td>"
            
            html_table += f"<td>{_fmt_count(dent_num)}</td><td>{_fmt_count(dent_den)}</td>"
            html_table += f"<td>{_format_yield_cell(dent_percent, dent_num, dent_den, spec_dent)}</td>"
            
            html_table += "</tr>"

        html_table += "</tbody></table>"
        breakdown_blocks.append(html_table)

    # Microsoft Teams incoming webhooks reject a message once the WHOLE JSON
    # payload exceeds ~28 KB (not just the visible text). The previous logic
    # measured only the HTML character length, so payloads still went over the
    # byte limit (JSON escaping + wrapper fields + multi-byte chars all add up),
    # and the SUMMARY table itself was never split. Fix: measure the real
    # serialized payload byte size, and split BOTH the summary and the machine
    # breakdown on row boundaries so every message stays safely under 28 KB.
    TEAMS_PAYLOAD_LIMIT = 28000                       # hard Teams limit (bytes)
    TEAMS_SAFE_BYTES = 24000                          # safety budget for the text body

    def _payload_bytes(text_body):
        """Actual serialized JSON size of the message we would POST."""
        payload = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "summary": title_str,
            "themeColor": "0076D7",
            "title": title_str,
            "text": text_body,
        }
        return len(json.dumps(payload).encode("utf-8"))

    def _post_teams(text_body, label):
        """Post one Teams message. Returns True only on a genuine success."""
        payload = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "summary": title_str,
            "themeColor": "0076D7",
            "title": title_str,
            "text": text_body,
        }
        try:
            resp = requests.post(
                teams_webhook_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
        except Exception as e:
            print(f"[ERROR] Exception sending Teams {label}: {e}")
            return False

        body = (resp.text or "").strip()
        # Teams returns "1" on success. Any HTTP error, or an error string in the
        # body (e.g. "payload is too large"), is a failure.
        body_indicates_error = ("error" in body.lower()) or ("too large" in body.lower())
        if resp.status_code in (200, 202) and not body_indicates_error:
            print(f"[INFO] Teams {label} sent successfully for {alert_type}")
            return True

        print(f"[ERROR] Failed to send Teams {label}: {resp.status_code} {body[:300]}")
        return False

    def _split_on_rows(html, header=""):
        """Split HTML into chunks whose FULL payload stays under the byte limit.
        Splits on </tr> boundaries so no table cell is broken. Falls back to a
        hard character split if a single row is still too large."""
        pieces = html.split("</tr>")
        chunks, current = [], ""
        for part in pieces:
            piece = part + "</tr>" if part.strip() else part
            candidate = current + piece
            if current and _payload_bytes(header + candidate) > TEAMS_SAFE_BYTES:
                chunks.append(current)
                current = piece
            else:
                current = candidate
        if current.strip():
            chunks.append(current)

        # Safety net: if any single chunk is still over the limit (e.g. one giant
        # row), hard-split it by characters.
        safe_chunks = []
        for ch in chunks:
            if _payload_bytes(header + ch) <= TEAMS_SAFE_BYTES:
                safe_chunks.append(ch)
            else:
                step = 8000
                for i in range(0, len(ch), step):
                    safe_chunks.append(ch[i:i + step])
        return safe_chunks

    def _batch_blocks(blocks, header_for_measure=""):
        """Group breakdown blocks into batches whose full payload stays under the
        byte limit. Multiple SKU/Machine blocks are combined into one message so
        we send a handful of batch messages instead of one per machine. A single
        block that is itself too large is split on row boundaries."""
        batches, current = [], []
        separator = "<br><br>"
        for block in blocks:
            # A single oversized block: flush current batch, then split it out.
            if _payload_bytes(header_for_measure + block) > TEAMS_SAFE_BYTES:
                if current:
                    batches.append(current)
                    current = []
                for chunk in _split_on_rows(block, header_for_measure):
                    batches.append([chunk])
                continue

            candidate = current + [block]
            candidate_text = separator.join(candidate)
            if current and _payload_bytes(header_for_measure + candidate_text) > TEAMS_SAFE_BYTES:
                batches.append(current)
                current = [block]
            else:
                current.append(block)
        if current:
            batches.append(current)
        return batches

    overall_ok = True

    # ── Summary table (split into parts if it is itself too large) ────────────
    summary_header = f"**{title_str}**\n\n"
    summary_chunks = _split_on_rows(markdown_table, summary_header) if _payload_bytes(summary_header + markdown_table) > TEAMS_SAFE_BYTES else [markdown_table]
    for s_idx, chunk in enumerate(summary_chunks, 1):
        label = "summary" if len(summary_chunks) == 1 else f"summary (part {s_idx}/{len(summary_chunks)})"
        ok = _post_teams(summary_header + chunk, label)
        overall_ok = overall_ok and ok

    # ── Batch-wise machine breakdown messages ─────────────────────────────────
    if breakdown_blocks:
        # Use a representative header for size measurement.
        batches = _batch_blocks(breakdown_blocks, header_for_measure="**Machine Breakdown — Batch 00/00**<br><br>")
        total_batches = len(batches)
        for b_idx, batch in enumerate(batches, 1):
            header = f"**Machine Breakdown — Batch {b_idx}/{total_batches}**<br><br>"
            body = "<br><br>".join(batch)
            ok = _post_teams(header + body, f"breakdown batch {b_idx}/{total_batches}")
            overall_ok = overall_ok and ok
    else:
        overall_ok = overall_ok and _post_teams(
            f"**Machine Breakdown**\n\n<p>No data available.</p>", "breakdown"
        )

    return overall_ok

def format_value(v, decimals=2):
    if v is None or pd.isna(v):
        return "N/A"
    try:
        return f"{float(v):.{decimals}f}"
    except:
        return "N/A"

def format_datetime(dt, fmt="%d-%m-%Y %I:%M:%S%p"):
    if not dt or dt == "":
        return None

    if isinstance(dt, dict) and "$date" in dt:
        dt = dt["$date"]

    try:
        dt_parsed = pd.to_datetime(dt, errors='coerce')
        if pd.isnull(dt_parsed):
            return None
        return dt_parsed.strftime(fmt)
    except Exception:
        return None
    
def format_shift_date(dt):
    if not dt or dt == "":
        return None
    if isinstance(dt, dict) and "$date" in dt:
        dt = dt["$date"]
    try:
        dt_parsed = pd.to_datetime(dt, errors='coerce')
        if pd.isnull(dt_parsed):
            return None
        return dt_parsed.strftime("%Y-%B-%d")
    except Exception:
        return None

# PDF class
class PDF(FPDF):
    def __init__(self):
        super().__init__('L','mm',(300,250))
        self.set_auto_page_break(True,15)
        self.set_margins(10,10,10)

    def footer(self):
        self.set_y(-15)
        self.set_font('Arial','I',8)
        self.cell(0,10,f'Page {self.page_no()}',0,0,'C')

    def section_banner(self, text):
        self.set_fill_color(238,174,193)
        self.set_text_color(0)
        self.set_font('Arial','B',10)
        self.cell(0,8,sanitize_text(text),0,1,'C',fill=True)
        self.ln(4)

    def grouped_parameter_table(self, headers, widths, group_defs, data, cols):
        self.set_fill_color(106, 13, 173)
        self.set_text_color(255)
        self.set_font('Arial', 'B', 9)
        
        def print_header():
            self.cell(widths[0], 7, "Stage", 1, 0, 'C', fill=True)
            for w, h in zip(widths[1:], headers):
                self.cell(w, 7, sanitize_text(h), 1, 0, 'C', fill=True)
            self.ln()

        print_header()
        self.set_text_color(0)

        row_h = 6
        for group_label, keys in group_defs:
            total_rows = len(keys)
            total_height = row_h * total_rows

            if self.get_y() + total_height + 10 > self.h - self.b_margin:
                self.add_page()
                print_header()
                self.set_text_color(0)
            
            y_start = self.get_y()
            
            for i, key in enumerate(keys):
                border = 'L'
                if i == 0:
                    border += 'T'
                if i == len(keys) - 1:
                    border += 'B'
                
                self.cell(widths[0], row_h, "", border)
                param_name = sanitize_text(key.replace('_', ' ').upper())
                self.cell(widths[1], row_h, param_name, 1)
                
                for col_name, w in zip(cols, widths[2:]):
                    val = data.get(key, {}).get(col_name)
                    vs = format_value(val)
                    hl = ((col_name == 'difference' and data.get(key, {}).get('shift_occurred_thershold')) or \
                        (col_name == 'spec_diff' and data.get(key, {}).get('shift_occurred_spec')))
                    if hl:
                        self.set_fill_color(255, 0, 0)
                        self.set_text_color(255)
                        self.cell(w, row_h, vs, 1, 0, 'C', fill=True)
                        self.set_fill_color(255)
                        self.set_text_color(0)
                    else:
                        self.cell(w, row_h, vs, 1, 0, 'C')
                self.ln()
            
            self.set_xy(self.l_margin, y_start)
            self.set_font('Arial', 'B', 9)
            self.cell(widths[0], total_height, sanitize_text(group_label), 'LR', 0, 'C')
            self.set_xy(self.l_margin, y_start + total_height)
            self.ln(2)
    
    def grouped_summary(self, machines):
        usable = self.w - self.l_margin - self.r_margin
        first  = usable * 0.40
        each = (usable - first) / (2 * len(machines))
        widths = [first] + [each, each] * len(machines)
        h = 6

        self.set_font('Arial','B',9)
        self.cell(widths[0],h,'UBNO Machine',1)
        for m in machines:
            self.cell(widths[1] + widths[2], h, sanitize_text(m['uf_id']), 1, 0, 'C')
        self.ln()

        self.set_font('Arial','',9)
        self.cell(widths[0], h, 'Start Time', 1)
        for m in machines:
            self.cell(widths[1] + widths[2], h, str(format_datetime(m['start'])), 1, 0, 'C')
        self.ln()

        self.cell(widths[0],h,'End Time',1)
        for m in machines:
            self.cell(widths[1] + widths[2], h, str(format_datetime(m['end'])), 1, 0, 'C')
        self.ln()

        self.cell(widths[0], h, 'Barcode Count', 1)
        for m in machines:
            self.cell(widths[1] + widths[2], h, str(m['barcode_count']), 1, 0, 'C')
        self.ln(5)

        self.set_fill_color(106,13,173)
        self.set_text_color(255)
        self.set_font('Arial','B',9)
        self.cell(widths[0], h, 'Parameters', 1, 0, 'C', fill=True)
        for _ in machines:
            self.cell(widths[1], h, 'SPEC', 1, 0, 'C', fill=True)
            self.cell(widths[2], h, 'AVG', 1, 0, 'C', fill=True)
        self.ln()
        self.set_text_color(0)

        for key in PARAM_KEYS:
            self.cell(widths[0], h, sanitize_text(key.replace('_',' ').upper()), 1)
            for m in machines:
                avg = format_value(m['averages'].get(f'avg_{key}') if f'avg_{key}' in m['averages'] else m['averages'].get(key))
                spec = format_value(m['specs'].get(key))
                self.cell(widths[1], h, spec, 1, 0, 'C')
                if m['parameter_map'].get(key):
                    self.set_fill_color(255,0,0)
                    self.set_text_color(255)
                    self.cell(widths[2], h, avg, 1, 0, 'C', fill=True)
                    self.set_fill_color(255)
                    self.set_text_color(0)
                else:
                    self.cell(widths[2], h, avg, 1, 0, 'C')
            self.ln()

        for grade in [3, 4, 5]:
            grade_key = f"overallufmgrade_{grade}"
            label     = f"OVERALL UFM GRADE {grade}"
            self.cell(widths[0], h, label, 1)
            for m in machines:
                avg  = format_value(m['averages'].get(grade_key))
                spec = format_value(m['specs'].get(grade_key))
                self.cell(widths[1], h, spec, 1, 0, 'C')
                if m['parameter_map'].get(grade_key):
                    self.set_fill_color(255,0,0)
                    self.set_text_color(255)
                    self.cell(widths[2], h, avg, 1, 0, 'C', fill=True)
                    self.set_fill_color(255)
                    self.set_text_color(0)
                else:
                    self.cell(widths[2], h, avg, 1, 0, 'C')
            self.ln()
        self.ln(5)

        if len(machines) > 1:
            barcode_width = usable / len(machines)
            self.set_fill_color(106, 13, 173)
            self.set_text_color(255)
            self.set_font('Arial', 'B', 9)
            for m in machines:
                self.cell(barcode_width, h, f"{sanitize_text(m['uf_id'])} Barcodes", 1, 0, 'C', fill=True)
            self.ln()
            self.set_text_color(0)
            self.set_font('Arial', '', 9)
            
            max_rows = max(len(m['barcodes']) // 2 + (1 if len(m['barcodes']) % 2 else 0) for m in machines)
            for row in range(max_rows):
                x_start = self.l_margin
                for m in machines:
                    self.set_x(x_start)
                    chunk_size = 2
                    start_idx = row * chunk_size
                    chunk = m['barcodes'][start_idx:start_idx + chunk_size]
                    barcode_str = ", ".join(sanitize_text(str(b)) for b in chunk if b is not None) if chunk else ""
                    self.cell(barcode_width, h, barcode_str, 1, 0, 'L')
                    x_start += barcode_width
                self.ln()
        else:
            m = machines[0]
            self.set_fill_color(106, 13, 173)
            self.set_text_color(255)
            self.set_font('Arial', 'B', 9)
            self.cell(usable, h, f"{sanitize_text(m['uf_id'])} Barcodes", 1, 0, 'C', fill=True)
            self.ln()
            self.set_text_color(0)
            self.set_font('Arial', '', 9)
            
            barcodes = m['barcodes']
            chunk_size = 5
            barcode_width = usable / chunk_size
            total_rows = len(barcodes) // chunk_size + (1 if len(barcodes) % chunk_size else 0)

            for row in range(total_rows):
                start_idx = row * chunk_size
                chunk = barcodes[start_idx:start_idx + chunk_size]
                for b in chunk:
                    self.cell(barcode_width, h, sanitize_text(str(b)) if b is not None else "", 1, 0, 'L')
                for _ in range(chunk_size - len(chunk)):
                    self.cell(barcode_width, h, "", 1, 0, 'L')
                self.ln()
    
    def barcodes_table(self, barcodes, width):
        self.set_fill_color(106, 13, 173)
        self.set_text_color(255)
        self.set_font('Arial', 'B', 9)
        self.cell(width, 7, "Barcodes", 1, 0, 'C', fill=True)
        self.ln()
        self.set_text_color(0)
        self.set_font('Arial', '', 9)

        row_h = 6
        if not barcodes or not isinstance(barcodes, list):
            self.cell(width, row_h, "No barcodes available", 1, 0, 'C')
            self.ln()
            return

        chunk_size = 5
        max_barcodes = 100
        for i in range(0, min(len(barcodes), max_barcodes), chunk_size):
            chunk = barcodes[i:i + chunk_size]
            barcode_str = ", ".join(sanitize_text(str(b)) for b in chunk if b is not None)
            self.cell(width, row_h, barcode_str, 1, 0, 'L')
            self.ln()

    def shift_data_table(self, shifts_data, machine_name, alert_type, width):
        self.set_fill_color(238, 174, 193)
        self.set_text_color(0)
        self.set_font('Arial', 'B', 10)
        self.cell(0, 8, "TBM SHIFT DATA", 0, 1, 'C', fill=True)
        self.ln(4)

        self.set_fill_color(106, 13, 173)
        self.set_text_color(255)
        self.set_font('Arial', 'B', 9)
        self.cell(width, 7, "TBM Machine", 1, 0, 'C', fill=True)
        self.ln()
        self.set_text_color(0)
        self.set_font('Arial', '', 9)
        self.cell(width, 6, sanitize_text(machine_name), 1, 0, 'C')
        self.ln(5)

        if not shifts_data or not isinstance(shifts_data, list):
            self.set_font('Arial', 'B', 9)
            self.cell(width, 6, "No shift data available", 1, 0, 'C')
            self.ln()
            return
                  
        shift_info = []
        for shift in shifts_data:
            shift_name = shift.get('shift', 'N/A')
            start_time = shift.get('machine_shift_tbm_start_time', None)
            barcodes = shift.get('barcodes', [])
            formatted_date = format_shift_date(start_time)
            if formatted_date and shift_name != 'N/A':
                shift_display = f"{formatted_date}/{shift_name}"
            else:
                shift_display = shift_name
            shift_info.append({
                'shift_display': shift_display,
                'barcodes': barcodes
            })

        shift_info = shift_info[:3]
        num_shifts = len(shift_info)
        if num_shifts == 0:
            self.set_font('Arial', 'B', 9)
            self.cell(width, 6, "No valid shift data available", 1, 0, 'C')
            self.ln()
            return

        col_width = width / max(num_shifts, 1)
        row_h = 6
        self.set_fill_color(106, 13, 173)
        self.set_text_color(255)
        self.set_font('Arial', 'B', 9)
        for shift in shift_info:
            self.cell(col_width, 7, "Shift", 1, 0, 'C', fill=True)
        self.ln()
        self.set_text_color(0)
        self.set_font('Arial', '', 9)
        for shift in shift_info:
            self.cell(col_width, row_h, sanitize_text(shift['shift_display']), 1, 0, 'C')
        self.ln(5)

        self.set_fill_color(106, 13, 173)
        self.set_text_color(255)
        self.set_font('Arial', 'B', 9)
        for _ in shift_info:
            self.cell(col_width, 7, "Barcodes", 1, 0, 'C', fill=True)
        self.ln()
        self.set_text_color(0)
        self.set_font('Arial', '', 9)

        max_barcodes = max(len(shift['barcodes']) for shift in shift_info) if shift_info else 0
        for row in range(max_barcodes):
            for shift in shift_info:
                barcode = shift['barcodes'][row] if row < len(shift['barcodes']) else ""
                self.cell(col_width, row_h, sanitize_text(str(barcode)), 1, 0, 'L')
            self.ln()

# PDF generation
def generate_alert_pdf(doc, atype):
    entry = doc['machine_alerts'][0]
    alert = {
        **entry,
        'sku': doc.get('sku', entry.get('sku', 'N/A')),
        'target_machine_lot_size': doc.get('target_machine_lot_size'),
        atype['alert_time_key']: doc.get(atype['alert_time_key'])
    }

    raw  = alert['parameter_alerts']
    data = {p['parameter']: p for p in raw}
    for p in data.values():
        c, s = p.get('current_value'), p.get('spec_value')
        p['spec_diff'] = float(c)-float(s) if c is not None and s is not None else None

    all_ufm      = PARAM_KEYS[:7]
    all_geometery = PARAM_KEYS[7:15]
    all_bal      = PARAM_KEYS[15:18]
    all_yields   = PARAM_KEYS[18:]
    ufm_keys      = [k for k in all_ufm if k in data]
    geometry_keys = [k for k in all_geometery if k in data]
    bal_keys      = [k for k in all_bal if k in data]
    yields_keys   = [k for k in all_yields if k in data]

    pdf = PDF()
    pdf.add_page()

    usable = pdf.w - pdf.l_margin - pdf.r_margin
    half   = usable / 2
    x1     = pdf.l_margin
    x2     = pdf.l_margin + half
    label_1=55
    value_1=half-label_1
    line_h = 6

    pdf.set_xy(x1, pdf.get_y())
    pdf.set_font('Arial','B',11)
    pdf.cell(half, line_h, f"{sanitize_text(atype['name'])} ALERT", 0, 0, 'L')

    raw_ts = alert.get(atype['alert_time_key'])

    if isinstance(raw_ts, dict) and '$date' in raw_ts:
        raw_ts = raw_ts['$date']

    ts_str = None
    if raw_ts:
        try:
            ts_parsed = pd.to_datetime(raw_ts, errors='coerce', utc=True)
            if not pd.isna(ts_parsed):
                ts_str = ts_parsed.strftime("%Y%m%d_%H%M%S")
            else:
                print(f"[DEBUG] Timestamp parsing resulted in NaT for raw_ts: {raw_ts}")
        except Exception as e:
            print(f"[DEBUG] Error parsing timestamp {raw_ts}: {e}")

    atype_name = sanitize_text(str(atype.get('name', 'UNKNOWN')))
    machine = sanitize_text(str(alert.get('machine', 'UNKNOWN')))
    gt_code = sanitize_text(str(alert.get('sku', 'UNKNOWN')))
    identifier = f":{atype_name}_{machine}_{gt_code}_{ts_str if ts_str else 'None'}"

    pdf.set_font('Arial','B',11)
    pdf.set_xy(x2, pdf.get_y())
    pdf.cell(value_1, line_h, sanitize_text(identifier), 0, 1, 'R')

    pdf.ln(2)

    label_w, h = 40, 6
    val_w = half - label_w
    y = pdf.get_y()

    pdf.set_xy(x1,y)
    pdf.set_font('Arial','B',11); pdf.cell(label_w,h,"SKU",0,0)
    pdf.set_font('Arial','',11);  pdf.cell(val_w,h,f": {sanitize_text(alert['sku'])}",0,0)
    pdf.set_xy(x2,y)
    pdf.set_font('Arial','B',11); pdf.cell(label_w,h,"Lot Size",0,0)
    pdf.set_font('Arial','',11);  pdf.cell(val_w,h,f": {sanitize_text(alert['processed_machine_barcodes'])}",0,1)

    y = pdf.get_y()
    pdf.set_xy(x1,y)
    pdf.set_font('Arial','B',11); pdf.cell(label_w,h,"Machine No",0,0)
    pdf.set_font('Arial','',11);  pdf.cell(val_w,h,f": {sanitize_text(alert['machine'])}",0,1)
    pdf.set_xy(x2,y)
    pdf.set_font('Arial','B',11); pdf.cell(label_w,h,"Size Code",0,0)
    pdf.set_font('Arial','',11);  pdf.cell(val_w,h,f": {sanitize_text(alert['sizecode'])}",0,1)

    y = pdf.get_y()
    pdf.set_xy(x1,y)
    pdf.set_font('Arial','B',11); pdf.cell(label_w,h,"Gt Code",0,0)
    pdf.set_font('Arial','',11);  pdf.cell(val_w,h,f": {sanitize_text(alert['gt_code'])}",0,1)

    y = pdf.get_y()
    pdf.set_xy(x1,y)
    pdf.set_font('Arial','B',11); pdf.cell(label_w,h,f"{sanitize_text(atype['name'])} Start Time",0,0)
    pdf.set_font('Arial','',11);  pdf.cell(val_w,h,f": {format_datetime(alert[atype['start_key']])}",0,0)
    pdf.set_xy(x2,y)
    pdf.set_font('Arial','B',11); pdf.cell(label_w,h,f"{sanitize_text(atype['name'])} End Time",0,0)
    pdf.set_font('Arial','',11);  pdf.cell(val_w,h,f": {format_datetime(alert[atype['end_key']])}",0,1)
    pdf.ln(5)

    pdf.section_banner(f"{sanitize_text(atype['name'])} STAGE")
    usable = pdf.w - pdf.l_margin - pdf.r_margin

    group_w = 25
    param_w = usable * 0.45 - group_w
    val_w = (usable - group_w - param_w) / 3
    widths = [group_w, param_w, val_w, val_w, val_w]

    ufm_keys      = [k for k in PARAM_KEYS[:10] if k in data]
    geometry_keys = [k for k in PARAM_KEYS[10:20] if k in data]
    bal_keys      = [k for k in PARAM_KEYS[20:23] if k in data]
    yields_keys   = [k for k in PARAM_KEYS[23:] if k in data]
    group_defs = [("UFM", ufm_keys), ("GEOMETRY", geometry_keys), ("BALANCING", bal_keys), ("YIELDS", yields_keys)]

    pdf.grouped_parameter_table(
        headers=["Parameters", "PREV", "CURRENT", "DIFF"],
        widths=widths,
        group_defs=group_defs,
        data=data,
        cols=['previous_value', 'current_value', 'difference']
    )

    pdf.ln(5)
    pdf.section_banner("SPECIFICATIONS")

    pdf.grouped_parameter_table(
        headers=["Parameters","SPEC","CURRENT","SPEC DIFF"],
        widths=widths,
        group_defs=[("UFM", ufm_keys), ("GEOMETRY", geometry_keys), ("BALANCING", bal_keys), ("YIELDS", yields_keys)],
        data=data,
        cols=['spec_value','current_value','spec_diff']
    )

    if atype['name'].upper() == "UFM":
        pdf.ln(10)
        pdf.section_banner("BARCODES")
        barcodes = alert.get('barcodes', [])
        pdf.barcodes_table(barcodes, usable)

    if atype['use_grouped']:
        raw_group = entry['grouped_averages']['uf_id']
        machines = []
        for rec in raw_group:
            param_map = {
                p['parameter']: p['spec_threshold_exceeded']
                for p in rec.get('parameter_list', [])
            }
            spec_map = {
                p['parameter']: p['spec_value']
                for p in rec.get('parameter_list', [])
            }
            averages = {
                p['parameter']: p['avg_value']
                for p in rec.get('parameter_list', [])
            }
            machines.append({
                'ubno': rec['uf_id'],
                'uf_id': rec['uf_id'],
                'start': rec['tub_date_start_time'],
                'end':   rec['tub_date_end_time'],
                'barcode_count': rec.get('barcode_count', 0),
                'averages': averages,
                'parameter_map': param_map,
                'specs':         spec_map,
                'barcodes': rec.get('barcodes', []),
            })

        pdf.section_banner(f"UFM STAGE")
        pdf.grouped_summary(machines)
        pdf.ln(10)
    
    pdf.ln(10)
    shift_data = alert.get(atype['shift_data_key'], [])
    machine_name = alert.get('tbm_machine' if atype['name'].upper() in ['CURING', 'UFM'] else 'machine', 'N/A')
    pdf.shift_data_table(shift_data, machine_name, atype['name'], usable)
                                 
    return pdf.output(dest='S')

def get_admin_emails(client):
    cols = client[alert_mail_db][alert_users]
    users = cols.find(
        {"is_alert_user": True},
        {"_id": 0, "email": 1}
    )
    emails = [u["email"] for u in users if u.get("email")]
    return emails

def send_alerts():
    client = MongoClient(current_app.config['MONGO_URI'])
    db = client[ALERT_DB_NAME]
    emails = get_admin_emails(client)
    
    if not emails:
        print("[ERROR] No admin emails")
        return

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    total_pdfs_processed = 0

    all_alerts = []
    for at in ALERT_TYPES:
        coll = db[at['coll']]
        docs = list(coll.find({'alert_flag_repl': False}))
        if docs:
            summary_html = generate_combined_summary_table(docs, at['name'])
            teams_ok = send_teams_alert(summary_html, at['name'], docs)
            if teams_ok:
                print(f"[INFO] Teams alert fully sent for {at['name']} ({len(docs)} alerts)")
            else:
                print(f"[ERROR] Teams alert incomplete for {at['name']} — one or more messages failed to send")
            for doc in docs:
                doc['alert_type'] = at
                all_alerts.append(doc)

    if not all_alerts:
        print("[INFO] No new alerts.")
        return

    alerts_by_modelno = {}
    for doc in all_alerts:
        modelno = sanitize_text(doc.get('sku', 'N/A'))
        if modelno not in alerts_by_modelno:
            alerts_by_modelno[modelno] = []
        alerts_by_modelno[modelno].append(doc)

    for modelno, docs in alerts_by_modelno.items():
        size_codes = sorted(set(sanitize_text(m.get('sizecode', 'N/A')) for d in docs for m in d.get('machine_alerts', [])))
        sizecode_str = ", ".join(size_codes) if size_codes else "N/A"

        pdf_list = []
        for doc in docs:
            at = doc['alert_type']
            pdf_bytes = generate_alert_pdf(doc, at)
            fn = f"{at['filename_prefix']}_{sanitize_text(doc['machine_alerts'][0]['machine'])}_{modelno}_{ts}.pdf"
            pdf_list.append({
                'bytes': pdf_bytes,
                'filename': fn,
                'doc_id': doc['_id'],
                'collection': at['coll'],
                'alert_type': at['name']
            })

        current_batch = []
        current_size = 0
        batch_number = 1
        batched_doc_ids = []

        for pdf in pdf_list:
            pdf_size = len(pdf['bytes'])
            pdf_size_encoded = pdf_size * 1.33
            email_overhead = len(json.dumps({
                "message": {
                    "subject": f"PCR GYO - Yield Alert: SKU - {modelno} | Size Code - {sizecode_str} (Batch {batch_number})",
                    "body": {"contentType": "Text", "content": f"Attached: Reports for {', '.join(sorted(set(p['alert_type'] for p in current_batch + [pdf])))}.\n\n"},
                    "toRecipients": [{"emailAddress": {"address": e}} for e in emails],
                    "attachments": []
                },
                "saveToSentItems": True
            }).encode('utf-8')) * 1.33

            if current_size + pdf_size_encoded + email_overhead > MAX_EMAIL_SIZE or len(current_batch) >= 180:
                if current_batch:
                    alert_types = sorted(set(p['alert_type'] for p in current_batch))
                    alert_types_str = ", ".join(alert_types)
                    endpoint = f"https://graph.microsoft.com/v1.0/users/{mail_config.shared_mailbox}/sendMail"
                    email_data = {
                        "message": {
                            "subject": f"PCR GYO - Yield Alert: SKU - {modelno} | Size Code - {sizecode_str} (Batch {batch_number})",
                            "body": {"contentType": "Text", "content": f"Attached: Reports for {alert_types_str}.\n\n"},
                            "toRecipients": [{"emailAddress": {"address": e}} for e in emails],
                            "attachments": [
                                {
                                    '@odata.type': '#microsoft.graph.fileAttachment',
                                    'Name': pdf_item['filename'],
                                    'ContentBytes': base64.b64encode(pdf_item['bytes'].encode('latin-1')).decode('utf-8')
                                } for pdf_item in current_batch
                            ]
                        },
                        "saveToSentItems": True
                    }
                    headers = {
                        "Authorization": f"Bearer {mail_config.access_token}",
                        "Content-Type": "application/json"
                    }
                    try:
                        resp = requests.post(endpoint, headers=headers, json=email_data)
                        if resp.status_code == 202:
                            print(f"[INFO] Batch {batch_number} emailed via Microsoft Graph successfully for SKU {modelno}.")
                            for pdf_item in current_batch:
                                db[pdf_item['collection']].update_one(
                                    {'_id': pdf_item['doc_id']},
                                    {'$set': {'alert_flag_repl': True, doc['alert_type']['alert_time_key']: datetime.now()}}
                                )
                            batched_doc_ids.extend([pdf_item['doc_id'] for pdf_item in current_batch])
                            total_pdfs_processed += len(current_batch)
                        else:
                            print(f"[ERROR] Graph API sendMail failed for batch {batch_number} (SKU {modelno}): {resp.status_code} {resp.text}")
                    except Exception as e:
                        print(f"[ERROR] Exception while sending batch {batch_number} (SKU {modelno}): {e}")
                        traceback.print_exc()
                    batch_number += 1
                    current_batch = []
                    current_size = 0
                if pdf_size_encoded + email_overhead > MAX_EMAIL_SIZE:
                    print(f"[WARN] Single PDF ({pdf['filename']}) exceeds size limit, skipping.")
                    continue
            current_batch.append(pdf)
            current_size += pdf_size_encoded
        
        if current_batch:
            alert_types = sorted(set(p['alert_type'] for p in current_batch))
            alert_types_str = ", ".join(alert_types)
            endpoint = f"https://graph.microsoft.com/v1.0/users/{mail_config.shared_mailbox}/sendMail"
            email_data = {
                "message": {
                    "subject": f"PCR GYO - Yield Alert: SKU - {modelno} | Size Code - {sizecode_str} (Batch {batch_number})",
                    "body": {"contentType": "Text", "content": f"Attached: Reports for {alert_types_str}.\n\n"},
                    "toRecipients": [{"emailAddress": {"address": e}} for e in emails],
                    "attachments": [
                        {
                            '@odata.type': '#microsoft.graph.fileAttachment',
                            'Name': pdf_item['filename'],
                            'ContentBytes': base64.b64encode(pdf_item['bytes'].encode('latin-1')).decode('utf-8')
                        } for pdf_item in current_batch
                    ]
                },
                "saveToSentItems": True
            }
            headers = {
                "Authorization": f"Bearer {mail_config.access_token}",
                "Content-Type": "application/json"
            }
            try:
                resp = requests.post(endpoint, headers=headers, json=email_data)
                if resp.status_code == 202:
                    print(f"[INFO] Final batch {batch_number} emailed via Microsoft Graph successfully for SKU {modelno}.")
                    for pdf_item in current_batch:
                        db[pdf_item['collection']].update_one(
                            {'_id': pdf_item['doc_id']},
                            {'$set': {'alert_flag_repl': True, doc['alert_type']['alert_time_key']: datetime.now()}}
                        )
                    batched_doc_ids.extend([pdf_item['doc_id'] for pdf_item in current_batch])
                    total_pdfs_processed += len(current_batch)
                else:
                    print(f"[ERROR] Graph API sendMail failed for final batch {batch_number} (SKU {modelno}): {resp.status_code} {resp.text}")
            except Exception as e:
                print(f"[ERROR] Exception while sending final batch {batch_number} (SKU {modelno}): {e}")
                traceback.print_exc()
        
        print(f"[INFO] Processed {total_pdfs_processed} PDFs for SKU {modelno} in {batch_number} batches.")
    
    if total_pdfs_processed == 0:
        print("[INFO] No new alerts.")

if __name__ == "__main__":
    app = Flask(__name__)
    app.config.update({
        'MONGO_URI': MONGO_URI,
        'SENDER_EMAIL': SENDER_EMAIL,
        'SENDER_PASSWORD': SENDER_PASSWORD
    })
    with app.app_context():
        send_alerts()


# import os
# import traceback
# from datetime import datetime
# from dateutil.parser import parse
# from io import BytesIO
# import requests
# import json
# import pandas as pd
# import smtplib
# from email.mime.application import MIMEApplication
# from email.mime.multipart import MIMEMultipart
# from email.mime.text import MIMEText
# from flask import Flask, current_app
# from pymongo import MongoClient
# from fpdf import FPDF
# import base64
# import mail_config
# from config_hungary_repl import (
#     alert_triggering_ubno_collect_name,
#     alert_triggering_collect_name,
#     alert_triggering_curing_collect_name,
#     alert_db_virgin_yield,
#     alert_mail_db,
#     alert_users,
#     config_databse,
#     channel_no_collect,
#     MONGO_URI,
#     SENDER_EMAIL,
#     SENDER_PASSWORD,
#     TEAMS_WEBHOOK_URL,
#     TUB_CIMCORP_VISUAL_MERGED_DB_alert,
#     DATA_MERGED_SUMMARY_COLLECTION_alert,
# )

# # --- Configuration ---
# MONGO_URI = MONGO_URI
# SENDER_EMAIL = SENDER_EMAIL
# SENDER_PASSWORD = SENDER_PASSWORD
# ALERT_DB_NAME = alert_db_virgin_yield
# CONFIG_DB_NAME = config_databse
# CHANNEL_NO_COLLECT = channel_no_collect
# BASE_PDF_DIR = "generated_pdfs"
# MAX_EMAIL_SIZE = 20 * 1024 * 1024  # 20 MB

# # Full list of possible keys
# PARAM_KEYS = [
#     "rfppcc","rfppcw","rfh1cc","rfh1cw","lfppcc","lfppcw","cony","trro","brro","lrot","lrob","tbul","bbul","tdep","bdep",
#     "upper","lower","sbal""rfv_yield", "rfh1_yield", "lfv_yield", "conicity_yield", 
#     "lateral_run_out_yield", "bulge_yield", "dent_yield", "static_yield","upper_yield", "lower_yield", 
#     "overall_uniformity_yield", "overall_visual_yield", "overall_warehouse_yield", "overall_scrap_yield"
# ]

# ALERT_TYPES = [
#     {
#         "name": "UFM",
#         "coll": alert_triggering_ubno_collect_name,
#         "banner": "UFM ALERT",
#         "filename_prefix": "UFM",
#         "start_key": "machine_lot_start_ufm_time",
#         "end_key": "machine_lot_end_ufm_time",
#         "alert_time_key": "timestamp_alert_generated",
#         "use_grouped": False,
#         "shift_data_key": "shifts_data_tbm",
#         "shift_start_time_key": "machine_shift_tbm_start_time"
#     },
#     {
#         "name": "TBM",
#         "coll": alert_triggering_collect_name,
#         "banner": "TBM ALERT",
#         "filename_prefix": "TBM",
#         "start_key": "machine_lot_start_build_time",
#         "end_key": "machine_lot_end_build_time",
#         "alert_time_key": "timestamp_alert_generated",
#         "use_grouped": True,
#         "shift_data_key": "shifts_data",
#         "shift_start_time_key": "machine_shift_tbm_start_time"
#     },
#     {
#         "name": "CURING",
#         "coll": alert_triggering_curing_collect_name,
#         "banner": "CURING ALERT",
#         "filename_prefix": "CURING",
#         "start_key": "machine_lot_start_curing_time",
#         "end_key": "machine_lot_end_curing_time",
#         "alert_time_key": "timestamp_alert_generated",
#         "use_grouped": True,
#         "shift_data_key": "shifts_data_tbm",
#         "shift_start_time_key": "machine_shift_tbm_start_time"
#     }
# ]

# # -------------------------------------------------
# # Get OE Replacement from config database
# # -------------------------------------------------
# def get_oe_replacement(client, gt_code):
#     try:
#         cfg_db = client[CONFIG_DB_NAME]
#         coll = cfg_db[CHANNEL_NO_COLLECT]
#         doc = coll.find_one({"gt_code": {"$in": [gt_code]}})
#         if doc:
#             return sanitize_text(doc.get("oe_replacement", "N/A"))
#         else:
#             return "N/A"
#     except Exception as e:
#         print(f"[WARN] OE replacement lookup failed for {gt_code}: {e}")
#         return "N/A"

# # Utility functions
# def sanitize_text(text):
#     if text is None:
#         return "N/A"
#     text = str(text)
#     return text.encode('ascii', 'replace').decode('ascii').replace('\ufffd', '?')

# def html_table_to_markdown(html_table):
#     from bs4 import BeautifulSoup
#     soup = BeautifulSoup(html_table, 'html.parser')
#     markdown_table = ""
#     headers = [th.get_text() for th in soup.find_all('th')]
#     markdown_table += "| " + " | ".join(headers) + " |\n"
#     markdown_table += "|" + "|".join(["---"] * len(headers)) + "|\n"
#     for row in soup.find_all('tr')[1:]:
#         cells = [td.get_text() for td in row.find_all('td')]
#         markdown_table += "| " + " | ".join(cells) + " |\n"
#     return markdown_table

# def generate_combined_summary_table(alerts, alert_type, client=None):
#     if not alerts:
#         return "<table><tr><td>No alert data available</td></tr></table>"
    
#     html = "<table>"
    
#     if alert_type.upper() == "TBM":
#         html += """
#         <tr>
#             <th>S.No</th>
#             <th>SKU</th>
#             <th>Gt Code</th>
#             <th>Size Code</th>
#             <th>TBM</th>
#             <th>Total Tyre Produced</th>
#             <th>Overall WAREHOUSE Yield (%)</th>
#             <th>Overall UNIFORMITY Yield (%)</th>
#             <th>Overall VISUAL Yield (%)</th>
#             <th>Overall SCRAP Yield (%)</th>
#         </tr>
#         """
#     elif alert_type.upper() == "CURING":
#         html += """
#         <tr>
#             <th>S.No</th>
#             <th>SKU</th>
#             <th>Gt Code</th>
#             <th>Size Code</th>
#             <th>CURING</th>
#             <th>Total Tyre Produced</th>
#             <th>Overall WAREHOUSE Yield (%)</th>
#             <th>Overall UNIFORMITY Yield (%)</th>
#             <th>Overall VISUAL Yield (%)</th>
#             <th>Overall SCRAP Yield (%)</th>
#         </tr>
#         """
#     elif alert_type.upper() == "UFM":
#         html += """
#         <tr>
#             <th>S.No</th>
#             <th>SKU</th>
#             <th>Size Code</th>
#             <th>GT Code</th>
#             <th>UFM</th>
#             <th>Total Tyres Produced</th>
#             <th>Overall WAREHOUSE Yield (%)</th>
#             <th>Overall UNIFORMITY Yield (%)</th>
#             <th>Overall VISUAL Yield (%)</th>
#             <th>Overall SCRAP Yield (%)</th>
#             <th>Overall UFM Grade (3)</th>
#             <th>Overall UFM Grade (4)</th>
#             <th>Overall UFM Grade (5)</th>
#         </tr>
#         """
    
#     if alert_type.upper() == "TBM":
#         for i, alert in enumerate(alerts):
#             for machine in alert.get("machine_alerts", []):
#                 try:
#                     warehouse = f"{float(machine.get('overall_warehouse_yield', 0)):.2f}" if machine.get("overall_warehouse_yield") not in [None, "N/A"] else "N/A"
#                     uniformity = f"{float(machine.get('overall_uniformity_yield', 0)):.2f}" if machine.get("overall_uniformity_yield") not in [None, "N/A"] else "N/A"
#                     visual = f"{float(machine.get('overall_visual_yield', 0)):.2f}" if machine.get("overall_visual_yield") not in [None, "N/A"] else "N/A"
#                     scrap = f"{float(machine.get('overall_scrap_yield', 0)):.2f}" if machine.get("overall_scrap_yield") not in [None, "N/A"] else "N/A"
#                 except (ValueError, TypeError):
#                     warehouse = uniformity = visual = scrap ="N/A"

#                 html += f"""
#                 <tr>
#                     <td>{i+1}</td>
#                     <td>{sanitize_text(alert.get('sku', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('gt_code', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('sizecode', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('machine', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('processed_machine_barcodes', 'N/A'))}</td>
#                     <td>{warehouse}</td>
#                     <td>{uniformity}</td>
#                     <td>{visual}</td>
#                     <td>{scrap}</td>
#                 </tr>
#                 """
    
#     elif alert_type.upper() == "CURING":
#         for i, alert in enumerate(alerts):
#             for machine in alert.get("machine_alerts", []):
#                 try:
#                     warehouse = f"{float(machine.get('overall_warehouse_yield', 0)):.2f}" if machine.get("overall_warehouse_yield") not in [None, "N/A"] else "N/A"
#                     uniformity = f"{float(machine.get('overall_uniformity_yield', 0)):.2f}" if machine.get("overall_uniformity_yield") not in [None, "N/A"] else "N/A"
#                     visual = f"{float(machine.get('overall_visual_yield', 0)):.2f}" if machine.get("overall_visual_yield") not in [None, "N/A"] else "N/A"
#                     scrap = f"{float(machine.get('overall_scrap_yield', 0)):.2f}" if machine.get("overall_scrap_yield") not in [None, "N/A"] else "N/A"
#                 except (ValueError, TypeError):
#                     warehouse = uniformity = visual = scrap ="N/A"

#                 html += f"""
#                 <tr>
#                     <td>{i+1}</td>
#                     <td>{sanitize_text(alert.get('sku', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('gt_code', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('sizecode', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('machine', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('processed_machine_barcodes', 'N/A'))}</td>
#                     <td>{warehouse}</td>
#                     <td>{uniformity}</td>
#                     <td>{visual}</td>
#                     <td>{scrap}</td>
#                 </tr>
#                 """
    
#     elif alert_type.upper() == "UFM":
#         for i, alert in enumerate(alerts):
#             sku_val = alert.get('sku', 'N/A')
#             for machine in alert.get("machine_alerts", []):
#                 try:
#                     grade3 = grade4 = grade5 = 0
#                     try:
#                         _mc2 = MongoClient(MONGO_URI)
#                         _summary_coll = _mc2[TUB_CIMCORP_VISUAL_MERGED_DB_alert][DATA_MERGED_SUMMARY_COLLECTION_alert]
#                         machine_barcodes = []
#                         for shift in machine.get('shifts_data_tbm', []):
#                             machine_barcodes.extend(shift.get('barcodes', []))
#                         for shift in machine.get('shifts_data', []):
#                             machine_barcodes.extend(shift.get('barcodes', []))
#                         machine_barcodes = list(set(machine_barcodes))
#                         # A barcode can have multiple UFM test records (re-runs).
#                         # Only the latest test per barcode is marked last == 1, so we
#                         # restrict to it — otherwise counts exceed the lot size.
#                         if not machine_barcodes:
#                             query_filter = {"sku": sku_val, "last": 1, "overallufmgrade": {"$in": [3, 4, 5]}}
#                         else:
#                             query_filter = {"barcode": {"$in": machine_barcodes}, "last": 1, "overallufmgrade": {"$in": [3, 4, 5]}}
#                         grade3 = _summary_coll.count_documents({**query_filter, "overallufmgrade": 3})
#                         grade4 = _summary_coll.count_documents({**query_filter, "overallufmgrade": 4})
#                         grade5 = _summary_coll.count_documents({**query_filter, "overallufmgrade": 5})
#                         _mc2.close()
#                     except Exception as _ge:
#                         print(f"[WARN] overallufmgrade fetch failed: {_ge}")

#                     warehouse  = f"{float(machine.get('overall_warehouse_yield',  0)):.2f}" if machine.get("overall_warehouse_yield")  not in [None, "N/A"] else "N/A"
#                     uniformity = f"{float(machine.get('overall_uniformity_yield', 0)):.2f}" if machine.get("overall_uniformity_yield") not in [None, "N/A"] else "N/A"
#                     visual     = f"{float(machine.get('overall_visual_yield',     0)):.2f}" if machine.get("overall_visual_yield")     not in [None, "N/A"] else "N/A"
#                     scrap      = f"{float(machine.get('overall_scrap_yield',      0)):.2f}" if machine.get("overall_scrap_yield")      not in [None, "N/A"] else "N/A"
#                 except (ValueError, TypeError):
#                     grade3 = grade4 = grade5 = 0
#                     warehouse = uniformity = visual = scrap = "N/A"

#                 html += f"""
#                 <tr>
#                     <td>{i+1}</td>
#                     <td>{sanitize_text(sku_val)}</td>
#                     <td>{sanitize_text(machine.get('sizecode', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('gt_code', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('machine', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('processed_machine_barcodes', 'N/A'))}</td>
#                     <td>{warehouse}</td>
#                     <td>{uniformity}</td>
#                     <td>{visual}</td>
#                     <td>{scrap}</td>
#                     <td>{grade3}</td>
#                     <td>{grade4}</td>
#                     <td>{grade5}</td>
#                 </tr>
#                 """
    
#     html += "</table>"
#     return html

# def send_teams_alert(summary_html, alert_type, docs):
#     teams_webhook_url = TEAMS_WEBHOOK_URL
#     if not teams_webhook_url:
#         print("[ERROR] TEAMS_WEBHOOK_URL not configured.")
#         return False

#     # Fetch yield spec thresholds from channel_no_collect (keyed by sku)
#     yield_spec_lookup = {}
#     try:
#         _mc = MongoClient(MONGO_URI)
#         _cfg_coll = _mc[CONFIG_DB_NAME][CHANNEL_NO_COLLECT]
#         for doc in _cfg_coll.find(
#             {},
#             {"_id": 0, "sku": 1,
#              "spec_overall_warehouse_yield": 1,
#              "spec_overall_uniformity_yield": 1,
#              "spec_overall_visual_yield": 1,
#              "spec_rfppwc_yield": 1,
#              "spec_rfh1wc_yield": 1,
#              "spec_plys_yield": 1,
#              "spec_lfppwc_yield": 1,
#              "spec_cony_yield": 1,
#              "spec_lro_yield": 1,
#              "spec_bulge_yield": 1,
#              "spec_dent_yield": 1,
#              "spec_upper_yield": 1,
#              "spec_lower_yield": 1,
#              "spec_sbal_yield": 1}
#         ):
#             sku_key = str(doc.get("sku", "")).strip()
#             if sku_key:
#                 yield_spec_lookup[sku_key] = {
#                     "spec_overall_warehouse_yield": doc.get("spec_overall_warehouse_yield"),
#                     "spec_overall_uniformity_yield": doc.get("spec_overall_uniformity_yield"),
#                     "spec_overall_visual_yield":    doc.get("spec_overall_visual_yield"),
#                     "spec_rfv_yield":               doc.get("spec_rfppwc_yield"),
#                     "spec_rfh1_yield":              doc.get("spec_rfh1wc_yield"),
#                     "spec_plysteer_yield":          doc.get("spec_plys_yield"),
#                     "spec_lfv_yield":               doc.get("spec_lfppwc_yield"),
#                     "spec_conicity_yield":          doc.get("spec_cony_yield"),
#                     "spec_lateral_run_out_yield":   doc.get("spec_lro_yield"),
#                     "spec_bulge_yield":             doc.get("spec_bulge_yield"),
#                     "spec_dent_yield":              doc.get("spec_dent_yield"),
#                     "spec_upper_yield":             doc.get("spec_upper_yield"),
#                     "spec_lower_yield":             doc.get("spec_lower_yield"),
#                     "spec_static_yield":            doc.get("spec_sbal_yield"),
#                 }
#         _mc.close()
#     except Exception as _e:
#         print(f"[WARN] Could not fetch yield specs from channel_no_collect: {_e}")

#     # Yield calculation functions
#     def yield_percentage(numerator, denominator):
#         try:
#             if denominator is None or denominator == 0:
#                 return 0.0
#             return round((numerator / denominator) * 100, 2)
#         except (TypeError, ValueError, ZeroDivisionError):
#             return 0.0

#     def get_yield_from_rank_counts(rank_counts, total_bc):
#         """
#         Extract numerator and denominator from rank_counts.
#         rank_counts format: {'grade_0': count, 'grade_3': count}
#         Pass grades are 1, 2, 3
#         """
#         if not rank_counts:
#             return 0, total_bc if total_bc > 0 else 1
        
#         numerator = 0
#         denominator = 0
        
#         for key, count in rank_counts.items():
#             # Extract grade number from key like 'rfppwcgrade_3' -> 3
#             try:
#                 if '_' in key:
#                     grade = int(key.split('_')[-1])
#                 else:
#                     grade = int(key)
                
#                 denominator += count
                
#                 # Grades 1, 2, 3 are PASS
#                 if grade in [1, 2, 3]:
#                     numerator += count
#             except (ValueError, IndexError):
#                 # If we can't parse the grade, skip
#                 continue
        
#         # If no denominator found, use total_bc
#         if denominator == 0:
#             denominator = total_bc if total_bc > 0 else 1
        
#         return numerator, denominator

#     def calculate_yields_from_machine_obj(machine_obj, total_bc, specific_barcodes=None):
#         """Calculate yields from machine_obj, optionally for specific barcodes"""
#         # If specific barcodes are provided, we need to filter the data
#         if specific_barcodes and len(specific_barcodes) > 0:
#             return calculate_yields_from_barcodes(specific_barcodes, total_bc, machine_obj)
#         else:
#             # Use the aggregated data from machine_obj
#             return calculate_yields_from_aggregated_data(machine_obj, total_bc)

#     def _grade_yield_counts(df, grade_col):
#         """Numerator/denominator using the canonical *_yield formula:
#         denominator = rows where (grade > 0) & (grade <= 5) & barcode notnull
#         numerator   = subset of denominator where grade in [1, 2, 3] & barcode notnull
#         """
#         try:
#             if df is None or df.empty or grade_col not in df.columns or 'barcode' not in df.columns:
#                 return 0, 0
#             grade = pd.to_numeric(df[grade_col], errors='coerce')
#             den_mask = (grade > 0) & (grade <= 5) & df['barcode'].notnull()
#             denominator = int(den_mask.sum())
#             num_mask = den_mask & grade.isin([1, 2, 3])
#             numerator = int(num_mask.sum())
#             return numerator, denominator
#         except Exception:
#             return 0, 0

#     def _overall_uniformity_counts(df):
#         """overall_uniformity_yield: den = measured - vi_downgrade, num = ufm pass & vi_yield==1"""
#         try:
#             grade = pd.to_numeric(df['overallufmgrade'], errors='coerce')
#             measured = int(((grade.isin([1, 2, 3, 4, 5])) & df['barcode'].notnull()).sum())
#             vi = pd.to_numeric(df['vi_yield'], errors='coerce') if 'vi_yield' in df.columns else pd.Series([], dtype=float)
#             vi_downgrade = int(((vi == 0) & vi.notna()).sum()) if len(vi) else 0
#             denominator = measured - vi_downgrade
#             if 'vi_yield' in df.columns:
#                 numerator = int(((grade.isin([1, 2, 3])) & (pd.to_numeric(df['vi_yield'], errors='coerce') == 1)).sum())
#             else:
#                 numerator = int(grade.isin([1, 2, 3]).sum())
#             return numerator, denominator
#         except Exception:
#             return 0, 0

#     def _overall_visual_counts(df):
#         """overall_visual_yield: den = ufm_count + vi_scrap, num = ufm_count - vi_downgrade"""
#         try:
#             grade = pd.to_numeric(df['overallufmgrade'], errors='coerce')
#             vi = pd.to_numeric(df['vi_yield'], errors='coerce') if 'vi_yield' in df.columns else pd.Series([], dtype=float)
#             vi_downgrade = int((vi == 0).sum()) if len(vi) else 0
#             if 'scrap_category_tub' in df.columns:
#                 vi_scrap = int((df['scrap_category_tub'] == "VISUAL").sum())
#             else:
#                 vi_scrap = 0
#             ufm_count = int(grade.isin([1, 2, 3, 4, 5]).sum())
#             denominator = ufm_count + vi_scrap
#             numerator = ufm_count - vi_downgrade
#             return numerator, denominator
#         except Exception:
#             return 0, 0

#     def _overall_warehouse_counts(df):
#         """overall_warehouse_yield: den = measured + vi_scrap, num = ufm grade in [1,2,3]"""
#         try:
#             grade = pd.to_numeric(df['overallufmgrade'], errors='coerce')
#             measured = int(((grade.isin([1, 2, 3, 4, 5])) & df['barcode'].notnull()).sum())
#             if 'scrap_category_tub' in df.columns:
#                 vi_scrap = int(((df['scrap_category_tub'] == "VISUAL") & df['scrap_category_tub'].notna()).sum())
#             else:
#                 vi_scrap = 0
#             denominator = measured + vi_scrap
#             numerator = int(grade.isin([1, 2, 3]).sum())
#             return numerator, denominator
#         except Exception:
#             return 0, 0

#     def calculate_yields_from_barcodes(barcodes, total_bc, machine_obj):
#         """Calculate yields for the given machine using ONLY this lot's barcodes.

#         The barcodes passed in are intersected with the lot barcodes (machine_obj['barcodes'])
#         so a machine never pulls tyres from other lots/SKUs. Every yield is computed with the
#         exact canonical formula (grade>0 & grade<=5 for denominator, grade in [1,2,3] for
#         numerator; overall_* use vi_yield / scrap_category_tub logic).
#         """
#         # Restrict to this lot's barcodes only
#         lot_barcodes = [str(b) for b in (machine_obj.get('barcodes') or []) if b]
#         machine_barcodes = [str(b) for b in (barcodes or []) if b]
#         if lot_barcodes:
#             lot_set = set(lot_barcodes)
#             scoped = [b for b in machine_barcodes if b in lot_set]
#             # if the machine-specific list didn't intersect, fall back to the lot barcodes
#             query_barcodes = scoped if scoped else lot_barcodes
#         else:
#             query_barcodes = machine_barcodes

#         if not query_barcodes:
#             return calculate_yields_from_aggregated_data(machine_obj, total_bc)

#         try:
#             _mc = MongoClient(MONGO_URI)
#             _db = _mc[TUB_CIMCORP_VISUAL_MERGED_DB_alert]
#             _coll = _db[DATA_MERGED_SUMMARY_COLLECTION_alert]

#             projection = {
#                 "_id": 0, "barcode": 1, "last": 1,
#                 "rfppwcgrade": 1, "rfh1wcgrade": 1, "lfppwcgrade": 1,
#                 "conygrade": 1, "lrograde": 1, "sbalgrade": 1,
#                 "uppergrade": 1, "lowergrade": 1, "bulwcgrade": 1, "depwcgrade": 1,
#                 "overallufmgrade": 1, "vi_yield": 1, "scrap_category_tub": 1,
#             }
#             # A barcode can have multiple UFM test records (re-runs). Only the
#             # latest test per barcode is marked last == 1. Restricting to it keeps
#             # one row per barcode so denominators never exceed the lot size.
#             docs = list(_coll.find(
#                 {"barcode": {"$in": query_barcodes}, "last": 1},
#                 projection
#             ))
#             _mc.close()
#         except Exception as e:
#             print(f"[ERROR] Failed to get specific barcode data: {e}")
#             return calculate_yields_from_aggregated_data(machine_obj, total_bc)

#         # Fallback: if the last==1 filter returned nothing (e.g. older records
#         # written before the 'last' flag existed), re-query without it and
#         # deduplicate to one row per barcode below.
#         if not docs:
#             try:
#                 _mc = MongoClient(MONGO_URI)
#                 _coll = _mc[TUB_CIMCORP_VISUAL_MERGED_DB_alert][DATA_MERGED_SUMMARY_COLLECTION_alert]
#                 docs = list(_coll.find({"barcode": {"$in": query_barcodes}}, projection))
#                 _mc.close()
#             except Exception as e:
#                 print(f"[ERROR] Fallback barcode fetch failed: {e}")

#         if not docs:
#             return calculate_yields_from_aggregated_data(machine_obj, total_bc)

#         df = pd.DataFrame(docs)

#         # Safety net: guarantee one row per barcode. Prefer the record flagged
#         # last == 1; otherwise keep the first occurrence.
#         if 'barcode' in df.columns and df['barcode'].duplicated().any():
#             if 'last' in df.columns:
#                 df['_last_pref'] = (pd.to_numeric(df['last'], errors='coerce') == 1).astype(int)
#                 df = (df.sort_values('_last_pref', ascending=False)
#                         .drop_duplicates(subset='barcode', keep='first')
#                         .drop(columns=['_last_pref']))
#             else:
#                 df = df.drop_duplicates(subset='barcode', keep='first')

#         # Per-parameter yields (canonical grade formula)
#         rfv_num, rfv_den = _grade_yield_counts(df, 'rfppwcgrade')
#         rfh1_num, rfh1_den = _grade_yield_counts(df, 'rfh1wcgrade')
#         lfv_num, lfv_den = _grade_yield_counts(df, 'lfppwcgrade')
#         conicity_num, conicity_den = _grade_yield_counts(df, 'conygrade')
#         static_num, static_den = _grade_yield_counts(df, 'sbalgrade')
#         upper_num, upper_den = _grade_yield_counts(df, 'uppergrade')
#         lower_num, lower_den = _grade_yield_counts(df, 'lowergrade')
#         bulge_num, bulge_den = _grade_yield_counts(df, 'bulwcgrade')
#         dent_num, dent_den = _grade_yield_counts(df, 'depwcgrade')

#         # Overall yields (canonical overall_* formulas)
#         wh_num, wh_den = _overall_warehouse_counts(df)
#         uni_num, uni_den = _overall_uniformity_counts(df)
#         vis_num, vis_den = _overall_visual_counts(df)

#         return {
#             'wh': (yield_percentage(wh_num, wh_den), wh_num, wh_den),
#             'uni': (yield_percentage(uni_num, uni_den), uni_num, uni_den),
#             'vis': (yield_percentage(vis_num, vis_den), vis_num, vis_den),
#             'rfv': (yield_percentage(rfv_num, rfv_den), rfv_num, rfv_den),
#             'rfh1': (yield_percentage(rfh1_num, rfh1_den), rfh1_num, rfh1_den),
#             'lfv': (yield_percentage(lfv_num, lfv_den), lfv_num, lfv_den),
#             'conicity': (yield_percentage(conicity_num, conicity_den), conicity_num, conicity_den),
#             'static': (yield_percentage(static_num, static_den), static_num, static_den),
#             'upper': (yield_percentage(upper_num, upper_den), upper_num, upper_den),
#             'lower': (yield_percentage(lower_num, lower_den), lower_num, lower_den),
#             'bulge': (yield_percentage(bulge_num, bulge_den), bulge_num, bulge_den),
#             'dent': (yield_percentage(dent_num, dent_den), dent_num, dent_den),
#         }

#     def calculate_yields_from_aggregated_data(machine_obj, total_bc):
#         """Calculate yields from aggregated machine_obj data"""
#         # Initialize with zeros
#         wh_percent, wh_num, wh_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         uni_percent, uni_num, uni_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         vis_percent, vis_num, vis_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         rfv_percent, rfv_num, rfv_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         rfh1_percent, rfh1_num, rfh1_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         lfv_percent, lfv_num, lfv_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         conicity_percent, conicity_num, conicity_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         static_percent, static_num, static_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         upper_percent, upper_num, upper_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         lower_percent, lower_num, lower_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         bulge_percent, bulge_num, bulge_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         dent_percent, dent_num, dent_den = 0.0, 0, total_bc if total_bc > 0 else 1
        
#         # Get parameter_alerts from machine_obj
#         param_alerts = machine_obj.get('parameter_alerts', [])
        
#         # Also check lot_averages for yields
#         lot_averages = machine_obj.get('lot_averages', {})
        
#         for param in param_alerts:
#             param_name = param.get('parameter')
#             current_value = param.get('current_value')
#             rank_counts = param.get('rank_counts', {})
            
#             # If rank_counts exists, use it to calculate numerator and denominator
#             if rank_counts:
#                 num, den = get_yield_from_rank_counts(rank_counts, total_bc)
                
#                 if param_name == 'rfv_yield':
#                     rfv_num, rfv_den = num, den
#                     rfv_percent = yield_percentage(rfv_num, rfv_den)
#                 elif param_name == 'rfh1_yield':
#                     rfh1_num, rfh1_den = num, den
#                     rfh1_percent = yield_percentage(rfh1_num, rfh1_den)
#                 elif param_name == 'lfv_yield':
#                     lfv_num, lfv_den = num, den
#                     lfv_percent = yield_percentage(lfv_num, lfv_den)
#                 elif param_name == 'conicity_yield':
#                     conicity_num, conicity_den = num, den
#                     conicity_percent = yield_percentage(conicity_num, conicity_den)
#                 elif param_name == 'static_yield':
#                     static_num, static_den = num, den
#                     static_percent = yield_percentage(static_num, static_den)
#                 elif param_name == 'upper_yield':
#                     upper_num, upper_den = num, den
#                     upper_percent = yield_percentage(upper_num, upper_den)
#                 elif param_name == 'lower_yield':
#                     lower_num, lower_den = num, den
#                     lower_percent = yield_percentage(lower_num, lower_den)
#                 elif param_name == 'bulge_yield':
#                     bulge_num, bulge_den = num, den
#                     bulge_percent = yield_percentage(bulge_num, bulge_den)
#                 elif param_name == 'dent_yield':
#                     dent_num, dent_den = num, den
#                     dent_percent = yield_percentage(dent_num, dent_den)
#                 elif param_name == 'overall_warehouse_yield':
#                     wh_num, wh_den = num, den
#                     wh_percent = yield_percentage(wh_num, wh_den)
#                 elif param_name == 'overall_uniformity_yield':
#                     uni_num, uni_den = num, den
#                     uni_percent = yield_percentage(uni_num, uni_den)
#                 elif param_name == 'overall_visual_yield':
#                     vis_num, vis_den = num, den
#                     vis_percent = yield_percentage(vis_num, vis_den)
#             else:
#                 # If no rank_counts, try to use current_value from lot_averages
#                 if param_name in lot_averages:
#                     value = lot_averages.get(param_name)
#                     if value is not None:
#                         # We only have percentage, not numerator/denominator
#                         # Use total_bc as denominator and calculate numerator
#                         den = total_bc if total_bc > 0 else 1
#                         num = int(round((value / 100) * den)) if value else 0
                        
#                         if param_name == 'rfv_yield':
#                             rfv_percent = float(value) if value else 0.0
#                             rfv_num, rfv_den = num, den
#                         elif param_name == 'rfh1_yield':
#                             rfh1_percent = float(value) if value else 0.0
#                             rfh1_num, rfh1_den = num, den
#                         elif param_name == 'lfv_yield':
#                             lfv_percent = float(value) if value else 0.0
#                             lfv_num, lfv_den = num, den
#                         elif param_name == 'conicity_yield':
#                             conicity_percent = float(value) if value else 0.0
#                             conicity_num, conicity_den = num, den
#                         elif param_name == 'static_yield':
#                             static_percent = float(value) if value else 0.0
#                             static_num, static_den = num, den
#                         elif param_name == 'upper_yield':
#                             upper_percent = float(value) if value else 0.0
#                             upper_num, upper_den = num, den
#                         elif param_name == 'lower_yield':
#                             lower_percent = float(value) if value else 0.0
#                             lower_num, lower_den = num, den
#                         elif param_name == 'bulge_yield':
#                             bulge_percent = float(value) if value else 0.0
#                             bulge_num, bulge_den = num, den
#                         elif param_name == 'dent_yield':
#                             dent_percent = float(value) if value else 0.0
#                             dent_num, dent_den = num, den
#                         elif param_name == 'overall_warehouse_yield':
#                             wh_percent = float(value) if value else 0.0
#                             wh_num, wh_den = num, den
#                         elif param_name == 'overall_uniformity_yield':
#                             uni_percent = float(value) if value else 0.0
#                             uni_num, uni_den = num, den
#                         elif param_name == 'overall_visual_yield':
#                             vis_percent = float(value) if value else 0.0
#                             vis_num, vis_den = num, den
        
#         # If we still don't have values, try lot_averages directly
#         if rfv_num == 0 and rfv_den == total_bc:
#             for key, value in lot_averages.items():
#                 if key == 'rfv_yield' and value is not None:
#                     rfv_percent = float(value)
#                     rfv_num = int(round((rfv_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     rfv_den = total_bc if total_bc > 0 else 1
#                 elif key == 'rfh1_yield' and value is not None:
#                     rfh1_percent = float(value)
#                     rfh1_num = int(round((rfh1_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     rfh1_den = total_bc if total_bc > 0 else 1
#                 elif key == 'lfv_yield' and value is not None:
#                     lfv_percent = float(value)
#                     lfv_num = int(round((lfv_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     lfv_den = total_bc if total_bc > 0 else 1
#                 elif key == 'conicity_yield' and value is not None:
#                     conicity_percent = float(value)
#                     conicity_num = int(round((conicity_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     conicity_den = total_bc if total_bc > 0 else 1
#                 elif key == 'static_yield' and value is not None:
#                     static_percent = float(value)
#                     static_num = int(round((static_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     static_den = total_bc if total_bc > 0 else 1
#                 elif key == 'upper_yield' and value is not None:
#                     upper_percent = float(value)
#                     upper_num = int(round((upper_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     upper_den = total_bc if total_bc > 0 else 1
#                 elif key == 'lower_yield' and value is not None:
#                     lower_percent = float(value)
#                     lower_num = int(round((lower_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     lower_den = total_bc if total_bc > 0 else 1
#                 elif key == 'bulge_yield' and value is not None:
#                     bulge_percent = float(value)
#                     bulge_num = int(round((bulge_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     bulge_den = total_bc if total_bc > 0 else 1
#                 elif key == 'dent_yield' and value is not None:
#                     dent_percent = float(value)
#                     dent_num = int(round((dent_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     dent_den = total_bc if total_bc > 0 else 1
#                 elif key == 'overall_warehouse_yield' and value is not None:
#                     wh_percent = float(value)
#                     wh_num = int(round((wh_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     wh_den = total_bc if total_bc > 0 else 1
#                 elif key == 'overall_uniformity_yield' and value is not None:
#                     uni_percent = float(value)
#                     uni_num = int(round((uni_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     uni_den = total_bc if total_bc > 0 else 1
#                 elif key == 'overall_visual_yield' and value is not None:
#                     vis_percent = float(value)
#                     vis_num = int(round((vis_percent / 100) * total_bc)) if total_bc > 0 else 0
#                     vis_den = total_bc if total_bc > 0 else 1
        
#         return {
#             'wh': (wh_percent, wh_num, wh_den),
#             'uni': (uni_percent, uni_num, uni_den),
#             'vis': (vis_percent, vis_num, vis_den),
#             'rfv': (rfv_percent, rfv_num, rfv_den),
#             'rfh1': (rfh1_percent, rfh1_num, rfh1_den),
#             'lfv': (lfv_percent, lfv_num, lfv_den),
#             'conicity': (conicity_percent, conicity_num, conicity_den),
#             'static': (static_percent, static_num, static_den),
#             'upper': (upper_percent, upper_num, upper_den),
#             'lower': (lower_percent, lower_num, lower_den),
#             'bulge': (bulge_percent, bulge_num, bulge_den),
#             'dent': (dent_percent, dent_num, dent_den)
#         }

#     def _fmt(val):
#         try:
#             return f"{float(val):.2f}%" if val not in (None, 'N/A') else "N/A"
#         except (TypeError, ValueError):
#             return "N/A"

#     def _color(actual, spec):
#         try:
#             if actual is None or spec is None:
#                 return "none"
#             return "red" if float(actual) < float(spec) else "green"
#         except (TypeError, ValueError):
#             return "none"

#     def _ind(color_tag):
#         if color_tag == "green": return "✅"
#         if color_tag == "red":   return "🔴"
#         return ""

#     def _fmt_count(count):
#         if count is None:
#             return "N/A"
#         try:
#             return str(int(count))
#         except (TypeError, ValueError):
#             return "N/A"

#     alert_machine_entries = []

#     for alert in docs:
#         for machine in alert.get("machine_alerts", []):
#             raw_sizecode = machine.get('sizecode', 'N/A')
#             sizecode = raw_sizecode if raw_sizecode != 'N/A' else sanitize_text(raw_sizecode)
#             raw_gt_code = machine.get('gt_code', 'N/A')
#             gt_code = raw_gt_code if raw_gt_code != 'N/A' else sanitize_text(raw_gt_code)
#             param_alerts = machine.get('parameter_alerts', [])

#             entry = {
#                 'sku': sanitize_text(alert.get('sku', 'N/A')),
#                 'machine_name': sanitize_text(machine.get('machine', 'N/A')),
#                 'sizecode': sizecode,
#                 'gt_code': gt_code,
#                 'params': [],
#                 'machine_obj': machine,
#                 'alert_obj': alert,
#             }

#             for param in param_alerts:
#                 param_name = param.get('parameter')
#                 current_val = param.get('current_value')
#                 shift_threshold = param.get('shift_occurred_thershold', False)
#                 shift_spec = param.get('shift_occurred_spec', False)
#                 rank_counts = param.get('rank_counts', {})

#                 if shift_threshold or shift_spec:
#                     if param_name.endswith('_yield'):
#                         display_name = param_name.replace('_yield', ' yield')
#                     else:
#                         display_name = param_name

#                     if current_val == 0 or (isinstance(current_val, (int, float)) and current_val == 0):
#                         rank_str = ", ".join([
#                             f"{k.rsplit('_', 1)[0]}->{k.rsplit('_', 1)[1]}:{v}(count)"
#                             if '_' in k else f"{k}->{v}"
#                             for k, v in rank_counts.items()
#                         ]) if rank_counts else "No ranks"
#                         deviated_param = f"{display_name}({rank_str})"
#                     else:
#                         deviated_param = display_name

#                     entry['params'].append(deviated_param)

#             alert_machine_entries.append(entry)

#     if alert_type.upper() == "TBM":
#         title_str = "Alert: TBM Alert Summary"
#     elif alert_type.upper() == "CURING":
#         title_str = "Alert: CURING Alert Summary"
#     elif alert_type.upper() == "UFM":
#         title_str = "Alert: UFM Alert Summary"
#     else:
#         title_str = "Alert Summary"

#     markdown_table = html_table_to_markdown(summary_html)

#     breakdown_blocks = []

#     # Helper function to get barcodes for a specific curing machine
#     def get_barcodes_for_curing_machine(machine_obj, cur_id):
#         """
#         Get the list of barcodes that went through a specific curing machine.
#         Queries data_merged_summary_alerts_data by cur_id field — gives exact per-press barcodes.
#         """
#         try:
#             sku = machine_obj.get('sku') or machine_obj.get('gt_code', '')
#             _mc = MongoClient(MONGO_URI)
#             _db = _mc[TUB_CIMCORP_VISUAL_MERGED_DB_alert]
#             _coll = _db[DATA_MERGED_SUMMARY_COLLECTION_alert]

#             lot_barcodes = [str(b) for b in (machine_obj.get('barcodes') or []) if b]
#             query = {"cur_id": cur_id}
#             # Restrict to this lot's barcodes so we never pull other lots/SKUs
#             if lot_barcodes:
#                 query["barcode"] = {"$in": lot_barcodes}

#             docs = list(_coll.find(query, {"barcode": 1, "_id": 0}))
#             _mc.close()

#             barcodes = [str(d["barcode"]) for d in docs if d.get("barcode")]
#             if barcodes:
#                 return barcodes
#         except Exception as e:
#             print(f"[WARN] get_barcodes_for_curing_machine failed for {cur_id}: {e}")

#         # Fallback: use the main barcodes list (less accurate)
#         return machine_obj.get('barcodes', [])

#     # Helper function to get barcodes for a specific TBM machine
#     def get_barcodes_for_tbm_machine(machine_obj, tbm_id):
#         """
#         Get the list of barcodes that went through a specific TBM machine.
#         Queries data_merged_summary_alerts_data by ptb_id field.
#         """
#         try:
#             _mc = MongoClient(MONGO_URI)
#             _db = _mc[TUB_CIMCORP_VISUAL_MERGED_DB_alert]
#             _coll = _db[DATA_MERGED_SUMMARY_COLLECTION_alert]

#             lot_barcodes = [str(b) for b in (machine_obj.get('barcodes') or []) if b]
#             query = {"ptb_id": tbm_id}
#             # Restrict to this lot's barcodes so we never pull other lots/SKUs
#             if lot_barcodes:
#                 query["barcode"] = {"$in": lot_barcodes}

#             docs = list(_coll.find(query, {"barcode": 1, "_id": 0}))
#             _mc.close()

#             barcodes = [str(d["barcode"]) for d in docs if d.get("barcode")]
#             if barcodes:
#                 return barcodes
#         except Exception as e:
#             print(f"[WARN] get_barcodes_for_tbm_machine failed for {tbm_id}: {e}")

#         # Fallback: use the main barcodes list
#         return machine_obj.get('barcodes', [])

#     for entry in alert_machine_entries:
#         sku         = entry['sku']
#         machine_obj = entry['machine_obj']
#         alert_obj   = entry['alert_obj']
#         _at         = alert_type.upper()

#         total_bc  = machine_obj.get('processed_machine_barcodes', 0)
#         sku_specs = yield_spec_lookup.get(str(sku).strip(), {})
#         spec_wh   = sku_specs.get('spec_overall_warehouse_yield')
#         spec_uni  = sku_specs.get('spec_overall_uniformity_yield')
#         spec_vis  = sku_specs.get('spec_overall_visual_yield')
#         spec_rfv  = sku_specs.get('spec_rfv_yield')
#         spec_rfh1 = sku_specs.get('spec_rfh1_yield')
#         spec_lfv  = sku_specs.get('spec_lfv_yield')
#         spec_conicity = sku_specs.get('spec_conicity_yield')
#         spec_static   = sku_specs.get('spec_static_yield')
#         spec_upper    = sku_specs.get('spec_upper_yield')
#         spec_lower    = sku_specs.get('spec_lower_yield')
#         spec_bulge    = sku_specs.get('spec_bulge_yield')
#         spec_dent     = sku_specs.get('spec_dent_yield')

#         entry_rows = []

#         def _add_row(mname, mtype, bc, mach_obj, specific_barcodes=None):
#             """Add a row with yields calculated from machine_obj"""
#             # Calculate yields directly from machine_obj
#             yields = calculate_yields_from_machine_obj(mach_obj, bc, specific_barcodes)
            
#             wh_percent, wh_num, wh_den = yields['wh']
#             uni_percent, uni_num, uni_den = yields['uni']
#             vis_percent, vis_num, vis_den = yields['vis']
#             rfv_percent, rfv_num, rfv_den = yields['rfv']
#             rfh1_percent, rfh1_num, rfh1_den = yields['rfh1']
#             lfv_percent, lfv_num, lfv_den = yields['lfv']
#             conicity_percent, conicity_num, conicity_den = yields['conicity']
#             static_percent, static_num, static_den = yields['static']
#             upper_percent, upper_num, upper_den = yields['upper']
#             lower_percent, lower_num, lower_den = yields['lower']
#             bulge_percent, bulge_num, bulge_den = yields['bulge']
#             dent_percent, dent_num, dent_den = yields['dent']
            
#             entry_rows.append({
#                 'mname': mname,
#                 'mtype': mtype,
#                 'bc': bc,
#                 'wh': (wh_percent, wh_num, wh_den),
#                 'uni': (uni_percent, uni_num, uni_den),
#                 'vis': (vis_percent, vis_num, vis_den),
#                 'rfv': (rfv_percent, rfv_num, rfv_den),
#                 'rfh1': (rfh1_percent, rfh1_num, rfh1_den),
#                 'lfv': (lfv_percent, lfv_num, lfv_den),
#                 'conicity': (conicity_percent, conicity_num, conicity_den),
#                 'static': (static_percent, static_num, static_den),
#                 'upper': (upper_percent, upper_num, upper_den),
#                 'lower': (lower_percent, lower_num, lower_den),
#                 'bulge': (bulge_percent, bulge_num, bulge_den),
#                 'dent': (dent_percent, dent_num, dent_den),
#                 'wh_color': _color(wh_percent, spec_wh),
#                 'uni_color': _color(uni_percent, spec_uni),
#                 'vis_color': _color(vis_percent, spec_vis),
#                 'rfv_color': _color(rfv_percent, spec_rfv),
#                 'rfh1_color': _color(rfh1_percent, spec_rfh1),
#                 'lfv_color': _color(lfv_percent, spec_lfv),
#                 'conicity_color': _color(conicity_percent, spec_conicity),
#                 'static_color': _color(static_percent, spec_static),
#                 'upper_color': _color(upper_percent, spec_upper),
#                 'lower_color': _color(lower_percent, spec_lower),
#                 'bulge_color': _color(bulge_percent, spec_bulge),
#                 'dent_color': _color(dent_percent, spec_dent),
#             })

#         # Build rows based on alert type
#         if _at == "UFM":
#             # UFM row - compute from this lot's own barcodes so num/den are real
#             # counts (consistent with the machine breakdown rows) using the
#             # canonical yield formulas.
#             _add_row(
#                 sanitize_text(machine_obj.get('machine', 'N/A')), 
#                 "UFM", 
#                 int(total_bc) if total_bc else 0,
#                 machine_obj,
#                 machine_obj.get('barcodes', [])  # this lot's barcodes only
#             )
            
#             # Curing rows - get specific barcodes for each curing machine
#             for c in machine_obj.get('cur_ids', []):
#                 bc = c.get('barcode', 0)
#                 cur_id = c.get('cur_id', 'N/A')
                
#                 # Get the barcodes that went through this curing machine
#                 cur_barcodes = get_barcodes_for_curing_machine(machine_obj, cur_id)
                
#                 # If we couldn't get specific barcodes, use the main barcodes list
#                 if not cur_barcodes:
#                     cur_barcodes = machine_obj.get('barcodes', [])
                
#                 curing_obj = machine_obj.copy()
#                 curing_obj['processed_machine_barcodes'] = bc
#                 _add_row(
#                     sanitize_text(str(cur_id)), 
#                     "Curing", 
#                     int(bc),
#                     curing_obj,
#                     cur_barcodes  # Pass specific barcodes for this curing machine
#                 )
            
#             # TBM rows - get specific barcodes for each TBM machine
#             tbm_ids_raw = machine_obj.get('tbm_ids', [])
#             tbm_machine = sanitize_text(machine_obj.get('tbm_machine', 'N/A'))
#             if tbm_ids_raw:
#                 for t in tbm_ids_raw:
#                     bc = t.get('barcode', 0)
#                     tbm_id = t.get('tbm_id', 'N/A')
                    
#                     # Get the barcodes that went through this TBM machine
#                     tbm_barcodes = get_barcodes_for_tbm_machine(machine_obj, tbm_id)
                    
#                     # If we couldn't get specific barcodes, use the main barcodes list
#                     if not tbm_barcodes:
#                         tbm_barcodes = machine_obj.get('barcodes', [])
                    
#                     tbm_obj = machine_obj.copy()
#                     tbm_obj['processed_machine_barcodes'] = bc
#                     _add_row(
#                         sanitize_text(str(tbm_id)), 
#                         "TBM", 
#                         int(bc),
#                         tbm_obj,
#                         tbm_barcodes  # Pass specific barcodes for this TBM machine
#                     )
#             elif tbm_machine and tbm_machine != 'N/A':
#                 # If no specific TBM IDs, use the main TBM machine with all barcodes
#                 tbm_barcodes = machine_obj.get('barcodes', [])
#                 tbm_obj = machine_obj.copy()
#                 tbm_obj['processed_machine_barcodes'] = total_bc
#                 _add_row(
#                     tbm_machine, 
#                     "TBM", 
#                     int(total_bc) if total_bc else 0,
#                     tbm_obj,
#                     tbm_barcodes
#                 )

#         elif _at == "TBM":
#             # TBM row - use aggregated data
#             _add_row(
#                 sanitize_text(machine_obj.get('machine', 'N/A')), 
#                 "TBM", 
#                 int(total_bc) if total_bc else 0,
#                 machine_obj,
#                 None
#             )
            
#             # UFM rows
#             seen_ubno = set()
#             for ma in alert_obj.get("machine_alerts", []):
#                 for rec in ma.get("grouped_averages", {}).get("uf_id", []):
#                     uf = sanitize_text(rec.get('uf_id', ''))
#                     bc = rec.get('barcode_count', 0)
#                     if uf and uf not in seen_ubno:
#                         seen_ubno.add(uf)
#                         # Get barcodes for this UFM machine
#                         ufm_barcodes = machine_obj.get('barcodes', [])
#                         ufm_obj = machine_obj.copy()
#                         ufm_obj['processed_machine_barcodes'] = bc
#                         _add_row(
#                             uf, 
#                             "UFM", 
#                             int(bc),
#                             ufm_obj,
#                             ufm_barcodes
#                         )
            
#             # Curing rows
#             for c in machine_obj.get('cur_ids', []):
#                 bc = c.get('barcode', 0)
#                 cur_id = c.get('cur_id', 'N/A')
#                 cur_barcodes = get_barcodes_for_curing_machine(machine_obj, cur_id)
#                 if not cur_barcodes:
#                     cur_barcodes = machine_obj.get('barcodes', [])
#                 cur_obj = machine_obj.copy()
#                 cur_obj['processed_machine_barcodes'] = bc
#                 _add_row(
#                     sanitize_text(str(cur_id)), 
#                     "Curing", 
#                     int(bc),
#                     cur_obj,
#                     cur_barcodes
#                 )

#         elif _at == "CURING":
#             # Curing row - use aggregated data
#             _add_row(
#                 sanitize_text(machine_obj.get('machine', 'N/A')), 
#                 "Curing", 
#                 int(total_bc) if total_bc else 0,
#                 machine_obj,
#                 None
#             )
            
#             # UFM rows
#             seen_ubno = set()
#             for ma in alert_obj.get("machine_alerts", []):
#                 for rec in ma.get("grouped_averages", {}).get("uf_id", []):
#                     uf = sanitize_text(rec.get('uf_id', ''))
#                     bc = rec.get('barcode_count', 0)
#                     if uf and uf not in seen_ubno:
#                         seen_ubno.add(uf)
#                         ufm_barcodes = machine_obj.get('barcodes', [])
#                         ufm_obj = machine_obj.copy()
#                         ufm_obj['processed_machine_barcodes'] = bc
#                         _add_row(
#                             uf, 
#                             "UFM", 
#                             int(bc),
#                             ufm_obj,
#                             ufm_barcodes
#                         )
            
#             # TBM rows
#             for p in machine_obj.get('ptb_ids', []):
#                 bc = p.get('barcode', 0)
#                 ptb_id = p.get('ptb_id', 'N/A')
#                 tbm_barcodes = machine_obj.get('barcodes', [])
#                 tbm_obj = machine_obj.copy()
#                 tbm_obj['processed_machine_barcodes'] = bc
#                 _add_row(
#                     sanitize_text(str(ptb_id)), 
#                     "TBM", 
#                     int(bc),
#                     tbm_obj,
#                     tbm_barcodes
#                 )

#         if not entry_rows:
#             continue

#         primary_machine = sanitize_text(machine_obj.get('machine', 'N/A'))

#         html_table = f"<p><strong>SKU: {sku} | Machine: {primary_machine}</strong></p>"
#         html_table += "<table border='1' cellpadding='5' cellspacing='0' style='border-collapse: collapse; width: 100%;'>"
#         html_table += "<thead><tr style='background-color: #4A90E2; color: white;'>"
#         html_table += "<th>Machine</th><th>Machine Type</th><th>Total Tyres</th>"
#         html_table += "<th>WH Num</th><th>WH Den</th><th>WAREHOUSE Yield %</th>"
#         html_table += "<th>UNI Num</th><th>UNI Den</th><th>UNIFORMITY Yield %</th>"
#         html_table += "<th>VIS Num</th><th>VIS Den</th><th>VISUAL Yield %</th>"
#         html_table += "<th>RFV Num</th><th>RFV Den</th><th>RFV Yield %</th>"
#         html_table += "<th>RFH1 Num</th><th>RFH1 Den</th><th>RFH1 Yield %</th>"
#         html_table += "<th>LFV Num</th><th>LFV Den</th><th>LFV Yield %</th>"
#         html_table += "<th>CON Num</th><th>CON Den</th><th>Conicity Yield %</th>"
#         html_table += "<th>STA Num</th><th>STA Den</th><th>Static Yield %</th>"
#         html_table += "<th>UPP Num</th><th>UPP Den</th><th>Upper Yield %</th>"
#         html_table += "<th>LOW Num</th><th>LOW Den</th><th>Lower Yield %</th>"
#         html_table += "<th>BUL Num</th><th>BUL Den</th><th>Bulge Yield %</th>"
#         html_table += "<th>DEN Num</th><th>DEN Den</th><th>Dent Yield %</th>"
#         html_table += "</tr></thead><tbody>"

#         def _format_yield_cell(percent, num, den, spec_val):
#             color = _color(percent, spec_val)
#             if percent is None or percent == 0:
#                 return f"0.00% {_ind(color)}"
#             return f"{_fmt(percent)} {_ind(color)}"

#         for row in entry_rows:
#             mname = row['mname']
#             mtype = row['mtype']
#             bc = row['bc']
            
#             wh_percent, wh_num, wh_den = row['wh']
#             uni_percent, uni_num, uni_den = row['uni']
#             vis_percent, vis_num, vis_den = row['vis']
#             rfv_percent, rfv_num, rfv_den = row['rfv']
#             rfh1_percent, rfh1_num, rfh1_den = row['rfh1']
#             lfv_percent, lfv_num, lfv_den = row['lfv']
#             conicity_percent, conicity_num, conicity_den = row['conicity']
#             static_percent, static_num, static_den = row['static']
#             upper_percent, upper_num, upper_den = row['upper']
#             lower_percent, lower_num, lower_den = row['lower']
#             bulge_percent, bulge_num, bulge_den = row['bulge']
#             dent_percent, dent_num, dent_den = row['dent']

#             html_table += "<tr>"
#             html_table += f"<td>{mname}</td><td>{mtype}</td><td>{bc}</td>"
            
#             html_table += f"<td>{_fmt_count(wh_num)}</td><td>{_fmt_count(wh_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(wh_percent, wh_num, wh_den, spec_wh)}</td>"
            
#             html_table += f"<td>{_fmt_count(uni_num)}</td><td>{_fmt_count(uni_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(uni_percent, uni_num, uni_den, spec_uni)}</td>"
            
#             html_table += f"<td>{_fmt_count(vis_num)}</td><td>{_fmt_count(vis_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(vis_percent, vis_num, vis_den, spec_vis)}</td>"
            
#             html_table += f"<td>{_fmt_count(rfv_num)}</td><td>{_fmt_count(rfv_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(rfv_percent, rfv_num, rfv_den, spec_rfv)}</td>"
            
#             html_table += f"<td>{_fmt_count(rfh1_num)}</td><td>{_fmt_count(rfh1_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(rfh1_percent, rfh1_num, rfh1_den, spec_rfh1)}</td>"
            
#             html_table += f"<td>{_fmt_count(lfv_num)}</td><td>{_fmt_count(lfv_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(lfv_percent, lfv_num, lfv_den, spec_lfv)}</td>"
            
#             html_table += f"<td>{_fmt_count(conicity_num)}</td><td>{_fmt_count(conicity_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(conicity_percent, conicity_num, conicity_den, spec_conicity)}</td>"
            
#             html_table += f"<td>{_fmt_count(static_num)}</td><td>{_fmt_count(static_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(static_percent, static_num, static_den, spec_static)}</td>"
            
#             html_table += f"<td>{_fmt_count(upper_num)}</td><td>{_fmt_count(upper_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(upper_percent, upper_num, upper_den, spec_upper)}</td>"
            
#             html_table += f"<td>{_fmt_count(lower_num)}</td><td>{_fmt_count(lower_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(lower_percent, lower_num, lower_den, spec_lower)}</td>"
            
#             html_table += f"<td>{_fmt_count(bulge_num)}</td><td>{_fmt_count(bulge_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(bulge_percent, bulge_num, bulge_den, spec_bulge)}</td>"
            
#             html_table += f"<td>{_fmt_count(dent_num)}</td><td>{_fmt_count(dent_den)}</td>"
#             html_table += f"<td>{_format_yield_cell(dent_percent, dent_num, dent_den, spec_dent)}</td>"
            
#             html_table += "</tr>"

#         html_table += "</tbody></table>"
#         breakdown_blocks.append(html_table)

#     machine_breakdown_html = "<br><br>".join(breakdown_blocks) if breakdown_blocks else "<p>No data available.</p>"

#     payload = {
#         "title": title_str,
#         "text": (
#             f"**{title_str}**\n\n"
#             f"{markdown_table}\n\n"
#             f"---\n\n"
#             f"**Machine Breakdown**\n\n"
#             f"{machine_breakdown_html}"
#         )
#     }

#     try:
#         response = requests.post(
#             teams_webhook_url,
#             json=payload,
#             headers={"Content-Type": "application/json"}
#         )
#         if response.status_code in (200, 202):
#             print(f"[INFO] Teams alert sent successfully for {alert_type} alerts")
#             return True
#         else:
#             print(f"[ERROR] Failed to send Teams alert: {response.status_code} {response.text}")
#             return False
#     except Exception as e:
#         print(f"[ERROR] Exception sending Teams alert: {e}")
#         return False

# def format_value(v, decimals=2):
#     if v is None or pd.isna(v):
#         return "N/A"
#     try:
#         return f"{float(v):.{decimals}f}"
#     except:
#         return "N/A"

# def format_datetime(dt, fmt="%d-%m-%Y %I:%M:%S%p"):
#     if not dt or dt == "":
#         return None

#     if isinstance(dt, dict) and "$date" in dt:
#         dt = dt["$date"]

#     try:
#         dt_parsed = pd.to_datetime(dt, errors='coerce')
#         if pd.isnull(dt_parsed):
#             return None
#         return dt_parsed.strftime(fmt)
#     except Exception:
#         return None
    
# def format_shift_date(dt):
#     if not dt or dt == "":
#         return None
#     if isinstance(dt, dict) and "$date" in dt:
#         dt = dt["$date"]
#     try:
#         dt_parsed = pd.to_datetime(dt, errors='coerce')
#         if pd.isnull(dt_parsed):
#             return None
#         return dt_parsed.strftime("%Y-%B-%d")
#     except Exception:
#         return None

# # PDF class
# class PDF(FPDF):
#     def __init__(self):
#         super().__init__('L','mm',(300,250))
#         self.set_auto_page_break(True,15)
#         self.set_margins(10,10,10)

#     def footer(self):
#         self.set_y(-15)
#         self.set_font('Arial','I',8)
#         self.cell(0,10,f'Page {self.page_no()}',0,0,'C')

#     def section_banner(self, text):
#         self.set_fill_color(238,174,193)
#         self.set_text_color(0)
#         self.set_font('Arial','B',10)
#         self.cell(0,8,sanitize_text(text),0,1,'C',fill=True)
#         self.ln(4)

#     def grouped_parameter_table(self, headers, widths, group_defs, data, cols):
#         self.set_fill_color(106, 13, 173)
#         self.set_text_color(255)
#         self.set_font('Arial', 'B', 9)
        
#         def print_header():
#             self.cell(widths[0], 7, "Stage", 1, 0, 'C', fill=True)
#             for w, h in zip(widths[1:], headers):
#                 self.cell(w, 7, sanitize_text(h), 1, 0, 'C', fill=True)
#             self.ln()

#         print_header()
#         self.set_text_color(0)

#         row_h = 6
#         for group_label, keys in group_defs:
#             total_rows = len(keys)
#             total_height = row_h * total_rows

#             if self.get_y() + total_height + 10 > self.h - self.b_margin:
#                 self.add_page()
#                 print_header()
#                 self.set_text_color(0)
            
#             y_start = self.get_y()
            
#             for i, key in enumerate(keys):
#                 border = 'L'
#                 if i == 0:
#                     border += 'T'
#                 if i == len(keys) - 1:
#                     border += 'B'
                
#                 self.cell(widths[0], row_h, "", border)
#                 param_name = sanitize_text(key.replace('_', ' ').upper())
#                 self.cell(widths[1], row_h, param_name, 1)
                
#                 for col_name, w in zip(cols, widths[2:]):
#                     val = data.get(key, {}).get(col_name)
#                     vs = format_value(val)
#                     hl = ((col_name == 'difference' and data.get(key, {}).get('shift_occurred_thershold')) or \
#                         (col_name == 'spec_diff' and data.get(key, {}).get('shift_occurred_spec')))
#                     if hl:
#                         self.set_fill_color(255, 0, 0)
#                         self.set_text_color(255)
#                         self.cell(w, row_h, vs, 1, 0, 'C', fill=True)
#                         self.set_fill_color(255)
#                         self.set_text_color(0)
#                     else:
#                         self.cell(w, row_h, vs, 1, 0, 'C')
#                 self.ln()
            
#             self.set_xy(self.l_margin, y_start)
#             self.set_font('Arial', 'B', 9)
#             self.cell(widths[0], total_height, sanitize_text(group_label), 'LR', 0, 'C')
#             self.set_xy(self.l_margin, y_start + total_height)
#             self.ln(2)
    
#     def grouped_summary(self, machines):
#         usable = self.w - self.l_margin - self.r_margin
#         first  = usable * 0.40
#         each = (usable - first) / (2 * len(machines))
#         widths = [first] + [each, each] * len(machines)
#         h = 6

#         self.set_font('Arial','B',9)
#         self.cell(widths[0],h,'UBNO Machine',1)
#         for m in machines:
#             self.cell(widths[1] + widths[2], h, sanitize_text(m['uf_id']), 1, 0, 'C')
#         self.ln()

#         self.set_font('Arial','',9)
#         self.cell(widths[0], h, 'Start Time', 1)
#         for m in machines:
#             self.cell(widths[1] + widths[2], h, str(format_datetime(m['start'])), 1, 0, 'C')
#         self.ln()

#         self.cell(widths[0],h,'End Time',1)
#         for m in machines:
#             self.cell(widths[1] + widths[2], h, str(format_datetime(m['end'])), 1, 0, 'C')
#         self.ln()

#         self.cell(widths[0], h, 'Barcode Count', 1)
#         for m in machines:
#             self.cell(widths[1] + widths[2], h, str(m['barcode_count']), 1, 0, 'C')
#         self.ln(5)

#         self.set_fill_color(106,13,173)
#         self.set_text_color(255)
#         self.set_font('Arial','B',9)
#         self.cell(widths[0], h, 'Parameters', 1, 0, 'C', fill=True)
#         for _ in machines:
#             self.cell(widths[1], h, 'SPEC', 1, 0, 'C', fill=True)
#             self.cell(widths[2], h, 'AVG', 1, 0, 'C', fill=True)
#         self.ln()
#         self.set_text_color(0)

#         for key in PARAM_KEYS:
#             self.cell(widths[0], h, sanitize_text(key.replace('_',' ').upper()), 1)
#             for m in machines:
#                 avg = format_value(m['averages'].get(f'avg_{key}') if f'avg_{key}' in m['averages'] else m['averages'].get(key))
#                 spec = format_value(m['specs'].get(key))
#                 self.cell(widths[1], h, spec, 1, 0, 'C')
#                 if m['parameter_map'].get(key):
#                     self.set_fill_color(255,0,0)
#                     self.set_text_color(255)
#                     self.cell(widths[2], h, avg, 1, 0, 'C', fill=True)
#                     self.set_fill_color(255)
#                     self.set_text_color(0)
#                 else:
#                     self.cell(widths[2], h, avg, 1, 0, 'C')
#             self.ln()

#         for grade in [3, 4, 5]:
#             grade_key = f"overallufmgrade_{grade}"
#             label     = f"OVERALL UFM GRADE {grade}"
#             self.cell(widths[0], h, label, 1)
#             for m in machines:
#                 avg  = format_value(m['averages'].get(grade_key))
#                 spec = format_value(m['specs'].get(grade_key))
#                 self.cell(widths[1], h, spec, 1, 0, 'C')
#                 if m['parameter_map'].get(grade_key):
#                     self.set_fill_color(255,0,0)
#                     self.set_text_color(255)
#                     self.cell(widths[2], h, avg, 1, 0, 'C', fill=True)
#                     self.set_fill_color(255)
#                     self.set_text_color(0)
#                 else:
#                     self.cell(widths[2], h, avg, 1, 0, 'C')
#             self.ln()
#         self.ln(5)

#         if len(machines) > 1:
#             barcode_width = usable / len(machines)
#             self.set_fill_color(106, 13, 173)
#             self.set_text_color(255)
#             self.set_font('Arial', 'B', 9)
#             for m in machines:
#                 self.cell(barcode_width, h, f"{sanitize_text(m['uf_id'])} Barcodes", 1, 0, 'C', fill=True)
#             self.ln()
#             self.set_text_color(0)
#             self.set_font('Arial', '', 9)
            
#             max_rows = max(len(m['barcodes']) // 2 + (1 if len(m['barcodes']) % 2 else 0) for m in machines)
#             for row in range(max_rows):
#                 x_start = self.l_margin
#                 for m in machines:
#                     self.set_x(x_start)
#                     chunk_size = 2
#                     start_idx = row * chunk_size
#                     chunk = m['barcodes'][start_idx:start_idx + chunk_size]
#                     barcode_str = ", ".join(sanitize_text(str(b)) for b in chunk if b is not None) if chunk else ""
#                     self.cell(barcode_width, h, barcode_str, 1, 0, 'L')
#                     x_start += barcode_width
#                 self.ln()
#         else:
#             m = machines[0]
#             self.set_fill_color(106, 13, 173)
#             self.set_text_color(255)
#             self.set_font('Arial', 'B', 9)
#             self.cell(usable, h, f"{sanitize_text(m['uf_id'])} Barcodes", 1, 0, 'C', fill=True)
#             self.ln()
#             self.set_text_color(0)
#             self.set_font('Arial', '', 9)
            
#             barcodes = m['barcodes']
#             chunk_size = 5
#             barcode_width = usable / chunk_size
#             total_rows = len(barcodes) // chunk_size + (1 if len(barcodes) % chunk_size else 0)

#             for row in range(total_rows):
#                 start_idx = row * chunk_size
#                 chunk = barcodes[start_idx:start_idx + chunk_size]
#                 for b in chunk:
#                     self.cell(barcode_width, h, sanitize_text(str(b)) if b is not None else "", 1, 0, 'L')
#                 for _ in range(chunk_size - len(chunk)):
#                     self.cell(barcode_width, h, "", 1, 0, 'L')
#                 self.ln()
    
#     def barcodes_table(self, barcodes, width):
#         self.set_fill_color(106, 13, 173)
#         self.set_text_color(255)
#         self.set_font('Arial', 'B', 9)
#         self.cell(width, 7, "Barcodes", 1, 0, 'C', fill=True)
#         self.ln()
#         self.set_text_color(0)
#         self.set_font('Arial', '', 9)

#         row_h = 6
#         if not barcodes or not isinstance(barcodes, list):
#             self.cell(width, row_h, "No barcodes available", 1, 0, 'C')
#             self.ln()
#             return

#         chunk_size = 5
#         max_barcodes = 100
#         for i in range(0, min(len(barcodes), max_barcodes), chunk_size):
#             chunk = barcodes[i:i + chunk_size]
#             barcode_str = ", ".join(sanitize_text(str(b)) for b in chunk if b is not None)
#             self.cell(width, row_h, barcode_str, 1, 0, 'L')
#             self.ln()

#     def shift_data_table(self, shifts_data, machine_name, alert_type, width):
#         self.set_fill_color(238, 174, 193)
#         self.set_text_color(0)
#         self.set_font('Arial', 'B', 10)
#         self.cell(0, 8, "TBM SHIFT DATA", 0, 1, 'C', fill=True)
#         self.ln(4)

#         self.set_fill_color(106, 13, 173)
#         self.set_text_color(255)
#         self.set_font('Arial', 'B', 9)
#         self.cell(width, 7, "TBM Machine", 1, 0, 'C', fill=True)
#         self.ln()
#         self.set_text_color(0)
#         self.set_font('Arial', '', 9)
#         self.cell(width, 6, sanitize_text(machine_name), 1, 0, 'C')
#         self.ln(5)

#         if not shifts_data or not isinstance(shifts_data, list):
#             self.set_font('Arial', 'B', 9)
#             self.cell(width, 6, "No shift data available", 1, 0, 'C')
#             self.ln()
#             return
                  
#         shift_info = []
#         for shift in shifts_data:
#             shift_name = shift.get('shift', 'N/A')
#             start_time = shift.get('machine_shift_tbm_start_time', None)
#             barcodes = shift.get('barcodes', [])
#             formatted_date = format_shift_date(start_time)
#             if formatted_date and shift_name != 'N/A':
#                 shift_display = f"{formatted_date}/{shift_name}"
#             else:
#                 shift_display = shift_name
#             shift_info.append({
#                 'shift_display': shift_display,
#                 'barcodes': barcodes
#             })

#         shift_info = shift_info[:3]
#         num_shifts = len(shift_info)
#         if num_shifts == 0:
#             self.set_font('Arial', 'B', 9)
#             self.cell(width, 6, "No valid shift data available", 1, 0, 'C')
#             self.ln()
#             return

#         col_width = width / max(num_shifts, 1)
#         row_h = 6
#         self.set_fill_color(106, 13, 173)
#         self.set_text_color(255)
#         self.set_font('Arial', 'B', 9)
#         for shift in shift_info:
#             self.cell(col_width, 7, "Shift", 1, 0, 'C', fill=True)
#         self.ln()
#         self.set_text_color(0)
#         self.set_font('Arial', '', 9)
#         for shift in shift_info:
#             self.cell(col_width, row_h, sanitize_text(shift['shift_display']), 1, 0, 'C')
#         self.ln(5)

#         self.set_fill_color(106, 13, 173)
#         self.set_text_color(255)
#         self.set_font('Arial', 'B', 9)
#         for _ in shift_info:
#             self.cell(col_width, 7, "Barcodes", 1, 0, 'C', fill=True)
#         self.ln()
#         self.set_text_color(0)
#         self.set_font('Arial', '', 9)

#         max_barcodes = max(len(shift['barcodes']) for shift in shift_info) if shift_info else 0
#         for row in range(max_barcodes):
#             for shift in shift_info:
#                 barcode = shift['barcodes'][row] if row < len(shift['barcodes']) else ""
#                 self.cell(col_width, row_h, sanitize_text(str(barcode)), 1, 0, 'L')
#             self.ln()

# # PDF generation
# def generate_alert_pdf(doc, atype):
#     entry = doc['machine_alerts'][0]
#     alert = {
#         **entry,
#         'sku': doc.get('sku', entry.get('sku', 'N/A')),
#         'target_machine_lot_size': doc.get('target_machine_lot_size'),
#         atype['alert_time_key']: doc.get(atype['alert_time_key'])
#     }

#     raw  = alert['parameter_alerts']
#     data = {p['parameter']: p for p in raw}
#     for p in data.values():
#         c, s = p.get('current_value'), p.get('spec_value')
#         p['spec_diff'] = float(c)-float(s) if c is not None and s is not None else None

#     all_ufm      = PARAM_KEYS[:7]
#     all_geometery = PARAM_KEYS[7:15]
#     all_bal      = PARAM_KEYS[15:18]
#     all_yields   = PARAM_KEYS[18:]
#     ufm_keys      = [k for k in all_ufm if k in data]
#     geometry_keys = [k for k in all_geometery if k in data]
#     bal_keys      = [k for k in all_bal if k in data]
#     yields_keys   = [k for k in all_yields if k in data]

#     pdf = PDF()
#     pdf.add_page()

#     usable = pdf.w - pdf.l_margin - pdf.r_margin
#     half   = usable / 2
#     x1     = pdf.l_margin
#     x2     = pdf.l_margin + half
#     label_1=55
#     value_1=half-label_1
#     line_h = 6

#     pdf.set_xy(x1, pdf.get_y())
#     pdf.set_font('Arial','B',11)
#     pdf.cell(half, line_h, f"{sanitize_text(atype['name'])} ALERT", 0, 0, 'L')

#     raw_ts = alert.get(atype['alert_time_key'])

#     if isinstance(raw_ts, dict) and '$date' in raw_ts:
#         raw_ts = raw_ts['$date']

#     ts_str = None
#     if raw_ts:
#         try:
#             ts_parsed = pd.to_datetime(raw_ts, errors='coerce', utc=True)
#             if not pd.isna(ts_parsed):
#                 ts_str = ts_parsed.strftime("%Y%m%d_%H%M%S")
#             else:
#                 print(f"[DEBUG] Timestamp parsing resulted in NaT for raw_ts: {raw_ts}")
#         except Exception as e:
#             print(f"[DEBUG] Error parsing timestamp {raw_ts}: {e}")

#     atype_name = sanitize_text(str(atype.get('name', 'UNKNOWN')))
#     machine = sanitize_text(str(alert.get('machine', 'UNKNOWN')))
#     gt_code = sanitize_text(str(alert.get('sku', 'UNKNOWN')))
#     identifier = f":{atype_name}_{machine}_{gt_code}_{ts_str if ts_str else 'None'}"

#     pdf.set_font('Arial','B',11)
#     pdf.set_xy(x2, pdf.get_y())
#     pdf.cell(value_1, line_h, sanitize_text(identifier), 0, 1, 'R')

#     pdf.ln(2)

#     label_w, h = 40, 6
#     val_w = half - label_w
#     y = pdf.get_y()

#     pdf.set_xy(x1,y)
#     pdf.set_font('Arial','B',11); pdf.cell(label_w,h,"SKU",0,0)
#     pdf.set_font('Arial','',11);  pdf.cell(val_w,h,f": {sanitize_text(alert['sku'])}",0,0)
#     pdf.set_xy(x2,y)
#     pdf.set_font('Arial','B',11); pdf.cell(label_w,h,"Lot Size",0,0)
#     pdf.set_font('Arial','',11);  pdf.cell(val_w,h,f": {sanitize_text(alert['processed_machine_barcodes'])}",0,1)

#     y = pdf.get_y()
#     pdf.set_xy(x1,y)
#     pdf.set_font('Arial','B',11); pdf.cell(label_w,h,"Machine No",0,0)
#     pdf.set_font('Arial','',11);  pdf.cell(val_w,h,f": {sanitize_text(alert['machine'])}",0,1)
#     pdf.set_xy(x2,y)
#     pdf.set_font('Arial','B',11); pdf.cell(label_w,h,"Size Code",0,0)
#     pdf.set_font('Arial','',11);  pdf.cell(val_w,h,f": {sanitize_text(alert['sizecode'])}",0,1)

#     y = pdf.get_y()
#     pdf.set_xy(x1,y)
#     pdf.set_font('Arial','B',11); pdf.cell(label_w,h,"Gt Code",0,0)
#     pdf.set_font('Arial','',11);  pdf.cell(val_w,h,f": {sanitize_text(alert['gt_code'])}",0,1)

#     y = pdf.get_y()
#     pdf.set_xy(x1,y)
#     pdf.set_font('Arial','B',11); pdf.cell(label_w,h,f"{sanitize_text(atype['name'])} Start Time",0,0)
#     pdf.set_font('Arial','',11);  pdf.cell(val_w,h,f": {format_datetime(alert[atype['start_key']])}",0,0)
#     pdf.set_xy(x2,y)
#     pdf.set_font('Arial','B',11); pdf.cell(label_w,h,f"{sanitize_text(atype['name'])} End Time",0,0)
#     pdf.set_font('Arial','',11);  pdf.cell(val_w,h,f": {format_datetime(alert[atype['end_key']])}",0,1)
#     pdf.ln(5)

#     pdf.section_banner(f"{sanitize_text(atype['name'])} STAGE")
#     usable = pdf.w - pdf.l_margin - pdf.r_margin

#     group_w = 25
#     param_w = usable * 0.45 - group_w
#     val_w = (usable - group_w - param_w) / 3
#     widths = [group_w, param_w, val_w, val_w, val_w]

#     ufm_keys      = [k for k in PARAM_KEYS[:10] if k in data]
#     geometry_keys = [k for k in PARAM_KEYS[10:20] if k in data]
#     bal_keys      = [k for k in PARAM_KEYS[20:23] if k in data]
#     yields_keys   = [k for k in PARAM_KEYS[23:] if k in data]
#     group_defs = [("UFM", ufm_keys), ("GEOMETRY", geometry_keys), ("BALANCING", bal_keys), ("YIELDS", yields_keys)]

#     pdf.grouped_parameter_table(
#         headers=["Parameters", "PREV", "CURRENT", "DIFF"],
#         widths=widths,
#         group_defs=group_defs,
#         data=data,
#         cols=['previous_value', 'current_value', 'difference']
#     )

#     pdf.ln(5)
#     pdf.section_banner("SPECIFICATIONS")

#     pdf.grouped_parameter_table(
#         headers=["Parameters","SPEC","CURRENT","SPEC DIFF"],
#         widths=widths,
#         group_defs=[("UFM", ufm_keys), ("GEOMETRY", geometry_keys), ("BALANCING", bal_keys), ("YIELDS", yields_keys)],
#         data=data,
#         cols=['spec_value','current_value','spec_diff']
#     )

#     if atype['name'].upper() == "UFM":
#         pdf.ln(10)
#         pdf.section_banner("BARCODES")
#         barcodes = alert.get('barcodes', [])
#         pdf.barcodes_table(barcodes, usable)

#     if atype['use_grouped']:
#         raw_group = entry['grouped_averages']['uf_id']
#         machines = []
#         for rec in raw_group:
#             param_map = {
#                 p['parameter']: p['spec_threshold_exceeded']
#                 for p in rec.get('parameter_list', [])
#             }
#             spec_map = {
#                 p['parameter']: p['spec_value']
#                 for p in rec.get('parameter_list', [])
#             }
#             averages = {
#                 p['parameter']: p['avg_value']
#                 for p in rec.get('parameter_list', [])
#             }
#             machines.append({
#                 'ubno': rec['uf_id'],
#                 'uf_id': rec['uf_id'],
#                 'start': rec['tub_date_start_time'],
#                 'end':   rec['tub_date_end_time'],
#                 'barcode_count': rec.get('barcode_count', 0),
#                 'averages': averages,
#                 'parameter_map': param_map,
#                 'specs':         spec_map,
#                 'barcodes': rec.get('barcodes', []),
#             })

#         pdf.section_banner(f"UFM STAGE")
#         pdf.grouped_summary(machines)
#         pdf.ln(10)
    
#     pdf.ln(10)
#     shift_data = alert.get(atype['shift_data_key'], [])
#     machine_name = alert.get('tbm_machine' if atype['name'].upper() in ['CURING', 'UFM'] else 'machine', 'N/A')
#     pdf.shift_data_table(shift_data, machine_name, atype['name'], usable)
                                 
#     return pdf.output(dest='S')

# def get_admin_emails(client):
#     cols = client[alert_mail_db][alert_users]
#     users = cols.find(
#         {"is_alert_user": True},
#         {"_id": 0, "email": 1}
#     )
#     emails = [u["email"] for u in users if u.get("email")]
#     return emails

# def send_alerts():
#     client = MongoClient(current_app.config['MONGO_URI'])
#     db = client[ALERT_DB_NAME]
#     emails = get_admin_emails(client)
    
#     if not emails:
#         print("[ERROR] No admin emails")
#         return

#     ts = datetime.now().strftime("%Y%m%d_%H%M%S")
#     total_pdfs_processed = 0

#     all_alerts = []
#     for at in ALERT_TYPES:
#         coll = db[at['coll']]
#         docs = list(coll.find({'alert_flag_repl': False}))
#         if docs:
#             summary_html = generate_combined_summary_table(docs, at['name'])
#             send_teams_alert(summary_html, at['name'], docs)
#             for doc in docs:
#                 doc['alert_type'] = at
#                 all_alerts.append(doc)

#     if not all_alerts:
#         print("[INFO] No new alerts.")
#         return

#     alerts_by_modelno = {}
#     for doc in all_alerts:
#         modelno = sanitize_text(doc.get('sku', 'N/A'))
#         if modelno not in alerts_by_modelno:
#             alerts_by_modelno[modelno] = []
#         alerts_by_modelno[modelno].append(doc)

#     for modelno, docs in alerts_by_modelno.items():
#         size_codes = sorted(set(sanitize_text(m.get('sizecode', 'N/A')) for d in docs for m in d.get('machine_alerts', [])))
#         sizecode_str = ", ".join(size_codes) if size_codes else "N/A"

#         pdf_list = []
#         for doc in docs:
#             at = doc['alert_type']
#             pdf_bytes = generate_alert_pdf(doc, at)
#             fn = f"{at['filename_prefix']}_{sanitize_text(doc['machine_alerts'][0]['machine'])}_{modelno}_{ts}.pdf"
#             pdf_list.append({
#                 'bytes': pdf_bytes,
#                 'filename': fn,
#                 'doc_id': doc['_id'],
#                 'collection': at['coll'],
#                 'alert_type': at['name']
#             })

#         current_batch = []
#         current_size = 0
#         batch_number = 1
#         batched_doc_ids = []

#         for pdf in pdf_list:
#             pdf_size = len(pdf['bytes'])
#             pdf_size_encoded = pdf_size * 1.33
#             email_overhead = len(json.dumps({
#                 "message": {
#                     "subject": f"PCR GYO - Yield Alert: SKU - {modelno} | Size Code - {sizecode_str} (Batch {batch_number})",
#                     "body": {"contentType": "Text", "content": f"Attached: Reports for {', '.join(sorted(set(p['alert_type'] for p in current_batch + [pdf])))}.\n\n"},
#                     "toRecipients": [{"emailAddress": {"address": e}} for e in emails],
#                     "attachments": []
#                 },
#                 "saveToSentItems": True
#             }).encode('utf-8')) * 1.33

#             if current_size + pdf_size_encoded + email_overhead > MAX_EMAIL_SIZE or len(current_batch) >= 180:
#                 if current_batch:
#                     alert_types = sorted(set(p['alert_type'] for p in current_batch))
#                     alert_types_str = ", ".join(alert_types)
#                     endpoint = f"https://graph.microsoft.com/v1.0/users/{mail_config.shared_mailbox}/sendMail"
#                     email_data = {
#                         "message": {
#                             "subject": f"PCR GYO - Yield Alert: SKU - {modelno} | Size Code - {sizecode_str} (Batch {batch_number})",
#                             "body": {"contentType": "Text", "content": f"Attached: Reports for {alert_types_str}.\n\n"},
#                             "toRecipients": [{"emailAddress": {"address": e}} for e in emails],
#                             "attachments": [
#                                 {
#                                     '@odata.type': '#microsoft.graph.fileAttachment',
#                                     'Name': pdf_item['filename'],
#                                     'ContentBytes': base64.b64encode(pdf_item['bytes'].encode('latin-1')).decode('utf-8')
#                                 } for pdf_item in current_batch
#                             ]
#                         },
#                         "saveToSentItems": True
#                     }
#                     headers = {
#                         "Authorization": f"Bearer {mail_config.access_token}",
#                         "Content-Type": "application/json"
#                     }
#                     try:
#                         resp = requests.post(endpoint, headers=headers, json=email_data)
#                         if resp.status_code == 202:
#                             print(f"[INFO] Batch {batch_number} emailed via Microsoft Graph successfully for SKU {modelno}.")
#                             for pdf_item in current_batch:
#                                 db[pdf_item['collection']].update_one(
#                                     {'_id': pdf_item['doc_id']},
#                                     {'$set': {'alert_flag_repl': True, doc['alert_type']['alert_time_key']: datetime.now()}}
#                                 )
#                             batched_doc_ids.extend([pdf_item['doc_id'] for pdf_item in current_batch])
#                             total_pdfs_processed += len(current_batch)
#                         else:
#                             print(f"[ERROR] Graph API sendMail failed for batch {batch_number} (SKU {modelno}): {resp.status_code} {resp.text}")
#                     except Exception as e:
#                         print(f"[ERROR] Exception while sending batch {batch_number} (SKU {modelno}): {e}")
#                         traceback.print_exc()
#                     batch_number += 1
#                     current_batch = []
#                     current_size = 0
#                 if pdf_size_encoded + email_overhead > MAX_EMAIL_SIZE:
#                     print(f"[WARN] Single PDF ({pdf['filename']}) exceeds size limit, skipping.")
#                     continue
#             current_batch.append(pdf)
#             current_size += pdf_size_encoded
        
#         if current_batch:
#             alert_types = sorted(set(p['alert_type'] for p in current_batch))
#             alert_types_str = ", ".join(alert_types)
#             endpoint = f"https://graph.microsoft.com/v1.0/users/{mail_config.shared_mailbox}/sendMail"
#             email_data = {
#                 "message": {
#                     "subject": f"PCR GYO - Yield Alert: SKU - {modelno} | Size Code - {sizecode_str} (Batch {batch_number})",
#                     "body": {"contentType": "Text", "content": f"Attached: Reports for {alert_types_str}.\n\n"},
#                     "toRecipients": [{"emailAddress": {"address": e}} for e in emails],
#                     "attachments": [
#                         {
#                             '@odata.type': '#microsoft.graph.fileAttachment',
#                             'Name': pdf_item['filename'],
#                             'ContentBytes': base64.b64encode(pdf_item['bytes'].encode('latin-1')).decode('utf-8')
#                         } for pdf_item in current_batch
#                     ]
#                 },
#                 "saveToSentItems": True
#             }
#             headers = {
#                 "Authorization": f"Bearer {mail_config.access_token}",
#                 "Content-Type": "application/json"
#             }
#             try:
#                 resp = requests.post(endpoint, headers=headers, json=email_data)
#                 if resp.status_code == 202:
#                     print(f"[INFO] Final batch {batch_number} emailed via Microsoft Graph successfully for SKU {modelno}.")
#                     for pdf_item in current_batch:
#                         db[pdf_item['collection']].update_one(
#                             {'_id': pdf_item['doc_id']},
#                             {'$set': {'alert_flag_repl': True, doc['alert_type']['alert_time_key']: datetime.now()}}
#                         )
#                     batched_doc_ids.extend([pdf_item['doc_id'] for pdf_item in current_batch])
#                     total_pdfs_processed += len(current_batch)
#                 else:
#                     print(f"[ERROR] Graph API sendMail failed for final batch {batch_number} (SKU {modelno}): {resp.status_code} {resp.text}")
#             except Exception as e:
#                 print(f"[ERROR] Exception while sending final batch {batch_number} (SKU {modelno}): {e}")
#                 traceback.print_exc()
        
#         print(f"[INFO] Processed {total_pdfs_processed} PDFs for SKU {modelno} in {batch_number} batches.")
    
#     if total_pdfs_processed == 0:
#         print("[INFO] No new alerts.")

# if __name__ == "__main__":
#     app = Flask(__name__)
#     app.config.update({
#         'MONGO_URI': MONGO_URI,
#         'SENDER_EMAIL': SENDER_EMAIL,
#         'SENDER_PASSWORD': SENDER_PASSWORD
#     })
#     with app.app_context():
#         send_alerts()