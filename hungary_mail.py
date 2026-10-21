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
# from config_hungary_uat import (
#     alert_triggering_ubno_collect_name,
#     alert_triggering_collect_name,
#     alert_triggering_curing_collect_name,
#     # alert_db,
#     alert_db_virgin_yield,
#     alert_mail_db,
#     alert_users,
#     config_databse,
#     channel_no_collect,                  
#     MONGO_URI,
#     SENDER_EMAIL,
#     SENDER_PASSWORD,
#     TEAMS_WEBHOOK_URL

# )

# # --- Configuration ---
# MONGO_URI       =  MONGO_URI
# SENDER_EMAIL    = SENDER_EMAIL
# SENDER_PASSWORD = SENDER_PASSWORD
# ALERT_DB_NAME   = alert_db_virgin_yield
# CONFIG_DB_NAME = config_databse  # NEW: Database for OE Replacement lookup
# CHANNEL_NO_COLLECT = channel_no_collect  # NEW: Collection for OE Replacement                                                                            
# BASE_PDF_DIR    = "generated_pdfs"
																			  
# MAX_EMAIL_SIZE = 20 * 1024 * 1024  # 20 MB																			   

# # Full list of possible keys
# PARAM_KEYS = [
#     "rfppcc","rfppcw","rfh1cc","rfh1cw","rfh2cc","rfh2cw","lfppcc","lfppcw","cony","plys","crro","trro","brro",
#     "lrot","lrob","lroh1grade","tbul","bbul","tdep","bdep","upper","lower","sbal","aircc","aircw","loadcc","loadcw",
#     "rfv_yield", "rfh1_yield", "rfh2_yield", "lfv_yield", "conicity_yield", "plysteer_yield","center_radial_run_out_yield", 
#     "top_radial_run_out_yield", "bottom_radial_run_out_yield", "lateral_run_out_yield", "lateral_run_out_harmonic_first_yield",
#     "center_radial_run_out_harmonic_first_yield", "top_radial_run_out_harmonic_first_yield", "bottom_radial_run_out_harmonic_first_yield",
#     "lateral_radial_run_out_yield", "static_yield", "couple_yield","upper_yield", "lower_yield", "overall_uniformity_yield", 
#     "overall_visual_yield", "overall_warehouse_yield","overall_scrap_yield", "visual_loss_yield", "uf_loss_yield", "oe_ok_yield"
# ]

# ALERT_TYPES = [
#     {
#         "name": "UFM",
#         "coll": alert_triggering_ubno_collect_name,
#         "banner": "UFM ALERT",
#         "filename_prefix": "UFM",
#         "start_key": "machine_lot_start_ufm_time",
#         "end_key":   "machine_lot_end_ufm_time",
#         "alert_time_key": "timestamp_alert_generated",
#         "use_grouped": False,
#         "shift_data_key": "shifts_data_tbm",
#         "shift_start_time_key": "machine_shift_tbm_start_time"
#     },
#     {
#         "name": "TBM",
#         "coll":  alert_triggering_collect_name,
#         "banner": "TBM ALERT",
#         "filename_prefix": "TBM",
#         "start_key": "machine_lot_start_build_time",
#         "end_key":   "machine_lot_end_build_time",
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
#         "end_key":   "machine_lot_end_curing_time",
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
#     # Convert to string and replace problematic Unicode characters
#     text = str(text)
#     # Replace \ufffd and other problematic characters with a safe alternative
#     return text.encode('ascii', 'replace').decode('ascii').replace('\ufffd', '?')			

# def html_table_to_markdown(html_table):
#     """
#     Converts HTML table to Markdown format for better Teams compatibility
#     """
#     # Parse the HTML
#     from bs4 import BeautifulSoup
#     soup = BeautifulSoup(html_table, 'html.parser')
    
#     # Initialize markdown table
#     markdown_table = ""
    
#     # Process headers
#     headers = [sanitize_text(th.get_text()) for th in soup.find_all('th')]
#     markdown_table += "| " + " | ".join(headers) + " |\n"
#     markdown_table += "|" + "|".join(["---"] * len(headers)) + "|\n"
    
#     # Process rows
#     for row in soup.find_all('tr')[1:]:  # Skip header row
#         cells = [sanitize_text(td.get_text()) for td in row.find_all('td')]
#         markdown_table += "| " + " | ".join(cells) + " |\n"
    
#     return markdown_table

# def generate_combined_summary_table(alerts, alert_type):
#     """
#     Changed 14-04
#     Generates properly formatted HTML table for all alerts.
    
#     Args:
#         alerts (list): List of alert dictionaries containing relevant data.
#         alert_type (str): Type of alert ("TBM", "CURING", or "UBNO").
    
#     Returns:
#         str: HTML table as a string.
#     """
#     # Check if alerts list is empty
#     if not alerts:
#         return "<table><tr><td>No alert data available</td></tr></table>"
    
#     # Start HTML table
#     html = "<table>"
    
#     # Define headers based on alert_type
#     if alert_type.upper() == "TBM":
#         html += """
#         <tr>
#             <th>S.No</th>
#             <th>SKU</th>
#             <th>Size Code</th>
#             <th>Gt Code</th>
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
#             <th>Size Code</th>
#             <th>Gt Code</th>
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
#             <th>Gt Code</th>
#             <th>UFM</th>
#             <th>Total Tyre Produced</th>
#             <th>Overall WAREHOUSE Yield (%)</th>
#             <th>Overall UNIFORMITY Yield (%)</th>
#             <th>Overall VISUAL Yield (%)</th>
#             <th>Overall SCRAP Yield (%)</th>
#         </tr>
#         """

#     # Handle different alert types
#     if alert_type.upper() == "TBM":
#         # For TBM alerts
#         # for alert in alerts:
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
#                     <td>{sanitize_text(machine.get('sizecode', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('gt_code', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('machine', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('processed_machine_barcodes', 'N/A'))}</td>
#                     <td>{warehouse}</td>
#                     <td>{uniformity}</td>
#                     <td>{visual}</td>
#                     <td>{scrap}</td>
#                 </tr>
#                 """
    
#     elif alert_type.upper() == "CURING":
#         # For CURING alerts
#         # for alert in alerts:
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
#                     <td>{sanitize_text(machine.get('sizecode', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('gt_code', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('machine', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('processed_machine_barcodes', 'N/A'))}</td>
#                     <td>{warehouse}</td>
#                     <td>{uniformity}</td>
#                     <td>{visual}</td>
#                     <td>{scrap}</td>
#                 </tr>
#                 """
    
#     elif alert_type.upper() == "UFM":
#     # For CURING alerts
#         # for alert in alerts:
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
#                     <td>{sanitize_text(machine.get('sizecode', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('gt_code', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('machine', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('processed_machine_barcodes', 'N/A'))}</td>
#                     <td>{warehouse}</td>
#                     <td>{uniformity}</td>
#                     <td>{visual}</td>
#                     <td>{scrap}</td>
#                 </tr>
#                 """
    
#     # Close the table
#     html += "</table>"
#     return html

# def send_teams_alert(summary_html, alert_type, docs):
#     """
#     Sends a Microsoft Teams alert message with properly formatted table.
#     Args:
#         summary_html (str): HTML string of the summary table.
#         alert_type (str): Type of alert (UFM, TBM, CURING).
#         docs (list): List of alert documents from MongoDB.
#     """   
#     teams_webhook_url = TEAMS_WEBHOOK_URL
#     if not teams_webhook_url:
#         print("[ERROR] TEAMS_WEBHOOK_URL not configured.")
#         return False

#     # ── Fetch yield spec thresholds from channel_no_collect ──
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
#              "spec_rfh2wc_yield": 1,
#              "spec_plys_yield": 1}
#         ):
#             sku_key = str(doc.get("sku", "")).strip()
#             if sku_key:
#                 yield_spec_lookup[sku_key] = {
#                     "spec_overall_warehouse_yield": doc.get("spec_overall_warehouse_yield"),
#                     "spec_overall_uniformity_yield": doc.get("spec_overall_uniformity_yield"),
#                     "spec_overall_visual_yield": doc.get("spec_overall_visual_yield"),
#                     "spec_rfv_yield":      doc.get("spec_rfppwc_yield"),
#                     "spec_rfh1_yield":     doc.get("spec_rfh1wc_yield"),
#                     "spec_rfh2_yield":     doc.get("spec_rfh2wc_yield"),
#                     "spec_lfv_yield":      doc.get("spec_rfppwc_yield"),
#                     "spec_plysteer_yield": doc.get("spec_plys_yield"),
#                 }
#         _mc.close()
#     except Exception as _e:
#         print(f"[WARN] Could not fetch yield specs: {_e}")

#     # ── Functions to calculate yields from batch data ──
#     def yield_percentage(numerator, denominator):
#         try:
#             if denominator is None or denominator == 0:
#                 return 0.0
#             return round((numerator / denominator) * 100, 2)
#         except (TypeError, ValueError, ZeroDivisionError):
#             return 0.0

#     def overall_warehouse_yield(batch_data):
#         try:
#             if batch_data is None or batch_data.empty:
#                 return 0.0, 0, 0
            
#             required_cols = ['overallufmgrade', 'barcode', 'scrap_category_tub', 'last']
#             missing_cols = [col for col in required_cols if col not in batch_data.columns]
#             if missing_cols:
#                 print(f"[WARN] Missing columns for warehouse yield: {missing_cols}")
#                 return 0.0, 0, len(batch_data)
            
#             measured_data = batch_data[
#                 (batch_data['overallufmgrade'].isin([1, 2, 3, 4, 5])) &
#                 (batch_data['barcode'].notnull())
#             ]
#             measured = len(measured_data)

#             vi_scrap_data = batch_data[
#                 (batch_data['scrap_category_tub'] == "VISUAL") & 
#                 (batch_data["scrap_category_tub"].notna())
#             ]
#             vi_scrap = len(vi_scrap_data)

#             denominator = measured + vi_scrap
#             if denominator == 0:
#                 denominator = len(batch_data)

#             numerator_data = batch_data[
#                 (batch_data['overallufmgrade'] == 1) &
#                 (batch_data['last'] == 1)
#             ]
#             numerator = len(numerator_data)

#             return yield_percentage(numerator, denominator), numerator, denominator
#         except Exception as e:
#             print(f"[ERROR] Error calculating warehouse yield: {e}")
#             return 0.0, 0, len(batch_data) if batch_data is not None else 0

#     def overall_uniformity_yield(batch_data):
#         try:
#             if batch_data is None or batch_data.empty:
#                 return 0.0, 0, 0
            
#             required_cols = ['overallufmgrade', 'barcode', 'vi_yield', 'last']
#             missing_cols = [col for col in required_cols if col not in batch_data.columns]
#             if missing_cols:
#                 print(f"[WARN] Missing columns for uniformity yield: {missing_cols}")
#                 return 0.0, 0, len(batch_data)
            
#             measured_data = batch_data[
#                 (batch_data['overallufmgrade'].isin([1, 2, 3, 4, 5])) &
#                 (batch_data['barcode'].notnull())
#             ]
#             measured = len(measured_data)

#             vi_downgrade_data = batch_data[
#                 (batch_data['vi_yield'] == 0) & 
#                 (batch_data['last'] == 1) & 
#                 (batch_data["vi_yield"].notna())
#             ]
#             vi_downgrade = len(vi_downgrade_data)

#             denominator = measured - vi_downgrade
#             if denominator <= 0:
#                 denominator = measured if measured > 0 else len(batch_data)

#             numerator_data = batch_data[
#                 (batch_data['overallufmgrade'] == 1) &
#                 (batch_data['last'] == 1)
#             ]
#             numerator = len(numerator_data)

#             return yield_percentage(numerator, denominator), numerator, denominator
#         except Exception as e:
#             print(f"[ERROR] Error calculating uniformity yield: {e}")
#             return 0.0, 0, len(batch_data) if batch_data is not None else 0

#     def overall_visual_yield(batch_data):
#         try:
#             if batch_data is None or batch_data.empty:
#                 return 0.0, 0, 0
            
#             required_cols = ['vi_yield', 'last', 'scrap_category_tub', 'overallufmgrade']
#             missing_cols = [col for col in required_cols if col not in batch_data.columns]
#             if missing_cols:
#                 print(f"[WARN] Missing columns for visual yield: {missing_cols}")
#                 return 0.0, 0, len(batch_data)
            
#             vi_downgrade_data = batch_data[
#                 (batch_data['vi_yield'] == 0) &
#                 (batch_data['last'] == 1)
#             ]
#             vi_downgrade = len(vi_downgrade_data)

#             vi_scrap_data = batch_data[
#                 batch_data['scrap_category_tub'] == "VISUAL"
#             ]
#             vi_scrap = len(vi_scrap_data)

#             ufm_data = batch_data[
#                 batch_data['overallufmgrade'].isin([1, 2, 3, 4, 5])
#             ]
#             ufm_count = len(ufm_data)

#             possible_oe = (ufm_count + vi_scrap) - vi_downgrade
#             vi_pass = possible_oe if possible_oe > 0 else 0
#             denominator = vi_pass + vi_scrap
#             if denominator <= 0:
#                 denominator = len(batch_data)
#             numerator = vi_pass

#             return yield_percentage(numerator, denominator), numerator, denominator
#         except Exception as e:
#             print(f"[ERROR] Error calculating visual yield: {e}")
#             return 0.0, 0, len(batch_data) if batch_data is not None else 0

#     def rfv_yield(batch_data):
#         try:
#             if batch_data is None or batch_data.empty:
#                 return 0.0, 0, 0
            
#             required_cols = ['overallufmgrade', 'barcode', 'rfppwcgrade']
#             missing_cols = [col for col in required_cols if col not in batch_data.columns]
#             if missing_cols:
#                 print(f"[WARN] Missing columns for RFV yield: {missing_cols}")
#                 return 0.0, 0, len(batch_data)
            
#             # Denominator: count records with overallufmgrade 1-5 and barcode not null
#             denominator_data = batch_data[
#                 ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
#                 batch_data['barcode'].notnull()
#             ]
#             denominator = len(denominator_data)
#             if denominator == 0:
#                 denominator = len(batch_data)
            
#             # FIXED: Numerator: count ONLY rfppwcgrade == 1 with barcode not null
#             # DO NOT check overallufmgrade here
#             numerator_data = denominator_data[
#                 (denominator_data['rfppwcgrade'].isin([1])) &
#                 denominator_data['barcode'].notnull()
#             ]
#             numerator = len(numerator_data)
#             return yield_percentage(numerator, denominator), numerator, denominator
#         except Exception as e:
#             print(f"[ERROR] Error calculating RFV yield: {e}")
#             return 0.0, 0, len(batch_data) if batch_data is not None else 0

#     def rfh1_yield(batch_data):
#         try:
#             if batch_data is None or batch_data.empty:
#                 return 0.0, 0, 0
            
#             required_cols = ['overallufmgrade', 'barcode', 'rfh1wcgrade']
#             missing_cols = [col for col in required_cols if col not in batch_data.columns]
#             if missing_cols:
#                 print(f"[WARN] Missing columns for RFH1 yield: {missing_cols}")
#                 return 0.0, 0, len(batch_data)
            
#             # Denominator: count records with overallufmgrade 1-5 and barcode not null
#             denominator_data = batch_data[
#                 ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
#                 batch_data['barcode'].notnull()
#             ]
#             denominator = len(denominator_data)
#             if denominator == 0:
#                 denominator = len(batch_data)
            
#             # FIXED: Numerator: count ONLY rfh1wcgrade == 1 with barcode not null
#             # DO NOT check overallufmgrade here
#             numerator_data = denominator_data[
#                 (denominator_data['rfh1wcgrade'].isin([1])) &
#                 denominator_data['barcode'].notnull()
#             ]
#             numerator = len(numerator_data)
#             return yield_percentage(numerator, denominator), numerator, denominator
#         except Exception as e:
#             print(f"[ERROR] Error calculating RFH1 yield: {e}")
#             return 0.0, 0, len(batch_data) if batch_data is not None else 0

#     def rfh2_yield(batch_data):
#         try:
#             if batch_data is None or batch_data.empty:
#                 return 0.0, 0, 0
            
#             required_cols = ['overallufmgrade', 'barcode', 'rfh2wcgrade']
#             missing_cols = [col for col in required_cols if col not in batch_data.columns]
#             if missing_cols:
#                 print(f"[WARN] Missing columns for RFH2 yield: {missing_cols}")
#                 return 0.0, 0, len(batch_data)
            
#             # Denominator: count records with overallufmgrade 1-5 and barcode not null
#             denominator_data = batch_data[
#                 ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
#                 batch_data['barcode'].notnull()
#             ]
#             denominator = len(denominator_data)
#             if denominator == 0:
#                 denominator = len(batch_data)
            
#             # FIXED: Numerator: count ONLY rfh2wcgrade == 1 with barcode not null
#             # DO NOT check overallufmgrade here
#             numerator_data = denominator_data[
#                 (denominator_data['rfh2wcgrade'].isin([1])) &
#                 denominator_data['barcode'].notnull()
#             ]
#             numerator = len(numerator_data)
#             return yield_percentage(numerator, denominator), numerator, denominator
#         except Exception as e:
#             print(f"[ERROR] Error calculating RFH2 yield: {e}")
#             return 0.0, 0, len(batch_data) if batch_data is not None else 0

#     def lfv_yield(batch_data):
#         try:
#             if batch_data is None or batch_data.empty:
#                 return 0.0, 0, 0
            
#             required_cols = ['overallufmgrade', 'barcode', 'lfppwcgrade']
#             missing_cols = [col for col in required_cols if col not in batch_data.columns]
#             if missing_cols:
#                 print(f"[WARN] Missing columns for LFV yield: {missing_cols}")
#                 return 0.0, 0, len(batch_data)
            
#             # Denominator: count records with overallufmgrade 1-5 and barcode not null
#             denominator_data = batch_data[
#                 ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
#                 batch_data['barcode'].notnull()
#             ]
#             denominator = len(denominator_data)
#             if denominator == 0:
#                 denominator = len(batch_data)
            
#             # FIXED: Numerator: count ONLY lfppwcgrade == 1 with barcode not null
#             # DO NOT check overallufmgrade here
#             numerator_data = denominator_data[
#                 (denominator_data['lfppwcgrade'].isin([1])) &
#                 denominator_data['barcode'].notnull()
#             ]
#             numerator = len(numerator_data)
#             return yield_percentage(numerator, denominator), numerator, denominator
#         except Exception as e:
#             print(f"[ERROR] Error calculating LFV yield: {e}")
#             return 0.0, 0, len(batch_data) if batch_data is not None else 0

#     def plysteer_yield(batch_data):
#         try:
#             if batch_data is None or batch_data.empty:
#                 return 0.0, 0, 0
            
#             required_cols = ['overallufmgrade', 'barcode', 'plysgrade']
#             missing_cols = [col for col in required_cols if col not in batch_data.columns]
#             if missing_cols:
#                 print(f"[WARN] Missing columns for Plysteer yield: {missing_cols}")
#                 return 0.0, 0, len(batch_data)
            
#             # Denominator: count records with overallufmgrade 1-5 and barcode not null
#             denominator_data = batch_data[
#                 ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
#                 batch_data['barcode'].notnull()
#             ]
#             denominator = len(denominator_data)
#             if denominator == 0:
#                 denominator = len(batch_data)
            
#             # FIXED: Numerator: count ONLY plysgrade == 1 with barcode not null
#             # DO NOT check overallufmgrade here
#             numerator_data = denominator_data[
#                 (denominator_data['plysgrade'].isin([1])) &
#                 denominator_data['barcode'].notnull()
#             ]
#             numerator = len(numerator_data)
#             return yield_percentage(numerator, denominator), numerator, denominator
#         except Exception as e:
#             print(f"[ERROR] Error calculating Plysteer yield: {e}")
#             return 0.0, 0, len(batch_data) if batch_data is not None else 0

#     # ── Fetch batch data from MongoDB ──
#     def fetch_batch_data_by_machine(sku, machine, start_time, end_time):
#         """Fetch batch data for a specific SKU and machine within time range"""
#         try:
#             client = MongoClient(MONGO_URI)
#             db = client["TUB_CIMCORP_VISUAL_merged_cleaned_data_alerts_data"]
            
#             collection = db["data_merged_scrap_alerts_data"]
            
#             query = {
#                 "sku": sku,
#                 "machine": machine,
#                 "barcode_time": {"$gte": start_time, "$lte": end_time}
#             }
            
#             print(f"[DEBUG] Fetching data for SKU: {sku}, Machine: {machine}")
            
#             cursor = collection.find(query)
#             data = list(cursor)
#             print(f"[DEBUG] Found {len(data)} records in data_merged_scrap_alerts_data")
            
#             if data:
#                 df = pd.DataFrame(data)
#                 print(f"[DEBUG] DataFrame columns: {df.columns.tolist()}")
#                 print(f"[DEBUG] DataFrame shape: {df.shape}")
#                 return df
#             else:
#                 collection = db["data_merged_summary_alerts_data"]
#                 cursor = collection.find(query)
#                 data = list(cursor)
#                 print(f"[DEBUG] Found {len(data)} records in data_merged_summary_alerts_data")
#                 if data:
#                     df = pd.DataFrame(data)
#                     print(f"[DEBUG] DataFrame columns: {df.columns.tolist()}")
#                     print(f"[DEBUG] DataFrame shape: {df.shape}")
#                     return df
#                 else:
#                     print(f"[WARN] No batch data found for sku={sku}, machine={machine}")
#                     return None
                    
#         except Exception as e:
#             print(f"[ERROR] Error fetching batch data for machine {machine}: {e}")
#             import traceback
#             traceback.print_exc()
#             return None
#         finally:
#             try:
#                 client.close()
#             except:
#                 pass

#     # ── Function to calculate yields for a specific machine ──
#     def calculate_yields_for_machine(batch_df, total_bc, machine_obj):
#         """Calculate all yields for a specific machine's batch data"""
#         # Initialize with default values
#         wh_percent, wh_num, wh_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         uni_percent, uni_num, uni_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         vis_percent, vis_num, vis_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         rfv_percent, rfv_num, rfv_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         rfh1_percent, rfh1_num, rfh1_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         rfh2_percent, rfh2_num, rfh2_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         lfv_percent, lfv_num, lfv_den = 0.0, 0, total_bc if total_bc > 0 else 1
#         plysteer_percent, plysteer_num, plysteer_den = 0.0, 0, total_bc if total_bc > 0 else 1
        
#         # If batch data is available, use it to calculate yields per machine
#         if batch_df is not None and not batch_df.empty:
#             try:
#                 # Calculate each yield using the machine-specific data
#                 wh_percent, wh_num, wh_den = overall_warehouse_yield(batch_df)
#                 uni_percent, uni_num, uni_den = overall_uniformity_yield(batch_df)
#                 vis_percent, vis_num, vis_den = overall_visual_yield(batch_df)
#                 rfv_percent, rfv_num, rfv_den = rfv_yield(batch_df)
#                 rfh1_percent, rfh1_num, rfh1_den = rfh1_yield(batch_df)
#                 rfh2_percent, rfh2_num, rfh2_den = rfh2_yield(batch_df)
#                 lfv_percent, lfv_num, lfv_den = lfv_yield(batch_df)
#                 plysteer_percent, plysteer_num, plysteer_den = plysteer_yield(batch_df)
                
#                 print(f"[DEBUG] Calculated yields for machine from batch data:")
#                 print(f"  WH: {wh_percent}% ({wh_num}/{wh_den})")
#                 print(f"  UNI: {uni_percent}% ({uni_num}/{uni_den})")
#                 print(f"  VIS: {vis_percent}% ({vis_num}/{vis_den})")
#                 print(f"  RFH1: {rfh1_percent}% ({rfh1_num}/{rfh1_den})")
                
#             except Exception as e:
#                 print(f"[ERROR] Error calculating yields from batch data: {e}")
#                 import traceback
#                 traceback.print_exc()
#         else:
#             # If no batch data, try to get yields from the machine object's parameter_alerts
#             print(f"[DEBUG] No batch data available, trying to get yields from parameter_alerts")
            
#             # Try to get yields from parameter_alerts
#             for param in machine_obj.get('parameter_alerts', []):
#                 param_name = param.get('parameter')
#                 current_val = param.get('current_value')
                
#                 if param_name == 'overall_warehouse_yield':
#                     wh_percent = float(current_val) if current_val else 0.0
#                     wh_num = int((wh_percent / 100) * total_bc) if total_bc > 0 else 0
#                     wh_den = total_bc if total_bc > 0 else 1
#                 elif param_name == 'overall_uniformity_yield':
#                     uni_percent = float(current_val) if current_val else 0.0
#                     uni_num = int((uni_percent / 100) * total_bc) if total_bc > 0 else 0
#                     uni_den = total_bc if total_bc > 0 else 1
#                 elif param_name == 'overall_visual_yield':
#                     vis_percent = float(current_val) if current_val else 0.0
#                     vis_num = int((vis_percent / 100) * total_bc) if total_bc > 0 else 0
#                     vis_den = total_bc if total_bc > 0 else 1
#                 elif param_name == 'rfv_yield':
#                     rfv_percent = float(current_val) if current_val else 0.0
#                     rfv_num = int((rfv_percent / 100) * total_bc) if total_bc > 0 else 0
#                     rfv_den = total_bc if total_bc > 0 else 1
#                 elif param_name == 'rfh1_yield':
#                     rfh1_percent = float(current_val) if current_val else 0.0
#                     rfh1_num = int((rfh1_percent / 100) * total_bc) if total_bc > 0 else 0
#                     rfh1_den = total_bc if total_bc > 0 else 1
#                 elif param_name == 'rfh2_yield':
#                     rfh2_percent = float(current_val) if current_val else 0.0
#                     rfh2_num = int((rfh2_percent / 100) * total_bc) if total_bc > 0 else 0
#                     rfh2_den = total_bc if total_bc > 0 else 1
#                 elif param_name == 'lfv_yield':
#                     lfv_percent = float(current_val) if current_val else 0.0
#                     lfv_num = int((lfv_percent / 100) * total_bc) if total_bc > 0 else 0
#                     lfv_den = total_bc if total_bc > 0 else 1
#                 elif param_name == 'plysteer_yield':
#                     plysteer_percent = float(current_val) if current_val else 0.0
#                     plysteer_num = int((plysteer_percent / 100) * total_bc) if total_bc > 0 else 0
#                     plysteer_den = total_bc if total_bc > 0 else 1
        
#         # CRITICAL FIX: Recalculate yield percentage using the machine's specific numerator and denominator
#         # This ensures the yield % is calculated from the machine's own num/den, not from aggregate data
#         if wh_den > 0:
#             wh_percent = round((wh_num / wh_den) * 100, 2)
#         if uni_den > 0:
#             uni_percent = round((uni_num / uni_den) * 100, 2)
#         if vis_den > 0:
#             vis_percent = round((vis_num / vis_den) * 100, 2)
#         if rfv_den > 0:
#             rfv_percent = round((rfv_num / rfv_den) * 100, 2)
#         if rfh1_den > 0:
#             rfh1_percent = round((rfh1_num / rfh1_den) * 100, 2)
#         if rfh2_den > 0:
#             rfh2_percent = round((rfh2_num / rfh2_den) * 100, 2)
#         if lfv_den > 0:
#             lfv_percent = round((lfv_num / lfv_den) * 100, 2)
#         if plysteer_den > 0:
#             plysteer_percent = round((plysteer_num / plysteer_den) * 100, 2)
        
#         print(f"[DEBUG] Final yields after recalculation:")
#         print(f"  WH: {wh_percent}% ({wh_num}/{wh_den})")
#         print(f"  UNI: {uni_percent}% ({uni_num}/{uni_den})")
#         print(f"  VIS: {vis_percent}% ({vis_num}/{vis_den})")
#         print(f"  RFH1: {rfh1_percent}% ({rfh1_num}/{rfh1_den})")

#         return {
#             'wh': (wh_percent, wh_num, wh_den),
#             'uni': (uni_percent, uni_num, uni_den),
#             'vis': (vis_percent, vis_num, vis_den),
#             'rfv': (rfv_percent, rfv_num, rfv_den),
#             'rfh1': (rfh1_percent, rfh1_num, rfh1_den),
#             'rfh2': (rfh2_percent, rfh2_num, rfh2_den),
#             'lfv': (lfv_percent, lfv_num, lfv_den),
#             'plysteer': (plysteer_percent, plysteer_num, plysteer_den)
#         }

#     # Initialize list to collect one entry per (alert_doc, machine) pair
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

#     # Create payload title
#     if alert_type.upper() == "TBM":
#         title_str = "Alert: TBM Alert Summary"
#     elif alert_type.upper() == "CURING":
#         title_str = "Alert: CURING Alert Summary"
#     elif alert_type.upper() == "UFM":
#         title_str = "Alert: UFM Alert Summary"
#     else:
#         title_str = "Alert Summary"

#     # Convert summary HTML to markdown
#     markdown_table = html_table_to_markdown(summary_html)

#     # ── Machine Breakdown ──
#     breakdown_blocks = []

#     for entry in alert_machine_entries:
#         sku = entry['sku']
#         machine_obj = entry['machine_obj']
#         alert_obj = entry['alert_obj']
#         _at = alert_type.upper()

#         def _color(actual, spec):
#             try:
#                 if actual is None or spec is None:
#                     return "none"
#                 return "red" if float(actual) < float(spec) else "green"
#             except (TypeError, ValueError):
#                 return "none"

#         # ── Get the start and end times for this alert ──
#         start_time = alert_obj.get('machine_lot_start_build_time' if _at == "TBM" else 
#                                    'machine_lot_start_curing_time' if _at == "CURING" else 
#                                    'machine_lot_start_ufm_time')
#         end_time = alert_obj.get('machine_lot_end_build_time' if _at == "TBM" else 
#                                  'machine_lot_end_curing_time' if _at == "CURING" else 
#                                  'machine_lot_end_ufm_time')
        
#         # ── Get spec values ──
#         sku_specs = yield_spec_lookup.get(str(sku).strip(), {})
#         spec_wh = sku_specs.get('spec_overall_warehouse_yield')
#         spec_uni = sku_specs.get('spec_overall_uniformity_yield')
#         spec_vis = sku_specs.get('spec_overall_visual_yield')
#         spec_rfv = sku_specs.get('spec_rfv_yield')
#         spec_rfh1 = sku_specs.get('spec_rfh1_yield')
#         spec_rfh2 = sku_specs.get('spec_rfh2_yield')
#         spec_lfv = sku_specs.get('spec_lfv_yield')
#         spec_plysteer = sku_specs.get('spec_plysteer_yield')

#         # ── Rows for this individual breakdown ──
#         entry_rows = []

#         def _add_row_with_yields(mname, mtype, bc, machine_name_for_fetch):
#             """Add a row with yields calculated specifically for this machine"""
#             # Fetch batch data for this specific machine
#             batch_df = fetch_batch_data_by_machine(sku, machine_name_for_fetch, start_time, end_time)
            
#             # Calculate yields for this machine
#             yields = calculate_yields_for_machine(batch_df, bc, machine_obj)
            
#             entry_rows.append({
#                 'mname': mname,
#                 'mtype': mtype,
#                 'bc': bc,
#                 'yields': yields
#             })

#         # Build rows based on alert type
#         if _at == "UFM":
#             main_machine = machine_obj.get('machine', 'N/A')
#             total_bc = machine_obj.get('processed_machine_barcodes', 0)
#             _add_row_with_yields(
#                 sanitize_text(main_machine), 
#                 "UFM", 
#                 int(total_bc) if total_bc else 0,
#                 main_machine
#             )
            
#             for c in machine_obj.get('cur_ids', []):
#                 bc = c.get('barcode', 0)
#                 cur_id = c.get('cur_id', 'N/A')
#                 _add_row_with_yields(
#                     sanitize_text(str(cur_id)), 
#                     "Curing", 
#                     int(bc),
#                     cur_id
#                 )
            
#             tbm_ids_raw = machine_obj.get('tbm_ids', [])
#             tbm_machine = machine_obj.get('tbm_machine', 'N/A')
#             if tbm_ids_raw:
#                 for t in tbm_ids_raw:
#                     bc = t.get('barcode', 0)
#                     tbm_id = t.get('tbm_id', 'N/A')
#                     _add_row_with_yields(
#                         sanitize_text(str(tbm_id)), 
#                         "TBM", 
#                         int(bc),
#                         tbm_id
#                     )
#             elif tbm_machine and tbm_machine != 'N/A':
#                 _add_row_with_yields(
#                     tbm_machine, 
#                     "TBM", 
#                     int(total_bc) if total_bc else 0,
#                     tbm_machine
#                 )

#         elif _at == "TBM":
#             main_machine = machine_obj.get('machine', 'N/A')
#             total_bc = machine_obj.get('processed_machine_barcodes', 0)
#             _add_row_with_yields(
#                 sanitize_text(main_machine), 
#                 "TBM", 
#                 int(total_bc) if total_bc else 0,
#                 main_machine
#             )
            
#             seen_ubno = set()
#             for ma in alert_obj.get("machine_alerts", []):
#                 for rec in ma.get("grouped_averages", {}).get("uf_id", []):
#                     uf = rec.get('uf_id', '')
#                     bc = rec.get('barcode_count', 0)
#                     if uf and uf not in seen_ubno:
#                         seen_ubno.add(uf)
#                         _add_row_with_yields(
#                             sanitize_text(uf), 
#                             "UFM", 
#                             int(bc),
#                             uf
#                         )
            
#             for c in machine_obj.get('cur_ids', []):
#                 bc = c.get('barcode', 0)
#                 cur_id = c.get('cur_id', 'N/A')
#                 _add_row_with_yields(
#                     sanitize_text(str(cur_id)), 
#                     "Curing", 
#                     int(bc),
#                     cur_id
#                 )

#         elif _at == "CURING":
#             main_machine = machine_obj.get('machine', 'N/A')
#             total_bc = machine_obj.get('processed_machine_barcodes', 0)
#             _add_row_with_yields(
#                 sanitize_text(main_machine), 
#                 "Curing", 
#                 int(total_bc) if total_bc else 0,
#                 main_machine
#             )
            
#             seen_ubno = set()
#             for ma in alert_obj.get("machine_alerts", []):
#                 for rec in ma.get("grouped_averages", {}).get("uf_id", []):
#                     uf = rec.get('uf_id', '')
#                     bc = rec.get('barcode_count', 0)
#                     if uf and uf not in seen_ubno:
#                         seen_ubno.add(uf)
#                         _add_row_with_yields(
#                             sanitize_text(uf), 
#                             "UFM", 
#                             int(bc),
#                             uf
#                         )
            
#             for p in machine_obj.get('ptb_ids', []):
#                 bc = p.get('barcode', 0)
#                 ptb_id = p.get('ptb_id', 'N/A')
#                 _add_row_with_yields(
#                     sanitize_text(str(ptb_id)), 
#                     "TBM", 
#                     int(bc),
#                     ptb_id
#                 )

#         if not entry_rows:
#             continue

#         # Build HTML table for this entry
#         primary_machine = sanitize_text(machine_obj.get('machine', 'N/A'))

#         html_table = f"<p><strong>SKU: {sku} | Machine: {primary_machine}</strong></p>"
#         html_table += "<table border='1' cellpadding='5' cellspacing='0' style='border-collapse: collapse; width: 100%;'>"
#         html_table += "<thead><tr style='background-color: #4A90E2; color: white;'>"
        
#         html_table += (
#             "<th>Machine</th><th>Machine Type</th>"
#             "<th>Total Tyres</th>"
#             "<th>WH Num</th><th>WH Den</th><th>WAREHOUSE Yield %</th>"
#             "<th>UNI Num</th><th>UNI Den</th><th>UNIFORMITY Yield %</th>"
#             "<th>VIS Num</th><th>VIS Den</th><th>VISUAL Yield %</th>"
#             "<th>RFV Num</th><th>RFV Den</th><th>RFV Yield %</th>"
#             "<th>RFH1 Num</th><th>RFH1 Den</th><th>RFH1 Yield %</th>"
#             "<th>RFH2 Num</th><th>RFH2 Den</th><th>RFH2 Yield %</th>"
#             "<th>LFV Num</th><th>LFV Den</th><th>LFV Yield %</th>"
#             "<th>PLYSTEER Num</th><th>PLYSTEER Den</th><th>PLYSTEER Yield %</th>"
#         )
        
#         html_table += "</tr></thead><tbody>"

#         def _fmt_yield(yield_percent):
#             if yield_percent is None:
#                 return "N/A"
#             try:
#                 return f"{float(yield_percent):.2f}%"
#             except (TypeError, ValueError):
#                 return "N/A"

#         def _fmt_count(count):
#             if count is None:
#                 return "N/A"
#             try:
#                 return str(int(count))
#             except (TypeError, ValueError):
#                 return "N/A"

#         def _get_color_indicator(yield_percent, spec_val):
#             if yield_percent is None or spec_val is None:
#                 return ""
#             try:
#                 if float(yield_percent) < float(spec_val):
#                     return "🔴"
#                 else:
#                     return "🟢"
#             except (TypeError, ValueError):
#                 return ""

#         for row in entry_rows:
#             mname = row['mname']
#             mtype = row['mtype']
#             bc = row['bc']
#             yields = row['yields']
            
#             wh_percent, wh_num, wh_den = yields['wh']
#             uni_percent, uni_num, uni_den = yields['uni']
#             vis_percent, vis_num, vis_den = yields['vis']
#             rfv_percent, rfv_num, rfv_den = yields['rfv']
#             rfh1_percent, rfh1_num, rfh1_den = yields['rfh1']
#             rfh2_percent, rfh2_num, rfh2_den = yields['rfh2']
#             lfv_percent, lfv_num, lfv_den = yields['lfv']
#             plysteer_percent, plysteer_num, plysteer_den = yields['plysteer']
            
#             wh_color = _get_color_indicator(wh_percent, spec_wh)
#             uni_color = _get_color_indicator(uni_percent, spec_uni)
#             vis_color = _get_color_indicator(vis_percent, spec_vis)
#             rfv_color = _get_color_indicator(rfv_percent, spec_rfv)
#             rfh1_color = _get_color_indicator(rfh1_percent, spec_rfh1)
#             rfh2_color = _get_color_indicator(rfh2_percent, spec_rfh2)
#             lfv_color = _get_color_indicator(lfv_percent, spec_lfv)
#             plysteer_color = _get_color_indicator(plysteer_percent, spec_plysteer)

#             html_table += "<tr>"
#             html_table += f"<td>{mname}</td><td>{mtype}</td>"
#             html_table += f"<td>{bc}</td>"
            
#             html_table += f"<td>{_fmt_count(wh_num)}</td>"
#             html_table += f"<td>{_fmt_count(wh_den)}</td>"
#             html_table += f"<td>{_fmt_yield(wh_percent)} {wh_color}</td>"
            
#             html_table += f"<td>{_fmt_count(uni_num)}</td>"
#             html_table += f"<td>{_fmt_count(uni_den)}</td>"
#             html_table += f"<td>{_fmt_yield(uni_percent)} {uni_color}</td>"
            
#             html_table += f"<td>{_fmt_count(vis_num)}</td>"
#             html_table += f"<td>{_fmt_count(vis_den)}</td>"
#             html_table += f"<td>{_fmt_yield(vis_percent)} {vis_color}</td>"
            
#             html_table += f"<td>{_fmt_count(rfv_num)}</td>"
#             html_table += f"<td>{_fmt_count(rfv_den)}</td>"
#             html_table += f"<td>{_fmt_yield(rfv_percent)} {rfv_color}</td>"
            
#             html_table += f"<td>{_fmt_count(rfh1_num)}</td>"
#             html_table += f"<td>{_fmt_count(rfh1_den)}</td>"
#             html_table += f"<td>{_fmt_yield(rfh1_percent)} {rfh1_color}</td>"
            
#             html_table += f"<td>{_fmt_count(rfh2_num)}</td>"
#             html_table += f"<td>{_fmt_count(rfh2_den)}</td>"
#             html_table += f"<td>{_fmt_yield(rfh2_percent)} {rfh2_color}</td>"
            
#             html_table += f"<td>{_fmt_count(lfv_num)}</td>"
#             html_table += f"<td>{_fmt_count(lfv_den)}</td>"
#             html_table += f"<td>{_fmt_yield(lfv_percent)} {lfv_color}</td>"
            
#             html_table += f"<td>{_fmt_count(plysteer_num)}</td>"
#             html_table += f"<td>{_fmt_count(plysteer_den)}</td>"
#             html_table += f"<td>{_fmt_yield(plysteer_percent)} {plysteer_color}</td>"
            
#             html_table += "</tr>"

#         html_table += "</tbody></table>"
#         breakdown_blocks.append(html_table)

#     machine_breakdown_html = "<br><br>".join(breakdown_blocks) if breakdown_blocks else "<p>No data available.</p>"

#     # ── Send combined payload ──
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
#         'sku': doc['sku'],
#         'target_machine_lot_size': doc.get('target_machine_lot_size'),
#         atype['alert_time_key']: doc.get(atype['alert_time_key'])
#     }

#     raw  = alert['parameter_alerts']
#     data = {p['parameter']: p for p in raw}
#     for p in data.values():
#         c, s = p.get('current_value'), p.get('spec_value')
#         p['spec_diff'] = float(c)-float(s) if c is not None and s is not None else None

#     # filter keys present
#     all_ufm = PARAM_KEYS[:10]
#     all_geometery = PARAM_KEYS[10:20]
#     all_bal = PARAM_KEYS[20:23]
#     all_load = PARAM_KEYS[23:27]
#     all_yields = PARAM_KEYS[27:]
#     ufm_keys = [k for k in all_ufm if k in data]
#     geometry_keys = [k for k in all_geometery if k in data]
#     bal_keys = [k for k in all_bal if k in data]
#     load_keys = [k for k in all_load if k in data]
#     yields_keys = [k for k in all_yields if k in data]

#     pdf = PDF()
#     pdf.add_page()

#      # 1) Compute our twoâ€column split
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
#     modelno = sanitize_text(str(alert.get('sku', 'UNKNOWN')))

#     # Build identifier
#     identifier = f":{atype_name}_{machine}_{modelno}_{ts_str if ts_str else 'None'}"

										
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

																					   
#     ufm_keys = [k for k in PARAM_KEYS[:10] if k in data]
#     geometry_keys = [k for k in PARAM_KEYS[10:20] if k in data]
#     bal_keys = [k for k in PARAM_KEYS[20:23] if k in data]
#     load_keys = [k for k in PARAM_KEYS[23:27] if k in data]
#     yields_keys = [k for k in PARAM_KEYS[27:] if k in data]
#     group_defs = [("UFM", ufm_keys), ("GEOMETRY", geometry_keys), ("BALANCING", bal_keys), ("LOAD", load_keys), ("YIELDS", yields_keys)]
                  
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
#         group_defs=[("UFM", ufm_keys), ("GEOMETRY", geometry_keys), ("BALANCING", bal_keys), ("LOAD", load_keys), ("YIELDS", yields_keys)],
                                                                                   
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
#     # return pdf.output(dest='S').encode('latin-1')

# def get_admin_emails(client):
#     cols = client[alert_mail_db][alert_users]
#     users = cols.find(
#         {"is_alert_user": True},
#         {"_id": 0, "email": 1}
#     )
#     emails = [u["email"] for u in users if u.get("email")]
#     #return [r['email'] for r in cols.find({}, {'_id':0,'email':1})]
#     return emails

# def send_alerts():
#     """
#     Sends alerts by grouping PDFs by modelno across all work centers (TBM, CURING, UFM).
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
#         docs = list(coll.find({'alert_flag': False}))
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
#                                     {'$set': {'alert_flag': True, doc['alert_type']['alert_time_key']: datetime.now()}}
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
#                             {'$set': {'alert_flag': True, doc['alert_type']['alert_time_key']: datetime.now()}}
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
from config_hungary_uat import (
    alert_triggering_ubno_collect_name,
    alert_triggering_collect_name,
    alert_triggering_curing_collect_name,
    # alert_db,
    alert_db_virgin_yield,
    alert_mail_db,
    alert_users,
    config_databse,
    channel_no_collect,                  
    MONGO_URI,
    SENDER_EMAIL,
    SENDER_PASSWORD,
    TEAMS_WEBHOOK_URL

)

# --- Configuration ---
MONGO_URI       =  MONGO_URI
SENDER_EMAIL    = SENDER_EMAIL
SENDER_PASSWORD = SENDER_PASSWORD
ALERT_DB_NAME   = alert_db_virgin_yield
CONFIG_DB_NAME = config_databse  # NEW: Database for OE Replacement lookup
CHANNEL_NO_COLLECT = channel_no_collect  # NEW: Collection for OE Replacement                                                                            
BASE_PDF_DIR    = "generated_pdfs"
																			  
MAX_EMAIL_SIZE = 20 * 1024 * 1024  # 20 MB																			   

# Full list of possible keys
PARAM_KEYS = [
    "rfppcc","rfppcw","rfh1cc","rfh1cw","rfh2cc","rfh2cw","lfppcc","lfppcw","cony","plys","crro","trro","brro",
    "lrot","lrob","lroh1grade","tbul","bbul","tdep","bdep","upper","lower","sbal","aircc","aircw","loadcc","loadcw",
    "rfv_yield", "rfh1_yield", "rfh2_yield", "lfv_yield", "conicity_yield", "plysteer_yield","center_radial_run_out_yield", 
    "top_radial_run_out_yield", "bottom_radial_run_out_yield", "lateral_run_out_yield", "lateral_run_out_harmonic_first_yield",
    "center_radial_run_out_harmonic_first_yield", "top_radial_run_out_harmonic_first_yield", "bottom_radial_run_out_harmonic_first_yield",
    "lateral_radial_run_out_yield", "static_yield", "couple_yield","upper_yield", "lower_yield", "overall_uniformity_yield", 
    "overall_visual_yield", "overall_warehouse_yield","overall_scrap_yield", "visual_loss_yield", "uf_loss_yield", "oe_ok_yield"
]

ALERT_TYPES = [
    {
        "name": "UFM",
        "coll": alert_triggering_ubno_collect_name,
        "banner": "UFM ALERT",
        "filename_prefix": "UFM",
        "start_key": "machine_lot_start_ufm_time",
        "end_key":   "machine_lot_end_ufm_time",
        "alert_time_key": "timestamp_alert_generated",
        "use_grouped": False,
        "shift_data_key": "shifts_data_tbm",
        "shift_start_time_key": "machine_shift_tbm_start_time"
    },
    {
        "name": "TBM",
        "coll":  alert_triggering_collect_name,
        "banner": "TBM ALERT",
        "filename_prefix": "TBM",
        "start_key": "machine_lot_start_build_time",
        "end_key":   "machine_lot_end_build_time",
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
        "end_key":   "machine_lot_end_curing_time",
        "alert_time_key": "timestamp_alert_generated",
        "use_grouped": True,
        "shift_data_key": "shifts_data_tbm",
        "shift_start_time_key": "machine_shift_tbm_start_time"
    }
]

# -------------------------------------------------
# NEW: Get OE Replacement from config database
# -------------------------------------------------
def get_oe_replacement(client, gt_code):
    """
    Looks up the OE replacement for a given gt_code in
    config_databse.channel_no_collect.
    Returns the value of field `oe_replacement` or "N/A".
    """
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
    """
    Sanitizes text to ensure compatibility with PDF encoding by replacing problematic characters.
    """
    if text is None:
        return "N/A"
    # Convert to string and replace problematic Unicode characters
    text = str(text)
    # Replace \ufffd and other problematic characters with a safe alternative
    return text.encode('ascii', 'replace').decode('ascii').replace('\ufffd', '?')			

def html_table_to_markdown(html_table):
    """
    Converts HTML table to Markdown format for better Teams compatibility
    """
    # Parse the HTML
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html_table, 'html.parser')
    
    # Initialize markdown table
    markdown_table = ""
    
    # Process headers
    headers = [sanitize_text(th.get_text()) for th in soup.find_all('th')]
    markdown_table += "| " + " | ".join(headers) + " |\n"
    markdown_table += "|" + "|".join(["---"] * len(headers)) + "|\n"
    
    # Process rows
    for row in soup.find_all('tr')[1:]:  # Skip header row
        cells = [sanitize_text(td.get_text()) for td in row.find_all('td')]
        markdown_table += "| " + " | ".join(cells) + " |\n"
    
    return markdown_table

def generate_combined_summary_table(alerts, alert_type):
    """
    Changed 14-04
    Generates properly formatted HTML table for all alerts.
    
    Args:
        alerts (list): List of alert dictionaries containing relevant data.
        alert_type (str): Type of alert ("TBM", "CURING", or "UBNO").
    
    Returns:
        str: HTML table as a string.
    """
    # Check if alerts list is empty
    if not alerts:
        return "<table><tr><td>No alert data available</td></tr></table>"
    
    # Start HTML table
    html = "<table>"
    
    # Define headers based on alert_type
    if alert_type.upper() == "TBM":
        html += """
        <tr>
            <th>S.No</th>
            <th>SKU</th>
            <th>Size Code</th>
            <th>Gt Code</th>
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
            <th>Size Code</th>
            <th>Gt Code</th>
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
            <th>Gt Code</th>
            <th>UFM</th>
            <th>Total Tyre Produced</th>
            <th>Overall WAREHOUSE Yield (%)</th>
            <th>Overall UNIFORMITY Yield (%)</th>
            <th>Overall VISUAL Yield (%)</th>
            <th>Overall SCRAP Yield (%)</th>
        </tr>
        """

    # Handle different alert types
    if alert_type.upper() == "TBM":
        # For TBM alerts
        # for alert in alerts:
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
                    <td>{sanitize_text(machine.get('sizecode', 'N/A'))}</td>
                    <td>{sanitize_text(machine.get('gt_code', 'N/A'))}</td>
                    <td>{sanitize_text(machine.get('machine', 'N/A'))}</td>
                    <td>{sanitize_text(machine.get('processed_machine_barcodes', 'N/A'))}</td>
                    <td>{warehouse}</td>
                    <td>{uniformity}</td>
                    <td>{visual}</td>
                    <td>{scrap}</td>
                </tr>
                """
    
    elif alert_type.upper() == "CURING":
        # For CURING alerts
        # for alert in alerts:
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
                    <td>{sanitize_text(machine.get('sizecode', 'N/A'))}</td>
                    <td>{sanitize_text(machine.get('gt_code', 'N/A'))}</td>
                    <td>{sanitize_text(machine.get('machine', 'N/A'))}</td>
                    <td>{sanitize_text(machine.get('processed_machine_barcodes', 'N/A'))}</td>
                    <td>{warehouse}</td>
                    <td>{uniformity}</td>
                    <td>{visual}</td>
                    <td>{scrap}</td>
                </tr>
                """
    
    elif alert_type.upper() == "UFM":
    # For CURING alerts
        # for alert in alerts:
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
                    <td>{sanitize_text(machine.get('sizecode', 'N/A'))}</td>
                    <td>{sanitize_text(machine.get('gt_code', 'N/A'))}</td>
                    <td>{sanitize_text(machine.get('machine', 'N/A'))}</td>
                    <td>{sanitize_text(machine.get('processed_machine_barcodes', 'N/A'))}</td>
                    <td>{warehouse}</td>
                    <td>{uniformity}</td>
                    <td>{visual}</td>
                    <td>{scrap}</td>
                </tr>
                """
    
    # Close the table
    html += "</table>"
    return html

def send_teams_alert(summary_html, alert_type, docs):
    """
    Sends a Microsoft Teams alert message with properly formatted table.
    Args:
        summary_html (str): HTML string of the summary table.
        alert_type (str): Type of alert (UFM, TBM, CURING).
        docs (list): List of alert documents from MongoDB.
    """   
    teams_webhook_url = TEAMS_WEBHOOK_URL
    if not teams_webhook_url:
        print("[ERROR] TEAMS_WEBHOOK_URL not configured.")
        return False

    # ── Only alert on documents with alert_flag == False ──
    # These come from alert_db_virgin_yield; a Teams alert must be sent only for
    # not-yet-alerted lots. Filter defensively here so this holds even if the
    # caller passes a broader set of docs.
    docs = [d for d in (docs or []) if d.get("alert_flag", False) is False]
    if not docs:
        print(f"[INFO] No alert_flag=False documents to send for {alert_type}.")
        return True

    # ── Fetch yield spec thresholds from channel_no_collect ──
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
             "spec_rfh2wc_yield": 1,
             "spec_plys_yield": 1}
        ):
            sku_key = str(doc.get("sku", "")).strip()
            if sku_key:
                yield_spec_lookup[sku_key] = {
                    "spec_overall_warehouse_yield": doc.get("spec_overall_warehouse_yield"),
                    "spec_overall_uniformity_yield": doc.get("spec_overall_uniformity_yield"),
                    "spec_overall_visual_yield": doc.get("spec_overall_visual_yield"),
                    "spec_rfv_yield":      doc.get("spec_rfppwc_yield"),
                    "spec_rfh1_yield":     doc.get("spec_rfh1wc_yield"),
                    "spec_rfh2_yield":     doc.get("spec_rfh2wc_yield"),
                    "spec_lfv_yield":      doc.get("spec_rfppwc_yield"),
                    "spec_plysteer_yield": doc.get("spec_plys_yield"),
                }
        _mc.close()
    except Exception as _e:
        print(f"[WARN] Could not fetch yield specs: {_e}")

    # ── Functions to calculate yields from batch data ──
    def yield_percentage(numerator, denominator):
        try:
            if denominator is None or denominator == 0:
                return 0.0
            return round((numerator / denominator) * 100, 2)
        except (TypeError, ValueError, ZeroDivisionError):
            return 0.0

    def overall_warehouse_yield(batch_data):
        try:
            if batch_data is None or batch_data.empty:
                return 0.0, 0, 0
            
            required_cols = ['overallufmgrade', 'barcode', 'scrap_category_tub', 'last']
            missing_cols = [col for col in required_cols if col not in batch_data.columns]
            if missing_cols:
                print(f"[WARN] Missing columns for warehouse yield: {missing_cols}")
                return 0.0, 0, len(batch_data)
            
            measured_data = batch_data[
                (batch_data['overallufmgrade'].isin([1, 2, 3, 4, 5])) &
                (batch_data['barcode'].notnull())
            ]
            measured = len(measured_data)

            vi_scrap_data = batch_data[
                (batch_data['scrap_category_tub'] == "VISUAL") & 
                (batch_data["scrap_category_tub"].notna())
            ]
            vi_scrap = len(vi_scrap_data)

            denominator = measured + vi_scrap
            if denominator == 0:
                denominator = len(batch_data)

            numerator_data = batch_data[
                (batch_data['overallufmgrade'] == 1) &
                (batch_data['last'] == 1)
            ]
            numerator = len(numerator_data)

            return yield_percentage(numerator, denominator), numerator, denominator
        except Exception as e:
            print(f"[ERROR] Error calculating warehouse yield: {e}")
            return 0.0, 0, len(batch_data) if batch_data is not None else 0

    def overall_uniformity_yield(batch_data):
        try:
            if batch_data is None or batch_data.empty:
                return 0.0, 0, 0
            
            required_cols = ['overallufmgrade', 'barcode', 'vi_yield', 'last']
            missing_cols = [col for col in required_cols if col not in batch_data.columns]
            if missing_cols:
                print(f"[WARN] Missing columns for uniformity yield: {missing_cols}")
                return 0.0, 0, len(batch_data)
            
            measured_data = batch_data[
                (batch_data['overallufmgrade'].isin([1, 2, 3, 4, 5])) &
                (batch_data['barcode'].notnull())
            ]
            measured = len(measured_data)

            vi_downgrade_data = batch_data[
                (batch_data['vi_yield'] == 0) & 
                (batch_data['last'] == 1) & 
                (batch_data["vi_yield"].notna())
            ]
            vi_downgrade = len(vi_downgrade_data)

            denominator = measured - vi_downgrade
            if denominator <= 0:
                denominator = measured if measured > 0 else len(batch_data)

            numerator_data = batch_data[
                (batch_data['overallufmgrade'] == 1) &
                (batch_data['last'] == 1)
            ]
            numerator = len(numerator_data)

            return yield_percentage(numerator, denominator), numerator, denominator
        except Exception as e:
            print(f"[ERROR] Error calculating uniformity yield: {e}")
            return 0.0, 0, len(batch_data) if batch_data is not None else 0

    def overall_visual_yield(batch_data):
        try:
            if batch_data is None or batch_data.empty:
                return 0.0, 0, 0
            
            required_cols = ['vi_yield', 'last', 'scrap_category_tub', 'overallufmgrade']
            missing_cols = [col for col in required_cols if col not in batch_data.columns]
            if missing_cols:
                print(f"[WARN] Missing columns for visual yield: {missing_cols}")
                return 0.0, 0, len(batch_data)
            
            vi_downgrade_data = batch_data[
                (batch_data['vi_yield'] == 0) &
                (batch_data['last'] == 1)
            ]
            vi_downgrade = len(vi_downgrade_data)

            vi_scrap_data = batch_data[
                batch_data['scrap_category_tub'] == "VISUAL"
            ]
            vi_scrap = len(vi_scrap_data)

            ufm_data = batch_data[
                batch_data['overallufmgrade'].isin([1, 2, 3, 4, 5])
            ]
            ufm_count = len(ufm_data)

            possible_oe = (ufm_count + vi_scrap) - vi_downgrade
            vi_pass = possible_oe if possible_oe > 0 else 0
            denominator = vi_pass + vi_scrap
            if denominator <= 0:
                denominator = len(batch_data)
            numerator = vi_pass

            return yield_percentage(numerator, denominator), numerator, denominator
        except Exception as e:
            print(f"[ERROR] Error calculating visual yield: {e}")
            return 0.0, 0, len(batch_data) if batch_data is not None else 0

    def rfv_yield(batch_data):
        try:
            if batch_data is None or batch_data.empty:
                return 0.0, 0, 0
            
            required_cols = ['overallufmgrade', 'barcode', 'rfppwcgrade']
            missing_cols = [col for col in required_cols if col not in batch_data.columns]
            if missing_cols:
                print(f"[WARN] Missing columns for RFV yield: {missing_cols}")
                return 0.0, 0, len(batch_data)
            
            # Denominator: count records with overallufmgrade 1-5 and barcode not null
            denominator_data = batch_data[
                ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
                batch_data['barcode'].notnull()
            ]
            denominator = len(denominator_data)
            if denominator == 0:
                denominator = len(batch_data)
            
            # FIXED: Numerator: count ONLY rfppwcgrade == 1 with barcode not null
            # DO NOT check overallufmgrade here
            numerator_data = denominator_data[
                (denominator_data['rfppwcgrade'].isin([1])) &
                denominator_data['barcode'].notnull()
            ]
            numerator = len(numerator_data)
            return yield_percentage(numerator, denominator), numerator, denominator
        except Exception as e:
            print(f"[ERROR] Error calculating RFV yield: {e}")
            return 0.0, 0, len(batch_data) if batch_data is not None else 0

    def rfh1_yield(batch_data):
        try:
            if batch_data is None or batch_data.empty:
                return 0.0, 0, 0
            
            required_cols = ['overallufmgrade', 'barcode', 'rfh1wcgrade']
            missing_cols = [col for col in required_cols if col not in batch_data.columns]
            if missing_cols:
                print(f"[WARN] Missing columns for RFH1 yield: {missing_cols}")
                return 0.0, 0, len(batch_data)
            
            # Denominator: count records with overallufmgrade 1-5 and barcode not null
            denominator_data = batch_data[
                ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
                batch_data['barcode'].notnull()
            ]
            denominator = len(denominator_data)
            if denominator == 0:
                denominator = len(batch_data)
            
            # FIXED: Numerator: count ONLY rfh1wcgrade == 1 with barcode not null
            # DO NOT check overallufmgrade here
            numerator_data = denominator_data[
                (denominator_data['rfh1wcgrade'].isin([1])) &
                denominator_data['barcode'].notnull()
            ]
            numerator = len(numerator_data)
            return yield_percentage(numerator, denominator), numerator, denominator
        except Exception as e:
            print(f"[ERROR] Error calculating RFH1 yield: {e}")
            return 0.0, 0, len(batch_data) if batch_data is not None else 0

    def rfh2_yield(batch_data):
        try:
            if batch_data is None or batch_data.empty:
                return 0.0, 0, 0
            
            required_cols = ['overallufmgrade', 'barcode', 'rfh2wcgrade']
            missing_cols = [col for col in required_cols if col not in batch_data.columns]
            if missing_cols:
                print(f"[WARN] Missing columns for RFH2 yield: {missing_cols}")
                return 0.0, 0, len(batch_data)
            
            # Denominator: count records with overallufmgrade 1-5 and barcode not null
            denominator_data = batch_data[
                ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
                batch_data['barcode'].notnull()
            ]
            denominator = len(denominator_data)
            if denominator == 0:
                denominator = len(batch_data)
            
            # FIXED: Numerator: count ONLY rfh2wcgrade == 1 with barcode not null
            # DO NOT check overallufmgrade here
            numerator_data = denominator_data[
                (denominator_data['rfh2wcgrade'].isin([1])) &
                denominator_data['barcode'].notnull()
            ]
            numerator = len(numerator_data)
            return yield_percentage(numerator, denominator), numerator, denominator
        except Exception as e:
            print(f"[ERROR] Error calculating RFH2 yield: {e}")
            return 0.0, 0, len(batch_data) if batch_data is not None else 0

    def lfv_yield(batch_data):
        try:
            if batch_data is None or batch_data.empty:
                return 0.0, 0, 0
            
            required_cols = ['overallufmgrade', 'barcode', 'lfppwcgrade']
            missing_cols = [col for col in required_cols if col not in batch_data.columns]
            if missing_cols:
                print(f"[WARN] Missing columns for LFV yield: {missing_cols}")
                return 0.0, 0, len(batch_data)
            
            # Denominator: count records with overallufmgrade 1-5 and barcode not null
            denominator_data = batch_data[
                ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
                batch_data['barcode'].notnull()
            ]
            denominator = len(denominator_data)
            if denominator == 0:
                denominator = len(batch_data)
            
            # FIXED: Numerator: count ONLY lfppwcgrade == 1 with barcode not null
            # DO NOT check overallufmgrade here
            numerator_data = denominator_data[
                (denominator_data['lfppwcgrade'].isin([1])) &
                denominator_data['barcode'].notnull()
            ]
            numerator = len(numerator_data)
            return yield_percentage(numerator, denominator), numerator, denominator
        except Exception as e:
            print(f"[ERROR] Error calculating LFV yield: {e}")
            return 0.0, 0, len(batch_data) if batch_data is not None else 0

    def plysteer_yield(batch_data):
        try:
            if batch_data is None or batch_data.empty:
                return 0.0, 0, 0
            
            required_cols = ['overallufmgrade', 'barcode', 'plysgrade']
            missing_cols = [col for col in required_cols if col not in batch_data.columns]
            if missing_cols:
                print(f"[WARN] Missing columns for Plysteer yield: {missing_cols}")
                return 0.0, 0, len(batch_data)
            
            # Denominator: count records with overallufmgrade 1-5 and barcode not null
            denominator_data = batch_data[
                ((batch_data['overallufmgrade'] > 0) & (batch_data['overallufmgrade'] <= 5)) &
                batch_data['barcode'].notnull()
            ]
            denominator = len(denominator_data)
            if denominator == 0:
                denominator = len(batch_data)
            
            # FIXED: Numerator: count ONLY plysgrade == 1 with barcode not null
            # DO NOT check overallufmgrade here
            numerator_data = denominator_data[
                (denominator_data['plysgrade'].isin([1])) &
                denominator_data['barcode'].notnull()
            ]
            numerator = len(numerator_data)
            return yield_percentage(numerator, denominator), numerator, denominator
        except Exception as e:
            print(f"[ERROR] Error calculating Plysteer yield: {e}")
            return 0.0, 0, len(batch_data) if batch_data is not None else 0

    # ── Fetch batch data from MongoDB ──
    def fetch_batch_data_by_machine(sku, machine, start_time, end_time):
        """Fetch batch data for a specific SKU and machine within time range"""
        try:
            client = MongoClient(MONGO_URI)
            db = client["TUB_CIMCORP_VISUAL_merged_cleaned_data_alerts_data"]
            
            collection = db["data_merged_scrap_alerts_data"]
            
            query = {
                "sku": sku,
                "machine": machine,
                "barcode_time": {"$gte": start_time, "$lte": end_time}
            }
            
            print(f"[DEBUG] Fetching data for SKU: {sku}, Machine: {machine}")
            
            cursor = collection.find(query)
            data = list(cursor)
            print(f"[DEBUG] Found {len(data)} records in data_merged_scrap_alerts_data")
            
            if data:
                df = pd.DataFrame(data)
                print(f"[DEBUG] DataFrame columns: {df.columns.tolist()}")
                print(f"[DEBUG] DataFrame shape: {df.shape}")
                return df
            else:
                collection = db["data_merged_summary_alerts_data"]
                cursor = collection.find(query)
                data = list(cursor)
                print(f"[DEBUG] Found {len(data)} records in data_merged_summary_alerts_data")
                if data:
                    df = pd.DataFrame(data)
                    print(f"[DEBUG] DataFrame columns: {df.columns.tolist()}")
                    print(f"[DEBUG] DataFrame shape: {df.shape}")
                    return df
                else:
                    print(f"[WARN] No batch data found for sku={sku}, machine={machine}")
                    return None
                    
        except Exception as e:
            print(f"[ERROR] Error fetching batch data for machine {machine}: {e}")
            import traceback
            traceback.print_exc()
            return None
        finally:
            try:
                client.close()
            except:
                pass

    # ── Function to calculate yields for a specific machine ──
    def calculate_yields_for_machine(batch_df, total_bc, machine_obj):
        """Calculate all yields for a specific machine's batch data"""
        # Initialize with default values
        wh_percent, wh_num, wh_den = 0.0, 0, total_bc if total_bc > 0 else 1
        uni_percent, uni_num, uni_den = 0.0, 0, total_bc if total_bc > 0 else 1
        vis_percent, vis_num, vis_den = 0.0, 0, total_bc if total_bc > 0 else 1
        rfv_percent, rfv_num, rfv_den = 0.0, 0, total_bc if total_bc > 0 else 1
        rfh1_percent, rfh1_num, rfh1_den = 0.0, 0, total_bc if total_bc > 0 else 1
        rfh2_percent, rfh2_num, rfh2_den = 0.0, 0, total_bc if total_bc > 0 else 1
        lfv_percent, lfv_num, lfv_den = 0.0, 0, total_bc if total_bc > 0 else 1
        plysteer_percent, plysteer_num, plysteer_den = 0.0, 0, total_bc if total_bc > 0 else 1
        
        # If batch data is available, use it to calculate yields per machine
        if batch_df is not None and not batch_df.empty:
            try:
                # Calculate each yield using the machine-specific data
                wh_percent, wh_num, wh_den = overall_warehouse_yield(batch_df)
                uni_percent, uni_num, uni_den = overall_uniformity_yield(batch_df)
                vis_percent, vis_num, vis_den = overall_visual_yield(batch_df)
                rfv_percent, rfv_num, rfv_den = rfv_yield(batch_df)
                rfh1_percent, rfh1_num, rfh1_den = rfh1_yield(batch_df)
                rfh2_percent, rfh2_num, rfh2_den = rfh2_yield(batch_df)
                lfv_percent, lfv_num, lfv_den = lfv_yield(batch_df)
                plysteer_percent, plysteer_num, plysteer_den = plysteer_yield(batch_df)
                
                print(f"[DEBUG] Calculated yields for machine from batch data:")
                print(f"  WH: {wh_percent}% ({wh_num}/{wh_den})")
                print(f"  UNI: {uni_percent}% ({uni_num}/{uni_den})")
                print(f"  VIS: {vis_percent}% ({vis_num}/{vis_den})")
                print(f"  RFH1: {rfh1_percent}% ({rfh1_num}/{rfh1_den})")
                
            except Exception as e:
                print(f"[ERROR] Error calculating yields from batch data: {e}")
                import traceback
                traceback.print_exc()
        else:
            # If no batch data, try to get yields from the machine object's parameter_alerts
            print(f"[DEBUG] No batch data available, trying to get yields from parameter_alerts")
            
            # Try to get yields from parameter_alerts
            for param in machine_obj.get('parameter_alerts', []):
                param_name = param.get('parameter')
                current_val = param.get('current_value')
                
                if param_name == 'overall_warehouse_yield':
                    wh_percent = float(current_val) if current_val else 0.0
                    wh_num = int((wh_percent / 100) * total_bc) if total_bc > 0 else 0
                    wh_den = total_bc if total_bc > 0 else 1
                elif param_name == 'overall_uniformity_yield':
                    uni_percent = float(current_val) if current_val else 0.0
                    uni_num = int((uni_percent / 100) * total_bc) if total_bc > 0 else 0
                    uni_den = total_bc if total_bc > 0 else 1
                elif param_name == 'overall_visual_yield':
                    vis_percent = float(current_val) if current_val else 0.0
                    vis_num = int((vis_percent / 100) * total_bc) if total_bc > 0 else 0
                    vis_den = total_bc if total_bc > 0 else 1
                elif param_name == 'rfv_yield':
                    rfv_percent = float(current_val) if current_val else 0.0
                    rfv_num = int((rfv_percent / 100) * total_bc) if total_bc > 0 else 0
                    rfv_den = total_bc if total_bc > 0 else 1
                elif param_name == 'rfh1_yield':
                    rfh1_percent = float(current_val) if current_val else 0.0
                    rfh1_num = int((rfh1_percent / 100) * total_bc) if total_bc > 0 else 0
                    rfh1_den = total_bc if total_bc > 0 else 1
                elif param_name == 'rfh2_yield':
                    rfh2_percent = float(current_val) if current_val else 0.0
                    rfh2_num = int((rfh2_percent / 100) * total_bc) if total_bc > 0 else 0
                    rfh2_den = total_bc if total_bc > 0 else 1
                elif param_name == 'lfv_yield':
                    lfv_percent = float(current_val) if current_val else 0.0
                    lfv_num = int((lfv_percent / 100) * total_bc) if total_bc > 0 else 0
                    lfv_den = total_bc if total_bc > 0 else 1
                elif param_name == 'plysteer_yield':
                    plysteer_percent = float(current_val) if current_val else 0.0
                    plysteer_num = int((plysteer_percent / 100) * total_bc) if total_bc > 0 else 0
                    plysteer_den = total_bc if total_bc > 0 else 1
        
        # CRITICAL FIX: Recalculate yield percentage using the machine's specific numerator and denominator
        # This ensures the yield % is calculated from the machine's own num/den, not from aggregate data
        if wh_den > 0:
            wh_percent = round((wh_num / wh_den) * 100, 2)
        if uni_den > 0:
            uni_percent = round((uni_num / uni_den) * 100, 2)
        if vis_den > 0:
            vis_percent = round((vis_num / vis_den) * 100, 2)
        if rfv_den > 0:
            rfv_percent = round((rfv_num / rfv_den) * 100, 2)
        if rfh1_den > 0:
            rfh1_percent = round((rfh1_num / rfh1_den) * 100, 2)
        if rfh2_den > 0:
            rfh2_percent = round((rfh2_num / rfh2_den) * 100, 2)
        if lfv_den > 0:
            lfv_percent = round((lfv_num / lfv_den) * 100, 2)
        if plysteer_den > 0:
            plysteer_percent = round((plysteer_num / plysteer_den) * 100, 2)
        
        print(f"[DEBUG] Final yields after recalculation:")
        print(f"  WH: {wh_percent}% ({wh_num}/{wh_den})")
        print(f"  UNI: {uni_percent}% ({uni_num}/{uni_den})")
        print(f"  VIS: {vis_percent}% ({vis_num}/{vis_den})")
        print(f"  RFH1: {rfh1_percent}% ({rfh1_num}/{rfh1_den})")

        return {
            'wh': (wh_percent, wh_num, wh_den),
            'uni': (uni_percent, uni_num, uni_den),
            'vis': (vis_percent, vis_num, vis_den),
            'rfv': (rfv_percent, rfv_num, rfv_den),
            'rfh1': (rfh1_percent, rfh1_num, rfh1_den),
            'rfh2': (rfh2_percent, rfh2_num, rfh2_den),
            'lfv': (lfv_percent, lfv_num, lfv_den),
            'plysteer': (plysteer_percent, plysteer_num, plysteer_den)
        }

    # Initialize list to collect one entry per (alert_doc, machine) pair
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

    # Create payload title
    if alert_type.upper() == "TBM":
        title_str = "Alert: TBM Alert Summary"
    elif alert_type.upper() == "CURING":
        title_str = "Alert: CURING Alert Summary"
    elif alert_type.upper() == "UFM":
        title_str = "Alert: UFM Alert Summary"
    else:
        title_str = "Alert Summary"

    # Convert summary HTML to markdown
    markdown_table = html_table_to_markdown(summary_html)

    # ── Machine Breakdown ──
    breakdown_blocks = []

    for entry in alert_machine_entries:
        sku = entry['sku']
        machine_obj = entry['machine_obj']
        alert_obj = entry['alert_obj']
        _at = alert_type.upper()

        def _color(actual, spec):
            try:
                if actual is None or spec is None:
                    return "none"
                return "red" if float(actual) < float(spec) else "green"
            except (TypeError, ValueError):
                return "none"

        # ── Get the start and end times for this alert ──
        start_time = alert_obj.get('machine_lot_start_build_time' if _at == "TBM" else 
                                   'machine_lot_start_curing_time' if _at == "CURING" else 
                                   'machine_lot_start_ufm_time')
        end_time = alert_obj.get('machine_lot_end_build_time' if _at == "TBM" else 
                                 'machine_lot_end_curing_time' if _at == "CURING" else 
                                 'machine_lot_end_ufm_time')
        
        # ── Get spec values ──
        sku_specs = yield_spec_lookup.get(str(sku).strip(), {})
        spec_wh = sku_specs.get('spec_overall_warehouse_yield')
        spec_uni = sku_specs.get('spec_overall_uniformity_yield')
        spec_vis = sku_specs.get('spec_overall_visual_yield')
        spec_rfv = sku_specs.get('spec_rfv_yield')
        spec_rfh1 = sku_specs.get('spec_rfh1_yield')
        spec_rfh2 = sku_specs.get('spec_rfh2_yield')
        spec_lfv = sku_specs.get('spec_lfv_yield')
        spec_plysteer = sku_specs.get('spec_plysteer_yield')

        # ── Rows for this individual breakdown ──
        entry_rows = []

        def _add_row_with_yields(mname, mtype, bc, machine_name_for_fetch):
            """Add a row with yields calculated specifically for this machine"""
            # Fetch batch data for this specific machine
            batch_df = fetch_batch_data_by_machine(sku, machine_name_for_fetch, start_time, end_time)
            
            # Calculate yields for this machine
            yields = calculate_yields_for_machine(batch_df, bc, machine_obj)
            
            entry_rows.append({
                'mname': mname,
                'mtype': mtype,
                'bc': bc,
                'yields': yields
            })

        # Build rows based on alert type
        if _at == "UFM":
            main_machine = machine_obj.get('machine', 'N/A')
            total_bc = machine_obj.get('processed_machine_barcodes', 0)
            _add_row_with_yields(
                sanitize_text(main_machine), 
                "UFM", 
                int(total_bc) if total_bc else 0,
                main_machine
            )
            
            for c in machine_obj.get('cur_ids', []):
                bc = c.get('barcode', 0)
                cur_id = c.get('cur_id', 'N/A')
                _add_row_with_yields(
                    sanitize_text(str(cur_id)), 
                    "Curing", 
                    int(bc),
                    cur_id
                )
            
            tbm_ids_raw = machine_obj.get('tbm_ids', [])
            tbm_machine = machine_obj.get('tbm_machine', 'N/A')
            if tbm_ids_raw:
                for t in tbm_ids_raw:
                    bc = t.get('barcode', 0)
                    tbm_id = t.get('tbm_id', 'N/A')
                    _add_row_with_yields(
                        sanitize_text(str(tbm_id)), 
                        "TBM", 
                        int(bc),
                        tbm_id
                    )
            elif tbm_machine and tbm_machine != 'N/A':
                _add_row_with_yields(
                    tbm_machine, 
                    "TBM", 
                    int(total_bc) if total_bc else 0,
                    tbm_machine
                )

        elif _at == "TBM":
            main_machine = machine_obj.get('machine', 'N/A')
            total_bc = machine_obj.get('processed_machine_barcodes', 0)
            _add_row_with_yields(
                sanitize_text(main_machine), 
                "TBM", 
                int(total_bc) if total_bc else 0,
                main_machine
            )
            
            seen_ubno = set()
            for ma in alert_obj.get("machine_alerts", []):
                for rec in ma.get("grouped_averages", {}).get("uf_id", []):
                    uf = rec.get('uf_id', '')
                    bc = rec.get('barcode_count', 0)
                    if uf and uf not in seen_ubno:
                        seen_ubno.add(uf)
                        _add_row_with_yields(
                            sanitize_text(uf), 
                            "UFM", 
                            int(bc),
                            uf
                        )
            
            for c in machine_obj.get('cur_ids', []):
                bc = c.get('barcode', 0)
                cur_id = c.get('cur_id', 'N/A')
                _add_row_with_yields(
                    sanitize_text(str(cur_id)), 
                    "Curing", 
                    int(bc),
                    cur_id
                )

        elif _at == "CURING":
            main_machine = machine_obj.get('machine', 'N/A')
            total_bc = machine_obj.get('processed_machine_barcodes', 0)
            _add_row_with_yields(
                sanitize_text(main_machine), 
                "Curing", 
                int(total_bc) if total_bc else 0,
                main_machine
            )
            
            seen_ubno = set()
            for ma in alert_obj.get("machine_alerts", []):
                for rec in ma.get("grouped_averages", {}).get("uf_id", []):
                    uf = rec.get('uf_id', '')
                    bc = rec.get('barcode_count', 0)
                    if uf and uf not in seen_ubno:
                        seen_ubno.add(uf)
                        _add_row_with_yields(
                            sanitize_text(uf), 
                            "UFM", 
                            int(bc),
                            uf
                        )
            
            for p in machine_obj.get('ptb_ids', []):
                bc = p.get('barcode', 0)
                ptb_id = p.get('ptb_id', 'N/A')
                _add_row_with_yields(
                    sanitize_text(str(ptb_id)), 
                    "TBM", 
                    int(bc),
                    ptb_id
                )

        if not entry_rows:
            continue

        # Build HTML table for this entry
        primary_machine = sanitize_text(machine_obj.get('machine', 'N/A'))

        html_table = f"<p><strong>SKU: {sku} | Machine: {primary_machine}</strong></p>"
        html_table += "<table border='1' cellpadding='5' cellspacing='0' style='border-collapse: collapse; width: 100%;'>"
        html_table += "<thead><tr style='background-color: #4A90E2; color: white;'>"
        
        html_table += (
            "<th>Machine</th><th>Machine Type</th>"
            "<th>Total Tyres</th>"
            "<th>WH Num</th><th>WH Den</th><th>WAREHOUSE Yield %</th>"
            "<th>UNI Num</th><th>UNI Den</th><th>UNIFORMITY Yield %</th>"
            "<th>VIS Num</th><th>VIS Den</th><th>VISUAL Yield %</th>"
            "<th>RFV Num</th><th>RFV Den</th><th>RFV Yield %</th>"
            "<th>RFH1 Num</th><th>RFH1 Den</th><th>RFH1 Yield %</th>"
            "<th>RFH2 Num</th><th>RFH2 Den</th><th>RFH2 Yield %</th>"
            "<th>LFV Num</th><th>LFV Den</th><th>LFV Yield %</th>"
            "<th>PLYSTEER Num</th><th>PLYSTEER Den</th><th>PLYSTEER Yield %</th>"
        )
        
        html_table += "</tr></thead><tbody>"

        def _fmt_yield(yield_percent):
            if yield_percent is None:
                return "N/A"
            try:
                return f"{float(yield_percent):.2f}%"
            except (TypeError, ValueError):
                return "N/A"

        def _fmt_count(count):
            if count is None:
                return "N/A"
            try:
                return str(int(count))
            except (TypeError, ValueError):
                return "N/A"

        def _get_color_indicator(yield_percent, spec_val):
            if yield_percent is None or spec_val is None:
                return ""
            try:
                if float(yield_percent) < float(spec_val):
                    return "🔴"
                else:
                    return "🟢"
            except (TypeError, ValueError):
                return ""

        for row in entry_rows:
            mname = row['mname']
            mtype = row['mtype']
            bc = row['bc']
            yields = row['yields']
            
            wh_percent, wh_num, wh_den = yields['wh']
            uni_percent, uni_num, uni_den = yields['uni']
            vis_percent, vis_num, vis_den = yields['vis']
            rfv_percent, rfv_num, rfv_den = yields['rfv']
            rfh1_percent, rfh1_num, rfh1_den = yields['rfh1']
            rfh2_percent, rfh2_num, rfh2_den = yields['rfh2']
            lfv_percent, lfv_num, lfv_den = yields['lfv']
            plysteer_percent, plysteer_num, plysteer_den = yields['plysteer']
            
            wh_color = _get_color_indicator(wh_percent, spec_wh)
            uni_color = _get_color_indicator(uni_percent, spec_uni)
            vis_color = _get_color_indicator(vis_percent, spec_vis)
            rfv_color = _get_color_indicator(rfv_percent, spec_rfv)
            rfh1_color = _get_color_indicator(rfh1_percent, spec_rfh1)
            rfh2_color = _get_color_indicator(rfh2_percent, spec_rfh2)
            lfv_color = _get_color_indicator(lfv_percent, spec_lfv)
            plysteer_color = _get_color_indicator(plysteer_percent, spec_plysteer)

            html_table += "<tr>"
            html_table += f"<td>{mname}</td><td>{mtype}</td>"
            html_table += f"<td>{bc}</td>"
            
            html_table += f"<td>{_fmt_count(wh_num)}</td>"
            html_table += f"<td>{_fmt_count(wh_den)}</td>"
            html_table += f"<td>{_fmt_yield(wh_percent)} {wh_color}</td>"
            
            html_table += f"<td>{_fmt_count(uni_num)}</td>"
            html_table += f"<td>{_fmt_count(uni_den)}</td>"
            html_table += f"<td>{_fmt_yield(uni_percent)} {uni_color}</td>"
            
            html_table += f"<td>{_fmt_count(vis_num)}</td>"
            html_table += f"<td>{_fmt_count(vis_den)}</td>"
            html_table += f"<td>{_fmt_yield(vis_percent)} {vis_color}</td>"
            
            html_table += f"<td>{_fmt_count(rfv_num)}</td>"
            html_table += f"<td>{_fmt_count(rfv_den)}</td>"
            html_table += f"<td>{_fmt_yield(rfv_percent)} {rfv_color}</td>"
            
            html_table += f"<td>{_fmt_count(rfh1_num)}</td>"
            html_table += f"<td>{_fmt_count(rfh1_den)}</td>"
            html_table += f"<td>{_fmt_yield(rfh1_percent)} {rfh1_color}</td>"
            
            html_table += f"<td>{_fmt_count(rfh2_num)}</td>"
            html_table += f"<td>{_fmt_count(rfh2_den)}</td>"
            html_table += f"<td>{_fmt_yield(rfh2_percent)} {rfh2_color}</td>"
            
            html_table += f"<td>{_fmt_count(lfv_num)}</td>"
            html_table += f"<td>{_fmt_count(lfv_den)}</td>"
            html_table += f"<td>{_fmt_yield(lfv_percent)} {lfv_color}</td>"
            
            html_table += f"<td>{_fmt_count(plysteer_num)}</td>"
            html_table += f"<td>{_fmt_count(plysteer_den)}</td>"
            html_table += f"<td>{_fmt_yield(plysteer_percent)} {plysteer_color}</td>"
            
            html_table += "</tr>"

        html_table += "</tbody></table>"
        breakdown_blocks.append(html_table)

    # ── Send Teams messages ──
    # Microsoft Teams incoming webhooks reject a single message once it grows
    # past ~28 KB and can silently drop large payloads. Packing every alert into
    # ONE message causes alerts to disappear when there are many. So when there
    # are more than 10 breakdown blocks we send them BATCH-WISE (multiple blocks
    # per message, each kept under the size limit) instead of one giant message.
    TEAMS_TEXT_LIMIT = 20000  # conservative per-message character budget
    BATCH_THRESHOLD = 10      # >10 blocks -> send batch-wise

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
        body_indicates_error = ("error" in body.lower()) or ("too large" in body.lower())
        if resp.status_code in (200, 202) and not body_indicates_error:
            print(f"[INFO] Teams {label} sent successfully for {alert_type}")
            return True
        print(f"[ERROR] Failed to send Teams {label}: {resp.status_code} {body[:300]}")
        return False

    def _split_block(block_html):
        """Split a single oversized block on <tr> boundaries so no cell breaks."""
        if len(block_html) <= TEAMS_TEXT_LIMIT:
            return [block_html]
        parts = block_html.split("</tr>")
        chunks, current = [], ""
        for part in parts:
            piece = part + "</tr>" if part.strip() else part
            if len(current) + len(piece) > TEAMS_TEXT_LIMIT and current:
                chunks.append(current)
                current = piece
            else:
                current += piece
        if current.strip():
            chunks.append(current)
        return chunks

    def _batch_blocks(blocks):
        """Group breakdown blocks into batches that each stay under the limit."""
        batches, current, current_len = [], [], 0
        separator = "<br><br>"
        for block in blocks:
            if len(block) > TEAMS_TEXT_LIMIT:
                if current:
                    batches.append(current)
                    current, current_len = [], 0
                for chunk in _split_block(block):
                    batches.append([chunk])
                continue
            added_len = len(block) + (len(separator) if current else 0)
            if current and (current_len + added_len) > TEAMS_TEXT_LIMIT:
                batches.append(current)
                current, current_len = [block], len(block)
            else:
                current.append(block)
                current_len += added_len
        if current:
            batches.append(current)
        return batches

    overall_ok = True

    # ── Message 1: summary table ──
    summary_ok = _post_teams(f"**{title_str}**\n\n{markdown_table}", "summary")
    overall_ok = overall_ok and summary_ok

    # ── Machine breakdown ──
    if breakdown_blocks:
        if len(breakdown_blocks) > BATCH_THRESHOLD:
            # More than 10 -> send batch-wise (multiple blocks per message)
            batches = _batch_blocks(breakdown_blocks)
            total_batches = len(batches)
            for b_idx, batch in enumerate(batches, 1):
                header = f"**Machine Breakdown — Batch {b_idx}/{total_batches}**<br><br>"
                body = "<br><br>".join(batch)
                ok = _post_teams(header + body, f"breakdown batch {b_idx}/{total_batches}")
                overall_ok = overall_ok and ok
        else:
            # 10 or fewer -> single breakdown message (split only if oversized)
            machine_breakdown_html = "<br><br>".join(breakdown_blocks)
            for sub_i, chunk in enumerate(_split_block(machine_breakdown_html), 1):
                label = "breakdown" if sub_i == 1 else f"breakdown (part {sub_i})"
                ok = _post_teams(f"**Machine Breakdown**<br><br>{chunk}", label)
                overall_ok = overall_ok and ok
    else:
        overall_ok = overall_ok and _post_teams(
            "**Machine Breakdown**\n\n<p>No data available.</p>", "breakdown"
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
    """Format datetime to YYYY-Month-DD format for shift data."""
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
        # Header with Group column
        self.set_fill_color(106, 13, 173)
        self.set_text_color(255)
        self.set_font('Arial', 'B', 9)
        
        # Header row
        def print_header():
            # Header row (keep all borders)
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

            # Check if the group fits on the current page
            if self.get_y() + total_height + 10 > self.h - self.b_margin:
                self.add_page()
                print_header()
                self.set_text_color(0)
            
            # Calculate vertical position
            y_start = self.get_y()
            
            # Draw rows with special border handling
            for i, key in enumerate(keys):
                # Stage cell borders:
                # - Always left border (L)
                # - Top border for first row (T)
                # - Bottom border for last row (B)
                border = 'L'  # Always keep left border
                if i == 0:
                    border += 'T'  # Add top border for first row
                if i == len(keys) - 1:
                    border += 'B'  # Add bottom border for last row
                
                self.cell(widths[0], row_h, "", border)
                
                # Parameter cell - keep all borders
                param_name = sanitize_text(key.replace('_', ' ').upper())
                self.cell(widths[1], row_h, param_name, 1)
                
                # Data cells - keep all borders
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
            
            # Draw the merged group label with left and right borders
            self.set_xy(self.l_margin, y_start)
            self.set_font('Arial', 'B', 9)
            self.cell(widths[0], total_height, sanitize_text(group_label), 'LR', 0, 'C')
            
            # Move to next position
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
            # self.cell(widths[1] + widths[2], h, format_datetime(m['start']), 1, 0, 'C')
            self.cell(widths[1] + widths[2], h, str(format_datetime(m['start'])), 1, 0, 'C')
        self.ln()

        self.cell(widths[0],h,'End Time',1)
        for m in machines:
            # self.cell(widths[1] + widths[2], h, format_datetime(m['end']), 1, 0, 'C')
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
        self.ln(5)

        # Barcodes section at the bottom
        if len(machines) > 1:
            # Side-by-side layout for multiple machines
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
            # Single machine layout
            m = machines[0]
            self.set_fill_color(106, 13, 173)
            self.set_text_color(255)
            self.set_font('Arial', 'B', 9)
            self.cell(usable, h, f"{sanitize_text(m['uf_id'])} Barcodes", 1, 0, 'C', fill=True)
            self.ln()
            self.set_text_color(0)
            self.set_font('Arial', '', 9)
            
            # Custom layout: 5 barcodes per row
            barcodes = m['barcodes']
            chunk_size = 5
            barcode_width = usable / chunk_size
            total_rows = len(barcodes) // chunk_size + (1 if len(barcodes) % chunk_size else 0)

            for row in range(total_rows):
                start_idx = row * chunk_size
                chunk = barcodes[start_idx:start_idx + chunk_size]
                for b in chunk:
                    self.cell(barcode_width, h, sanitize_text(str(b)) if b is not None else "", 1, 0, 'L')
                # Fill remaining cells if the row has fewer than 5 barcodes
                for _ in range(chunk_size - len(chunk)):
                    self.cell(barcode_width, h, "", 1, 0, 'L')
                self.ln()
    
    def barcodes_table(self, barcodes, width):
        """
        Generates a table displaying the barcodes in chunks.
        """
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

        # Chunk barcodes into groups of 5 for readability
        chunk_size = 5
        max_barcodes = 100  # Limit to 100 barcodes
        for i in range(0, min(len(barcodes), max_barcodes), chunk_size):
            chunk = barcodes[i:i + chunk_size]
            barcode_str = ", ".join(sanitize_text(str(b)) for b in chunk if b is not None)
            self.cell(width, row_h, barcode_str, 1, 0, 'L')
            self.ln()

    def shift_data_table(self, shifts_data, machine_name, alert_type, width):
        """
        Generates a table displaying shift data with shift names and barcodes.
        """
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
                  
        # Prepare shift data
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

        # Limit to 3 shifts for table layout
        shift_info = shift_info[:3]
        num_shifts = len(shift_info)
        if num_shifts == 0:
            self.set_font('Arial', 'B', 9)
            self.cell(width, 6, "No valid shift data available", 1, 0, 'C')
            self.ln()
            return

        # Calculate column widths
        col_width = width / max(num_shifts, 1)
        row_h = 6
        # Print shift headers
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

        # Print barcodes
        self.set_fill_color(106, 13, 173)
        self.set_text_color(255)
        self.set_font('Arial', 'B', 9)
        for _ in shift_info:
            self.cell(col_width, 7, "Barcodes", 1, 0, 'C', fill=True)
        self.ln()
        self.set_text_color(0)
        self.set_font('Arial', '', 9)

        # Find max number of barcodes to determine rows
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
        'sku': doc['sku'],
        'target_machine_lot_size': doc.get('target_machine_lot_size'),
        atype['alert_time_key']: doc.get(atype['alert_time_key'])
    }

    raw  = alert['parameter_alerts']
    data = {p['parameter']: p for p in raw}
    for p in data.values():
        c, s = p.get('current_value'), p.get('spec_value')
        p['spec_diff'] = float(c)-float(s) if c is not None and s is not None else None

    # filter keys present
    all_ufm = PARAM_KEYS[:10]
    all_geometery = PARAM_KEYS[10:20]
    all_bal = PARAM_KEYS[20:23]
    all_load = PARAM_KEYS[23:27]
    all_yields = PARAM_KEYS[27:]
    ufm_keys = [k for k in all_ufm if k in data]
    geometry_keys = [k for k in all_geometery if k in data]
    bal_keys = [k for k in all_bal if k in data]
    load_keys = [k for k in all_load if k in data]
    yields_keys = [k for k in all_yields if k in data]

    pdf = PDF()
    pdf.add_page()

     # 1) Compute our twoâ€column split
    usable = pdf.w - pdf.l_margin - pdf.r_margin
    half   = usable / 2
    x1     = pdf.l_margin
    x2     = pdf.l_margin + half
    label_1=55
    value_1=half-label_1
    line_h = 6

    # 2) Draw the banner on the left half
    pdf.set_xy(x1, pdf.get_y())
    pdf.set_font('Arial','B',11)
    pdf.cell(half, line_h, f"{sanitize_text(atype['name'])} ALERT", 0, 0, 'L')



    # 3) Compute identifier string		   
    # Extract timestamp
    raw_ts = alert.get(atype['alert_time_key'])

    # Handle dict-based timestamp
    if isinstance(raw_ts, dict) and '$date' in raw_ts:
        raw_ts = raw_ts['$date']

    # Try parsing the timestamp safely
    ts_str = None
    if raw_ts:
        try:
            # Parse the timestamp
            ts_parsed = pd.to_datetime(raw_ts, errors='coerce', utc=True)
            if not pd.isna(ts_parsed):  # Use pd.isna instead of pd.isnull for consistency
                ts_str = ts_parsed.strftime("%Y%m%d_%H%M%S")
            else:
                print(f"[DEBUG] Timestamp parsing resulted in NaT for raw_ts: {raw_ts}")
        except Exception as e:
            print(f"[DEBUG] Error parsing timestamp {raw_ts}: {e}")

    # Safely fetch values with defaults
    atype_name = sanitize_text(str(atype.get('name', 'UNKNOWN')))
    machine = sanitize_text(str(alert.get('machine', 'UNKNOWN')))
    modelno = sanitize_text(str(alert.get('sku', 'UNKNOWN')))

    # Build identifier
    identifier = f":{atype_name}_{machine}_{modelno}_{ts_str if ts_str else 'None'}"

										
    pdf.set_font('Arial','B',11)
    pdf.set_xy(x2, pdf.get_y())
    pdf.cell(value_1, line_h, sanitize_text(identifier), 0, 1, 'R')

    pdf.ln(2)


    # Details
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

    # UFM/BAL tables
    # UFM/BAL parameter tables
							  
    pdf.section_banner(f"{sanitize_text(atype['name'])} STAGE")
    usable = pdf.w - pdf.l_margin - pdf.r_margin

												
    group_w = 25
    param_w = usable * 0.45 - group_w
    val_w = (usable - group_w - param_w) / 3
    widths = [group_w, param_w, val_w, val_w, val_w]

																					   
    ufm_keys = [k for k in PARAM_KEYS[:10] if k in data]
    geometry_keys = [k for k in PARAM_KEYS[10:20] if k in data]
    bal_keys = [k for k in PARAM_KEYS[20:23] if k in data]
    load_keys = [k for k in PARAM_KEYS[23:27] if k in data]
    yields_keys = [k for k in PARAM_KEYS[27:] if k in data]
    group_defs = [("UFM", ufm_keys), ("GEOMETRY", geometry_keys), ("BALANCING", bal_keys), ("LOAD", load_keys), ("YIELDS", yields_keys)]
                  
    # PREV / CURRENT / DIFF
    pdf.grouped_parameter_table(
        headers=["Parameters", "PREV", "CURRENT", "DIFF"],
        widths=widths,
        group_defs=group_defs,
        data=data,
        cols=['previous_value', 'current_value', 'difference']
    )

    # separate second table
    pdf.ln(5)
    pdf.section_banner("SPECIFICATIONS")

							
    pdf.grouped_parameter_table(
        headers=["Parameters","SPEC","CURRENT","SPEC DIFF"],
        widths=widths,
        group_defs=[("UFM", ufm_keys), ("GEOMETRY", geometry_keys), ("BALANCING", bal_keys), ("LOAD", load_keys), ("YIELDS", yields_keys)],
                                                                                   
        data=data,
        cols=['spec_value','current_value','spec_diff']
							  
    )

    if atype['name'].upper() == "UFM":
        pdf.ln(10)
        pdf.section_banner("BARCODES")
        barcodes = alert.get('barcodes', [])
        pdf.barcodes_table(barcodes, usable)

    # UBNO summary
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
    
    # Add shift data section
    pdf.ln(10)
    shift_data = alert.get(atype['shift_data_key'], [])
    machine_name = alert.get('tbm_machine' if atype['name'].upper() in ['CURING', 'UFM'] else 'machine', 'N/A')
    pdf.shift_data_table(shift_data, machine_name, atype['name'], usable)
								 
    return pdf.output(dest='S')
    # return pdf.output(dest='S').encode('latin-1')

def get_admin_emails(client):
    cols = client[alert_mail_db][alert_users]
    users = cols.find(
        {"is_alert_user": True},
        {"_id": 0, "email": 1}
    )
    emails = [u["email"] for u in users if u.get("email")]
    #return [r['email'] for r in cols.find({}, {'_id':0,'email':1})]
    return emails

def send_alerts():
    """
    Sends alerts by grouping PDFs by modelno across all work centers (TBM, CURING, UFM).
    """
    client = MongoClient(current_app.config['MONGO_URI'])
    db = client[ALERT_DB_NAME]
    emails = get_admin_emails(client)
    
    if not emails:
        print("[ERROR] No admin emails")
        return

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    total_pdfs_processed = 0

    # Collect all alerts across all collections
    all_alerts = []
    for at in ALERT_TYPES:
        coll = db[at['coll']]
        docs = list(coll.find({'alert_flag': False}))
        if docs:
            # Send Teams alert for each work center (unchanged)
            summary_html = generate_combined_summary_table(docs, at['name'])
            send_teams_alert(summary_html, at['name'], docs)
            # Add alert type to each document for PDF generation
            for doc in docs:
                doc['alert_type'] = at
                all_alerts.append(doc)

    if not all_alerts:
        print("[INFO] No new alerts.")
        return

    # Group alerts by modelno
    alerts_by_modelno = {}
    for doc in all_alerts:
        modelno = sanitize_text(doc.get('sku', 'N/A'))
        if modelno not in alerts_by_modelno:
            alerts_by_modelno[modelno] = []
        alerts_by_modelno[modelno].append(doc)

    # Process each modelno group
    for modelno, docs in alerts_by_modelno.items():
        # Collect size codes for this modelno
        size_codes = sorted(set(sanitize_text(m.get('sizecode', 'N/A')) for d in docs for m in d.get('machine_alerts', [])))
        sizecode_str = ", ".join(size_codes) if size_codes else "N/A"

        # Generate PDFs for all alerts in this modelno
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
                'alert_type': at['name']  # Store alert type name for email body
            })

        # Batch PDFs based on size
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
                            # Mark alerts sent in MongoDB for this batch
                            for pdf_item in current_batch:
                                db[pdf_item['collection']].update_one(
                                    {'_id': pdf_item['doc_id']},
                                    {'$set': {'alert_flag': True, doc['alert_type']['alert_time_key']: datetime.now()}}
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
                            {'$set': {'alert_flag': True, doc['alert_type']['alert_time_key']: datetime.now()}}
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
# from config_hungary import (
#     alert_triggering_ubno_collect_name,
#     alert_triggering_collect_name,
#     alert_triggering_curing_collect_name,
#     # alert_db,
#     alert_db_virgin_yield,
#     alert_mail_db,
#     alert_users,
#     config_databse,
#     channel_no_collect,                  
#     MONGO_URI,
#     SENDER_EMAIL,
#     SENDER_PASSWORD,
#     TEAMS_WEBHOOK_URL

# )

# # --- Configuration ---
# MONGO_URI       =  MONGO_URI
# SENDER_EMAIL    = SENDER_EMAIL
# SENDER_PASSWORD = SENDER_PASSWORD
# ALERT_DB_NAME   = alert_db_virgin_yield
# CONFIG_DB_NAME = config_databse  # NEW: Database for OE Replacement lookup
# CHANNEL_NO_COLLECT = channel_no_collect  # NEW: Collection for OE Replacement                                                                            
# BASE_PDF_DIR    = "generated_pdfs"
																			  
# MAX_EMAIL_SIZE = 20 * 1024 * 1024  # 20 MB																			   

# # Full list of possible keys
# PARAM_KEYS = [
#     "rfppcc","rfppcw","rfh1cc","rfh1cw","rfh2cc","rfh2cw","lfppcc","lfppcw","cony","plys","crro","trro","brro",
#     "lrot","lrob","lroh1grade","tbul","bbul","tdep","bdep","upper","lower","sbal","aircc","aircw","loadcc","loadcw",
#     "rfv_yield", "rfh1_yield", "rfh2_yield", "lfv_yield", "conicity_yield", "plysteer_yield","center_radial_run_out_yield", 
#     "top_radial_run_out_yield", "bottom_radial_run_out_yield", "lateral_run_out_yield", "lateral_run_out_harmonic_first_yield",
#     "center_radial_run_out_harmonic_first_yield", "top_radial_run_out_harmonic_first_yield", "bottom_radial_run_out_harmonic_first_yield",
#     "lateral_radial_run_out_yield", "static_yield", "couple_yield","upper_yield", "lower_yield", "overall_uniformity_yield", 
#     "overall_visual_yield", "overall_warehouse_yield","overall_scrap_yield", "visual_loss_yield", "uf_loss_yield", "oe_ok_yield"
# ]

# ALERT_TYPES = [
#     {
#         "name": "UFM",
#         "coll": alert_triggering_ubno_collect_name,
#         "banner": "UFM ALERT",
#         "filename_prefix": "UFM",
#         "start_key": "machine_lot_start_ufm_time",
#         "end_key":   "machine_lot_end_ufm_time",
#         "alert_time_key": "timestamp_alert_generated",
#         "use_grouped": False,
#         "shift_data_key": "shifts_data_tbm",
#         "shift_start_time_key": "machine_shift_tbm_start_time"
#     },
#     {
#         "name": "TBM",
#         "coll":  alert_triggering_collect_name,
#         "banner": "TBM ALERT",
#         "filename_prefix": "TBM",
#         "start_key": "machine_lot_start_build_time",
#         "end_key":   "machine_lot_end_build_time",
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
#         "end_key":   "machine_lot_end_curing_time",
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
#     # Convert to string and replace problematic Unicode characters
#     text = str(text)
#     # Replace \ufffd and other problematic characters with a safe alternative
#     return text.encode('ascii', 'replace').decode('ascii').replace('\ufffd', '?')			

# def html_table_to_markdown(html_table):
#     """
#     Converts HTML table to Markdown format for better Teams compatibility
#     """
#     # Parse the HTML
#     from bs4 import BeautifulSoup
#     soup = BeautifulSoup(html_table, 'html.parser')
    
#     # Initialize markdown table
#     markdown_table = ""
    
#     # Process headers
#     headers = [sanitize_text(th.get_text()) for th in soup.find_all('th')]
#     markdown_table += "| " + " | ".join(headers) + " |\n"
#     markdown_table += "|" + "|".join(["---"] * len(headers)) + "|\n"
    
#     # Process rows
#     for row in soup.find_all('tr')[1:]:  # Skip header row
#         cells = [sanitize_text(td.get_text()) for td in row.find_all('td')]
#         markdown_table += "| " + " | ".join(cells) + " |\n"
    
#     return markdown_table

# def generate_combined_summary_table(alerts, alert_type):
#     """
#     Changed 14-04
#     Generates properly formatted HTML table for all alerts.
    
#     Args:
#         alerts (list): List of alert dictionaries containing relevant data.
#         alert_type (str): Type of alert ("TBM", "CURING", or "UBNO").
    
#     Returns:
#         str: HTML table as a string.
#     """
#     # Check if alerts list is empty
#     if not alerts:
#         return "<table><tr><td>No alert data available</td></tr></table>"
    
#     # Start HTML table
#     html = "<table>"
    
#     # Define headers based on alert_type
#     if alert_type.upper() == "TBM":
#         html += """
#         <tr>
#             <th>S.No</th>
#             <th>SKU</th>
#             <th>Size Code</th>
#             <th>Gt Code</th>
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
#             <th>Size Code</th>
#             <th>Gt Code</th>
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
#             <th>Gt Code</th>
#             <th>UFM</th>
#             <th>Total Tyre Produced</th>
#             <th>Overall WAREHOUSE Yield (%)</th>
#             <th>Overall UNIFORMITY Yield (%)</th>
#             <th>Overall VISUAL Yield (%)</th>
#             <th>Overall SCRAP Yield (%)</th>
#         </tr>
#         """

#     # Handle different alert types
#     if alert_type.upper() == "TBM":
#         # For TBM alerts
#         # for alert in alerts:
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
#                     <td>{sanitize_text(machine.get('sizecode', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('gt_code', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('machine', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('processed_machine_barcodes', 'N/A'))}</td>
#                     <td>{warehouse}</td>
#                     <td>{uniformity}</td>
#                     <td>{visual}</td>
#                     <td>{scrap}</td>
#                 </tr>
#                 """
    
#     elif alert_type.upper() == "CURING":
#         # For CURING alerts
#         # for alert in alerts:
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
#                     <td>{sanitize_text(machine.get('sizecode', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('gt_code', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('machine', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('processed_machine_barcodes', 'N/A'))}</td>
#                     <td>{warehouse}</td>
#                     <td>{uniformity}</td>
#                     <td>{visual}</td>
#                     <td>{scrap}</td>
#                 </tr>
#                 """
    
#     elif alert_type.upper() == "UFM":
#     # For CURING alerts
#         # for alert in alerts:
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
#                     <td>{sanitize_text(machine.get('sizecode', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('gt_code', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('machine', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('processed_machine_barcodes', 'N/A'))}</td>
#                     <td>{warehouse}</td>
#                     <td>{uniformity}</td>
#                     <td>{visual}</td>
#                     <td>{scrap}</td>
#                 </tr>
#                 """
    
#     # Close the table
#     html += "</table>"
#     return html


# def send_teams_alert(summary_html, alert_type, docs):
#     """
#     Sends a Microsoft Teams alert message with properly formatted table.
#     Args:
#         summary_html (str): HTML string of the summary table.
#         alert_type (str): Type of alert (UFM, TBM, CURING).
#         docs (list): List of alert documents from MongoDB.
#     """   
#     teams_webhook_url = TEAMS_WEBHOOK_URL
#     if not teams_webhook_url:
#         print("[ERROR] TEAMS_WEBHOOK_URL not configured.")
#         return False

#     # ── Fetch yield spec thresholds from channel_no_collect (keyed by sku) ──
#     yield_spec_lookup = {}   # {sku: {spec_overall_warehouse_yield, ..., spec_rfppwc_yield, ...}}
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
#              "spec_rfh2wc_yield": 1,
#              "spec_plys_yield": 1}
#         ):
#             sku_key = str(doc.get("sku", "")).strip()
#             if sku_key:
#                 yield_spec_lookup[sku_key] = {
#                     "spec_overall_warehouse_yield": doc.get("spec_overall_warehouse_yield"),
#                     "spec_overall_uniformity_yield": doc.get("spec_overall_uniformity_yield"),
#                     "spec_overall_visual_yield": doc.get("spec_overall_visual_yield"),
#                     "spec_rfv_yield":      doc.get("spec_rfppwc_yield"),
#                     "spec_rfh1_yield":     doc.get("spec_rfh1wc_yield"),
#                     "spec_rfh2_yield":     doc.get("spec_rfh2wc_yield"),
#                     "spec_plysteer_yield": doc.get("spec_plys_yield"),
#                 }
#         _mc.close()
#     except Exception as _e:
#         print(f"[WARN] Could not fetch yield specs from channel_no_collect: {_e}")

#     def _yield_status(actual_val, spec_val):
#         """Return (🔴 OE Yield Dropped) if actual < spec, else (🟢 OE Yield not Dropped)"""
#         try:
#             if actual_val is None or spec_val is None:
#                 return ""
#             if float(actual_val) < float(spec_val):
#                 return " (🔴 OE Yield Dropped)"
#             return " (🟢 OE Yield not Dropped)"
#         except (TypeError, ValueError):
#             return ""

#     # Initialize list to collect one entry per (alert_doc, machine) pair
#     # Each entry is: {sku, machine_name, sizecode, gt_code, params, machine_obj, alert_obj}
#     alert_machine_entries = []
#     # Aggregate deviated parameters across all alerts and machines
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
                   

#     # Keep deviated_by_gt_code as a compatibility alias (used nowhere else in this function)
#     deviated_by_gt_code = {
#         (e['sku'], e['machine_name'], e['sizecode'], e['gt_code']): e
#         for e in alert_machine_entries
#     }

#     # Create payload title
#     if alert_type.upper() == "TBM":
#         title_str = "Alert: TBM Alert Summary"
#     elif alert_type.upper() == "CURING":
#         title_str = "Alert: CURING Alert Summary"
#     elif alert_type.upper() == "UFM":
#         title_str = "Alert: UFM Alert Summary"
#     else:
#         title_str = "Alert Summary"

#     markdown_table = html_table_to_markdown(summary_html)
    
#     # ── Machine Breakdown — one HTML table per individual alert entry ──
#     def _ind(color_tag):
#         if color_tag == "green": return "✅"
#         if color_tag == "red":   return "🔴"
#         return ""

#     # Build one breakdown HTML table per individual (alert_doc × machine) entry
#     breakdown_blocks = []

#     for entry in alert_machine_entries:
#         sku         = entry['sku']
#         machine_obj = entry['machine_obj']
#         alert_obj   = entry['alert_obj']
#         _at         = alert_type.upper()

#         def _prop_yield(bc, total_bc, overall_val):
#             try:
#                 return float(overall_val) * int(bc) / int(total_bc) if int(total_bc) > 0 else None
#             except (TypeError, ValueError, ZeroDivisionError):
#                 return None

#         def _fmt(val):
#             try:
#                 return f"{float(val):.2f}%" if val not in (None, 'N/A') else "N/A"
#             except (TypeError, ValueError):
#                 return "N/A"

#         def _color(actual, spec):
#             try:
#                 if actual is None or spec is None:
#                     return "none"
#                 return "red" if float(actual) < float(spec) else "green"
#             except (TypeError, ValueError):
#                 return "none"

#         total_bc  = machine_obj.get('processed_machine_barcodes', 0)
#         wh_val    = machine_obj.get('overall_warehouse_yield')
#         uni_val   = machine_obj.get('overall_uniformity_yield')
#         vis_val   = machine_obj.get('overall_visual_yield')
#         oe_ok_val = machine_obj.get('oe_ok_yield')  # Get OE OK yield
#         sku_specs = yield_spec_lookup.get(str(sku).strip(), {})
#         spec_wh   = sku_specs.get('spec_overall_warehouse_yield')
#         spec_uni  = sku_specs.get('spec_overall_uniformity_yield')
#         spec_vis  = sku_specs.get('spec_overall_visual_yield')
#         # Spec values for the special yield columns
#         YIELD_SPEC_KEYS = {
#             "rfv_yield":      sku_specs.get('spec_rfv_yield'),
#             "rfh1_yield":     sku_specs.get('spec_rfh1_yield'),
#             "rfh2_yield":     sku_specs.get('spec_rfh2_yield'),
#             "plysteer_yield": sku_specs.get('spec_plysteer_yield'),
#         }

#         # Helper: read overall yield value for a named yield param from parameter_alerts
#         def _get_overall_yield(yield_param_name):
#             for p in machine_obj.get('parameter_alerts', []):
#                 if p.get('parameter') == yield_param_name:
#                     val = p.get('current_value')
#                     try:
#                         return float(val) if val not in (None, 'N/A') else None
#                     except (TypeError, ValueError):
#                         return None
#             return None

#         # Rows for this individual breakdown
#         # Each row tuple now carries per-row yield values for all active yield columns
#         entry_rows = []

#         def _add_row(mname, mtype, bc, wh, uni, vis, sw, su, sv, oe_yield=None, row_yield_vals=None):
#             """
#             row_yield_vals: dict {yield_field: numeric_value_or_None} — already proportional for sub-rows.
#             """
#             # Calculate OE tyres from oe_ok_yield % × barcode count
#             try:
#                 if oe_yield is not None and oe_yield != 'N/A' and bc:
#                     oe_t = int(float(bc) * (float(oe_yield) / 100))
#                 else:
#                     oe_t = "N/A"
#             except (TypeError, ValueError):
#                 oe_t = "N/A"
#             entry_rows.append((
#                 mname, mtype, bc, oe_t,
#                 _fmt(wh), _fmt(uni), _fmt(vis),
#                 _color(wh, sw), _color(uni, su), _color(vis, sv),
#                 row_yield_vals or {}
#             ))

#         # Pre-read overall values for the yield columns we will need
#         # (populated after active_special_params is known — resolved lazily below)
#         _overall_yield_cache = {}

#         def _get_cached_overall(yname):
#             if yname not in _overall_yield_cache:
#                 _overall_yield_cache[yname] = _get_overall_yield(yname)
#             return _overall_yield_cache[yname]

#         def _build_yield_vals(bc_sub, total_bc_val, yield_fields):
#             """Return dict of proportional yield values for sub-machine row."""
#             result = {}
#             for yf in yield_fields:
#                 ov = _get_cached_overall(yf)
#                 result[yf] = _prop_yield(bc_sub, total_bc_val, ov) if ov is not None else None
#             return result

#         def _build_yield_vals_full(yield_fields):
#             """Return dict of overall yield values for the primary machine row."""
#             return {yf: _get_cached_overall(yf) for yf in yield_fields}

#         # We need active_yield_cols before building rows — derive them now
#         # (duplicates the logic below, but needed to pass yield vals into _add_row)
#         _SPECIAL_PARAM_YIELD_MAP_PRE = {
#             "rfppcc": "rfv_yield", "rfppcw": "rfv_yield",
#             "rfh1cc": "rfh1_yield", "rfh1cw": "rfh1_yield",
#             "rfh2cc": "rfh2_yield", "rfh2cw": "rfh2_yield",
#             "plys":   "plysteer_yield",
#         }
#         _dev_names_pre = []
#         for _p in machine_obj.get('parameter_alerts', []):
#             _pn = _p.get('parameter', '')
#             if (_p.get('shift_occurred_thershold') or _p.get('shift_occurred_spec')) and _pn:
#                 _dev_names_pre.append(_pn)
#         _active_sp_pre = [p for p in _SPECIAL_PARAM_YIELD_MAP_PRE if p in _dev_names_pre]
#         _seen_y = set()
#         _pre_yield_fields = []
#         for sp in _active_sp_pre:
#             y = _SPECIAL_PARAM_YIELD_MAP_PRE[sp]
#             if y not in _seen_y:
#                 _seen_y.add(y)
#                 _pre_yield_fields.append(y)

#         if _at == "UFM":
#             _add_row(sanitize_text(machine_obj.get('machine', 'N/A')), "UFM",
#                      int(total_bc) if total_bc else 0,
#                      wh_val, uni_val, vis_val, spec_wh, spec_uni, spec_vis, oe_ok_val,
#                      _build_yield_vals_full(_pre_yield_fields))
#             for c in machine_obj.get('cur_ids', []):
#                 bc = c.get('barcode', 0)
#                 _add_row(sanitize_text(str(c.get('cur_id', 'N/A'))), "Curing", int(bc),
#                          _prop_yield(bc, total_bc, wh_val), _prop_yield(bc, total_bc, uni_val),
#                          _prop_yield(bc, total_bc, vis_val), spec_wh, spec_uni, spec_vis, oe_ok_val,
#                          _build_yield_vals(bc, total_bc, _pre_yield_fields))
#             tbm_ids_raw = machine_obj.get('tbm_ids', [])
#             tbm_machine = sanitize_text(machine_obj.get('tbm_machine', 'N/A'))
#             if tbm_ids_raw:
#                 for t in tbm_ids_raw:
#                     bc = t.get('barcode', 0)
#                     _add_row(sanitize_text(str(t.get('tbm_id', 'N/A'))), "TBM", int(bc),
#                              _prop_yield(bc, total_bc, wh_val), _prop_yield(bc, total_bc, uni_val),
#                              _prop_yield(bc, total_bc, vis_val), spec_wh, spec_uni, spec_vis, oe_ok_val,
#                              _build_yield_vals(bc, total_bc, _pre_yield_fields))
#             elif tbm_machine and tbm_machine != 'N/A':
#                 _add_row(tbm_machine, "TBM", int(total_bc) if total_bc else 0,
#                          wh_val, uni_val, vis_val, spec_wh, spec_uni, spec_vis, oe_ok_val,
#                          _build_yield_vals_full(_pre_yield_fields))

#         elif _at == "TBM":
#             _add_row(sanitize_text(machine_obj.get('machine', 'N/A')), "TBM",
#                      int(total_bc) if total_bc else 0,
#                      wh_val, uni_val, vis_val, spec_wh, spec_uni, spec_vis, oe_ok_val,
#                      _build_yield_vals_full(_pre_yield_fields))
#             seen_ubno = set()
#             for ma in alert_obj.get("machine_alerts", []):
#                 for rec in ma.get("grouped_averages", {}).get("uf_id", []):
#                     uf = sanitize_text(rec.get('uf_id', ''))
#                     bc = rec.get('barcode_count', 0)
#                     if uf and uf not in seen_ubno:
#                         seen_ubno.add(uf)
#                         _add_row(uf, "UFM", int(bc),
#                                  _prop_yield(bc, total_bc, wh_val), _prop_yield(bc, total_bc, uni_val),
#                                  _prop_yield(bc, total_bc, vis_val), spec_wh, spec_uni, spec_vis, oe_ok_val,
#                                  _build_yield_vals(bc, total_bc, _pre_yield_fields))
#             for c in machine_obj.get('cur_ids', []):
#                 bc = c.get('barcode', 0)
#                 _add_row(sanitize_text(str(c.get('cur_id', 'N/A'))), "Curing", int(bc),
#                          _prop_yield(bc, total_bc, wh_val), _prop_yield(bc, total_bc, uni_val),
#                          _prop_yield(bc, total_bc, vis_val), spec_wh, spec_uni, spec_vis, oe_ok_val,
#                          _build_yield_vals(bc, total_bc, _pre_yield_fields))

#         elif _at == "CURING":
#             _add_row(sanitize_text(machine_obj.get('machine', 'N/A')), "Curing",
#                      int(total_bc) if total_bc else 0,
#                      wh_val, uni_val, vis_val, spec_wh, spec_uni, spec_vis, oe_ok_val,
#                      _build_yield_vals_full(_pre_yield_fields))
#             seen_ubno = set()
#             for ma in alert_obj.get("machine_alerts", []):
#                 for rec in ma.get("grouped_averages", {}).get("uf_id", []):
#                     uf = sanitize_text(rec.get('uf_id', ''))
#                     bc = rec.get('barcode_count', 0)
#                     if uf and uf not in seen_ubno:
#                         seen_ubno.add(uf)
#                         _add_row(uf, "UFM", int(bc),
#                                  _prop_yield(bc, total_bc, wh_val), _prop_yield(bc, total_bc, uni_val),
#                                  _prop_yield(bc, total_bc, vis_val), spec_wh, spec_uni, spec_vis, oe_ok_val,
#                                  _build_yield_vals(bc, total_bc, _pre_yield_fields))
#             for p in machine_obj.get('ptb_ids', []):
#                 bc = p.get('barcode', 0)
#                 _add_row(sanitize_text(str(p.get('ptb_id', 'N/A'))), "TBM", int(bc),
#                          _prop_yield(bc, total_bc, wh_val), _prop_yield(bc, total_bc, uni_val),
#                          _prop_yield(bc, total_bc, vis_val), spec_wh, spec_uni, spec_vis, oe_ok_val,
#                          _build_yield_vals(bc, total_bc, _pre_yield_fields))

#         if not entry_rows:
#             continue

#         # Build HTML table for this entry
#         primary_machine = sanitize_text(machine_obj.get('machine', 'N/A'))

#         # Mapping: special param → its respective yield field  (cony/conicity_yield removed)
#         SPECIAL_PARAM_YIELD_MAP = {
#             "rfppcc":  "rfv_yield",
#             "rfppcw":  "rfv_yield",
#             "rfh1cc":  "rfh1_yield",
#             "rfh1cw":  "rfh1_yield",
#             "rfh2cc":  "rfh2_yield",
#             "rfh2cw":  "rfh2_yield",
#             "plys":    "plysteer_yield",
#         }
#         SPECIAL_PARAMS = list(SPECIAL_PARAM_YIELD_MAP.keys())

#         deviated_param_names = []
#         for param in machine_obj.get('parameter_alerts', []):
#             pname = param.get('parameter', '')
#             shift_threshold = param.get('shift_occurred_thershold', False)
#             shift_spec      = param.get('shift_occurred_spec', False)
#             if (shift_threshold or shift_spec) and pname:
#                 deviated_param_names.append(pname)

#         # Only keep special params that are actually deviated
#         active_special_params = [p for p in SPECIAL_PARAMS if p in deviated_param_names]

#         # Collect which yield columns to show (deduplicated, preserving order)
#         seen_yields = set()
#         active_yield_cols = []
#         for sp in active_special_params:
#             y = SPECIAL_PARAM_YIELD_MAP[sp]
#             if y not in seen_yields:
#                 seen_yields.add(y)
#                 active_yield_cols.append(y)

#         # Yield-only deviated params (yield params that deviated but have no special param companion)
#         yield_only_deviated = [
#             p for p in deviated_param_names
#             if p.endswith('_yield') and p not in active_yield_cols
#             and p not in SPECIAL_PARAM_YIELD_MAP.values()
#         ]
#         # Also include the mapped yields even if they themselves are deviated
#         for y in list(active_yield_cols):
#             # already included via special param — no action needed
#             pass

#         html_table = f"<p><strong>SKU: {sku} | Machine: {primary_machine}</strong></p>"
#         html_table += "<table border='1' cellpadding='5' cellspacing='0' style='border-collapse: collapse; width: 100%;'>"
#         html_table += "<thead><tr style='background-color: #4A90E2; color: white;'>"
#         html_table += (
#             "<th>Machine</th><th>Machine Type</th><th>Total Tyres</th><th>OE Tyres</th>"
#             "<th>WAREHOUSE Yield (%)</th>"
#             "<th>UNIFORMITY Yield (%)</th>"
#             "<th>VISUAL Yield (%)</th>"
#         )
#         # For each deviated special param: add param column + its yield column (yield shown once per unique yield)
#         shown_yield_headers = set()
#         for sp in active_special_params:
#             html_table += f"<th>{sp}</th>"
#             y = SPECIAL_PARAM_YIELD_MAP[sp]
#             if y not in shown_yield_headers:
#                 html_table += f"<th>{y.replace('_', ' ').title()}</th>"
#                 shown_yield_headers.add(y)
#         html_table += "</tr></thead><tbody>"

#         def _calc_nd(yield_pct, bc):
#             """Return (numerator, denominator) string e.g. '92/100'."""
#             try:
#                 if yield_pct in (None, 'N/A') or not bc:
#                     return "N/A"
#                 denom = int(bc)
#                 numer = round(float(yield_pct) / 100.0 * denom)
#                 return f"{numer}/{denom}"
#             except (TypeError, ValueError):
#                 return "N/A"

#         def _fmt_param_val(param_name, m_obj):
#             """Fetch the current_value of a specific param from parameter_alerts."""
#             for param in m_obj.get('parameter_alerts', []):
#                 if param.get('parameter') == param_name:
#                     val = param.get('current_value')
#                     try:
#                         return f"{float(val):.2f}" if val not in (None, 'N/A') else "N/A"
#                     except (TypeError, ValueError):
#                         return str(val) if val is not None else "N/A"
#             return "N/A"

#         for (mname, mtype, bc, oe_t, wh_str, uni_str, vis_str, wh_c, uni_c, vis_c, row_yield_vals) in entry_rows:
#             # Combine yield % with drop indicator in the same cell
#             wh_cell  = f"{wh_str} {_ind(wh_c)}"
#             uni_cell = f"{uni_str} {_ind(uni_c)}"
#             vis_cell = f"{vis_str} {_ind(vis_c)}"

#             html_table += "<tr>"
#             html_table += f"<td>{mname}</td><td>{mtype}</td><td>{bc}</td><td>{oe_t}</td>"
#             html_table += f"<td>{wh_cell}</td>"
#             html_table += f"<td>{uni_cell}</td>"
#             html_table += f"<td>{vis_cell}</td>"
#             # Add deviated special param + its respective proportional yield — yield shown once per unique yield field
#             emitted_yields = set()
#             for sp in active_special_params:
#                 # Parameter value — always the machine-level value (not per sub-row)
#                 html_table += f"<td>{_fmt_param_val(sp, machine_obj)}</td>"
#                 # Respective yield: proportional per sub-row, with 🔴/✅ indicator
#                 y = SPECIAL_PARAM_YIELD_MAP[sp]
#                 if y not in emitted_yields:
#                     yval = row_yield_vals.get(y)
#                     spec_y = YIELD_SPEC_KEYS.get(y)
#                     y_ind = _ind(_color(yval, spec_y))
#                     html_table += f"<td>{_fmt(yval)} {y_ind}</td>"
#                     emitted_yields.add(y)
#             html_table += "</tr>"

#         html_table += "</tbody></table>"
#         breakdown_blocks.append(html_table)

#     machine_breakdown_html = "<br><br>".join(breakdown_blocks) if breakdown_blocks else "<p>No data available.</p>"                                                                  
#     payload = {
#         "@type": "MessageCard",
#         "@context": "http://schema.org/extensions",
#         "summary": title_str,
#         "themeColor": "0076D7",
#         "title": title_str,
#         "text": (
#             f"**{title_str}**\n\n"
#             f"{markdown_table}\n\n"
#             f"---\n\n"
#             f"**Machine Breakdown**\n\n"
#             f"{machine_breakdown_html}"
#         )
#     }
#     payload_list = [payload]

#     # Send the single payload
#     for payload in payload_list:
#         try:
#             response = requests.post(teams_webhook_url, json=payload)
#             if response.status_code in (200, 202):
#                 print(f"[INFO] Teams alert sent successfully for {alert_type} alerts")
#                 return True
#             else:
#                 print(f"[ERROR] Failed to send Teams alert: {response.status_code} {response.text}")                                            
#                 return False
#         except Exception as e:
#             print(f"[ERROR] Exception sending Teams alert: {e}")
#             return False        

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
#         'sku': doc['sku'],
#         'target_machine_lot_size': doc.get('target_machine_lot_size'),
#         atype['alert_time_key']: doc.get(atype['alert_time_key'])
#     }

#     raw  = alert['parameter_alerts']
#     data = {p['parameter']: p for p in raw}
#     for p in data.values():
#         c, s = p.get('current_value'), p.get('spec_value')
#         p['spec_diff'] = float(c)-float(s) if c is not None and s is not None else None

#     # filter keys present
#     all_ufm = PARAM_KEYS[:10]
#     all_geometery = PARAM_KEYS[10:20]
#     all_bal = PARAM_KEYS[20:23]
#     all_load = PARAM_KEYS[23:27]
#     all_yields = PARAM_KEYS[27:]
#     ufm_keys = [k for k in all_ufm if k in data]
#     geometry_keys = [k for k in all_geometery if k in data]
#     bal_keys = [k for k in all_bal if k in data]
#     load_keys = [k for k in all_load if k in data]
#     yields_keys = [k for k in all_yields if k in data]

#     pdf = PDF()
#     pdf.add_page()

#      # 1) Compute our twoâ€column split
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
#     modelno = sanitize_text(str(alert.get('sku', 'UNKNOWN')))

#     # Build identifier
#     identifier = f":{atype_name}_{machine}_{modelno}_{ts_str if ts_str else 'None'}"

										
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

																					   
#     ufm_keys = [k for k in PARAM_KEYS[:10] if k in data]
#     geometry_keys = [k for k in PARAM_KEYS[10:20] if k in data]
#     bal_keys = [k for k in PARAM_KEYS[20:23] if k in data]
#     load_keys = [k for k in PARAM_KEYS[23:27] if k in data]
#     yields_keys = [k for k in PARAM_KEYS[27:] if k in data]
#     group_defs = [("UFM", ufm_keys), ("GEOMETRY", geometry_keys), ("BALANCING", bal_keys), ("LOAD", load_keys), ("YIELDS", yields_keys)]
                  
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
#         group_defs=[("UFM", ufm_keys), ("GEOMETRY", geometry_keys), ("BALANCING", bal_keys), ("LOAD", load_keys), ("YIELDS", yields_keys)],
                                                                                   
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
#     # return pdf.output(dest='S').encode('latin-1')

# def get_admin_emails(client):
#     cols = client[alert_mail_db][alert_users]
#     users = cols.find(
#         {"is_alert_user": True},
#         {"_id": 0, "email": 1}
#     )
#     emails = [u["email"] for u in users if u.get("email")]
#     #return [r['email'] for r in cols.find({}, {'_id':0,'email':1})]
#     return emails

# def send_alerts():
#     """
#     Sends alerts by grouping PDFs by modelno across all work centers (TBM, CURING, UFM).
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
#         docs = list(coll.find({'alert_flag': False}))
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
#                                     {'$set': {'alert_flag': True, doc['alert_type']['alert_time_key']: datetime.now()}}
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
#                             {'$set': {'alert_flag': True, doc['alert_type']['alert_time_key']: datetime.now()}}
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
