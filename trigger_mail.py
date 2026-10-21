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
# from config import (
#     alert_triggering_ubno_collect_name,
#     alert_triggering_collect_name,
#     alert_triggering_curing_collect_name,
#     alert_db_virgin_yield,
#     alert_mail_db,
#     alert_users,
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
# BASE_PDF_DIR    = "generated_pdfs"
																			  
# MAX_EMAIL_SIZE = 20 * 1024 * 1024  # 20 MB																			   

# # Full list of possible keys
# PARAM_KEYS = [
#     'rfpp_cw_kg','rfpp_ccw_kg','rfh1_cw_kg','rfh1_ccw_kg','rfh2_cw_kg','rfh2_ccw_kg','lfpp_cw_kg','lfpp_ccw_kg',
#     'lsft_cw_kg','lsft_ccw_kg','lfh1_cw_kg','lfh1_ccw_kg','plysteer_kg','cony_kg','rfmxa_cw_deg','rfmxa_ccw_deg',
#     'rfa1_cw_deg','rfa1_ccw_deg','rfa2_cw_deg','rfa2_ccw_deg','lfmxa_cw_deg','lfmxa_ccw_deg','lfa1_cw_deg',
#     'lfa1_ccw_deg','botplane_g','topplane_g','couple_g','static_g','botplane_deg','topplane_deg','couple_deg',
#     'static_deg','botbul_mm','botdep_mm','topbul_mm','topdep_mm','blro_mm','blroh1_mm','crro_mm','crroh1_mm',
#     'tlro_mm','tlroh1_mm','botbul_deg','botdep_deg','topbul_deg','topdep_deg','blro_deg','blroh1_deg','crro_deg',
#     'crroh1_deg','tlro_deg','tlroh1_deg','oe_yield', 'balancing_oe_yield', 'geometry_oe_yield'
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

# # Utility functions
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
#     headers = [sanitize_text(th.get_text().strip()) for th in soup.find_all('th')]
#     markdown_table += "| " + " | ".join(headers) + " |\n"
#     markdown_table += "| " + "| ".join(["---"] * len(headers)) + " |\n"
    
#     # Process rows
#     for row in soup.find_all('tr')[1:]:  # Skip header row
#         cells = [sanitize_text(td.get_text().strip()) for td in row.find_all('td')]
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
#             <th>Gt Code</th>
#             <th>Size Code</th>
#             <th>TBM</th>
#             <th>Total Tyre Produced</th>
#             <th>Overall OE Yield (%)</th>
#             <th>Overall BALANCING Yield (%)</th>
#             <th>Overall GEOMETRY Yield (%)</th>
#         </tr>
#         """
#     elif alert_type.upper() == "CURING":
#         html += """
#         <tr>
#             <th>S.No</th>
#             <th>Gt Code</th>
#             <th>Size Code</th>
#             <th>CURING</th>
#             <th>Total Tyre Produced</th>
#             <th>Overall OE Yield (%)</th>
#             <th>Overall BALANCING Yield (%)</th>
#             <th>Overall GEOMETRY Yield (%)</th>
#         </tr>
#         """
#     elif alert_type.upper() == "UFM":
#         html += """
#         <tr>
#             <th>S.No</th>
#             <th>Gt Code</th>
#             <th>Size Code</th>
#             <th>UBNO</th>
#             <th>Total Tyre Produced</th>
#             <th>Overall OE Yield (%)</th>
#             <th>Overall BALANCING Yield (%)</th>
#             <th>Overall GEOMETRY Yield (%)</th>
#         </tr>
#         """

#     # Handle different alert types
#     if alert_type.upper() == "TBM":
#         # For TBM alerts
#         # for alert in alerts:
#         for i, alert in enumerate(alerts):
#             for machine in alert.get("machine_alerts", []):
#                 try:
#                     total_rank = f"{float(machine.get('oe_yield', 0)):.2f}" if machine.get("oe_yield") not in [None, "N/A"] else "N/A"
#                     balancing = f"{float(machine.get('balancing_oe_yield', 0)):.2f}" if machine.get("balancing_oe_yield") not in [None, "N/A"] else "N/A"
#                     geometry = f"{float(machine.get('geometry_oe_yield', 0)):.2f}" if machine.get("geometry_oe_yield") not in [None, "N/A"] else "N/A"
#                     # ufm = f"{float(machine.get('ufm_yield', 0)):.2f}" if machine.get("ufm_yield") not in [None, "N/A"] else "N/A"
#                 except (ValueError, TypeError):
#                     total_rank = balancing = geometry = "N/A"

#                 html += f"""
#                 <tr>
#                     <td>{i+1}</td>
#                     <td>{sanitize_text(alert.get('gt_code', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('sizecode', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('machine', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('processed_machine_barcodes', 'N/A'))}</td>
#                     <td>{total_rank}</td>
#                     <td>{balancing}</td>
#                     <td>{geometry}</td>
#                 </tr>
#                 """
    
#     elif alert_type.upper() == "CURING":
#         # For CURING alerts
#         # for alert in alerts:
#         for i, alert in enumerate(alerts):
#             for machine in alert.get("machine_alerts", []):
#                 try:
#                     total_rank = f"{float(machine.get('oe_yield', 0)):.2f}" if machine.get("oe_yield") not in [None, "N/A"] else "N/A"
#                     balancing = f"{float(machine.get('balancing_oe_yield', 0)):.2f}" if machine.get("balancing_oe_yield") not in [None, "N/A"] else "N/A"
#                     geometry = f"{float(machine.get('geometry_oe_yield', 0)):.2f}" if machine.get("geometry_oe_yield") not in [None, "N/A"] else "N/A"
#                     # ufm = f"{float(machine.get('ufm_yield', 0)):.2f}" if machine.get("ufm_yield") not in [None, "N/A"] else "N/A"
#                 except (ValueError, TypeError):
#                     total_rank = balancing = geometry = "N/A"

#                 html += f"""
#                 <tr>
#                     <td>{i+1}</td>
#                     <td>{sanitize_text(alert.get('gt_code', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('sizecode', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('machine', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('processed_machine_barcodes', 'N/A'))}</td>
#                     <td>{total_rank}</td>
#                     <td>{balancing}</td>
#                     <td>{geometry}</td>
#                 </tr>
#                 """
    
#     elif alert_type.upper() == "UFM":
#     # For CURING alerts
#         # for alert in alerts:
#         for i, alert in enumerate(alerts):
#             for machine in alert.get("machine_alerts", []):
#                 try:
#                     total_rank = f"{float(machine.get('oe_yield', 0)):.2f}" if machine.get("oe_yield") not in [None, "N/A"] else "N/A"
#                     balancing = f"{float(machine.get('balancing_oe_yield', 0)):.2f}" if machine.get("balancing_oe_yield") not in [None, "N/A"] else "N/A"
#                     geometry = f"{float(machine.get('geometry_oe_yield', 0)):.2f}" if machine.get("geometry_oe_yield") not in [None, "N/A"] else "N/A"
#                     # ufm = f"{float(machine.get('ufm_yield', 0)):.2f}" if machine.get("ufm_yield") not in [None, "N/A"] else "N/A"
#                 except (ValueError, TypeError):
#                     total_rank = balancing = geometry = "N/A"

#                 html += f"""
#                 <tr>
#                     <td>{i+1}</td>
#                     <td>{sanitize_text(alert.get('gt_code', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('sizecode', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('machine', 'N/A'))}</td>
#                     <td>{sanitize_text(machine.get('processed_machine_barcodes', 'N/A'))}</td>
#                     <td>{total_rank}</td>
#                     <td>{balancing}</td>
#                     <td>{geometry}</td>
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

#     # Initialize dictionary to group deviated parameters across all alerts
#     deviated_by_gt_code = {}

#     # Aggregate deviated parameters across all alerts and machines
#     for alert in docs:
#         for machine in alert.get("machine_alerts", []):
#             raw_sizecode = machine.get('sizecode', 'N/A')
#             sizecode = raw_sizecode if raw_sizecode != 'N/A' else sanitize_text(raw_sizecode)
#             raw_gt_code = machine.get('gt_code', 'N/A')
#             gt_code = raw_gt_code if raw_gt_code != 'N/A' else sanitize_text(raw_gt_code)
#             param_alerts = machine.get('parameter_alerts', [])
#             key = (sanitize_text(machine.get('gt_code', 'N/A')), sanitize_text(machine.get('machine', 'N/A')), sizecode)
#             if key not in deviated_by_gt_code:
#                 deviated_by_gt_code[key] = []
                
#             for param in param_alerts:
#                 param_name = param.get('parameter')
#                 current_val = param.get('current_value')
#                 shift_threshold = param.get('shift_occurred_thershold', False)
#                 shift_spec = param.get('shift_occurred_spec', False)
#                 rank_counts = param.get('rank_counts', {})
                
#                 if shift_threshold or shift_spec:
#                     # Replace '_yield' with ' yield' for parameters containing '_yield'
#                     if param_name.endswith('_yield'):
#                         display_name = param_name.replace('_yield', ' yield')
#                     else:
#                         display_name = param_name
                        
#                     if current_val == 0 or (isinstance(current_val, (int, float)) and current_val == 0):
#                         # Format rank_counts as column_name->rank:count
#                         rank_str = ", ".join([
#                             f"{k.rsplit('_', 1)[0]}->{k.rsplit('_', 1)[1]}:{v}(count)" 
#                             if '_' in k else f"{k}->{v}" 
#                             for k, v in rank_counts.items()
#                         ]) if rank_counts else "No ranks"
#                         deviated_param = f"{display_name}({rank_str})"
#                     else:
#                         deviated_param = display_name

#                     deviated_by_gt_code[key].append(deviated_param)

#     # Generate note with grouped deviated parameters and add line between notes
#     note_lines = []
#     for i, ((gt_code, machine, sizecode), params) in enumerate(deviated_by_gt_code.items()):
#         note_lines.append(
#             f"**Note{i+1}:** Gt Code: {gt_code} | Deviated parameters: {', '.join(params)}"
#         )

#     # Join notes with two line breaks between them
#     note = "\n\n".join(note_lines) if note_lines else ""

#     # Create a single payload with generic title
#     if alert_type.upper() == "TBM":
#         title_str = f"Alert: TBM Alert Summary"
#     elif alert_type.upper() == "CURING":
#         title_str = f"Alert: CURING Alert Summary"
#     elif alert_type.upper() == "UFM":
#         title_str = f"Alert: UFM Alert Summary"
#     else:
#         title_str = "Alert Summary"

#     markdown_table = html_table_to_markdown(summary_html)
    
#     payload = {
#         "@type": "MessageCard",
#         "@context": "http://schema.org/extensions",
#         "summary": title_str,
#         "themeColor": "0076D7",
#         "title": title_str,
#         "text": f"**{title_str}**\n\n{markdown_table}\n\n{note}"
#     }
#     payload_list = [payload]

#     # Send the single payload
#     for payload in payload_list:
#         try:
#             response = requests.post(teams_webhook_url, json=payload)
#             if response.status_code == 200:
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
#                     hl = ((col_name == 'difference' and data[key].get('shift_occurred_thershold')) or \
#                         (col_name == 'spec_diff' and data[key].get('shift_occurred_spec')))
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
#             self.cell(widths[1] + widths[2], h, sanitize_text(m['ubno']), 1, 0, 'C')
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
#                 self.cell(barcode_width, h, f"{sanitize_text(m['ubno'])} Barcodes", 1, 0, 'C', fill=True)
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
#             self.cell(usable, h, f"{sanitize_text(m['ubno'])} Barcodes", 1, 0, 'C', fill=True)
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
#         # 'modelno': doc['modelno'],
#         'target_machine_lot_size': doc.get('target_machine_lot_size'),
#         atype['alert_time_key']: doc.get(atype['alert_time_key'])
#     }

#     raw  = alert['parameter_alerts']
#     data = {p['parameter']: p for p in raw}
#     for p in data.values():
#         c, s = p.get('current_value'), p.get('spec_value')
#         p['spec_diff'] = float(c)-float(s) if c is not None and s is not None else None

#     # filter keys present
#     all_ufm = PARAM_KEYS[:24]
#     all_bal = PARAM_KEYS[24:32]
#     all_geometery = PARAM_KEYS[32:52]
#     all_yields = PARAM_KEYS[52:]
#     ufm_keys = [k for k in all_ufm if k in data]
#     bal_keys = [k for k in all_bal if k in data]
#     geometry_keys = [k for k in all_geometery if k in data]
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
#     gt_code = sanitize_text(str(alert.get('gt_code', 'UNKNOWN')))
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
#     pdf.set_font('Arial','B',11); pdf.cell(label_w,h,"Gt Code",0,0)
#     pdf.set_font('Arial','',11);  pdf.cell(val_w,h,f": {sanitize_text(alert['gt_code'])}",0,0)
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

#     # y = pdf.get_y()
#     # pdf.set_xy(x1,y)
#     # pdf.set_font('Arial','B',11); pdf.cell(label_w,h,"Gt Code",0,0)
#     # pdf.set_font('Arial','',11);  pdf.cell(val_w,h,f": {sanitize_text(alert['gt_code'])}",0,1)

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

																					   
#     ufm_keys = PARAM_KEYS[:24]
#     bal_keys = PARAM_KEYS[24:32]
#     geometry_keys = PARAM_KEYS[32:52]
#     yields_keys = PARAM_KEYS[52:]
#     group_defs = [("UFM", ufm_keys), ("BALANCING", bal_keys), ("GEOMETRY", geometry_keys), ("YIELDS", yields_keys)]
																									
                  
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
#         group_defs=[("UFM", ufm_keys), ("BALANCING", bal_keys), ("GEOMETRY", geometry_keys), ("YIELDS", yields_keys)],
                                                                                   
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
#         raw_group = entry['grouped_averages']['ubno']
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
#                 'ubno': rec['ubno'],
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
#     return [r['email'] for r in cols.find({}, {'_id':0,'email':1})]

# def send_alerts():
#     """
#     Sends alerts by grouping PDFs by gt_code across all work centers (TBM, CURING, UFM).
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

#     # Group alerts by gt_code
#     alerts_by_gt_code = {}
#     for doc in all_alerts:
#         # modelno = sanitize_text(doc.get('modelno', 'N/A'))
#         gt_code = sanitize_text(doc.get('gt_code', 'N/A'))
#         if gt_code not in alerts_by_gt_code:
#             alerts_by_gt_code[gt_code] = []
#         alerts_by_gt_code[gt_code].append(doc)
                    

#     # Process each gt_code group
#     for gt_code, docs in alerts_by_gt_code.items():
#         # Collect size codes for this gt_code
#         size_codes = sorted(set(sanitize_text(m.get('sizecode', 'N/A')) for d in docs for m in d.get('machine_alerts', [])))
#         sizecode_str = ", ".join(size_codes) if size_codes else "N/A"
        
#         # Generate PDFs for all alerts in this gt_code
#         pdf_list = []
#         for doc in docs:
#             at = doc['alert_type']
#             pdf_bytes = generate_alert_pdf(doc, at)
#             fn = f"{at['filename_prefix']}_{sanitize_text(doc['machine_alerts'][0]['machine'])}_{gt_code}_{ts}.pdf"
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
#                     "subject": f"PCR AP - Yield Alert: GT CODE - {gt_code} | Size Code - {sizecode_str} (Batch {batch_number})",
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
#                             "subject": f"PCR AP - Yield Alert: GT CODE - {gt_code} | Size Code - {sizecode_str} (Batch {batch_number})",
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
#                             print(f"[INFO] Batch {batch_number} emailed via Microsoft Graph successfully for GT CODE {gt_code}.")
#                             # Mark alerts sent in MongoDB for this batch
#                             for pdf_item in current_batch:
#                                 db[pdf_item['collection']].update_one(
#                                     {'_id': pdf_item['doc_id']},
#                                     {'$set': {'alert_flag': True, doc['alert_type']['alert_time_key']: datetime.now()}}
#                                 )
#                             batched_doc_ids.extend([pdf_item['doc_id'] for pdf_item in current_batch])
#                             total_pdfs_processed += len(current_batch)
#                         else:
#                             print(f"[ERROR] Graph API sendMail failed for batch {batch_number} (GT CODE {gt_code}): {resp.status_code} {resp.text}")
#                     except Exception as e:
#                         print(f"[ERROR] Exception while sending batch {batch_number} (GT CODE {gt_code}): {e}")
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
#                     "subject": f"PCR AP - Yield Alert: GT CODE - {gt_code} | Size Code - {sizecode_str} (Batch {batch_number})",
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
#                     print(f"[INFO] Final batch {batch_number} emailed via Microsoft Graph successfully for GT CODE {gt_code}.")
#                     for pdf_item in current_batch:
#                         db[pdf_item['collection']].update_one(
#                             {'_id': pdf_item['doc_id']},
#                             {'$set': {'alert_flag': True, doc['alert_type']['alert_time_key']: datetime.now()}}
#                         )
#                     batched_doc_ids.extend([pdf_item['doc_id'] for pdf_item in current_batch])
#                     total_pdfs_processed += len(current_batch)
#                 else:
#                     print(f"[ERROR] Graph API sendMail failed for final batch {batch_number} (GT CODE {gt_code}): {resp.status_code} {resp.text}")
#             except Exception as e:
#                 print(f"[ERROR] Exception while sending final batch {batch_number} (GT CODE {gt_code}): {e}")
#                 traceback.print_exc()
#         print(f"[INFO] Processed {total_pdfs_processed} PDFs for GT CODE {gt_code} in {batch_number} batches.")
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
from config import (
    alert_triggering_ubno_collect_name,
    alert_triggering_collect_name,
    alert_triggering_curing_collect_name,
    alert_db_virgin_yield,
    alert_mail_db,
    alert_users,
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
BASE_PDF_DIR    = "generated_pdfs"
																			  
MAX_EMAIL_SIZE = 20 * 1024 * 1024  # 20 MB																			   

# Full list of possible keys
PARAM_KEYS = [
    'rfpp_cw_kg','rfpp_ccw_kg','rfh1_cw_kg','rfh1_ccw_kg','rfh2_cw_kg','rfh2_ccw_kg','lfpp_cw_kg','lfpp_ccw_kg',
    'lsft_cw_kg','lsft_ccw_kg','lfh1_cw_kg','lfh1_ccw_kg','plysteer_kg','cony_kg','rfmxa_ccw_deg',
    'botplane_g','topplane_g','couple_g','static_g','botplane_deg','topplane_deg','couple_deg',
    'static_deg','rfa1_cw_deg','rfa1_ccw_deg','rfa2_cw_deg','rfa2_ccw_deg','lfmxa_cw_deg','lfmxa_ccw_deg','lfa1_cw_deg',
    'lfa1_ccw_deg','botbul_mm','botdep_mm','topbul_mm','topdep_mm','blro_mm','blroh1_mm','crro_mm','crroh1_mm',
    'tlro_mm','tlroh1_mm','botbul_deg','botdep_deg','topbul_deg','topdep_deg','blro_deg','blroh1_deg','crro_deg',
    'crroh1_deg','tlro_deg','tlroh1_deg',"rfv_yield", "rfh1_yield", "rfh2_yield", "lfv_yield", "lsft_yield", "lfh1_yield", "conicity_yield", "oe_yield",
    "rfmxa_yield", "rfa1_yield", "rfa2_yield", "lfxma_yield", "lfa1_yield", "central_radial_runout_yield", "central_radial_runout_first_harmonic_yield",
    "lateral_run_out_top_over_yield", "lateral_run_out_bottom_over_yield", "lateral_run_out_yield", "lateral_run_out_bottom_first_harmonic_yield", "lateral_run_out_top_first_harmonic_yield",
    "lateral_run_out_harmonic_yield", "bulge_top_over_yield", "bulge_bottom_over_yield", "bulge_yield", "dent_top_over_yield", "dent_bottom_over_yield", "dent_yield", "geometry_oe_yield",
    "lower_yield", "upper_yield", "static_yield", "couple_yield", "balancing_oe_yield", "warehouse_yield", "visual_yield"
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

# Utility functions
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
    headers = [sanitize_text(th.get_text().strip()) for th in soup.find_all('th')]
    markdown_table += "| " + " | ".join(headers) + " |\n"
    markdown_table += "| " + "| ".join(["---"] * len(headers)) + " |\n"
    
    # Process rows
    for row in soup.find_all('tr')[1:]:  # Skip header row
        cells = [sanitize_text(td.get_text().strip()) for td in row.find_all('td')]
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
            <th>Gt Code</th>
            <th>Size Code</th>
            <th>TBM</th>
            <th>Total Tyre Produced</th>
            <th>Overall OE Yield (%)</th>
            <th>Overall BALANCING Yield (%)</th>
            <th>Overall GEOMETRY Yield (%)</th>
        </tr>
        """
    elif alert_type.upper() == "CURING":
        html += """
        <tr>
            <th>S.No</th>
            <th>Gt Code</th>
            <th>Size Code</th>
            <th>CURING</th>
            <th>Total Tyre Produced</th>
            <th>Overall OE Yield (%)</th>
            <th>Overall BALANCING Yield (%)</th>
            <th>Overall GEOMETRY Yield (%)</th>
        </tr>
        """
    elif alert_type.upper() == "UFM":
        html += """
        <tr>
            <th>S.No</th>
            <th>Gt Code</th>
            <th>Size Code</th>
            <th>UBNO</th>
            <th>Total Tyre Produced</th>
            <th>Overall OE Yield (%)</th>
            <th>Overall BALANCING Yield (%)</th>
            <th>Overall GEOMETRY Yield (%)</th>
        </tr>
        """

    # Handle different alert types
    if alert_type.upper() == "TBM":
        # For TBM alerts
        # for alert in alerts:
        for i, alert in enumerate(alerts):
            for machine in alert.get("machine_alerts", []):
                try:
                    total_rank = f"{float(machine.get('oe_yield', 0)):.2f}" if machine.get("oe_yield") not in [None, "N/A"] else "N/A"
                    balancing = f"{float(machine.get('balancing_oe_yield', 0)):.2f}" if machine.get("balancing_oe_yield") not in [None, "N/A"] else "N/A"
                    geometry = f"{float(machine.get('geometry_oe_yield', 0)):.2f}" if machine.get("geometry_oe_yield") not in [None, "N/A"] else "N/A"
                    # ufm = f"{float(machine.get('ufm_yield', 0)):.2f}" if machine.get("ufm_yield") not in [None, "N/A"] else "N/A"
                except (ValueError, TypeError):
                    total_rank = balancing = geometry = "N/A"

                html += f"""
                <tr>
                    <td>{i+1}</td>
                    <td>{sanitize_text(alert.get('gt_code', 'N/A'))}</td>
                    <td>{sanitize_text(machine.get('sizecode', 'N/A'))}</td>
                    <td>{sanitize_text(machine.get('machine', 'N/A'))}</td>
                    <td>{sanitize_text(machine.get('processed_machine_barcodes', 'N/A'))}</td>
                    <td>{total_rank}</td>
                    <td>{balancing}</td>
                    <td>{geometry}</td>
                </tr>
                """
    
    elif alert_type.upper() == "CURING":
        # For CURING alerts
        # for alert in alerts:
        for i, alert in enumerate(alerts):
            for machine in alert.get("machine_alerts", []):
                try:
                    total_rank = f"{float(machine.get('oe_yield', 0)):.2f}" if machine.get("oe_yield") not in [None, "N/A"] else "N/A"
                    balancing = f"{float(machine.get('balancing_oe_yield', 0)):.2f}" if machine.get("balancing_oe_yield") not in [None, "N/A"] else "N/A"
                    geometry = f"{float(machine.get('geometry_oe_yield', 0)):.2f}" if machine.get("geometry_oe_yield") not in [None, "N/A"] else "N/A"
                    # ufm = f"{float(machine.get('ufm_yield', 0)):.2f}" if machine.get("ufm_yield") not in [None, "N/A"] else "N/A"
                except (ValueError, TypeError):
                    total_rank = balancing = geometry = "N/A"

                html += f"""
                <tr>
                    <td>{i+1}</td>
                    <td>{sanitize_text(alert.get('gt_code', 'N/A'))}</td>
                    <td>{sanitize_text(machine.get('sizecode', 'N/A'))}</td>
                    <td>{sanitize_text(machine.get('machine', 'N/A'))}</td>
                    <td>{sanitize_text(machine.get('processed_machine_barcodes', 'N/A'))}</td>
                    <td>{total_rank}</td>
                    <td>{balancing}</td>
                    <td>{geometry}</td>
                </tr>
                """
    
    elif alert_type.upper() == "UFM":
    # For CURING alerts
        # for alert in alerts:
        for i, alert in enumerate(alerts):
            for machine in alert.get("machine_alerts", []):
                try:
                    total_rank = f"{float(machine.get('oe_yield', 0)):.2f}" if machine.get("oe_yield") not in [None, "N/A"] else "N/A"
                    balancing = f"{float(machine.get('balancing_oe_yield', 0)):.2f}" if machine.get("balancing_oe_yield") not in [None, "N/A"] else "N/A"
                    geometry = f"{float(machine.get('geometry_oe_yield', 0)):.2f}" if machine.get("geometry_oe_yield") not in [None, "N/A"] else "N/A"
                    # ufm = f"{float(machine.get('ufm_yield', 0)):.2f}" if machine.get("ufm_yield") not in [None, "N/A"] else "N/A"
                except (ValueError, TypeError):
                    total_rank = balancing = geometry = "N/A"

                html += f"""
                <tr>
                    <td>{i+1}</td>
                    <td>{sanitize_text(alert.get('gt_code', 'N/A'))}</td>
                    <td>{sanitize_text(machine.get('sizecode', 'N/A'))}</td>
                    <td>{sanitize_text(machine.get('machine', 'N/A'))}</td>
                    <td>{sanitize_text(machine.get('processed_machine_barcodes', 'N/A'))}</td>
                    <td>{total_rank}</td>
                    <td>{balancing}</td>
                    <td>{geometry}</td>
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

    # Initialize dictionary to group deviated parameters across all alerts
    deviated_by_gt_code = {}

    # Aggregate deviated parameters across all alerts and machines
    for alert in docs:
        for machine in alert.get("machine_alerts", []):
            raw_sizecode = machine.get('sizecode', 'N/A')
            sizecode = raw_sizecode if raw_sizecode != 'N/A' else sanitize_text(raw_sizecode)
            raw_gt_code = machine.get('gt_code', 'N/A')
            gt_code = raw_gt_code if raw_gt_code != 'N/A' else sanitize_text(raw_gt_code)
            param_alerts = machine.get('parameter_alerts', [])
            key = (sanitize_text(machine.get('gt_code', 'N/A')), sanitize_text(machine.get('machine', 'N/A')), sizecode)
            if key not in deviated_by_gt_code:
                deviated_by_gt_code[key] = []
                
            for param in param_alerts:
                param_name = param.get('parameter')
                current_val = param.get('current_value')
                shift_threshold = param.get('shift_occurred_thershold', False)
                shift_spec = param.get('shift_occurred_spec', False)
                rank_counts = param.get('rank_counts', {})
                
                if shift_threshold or shift_spec:
                    # Replace '_yield' with ' yield' for parameters containing '_yield'
                    if param_name.endswith('_yield'):
                        display_name = param_name.replace('_yield', ' yield')
                    else:
                        display_name = param_name
                        
                    if current_val == 0 or (isinstance(current_val, (int, float)) and current_val == 0):
                        # Format rank_counts as column_name->rank:count
                        rank_str = ", ".join([
                            f"{k.rsplit('_', 1)[0]}->{k.rsplit('_', 1)[1]}:{v}(count)" 
                            if '_' in k else f"{k}->{v}" 
                            for k, v in rank_counts.items()
                        ]) if rank_counts else "No ranks"
                        deviated_param = f"{display_name}({rank_str})"
                    else:
                        deviated_param = display_name

                    deviated_by_gt_code[key].append(deviated_param)

    # Generate note with grouped deviated parameters and add line between notes
    note_lines = []
    for i, ((gt_code, machine, sizecode), params) in enumerate(deviated_by_gt_code.items()):
        note_lines.append(
            f"**Note{i+1}:** Gt Code: {gt_code} | Deviated parameters: {', '.join(params)}"
        )

    # Join notes with two line breaks between them
    note = "\n\n".join(note_lines) if note_lines else ""

    # Create a single payload with generic title
    if alert_type.upper() == "TBM":
        title_str = f"Alert: TBM Alert Summary"
    elif alert_type.upper() == "CURING":
        title_str = f"Alert: CURING Alert Summary"
    elif alert_type.upper() == "UFM":
        title_str = f"Alert: UFM Alert Summary"
    else:
        title_str = "Alert Summary"

    markdown_table = html_table_to_markdown(summary_html)
    
    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "summary": title_str,
        "themeColor": "0076D7",
        "title": title_str,
        "text": f"**{title_str}**\n\n{markdown_table}\n\n{note}"
    }
    payload_list = [payload]

    # Send the single payload
    for payload in payload_list:
        try:
            response = requests.post(teams_webhook_url, json=payload)
            if response.status_code == 200:
                print(f"[INFO] Teams alert sent successfully for {alert_type} alerts")
                return True
            else:
                print(f"[ERROR] Failed to send Teams alert: {response.status_code} {response.text}")
                return False
        except Exception as e:
            print(f"[ERROR] Exception sending Teams alert: {e}")
            return False

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
                    hl = ((col_name == 'difference' and data[key].get('shift_occurred_thershold')) or \
                        (col_name == 'spec_diff' and data[key].get('shift_occurred_spec')))
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
            self.cell(widths[1] + widths[2], h, sanitize_text(m['ubno']), 1, 0, 'C')
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
                self.cell(barcode_width, h, f"{sanitize_text(m['ubno'])} Barcodes", 1, 0, 'C', fill=True)
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
            self.cell(usable, h, f"{sanitize_text(m['ubno'])} Barcodes", 1, 0, 'C', fill=True)
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
        # 'modelno': doc['modelno'],
        'target_machine_lot_size': doc.get('target_machine_lot_size'),
        atype['alert_time_key']: doc.get(atype['alert_time_key'])
    }

    raw  = alert['parameter_alerts']
    data = {p['parameter']: p for p in raw}
    for p in data.values():
        c, s = p.get('current_value'), p.get('spec_value')
        p['spec_diff'] = float(c)-float(s) if c is not None and s is not None else None

    # filter keys present
    all_ufm = PARAM_KEYS[:15]
    all_bal = PARAM_KEYS[15:23]
    all_geometery = PARAM_KEYS[23:51]
    all_yields = PARAM_KEYS[51:]
    ufm_keys = [k for k in all_ufm if k in data]
    bal_keys = [k for k in all_bal if k in data]
    geometry_keys = [k for k in all_geometery if k in data]
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
    gt_code = sanitize_text(str(alert.get('gt_code', 'UNKNOWN')))
    identifier = f":{atype_name}_{machine}_{gt_code}_{ts_str if ts_str else 'None'}"

    # Build identifier
    # identifier = f":{atype_name}_{machine}_{modelno}_{ts_str if ts_str else 'None'}"

										
    pdf.set_font('Arial','B',11)
    pdf.set_xy(x2, pdf.get_y())
    pdf.cell(value_1, line_h, sanitize_text(identifier), 0, 1, 'R')

    pdf.ln(2)


    # Details
    label_w, h = 40, 6
    val_w = half - label_w
    y = pdf.get_y()


    pdf.set_xy(x1,y)
    pdf.set_font('Arial','B',11); pdf.cell(label_w,h,"Gt Code",0,0)
    pdf.set_font('Arial','',11);  pdf.cell(val_w,h,f": {sanitize_text(alert['gt_code'])}",0,0)
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

    # y = pdf.get_y()
    # pdf.set_xy(x1,y)
    # pdf.set_font('Arial','B',11); pdf.cell(label_w,h,"Gt Code",0,0)
    # pdf.set_font('Arial','',11);  pdf.cell(val_w,h,f": {sanitize_text(alert['gt_code'])}",0,1)

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

																					   
    ufm_keys = PARAM_KEYS[:15]
    bal_keys = PARAM_KEYS[15:23]
    geometry_keys = PARAM_KEYS[23:51]
    yields_keys = PARAM_KEYS[51:]
    group_defs = [("UFM", ufm_keys), ("BALANCING", bal_keys), ("GEOMETRY", geometry_keys), ("YIELDS", yields_keys)]
																									
                  
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
        group_defs=[("UFM", ufm_keys), ("BALANCING", bal_keys), ("GEOMETRY", geometry_keys), ("YIELDS", yields_keys)],
                                                                                   
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
        raw_group = entry['grouped_averages']['ubno']
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
                'ubno': rec['ubno'],
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

# def get_admin_emails(client):
#     cols = client[alert_mail_db][alert_users]
#     return [r['email'] for r in cols.find({}, {'_id':0,'email':1})]

def get_admin_emails(client):
    cols = client[alert_mail_db][alert_users]
    users = cols.find(
        {"is_alert_user": True},
        {"_id": 0, "email": 1}
    )
    emails = [u["email"] for u in users if u.get("email")]
    return emails

def send_alerts():
    """
    Sends alerts by grouping PDFs by gt_code across all work centers (TBM, CURING, UFM).
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

    # Group alerts by gt_code
    alerts_by_gt_code = {}
    for doc in all_alerts:
        # modelno = sanitize_text(doc.get('modelno', 'N/A'))
        gt_code = sanitize_text(doc.get('gt_code', 'N/A'))
        if gt_code not in alerts_by_gt_code:
            alerts_by_gt_code[gt_code] = []
        alerts_by_gt_code[gt_code].append(doc)
                    

    # Process each gt_code group
    for gt_code, docs in alerts_by_gt_code.items():
        # Collect size codes for this gt_code
        size_codes = sorted(set(sanitize_text(m.get('sizecode', 'N/A')) for d in docs for m in d.get('machine_alerts', [])))
        sizecode_str = ", ".join(size_codes) if size_codes else "N/A"
        
        # Generate PDFs for all alerts in this gt_code
        pdf_list = []
        for doc in docs:
            at = doc['alert_type']
            pdf_bytes = generate_alert_pdf(doc, at)
            fn = f"{at['filename_prefix']}_{sanitize_text(doc['machine_alerts'][0]['machine'])}_{gt_code}_{ts}.pdf"
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
                    "subject": f"PCR AP - Yield Alert: GT CODE - {gt_code} | Size Code - {sizecode_str} (Batch {batch_number})",
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
                            "subject": f"PCR AP - Yield Alert: GT CODE - {gt_code} | Size Code - {sizecode_str} (Batch {batch_number})",
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
                            print(f"[INFO] Batch {batch_number} emailed via Microsoft Graph successfully for GT CODE {gt_code}.")
                            # Mark alerts sent in MongoDB for this batch
                            for pdf_item in current_batch:
                                db[pdf_item['collection']].update_one(
                                    {'_id': pdf_item['doc_id']},
                                    {'$set': {'alert_flag': True, doc['alert_type']['alert_time_key']: datetime.now()}}
                                )
                            batched_doc_ids.extend([pdf_item['doc_id'] for pdf_item in current_batch])
                            total_pdfs_processed += len(current_batch)
                        else:
                            print(f"[ERROR] Graph API sendMail failed for batch {batch_number} (GT CODE {gt_code}): {resp.status_code} {resp.text}")
                    except Exception as e:
                        print(f"[ERROR] Exception while sending batch {batch_number} (GT CODE {gt_code}): {e}")
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
                    "subject": f"PCR AP - Yield Alert: GT CODE - {gt_code} | Size Code - {sizecode_str} (Batch {batch_number})",
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
                    print(f"[INFO] Final batch {batch_number} emailed via Microsoft Graph successfully for GT CODE {gt_code}.")
                    for pdf_item in current_batch:
                        db[pdf_item['collection']].update_one(
                            {'_id': pdf_item['doc_id']},
                            {'$set': {'alert_flag': True, doc['alert_type']['alert_time_key']: datetime.now()}}
                        )
                    batched_doc_ids.extend([pdf_item['doc_id'] for pdf_item in current_batch])
                    total_pdfs_processed += len(current_batch)
                else:
                    print(f"[ERROR] Graph API sendMail failed for final batch {batch_number} (GT CODE {gt_code}): {resp.status_code} {resp.text}")
            except Exception as e:
                print(f"[ERROR] Exception while sending final batch {batch_number} (GT CODE {gt_code}): {e}")
                traceback.print_exc()
        print(f"[INFO] Processed {total_pdfs_processed} PDFs for GT CODE {gt_code} in {batch_number} batches.")
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