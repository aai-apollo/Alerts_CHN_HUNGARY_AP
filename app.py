from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from pymongo import MongoClient
import pandas as pd
import re
from datetime import datetime, timezone, time as datetime_time
from datetime import datetime, date  # Ensure both datetime and date are imported
import inspect
import traceback
from uuid import uuid4
import time
import os
import numpy as np
import pymongo
import pytz
import warnings
import logging
from pymongo.errors import BulkWriteError, ConnectionFailure
from passlib.context import CryptContext
import csv
import bcrypt
import datetime
import redshift_connector
from datetime import datetime
from datetime import datetime, timedelta
from pymongo import ASCENDING
import decimal

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

from config import (
    client,
    FILE_TRACKING_DB,
    FILE_TRACKING_COLLECTION,
    UNIFORMITY_DB,
    BALANCING_DB,
    GEOMETRY_DB,
    Data_Clean_UNIFORMITY,
    Data_Clean_BALANCING,
    Data_Clean_GEOMETRY,
    UNIFORMITY_CLEANED_PREFIX,
    BALANCING_CLEANED_PREFIX,
    GEOMETRY_CLEANED_PREFIX,
    UNIFORMITY_INVALID_PREFIX,
    BALANCING_INVALID_PREFIX,
    GEOMETRY_INVALID_PREFIX,
    COMBINED_Database,
    COMBINED_COLLECTION_NAME,
    ALL_COMBINED_UNIFORMITY_COLLECTION,
    ALL_COMBINED_BALANCING_COLLECTION,
    ALL_COMBINED_GEOMETRY_COLLECTION,
    INVALID_ROWS_DB,
    UNIFORMITY_BALANCING_GEOMETRY_DB,
    all_combined_uniformity_data,
    all_combined_balancing_data,
    all_combined_geometry_data,
    uniformity_collection_name,
    balancing_collection_name,
    geometry_collection_name,
    new_uniformity_collection_name,
    new_balancing_collection_name,
    new_geometry_collection_name,
    config_databse,
    channel_no_collect,
    ub_machine_no_collect,
    lot_db,
    alert_db_virgin_yield,
    alert_db_final_yield,
    alert_deviated_final_yield,
    alert_table_final_yield,
    summary_db,
    lot_table_collect,
    machine_no_collect,
    curing_press_dict_collect,
    average_db,
    lot_tbm_collect_name,
    alert_tbm_collect_name,
    alert_ufm_collect_name,
    alert_triggering_deviated_ufm,
    alert_curing_collect_name,
    deviation_occurred_collect_name,
    deviation_occurred_param_collect_name,

    averages_tbm_table,
    alert_triggering_deviated_tbm,
    averages_ufm_table,
    alert_triggering_collect_name,
    summary_triggering_collect_name_tbm,
    lot_curing_collect_name,
    alert_curing_collect_name,
    averages_curing_table_name,
    alert_triggering_curing_collect_name,
    channel_no_collect_tbm,
    channel_no_collect_curing,
    alert_triggering_deviated_curing,
    alert_triggering_ubno_collect_name,
       
    # tbm new without lot size
    # new_inner_join_collection_collection,
    lot_tbm_collect_name_new,
    averages_tbm_table_new,
    alert_tbm_collect_name_new,
    alert_triggering_collect_name_new,
    alert_triggering_deviated_tbm_new,

    # curing new without lot size
    # new_inner_join_collection_collection,
    lot_curing_collect_name_new,
    alert_curing_collect_name_new,
    averages_curing_table_name_new,
    alert_triggering_curing_collect_name_new,
    alert_triggering_deviated_curing_new,

   # ufm new without lot size
    lot_ufm_collect_name_new,
    alert_ufm_collect_name_new,
    averages_ufm_table_new,
    alert_ufm_collect_name_new,
    alert_triggering_ubno_collect_name_new,
    alert_triggering_deviated_ufm_new,
    # new_tub_collection_collection,
    alert_mail_db, # Add 05-07
    admin_table,   # Add 05-07
    alert_users,
######### Add 18-07 ###########
    model_db,
    curing_collection,
    tbm_collection,
    ufm_collection,
    tbm_collection_machine,
    curing_collection_machine,
    ufm_collection_machine,
######### Add 18-07 ###########

    # Define new collection names
    new_uniformity_collection_name,
    new_balancing_collection_name,
    new_geometry_collection_name,
    new_COMBINED_COLLECTION_NAME

)


app = Flask(__name__)
app.secret_key = "supersecretkey"

def configure_logging():
    if not os.path.exists('logs'):
        os.makedirs('logs')
    current_date = datetime.now().strftime('%Y-%m-%d')
    log_filename = f'logs/log_alert_{current_date}.txt'
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.FileHandler(log_filename, mode='a', encoding='utf-8')]
    )
    warnings.simplefilter("always")
    logging.captureWarnings(True)

# # Call this at the beginning of your script           
# # Define yield calculation functions
def yield_percentage(numerator, denominator):
    """Calculate yield percentage, handling division by zero."""
    try:
        if denominator == 0:
            return 0.0
        return (numerator / denominator) * 100
    except (TypeError, ValueError) as e:
        logging.warning(f"Error calculating yield percentage: {e}")
        return None

############ Uniformity ##################
def rfv_yield(batch_data):
    try:
        # ---------------------------------------
        # Normalize CW and CCW grades
        # ---------------------------------------
        cw_grade = (
            batch_data['tu_rfpp_cw_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        ccw_grade = (
            batch_data['tu_rfpp_ccw_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        barcode_valid = batch_data['barcode'].notnull()

        # ---------------------------------------
        # Denominator:
        # Count of ALL applicable grades
        # from CW + CCW
        # ---------------------------------------
        denominator_cw = (
            (cw_grade != '') &
            barcode_valid
        ).sum()

        denominator_ccw = (
            (ccw_grade != '') &
            barcode_valid
        ).sum()

        denominator = denominator_cw + denominator_ccw

        # ---------------------------------------
        # Numerator:
        # Grades starting with A, B, C or D
        # Case-insensitive
        # ---------------------------------------
        numerator_cw = (
            cw_grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        numerator_ccw = (
            ccw_grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        numerator = numerator_cw + numerator_ccw

        # ---------------------------------------
        # Final Yield %
        # ---------------------------------------
        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0

def rfh1_yield(batch_data):
    try:
        # ---------------------------------------
        # Normalize CW and CCW grades
        # ---------------------------------------
        cw_grade = (
            batch_data['tu_rfh1_cw_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        ccw_grade = (
            batch_data['tu_rfh1_ccw_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        barcode_valid = batch_data['barcode'].notnull()

        # ---------------------------------------
        # Denominator:
        # Count of ALL applicable grades
        # from CW + CCW
        # ---------------------------------------
        denominator_cw = (
            (cw_grade != '') &
            barcode_valid
        ).sum()

        denominator_ccw = (
            (ccw_grade != '') &
            barcode_valid
        ).sum()

        denominator = denominator_cw + denominator_ccw

        # ---------------------------------------
        # Numerator:
        # Grades starting with A, B, C or D
        # Case-insensitive
        # ---------------------------------------
        numerator_cw = (
            cw_grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        numerator_ccw = (
            ccw_grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        numerator = numerator_cw + numerator_ccw

        # ---------------------------------------
        # Final Yield %
        # ---------------------------------------
        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0

def rfh2_yield(batch_data):
    try:
        # ---------------------------------------
        # Normalize CW and CCW grades
        # ---------------------------------------
        cw_grade = (
            batch_data['tu_rfh2_cw_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        ccw_grade = (
            batch_data['tu_rfh2_ccw_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        barcode_valid = batch_data['barcode'].notnull()

        # ---------------------------------------
        # Denominator:
        # Count of ALL applicable grades
        # from CW + CCW
        # ---------------------------------------
        denominator_cw = (
            (cw_grade != '') &
            barcode_valid
        ).sum()

        denominator_ccw = (
            (ccw_grade != '') &
            barcode_valid
        ).sum()

        denominator = denominator_cw + denominator_ccw

        # ---------------------------------------
        # Numerator:
        # Grades starting with A, B, C or D
        # Case-insensitive
        # ---------------------------------------
        numerator_cw = (
            cw_grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        numerator_ccw = (
            ccw_grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        numerator = numerator_cw + numerator_ccw

        # ---------------------------------------
        # Final Yield %
        # ---------------------------------------
        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0

def lfv_yield(batch_data):
    try:
        # ---------------------------------------
        # Normalize CW and CCW grades
        # ---------------------------------------
        cw_grade = (
            batch_data['tu_lfpp_cw_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        ccw_grade = (
            batch_data['tu_lfpp_ccw_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        barcode_valid = batch_data['barcode'].notnull()

        # ---------------------------------------
        # Denominator:
        # Count of ALL applicable grades
        # from CW + CCW
        # ---------------------------------------
        denominator_cw = (
            (cw_grade != '') &
            barcode_valid
        ).sum()

        denominator_ccw = (
            (ccw_grade != '') &
            barcode_valid
        ).sum()

        denominator = denominator_cw + denominator_ccw

        # ---------------------------------------
        # Numerator:
        # Grades starting with A, B, C or D
        # Case-insensitive
        # ---------------------------------------
        numerator_cw = (
            cw_grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        numerator_ccw = (
            ccw_grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        numerator = numerator_cw + numerator_ccw

        # ---------------------------------------
        # Final Yield %
        # ---------------------------------------
        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0

def lsft_yield(batch_data):
    try:
        # ---------------------------------------
        # Normalize CW and CCW grades
        # ---------------------------------------
        cw_grade = (
            batch_data['tu_lsft_cw_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        ccw_grade = (
            batch_data['tu_lsft_ccw_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        barcode_valid = batch_data['barcode'].notnull()

        # ---------------------------------------
        # Denominator:
        # Count of ALL applicable grades
        # from CW + CCW
        # ---------------------------------------
        denominator_cw = (
            (cw_grade != '') &
            barcode_valid
        ).sum()

        denominator_ccw = (
            (ccw_grade != '') &
            barcode_valid
        ).sum()

        denominator = denominator_cw + denominator_ccw

        # ---------------------------------------
        # Numerator:
        # Grades starting with A, B, C or D
        # Case-insensitive
        # ---------------------------------------
        numerator_cw = (
            cw_grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        numerator_ccw = (
            ccw_grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        numerator = numerator_cw + numerator_ccw

        # ---------------------------------------
        # Final Yield %
        # ---------------------------------------
        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0

def lfh1_yield(batch_data):
    try:
        # ---------------------------------------
        # Normalize CW and CCW grades
        # ---------------------------------------
        cw_grade = (
            batch_data['tu_lfh1_cw_grad']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        ccw_grade = (
            batch_data['tu_lfh1_ccw_grad']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        barcode_valid = batch_data['barcode'].notnull()

        # ---------------------------------------
        # Denominator:
        # Count of ALL applicable grades
        # from CW + CCW
        # ---------------------------------------
        denominator_cw = (
            (cw_grade != '') &
            barcode_valid
        ).sum()

        denominator_ccw = (
            (ccw_grade != '') &
            barcode_valid
        ).sum()

        denominator = denominator_cw + denominator_ccw

        # ---------------------------------------
        # Numerator:
        # Grades starting with A, B, C or D
        # Case-insensitive
        # ---------------------------------------
        numerator_cw = (
            cw_grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        numerator_ccw = (
            ccw_grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        numerator = numerator_cw + numerator_ccw

        # ---------------------------------------
        # Final Yield %
        # ---------------------------------------
        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0

def conicity_yield(batch_data):        # Add 05-07
    try:
        # ---------------------------------------
        # Normalize Conicity Grade
        # ---------------------------------------
        grade = (
            batch_data['tu_cony_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        barcode_valid = batch_data['barcode'].notnull()

        # ---------------------------------------
        # Denominator:
        # Count of all applicable grades
        # ---------------------------------------
        denominator = (
            (grade != '') &
            barcode_valid
        ).sum()

        # ---------------------------------------
        # Numerator:
        # Grades starting with A, B, C or D
        # ---------------------------------------
        numerator = (
            grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        # ---------------------------------------
        # Final Yield %
        # ---------------------------------------
        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0

def oe_yield(batch_data):
    """
    Calculate OE yield percentage based on tu_class values in batch_data.
    
    Args:
        batch_data: pandas DataFrame with 'tu_class' and 'barcode' columns.
    
    Returns:
        float: Yield percentage, or None if an error occurs.
    """
    try:
        # Validate input
        if not isinstance(batch_data, pd.DataFrame):
            logging.warning("Input batch_data is not a pandas DataFrame")
            return None
        
        # Check for required columns
        if 'tu_class' not in batch_data.columns or 'barcode' not in batch_data.columns:
            logging.warning("Missing required columns: 'tu_class' or 'barcode'")
            return None
        
        # Check for missing values in 'tu_class' column
        if batch_data['tu_class'].isna().any():
            logging.warning("Missing values found in 'tu_class' column")
            return None

        # Define valid classes for denominator
        valid_classes = ['TX', 'T1', 'T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'T8']
        denominator_data = batch_data[batch_data['tu_class'].isin(valid_classes)]
        denominator = denominator_data['barcode'].nunique()
        
        if denominator == 0:
            logging.info("No valid barcodes found for denominator classes")
            return 0.0
        
        # Filter for numerator classes (T1, T2, T3, T4)
        numerator_data = denominator_data[denominator_data['tu_class'].isin(['T1', 'T2', 'T3', 'T4'])]
        numerator = numerator_data['barcode'].nunique()
        
        result = yield_percentage(numerator, denominator)
        if result is None:
            logging.warning("Failed to calculate yield percentage")
            return None
            
        return float(result)
    
    except (KeyError, TypeError, ValueError) as e:
        logging.error(f"Error in oe_yield: {e}")
        return None

def rfmxa_yield(batch_data):        # Add 05-07
    try:
        # ---------------------------------------
        # Normalize Conicity Grade
        # ---------------------------------------
        grade = (
            batch_data['tu_rfmxa_ccw_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        barcode_valid = batch_data['barcode'].notnull()

        # ---------------------------------------
        # Denominator:
        # Count of all applicable grades
        # ---------------------------------------
        denominator = (
            (grade != '') &
            barcode_valid
        ).sum()

        # ---------------------------------------
        # Numerator:
        # Grades starting with A, B, C or D
        # ---------------------------------------
        numerator = (
            grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        # ---------------------------------------
        # Final Yield %
        # ---------------------------------------
        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0

################ Geometry ######################
def rfa1_yield(batch_data):
    try:
        # ---------------------------------------
        # Normalize CW and CCW grades
        # ---------------------------------------
        cw_grade = (
            batch_data['tu_rfa1_cw_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        ccw_grade = (
            batch_data['tu_rfa1_ccw_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        barcode_valid = batch_data['barcode'].notnull()

        # ---------------------------------------
        # Denominator:
        # Count of ALL applicable grades
        # from CW + CCW
        # ---------------------------------------
        denominator_cw = (
            (cw_grade != '') &
            barcode_valid
        ).sum()

        denominator_ccw = (
            (ccw_grade != '') &
            barcode_valid
        ).sum()

        denominator = denominator_cw + denominator_ccw

        # ---------------------------------------
        # Numerator:
        # Grades starting with A, B, C or D
        # Case-insensitive
        # ---------------------------------------
        numerator_cw = (
            cw_grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        numerator_ccw = (
            ccw_grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        numerator = numerator_cw + numerator_ccw

        # ---------------------------------------
        # Final Yield %
        # ---------------------------------------
        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0

def rfa2_yield(batch_data):
    try:
        # ---------------------------------------
        # Normalize CW and CCW grades
        # ---------------------------------------
        cw_grade = (
            batch_data['tu_rfa2_cw_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        ccw_grade = (
            batch_data['tu_rfa2_ccw_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        barcode_valid = batch_data['barcode'].notnull()

        # ---------------------------------------
        # Denominator:
        # Count of ALL applicable grades
        # from CW + CCW
        # ---------------------------------------
        denominator_cw = (
            (cw_grade != '') &
            barcode_valid
        ).sum()

        denominator_ccw = (
            (ccw_grade != '') &
            barcode_valid
        ).sum()

        denominator = denominator_cw + denominator_ccw

        # ---------------------------------------
        # Numerator:
        # Grades starting with A, B, C or D
        # Case-insensitive
        # ---------------------------------------
        numerator_cw = (
            cw_grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        numerator_ccw = (
            ccw_grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        numerator = numerator_cw + numerator_ccw

        # ---------------------------------------
        # Final Yield %
        # ---------------------------------------
        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0

def lfxma_yield(batch_data):
    try:
        # ---------------------------------------
        # Normalize CW and CCW grades
        # ---------------------------------------
        cw_grade = (
            batch_data['tu_lfmxa_cw_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        ccw_grade = (
            batch_data['tu_lfmxa_ccw_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        barcode_valid = batch_data['barcode'].notnull()

        # ---------------------------------------
        # Denominator:
        # Count of ALL applicable grades
        # from CW + CCW
        # ---------------------------------------
        denominator_cw = (
            (cw_grade != '') &
            barcode_valid
        ).sum()

        denominator_ccw = (
            (ccw_grade != '') &
            barcode_valid
        ).sum()

        denominator = denominator_cw + denominator_ccw

        # ---------------------------------------
        # Numerator:
        # Grades starting with A, B, C or D
        # Case-insensitive
        # ---------------------------------------
        numerator_cw = (
            cw_grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        numerator_ccw = (
            ccw_grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        numerator = numerator_cw + numerator_ccw

        # ---------------------------------------
        # Final Yield %
        # ---------------------------------------
        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0

def lfa1_yield(batch_data):
    try:
        # ---------------------------------------
        # Normalize CW and CCW grades
        # ---------------------------------------
        cw_grade = (
            batch_data['tu_lfa1_cw_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        ccw_grade = (
            batch_data['tu_lfa1_ccw_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        barcode_valid = batch_data['barcode'].notnull()

        # ---------------------------------------
        # Denominator:
        # Count of ALL applicable grades
        # from CW + CCW
        # ---------------------------------------
        denominator_cw = (
            (cw_grade != '') &
            barcode_valid
        ).sum()

        denominator_ccw = (
            (ccw_grade != '') &
            barcode_valid
        ).sum()

        denominator = denominator_cw + denominator_ccw

        # ---------------------------------------
        # Numerator:
        # Grades starting with A, B, C or D
        # Case-insensitive
        # ---------------------------------------
        numerator_cw = (
            cw_grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        numerator_ccw = (
            ccw_grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        numerator = numerator_cw + numerator_ccw

        # ---------------------------------------
        # Final Yield %
        # ---------------------------------------
        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0

def central_radial_runout_yield(batch_data):        # Add 05-07
    try:
        # ---------------------------------------
        # Normalize Conicity Grade
        # ---------------------------------------
        grade = (
            batch_data['tg_crro_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        barcode_valid = batch_data['barcode'].notnull()

        # ---------------------------------------
        # Denominator:
        # Count of all applicable grades
        # ---------------------------------------
        denominator = (
            (grade != '') &
            barcode_valid
        ).sum()

        # ---------------------------------------
        # Numerator:
        # Grades starting with A, B, C or D
        # ---------------------------------------
        numerator = (
            grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        # ---------------------------------------
        # Final Yield %
        # ---------------------------------------
        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0

def central_radial_runout_first_harmonic_yield(batch_data):        # Add 05-07
    try:
        # ---------------------------------------
        # Normalize Conicity Grade
        # ---------------------------------------
        grade = (
            batch_data['tg_crroh1_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        barcode_valid = batch_data['barcode'].notnull()

        # ---------------------------------------
        # Denominator:
        # Count of all applicable grades
        # ---------------------------------------
        denominator = (
            (grade != '') &
            barcode_valid
        ).sum()

        # ---------------------------------------
        # Numerator:
        # Grades starting with A, B, C or D
        # ---------------------------------------
        numerator = (
            grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        # ---------------------------------------
        # Final Yield %
        # ---------------------------------------
        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0

def lateral_run_out_top_over_yield(batch_data):        # Add 05-07
    try:
        # ---------------------------------------
        # Normalize Conicity Grade
        # ---------------------------------------
        grade = (
            batch_data['tg_tlro_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        barcode_valid = batch_data['barcode'].notnull()

        # ---------------------------------------
        # Denominator:
        # Count of all applicable grades
        # ---------------------------------------
        denominator = (
            (grade != '') &
            barcode_valid
        ).sum()

        # ---------------------------------------
        # Numerator:
        # Grades starting with A, B, C or D
        # ---------------------------------------
        numerator = (
            grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        # ---------------------------------------
        # Final Yield %
        # ---------------------------------------
        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0

def lateral_run_out_bottom_over_yield(batch_data):        # Add 05-07
    try:
        # ---------------------------------------
        # Normalize Conicity Grade
        # ---------------------------------------
        grade = (
            batch_data['tg_blro_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        barcode_valid = batch_data['barcode'].notnull()

        # ---------------------------------------
        # Denominator:
        # Count of all applicable grades
        # ---------------------------------------
        denominator = (
            (grade != '') &
            barcode_valid
        ).sum()

        # ---------------------------------------
        # Numerator:
        # Grades starting with A, B, C or D
        # ---------------------------------------
        numerator = (
            grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        # ---------------------------------------
        # Final Yield %
        # ---------------------------------------
        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0

def lateral_run_out_yield(batch_data):
    try:
        # ---------------------------------------
        # Normalize CW and CCW grades
        # ---------------------------------------
        cw_grade = (
            batch_data['tg_tlro_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        ccw_grade = (
            batch_data['tg_blro_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        barcode_valid = batch_data['barcode'].notnull()

        # ---------------------------------------
        # Denominator:
        # Count of ALL applicable grades
        # from CW + CCW
        # ---------------------------------------
        denominator_cw = (
            (cw_grade != '') &
            barcode_valid
        ).sum()

        denominator_ccw = (
            (ccw_grade != '') &
            barcode_valid
        ).sum()

        denominator = denominator_cw + denominator_ccw

        # ---------------------------------------
        # Numerator:
        # Grades starting with A, B, C or D
        # Case-insensitive
        # ---------------------------------------
        numerator_cw = (
            cw_grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        numerator_ccw = (
            ccw_grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        numerator = numerator_cw + numerator_ccw

        # ---------------------------------------
        # Final Yield %
        # ---------------------------------------
        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0

def lateral_run_out_bottom_first_harmonic_yield(batch_data):        # Add 05-07
    try:
        # ---------------------------------------
        # Normalize Conicity Grade
        # ---------------------------------------
        grade = (
            batch_data['tg_blroh1_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        barcode_valid = batch_data['barcode'].notnull()

        # ---------------------------------------
        # Denominator:
        # Count of all applicable grades
        # ---------------------------------------
        denominator = (
            (grade != '') &
            barcode_valid
        ).sum()

        # ---------------------------------------
        # Numerator:
        # Grades starting with A, B, C or D
        # ---------------------------------------
        numerator = (
            grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        # ---------------------------------------
        # Final Yield %
        # ---------------------------------------
        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0

def lateral_run_out_top_first_harmonic_yield(batch_data):        # Add 05-07
    try:
        # ---------------------------------------
        # Normalize Conicity Grade
        # ---------------------------------------
        grade = (
            batch_data['tg_tlroh1_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        barcode_valid = batch_data['barcode'].notnull()

        # ---------------------------------------
        # Denominator:
        # Count of all applicable grades
        # ---------------------------------------
        denominator = (
            (grade != '') &
            barcode_valid
        ).sum()

        # ---------------------------------------
        # Numerator:
        # Grades starting with A, B, C or D
        # ---------------------------------------
        numerator = (
            grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        # ---------------------------------------
        # Final Yield %
        # ---------------------------------------
        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0

def lateral_run_out_harmonic_yield(batch_data):
    try:
        # ---------------------------------------
        # Normalize CW and CCW grades
        # ---------------------------------------
        cw_grade = (
            batch_data['tg_tlroh1_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        ccw_grade = (
            batch_data['tg_blroh1_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        barcode_valid = batch_data['barcode'].notnull()

        # ---------------------------------------
        # Denominator:
        # Count of ALL applicable grades
        # from CW + CCW
        # ---------------------------------------
        denominator_cw = (
            (cw_grade != '') &
            barcode_valid
        ).sum()

        denominator_ccw = (
            (ccw_grade != '') &
            barcode_valid
        ).sum()

        denominator = denominator_cw + denominator_ccw

        # ---------------------------------------
        # Numerator:
        # Grades starting with A, B, C or D
        # Case-insensitive
        # ---------------------------------------
        numerator_cw = (
            cw_grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        numerator_ccw = (
            ccw_grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        numerator = numerator_cw + numerator_ccw

        # ---------------------------------------
        # Final Yield %
        # ---------------------------------------
        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0

def bulge_top_over_yield(batch_data):        # Add 05-07
    try:
        # ---------------------------------------
        # Normalize Conicity Grade
        # ---------------------------------------
        grade = (
            batch_data['tg_topbul_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        barcode_valid = batch_data['barcode'].notnull()

        # ---------------------------------------
        # Denominator:
        # Count of all applicable grades
        # ---------------------------------------
        denominator = (
            (grade != '') &
            barcode_valid
        ).sum()

        # ---------------------------------------
        # Numerator:
        # Grades starting with A, B, C or D
        # ---------------------------------------
        numerator = (
            grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        # ---------------------------------------
        # Final Yield %
        # ---------------------------------------
        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0

def bulge_bottom_over_yield(batch_data):        # Add 05-07
    try:
        # ---------------------------------------
        # Normalize Conicity Grade
        # ---------------------------------------
        grade = (
            batch_data['tg_botbul_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        barcode_valid = batch_data['barcode'].notnull()

        # ---------------------------------------
        # Denominator:
        # Count of all applicable grades
        # ---------------------------------------
        denominator = (
            (grade != '') &
            barcode_valid
        ).sum()

        # ---------------------------------------
        # Numerator:
        # Grades starting with A, B, C or D
        # ---------------------------------------
        numerator = (
            grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        # ---------------------------------------
        # Final Yield %
        # ---------------------------------------
        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0

def bulge_yield(batch_data):
    try:
        # ---------------------------------------
        # Normalize CW and CCW grades
        # ---------------------------------------
        cw_grade = (
            batch_data['tg_botbul_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        ccw_grade = (
            batch_data['tg_topbul_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        barcode_valid = batch_data['barcode'].notnull()

        # ---------------------------------------
        # Denominator:
        # Count of ALL applicable grades
        # from CW + CCW
        # ---------------------------------------
        denominator_cw = (
            (cw_grade != '') &
            barcode_valid
        ).sum()

        denominator_ccw = (
            (ccw_grade != '') &
            barcode_valid
        ).sum()

        denominator = denominator_cw + denominator_ccw

        # ---------------------------------------
        # Numerator:
        # Grades starting with A, B, C or D
        # Case-insensitive
        # ---------------------------------------
        numerator_cw = (
            cw_grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        numerator_ccw = (
            ccw_grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        numerator = numerator_cw + numerator_ccw

        # ---------------------------------------
        # Final Yield %
        # ---------------------------------------
        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0

def dent_top_over_yield(batch_data):        # Add 05-07
    try:
        # ---------------------------------------
        # Normalize Conicity Grade
        # ---------------------------------------
        grade = (
            batch_data['tg_topdep_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        barcode_valid = batch_data['barcode'].notnull()

        # ---------------------------------------
        # Denominator:
        # Count of all applicable grades
        # ---------------------------------------
        denominator = (
            (grade != '') &
            barcode_valid
        ).sum()

        # ---------------------------------------
        # Numerator:
        # Grades starting with A, B, C or D
        # ---------------------------------------
        numerator = (
            grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        # ---------------------------------------
        # Final Yield %
        # ---------------------------------------
        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0
        
def dent_bottom_over_yield(batch_data):        # Add 05-07
    try:
        # ---------------------------------------
        # Normalize Conicity Grade
        # ---------------------------------------
        grade = (
            batch_data['tg_botdep_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        barcode_valid = batch_data['barcode'].notnull()

        # ---------------------------------------
        # Denominator:
        # Count of all applicable grades
        # ---------------------------------------
        denominator = (
            (grade != '') &
            barcode_valid
        ).sum()

        # ---------------------------------------
        # Numerator:
        # Grades starting with A, B, C or D
        # ---------------------------------------
        numerator = (
            grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        # ---------------------------------------
        # Final Yield %
        # ---------------------------------------
        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0

def dent_yield(batch_data):
    try:
        # ---------------------------------------
        # Normalize CW and CCW grades
        # ---------------------------------------
        cw_grade = (
            batch_data['tg_topdep_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        ccw_grade = (
            batch_data['tg_botdep_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        barcode_valid = batch_data['barcode'].notnull()

        # ---------------------------------------
        # Denominator:
        # Count of ALL applicable grades
        # from CW + CCW
        # ---------------------------------------
        denominator_cw = (
            (cw_grade != '') &
            barcode_valid
        ).sum()

        denominator_ccw = (
            (ccw_grade != '') &
            barcode_valid
        ).sum()

        denominator = denominator_cw + denominator_ccw

        # ---------------------------------------
        # Numerator:
        # Grades starting with A, B, C or D
        # Case-insensitive
        # ---------------------------------------
        numerator_cw = (
            cw_grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        numerator_ccw = (
            ccw_grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        numerator = numerator_cw + numerator_ccw

        # ---------------------------------------
        # Final Yield %
        # ---------------------------------------
        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0

def geometry_oe_yield(batch_data):
    """
    Calculate OE yield percentage based on tg_class values in batch_data.
    
    Args:
        batch_data: pandas DataFrame with 'tg_class' and 'barcode' columns.
    
    Returns:
        float: Yield percentage, or None if an error occurs.
    """
    try:
        # Validate input
        if not isinstance(batch_data, pd.DataFrame):
            logging.warning("Input batch_data is not a pandas DataFrame")
            return None
        
        # Check for required columns
        if 'tg_class' not in batch_data.columns or 'barcode' not in batch_data.columns:
            logging.warning("Missing required columns: 'tg_class' or 'barcode'")
            return None
        
        # Check for missing values in 'tg_class' column
        if batch_data['tg_class'].isna().any():
            logging.warning("Missing values found in 'tg_class' column")
            return None

        # Define valid classes for denominator
        valid_classes = ['TX', 'T1', 'T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'T8']
        denominator_data = batch_data[batch_data['tg_class'].isin(valid_classes)]
        denominator = denominator_data['barcode'].nunique()
        
        if denominator == 0:
            logging.info("No valid barcodes found for denominator classes")
            return 0.0
        
        # Filter for numerator classes (T1, T2)
        numerator_data = denominator_data[denominator_data['tg_class'].isin(['T1', 'T2', 'T3', 'T4'])]
        numerator = numerator_data['barcode'].nunique()
        
        result = yield_percentage(numerator, denominator)
        if result is None:
            logging.warning("Failed to calculate yield percentage")
            return None
            
        return float(result)
    
    except (KeyError, TypeError, ValueError) as e:
        logging.error(f"Error in geometry_oe_yield: {e}")
        return None

############ Balancing #############
def lower_yield(batch_data):        # Add 05-07
    try:
        # ---------------------------------------
        # Normalize Conicity Grade
        # ---------------------------------------
        grade = (
            batch_data['tb_botplane_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        barcode_valid = batch_data['barcode'].notnull()

        # ---------------------------------------
        # Denominator:
        # Count of all applicable grades
        # ---------------------------------------
        denominator = (
            (grade != '') &
            barcode_valid
        ).sum()

        # ---------------------------------------
        # Numerator:
        # Grades starting with A, B, C or D
        # ---------------------------------------
        numerator = (
            grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        # ---------------------------------------
        # Final Yield %
        # ---------------------------------------
        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0

def upper_yield(batch_data):        # Add 05-07
    try:
        # ---------------------------------------
        # Normalize Conicity Grade
        # ---------------------------------------
        grade = (
            batch_data['tb_topplane_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        barcode_valid = batch_data['barcode'].notnull()

        # ---------------------------------------
        # Denominator:
        # Count of all applicable grades
        # ---------------------------------------
        denominator = (
            (grade != '') &
            barcode_valid
        ).sum()

        # ---------------------------------------
        # Numerator:
        # Grades starting with A, B, C or D
        # ---------------------------------------
        numerator = (
            grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        # ---------------------------------------
        # Final Yield %
        # ---------------------------------------
        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0

def static_yield(batch_data):        # Add 05-07
    try:
        # ---------------------------------------
        # Normalize Conicity Grade
        # ---------------------------------------
        grade = (
            batch_data['tb_static_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        barcode_valid = batch_data['barcode'].notnull()

        # ---------------------------------------
        # Denominator:
        # Count of all applicable grades
        # ---------------------------------------
        denominator = (
            (grade != '') &
            barcode_valid
        ).sum()

        # ---------------------------------------
        # Numerator:
        # Grades starting with A, B, C or D
        # ---------------------------------------
        numerator = (
            grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        # ---------------------------------------
        # Final Yield %
        # ---------------------------------------
        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0

def couple_yield(batch_data):        # Add 05-07
    try:
        # ---------------------------------------
        # Normalize Conicity Grade
        # ---------------------------------------
        grade = (
            batch_data['tb_couple_grade']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
        )

        barcode_valid = batch_data['barcode'].notnull()

        # ---------------------------------------
        # Denominator:
        # Count of all applicable grades
        # ---------------------------------------
        denominator = (
            (grade != '') &
            barcode_valid
        ).sum()

        # ---------------------------------------
        # Numerator:
        # Grades starting with A, B, C or D
        # ---------------------------------------
        numerator = (
            grade.str.startswith(('A', 'B', 'C', 'D')) &
            barcode_valid
        ).sum()

        # ---------------------------------------
        # Final Yield %
        # ---------------------------------------
        return yield_percentage(numerator, denominator)

    except Exception:
        return 0.0

def balancing_oe_yield(batch_data):
    """
    Calculate OE yield percentage based on tb_class values in batch_data.
    
    Args:
        batch_data: pandas DataFrame with 'tb_class' and 'barcode' columns.
    
    Returns:
        float: Yield percentage, or None if an error occurs.
    """
    try:
        # Validate input
        if not isinstance(batch_data, pd.DataFrame):
            logging.warning("Input batch_data is not a pandas DataFrame")
            return None
        
        # Check for required columns
        if 'tb_class' not in batch_data.columns or 'barcode' not in batch_data.columns:
            logging.warning("Missing required columns: 'tb_class' or 'barcode'")
            return None
        
        # Check for missing values in 'tb_class' column
        if batch_data['tb_class'].isna().any():
            logging.warning("Missing values found in 'tb_class' column")
            return None

        # Define valid classes for denominator
        valid_classes = ['TX', 'T1', 'T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'T8']
        denominator_data = batch_data[batch_data['tb_class'].isin(valid_classes)]
        denominator = denominator_data['barcode'].nunique()
        
        if denominator == 0:
            logging.info("No valid barcodes found for denominator classes")
            return 0.0
        
        # Filter for numerator classes (T1, T2, T3, T4)
        numerator_data = denominator_data[denominator_data['tb_class'].isin(['T1', 'T2' 'T3', 'T4'])]
        numerator = numerator_data['barcode'].nunique()
        
        result = yield_percentage(numerator, denominator)
        if result is None:
            logging.warning("Failed to calculate yield percentage")
            return None
            
        return float(result)
    
    except (KeyError, TypeError, ValueError) as e:
        logging.error(f"Error in balancing_oe_yield: {e}")
        return None

def warehouse_yield(batch_data):
    try:
        if 'ufm_testing_result' not in batch_data.columns:
            raise ValueError(
                "'ufm_testing_result' column is not available in the data."
            )

        # Denominator: OE1 + Repl
        denominator_data = batch_data[
            batch_data['ufm_testing_result'].isin(['OE1', 'Repl'])
        ]
        denominator = len(denominator_data)

        # Numerator: OE1
        numerator_data = batch_data[
            batch_data['ufm_testing_result'] == 'OE1'
        ]
        numerator = len(numerator_data)

        # Calculate yield percentage
        return yield_percentage(numerator, denominator)

    except Exception as e:
        logging.info("Error in warehouse_yield: %s", e)
        return None

def visual_yield(batch_data):
    try:
        if 'visualinspection_result' not in batch_data.columns:
            raise ValueError(
                "'visualinspection_result' column is not available in the data."
            )

        # Consider only valid visual inspection results: OK and NOK
        denominator_data = batch_data[
            batch_data['visualinspection_result'].isin(['OK', 'NOK'])
        ]

        denominator = len(denominator_data)

        # Numerator: OK results
        numerator = (
            denominator_data['visualinspection_result'] == 'OK'
        ).sum()

        # Calculate visual yield
        return (numerator / denominator) * 100 if denominator > 0 else 0.0

    except Exception as e:
        logging.info("Error in visual_yield: %s", e)
        return 0.0

# Initialize MongoDB client
db_tracking = client[FILE_TRACKING_DB]
tracking_collection = db_tracking[FILE_TRACKING_COLLECTION]

def some_function():
    india_time = datetime.now(pytz.timezone('Asia/Kolkata'))
    return india_time

logging.info(some_function())

DEFAULT_DATETIME = datetime(1900, 1, 1, 12, 30, 0)
DEFAULT_DATE = datetime(1900, 1, 1)
date_formats = ["%d-%m-%Y", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"]

# Access databases
file_tracking_db = client['file_tracking']
uniformity_db = client['UNIFORMITY']
balancing_db = client['BALANCING']
geometry_db = client['GEOMETRY']

# NOTE: The raw databases 'UNIFORMITY', 'BALANCING', 'GEOMETRY' and the cleaned
# databases 'Data_Clean_UNIFORMITY', 'Data_Clean_BALANCING', 'Data_Clean_GEOMETRY'
# are intentionally NOT created/populated. The data flow is preserved by keeping the
# raw DataFrames in memory (see RAW_DATAFRAME_STORE below) instead of round-tripping
# through MongoDB.
RAW_DATAFRAME_STORE = {}

def insert_with_timestamp(df, collection_name_prefix, db_name):
    """
    Comprehensive function to insert DataFrame into MongoDB with timestamp.
    Handles all data types including datetime, date, numpy types, and NaN values.
    
    Args:
        df: pandas DataFrame to insert
        collection_name_prefix: Prefix for collection name
        db_name: Database name ('BALANCING', 'UNIFORMITY', or 'GEOMETRY')
    
    Returns:
        str: Name of the created collection
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Expected a pandas DataFrame as input")
    
    # Create a deep copy to avoid modifying original
    df = df.copy(deep=True)
    
    # Helper function to safely check if a value is NaN/None
    def is_nan_or_none(value):
        """Safely check if a value is NaN or None."""
        # First check if it's a Series (which should not happen here but just in case)
        if isinstance(value, pd.Series):
            # If it's a Series, extract the first value or return None
            try:
                if len(value) == 0:
                    return True
                return is_nan_or_none(value.iloc[0])
            except:
                return True
        
        # Check for None
        if value is None:
            return True
        
        # Check for pandas NaT
        if isinstance(value, pd._libs.tslibs.nattype.NaTType):
            return True
        
        # Check for numpy/pandas NaN
        try:
            # Try to use pd.isna - but only on scalar values
            if hasattr(value, '__class__'):
                # For scalar numpy/pandas values
                if isinstance(value, (float, np.floating)):
                    return np.isnan(value)
                elif isinstance(value, (pd.Timestamp, datetime)):
                    # For datetime-like objects, check if they're NaT
                    return pd.isna(value)
                else:
                    # For other types, try pd.isna
                    return pd.isna(value)
        except (ValueError, TypeError):
            # If pd.isna fails, it's probably not NaN
            pass
        
        return False
    
    # Helper function to convert any value to MongoDB-compatible format
    def to_mongo_compatible(value):
        """Convert any Python/pandas/NumPy value to MongoDB-compatible format."""
        # First check for NaN/None - SAFELY
        try:
            if is_nan_or_none(value):
                return None
        except Exception as e:
            print(f"Warning in is_nan_or_none: {e}")
            return None
        
        # Handle datetime objects
        if isinstance(value, (datetime, pd.Timestamp)):
            try:
                # Check if it's NaT (Not a Time)
                if pd.isna(value):
                    return None
                return value.strftime('%Y-%m-%d %H:%M:%S')
            except:
                return None
        
        # Handle date objects
        if isinstance(value, date):
            try:
                return value.strftime('%Y-%m-%d')
            except:
                return None
        
        # Handle numpy types
        if isinstance(value, np.generic):
            # Convert numpy scalar to Python scalar
            try:
                return value.item()
            except:
                try:
                    return float(value) if isinstance(value, np.floating) else int(value)
                except:
                    return str(value)

        # Handle Decimal types (returned by Redshift NUMERIC/DECIMAL columns)
        if isinstance(value, decimal.Decimal):
            return float(value)
        
        # Handle numpy arrays
        if isinstance(value, np.ndarray):
            try:
                return value.tolist()
            except:
                return str(value)
        
        # Handle pandas Series (shouldn't happen, but just in case)
        if isinstance(value, pd.Series):
            try:
                # Extract the first value if it's a single-item Series
                if len(value) == 1:
                    return to_mongo_compatible(value.iloc[0])
                else:
                    return value.tolist()
            except:
                return str(value)
        
        # Handle other iterables (lists, tuples, etc.)
        if hasattr(value, '__iter__') and not isinstance(value, (str, bytes)):
            try:
                return [to_mongo_compatible(item) for item in value]
            except:
                return str(value)
        
        # For everything else, return as-is
        return value
    
    # Alternative approach: Convert entire DataFrame first
    print(f"Processing DataFrame with {len(df)} rows and {len(df.columns)} columns")
    
    # Convert datetime columns to string first
    datetime_cols = df.select_dtypes(include=['datetime64', 'datetimetz']).columns
    for col in datetime_cols:
        try:
            # Remove timezone info first to avoid NaT utcoffset errors
            if hasattr(df[col].dtype, 'tz') and df[col].dtype.tz is not None:
                df[col] = df[col].dt.tz_convert(None)
            df[col] = df[col].apply(
                lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if not pd.isna(x) else None
            )
        except Exception as e:
            print(f"Warning converting datetime column {col}: {e}")
            df[col] = df[col].astype(str)
    
    # Convert date columns in object columns
    object_cols = df.select_dtypes(include=['object']).columns
    for col in object_cols:
        try:
            # Check if column contains date/datetime objects
            sample = None
            non_null_values = df[col].dropna()
            if not non_null_values.empty:
                sample = non_null_values.iloc[0]
                
            if sample and isinstance(sample, (date, datetime, pd.Timestamp)):
                df[col] = df[col].apply(
                    lambda x: (
                        x.strftime('%Y-%m-%d %H:%M:%S') if isinstance(x, (datetime, pd.Timestamp)) and not pd.isna(x)
                        else x.strftime('%Y-%m-%d') if isinstance(x, date) and not pd.isna(x)
                        else None if pd.isna(x)
                        else x
                    )
                )
        except Exception as e:
            print(f"Warning processing object column {col}: {e}")
            # Continue without conversion
    
    # Convert numpy types to Python types
    for col in df.columns:
        try:
            # Check if column contains numpy types
            dtype = str(df[col].dtype)
            if dtype.startswith('int') or dtype.startswith('float') or dtype == 'bool':
                # Convert non-null values
                df[col] = df[col].apply(
                    lambda x: x.item() if hasattr(x, 'item') and not pd.isna(x) else x
                )
        except Exception as e:
            print(f"Warning converting numpy types in column {col}: {e}")

    # Convert Decimal columns (Redshift NUMERIC/DECIMAL) to float
    for col in df.select_dtypes(include=['object']).columns:
        try:
            sample = df[col].dropna()
            if not sample.empty and isinstance(sample.iloc[0], decimal.Decimal):
                df[col] = df[col].apply(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)
        except Exception as e:
            print(f"Warning converting Decimal column {col}: {e}")
    
    # Replace all NaN/NaT with None
    df = df.replace({np.nan: None, pd.NaT: None})
    
    # Now process row by row with extra safety
    records = []
    processed_count = 0
    
    for idx, row in df.iterrows():
        try:
            record = {}
            for col in df.columns:
                try:
                    # Get value safely
                    cell_value = row[col]
                    
                    # Final conversion
                    if pd.isna(cell_value) or cell_value is None:
                        record[col] = None
                    elif isinstance(cell_value, (datetime, pd.Timestamp)):
                        record[col] = cell_value.strftime('%Y-%m-%d %H:%M:%S')
                    elif isinstance(cell_value, date):
                        record[col] = cell_value.strftime('%Y-%m-%d')
                    elif hasattr(cell_value, 'item'):  # Numpy scalar
                        record[col] = cell_value.item()
                    elif isinstance(cell_value, np.ndarray):
                        record[col] = cell_value.tolist()
                    else:
                        record[col] = cell_value
                except Exception as e:
                    # If all else fails, convert to string
                    try:
                        record[col] = str(row[col]) if not pd.isna(row[col]) else None
                    except:
                        record[col] = None
            
            records.append(record)
            processed_count += 1
            
            # Show progress for large datasets
            if processed_count % 1000 == 0:
                print(f"Processed {processed_count} rows...")
                
        except Exception as e:
            print(f"Error processing row {idx}: {e}")
            # Create a minimal valid record
            try:
                minimal_record = {}
                for col in df.columns:
                    minimal_record[col] = str(df.at[idx, col]) if col in df.columns else None
                records.append(minimal_record)
            except:
                # Skip this row if we can't process it at all
                print(f"Skipping row {idx} due to unprocessable data")
    
    print(f"Successfully processed {len(records)} records")
    
    # Get timestamp
    try:
        timestamp = some_function().strftime("%Y%m%d_%H%M%S")
    except Exception as e:
        print(f"Warning: Could not get timestamp from some_function(): {e}")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Validate database name (kept for parity with the original behaviour)
    if db_name not in ('BALANCING', 'UNIFORMITY', 'GEOMETRY'):
        raise ValueError(f"Invalid database name: {db_name}")

    collection_name = f"{collection_name_prefix}_{timestamp}"

    # NOTE: We intentionally DO NOT create or write to the raw '{db_name}' database.
    # Instead the processed records are kept in memory so the downstream cleaning flow
    # (run_data_cleaning) can consume them without persisting the raw databases.
    RAW_DATAFRAME_STORE[db_name] = {
        "records": records,
        "filename": collection_name,
        "num_rows": len(df),
    }
    print(f"Prepared {len(records)} '{db_name}' records in memory (raw DB write skipped)")

    # Track the file insertion in the file_tracking database (file_metadata collection).
    # The raw DB write is skipped, but the file-tracking record is still created.
    try:
        track_file_in_file_tracking(collection_name_prefix, timestamp, len(df), db_name)
    except Exception as e:
        print(f"Warning: Could not track file in file tracking: {e}")

    return collection_name

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
        self._conn_params = dict(
            host='atl-dl-prd-01.cuvxusm4qify.ap-south-1.redshift.amazonaws.com',
            database='db_prd_plant_ap',
            port=5439,
            user=os.environ.get('REDSHIFT_USER', ''),
            password=os.environ.get('REDSHIFT_PASSWORD', ''),
            sslmode='disable'
        )

    def _new_conn(self):
        conn = redshift_connector.connect(**self._conn_params)
        conn.autocommit = True  # prevents transaction-aborted state on errors
        return conn

    def execute_query(self, q):
        # Strip trailing semicolon — redshift_connector rejects it
        q = q.strip().rstrip(";")
        conn = self._new_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute(q)
                data = cursor.fetch_dataframe()
        finally:
            conn.close()
        return data
    
inst = RedshiftDataConnectorr()


############## Every one hour running code Start ###########################
# local_tz = pytz.timezone('Asia/Kolkata')
# now = datetime.now(pytz.utc).astimezone(local_tz)

# def get_prev_hour_range(run_dt):
#     # Get start of previous hour in IST
#     prev_hour = (run_dt - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
#     next_hour = prev_hour + timedelta(hours=1)

#     start_date= prev_hour.strftime("%Y-%m-%d")
#     end_date = prev_hour.strftime("%Y-%m-%d")

#     start_time = prev_hour.strftime("%H:%M:%S")
#     end_time = next_hour.strftime("%H:%M:%S")

#     return start_date, end_date, start_time, end_time

# # Get actual query date/time range for the cron run
# start_date, end_date, start_time, end_time = get_prev_hour_range(now)

# # Combine query_date and times to form full TIMESTAMP strings
# start_timestamp = f"{start_date} {start_time}"
# end_timestamp = f"{end_date} {end_time}"

local_tz = pytz.timezone('Asia/Kolkata')
now = datetime.now(pytz.utc).astimezone(local_tz)


def get_prev_5_hours_range(run_dt):
    # End time = exactly 1 hour before the cron run time
    end_time = run_dt - timedelta(hours=1)

    # Start time = 5 hours before the end time
    start_time = end_time - timedelta(hours=4)

    start_date = start_time.strftime("%Y-%m-%d")
    end_date = end_time.strftime("%Y-%m-%d")

    start_time_str = start_time.strftime("%H:%M:%S")
    end_time_str = end_time.strftime("%H:%M:%S")

    return start_date, end_date, start_time_str, end_time_str


# Get actual query date/time range
start_date, end_date, start_time, end_time = get_prev_5_hours_range(now)

# Combine date and time
start_timestamp = f"{start_date} {start_time}"
end_timestamp = f"{end_date} {end_time}"

print("Start Timestamp:", start_timestamp)
print("End Timestamp:", end_timestamp)
############## Every one hour running code Start ###########################


#################### custom date start ######################
# def get_custom_range():
#     now = datetime.now()
    
#     start_date = "2026-09-29"
#     start_time = "07:00:00"
#     end_date = "2026-09-30"
#     end_time = "15:22:00"
    
#     return start_date, start_time, end_date, end_time

# # Get the time range
# start_date, start_time, end_date, end_time = get_custom_range()

# # Combine query_date and times to form full TIMESTAMP strings
# start_timestamp = f"{start_date} {start_time}"
# end_timestamp = f"{end_date} {end_time}"
#################### custom date end ######################

# mongo_client = MongoClient(MONGO_URI)
mongo_db = client["production_db"]
channel_coll = mongo_db["channel_no_ufm"]

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


q1 = f"""SELECT barcode,
       mvarval10, mvargrade10, mvarval11, mvargrade11, mvarval61, mvargrade61, mvarval91, mvargrade91, mvarval62, mvargrade62, 
       mvarval92, mvargrade92, mvarval14, mvargrade14, mvarval15, mvargrade15, mvarval16, mvargrade16, mvarval17, mvargrade17, 
       mvarval121, mvargrade121, mvarval151, mvargrade151, mvarval25, mvargrade25, mvarval18, mvargrade18, mvarval129, mvargrade129, 
       mvarval33, mvargrade33, mvarval181, mvargrade181, mvarval211, mvargrade211, mvarval182, mvargrade182, mvarval212, mvargrade212, 
       mvarval37, mvargrade37, mvarval41, mvargrade41, mvarval241, mvargrade241, mvarval271, mvargrade271, mvarval1, mvargrade1, mvarval9, 
       mvargrade9, mvarval3, mvargrade3, mvarval7, mvargrade7, mvarval2, mvargrade2, mvarval4, mvargrade4, mvarval8, mvargrade8, mvarval27, 
       mvargrade27, mvarval19, mvargrade19, mvarval55, mvargrade55, mvarval46, mvargrade46, mvarval161, mvargrade161, 
       mvarval53, mvargrade53, mvarval280, mvargrade280, mvarval26, mvargrade26, mvarval28, mvargrade28, mvarval20, mvargrade20, mvarval56, 
       mvargrade56, mvarval47, mvargrade47, mvarval54, mvargrade54, mvarval300, mvargrade300, sku_name, attribute1,
       tbm_stage2, ptimebuild, cavity, mold, ptimecure, 
       machinenum, putctime, "class", shift, cycletime, ptime
        FROM db_prd_plant_ap.sc_ufs.t_pertiretu
        WHERE ptime >= '{start_timestamp}'
        AND ptime < '{end_timestamp}'
        AND attribute1 IN ({gt_code_sql})"""
df_uniformity = inst.execute_query(q1)
# df_uniformity.to_csv('df_uniformity_half.csv', escapechar='\\', quoting=csv.QUOTE_NONE, index=False)
# df_uniformity = pd.read_csv("df_uniformity_half.csv")

q2 = f"""SELECT barcode,mvarval25, mvargrade25, mvarval27, mvargrade27, mvarval17, mvargrade17, mvarval19, 
        mvargrade19, mvarval55, mvargrade55, mvarval320, mvargrade320, mvarval46, mvargrade46, mvarval161, 
        mvargrade161, mvarval53, mvargrade53, mvarval280, mvargrade280, mvarval26, mvargrade26, mvarval28, 
        mvargrade28, mvarval18, mvargrade18, mvarval20, mvargrade20, mvarval56, mvargrade56, mvarval340, 
        mvargrade340, mvarval47, mvargrade47, mvarval181, mvargrade181, mvarval54, mvargrade54, mvarval300, 
        mvargrade300,sku_name,attribute1,tbm_stage2,ptimebuild,
        cavity,mold,ptimecure,machinenum,putctime,"class",ptime 
        FROM db_prd_plant_ap.sc_ufs.t_pertiretg
        WHERE ptime >= '{start_timestamp}'
        AND ptime < '{end_timestamp}'
        AND attribute1 IN ({gt_code_sql})"""
df_geometry = inst.execute_query(q2)
# df_geometry.to_csv('df_geometry_half.csv', escapechar='\\', quoting=csv.QUOTE_NONE, index=False)
# df_geometry = pd.read_csv("df_geometry_half.csv")

q3 = f"""
SELECT
    tg.barcode,
    tu.mvarval1, tu.mvargrade1, tu.mvarval9, tu.mvargrade9,
    tu.mvarval3, tu.mvargrade3, tu.mvarval7, tu.mvargrade7,
    tu.mvarval2, tu.mvargrade2, tu.mvarval10, tu.mvargrade10,
    tu.mvarval4, tu.mvargrade4, tu.mvarval8, tu.mvargrade8,
    tg.sku_name, tg.attribute1, tg.tbm_stage2, tg.ptimebuild, tg.cavity, tg.mold, tg.ptimecure, tg.machinenum,
    tg.putctime, tg."class", tg.ptime
FROM db_prd_plant_ap.sc_ufs.t_pertiretg tg
JOIN db_prd_plant_ap.sc_ufs.t_pertiretu tu
    ON tg.barcode = tu.barcode
WHERE tg.ptime >= '{start_timestamp}'
  AND tg.ptime < '{end_timestamp}'
  AND tu.ptime >= '{start_timestamp}'
  AND tu.ptime < '{end_timestamp}'
  AND tg.attribute1 IN ({gt_code_sql})"""

df_balancing = inst.execute_query(q3)
# df_balancing.to_csv('df_balancing.csv', escapechar='\\', quoting=csv.QUOTE_NONE, index=False)
# df_balancing = pd.read_csv("df_balancing.csv")

q4 = f"""
SELECT
    tu.barcode,
    uft.result    AS ufm_testing_result,
    uft."timestamp" AS ufm_testing_result_timestamp,
    vis.result    AS visualinspection_result,
    vis."timestamp" AS visualinspection_time
FROM db_prd_plant_ap.sc_ufs.t_pertiretu tu
JOIN db_prd_plant_ap.sc_cimcorp_pcr.transportationunit tru
    ON tu.barcode = tru.barcode
LEFT JOIN db_prd_plant_ap.sc_cimcorp_pcr.ufmtesting uft
    ON tru.transportationunitid = uft.transportationunitid
LEFT JOIN db_prd_plant_ap.sc_cimcorp_pcr.visualinspection vis
    ON tru.transportationunitid = vis.transportationunitid
WHERE tu.ptime >= '{start_timestamp}'
  AND tu.ptime < '{end_timestamp}'
  AND tu.attribute1 IN ({gt_code_sql})"""

df_cimcorp_results = inst.execute_query(q4)

for _ts_col in ['ufm_testing_result_timestamp', 'visualinspection_time']:
    if _ts_col in df_cimcorp_results.columns:
        df_cimcorp_results[_ts_col] = df_cimcorp_results[_ts_col].astype(str).replace({'NaT': None, 'nan': None})

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

def clean_uniformity_data(df):
    output_messages = []
    invalid_rows = pd.DataFrame()

    # Convert any tz-aware datetime / object columns containing datetime values
    # to plain strings up front — prevents "NaTType does not support utcoffset"
    # which is raised by pd.isna()/pd.notna() on tz-aware NaT objects.
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            try:
                df[col] = df[col].dt.tz_convert(None)
            except TypeError:
                pass
            df[col] = df[col].astype(str).replace('NaT', None)
        elif df[col].dtype == object:
            def _safe_str(x):
                if x is None:
                    return None
                if type(x).__name__ in ('NaTType', 'Timestamp', 'datetime'):
                    try:
                        import pandas as _pd
                        if _pd.isna(x) if not hasattr(x, 'tzinfo') else False:
                            return None
                    except Exception:
                        pass
                    try:
                        return x.strftime('%Y-%m-%d %H:%M:%S') if hasattr(x, 'strftime') else str(x)
                    except Exception:
                        return str(x)
                return x
            try:
                # Only process if column actually contains datetime-like objects
                sample = df[col].dropna()
                if not sample.empty and hasattr(sample.iloc[0], 'tzinfo'):
                    df[col] = df[col].apply(_safe_str)
            except Exception:
                pass
    
    # Fix: Apply has_special_chars to individual values, not the Series
    if 'barcode' in df.columns:
        mask = df['barcode'].isnull() | df['barcode'].apply(lambda x: has_special_chars(x) if pd.notna(x) else False)
        invalid_rows = df[mask].copy()
        df_clean = df[~mask].copy()
    else:
        df_clean = df.copy()
        output_messages.append("Warning: 'barcode' column not found in DataFrame")
    
    # Remove duplicates
    df_clean = df_clean.drop_duplicates()
    
    # Drop unwanted columns
    cols_to_drop = ['rn', 'Unnamed: 226', 'Unnamed: 225']
    existing_cols_to_drop = [col for col in cols_to_drop if col in df_clean.columns]
    df_clean.drop(columns=existing_cols_to_drop, inplace=True, errors='ignore')
    
    # Clean column names
    df_clean.columns = (
        df_clean.columns
        .to_series()
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace(' ', '_')
        .str.replace(r'[^\w_]', '', regex=True)  # Remove special characters from column names
    )
    
    # Clean string values - only apply to string columns
    string_columns = df_clean.select_dtypes(include=['object']).columns
    for col in string_columns:
        df_clean[col] = df_clean[col].apply(
            lambda x: x.strip() if isinstance(x, str) else x
        )
    
    # Clean barcode if present
    if 'barcode' in df_clean.columns:
        df_clean['barcode'] = (
            df_clean['barcode']
            .astype(str)
            .apply(lambda x: x.rstrip('0').rstrip('.') if isinstance(x, str) else x)
        )
    
    # Rename specified columns
    rename_columns_mapping = {
        'mvarval10':   'rfpp_cw_kg',
        'mvargrade10': 'tu_rfpp_cw_grade',
        'mvarval11':   'rfpp_ccw_kg',
        'mvargrade11': 'tu_rfpp_ccw_grade',
        'mvarval61':   'rfh1_cw_kg',
        'mvargrade61': 'tu_rfh1_cw_grade',
        'mvarval91':   'rfh1_ccw_kg',
        'mvargrade91': 'tu_rfh1_ccw_grade',
        'mvarval62':   'rfh2_cw_kg',
        'mvargrade62': 'tu_rfh2_cw_grade',
        'mvarval92':   'rfh2_ccw_kg',
        'mvargrade92': 'tu_rfh2_ccw_grade',
        'mvarval14':   'lfpp_cw_kg',
        'mvargrade14': 'tu_lfpp_cw_grade',
        'mvarval15':   'lfpp_ccw_kg',
        'mvargrade15': 'tu_lfpp_ccw_grade',
        'mvarval16':   'lsft_cw_kg',
        'mvargrade16': 'tu_lsft_cw_grade',
        'mvarval17':   'lsft_ccw_kg',
        'mvargrade17': 'tu_lsft_ccw_grade',
        'mvarval121':  'lfh1_cw_kg',
        'mvargrade121':'tu_lfh1_cw_grade',
        'mvarval151':  'lfh1_ccw_kg',
        'mvargrade151':'tu_lfh1_ccw_grade',
        'mvarval25':   'plysteer_kg',
        'mvargrade125':'tu_plysteer_grade',
        'mvarval18':   'cony_kg',
        'mvargrade18': 'tu_cony_grade',
        'mvarval29':   'rfmxa_cw_deg',
        'mvargrade29': 'tu_rfmxa_cw_grade',
        'mvarval33':   'rfmxa_ccw_deg',
        'mvargrade33': 'tu_rfmxa_ccw_grade',
        'mvarval181':  'rfa1_cw_deg',
        'mvargrade181':'tu_rfa1_cw_grade',
        'mvarval211':  'rfa1_ccw_deg',
        'mvargrade211':'tu_rfa1_ccw_grade',
        'mvarval182':  'rfa2_cw_deg',
        'mvargrade182':'tu_rfa2_cw_grade',
        'mvarval212':  'rfa2_ccw_deg',
        'mvargrade212':'tu_rfa2_ccw_grade',
        'mvarval37':   'lfmxa_cw_deg',
        'mvargrade137':'tu_lfmxa_cw_grade',
        'mvarval41':   'lfmxa_ccw_deg',
        'mvargrade141':'tu_lfmxa_ccw_grade',
        'mvarval241':  'lfa1_cw_deg',
        'mvargrade241':'tu_lfa1_cw_grade',
        'mvarval271':  'lfa1_ccw_deg',
        'mvargrade271':'tu_lfa1_ccw_grade',
        'mvarval1':    'botplane_g',
        'mvargrade1':  'tu_botplane_grade',
        'mvarval9':    'topplane_g',
        'mvargrade9':  'tu_topplane_grade',
        'mvarval3':    'couple_g',
        'mvargrade3':  'tu_couple_grade',
        'mvarval7':    'static_g',
        'mvargrade7':  'tu_static_grade',
        'mvarval2':    'botplane_deg',
        'mvarval4':    'couple_deg',
        'mvarval8':    'static_deg',
        'mvarval27':   'botdep_mm',
        'mvarval19':   'topdep_mm',
        'mvarval55':   'blro_mm',
        'mvarval320':  'blroh1_mm',
        'mvarval46':   'crro_mm',
        'mvarval161':  'crroh1_mm',
        'mvarval53':   'tlro_mm',
        'mvarval280':  'tlroh1_mm',
        'mvarval26':   'botbul_deg',
        'mvarval28':   'botdep_deg',
        'mvarval20':   'topdep_deg',
        'mvarval56':   'blro_deg',
        'mvarval340':  'blroh1_deg',
        'mvarval47':   'crro_deg',
        'mvarval54':   'tlro_deg',
        'mvarval300':  'tlroh1_deg',
        'sku_name':    'sku',
        'attribute1':  'gt_code',
        'tbm_stage2':  'tbm',
        'ptimebuild':  'tbm_buildtime',
        'cavity':      'curing_press',
        'mold':        'curing_mouldid',
        'ptimecure':   'curing_infeedtime',
        'machinenum':  'ubno',
        'putctime':    'date_of_utc',
        'class':       'tu_class',
        'ptime':       'date_of_ufm',
        'cycletime':   'tu_cycletime'
    }
    
    # Only rename columns that exist
    existing_rename_mapping = {k: v for k, v in rename_columns_mapping.items() if k in df_clean.columns}
    df_clean.rename(columns=existing_rename_mapping, inplace=True)
    
    # Clean tbm if present
    if 'tbm' in df_clean.columns:
        df_clean['tbm'] = (
            df_clean['tbm']
            .astype(str)
            .apply(lambda x: x.rstrip('0').rstrip('.') if isinstance(x, str) else x)
        )
    
    # Define column types
    column_types = {
        'barcode': 'str',
        'gt_code': 'str',
        "sku": 'str',
        'tbm': 'str',
        'curing_press': 'str',
        'curing_mouldid': 'str',
        'ubno': 'str',
        'rfpp_cw_kg': 'float64',
        'tu_rfpp_cw_grade': 'str',
        'rfpp_ccw_kg': 'float64',
        'tu_rfpp_ccw_grade': 'str',
        'rfh1_cw_kg': 'float64',
        'tu_rfh1_cw_grade': 'str',
        'rfh1_ccw_kg': 'float64',
        'tu_rfh1_ccw_grade': 'str',
        'rfh2_cw_kg': 'float64',
        'tu_rfh2_cw_grade': 'str',
        'rfh2_ccw_kg': 'float64',
        'tu_rfh2_ccw_grade': 'str',
        'lfpp_cw_kg': 'float64',
        'tu_lfpp_cw_grade': 'str',
        'lfpp_ccw_kg': 'float64',
        'tu_lfpp_ccw_grade': 'str',
        'lsft_cw_kg': 'float64',
        'tu_lsft_cw_grade': 'str',
        'lsft_ccw_kg': 'float64',
        'tu_lsft_ccw_grade': 'str',
        'lfh1_cw_kg': 'float64',
        'tu_lfh1_cw_grade': 'str',
        'lfh1_ccw_kg': 'float64',
        'tu_lfh1_ccw_grade': 'str',
        'plysteer_kg': 'float64',
        'tu_plysteer_grade': 'str',
        'cony_kg': 'float64',
        'tu_cony_grade': 'str',
        'rfmxa_cw_deg': 'float64',
        'tu_rfmxa_cw_grade': 'str',
        'rfmxa_ccw_deg': 'float64',
        'tu_rfmxa_ccw_grade': 'str',
        'rfa1_cw_deg': 'float64',
        'tu_rfa1_cw_grade': 'str',
        'rfa1_ccw_deg': 'float64',
        'tu_rfa1_ccw_grade': 'str',
        'rfa2_cw_deg': 'float64',
        'tu_rfa2_cw_grade': 'str',
        'rfa2_ccw_deg': 'float64',
        'tu_rfa2_ccw_grade': 'str',
        'lfmxa_cw_deg': 'float64',
        'tu_lfmxa_cw_grade': 'str',
        'lfmxa_ccw_deg': 'float64',
        'tu_lfmxa_ccw_grade': 'str',
        'lfa1_cw_deg': 'float64',
        'tu_lfa1_cw_grade': 'str',
        'lfa1_ccw_deg': 'float64',
        'tu_lfa1_ccw_grade': 'str',
        'tu_class': 'str',
        'tu_cycletime': 'float64'
    }
    
    # Keep only barcode and renamed columns
    columns_to_keep = ['barcode'] + [new_col for new_col in rename_columns_mapping.values()]
    existing_columns_to_keep = [col for col in columns_to_keep if col in df_clean.columns]
    df_clean = df_clean[existing_columns_to_keep]
    
    # Clean specific columns
    for col in ['attribute1', 'gt_code', 'ubno']:
        if col in df_clean.columns:
            df_clean[col] = (
                df_clean[col]
                .astype(str)
                .apply(lambda x: x.rstrip('0').rstrip('.') if isinstance(x, str) else x)
            )
    
    # Handle date column
    if 'date' in df_clean.columns:
        df_clean['date'] = pd.to_datetime(df_clean['date'], errors='coerce')
        df_clean['date'] = df_clean['date'].dt.strftime('%Y-%m-%d')
    
    # Apply column types only for existing columns
    existing_column_types = {col: dtype for col, dtype in column_types.items() if col in df_clean.columns}
    
    for col, dtype in existing_column_types.items():
        try:
            if col in df_clean.columns:
                if dtype == 'float64':
                    df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
                elif dtype == 'str':
                    df_clean[col] = df_clean[col].astype(str)
        except Exception as e:
            output_messages.append(f"Warning: Could not convert column {col} to {dtype}: {e}")
    
    # Generate lot IDs
    if 'gt_code' in df_clean.columns:
        # Make sure gt_code is string
        df_clean['gt_code'] = df_clean['gt_code'].astype(str)
        
        # Create lot_id and previous_lot_id
        df_clean['lot_id'] = df_clean['gt_code'] + '_' + df_clean.groupby('gt_code').cumcount().astype(str)
        df_clean['previous_lot_id'] = df_clean['gt_code'] + '_' + (df_clean.groupby('gt_code').cumcount() - 1).astype(str)
        
        # Set previous_lot_id to None for first item in each group
        mask = df_clean['previous_lot_id'].str.contains('_-1', na=False)
        df_clean.loc[mask, 'previous_lot_id'] = None
        
        # Initialize machine model columns
        machine_columns = [
            "tbm_machine_model_lot_id",
            "tbm_previous_machine_model_lot_id",
            "curing_machine_model_lot_id",
            "curing_previous_machine_model_lot_id",
            "ufm_machine_model_lot_id",
            "ufm_previous_machine_model_lot_id"
        ]
        
        for col in machine_columns:
            if col not in df_clean.columns:
                df_clean[col] = None
    
    return df_clean, invalid_rows, output_messages

def clean_balancing_data(df):
    """
    Clean and process balancing data DataFrame while preserving original column dtypes
    where possible and only converting when necessary for cleaning.
    """
    output_messages = []
    invalid_barcode_rows = pd.DataFrame()
    duplicates_df = pd.DataFrame()

    try:
        # 1. Clean column names
        df.columns = [
            re.sub(r'[.\s]+', '_', col.strip()).lower()
            for col in df.columns
        ]

        # 2. Rename columns (only if they exist)
        rename_columns_mapping = {
            'mvarval1':    'botplane_g',
            'mvargrade1':  'tb_botplane_grade',
            'mvarval9':    'topplane_g',
            'mvargrade9':  'tb_topplane_grade',
            'mvarval3':    'couple_g',
            'mvargrade3':  'tb_couple_grade',
            'mvarval7':    'static_g',
            'mvargrade7':  'tb_static_grade',
            'mvarval2':    'botplane_deg',
            'mvargrade2':  'tb_botplane_deg_grade',
            'mvarval10':   'topplane_deg',
            'mvargrade10': 'tb_topplane_deg_grade',
            'mvarval4':    'couple_deg',
            'mvargrade4':  'tb_couple_deg_grade',
            'mvarval8':    'static_deg',
            'mvargrade8':  'tb_static_deg_grade',
            'sku_name':    'sku',
            'attribute1':  'gt_code',
            'tbm_stage2':  'tbm',
            'ptimebuild':  'tbm_buildtime',
            'cavity':      'curing_press',
            'mold':        'curing_mouldid',
            'ptimecure':   'curing_infeedtime',
            'machinenum':  'ubno',
            'putctime':    'date_of_utc',
            'class':       'tb_class',
            'ptime':       'date_of_ufm'
        }

        rename_dict = {k: v for k, v in rename_columns_mapping.items() if k in df.columns}
        if rename_dict:
            df.rename(columns=rename_dict, inplace=True)

        # 3. Convert ALL columns to appropriate types - FIXED
        # Define which columns should be strings vs numeric
        string_columns = [
            'sku', 'gt_code', 'tbm', 'tbm_buildtime', 'curing_press',
            'curing_mouldid', 'curing_infeedtime', 'ubno', 'date_of_utc',
            'tb_class', 'barcode', 'date_of_ufm',
            # grade columns are strings like 'A01', 'E05' — keep as str
            'tb_botplane_grade', 'tb_topplane_grade', 'tb_couple_grade', 'tb_static_grade',
            'tb_botplane_deg_grade', 'tb_topplane_deg_grade', 'tb_couple_deg_grade', 'tb_static_deg_grade',
        ]

        numeric_columns = [
            'botplane_g', 'topplane_g', 'couple_g', 'static_g',
            'botplane_deg', 'topplane_deg', 'couple_deg', 'static_deg',
        ]
        
        # Convert string columns
        for col in string_columns:
            if col in df.columns:
                # Convert to string, handling NaN/None values
                df[col] = df[col].astype(str).fillna('')
        
        # Convert numeric columns
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 4. Clean identifiers
        def clean_identifier(s):
            if not isinstance(s, str):
                return s
            s = s.strip()
            # Remove trailing .0 or .000 etc.
            if '.' in s:
                s = s.rstrip('0').rstrip('.')
            return s if s else ''

        # Clean specific string columns
        for col in ['ubno', 'barcode', 'tbm', 'gt_code']:
            if col in df.columns:
                df[col] = df[col].apply(clean_identifier)

        # 5. Strip whitespace from all string columns
        string_cols = df.select_dtypes(include=['object']).columns
        for col in string_cols:
            df[col] = df[col].str.strip().replace({'': None})

        # 6. Validate barcode
        if 'barcode' in df.columns:
            # Convert barcode to string for validation
            bc_clean = df['barcode'].astype(str).replace('nan', '').str.strip()
            
            special_char_regex = re.compile(r'[^0-9]')
            
            null_or_empty = bc_clean == ''
            has_special = bc_clean.apply(lambda x: bool(special_char_regex.search(x)) if x else False)
            invalid_mask = null_or_empty | has_special

            invalid_barcode_rows = df[invalid_mask].copy()
            df = df[~invalid_mask].copy()

            if not invalid_barcode_rows.empty:
                output_messages.append(f"Removed {len(invalid_barcode_rows)} rows with invalid barcode")

        # 7. Drop empty columns
        df = df.dropna(axis=1, how='all')
        
        # 8. Remove duplicates
        duplicates_df = df[df.duplicated(keep=False)].copy()
        if not duplicates_df.empty:
            df = df.drop_duplicates().copy()
            output_messages.append(f"Removed {len(duplicates_df) - len(df)} duplicate rows")

        output_messages.append("BALANCING Cleaning successful")
        logging.info("clean_balancing_data function completed successfully")

        return df, invalid_barcode_rows, duplicates_df, output_messages

    except Exception as e:
        error_message = f"Error during cleaning: {str(e)}"
        output_messages.append(error_message)
        logging.error(error_message)
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), output_messages

def clean_geometry_data(df):
    output_messages = []
    invalid_barcode_rows = pd.DataFrame()
    duplicates_df = pd.DataFrame()
    try:
        if 'barcode' not in df.columns:
            output_messages.append("Error: 'barcode' column not found in geometry data")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), output_messages

        # 1. Clean column names
        df.columns = [re.sub(r'[.\s]+', '_', col.strip()).lower() for col in df.columns]
        
        # 2. Rename columns
        rename_columns_mapping = {
            'mvarval25':    'botbul_mm',
            'mvargrade25':  'tg_botbul_grade',
            'mvarval27':    'botdep_mm',
            'mvargrade27':  'tg_botdep_grade',
            'mvarval17':    'topbul_mm',
            'mvargrade17':  'tg_topbul_grade',
            'mvarval19':    'topdep_mm',
            'mvargrade19':  'tg_topdep_grade',
            'mvarval55':    'blro_mm',
            'mvargrade55':  'tg_blro_grade',
            'mvarval320':   'blroh1_mm',
            'mvargrade320': 'tg_blroh1_grade',
            'mvarval46':    'crro_mm',
            'mvargrade46':  'tg_crro_grade',
            'mvarval161':   'crroh1_mm',
            'mvargrade161': 'tg_crroh1_grade',
            'mvarval53':    'tlro_mm',
            'mvargrade53':  'tg_tlro_grade',
            'mvarval280':   'tlroh1_mm',
            'mvargrade280': 'tg_tlroh1_grade',
            'mvarval26':    'botbul_deg',
            'mvargrade26':  'tg_botbul_deg_grade',
            'mvarval28':    'botdep_deg',
            'mvargrade28':  'tg_botdep_deg_grade',
            'mvarval18':    'topbul_deg',
            'mvargrade18':  'tg_topbul_deg_grade',
            'mvarval20':    'topdep_deg',
            'mvargrade20':  'tg_topdep_deg_grade',
            'mvarval56':    'blro_deg',
            'mvargrade56':  'tg_blro_deg_grade',
            'mvarval340':   'blroh1_deg',
            'mvargrade340': 'tg_blroh1_deg_grade',
            'mvarval47':    'crro_deg',
            'mvargrade47':  'tg_crro_deg_grade',
            'mvarval181':   'crroh1_deg',
            'mvargrade181': 'tg_crroh1_deg_grade',
            'mvarval54':    'tlro_deg',
            'mvargrade54':  'tg_tlro_deg_grade',
            'mvarval300':   'tlroh1_deg',
            'mvargrade300': 'tg_tlroh1_deg_grade',
            'sku_name':     'sku',
            'attribute1':   'gt_code',
            'tbm_stage2':   'tbm',
            'ptimebuild':   'tbm_buildtime',
            'cavity':       'curing_press',
            'mold':         'curing_mouldid',
            'ptimecure':    'curing_infeedtime',
            'machinenum':   'ubno',
            'putctime':     'date_of_utc',
            'class':        'tg_class',
            'ptime':        'date_of_ufm',
        }
        
        # Apply renaming
        for old_name, new_name in rename_columns_mapping.items():
            if old_name in df.columns:
                df.rename(columns={old_name: new_name}, inplace=True)
        
        # 3. Define column types
        string_columns = [
            'sku', 'gt_code', 'tbm', 'tbm_buildtime', 'curing_press',
            'curing_mouldid', 'curing_infeedtime', 'ubno', 'date_of_utc',
            'tg_class', 'barcode', 'date_of_ufm',
            # grade columns are strings like 'A01', 'E05'
            'tg_botbul_grade', 'tg_botdep_grade', 'tg_topbul_grade', 'tg_topdep_grade',
            'tg_blro_grade', 'tg_blroh1_grade', 'tg_crro_grade', 'tg_crroh1_grade', 'tg_tlro_grade', 'tg_tlroh1_grade',
            'tg_botbul_deg_grade', 'tg_botdep_deg_grade', 'tg_topbul_deg_grade', 'tg_topdep_deg_grade',
            'tg_blro_deg_grade', 'tg_blroh1_deg_grade', 'tg_crro_deg_grade', 'tg_crroh1_deg_grade',
            'tg_tlro_deg_grade', 'tg_tlroh1_deg_grade',
        ]

        measurement_columns = [
            'botbul_mm', 'botdep_mm', 'topbul_mm', 'topdep_mm',
            'blro_mm', 'blroh1_mm', 'crro_mm', 'crroh1_mm', 'tlro_mm', 'tlroh1_mm',
            'botbul_deg', 'botdep_deg', 'topbul_deg', 'topdep_deg',
            'blro_deg', 'blroh1_deg', 'crro_deg', 'crroh1_deg', 'tlro_deg', 'tlroh1_deg',
        ]
        
        # 4. Convert string columns
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).fillna('')
        
        # 5. Convert measurement columns to numeric
        for col in measurement_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 6. Clean identifiers
        def clean_identifier(s):
            if not isinstance(s, str):
                return s
            s = s.strip()
            if '.' in s:
                s = s.rstrip('0').rstrip('.')
            return s if s else ''
        
        for col in ['barcode', 'ubno', 'tbm', 'gt_code']:
            if col in df.columns:
                df[col] = df[col].apply(clean_identifier)
        
        # 7. Strip whitespace from string columns only
        string_cols = df.select_dtypes(include=['object']).columns
        for col in string_cols:
            df[col] = df[col].str.strip().replace({'': None})
        
        # 8. Validate barcode
        if 'barcode' in df.columns:
            bc_clean = df['barcode'].astype(str).replace('nan', '').str.strip()
            
            special_char_regex = re.compile(r'[^0-9]')
            
            null_or_empty = bc_clean == ''
            has_special = bc_clean.apply(lambda x: bool(special_char_regex.search(x)) if x else False)
            invalid_mask = null_or_empty | has_special
            
            invalid_barcode_rows = df[invalid_mask].copy()
            df = df[~invalid_mask].copy()
            
            if not invalid_barcode_rows.empty:
                output_messages.append(f"Removed {len(invalid_barcode_rows)} rows with invalid barcode")
        
        # 9. Drop empty columns
        df = df.dropna(axis=1, how='all')
        
        # 10. Remove duplicates
        duplicates_df = df[df.duplicated(keep=False)].copy()
        if not duplicates_df.empty:
            df = df.drop_duplicates().copy()
            output_messages.append(f"Removed {len(duplicates_df) - len(df)} duplicate rows")
        
        output_messages.append("GEOMETRY Cleaning successful")
        return df, invalid_barcode_rows, duplicates_df, output_messages
    except Exception as e:
        error_message = f"Error during cleaning: {e}"
        output_messages.append(error_message)
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), output_messages

def insert_cleaned_data(db_client, cleaned_df, database_name, filename):
    output_messages = []
    # NOTE: The cleaned databases 'Data_Clean_UNIFORMITY', 'Data_Clean_BALANCING' and
    # 'Data_Clean_GEOMETRY' are intentionally NOT created/populated. The cleaning still
    # runs and its results flow downstream via the combined collections; only the
    # persistence into the Data_Clean_* databases is skipped here.
    output_messages.append(f"Skipped saving cleaned {database_name} data to Data_Clean_{database_name} (persistence disabled).")
    return output_messages
    try:
        if cleaned_df.empty:
            output_messages.append("No valid data to save after cleaning.")
            return output_messages
        
        timestamp = some_function().strftime("%Y%m%d_%H%M%S")
        
        if database_name == "UNIFORMITY":
            cleaned_collection_name = f"{UNIFORMITY_CLEANED_PREFIX}_{timestamp}"
            target_db_name = Data_Clean_UNIFORMITY  # Fixed database name
        elif database_name == "BALANCING":
            cleaned_collection_name = f"{BALANCING_CLEANED_PREFIX}_{timestamp}"
            target_db_name = Data_Clean_BALANCING  # Fixed database name
        elif database_name == "GEOMETRY":
            cleaned_collection_name = f"{GEOMETRY_CLEANED_PREFIX}_{timestamp}"
            target_db_name = Data_Clean_GEOMETRY  # Fixed database name
        else:
            error_message = f"Unknown database: {database_name}. Cannot insert cleaned data."
            output_messages.append(error_message)
            return output_messages
        
        # Convert DataFrame to dictionary records
        data_to_insert = cleaned_df.to_dict(orient="records")
        
        # Insert into MongoDB
        db_client[target_db_name][cleaned_collection_name].insert_many(data_to_insert)
        
        success_message = f"Successfully inserted {len(data_to_insert)} records to {target_db_name}.{cleaned_collection_name}"
        output_messages.append(success_message)
        
        return output_messages
    except Exception as e:
        error_message = f"An error occurred during insert cleaned data: {e}"
        output_messages.append(error_message)
        return output_messages

def track_file_insertion(filename, db_name, num_rows, collection_name):
    tracking_entry = {
        "filename": filename,
        "database_name": db_name,
        "collection_name": collection_name,
        "num_rows": num_rows,
        "inserted_timestamp": some_function(),
        "processed": False
    }
    client[FILE_TRACKING_DB][FILE_TRACKING_COLLECTION].insert_one(tracking_entry)

def remove_duplicates_collection(collection):
    output_messages = []
    records = list(collection.find({}, {"_id": 0}))

    if records:
        df = pd.DataFrame(records)
        df_unique = df.drop_duplicates()

        if len(df) > len(df_unique):
            message = f"🔍 Found and removing {len(df) - len(df_unique)} duplicate records from {collection.name}."
            output_messages.append(message)

            collection.delete_many({})
            collection.insert_many(df_unique.to_dict(orient="records"))
            success_message = f"✅ Cleaned and reinserted {len(df_unique)} unique records in {collection.name}."
            output_messages.append(success_message)
        else:
            output_messages.append(f"✅ No duplicates found in {collection.name}.")
    else:
        output_messages.append(f"⚠️ No records found in {collection.name}.")
    return output_messages

def process_data_for_api(db_client, source_data, database_name, filename):
    output_messages = []
    cleaned_collection_name = None
    invalid_collection_name = None
    try:
        df = pd.DataFrame(list(source_data))

        # Strip timezone info from any tz-aware datetime columns to avoid
        # "NaTType does not support utcoffset" errors during processing
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                try:
                    df[col] = df[col].dt.tz_localize(None)
                except TypeError:
                    df[col] = df[col].dt.tz_convert(None)

        if database_name == "UNIFORMITY":
            cleaned_df, invalid_df, uniformity_messages = clean_uniformity_data(df.copy())
            output_messages.extend(uniformity_messages)

            cleaned_collection_name = UNIFORMITY_CLEANED_PREFIX + filename.replace(".csv", "").replace(".xlsx", "")
            invalid_collection_name = "invalid_data_UNIFORMITY_" + filename.replace(".csv", "").replace(".xlsx", "")

            if not invalid_df.empty:
                invalid_df = invalid_df.drop('_id', axis=1, errors='ignore')
                invalid_collect = db_client[INVALID_ROWS_DB][invalid_collection_name]
                output_messages.extend(remove_duplicates_collection(invalid_collect))
                db_client[INVALID_ROWS_DB][invalid_collection_name].insert_many(invalid_df.to_dict("records"))
                output_messages.append(f"Invalid UNIFORMITY data saved to invalid_data_UNIFORMITY.{invalid_collection_name}")
            else:
                output_messages.append("No invalid UNIFORMITY data to save.")

            output_messages.extend(insert_cleaned_data(db_client, cleaned_df, database_name, filename))

            if not cleaned_df.empty:
                cleaned_df = cleaned_df.drop('_id', axis=1, errors='ignore')
                # CHANGE 1: Delete all existing data in combined UNIFORMITY collection before insert
                combined_uniformity_collection = db_client[COMBINED_Database][ALL_COMBINED_UNIFORMITY_COLLECTION]
                delete_result = combined_uniformity_collection.delete_many({})
                output_messages.append(f"Deleted {delete_result.deleted_count} existing documents from {COMBINED_Database}.{ALL_COMBINED_UNIFORMITY_COLLECTION}")
                
                combined_uniformity_collection.insert_many(cleaned_df.to_dict("records"))
                output_messages.append(f"Cleaned UNIFORMITY data pushed to {COMBINED_Database}.{ALL_COMBINED_UNIFORMITY_COLLECTION}")
            else:
                output_messages.append("No cleaned UNIFORMITY data to push to combined collection")

        elif database_name == "BALANCING":
            cleaned_df, invalid_barcode_rows, duplicates_df, balancing_messages = clean_balancing_data(df.copy())
            output_messages.extend(balancing_messages)

            cleaned_collection_name = BALANCING_CLEANED_PREFIX + filename.replace(".csv", "").replace(".xlsx", "")
            invalid_collection_name = "invalid_data_BALANCING_" + filename.replace(".csv", "").replace(".xlsx", "")

            if not invalid_barcode_rows.empty:
                invalid_barcode_rows = invalid_barcode_rows.drop('_id', axis=1, errors='ignore')
                invalid_collect = db_client[INVALID_ROWS_DB][invalid_collection_name]
                output_messages.extend(remove_duplicates_collection(invalid_collect))
                db_client[INVALID_ROWS_DB][invalid_collection_name].insert_many(invalid_barcode_rows.to_dict("records"))
                output_messages.append(f"Invalid BALANCING data saved to invalid_data_BALANCING.{invalid_collection_name}")
            else:
                output_messages.append("No invalid barcode BALANCING data to save.")

            if not duplicates_df.empty:
                duplicate_collection_name = f"duplicate_BALANCING_{filename.replace('.csv', '').replace('.xlsx', '')}"
                duplicates_df = duplicates_df.drop('_id', axis=1, errors='ignore')
                db_client[INVALID_ROWS_DB][duplicate_collection_name].insert_many(duplicates_df.to_dict("records"))
                output_messages.append(f"Duplicate BALANCING data saved to {INVALID_ROWS_DB}.{duplicate_collection_name}")
            else:
                output_messages.append("No duplicate BALANCING data to save.")

            output_messages.extend(insert_cleaned_data(db_client, cleaned_df, database_name, filename))

            if not cleaned_df.empty:
                cleaned_df = cleaned_df.drop('_id', axis=1, errors='ignore')
                # CHANGE 1: Delete all existing data in combined BALANCING collection before insert
                combined_balancing_collection = db_client[COMBINED_Database][ALL_COMBINED_BALANCING_COLLECTION]
                delete_result = combined_balancing_collection.delete_many({})
                output_messages.append(f"Deleted {delete_result.deleted_count} existing documents from {COMBINED_Database}.{ALL_COMBINED_BALANCING_COLLECTION}")
                
                combined_balancing_collection.insert_many(cleaned_df.to_dict("records"))
                output_messages.append(f"Cleaned BALANCING data pushed to {COMBINED_Database}.{ALL_COMBINED_BALANCING_COLLECTION}")
            else:
                output_messages.append("No cleaned BALANCING data to push to combined collection")
                
        elif database_name == "GEOMETRY":
            cleaned_df, invalid_barcode_rows, duplicates_df, geometry_messages = clean_geometry_data(df.copy())
            output_messages.extend(geometry_messages)

            cleaned_collection_name = GEOMETRY_CLEANED_PREFIX + filename.replace(".csv", "").replace(".xlsx", "")
            invalid_collection_name = "invalid_data_GEOMETRY_" + filename.replace(".csv", "").replace(".xlsx", "")

            if not invalid_barcode_rows.empty:
                invalid_barcode_rows = invalid_barcode_rows.drop('_id', axis=1, errors='ignore')
                invalid_collect = db_client[INVALID_ROWS_DB][invalid_collection_name]
                output_messages.extend(remove_duplicates_collection(invalid_collect))
                db_client[INVALID_ROWS_DB][invalid_collection_name].insert_many(invalid_barcode_rows.to_dict("records"))
                output_messages.append(f"Invalid GEOMETRY data saved to invalid_data_GEOMETRY.{invalid_collection_name}")
            else:
                output_messages.append("No invalid barcode GEOMETRY data to save.")

            if not duplicates_df.empty:
                duplicate_collection_name = f"duplicate_GEOMETRY_{filename.replace('.csv', '').replace('.xlsx', '')}"
                duplicates_df = duplicates_df.drop('_id', axis=1, errors='ignore')
                db_client[INVALID_ROWS_DB][duplicate_collection_name].insert_many(duplicates_df.to_dict("records"))
                output_messages.append(f"Duplicate GEOMETRY data saved to {INVALID_ROWS_DB}.{duplicate_collection_name}")
            else:
                output_messages.append("No duplicate GEOMETRY data to save.")

            output_messages.extend(insert_cleaned_data(db_client, cleaned_df, database_name, filename))

            if not cleaned_df.empty:
                cleaned_df = cleaned_df.drop('_id', axis=1, errors='ignore')
                # CHANGE 1: Delete all existing data in combined BALANCING collection before insert
                combined_geometry_collection = db_client[COMBINED_Database][ALL_COMBINED_GEOMETRY_COLLECTION]
                delete_result = combined_geometry_collection.delete_many({})
                output_messages.append(f"Deleted {delete_result.deleted_count} existing documents from {COMBINED_Database}.{ALL_COMBINED_GEOMETRY_COLLECTION}")
                
                combined_geometry_collection.insert_many(cleaned_df.to_dict("records"))
                output_messages.append(f"Cleaned BALANCING data pushed to {COMBINED_Database}.{ALL_COMBINED_GEOMETRY_COLLECTION}")
            else:
                output_messages.append("No cleaned BALANCING data to push to combined collection")

        else:
            error_message = f"Unknown database: {database_name}"
            output_messages.append(error_message)
            return output_messages, None, None

    except Exception as e:
        error_message = f"Error processing data: {e}"
        output_messages.append(error_message)
        return output_messages, None, None

    return output_messages, cleaned_collection_name, invalid_collection_name

def get_unprocessed_files_api():
    db = client[FILE_TRACKING_DB]
    files_to_process = list(db[FILE_TRACKING_COLLECTION].find({"processed": False}).sort([("inserted_timestamp", -1)]))
    return files_to_process

def copy_to_single_collection_api():
    output_messages = []

    try:
        source_db = client[COMBINED_Database]
        target_db = client[UNIFORMITY_BALANCING_GEOMETRY_DB]

        # Retrieve data from source collections
        try:
            uniformity_data = list(source_db[ALL_COMBINED_UNIFORMITY_COLLECTION].find())
            output_messages.append(f"Retrieved {len(uniformity_data)} records from {ALL_COMBINED_UNIFORMITY_COLLECTION}")
            uniformity_df = pd.DataFrame(uniformity_data)
        except Exception as e:
            output_messages.append(f"Error retrieving data from {ALL_COMBINED_UNIFORMITY_COLLECTION}: {str(e)}")
            return output_messages

        try:
            balancing_data = list(source_db[ALL_COMBINED_BALANCING_COLLECTION].find())
            output_messages.append(f"Retrieved {len(balancing_data)} records from {ALL_COMBINED_BALANCING_COLLECTION}")
            balancing_df = pd.DataFrame(balancing_data)
        except Exception as e:
            output_messages.append(f"Error retrieving data from {ALL_COMBINED_BALANCING_COLLECTION}: {str(e)}")
            balancing_df = pd.DataFrame()

        try:
            geometry_data = list(source_db[ALL_COMBINED_GEOMETRY_COLLECTION].find())
            output_messages.append(f"Retrieved {len(geometry_data)} records from {ALL_COMBINED_GEOMETRY_COLLECTION}")
            geometry_df = pd.DataFrame(geometry_data)
        except Exception as e:
            output_messages.append(f"Error retrieving data from {ALL_COMBINED_GEOMETRY_COLLECTION}: {str(e)}")
            geometry_df = pd.DataFrame()

        if uniformity_df.empty:
            output_messages.append("UNIFORMITY data is empty. No data to process.")
            return output_messages
        
        db = client[COMBINED_Database]
        all_combined_balancing_collection = db[ALL_COMBINED_BALANCING_COLLECTION]
        all_combined_geometry_collection = db[ALL_COMBINED_GEOMETRY_COLLECTION]

        try:
            # Fetch balancing records
            balancing_data = list(all_combined_balancing_collection.find(
                {}, {"barcode": 1, "topplane_deg": 1, "_id": 0}
            ))

            # Fetch geometry records
            geometry_data = list(all_combined_geometry_collection.find(
                {}, {"barcode": 1, "topbul_mm": 1, "botbul_mm": 1, "topbul_deg": 1, "crroh1_deg": 1, "_id": 0}
            ))

            # Convert geometry data into dict for quick lookup by barcode
            geometry_dict = {rec["barcode"]: rec for rec in geometry_data if "barcode" in rec}

            # Merge balancing + geometry data by barcode
            cimcorp_records = []
            for bal in balancing_data:
                barcode = bal.get("barcode")
                merged = bal.copy()
                if barcode in geometry_dict:
                    merged.update(geometry_dict[barcode])
                cimcorp_records.append(merged)

            if not cimcorp_records:
                output_messages.append("Warning: No records found in both collections.")

        except Exception as e:
            output_messages.append(f"Error fetching data from MongoDB: {e}")
            cimcorp_records = []

        # Create lookup dictionaries from MongoDB records
        topplane_deg_lookup = {}
        topbul_mm_lookup = {}
        botbul_mm_lookup = {}
        topbul_deg_lookup = {}
        crroh1_deg_lookup = {}
 
        for rec in cimcorp_records:
            barcode = rec.get('barcode')
            if barcode is None:
                continue
 
            topplane_deg_lookup[barcode] = rec.get('topplane_deg')
            topbul_mm_lookup[barcode] = rec.get('topbul_mm')
            botbul_mm_lookup[barcode] = rec.get('botbul_mm')
            topbul_deg_lookup[barcode] = rec.get('topbul_deg')
            crroh1_deg_lookup[barcode] = rec.get('crroh1_deg')
 
        # Map values to uniformity_df
        try:
            uniformity_df['topplane_deg'] = uniformity_df['barcode'].map(topplane_deg_lookup)
        except Exception as e:
            output_messages.append(f"Error mapping topplane_deg to uniformity_df: {e}")
            uniformity_df['topplane_deg'] = None

        try:
            uniformity_df['topbul_mm'] = uniformity_df['barcode'].map(topbul_mm_lookup)
        except Exception as e:
            output_messages.append(f"Error mapping topbul_mm to uniformity_df: {e}")
            uniformity_df['topbul_mm'] = None
        
        try:
            uniformity_df['botbul_mm'] = uniformity_df['barcode'].map(botbul_mm_lookup)
        except Exception as e:
            output_messages.append(f"Error mapping botbul_mm to uniformity_df: {e}")
            uniformity_df['botbul_mm'] = None

        try:
            uniformity_df['topbul_deg'] = uniformity_df['barcode'].map(topbul_deg_lookup)
        except Exception as e:
            output_messages.append(f"Error mapping topbul_deg to uniformity_df: {e}")
            uniformity_df['topbul_deg'] = None

        try:
            uniformity_df['crroh1_deg'] = uniformity_df['barcode'].map(crroh1_deg_lookup)
        except Exception as e:
            output_messages.append(f"Error mapping crroh1_deg to uniformity_df: {e}")
            uniformity_df['crroh1_deg'] = None
 
        uniformity_df.replace('nan', pd.NA, inplace=True)
 
        balancing_df.replace('nan', pd.NA, inplace=True)
        geometry_df.replace('nan', pd.NA, inplace=True)
 
        uniformity_df['topplane_deg'] = uniformity_df['topplane_deg'].fillna('Unknown') if 'topplane_deg' in uniformity_df.columns else 'Unknown'
        balancing_df['topplane_deg'] = balancing_df['topplane_deg'].fillna('Unknown') if 'topplane_deg' in balancing_df.columns else None
        uniformity_df['topbul_mm'] = uniformity_df['topbul_mm'].fillna('Unknown') if 'topbul_mm' in uniformity_df.columns else 'Unknown'
        geometry_df['topbul_mm'] = geometry_df['topbul_mm'].fillna('Unknown') if 'topbul_mm' in geometry_df.columns else 'Unknown'
        uniformity_df['botbul_mm'] = uniformity_df['botbul_mm'].fillna('Unknown') if 'botbul_mm' in uniformity_df.columns else 'Unknown'
        geometry_df['botbul_mm'] = geometry_df['botbul_mm'].fillna('Unknown') if 'botbul_mm' in geometry_df.columns else 'Unknown'
        uniformity_df['topbul_deg'] = uniformity_df['topbul_deg'].fillna('Unknown') if 'topbul_deg' in uniformity_df.columns else 'Unknown'
        geometry_df['topbul_deg'] = geometry_df['topbul_deg'].fillna('Unknown') if 'topbul_deg' in geometry_df.columns else 'Unknown'
        uniformity_df['crroh1_deg'] = uniformity_df['crroh1_deg'].fillna('Unknown') if 'crroh1_deg' in uniformity_df.columns else 'Unknown'
        geometry_df['crroh1_deg'] = geometry_df['crroh1_deg'].fillna('Unknown') if 'crroh1_deg' in geometry_df.columns else 'Unknown'
        uniformity_df['tbm_buildtime_shift'] = uniformity_df['tbm_buildtime'].apply(get_shift)

        try:
            # Fetch balancing records with specified columns
            balancing_fields = {
                "barcode": 1, "topplane_deg": 1, "botplane_g": 1, "topplane_g": 1,
                "couple_g": 1, "static_g": 1, "botplane_deg": 1, "couple_deg": 1,
                "static_deg": 1, "tb_class": 1,
                "tb_botplane_grade": 1, "tb_topplane_grade": 1, "tb_couple_grade": 1, "tb_static_grade": 1,
                "tb_botplane_deg_grade": 1, "tb_topplane_deg_grade": 1, "tb_couple_deg_grade": 1, "tb_static_deg_grade": 1,
                "_id": 0
            }
            balancing_data = list(all_combined_balancing_collection.find({}, balancing_fields))

            # Fetch geometry records with specified columns
            geometry_fields = {
                "barcode": 1,
                "botbul_mm": 1, "botdep_mm": 1, "topbul_mm": 1, "topdep_mm": 1,
                "blro_mm": 1, "blroh1_mm": 1, "crro_mm": 1, "crroh1_mm": 1, "tlro_mm": 1, "tlroh1_mm": 1,
                "botbul_deg": 1, "botdep_deg": 1, "topbul_deg": 1, "topdep_deg": 1,
                "blro_deg": 1, "blroh1_deg": 1, "crro_deg": 1, "crroh1_deg": 1, "tlro_deg": 1, "tlroh1_deg": 1,
                "tg_botbul_grade": 1, "tg_botdep_grade": 1, "tg_topbul_grade": 1, "tg_topdep_grade": 1,
                "tg_blro_grade": 1, "tg_blroh1_grade": 1, "tg_crro_grade": 1, "tg_crroh1_grade": 1, "tg_tlro_grade": 1, "tg_tlroh1_grade": 1,
                "tg_botbul_deg_grade": 1, "tg_botdep_deg_grade": 1, "tg_topbul_deg_grade": 1, "tg_topdep_deg_grade": 1,
                "tg_blro_deg_grade": 1, "tg_blroh1_deg_grade": 1, "tg_crro_deg_grade": 1, "tg_crroh1_deg_grade": 1,
                "tg_tlro_deg_grade": 1, "tg_tlroh1_deg_grade": 1, "tg_class": 1,
                "_id": 0
            }
            geometry_data = list(all_combined_geometry_collection.find({}, geometry_fields))

            # Convert geometry data into dict for quick lookup by barcode
            geometry_dict = {rec["barcode"]: rec for rec in geometry_data if "barcode" in rec}

            # Merge balancing + geometry data by barcode
            cimcorp_records = []
            for bal in balancing_data:
                barcode = bal.get("barcode")
                merged = bal.copy()
                if barcode in geometry_dict:
                    merged.update(geometry_dict[barcode])
                cimcorp_records.append(merged)

            if not cimcorp_records:
                output_messages.append("Warning: No records found in both collections.")

        except Exception as e:
            output_messages.append(f"Error fetching data from MongoDB: {e}")
            cimcorp_records = []

        # Create lookup dictionaries from MongoDB records
        lookup_dicts = {
            'topplane_deg': {}, 'botplane_g': {}, 'topplane_g': {}, 'couple_g': {},
            'static_g': {}, 'botplane_deg': {}, 'couple_deg': {}, 'static_deg': {},
            'tb_botplane_grade': {}, 'tb_topplane_grade': {}, 'tb_couple_grade': {}, 'tb_static_grade': {},
            'tb_botplane_deg_grade': {}, 'tb_topplane_deg_grade': {}, 'tb_couple_deg_grade': {}, 'tb_static_deg_grade': {},
            'botbul_mm': {}, 'botdep_mm': {}, 'topbul_mm': {}, 'topdep_mm': {},
            'blro_mm': {}, 'blroh1_mm': {}, 'crro_mm': {}, 'crroh1_mm': {},
            'tlro_mm': {}, 'tlroh1_mm': {}, 'botbul_deg': {}, 'botdep_deg': {},
            'topbul_deg': {}, 'topdep_deg': {}, 'blro_deg': {}, 'blroh1_deg': {},
            'crro_deg': {}, 'crroh1_deg': {}, 'tlro_deg': {}, 'tlroh1_deg': {},
            'tg_botbul_grade': {}, 'tg_botdep_grade': {}, 'tg_topbul_grade': {}, 'tg_topdep_grade': {},
            'tg_blro_grade': {}, 'tg_blroh1_grade': {}, 'tg_crro_grade': {}, 'tg_crroh1_grade': {},
            'tg_tlro_grade': {}, 'tg_tlroh1_grade': {},
            'tg_botbul_deg_grade': {}, 'tg_botdep_deg_grade': {}, 'tg_topbul_deg_grade': {}, 'tg_topdep_deg_grade': {},
            'tg_blro_deg_grade': {}, 'tg_blroh1_deg_grade': {}, 'tg_crro_deg_grade': {}, 'tg_crroh1_deg_grade': {},
            'tg_tlro_deg_grade': {}, 'tg_tlroh1_deg_grade': {},
            'tg_class': {}, 'tb_class': {}
        }

        for rec in cimcorp_records:
            barcode = rec.get('barcode')
            if barcode is None:
                continue

            for field in lookup_dicts:
                lookup_dicts[field][barcode] = rec.get(field)

        # Map values to uniformity_df
        for field in lookup_dicts:
            try:
                uniformity_df[field] = uniformity_df['barcode'].map(lookup_dicts[field])
            except Exception as e:
                output_messages.append(f"Error mapping {field} to uniformity_df: {e}")
                uniformity_df[field] = None

        uniformity_df.replace('nan', pd.NA, inplace=True)
        balancing_df.replace('nan', pd.NA, inplace=True)
        geometry_df.replace('nan', pd.NA, inplace=True)

        # Fill NaN values with 'Unknown' for specified columns
        for field in lookup_dicts:
            if field in uniformity_df.columns:
                uniformity_df[field] = uniformity_df[field].fillna('Unknown')
            if field in balancing_df.columns:
                balancing_df[field] = balancing_df[field].fillna('Unknown')
            if field in geometry_df.columns:
                geometry_df[field] = geometry_df[field].fillna('Unknown')

        # Fetch channel mapping data
        try:
            db = client[config_databse]
            channel_no_collection = list(db[channel_no_collect].find())
            channel_no_collection = pd.DataFrame(channel_no_collection)
        except Exception as e:
            output_messages.append(f"Error retrieving data from {channel_no_collect}: {str(e)}")
            return output_messages

        # Validation checks for channel_no_collection
        if not isinstance(channel_no_collection, pd.DataFrame):
            raise TypeError("channel_no_collection must be a pandas DataFrame.")
        if 'sku' not in channel_no_collection.columns:
            raise ValueError("The 'sku' column is missing in channel_no_collection dataframe.")
        if 'gt_code' not in channel_no_collection.columns:
            raise ValueError("The 'gt_code' column is missing in channel_no_collection dataframe.")

        # Clean gt_code in channel_no_collection
        def clean_gt_code(x):
            try:
                if isinstance(x, list):
                    return [str(item) for sublist in x for item in (sublist if isinstance(sublist, list) else [sublist])]
                return [str(item) for item in eval(x)] if isinstance(x, str) else [str(x)]
            except (SyntaxError, NameError, TypeError):
                return [str(x)]

        channel_no_collection['gt_code'] = channel_no_collection['gt_code'].apply(clean_gt_code)
        valid_gt_codes = set()
        for gt_codes in channel_no_collection['gt_code']:
            valid_gt_codes.update(gt_codes)

        # Clean and extract scalar gt_code for source DataFrames
        def clean_source_gt_code(x):
            try:
                if isinstance(x, list):
                    return [str(item) for item in x]
                return [str(x)]
            except (SyntaxError, NameError, TypeError):
                return [str(x)]

        def get_scalar_gt_code(x):
            return x[0] if x and isinstance(x, list) and len(x) > 0 else None

        # Clean ubno values
        def clean_ubno(x):
            if isinstance(x, str) and x.endswith('.0'):
                return x[:-2]
            return x

        for df in [uniformity_df, balancing_df, geometry_df]:
            if not df.empty and 'gt_code' in df.columns:
                df['gt_code_list'] = df['gt_code'].apply(clean_source_gt_code)
                df['gt_code'] = df['gt_code_list'].apply(get_scalar_gt_code)
                if 'ubno' in df.columns:
                    df['ubno'] = df['ubno'].apply(clean_ubno)

        # Filter DataFrames based on valid gt_codes
        if not balancing_df.empty:
            balancing_df = balancing_df[balancing_df['gt_code_list'].apply(lambda x: any(gt in valid_gt_codes for gt in x))]
            output_messages.append(f"Filtered BALANCING to {len(balancing_df)} records based on channel_no_collect gt_code")
        if not geometry_df.empty:
            geometry_df = geometry_df[geometry_df['gt_code_list'].apply(lambda x: any(gt in valid_gt_codes for gt in x))]
            output_messages.append(f"Filtered GEOMETRY to {len(geometry_df)} records based on channel_no_collect gt_code")

        # Insert individual collections — always replace to reflect latest Redshift data
        def insert_with_check(collection_name, data_to_insert, existing_barcodes=None):
            if data_to_insert:
                try:
                    target_db[collection_name].insert_many(data_to_insert)
                    output_messages.append(f"Appended {len(data_to_insert)} records into {target_db.name}.{collection_name}")
                except Exception as e:
                    output_messages.append(f"Error inserting into {collection_name}: {str(e)}")
            else:
                output_messages.append(f"No records to insert into {collection_name}")
            return len(data_to_insert) if data_to_insert else 0

        # Insert individual collections — SKIPPED as per requirement
        # data_mereged_uniformity_home, data_mereged_balancing_home, data_mereged_geometry_home
        # are not written to; only the combined collection is used.

        # if not uniformity_df.empty: ...
        # if not balancing_df.empty: ...
        # if not geometry_df.empty: ...

        # Get unique barcodes and gt_codes
        uniformity_barcodes = set(uniformity_df['barcode'].astype(str)) if not uniformity_df.empty else set()
        balancing_barcodes = set(balancing_df['barcode'].astype(str)) if not balancing_df.empty else set()
        geometry_barcodes = set(geometry_df['barcode'].astype(str)) if not geometry_df.empty else set()

        uniformity_gt_codes = set()
        balancing_gt_codes = set()
        geometry_gt_codes = set()

        for df, gt_set in [(uniformity_df, uniformity_gt_codes), (balancing_df, balancing_gt_codes), (geometry_df, geometry_gt_codes)]:
            if not df.empty and 'gt_code_list' in df.columns:
                for gt_codes in df['gt_code_list']:
                    gt_set.update(gt_codes)

        common_barcodes = uniformity_barcodes & balancing_barcodes & geometry_barcodes
        common_gt_codes = uniformity_gt_codes & balancing_gt_codes & geometry_gt_codes

        output_messages.append(f"Found {len(common_barcodes)} common barcodes and {len(common_gt_codes)} common gt_codes")

        # Filter balancing and geometry DataFrames for combined data
        if not balancing_df.empty:
            balancing_df = balancing_df[
                (balancing_df['barcode'].astype(str).isin(common_barcodes)) &
                (balancing_df['gt_code_list'].apply(lambda x: any(gt in common_gt_codes for gt in x)))
            ]
            output_messages.append(f"Filtered BALANCING to {len(balancing_df)} records with common barcodes and gt_codes")

        if not geometry_df.empty:
            geometry_df = geometry_df[
                (geometry_df['barcode'].astype(str).isin(common_barcodes)) &
                (geometry_df['gt_code_list'].apply(lambda x: any(gt in valid_gt_codes for gt in x)))
            ]
            output_messages.append(f"Filtered GEOMETRY to {len(geometry_df)} records with common barcodes and gt_codes")

        # Merge DataFrames with explicit column handling to avoid _x, _y suffixes
        data_merged_combined = pd.DataFrame()
        if not (uniformity_df.empty or balancing_df.empty or geometry_df.empty):
            output_messages.append(f"balancing_df columns: {list(balancing_df.columns)}")
            output_messages.append(f"geometry_df columns: {list(geometry_df.columns)}")
            # Define columns to keep from each DataFrame — only include what actually exists
            _bal_wanted = ['barcode', 'gt_code',
                'botplane_g', 'topplane_g', 'couple_g', 'static_g',
                'botplane_deg', 'topplane_deg', 'couple_deg', 'static_deg',
                'tb_botplane_grade', 'tb_topplane_grade', 'tb_couple_grade', 'tb_static_grade',
                'tb_botplane_deg_grade', 'tb_topplane_deg_grade', 'tb_couple_deg_grade', 'tb_static_deg_grade',
                'tb_class']
            _geo_wanted = ['barcode', 'gt_code',
                'botbul_mm', 'botdep_mm', 'topbul_mm', 'topdep_mm',
                'blro_mm', 'blroh1_mm', 'crro_mm', 'crroh1_mm', 'tlro_mm', 'tlroh1_mm',
                'botbul_deg', 'botdep_deg', 'topbul_deg', 'topdep_deg',
                'blro_deg', 'blroh1_deg', 'crro_deg', 'crroh1_deg', 'tlro_deg', 'tlroh1_deg',
                'tg_botbul_grade', 'tg_botdep_grade', 'tg_topbul_grade', 'tg_topdep_grade',
                'tg_blro_grade', 'tg_blroh1_grade', 'tg_crro_grade', 'tg_crroh1_grade',
                'tg_tlro_grade', 'tg_tlroh1_grade',
                'tg_botbul_deg_grade', 'tg_botdep_deg_grade', 'tg_topbul_deg_grade', 'tg_topdep_deg_grade',
                'tg_blro_deg_grade', 'tg_blroh1_deg_grade', 'tg_crro_deg_grade', 'tg_crroh1_deg_grade',
                'tg_tlro_deg_grade', 'tg_tlroh1_deg_grade', 'tg_class']

            balancing_cols_to_keep = [c for c in _bal_wanted if c in balancing_df.columns]
            geometry_cols_to_keep  = [c for c in _geo_wanted  if c in geometry_df.columns]

            missing_bal = [c for c in _bal_wanted if c not in balancing_df.columns]
            missing_geo = [c for c in _geo_wanted  if c not in geometry_df.columns]
            if missing_bal:
                output_messages.append(f"Note: balancing columns not found (skipped): {missing_bal}")
            if missing_geo:
                output_messages.append(f"Note: geometry columns not found (skipped): {missing_geo}")
            uniformity_cols_to_keep = [col for col in uniformity_df.columns if col not in ['gt_code_list']]

            # Filter DataFrames to keep only specified columns
            temp_uniformity = uniformity_df[uniformity_cols_to_keep]
            temp_balancing = balancing_df[balancing_cols_to_keep] if not balancing_df.empty else pd.DataFrame()
            temp_geometry = geometry_df[geometry_cols_to_keep] if not geometry_df.empty else pd.DataFrame()

            # Perform merges with explicit column selection
            if not temp_balancing.empty:
                temp_merge = temp_uniformity.merge(temp_balancing, on=['barcode', 'gt_code'], how='inner', suffixes=('', '_bal'))
                # Drop any _bal suffixed columns
                temp_merge = temp_merge[[col for col in temp_merge.columns if not col.endswith('_bal')]]
            else:
                temp_merge = temp_uniformity

            if not temp_geometry.empty:
                data_merged_combined = temp_merge.merge(
                    temp_geometry.drop(columns=['gt_code'], errors='ignore'),
                    on='barcode', how='left', suffixes=('', '_geo')
                )
                # Drop any _geo suffixed columns
                data_merged_combined = data_merged_combined[[col for col in data_merged_combined.columns if not col.endswith('_geo')]]
            else:
                data_merged_combined = temp_merge

            if data_merged_combined.empty:
                output_messages.append("No matching records found after merging UNIFORMITY, BALANCING, and GEOMETRY data.")
            else:
                output_messages.append(f"Merged data resulted in {len(data_merged_combined)} records")
                if 'ubno' in data_merged_combined.columns:
                    data_merged_combined['ubno'] = data_merged_combined['ubno'].apply(clean_ubno)

        # Rename columns (excluding barcode and gt_code)
        uniformity_cols = {col: f'{col}' for col in uniformity_df.columns if col not in ['barcode', 'gt_code', 'gt_code_list']}
        balancing_cols = {col: f'{col}' for col in balancing_df.columns if col not in ['barcode', 'gt_code', 'gt_code_list']}
        geometry_cols = {col: f'{col}' for col in geometry_df.columns if col not in ['barcode', 'gt_code', 'gt_code_list']}

        uniformity_df.rename(columns=uniformity_cols, inplace=True)
        if not balancing_df.empty:
            balancing_df.rename(columns=balancing_cols, inplace=True)
        if not geometry_df.empty:
            geometry_df.rename(columns=geometry_cols, inplace=True)

        # Merge UFM testing + visual inspection results (q4) by barcode
        if not data_merged_combined.empty and 'df_cimcorp_results' in globals() and not df_cimcorp_results.empty:
            try:
                cimcorp_cols = ['barcode', 'ufm_testing_result', 'ufm_testing_result_timestamp',
                                'visualinspection_result', 'visualinspection_time']
                cimcorp_slice = df_cimcorp_results[[c for c in cimcorp_cols if c in df_cimcorp_results.columns]].copy()
                # Ensure barcode types match for the merge
                cimcorp_slice['barcode'] = cimcorp_slice['barcode'].astype(str)
                # One row per barcode (keep first occurrence)
                cimcorp_slice = cimcorp_slice.drop_duplicates(subset=['barcode'], keep='first')
                data_merged_combined['barcode'] = data_merged_combined['barcode'].astype(str)
                data_merged_combined = data_merged_combined.merge(
                    cimcorp_slice, on='barcode', how='left', suffixes=('', '_cimcorp')
                )
                data_merged_combined = data_merged_combined[
                    [col for col in data_merged_combined.columns if not col.endswith('_cimcorp')]
                ]
                output_messages.append(f"Merged cimcorp UFM/visual inspection results into {len(data_merged_combined)} records")
            except Exception as e:
                output_messages.append(f"Error merging cimcorp results: {str(e)}")

        # Modified: Remove specified alert and machine model columns from data_merged_combined before insertion
        if not data_merged_combined.empty:
            data_merged_combined.drop_duplicates(subset=['barcode', 'gt_code'], inplace=True)
            # Define columns to exclude from the combined collection
            columns_to_exclude_combined = [
                'alert_id_ubno_balancing', 'alert_id_ubno_yield_drop', 'alert_id_curing_ufm',
                'alert_id_curing_balancing', 'alert_id_tbm_ufm', 'alert_id_tbm_balancing',
                'alert_id_tbm_yield_drop', 'alert_id_curing_yield_drop', 'alert_id_tbm_load_pressure',
                'alert_id_curing_load_pressure', 'tbm_machine_model_lot_id', 'tbm_previous_machine_model_lot_id', 'curing_machine_model_lot_id', 'curing_previous_machine_model_lot_id'
            ]
            # Remove specified columns if they exist
            data_merged_combined = data_merged_combined.drop(columns=[col for col in columns_to_exclude_combined if col in data_merged_combined.columns])

        data_to_insert_combined = data_merged_combined.to_dict(orient="records")

        if data_merged_combined.empty:
            output_messages.append("No matching records found in merged dataset to be copied.")

        # Insert into COMBINED_COLLECTION_NAME
        # APPEND-ONLY: Each run appends its data to this collection. The existing data
        # is intentionally NOT cleared, and duplicate removal is intentionally NOT run,
        # so every run's records are kept and the collection grows over time.
        try:
            insert_with_check(COMBINED_COLLECTION_NAME, data_to_insert_combined)
        except Exception as e:
            output_messages.append(f"Error accessing {COMBINED_COLLECTION_NAME}: {str(e)}")

        # Insert the SAME data into the new combined collection (used by the without-lot-size functions)
        # APPEND-ONLY: same behaviour as above — do not clear, do not de-duplicate.
        try:
            # Strip any _id carried over so the new collection gets fresh documents
            data_to_insert_new = [
                {k: v for k, v in doc.items() if k != "_id"}
                for doc in data_to_insert_combined
            ]
            insert_with_check(new_COMBINED_COLLECTION_NAME, data_to_insert_new)
        except Exception as e:
            output_messages.append(f"Error accessing {new_COMBINED_COLLECTION_NAME}: {str(e)}")

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        output_messages.append(error_message)
        output_messages.append(traceback.format_exc())

    return output_messages

def remove_duplicates_collection(collection):
    messages = []
    pipeline = [
        {
            "$group": {
                "_id": {
                    "barcode":          "$barcode",
                    "date_of_ufm":      "$date_of_ufm",
                    "tbm_buildtime":    "$tbm_buildtime",
                    "curing_infeedtime":"$curing_infeedtime"
                },
                "uniqueIds": {"$addToSet": "$_id"},
                "count":     {"$sum": 1}
            }
        },
        {"$match": {"count": {"$gt": 1}}}
    ]

    duplicates = list(collection.aggregate(pipeline))

    for doc in duplicates:
        # Keep the first inserted document, remove the rest
        ids_to_remove = doc["uniqueIds"][1:]
        collection.delete_many({"_id": {"$in": ids_to_remove}})
        key = doc["_id"]
        messages.append(
            f"Removed {len(ids_to_remove)} duplicate(s) for "
            f"barcode={key.get('barcode')} date_of_ufm={key.get('date_of_ufm')} "
            f"tbm_buildtime={key.get('tbm_buildtime')} curing_infeedtime={key.get('curing_infeedtime')}"
        )

    return messages

def make_hashable(value):
    if isinstance(value, list):
        return tuple(make_hashable(v) for v in value)
    elif isinstance(value, dict):
        return tuple(sorted((k, make_hashable(v)) for k, v in value.items()))
    elif isinstance(value, np.generic):
        return value.item()
    return value

def run_data_cleaning():
    start_time = time.time()
    output_messages = []
    cleaned_collection_names = []
    invalid_collection_names = []

    try:
        # Source the raw data from the in-memory store populated by insert_with_timestamp
        # instead of reading it back from the raw 'UNIFORMITY'/'BALANCING'/'GEOMETRY'
        # databases (which we intentionally no longer create/populate).
        if not RAW_DATAFRAME_STORE:
            output_messages.append("No in-memory raw data found for data cleaning.")
            final_message = "No in-memory raw data found for data cleaning."
            return {"status": "success", "message": final_message, "output_log": output_messages, "cleaned_collections": cleaned_collection_names, "invalid_collections": invalid_collection_names, "execution_time": 0}

        # Preserve the original processing order: UNIFORMITY, BALANCING, GEOMETRY
        for database_name in ["UNIFORMITY", "BALANCING", "GEOMETRY"]:
            raw_entry = RAW_DATAFRAME_STORE.get(database_name)
            if not raw_entry:
                output_messages.append(f"No in-memory raw data for {database_name}, skipping.")
                continue

            filename = raw_entry["filename"]
            source_data = raw_entry["records"]

            output_messages.append(f"Processing in-memory data: {filename} from database: {database_name}")

            process_msgs, cleaned_collect_name, invalid_collect_name = process_data_for_api(client, source_data, database_name, filename)
            output_messages.extend(process_msgs)
            cleaned_collection_names.append(cleaned_collect_name)
            invalid_collection_names.append(invalid_collect_name)

            output_messages.append(f"In-memory data for {database_name} processed.")

        output_messages.append("Copying to single collection to final database...")
        copy_msgs = copy_to_single_collection_api()
        output_messages.extend(copy_msgs)

        final_message = "Data cleaning and processing completed successfully."
        end_time = time.time()
        execution_time = end_time - start_time
        output_messages.append(f"Data cleaning process completed in {execution_time:.2f} seconds.")

        return {"status": "success", "message": final_message, "output_log": output_messages, "cleaned_collections": cleaned_collection_names, "invalid_collections": invalid_collection_names, "execution_time": execution_time}

    except Exception as e:
        error_message = f"An error occurred in the data cleaning function: {e}"
        output_messages.append(error_message)
        return {"status": "error", "message": error_message, "output_log": output_messages, "cleaned_collections": cleaned_collection_names, "invalid_collections": invalid_collection_names, "execution_time": 0}

def get_rank_columns(batch_data):
    # Placeholder: Implement logic to extract/summarize rank columns
    rank_cols = [col for col in batch_data.columns if 'rank' in col.lower()]
    summary = {}
    for col in rank_cols:
        if col in batch_data.columns:
            # Example: Count occurrences of each rank
            summary[col] = batch_data[col].value_counts().to_dict()
            # Convert keys to string if they are numbers for JSON compatibility
            summary[col] = {str(k): v for k, v in summary[col].items()}
    return summary

def convert_numpy_types(data):
    if isinstance(data, np.generic):
        return data.item()
    elif isinstance(data, dict):
        return {k: convert_numpy_types(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_numpy_types(v) for v in data]
    # Handle pandas Timestamp
    elif isinstance(data, pd.Timestamp):
        return data.to_pydatetime() # Convert to Python datetime
    return data

def split_and_store_virgin_final(model_data, date_col, process_type):
    """Split model_data into virgin (first occurrence per barcode) and final
    (last occurrence per barcode) using the given date column.

    NOTE: This function ONLY splits the data in memory. It does NOT write any raw
    barcode-level rows to MongoDB. Raw data is persisted only in
    UNIFORMITY_BALANCING_GEOMETRY_DB (data_merged_combined_uniform_balan_geo /
    new_data_merged_combined_uniform_balan_geo). The alert databases
    (alert_db_virgin_yield, alert_db_final_yield, alert_table_final_yield,
    alert_deviated_final_yield) receive only the generated alert documents from
    the main lot/alert pipeline.

    Returns (virgin_data, final_data, messages). The caller typically continues
    main lot processing on virgin_data.
    """
    messages = []
    try:
        if model_data is None or model_data.empty:
            return model_data, model_data, messages

        model_data_full = model_data.copy()
        model_data_full["date_of_dt"] = pd.to_datetime(model_data_full[date_col], errors='coerce')

        # Virgin Yield: FIRST occurrence of each barcode
        virgin_data = model_data_full.sort_values("date_of_dt").drop_duplicates(subset="barcode", keep="first").copy()
        virgin_data.sort_values("date_of_dt", inplace=True)

        # Final Yield: LAST occurrence of each barcode
        final_data = model_data_full.sort_values("date_of_dt").drop_duplicates(subset="barcode", keep="last").copy()
        final_data.sort_values("date_of_dt", inplace=True)

        # Drop the helper datetime column before returning
        virgin_data = virgin_data.drop(columns=["date_of_dt"], errors="ignore")
        final_data = final_data.drop(columns=["date_of_dt"], errors="ignore")

        return virgin_data, final_data, messages
    except Exception as e:
        messages.append(f"❌ {process_type} Error during virgin/final split: {e}")
        logging.error(f"{process_type} Error during virgin/final split: {e}")
        return model_data, model_data, messages

def process_tbm_lot_and_alert_data():
    start_time = time.time()
    output_messages = []
    # WITH lot size: restrict processing to active machines only.
    _restrict_active_machines = True
    inserted_counts = {}
    updated_counts = {}
    # tbm_debug_csv_filename = "tbm_process_debug_data.csv"

    # Initialize MongoDB collections
    try:
        merge_collect = client[UNIFORMITY_BALANCING_GEOMETRY_DB][COMBINED_COLLECTION_NAME]
        channel_collect_tbm = client[config_databse][channel_no_collect_tbm]
        channel_collect_ufm = client[config_databse][channel_no_collect]
        lot_tbm_collect = client[lot_db][lot_tbm_collect_name]
        alert_collect = client[alert_table_final_yield][alert_tbm_collect_name]
        averages_collect = client[average_db][averages_tbm_table]
        machine_collect = client[config_databse][machine_no_collect]
        curing_press_collect = client[config_databse][curing_press_dict_collect]
        alert_triggering_collect = client[alert_db_virgin_yield][alert_triggering_collect_name]
        summary_triggering_collect = client[summary_db][summary_triggering_collect_name_tbm]
        alert_triggering_deviated_collect = client[alert_deviated_final_yield][alert_triggering_deviated_tbm]
        # NEW: Initialize collections for alert_db_final_yield with the same collection names
        alert_triggering_collect_dashboard = client[alert_db_final_yield][alert_triggering_collect_name]
        # NEW: Initialize collections for model_workcenter_db
        tbm_gtcodes_collect = client[model_db][tbm_collection]
        tbm_machine_collect = client[model_db][tbm_collection_machine]
        output_messages.append("✅ TBM Collections loaded successfully for lot and alert processing in both alert_db_virgin_yield and alert_db_final_yield.")
    except Exception as e:
        return {"status": "error", "message": "Database connection failed", "output_log": output_messages}, 500
    
    # Fetch active machines from model_workcenter_db.tbm_machine
    try:
        active_machines = set(tbm_machine_collect.distinct("tbm", {"status": "active"}))
        output_messages.append(f"✅ Fetched {len(active_machines)} active machines: {active_machines}")
        if not active_machines:
            output_messages.append("❌ No active machines found in model_workcenter_db.tbm_machine. Exiting.")
            return {"status": "error", "message": "No active machines found", "output_log": output_messages}, 500
    except Exception as e:
        output_messages.append(f"❌ Error fetching active machines: {e}")
        return {"status": "error", "message": "Failed to fetch active machines", "output_log": output_messages}, 500

    # Fetch active gt_code numbers
    try:
        # Step 1: Fetch distinct active gt_codes (flatten from list fields)
        active_gtcodes = set()
        cursor = tbm_gtcodes_collect.find({"status": "active"}, {"gt_code": 1})
        for doc in cursor:
            active_gtcodes.update(doc.get("gt_code", []))  # flatten the gt_code list
        output_messages.append(f"✅ Fetched {len(active_gtcodes)} active model numbers: {active_gtcodes}")
    except Exception as e:
        output_messages.append(f"❌ Error fetching active gt_code numbers: {e}")
        return {"status": "error", "message": "Failed to fetch active model numbers", "output_log": output_messages}, 500

    # Load data into DataFrames
    try:
        merge1 = list(merge_collect.find({}))
        merge_df = pd.DataFrame(merge1)
        channel_data_tbm = list(channel_collect_tbm.find({"gt_code": {"$in": list(active_gtcodes)}}))
        channel_data_tbm = list(channel_collect_tbm.find({}))
        channel_df = pd.DataFrame(channel_data_tbm)
        channel_data_ufm = list(channel_collect_ufm.find({}))
        channel_df_ufm = pd.DataFrame(channel_data_ufm)
        machine_data = list(machine_collect.find({"tbm": {"$in": list(active_machines)}}))
        machine_data = list(machine_collect.find({}))
        machine_df = pd.DataFrame(machine_data)
        output_messages.append(f"✅ TBM Data loaded from collections into DataFrames for processing (filtered for active model numbers: {active_gtcodes}).")
    except Exception as e:
        return {"status": "error", "message": "Data loading failed", "output_log": output_messages}, 500
    
    # Get distinct model numbers
    gt_code_lists = channel_collect_tbm.distinct("gt_code")

    # Check required columns — initialize any missing ones instead of exiting
    if merge_df.empty:
        output_messages.append("merge_df is empty. No TBM data to process.")
        return {"status": "success", "message": "No TBM data to process.", "output_log": output_messages, "inserted_counts": inserted_counts, "updated_counts": updated_counts}, 200
    if "gt_code" not in merge_df.columns:
        output_messages.append("'gt_code' column missing in merge_df. Skipping TBM processing.")
        return {"status": "success", "message": "'gt_code' column missing. Skipping TBM processing.", "output_log": output_messages, "inserted_counts": inserted_counts, "updated_counts": updated_counts}, 200
    if "alert_id_tbm_ufm" not in merge_df.columns:
        merge_df["alert_id_tbm_ufm"] = None

    # Filter data
    filtered_df = merge_df[merge_df["gt_code"].isin(gt_code_lists) & merge_df["alert_id_tbm_ufm"].isna()]  # Changed from modelno to gt_code
    if not filtered_df.empty:
        merge_df = filtered_df
    else:
        output_messages.append("No TBM data after filtering or condition not satisfied. Skipping.")
        return {"status": "success", "message": "No TBM data after filtering.", "output_log": output_messages, "inserted_counts": inserted_counts, "updated_counts": updated_counts}, 200

    # Assign date columns
    merge_df["date_of_tbm"] = merge_df.get("tbm_buildtime", pd.Series([None] * len(merge_df)))
    merge_df["date_of_curing"] = merge_df.get("curing_infeedtime", pd.Series([None] * len(merge_df)))

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
        merge_df["date_of_ufm"] = convert_datetime_with_invalid_mask(merge_df["date_of_ufm"], "date_of_ufm")
        merge_df["date_of_ufm"] = merge_df["date_of_ufm"].astype(object).where(merge_df["date_of_ufm"].notnull(), "No Date")
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

    # Fetch last lot IDs per tbm and model
    try:
        last_lots = list(lot_tbm_collect.aggregate([
            {
                "$match": {
                    "lot_id": {"$regex": "^TBM_[^_]+_[^_]+_\\d+$"},
                    "gt_code": {"$in": list(active_gtcodes)},
                    "tbm": {"$in": list(active_machines)}
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
                    "gt_code": "$gt_code",
                    "tbm": "$tbm"
                }
            },
            {
                "$sort": {"lot_number": -1}
            },
            {
                "$group": {
                    "_id": {"gt_code": "$gt_code", "tbm": "$tbm"},
                    "max_lot_number": {"$first": "$lot_number"},
                    "last_lot_id": {"$first": "$lot_id"}
                }
            }
        ]))
        last_lot_dict = {
            (item["_id"]["gt_code"], item["_id"]["tbm"]): {
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
    merge_df["tbm"] = merge_df["tbm"].astype(str)
    machine_df["tbm"] = machine_df["tbm"].astype(str)  # Changed from modelnumber to gt_code is handled as array later
    # Filter merge_df to include only active machines (only when restricting).
    if _restrict_active_machines:
        merge_df = merge_df[merge_df["tbm"].isin(active_machines)]
    if merge_df.empty:
        output_messages.append("❌ No data in merge_df after filtering for active machines. Exiting.")
        return {"status": "error", "message": "No data after active tbm filtering", "output_log": output_messages}, 500

    # Ensure lot-tracking columns are object dtype so string lot IDs can be assigned
    # (they may pre-exist as float64/NaN, which raises "Invalid value ... for dtype 'float64'")
    for _lot_col in ["lot_id", "previous_lot_id", "tbm_machine_model_lot_id", "tbm_previous_machine_model_lot_id", "alert_id_tbm_ufm"]:
        if _lot_col not in merge_df.columns:
            merge_df[_lot_col] = None
        merge_df[_lot_col] = merge_df[_lot_col].astype(object)

    # Initialize tracking variables
    machine_lot_avg_map = {}
    tbm_previous_machine_model_lot_ids = {}
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
                "gt_code": {"$in": list(active_gtcodes)},
                "machine": {"$in": list(active_machines)}
            }
            },
            {
                "$sort": {"tbm_machine_model_lot_id": -1}
            },
            {
                "$group": {
                    "_id": {"gt_code": "$gt_code", "machine": "$machine"},
                    "last_tbm_lot_id": {"$first": "$tbm_machine_model_lot_id"},
                    "last_averages": {"$first": "$current_lot_averages"}
                }
            }
        ]))
        for item in last_averages:
            gt_code = item["_id"]["gt_code"]
            tbm = item["_id"]["machine"]
            tbm_lot_id = item["last_tbm_lot_id"]
            averages = item["last_averages"]
            tbm_previous_machine_model_lot_ids[(gt_code, tbm)] = tbm_lot_id
            if averages:
                machine_lot_avg_map[(gt_code, tbm, tbm_lot_id)] = averages
                logging.info(f"Pre-populated machine_lot_avg_map with averages for {tbm_lot_id}")
            else:
                logging.warning(f"No averages found for {tbm_lot_id} in averages_collect")
        logging.info(f"Initialized tbm_previous_machine_model_lot_ids: {tbm_previous_machine_model_lot_ids}")
    except Exception as e:
        logging.error(f"Error pre-populating machine_lot_avg_map and tbm_previous_machine_model_lot_ids: {e}")
        output_messages.append(f"❌ Error pre-populating machine_lot_avg_map and tbm_previous_machine_model_lot_ids: {e}")

    numeric_params = [
        'rfpp_cw_kg','rfpp_ccw_kg','rfh1_cw_kg','rfh1_ccw_kg','rfh2_cw_kg','rfh2_ccw_kg','lfpp_cw_kg','lfpp_ccw_kg',
        'lsft_cw_kg','lsft_ccw_kg','lfh1_cw_kg','lfh1_ccw_kg','plysteer_kg','cony_kg','rfmxa_ccw_deg',
        'rfa1_cw_deg','rfa1_ccw_deg','rfa2_cw_deg','rfa2_ccw_deg','lfmxa_cw_deg','lfmxa_ccw_deg','lfa1_cw_deg',
        'lfa1_ccw_deg','botplane_g','topplane_g','couple_g','static_g','botplane_deg','topplane_deg','couple_deg',
        'static_deg','botbul_mm','botdep_mm','topbul_mm','topdep_mm','blro_mm','blroh1_mm','crro_mm','crroh1_mm',
        'tlro_mm','tlroh1_mm','botbul_deg','botdep_deg','topbul_deg','topdep_deg','blro_deg','blroh1_deg','crro_deg',
        'crroh1_deg','tlro_deg','tlroh1_deg'
        ]
    yield_params = [
        "rfv_yield", "rfh1_yield", "rfh2_yield", "lfv_yield", "lsft_yield", "lfh1_yield", "conicity_yield", "oe_yield",
        "rfmxa_yield", "rfa1_yield", "rfa2_yield", "lfxma_yield", "lfa1_yield", "central_radial_runout_yield", "central_radial_runout_first_harmonic_yield",
        "lateral_run_out_top_over_yield", "lateral_run_out_bottom_over_yield", "lateral_run_out_yield", "lateral_run_out_bottom_first_harmonic_yield", "lateral_run_out_top_first_harmonic_yield", 
        "lateral_run_out_harmonic_yield", "bulge_top_over_yield", "bulge_bottom_over_yield", "bulge_yield", "dent_top_over_yield", "dent_bottom_over_yield", "dent_yield", "geometry_oe_yield",
        "lower_yield", "upper_yield", "static_yield", "couple_yield", "balancing_oe_yield", "warehouse_yield", "visual_yield"
        ]
    
    # Define rank parameter mappings
    rank_param_mappings = {
        "rfv_yield": ["tu_rfpp_cw_grade", "tu_rfpp_ccw_grade"],
        "rfh1_yield": ["tu_rfh1_cw_grade", "tu_rfh1_ccw_grade"], 
        "rfh2_yield": ["tu_rfh2_cw_grade", "tu_rfh2_ccw_grade"],
        "lfv_yield": ["tu_lfpp_cw_grade", "tu_lfpp_ccw_grade"],
        "lsft_yield": ["tu_lsft_cw_grade", "tu_lsft_ccw_grade"],
        "lfh1_yield": ["tu_lfh1_cw_grad", "tu_lfh1_ccw_grad"],
        "conicity_yield": ["tu_cony_grade"],
        "oe_yield": ["tu_class"],
        "rfmxa_yield": ["tu_rfmxa_ccw_grade"],
        "rfa1_yield": ["tu_rfa1_cw_grade", "tu_rfa1_ccw_grade"],
        "rfa2_yield": ["tu_rfa2_cw_grade", "tu_rfa2_ccw_grade"],
        "lfxma_yield": ["tu_lfmxa_cw_grade", "tu_lfmxa_ccw_grade"],
        "lfa1_yield": ["tu_lfa1_cw_grade", "tu_lfa1_ccw_grade"],
        "central_radial_runout_yield": ["tg_crro_grade"],
        "central_radial_runout_first_harmonic_yield": ["tg_crroh1_grade"],
        "lateral_run_out_top_over_yield": ["tg_tlro_grade"],
        "lateral_run_out_bottom_over_yield": ["tg_blro_grade"],
        "lateral_run_out_yield": ["tg_tlro_grade", "tg_blro_grade"],
        "lateral_run_out_bottom_first_harmonic_yield": ["tg_blroh1_grade"],
        "lateral_run_out_top_first_harmonic_yield": ["tg_tlroh1_grade"],
        "lateral_run_out_harmonic_yield": ["tg_tlroh1_grade", "tg_blroh1_grade"],
        "bulge_top_over_yield": ["tg_topbul_grade"],
        "bulge_bottom_over_yield": ["tg_botbul_grade"],
        "bulge_yield": ["tg_botbul_grade", "tg_topbul_grade"],
        "dent_top_over_yield": ["tg_topdep_grade"],
        "dent_bottom_over_yield": ["tg_botdep_grade"],
        "dent_yield": ["tg_topdep_grade", "tg_botdep_grade"],
        "geometry_oe_yield": ["tg_class"],
        "lower_yield": ["tb_botplane_grade"],
        "upper_yield": ["tb_topplane_grade"],
        "static_yield": ["tb_static_grade"],
        "couple_yield": ["tb_couple_grade"],
        "balancing_oe_yield": ["tb_class"]
        }

    # TBM tbm-wise processing
    for _, row in channel_df.iterrows():
        try:
            gt_code_list = row.get("gt_code", [])
            model_number = gt_code_list[0].strip() if isinstance(gt_code_list, list) and gt_code_list else ""

            if model_number not in active_gtcodes:
                output_messages.append(f"Skipping TBM model {model_number} as it is not active.")
                continue
            gt_codes = row.get("gt_code", [])

            # Ensure gt_codes is a list
            if not isinstance(gt_codes, list):
                gt_codes = [gt_codes]
            
            try:
                target_lot_size = int(row.get("lot_size", 0))
            except (ValueError, TypeError):
                target_lot_size = 0
            if target_lot_size == 0:
                output_messages.append(f"Skipping TBM model {model_number} due to invalid lot size.")
                continue
            # spec_con = float(row.get("spec_con", 0))
            sizecode = str(row.get("sizecode", ""))
            spec_rfpp_cw_kg = float(row.get("spec_rfpp_cw_kg", 0))
            spec_rfpp_ccw_kg = float(row.get("spec_rfpp_ccw_kg", 0))
            spec_rfh1_cw_kg = float(row.get("spec_rfh1_cw_kg", 0))
            spec_rfh1_ccw_kg = float(row.get("spec_rfh1_ccw_kg", 0))
            spec_rfh2_cw_kg = float(row.get("spec_rfh2_cw_kg", 0))
            spec_rfh2_ccw_kg = float(row.get("spec_rfh2_ccw_kg", 0))
            spec_lfpp_cw_kg = float(row.get("spec_lfpp_cw_kg", 0))
            spec_lfpp_ccw_kg = float(row.get("spec_lfpp_ccw_kg", 0))
            spec_lsft_cw_kg = float(row.get("spec_lsft_cw_kg", 0))
            spec_lsft_ccw_kg = float(row.get("spec_lsft_ccw_kg", 0))
            spec_lfh1_cw_kg = float(row.get("spec_lfh1_cw_kg", 0))
            spec_lfh1_ccw_kg = float(row.get("spec_lfh1_ccw_kg", 0))
            spec_plysteer_kg = float(row.get("spec_plysteer_kg", 0))
            spec_rfmxa_ccw_deg = float(row.get("spec_rfmxa_ccw_deg", 0))
            spec_rfa1_cw_deg = float(row.get("spec_rfa1_cw_deg", 0))
            spec_rfa1_ccw_deg = float(row.get("spec_rfa1_ccw_deg", 0))
            spec_rfa2_cw_deg = float(row.get("spec_rfa2_cw_deg", 0))
            spec_rfa2_ccw_deg = float(row.get("spec_rfa2_ccw_deg", 0))
            spec_lfmxa_cw_deg = float(row.get("spec_lfmxa_cw_deg", 0))
            spec_lfmxa_ccw_deg = float(row.get("spec_lfmxa_ccw_deg", 0))
            spec_lfa1_cw_deg = float(row.get("spec_lfa1_cw_deg", 0))
            spec_lfa1_ccw_deg = float(row.get("spec_lfa1_ccw_deg", 0))
            spec_cony_kg = float(row.get("spec_cony_kg", 0))
            spec_static_g = float(row.get("spec_static_g", 0))
            spec_static_deg = float(row.get("spec_static_deg", 0))
            spec_couple_g = float(row.get("spec_couple_g", 0))
            spec_couple_deg = float(row.get("spec_couple_deg", 0))
            spec_bul_botbul_mm = float(row.get("spec_bul_botbul_mm", 0))
            spec_bul_topbul_mm = float(row.get("spec_bul_topbul_mm", 0))
            spec_bul_botbul_deg = float(row.get("spec_bul_botbul_deg", 0))
            spec_bul_topdep_deg = float(row.get("spec_bul_topdep_deg", 0))
            spec_dep_botdep_mm = float(row.get("spec_dep_botdep_mm", 0))
            spec_dep_topdep_mm = float(row.get("spec_dep_topdep_mm", 0))
            spec_dep_botdep_deg = float(row.get("spec_dep_botdep_deg", 0))
            spec_dep_topbul_deg = float(row.get("spec_dep_topbul_deg", 0))
            spec_rro_crro_mm = float(row.get("spec_rro_crro_mm", 0))
            spec_rro_crroh1_mm = float(row.get("spec_rro_crroh1_mm", 0))
            spec_lro_blro_mm = float(row.get("spec_lro_blro_mm", 0))
            spec_lro_blroh1_mm = float(row.get("spec_lro_blroh1_mm", 0))
            spec_lro_tlro_mm = float(row.get("spec_lro_tlro_mm", 0))
            spec_lro_tlroh1_mm = float(row.get("spec_lro_tlroh1_mm", 0))
            spec_rro_crro_deg = float(row.get("spec_rro_crro_deg", 0))
            spec_rro_crroh1_deg = float(row.get("spec_rro_crroh1_deg", 0))
            spec_lro_blro_deg = float(row.get("spec_lro_blro_deg", 0))
            spec_lro_blroh1_deg = float(row.get("spec_lro_blroh1_deg", 0))
            spec_lro_tlro_deg = float(row.get("spec_lro_tlro_deg", 0))
            spec_lro_tlroh1_deg = float(row.get("spec_lro_tlroh1_deg", 0))
            spec_dynamic_botplane_g = float(row.get("spec_dynamic_botplane_g", 0))
            spec_dynamic_topplane_g = float(row.get("spec_dynamic_topplane_g", 0))
            spec_dynamic_botplane_deg = float(row.get("spec_dynamic_botplane_deg", 0))
            spec_dynamic_topplane_deg = float(row.get("spec_dynamic_topplane_deg", 0))

            rfv_yield_spec_yield = float(row.get("rfv_yield_spec_yield", 0))
            rfh1_yield_spec_yield = float(row.get("rfh1_yield_spec_yield", 0))
            rfh2_yield_spec_yield = float(row.get("rfh2_yield_spec_yield", 0))
            lfv_yield_spec_yield = float(row.get("lfv_yield_spec_yield", 0))
            lsft_yield_spec_yield = float(row.get("lsft_yield_spec_yield", 0))
            lfh1_yield_spec_yield = float(row.get("lfh1_yield_spec_yield", 0))
            conicity_yield_spec_yield = float(row.get("conicity_yield_spec_yield", 0))
            oe_yield_spec_yield = float(row.get("oe_yield_spec_yield", 0))
            rfmxa_yield_spec_yield = float(row.get("rfmxa_yield_spec_yield", 0))
            rfa1_yield_spec_yield = float(row.get("rfa1_yield_spec_yield", 0))
            rfa2_yield_spec_yield = float(row.get("rfa2_yield_spec_yield", 0))
            lfxma_yield_spec_yield = float(row.get("lfxma_yield_spec_yield", 0))
            lfa1_yield_spec_yield = float(row.get("lfa1_yield_spec_yield", 0))
            central_radial_runout_yield_spec_yield = float(row.get("central_radial_runout_yield_spec_yield", 0))
            central_radial_runout_first_harmonic_yield_spec_yield = float(row.get("central_radial_runout_first_harmonic_yield_spec_yield", 0))
            lateral_run_out_top_over_yield_spec_yield = float(row.get("lateral_run_out_top_over_yield_spec_yield", 0))
            lateral_run_out_bottom_over_yield_spec_yield = float(row.get("lateral_run_out_bottom_over_yield_spec_yield", 0))
            lateral_run_out_yield_spec_yield = float(row.get("lateral_run_out_yield_spec_yield", 0))
            lateral_run_out_bottom_first_harmonic_yield_spec_yield = float(row.get("lateral_run_out_bottom_first_harmonic_yield_spec_yield", 0))
            lateral_run_out_top_first_harmonic_yield_spec_yield = float(row.get("lateral_run_out_top_first_harmonic_yield_spec_yield", 0))
            lateral_run_out_harmonic_yield_spec_yield = float(row.get("lateral_run_out_harmonic_yield_spec_yield", 0))
            bulge_top_over_yield_spec_yield = float(row.get("bulge_top_over_yield_spec_yield", 0))
            bulge_bottom_over_yield_spec_yield = float(row.get("bulge_bottom_over_yield_spec_yield", 0))
            bulge_yield_spec_yield = float(row.get("bulge_yield_spec_yield", 0))
            dent_top_over_yield_spec_yield = float(row.get("dent_top_over_yield_spec_yield", 0))
            dent_bottom_over_yield_spec_yield = float(row.get("dent_bottom_over_yield_spec_yield", 0))
            dent_yield_spec_yield = float(row.get("dent_yield_spec_yield", 0))
            geometry_oe_yield_spec_yield = float(row.get("geometry_oe_yield_spec_yield", 0))
            lower_yield_spec_yield = float(row.get("lower_yield_spec_yield", 0))
            upper_yield_spec_yield = float(row.get("upper_yield_spec_yield", 0))
            static_yield_spec_yield = float(row.get("static_yield_spec_yield", 0))
            couple_yield_spec_yield = float(row.get("couple_yield_spec_yield", 0))
            balancing_oe_yield_spec_yield = float(row.get("balancing_oe_yield_spec_yield", 0))
            warehouse_yield_spec_yield = float(row.get("warehouse_yield_spec_yield", 0))
            visual_yield_spec_yield = float(row.get("visual_yield_spec_yield", 0))
            
            class_spec_yield = float(row.get("class_spec_yield", 0))
            balancing_spec_yield = float(row.get("balancing_spec_yield", 0))
            geometry_spec_yield = float(row.get("geometry_spec_yield", 0))
            
            threshold_value_rfpp_cw_kg = float(row.get("threshold_value_rfpp_cw_kg", 5))
            threshold_value_rfpp_ccw_kg = float(row.get("threthreshold_value_rfpp_ccw_kgshold_value_bal", 5))
            threshold_value_rfh1_cw_kg = float(row.get("threshold_value_rfh1_cw_kg", 5))
            threshold_value_rfh1_ccw_kg = float(row.get("threshold_value_rfh1_ccw_kg", 5))
            threshold_value_rfh2_cw_kg = float(row.get("threshold_value_rfh2_cw_kg", 5))
            threshold_value_rfh2_ccw_kg = float(row.get("threshold_value_rfh2_ccw_kg", 5))
            threshold_value_lfpp_cw_kg = float(row.get("threshold_value_lfpp_cw_kg", 5))
            threshold_value_lfpp_ccw_kg = float(row.get("threshold_value_lfpp_ccw_kg", 5))
            threshold_value_lsft_cw_kg = float(row.get("threshold_value_lsft_cw_kg", 5))
            threshold_value_lsft_ccw_kg = float(row.get("threshold_value_lsft_ccw_kg", 5))
            threshold_value_lfh1_cw_kg = float(row.get("threshold_value_lfh1_cw_kg", 5))
            threshold_value_lfh1_ccw_kg = float(row.get("threshold_value_lfh1_ccw_kg", 5))
            threshold_value_plysteer_kg = float(row.get("threshold_value_plysteer_kg", 5))
            threshold_value_rfmxa_cw_deg = float(row.get("threshold_value_rfmxa_cw_deg", 5))
            threshold_value_rfmxa_ccw_deg = float(row.get("threshold_value_rfmxa_ccw_deg", 5))
            threshold_value_rfa1_cw_deg = float(row.get("threshold_value_rfa1_cw_deg", 5))
            threshold_value_rfa1_ccw_deg = float(row.get("threshold_value_rfa1_ccw_deg", 5))
            threshold_value_rfa2_cw_deg = float(row.get("threshold_value_rfa2_cw_deg", 5))
            threshold_value_rfa2_ccw_deg = float(row.get("threshold_value_rfa2_ccw_deg", 5))
            threshold_value_lfmxa_cw_deg = float(row.get("threshold_value_lfmxa_cw_deg", 5))
            threshold_value_lfmxa_ccw_deg = float(row.get("threshold_value_lfmxa_ccw_deg", 5))
            threshold_value_lfa1_cw_deg = float(row.get("threshold_value_lfa1_cw_deg", 5))
            threshold_value_lfa1_ccw_deg = float(row.get("threshold_value_lfa1_ccw_deg", 5))
            threshold_value_cony_kg = float(row.get("threshold_value_cony_kg", 5))
            threshold_value_bal_static_g = float(row.get("threshold_value_bal_static_g", 5))
            threshold_value_bal_static_deg = float(row.get("threshold_value_bal_static_deg", 5))
            threshold_value_bal_couple_g = float(row.get("threshold_value_bal_couple_g", 5))
            threshold_value_bal_couple_deg = float(row.get("threshold_value_bal_couple_deg", 5))
            threshold_value_bal_dynamic_botplane_g = float(row.get("threshold_value_bal_dynamic_botplane_g", 5))
            threshold_value_bal_dynamic_topplane_g = float(row.get("threshold_value_bal_dynamic_topplane_g", 5))
            threshold_value_bal_dynamic_botplane_deg = float(row.get("threshold_value_bal_dynamic_botplane_deg", 5))
            threshold_value_bal_dynamic_topplane_deg = float(row.get("threshold_value_bal_dynamic_topplane_deg", 5))
            threshold_value_geo_bul_botbul_mm = float(row.get("threshold_value_geo_bul_botbul_mm", 5))
            threshold_value_geo_bul_topbul_mm = float(row.get("threshold_value_geo_bul_topbul_mm", 5))
            threshold_value_geo_bul_botbul_deg = float(row.get("threshold_value_geo_bul_botbul_deg", 5))
            threshold_value_geo_bul_topdep_deg = float(row.get("threshold_value_geo_bul_topdep_deg", 5))
            threshold_value_geo_dep_botdep_mm = float(row.get("threshold_value_geo_dep_botdep_mm", 5))
            threshold_value_geo_dep_topdep_mm = float(row.get("threshold_value_geo_dep_topdep_mm", 5))
            threshold_value_geo_dep_botdep_deg = float(row.get("threshold_value_geo_dep_botdep_deg", 5))
            threshold_value_geo_dep_topbul_deg = float(row.get("threshold_value_geo_dep_topbul_deg", 5))
            threshold_value_geo_rro_crro_mm = float(row.get("threshold_value_geo_rro_crro_mm", 5))
            threshold_value_geo_rro_crroh1_mm = float(row.get("threshold_value_geo_rro_crroh1_mm", 5))
            threshold_value_geo_lro_blro_mm = float(row.get("threshold_value_geo_lro_blro_mm", 5))
            threshold_value_geo_lro_blroh1_mm = float(row.get("threshold_value_geo_lro_blroh1_mm", 5))
            threshold_value_geo_lro_tlro_mm = float(row.get("threshold_value_geo_lro_tlro_mm", 5))
            threshold_value_geo_lro_tlroh1_mm = float(row.get("threshold_value_geo_lro_tlroh1_mm", 5))
            threshold_value_geo_rro_crro_deg = float(row.get("threshold_value_geo_rro_crro_deg", 5))
            threshold_value_geo_rro_crroh1_deg = float(row.get("threshold_value_geo_rro_crroh1_deg", 5))
            threshold_value_geo_lro_blro_deg = float(row.get("threshold_value_geo_lro_blro_deg", 5))
            threshold_value_geo_lro_blroh1_deg = float(row.get("threshold_value_geo_lro_blroh1_deg", 5))
            threshold_value_geo_lro_tlro_deg = float(row.get("threshold_value_geo_lro_tlro_deg", 5))
            threshold_value_geo_lro_tlroh1_deg = float(row.get("threshold_value_geo_lro_tlroh1_deg", 5))

            threshold_value_yield_rfv_yield = float(row.get("threshold_value_yield_rfv_yield", 5))
            threshold_value_yield_rfh1_yield = float(row.get("threshold_value_yield_rfh1_yield", 5))
            threshold_value_yield_rfh2_yield = float(row.get("threshold_value_yield_rfh2_yield", 5))
            threshold_value_yield_lfv_yield = float(row.get("threshold_value_yield_lfv_yield", 5))
            threshold_value_yield_lsft_yield = float(row.get("threshold_value_yield_lsft_yield", 5))
            threshold_value_yield_lfh1_yield = float(row.get("threshold_value_yield_lfh1_yield", 5))
            threshold_value_yield_conicity_yield = float(row.get("threshold_value_yield_conicity_yield", 5))
            threshold_value_yield_oe_yield = float(row.get("threshold_value_yield_oe_yield", 5))
            threshold_value_yield_rfmxa_yield = float(row.get("threshold_value_yield_rfmxa_yield", 5))
            threshold_value_yield_rfa1_yield = float(row.get("threshold_value_yield_rfa1_yield", 5))
            threshold_value_yield_rfa2_yield = float(row.get("threshold_value_yield_rfa2_yield", 5))
            threshold_value_yield_lfxma_yield = float(row.get("threshold_value_yield_lfxma_yield", 5))
            threshold_value_yield_lfa1_yield = float(row.get("threshold_value_yield_lfa1_yield", 5))
            threshold_value_yield_central_radial_runout_yield = float(row.get("threshold_value_yield_central_radial_runout_yield", 5))
            threshold_value_yield_central_radial_runout_first_harmonic_yield = float(row.get("threshold_value_yield_central_radial_runout_first_harmonic_yield", 5))
            threshold_value_yield_lateral_run_out_top_over_yield = float(row.get("threshold_value_yield_lateral_run_out_top_over_yield", 5))
            threshold_value_yield_lateral_run_out_bottom_over_yield = float(row.get("threshold_value_yield_lateral_run_out_bottom_over_yield", 5))
            threshold_value_yield_lateral_run_out_yield = float(row.get("threshold_value_yield_lateral_run_out_yield", 5))
            threshold_value_yield_lateral_run_out_bottom_first_harmonic_yield = float(row.get("threshold_value_yield_lateral_run_out_bottom_first_harmonic_yield", 5))
            threshold_value_yield_lateral_run_out_top_first_harmonic_yield = float(row.get("threshold_value_yield_lateral_run_out_top_first_harmonic_yield", 5))
            threshold_value_yield_lateral_run_out_harmonic_yield = float(row.get("threshold_value_yield_lateral_run_out_harmonic_yield", 5))
            threshold_value_yield_bulge_top_over_yield = float(row.get("threshold_value_yield_bulge_top_over_yield", 5))
            threshold_value_yield_bulge_bottom_over_yield = float(row.get("threshold_value_yield_bulge_bottom_over_yield", 5))
            threshold_value_yield_bulge_yield = float(row.get("threshold_value_yield_bulge_yield", 5))
            threshold_value_yield_dent_top_over_yield = float(row.get("threshold_value_yield_dent_top_over_yield", 5))
            threshold_value_yield_dent_bottom_over_yield = float(row.get("threshold_value_yield_dent_bottom_over_yield", 5))
            threshold_value_yield_dent_yield = float(row.get("threshold_value_yield_dent_yield", 5))
            threshold_value_yield_geometry_oe_yield = float(row.get("threshold_value_yield_geometry_oe_yield", 5))
            threshold_value_yield_lower_yield = float(row.get("threshold_value_yield_lower_yield", 5))
            threshold_value_yield_upper_yield = float(row.get("threshold_value_yield_upper_yield", 5))
            threshold_value_yield_static_yield = float(row.get("threshold_value_yield_static_yield", 5))
            threshold_value_yield_couple_yield = float(row.get("threshold_value_yield_couple_yield", 5))
            threshold_value_yield_balancing_oe_yield = float(row.get("threshold_value_yield_balancing_oe_yield", 5))
            threshold_value_yield_warehouse_yield = float(row.get("threshold_value_yield_warehouse_yield", 5))
            threshold_value_yield_visual_yield = float(row.get("threshold_value_yield_visual_yield", 5))
            threshold_value_yield = float(row.get("threshold_value_yield", 5))

            class_threshold_value_yield_class_yield = float(row.get("class_threshold_value_yield_class_yield", 5))
            threshold_value_yield_balancing_yield = float(row.get("threshold_value_yield_balancing_yield", 5))
            threshold_value_yield_geometry_yield = float(row.get("threshold_value_yield_geometry_yield", 5))

            for gt_code in gt_codes:
                gt_code = str(gt_code).strip()
                
                # model_data = merge_df[(merge_df["gt_code"] == model_number) & (merge_df["tbm"].isin(active_machines))].copy()

                # if not model_data.empty:
                #     print(f"Condition satisfied at line 2504. Records found: {len(model_data)}")
                #     print("At line 4051 model_data:",model_data)
                # else:
                #     print("Condition NOT satisfied at line 2504. No matching records found.")


                # Filter merge_df for each gt_code individually
                model_data = merge_df[
                    (merge_df["gt_code"] == model_number) &  # Changed from modelno to gt_code
                    (merge_df["tbm"].isin(active_machines))
                ].copy() 

                total_barcodes_model = len(model_data)
                model_data.sort_values(["date_of_tbm"], ascending=True, inplace=True)
                alert_map = {}

                # Virgin/Final yield split: store raw barcode rows, then continue main
                # processing on virgin data (first occurrence per barcode).
                virgin_data, final_data, _vf_msgs = split_and_store_virgin_final(model_data, "date_of_tbm", "TBM")
                output_messages.extend(_vf_msgs)
                model_data = virgin_data.copy()
                total_barcodes_model = len(model_data)

                if total_barcodes_model < target_lot_size:
                    output_messages.append(f"Skipping TBM model {model_number} due to insufficient barcodes ({total_barcodes_model} < {target_lot_size}).")
                    continue

                for tbm_value, tbm_data in model_data.groupby("tbm"):
                    # NEW: Check if tbm_value (tbm) is active
                    # tbm_data = tbm_data[tbm_data["tbm_machine_model_lot_id"].isnull()].copy()
                    if tbm_data.empty:
                        continue
                    total_tbm_barcodes = len(tbm_data)
                    machine_no = str(tbm_value)
                    machine_row = machine_df[machine_df["tbm"] == machine_no]
                    if machine_row.empty:
                        output_messages.append(f"Skipping TBM tbm {machine_no} for model {model_number} due to unknown tbm target.")
                        continue
                    if total_tbm_barcodes < target_lot_size:
                        output_messages.append(f"Skipping TBM tbm {machine_no} for model {model_number} due to insufficient tbm barcodes ({total_tbm_barcodes} < {target_lot_size}).")
                        continue

                    barcode_counter = 0

                    while barcode_counter < total_tbm_barcodes:
                        remaining_model = total_tbm_barcodes - barcode_counter
                        if remaining_model < target_lot_size:
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
                        # logging.info(f"Initial tbm_previous_machine_model_lot_id: {tbm_previous_machine_model_lot_id} for model {model_number}, machine {machine_no}")
                        if tbm_previous_machine_model_lot_id is None and last_lot_number > 1:
                            prev_lot_number = last_lot_number - 1
                            prev_lot_id = f"TBM_{machine_no}_{model_number}_{prev_lot_number}"
                            logging.info(f"Fetching previous lot for lot_id: {prev_lot_id}")
                            try:
                                prev_lot = averages_collect.find_one(
                                    {
                                        "gt_code": model_number,
                                        "machine": machine_no,
                                        "lot_id": prev_lot_id
                                    },
                                    sort=[("tbm_machine_model_lot_id", -1)]
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
                                        "gt_code": model_number,
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
                            # logging.info(f"First lot for model {model_number}, machine {machine_no}. Setting previous_lot_averages to empty.")
                        # logging.info(f"prev_key: {prev_key}, prev_lot_avg: {prev_lot_avg}")

                        lot_doc = {
                            "gt_code": model_number,
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
                            "lsft_yield": lsft_yield(batch_data),
                            "lfh1_yield": lfh1_yield(batch_data),
                            "conicity_yield": conicity_yield(batch_data),
                            "oe_yield": oe_yield(batch_data),
                            "rfmxa_yield": rfmxa_yield(batch_data),
                            "rfa1_yield": rfa1_yield(batch_data),
                            "rfa2_yield": rfa2_yield(batch_data),
                            "lfxma_yield": lfxma_yield(batch_data),
                            "lfa1_yield": lfa1_yield(batch_data),
                            "central_radial_runout_yield": central_radial_runout_yield(batch_data),
                            "central_radial_runout_first_harmonic_yield": central_radial_runout_first_harmonic_yield(batch_data),
                            "lateral_run_out_top_over_yield": lateral_run_out_top_over_yield(batch_data),
                            "lateral_run_out_bottom_over_yield": lateral_run_out_bottom_over_yield(batch_data),
                            "lateral_run_out_yield": lateral_run_out_yield(batch_data),
                            "lateral_run_out_bottom_first_harmonic_yield": lateral_run_out_bottom_first_harmonic_yield(batch_data),
                            "lateral_run_out_top_first_harmonic_yield": lateral_run_out_top_first_harmonic_yield(batch_data),
                            "lateral_run_out_harmonic_yield": lateral_run_out_harmonic_yield(batch_data),
                            "bulge_top_over_yield": bulge_top_over_yield(batch_data),
                            "bulge_bottom_over_yield": bulge_bottom_over_yield(batch_data),
                            "bulge_yield": bulge_yield(batch_data),
                            "dent_top_over_yield": dent_top_over_yield(batch_data),
                            "dent_bottom_over_yield": dent_bottom_over_yield(batch_data),
                            "dent_yield": dent_yield(batch_data),
                            "geometry_oe_yield": geometry_oe_yield(batch_data),
                            "lower_yield": lower_yield(batch_data),
                            "upper_yield": upper_yield(batch_data),
                            "static_yield": static_yield(batch_data),
                            "couple_yield": couple_yield(batch_data),
                            "balancing_oe_yield": balancing_oe_yield(batch_data),
                            "warehouse_yield": warehouse_yield(batch_data),
                            "visual_yield": visual_yield(batch_data)})
                                                                                

                        grouped_averages = {}
                        param_spec_map = {
                            "cony_kg": spec_cony_kg,
                            "rfpp_cw_kg": spec_rfpp_cw_kg,
                            "rfpp_ccw_kg": spec_rfpp_ccw_kg,
                            "rfh1_cw_kg": spec_rfh1_cw_kg,
                            "rfh1_ccw_kg": spec_rfh1_ccw_kg,
                            "lfpp_cw_kg": spec_lfpp_cw_kg,
                            "lfpp_ccw_kg": spec_lfpp_ccw_kg,
                            'rfh2_cw_kg': spec_rfh2_cw_kg,
                            'rfh2_ccw_kg': spec_rfh2_ccw_kg,
                            'lsft_cw_kg': spec_lsft_cw_kg,
                            'lsft_ccw_kg': spec_lsft_ccw_kg,
                            'lfh1_cw_kg': spec_lfh1_cw_kg,
                            'lfh1_ccw_kg': spec_lfh1_ccw_kg,
                            'plysteer_kg': spec_plysteer_kg,
                            'rfmxa_ccw_deg':spec_rfmxa_ccw_deg,
                            'rfa1_cw_deg': spec_rfa1_cw_deg,
                            'rfa1_ccw_deg': spec_rfa1_ccw_deg,
                            'rfa2_cw_deg': spec_rfa2_cw_deg,
                            'rfa2_ccw_deg': spec_rfa2_ccw_deg,
                            'lfmxa_cw_deg': spec_lfmxa_cw_deg,
                            'lfmxa_ccw_deg': spec_lfmxa_ccw_deg,
                            'lfa1_cw_deg': spec_lfa1_cw_deg,
                            'lfa1_ccw_deg': spec_lfa1_ccw_deg,
                            'couple_g': spec_couple_g,
                            'botplane_deg': spec_dynamic_botplane_deg,
                            'topplane_deg': spec_dynamic_topplane_deg,
                            'couple_deg': spec_couple_deg,
                            'static_deg': spec_static_deg,
                            'botbul_mm': spec_bul_botbul_mm,
                            'botdep_mm': spec_dep_botdep_mm,
                            'topbul_mm': spec_bul_topbul_mm,
                            'topdep_mm': spec_dep_topdep_mm,
                            'blro_mm': spec_lro_blro_mm,
                            'blroh1_mm': spec_lro_blroh1_mm,
                            'crro_mm': spec_rro_crro_mm,
                            'crroh1_mm': spec_rro_crroh1_mm,
                            'tlro_mm': spec_lro_tlro_mm,
                            'tlroh1_mm': spec_lro_tlroh1_mm,
                            'botbul_deg': spec_bul_botbul_deg,
                            'botdep_deg': spec_dep_botdep_deg,
                            'topbul_deg': spec_dep_topbul_deg,
                            'topdep_deg': spec_bul_topdep_deg,
                            'blro_deg': spec_lro_blro_deg,
                            'blroh1_deg': spec_lro_blroh1_deg,
                            'crro_deg': spec_rro_crro_deg,
                            'crroh1_deg': spec_rro_crroh1_deg,
                            'tlro_deg': spec_lro_tlro_deg,
                            'tlroh1_deg': spec_lro_tlroh1_deg,
                            'botplane_g': spec_dynamic_botplane_g,
                            'topplane_g': spec_dynamic_topplane_g,
                            "static_g": spec_static_g,

                            "rfv_yield": rfv_yield_spec_yield,
                            "rfh1_yield": rfh1_yield_spec_yield,
                            "rfh2_yield": rfh2_yield_spec_yield,
                            "lfv_yield": lfv_yield_spec_yield,
                            "lsft_yield": lsft_yield_spec_yield,
                            "lfh1_yield": lfh1_yield_spec_yield,
                            "conicity_yield": conicity_yield_spec_yield,
                            "oe_yield": oe_yield_spec_yield,
                            "rfmxa_yield": rfmxa_yield_spec_yield,
                            "rfa1_yield": rfa1_yield_spec_yield,
                            "rfa2_yield": rfa2_yield_spec_yield,
                            "lfxma_yield": lfxma_yield_spec_yield,
                            "lfa1_yield": lfa1_yield_spec_yield,
                            "central_radial_runout_yield": central_radial_runout_yield_spec_yield,
                            "central_radial_runout_first_harmonic_yield": central_radial_runout_first_harmonic_yield_spec_yield,
                            "lateral_run_out_top_over_yield": lateral_run_out_top_over_yield_spec_yield,
                            "lateral_run_out_bottom_over_yield": lateral_run_out_bottom_over_yield_spec_yield,
                            "lateral_run_out_yield": lateral_run_out_yield_spec_yield,
                            "lateral_run_out_bottom_first_harmonic_yield": lateral_run_out_bottom_first_harmonic_yield_spec_yield,
                            "lateral_run_out_top_first_harmonic_yield": lateral_run_out_top_first_harmonic_yield_spec_yield,
                            "lateral_run_out_harmonic_yield": lateral_run_out_harmonic_yield_spec_yield,
                            "bulge_top_over_yield": bulge_top_over_yield_spec_yield,
                            "bulge_bottom_over_yield": bulge_bottom_over_yield_spec_yield,
                            "bulge_yield": bulge_yield_spec_yield,
                            "dent_top_over_yield": dent_top_over_yield_spec_yield,
                            "dent_bottom_over_yield": dent_bottom_over_yield_spec_yield,
                            "dent__yield": dent_yield_spec_yield,
                            "geometry_oe_yield": geometry_oe_yield_spec_yield,
                            "lower_yield": lower_yield_spec_yield,
                            "upper_yield": upper_yield_spec_yield,
                            "static_yield": static_yield_spec_yield,
                            "couple_yield": couple_yield_spec_yield,
                            "balancing_oe_yield": balancing_oe_yield_spec_yield,
                            "warehouse_yield": warehouse_yield_spec_yield,
                            "visual_yield": visual_yield_spec_yield
                        }
                        try:
                            batch_data["ubno"] = batch_data["ubno"].astype(str).str.strip()
                            batch_data.sort_values("date_of_ufm", ascending=True, inplace=True)
                            ubno_grouped = batch_data.groupby("ubno")
                            ubno_records = []
                            for ubno, group in ubno_grouped:
                                group.sort_values("date_of_ufm", ascending=True)
                                record = {
                                    "ubno": ubno,
                                    "barcode_count": len(group["barcode"]) if "barcode" in group.columns else 0,
                                    "tub_date_start_time": group["date_of_ufm"].iloc[0] if not group["date_of_ufm"].empty else None,
                                    "tub_date_end_time": group["date_of_ufm"].iloc[-1] if not group["date_of_ufm"].empty else None,
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
                                        if param == "cony_kg" and spec_cony_kg is not None and avg_val is not None:
                                            exceeded = not (-spec_cony_kg <= avg_val <= spec_cony_kg)
                                        if spec is not None and param not in ["cony_kg"] and avg_val is not None:
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
                            grouped_averages["ubno"] = ubno_records
                        except Exception as e:
                            logging.error(f"Error processing ubno grouped averages: {e}")
                            grouped_averages["ubno"] = []

                        batch_data1 = batch_data.copy()
                        batch_data.sort_values("date_of_tbm", ascending=True, inplace=True)
                        batch_data1.sort_values("date_of_ufm", ascending=True, inplace=True)
                        machine_lot_start_build_time = batch_data["date_of_tbm"].iloc[0]
                        machine_lot_end_build_time = batch_data["date_of_tbm"].iloc[-1]
                        tub_date_start_time = batch_data1["date_of_ufm"].iloc[0]
                        tub_date_end_time = batch_data1["date_of_ufm"].iloc[-1]
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
                                    if param == "cony_kg" and spec_cony_kg is not None:
                                        if abs(current_val - prev_val) > threshold_value_cony_kg:
                                            shift = True
                                        if not (-spec_cony_kg <= current_val <= spec_cony_kg):
                                            shift_occurred_spec = True
                                    if param == "rfpp_cw_kg" and spec_rfpp_cw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_rfpp_cw_kg:
                                            shift = True
                                        if current_val > spec_rfpp_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfpp_ccw_kg" and spec_rfpp_ccw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_rfpp_ccw_kg:
                                            shift = True
                                        if current_val > spec_rfpp_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfh1_cw_kg" and spec_rfh1_cw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_rfh1_cw_kg:
                                            shift = True
                                        if current_val > spec_rfh1_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfh1_ccw_kg" and spec_rfh1_ccw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_rfh1_ccw_kg:
                                            shift = True
                                        if current_val > spec_rfh1_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfh2_cw_kg" and spec_rfh2_cw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_rfh2_cw_kg:
                                            shift = True
                                        if current_val > spec_rfh2_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfh2_ccw_kg" and spec_rfh2_ccw_kg is not None:
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
                                                                                                                                                                                                                                                                                
                                                                                                                                                                                                                                                                                                                                                                                                    
                                                                                      
                                        if (current_val - prev_val) > threshold_value_rfh2_ccw_kg:
                                            shift = True
                                        if current_val > spec_rfh2_ccw_kg:
                                            shift_occurred_spec = True
                                    # if param in ("lfpp_cw_kg", "lfpp_ccw_kg") and spec_lfpp is not None:
                                    #     if (current_val - prev_val) > threshold_value:
                                    #         shift = True
                                    #     if current_val > spec_lfpp:
                                    #         shift_occurred_spec = True
                                    if param == "lfpp_cw_kg" and spec_lfpp_cw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_lfpp_cw_kg:
                                            shift = True
                                        if current_val > spec_lfpp_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "lfpp_ccw_kg" and spec_lfpp_ccw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_lfpp_ccw_kg:
                                            shift = True
                                        if current_val > spec_lfpp_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "lsft_cw_kg" and spec_lsft_cw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_lsft_cw_kg:
                                            shift = True
                                        if current_val > spec_lsft_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "lsft_ccw_kg" and spec_lsft_ccw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_lsft_ccw_kg:
                                            shift = True
                                        if current_val > spec_lsft_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "lfh1_cw_kg" and spec_lfh1_cw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_lfh1_cw_kg:
                                            shift = True
                                        if current_val > spec_lfh1_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "lfh1_ccw_kg" and spec_lfh1_ccw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_lfh1_ccw_kg:
                                            shift = True
                                        if current_val > spec_lfh1_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "plysteer_kg" and spec_plysteer_kg is not None:
                                        if (current_val - prev_val) > threshold_value_plysteer_kg:
                                            shift = True
                                        if current_val > spec_plysteer_kg:
                                            shift_occurred_spec = True
                                    if param == "rfmxa_ccw_deg" and spec_rfmxa_ccw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_rfmxa_ccw_deg:
                                            shift = True
                                        if current_val > spec_rfmxa_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "rfa1_cw_deg" and spec_rfa1_cw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_rfa1_cw_deg:
                                            shift = True
                                        if current_val > spec_rfa1_cw_deg:
                                            shift_occurred_spec = True
                                    if param == "rfa1_ccw_deg" and spec_rfa1_ccw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_rfa1_ccw_deg:
                                            shift = True
                                        if current_val > spec_rfa1_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "rfa2_cw_deg" and spec_rfa2_cw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_rfa2_cw_deg:
                                            shift = True
                                        if current_val > spec_rfa2_cw_deg:
                                            shift_occurred_spec = True
                                    if param == "rfa2_ccw_deg" and spec_rfa2_ccw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_rfa2_ccw_deg:
                                            shift = True
                                        if current_val > spec_rfa2_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "lfmxa_cw_deg" and spec_lfmxa_cw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_lfmxa_cw_deg:
                                            shift = True
                                        if current_val > spec_lfmxa_cw_deg:
                                            shift_occurred_spec = True
                                    if param == "lfmxa_ccw_deg" and spec_lfmxa_ccw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_lfmxa_ccw_deg:
                                            shift = True
                                        if current_val > spec_lfmxa_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "lfa1_cw_deg" and spec_lfa1_cw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_lfa1_cw_deg:
                                            shift = True
                                        if current_val > spec_lfa1_cw_deg:
                                            shift_occurred_spec = True
                                    if param == "lfa1_ccw_deg" and spec_lfa1_ccw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_lfa1_ccw_deg:
                                            shift = True
                                        if current_val > spec_lfa1_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "static_g" and spec_static_g is not None:
                                        if (current_val - prev_val) > threshold_value_bal_static_g:
                                            shift = True
                                        if current_val > spec_static_g:
                                            shift_occurred_spec = True
                                    if param == "static_deg" and spec_static_deg is not None:
                                        if (current_val - prev_val) > threshold_value_bal_static_deg:
                                            shift = True
                                        if current_val > spec_static_deg:
                                            shift_occurred_spec = True
                                    if param == "couple_g" and spec_couple_g is not None:
                                        if (current_val - prev_val) > threshold_value_bal_couple_g:
                                            shift = True
                                        if current_val > spec_couple_g:
                                            shift_occurred_spec = True
                                    if param == "couple_deg" and spec_couple_deg is not None:
                                        if (current_val - prev_val) > threshold_value_bal_couple_deg:
                                            shift = True
                                        if current_val > spec_couple_deg:
                                            shift_occurred_spec = True
                                    if param == "botbul_mm" and spec_bul_botbul_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_bul_botbul_mm:
                                            shift = True
                                        if current_val > spec_bul_botbul_mm:
                                            shift_occurred_spec = True
                                    if param == "topbul_mm" and spec_bul_topbul_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_bul_topbul_mm:
                                            shift = True
                                        if current_val > spec_bul_topbul_mm:
                                            shift_occurred_spec = True
                                    if param == "botbul_deg" and spec_bul_botbul_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_bul_botbul_deg:
                                            shift = True
                                        if current_val > spec_bul_botbul_deg:
                                            shift_occurred_spec = True
                                    if param == "topdep_deg" and spec_bul_topdep_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_bul_topdep_deg:
                                            shift = True
                                        if current_val > spec_bul_topdep_deg:
                                            shift_occurred_spec = True
                                    if param == "botdep_mm" and spec_dep_botdep_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_dep_botdep_mm:
                                            shift = True
                                        if current_val > spec_dep_botdep_mm:
                                            shift_occurred_spec = True
                                    if param == "topdep_mm" and spec_dep_topdep_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_dep_topdep_mm:
                                            shift = True
                                        if current_val > spec_dep_topdep_mm:
                                            shift_occurred_spec = True
                                    if param == "botdep_deg" and spec_dep_botdep_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_dep_botdep_deg:
                                            shift = True
                                        if current_val > spec_dep_botdep_deg:
                                            shift_occurred_spec = True
                                    if param == "topbul_deg" and spec_dep_topbul_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_dep_topbul_deg:
                                            shift = True
                                        if current_val > spec_dep_topbul_deg:
                                            shift_occurred_spec = True
                                    if param == "crro_mm" and spec_rro_crro_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_rro_crro_mm:
                                            shift = True
                                        if current_val > spec_rro_crro_mm:
                                            shift_occurred_spec = True
                                    if param == "crroh1_mm" and spec_rro_crroh1_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_rro_crroh1_mm:
                                            shift = True
                                        if current_val > spec_rro_crroh1_mm:
                                            shift_occurred_spec = True
                                    if param == "blro_mm" and spec_lro_blro_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_lro_blro_mm:
                                            shift = True
                                        if current_val > spec_lro_blro_mm:
                                            shift_occurred_spec = True
                                    if param == "blroh1_mm" and spec_lro_blroh1_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_lro_blroh1_mm:
                                            shift = True
                                        if current_val > spec_lro_blroh1_mm:
                                            shift_occurred_spec = True
                                    if param == "tlro_mm" and spec_lro_tlro_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_lro_tlro_mm:
                                            shift = True
                                        if current_val > spec_lro_tlro_mm:
                                            shift_occurred_spec = True
                                    if param == "tlroh1_mm" and spec_lro_tlroh1_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_lro_tlroh1_mm:
                                            shift = True
                                        if current_val > spec_lro_tlroh1_mm:
                                            shift_occurred_spec = True
                                    if param == "crro_deg" and spec_rro_crro_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_rro_crro_deg:
                                            shift = True
                                        if current_val > spec_rro_crro_deg:
                                            shift_occurred_spec = True
                                    if param == "crroh1_deg" and spec_rro_crroh1_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_rro_crroh1_deg:
                                            shift = True
                                        if current_val > spec_rro_crroh1_deg:
                                            shift_occurred_spec = True
                                    if param == "blro_deg" and spec_lro_blro_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_lro_blro_deg:
                                            shift = True
                                        if current_val > spec_lro_blro_deg:
                                            shift_occurred_spec = True
                                    if param == "blroh1_deg" and spec_lro_blroh1_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_lro_blroh1_deg:
                                            shift = True
                                        if current_val > spec_lro_blroh1_deg:
                                            shift_occurred_spec = True
                                    if param == "tlro_deg" and spec_lro_tlro_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_lro_tlro_deg:
                                            shift = True
                                        if current_val > spec_lro_tlro_deg:
                                            shift_occurred_spec = True
                                    if param == "tlroh1_deg" and spec_lro_tlroh1_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_lro_tlroh1_deg:
                                            shift = True
                                        if current_val > spec_lro_tlroh1_deg:
                                            shift_occurred_spec = True
                                    if param == "botplane_g" and spec_dynamic_botplane_g is not None:
                                        if (current_val - prev_val) > threshold_value_bal_dynamic_botplane_g:
                                            shift = True
                                        if current_val > spec_dynamic_botplane_g:
                                            shift_occurred_spec = True
                                    if param == "topplane_g" and spec_dynamic_topplane_g is not None:
                                        if (current_val - prev_val) > threshold_value_bal_dynamic_topplane_g:
                                            shift = True
                                        if current_val > spec_dynamic_topplane_g:
                                            shift_occurred_spec = True
                                    if param == "botplane_deg" and spec_dynamic_botplane_deg is not None:
                                        if (current_val - prev_val) > threshold_value_bal_dynamic_botplane_deg:
                                            shift = True
                                        if current_val > spec_dynamic_botplane_deg:
                                            shift_occurred_spec = True
                                    if param == "topplane_deg" and spec_dynamic_topplane_deg is not None:
                                        if (current_val - prev_val) > threshold_value_bal_dynamic_topplane_deg:
                                            shift = True
                                        if current_val > spec_dynamic_topplane_deg:
                                            shift_occurred_spec = True

                                    if param in yield_params:
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True

                                    # if param == "oe_yield":
                                    #     spec_yield = param_spec_map.get(param)
                                    #     if spec_yield is not None:
                                    #         if (prev_val > current_val) and (abs(current_val - prev_val) > (class_threshold_value_yield_class_yield)):
                                    #             shift = True
                                    #         if abs(current_val < spec_yield):
                                    #             shift_occurred_spec = True
                                    # if param == "balancing_oe_yield":
                                    #     spec_yield = param_spec_map.get(param)
                                    #     if spec_yield is not None:
                                    #         if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_balancing_yield)):
                                    #             shift = True
                                    #         if abs(current_val < spec_yield):
                                    #             shift_occurred_spec = True
                                    # if param == "geometry_oe_yield":
                                    #     spec_yield = param_spec_map.get(param)
                                    #     if spec_yield is not None:
                                    #         if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_geometry_yield)):
                                    #             shift = True
                                    #         if abs(current_val < spec_yield):
                                    #             shift_occurred_spec = True
                                    
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
                                parameter_alerts.append(alert_dict)
                                if shift_occurred:
                                    overall_shift_occurred = True
                        else:
                            for param in numeric_params + yield_params:
                                shift_occurred_thershold = False
                                shift_occurred_spec = False
                                current_val = lot_averages.get(param)
                                if current_val is not None:
                                    if param == "cony_kg" and spec_cony_kg is not None:
                                        if not (-spec_cony_kg <= current_val <= spec_cony_kg):
                                            shift_occurred_spec = True
                                    if param == "rfpp_cw_kg" and spec_rfpp_cw_kg is not None:
                                        if current_val > spec_rfpp_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfpp_ccw_kg" and spec_rfpp_ccw_kg is not None:
                                        if current_val > spec_rfpp_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfh1_cw_kg" and spec_rfh1_cw_kg is not None:
                                        if current_val > spec_rfh1_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfh1_ccw_kg" and spec_rfh1_ccw_kg is not None:
                                        if current_val > spec_rfh1_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfh2_cw_kg" and spec_rfh2_cw_kg is not None:
                                        if current_val > spec_rfh2_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfh2_ccw_kg" and spec_rfh2_ccw_kg is not None:
                                        if current_val > spec_rfh2_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "lfpp_cw_kg" and spec_lfpp_cw_kg is not None:
                                        if current_val > spec_lfpp_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "lfpp_ccw_kg" and spec_lfpp_ccw_kg is not None:
                                        if current_val > spec_lfpp_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "lsft_cw_kg" and spec_lsft_cw_kg is not None:
                                        if current_val > spec_lsft_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "lsft_ccw_kg" and spec_lsft_ccw_kg is not None:
                                        if current_val > spec_lsft_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "lfh1_cw_kg" and spec_lfh1_cw_kg is not None:
                                        if current_val > spec_lfh1_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "lfh1_ccw_kg" and spec_lfh1_ccw_kg is not None:
                                        if current_val > spec_lfh1_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "plysteer_kg" and spec_plysteer_kg is not None:
                                        if current_val > spec_plysteer_kg:
                                            shift_occurred_spec = True
                                    if param == "rfmxa_ccw_deg" and spec_rfmxa_ccw_deg is not None:
                                        if current_val > spec_rfmxa_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "rfa1_cw_deg" and spec_rfa1_cw_deg is not None:
                                        if current_val > spec_rfa1_cw_deg:
                                            shift_occurred_spec = True
                                    if param == "rfa1_ccw_deg" and spec_rfa1_ccw_deg is not None:
                                        if current_val > spec_rfa1_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "rfa2_cw_deg" and spec_rfa2_cw_deg is not None:
                                        if current_val > spec_rfa2_cw_deg:
                                            shift_occurred_spec = True
                                    if param == "rfa2_ccw_deg" and spec_rfa2_ccw_deg is not None:
                                        if current_val > spec_rfa2_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "lfmxa_cw_deg" and spec_lfmxa_cw_deg is not None:
                                        if current_val > spec_lfmxa_cw_deg:
                                            shift_occurred_spec = True
                                    if param == "lfmxa_ccw_deg" and spec_lfmxa_ccw_deg is not None:
                                        if current_val > spec_lfmxa_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "lfa1_cw_deg" and spec_lfa1_cw_deg is not None:
                                        if current_val > spec_lfa1_cw_deg:
                                            shift_occurred_spec = True
                                    if param == "lfa1_ccw_deg" and spec_lfa1_ccw_deg is not None:
                                        if current_val > spec_lfa1_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "static_g" and spec_static_g is not None:
                                        if current_val > spec_static_g:
                                            shift_occurred_spec = True
                                    if param == "static_deg" and spec_static_deg is not None:
                                        if current_val > spec_static_deg:
                                            shift_occurred_spec = True
                                    if param == "couple_g" and spec_couple_deg is not None:
                                        if current_val > spec_couple_deg:
                                            shift_occurred_spec = True
                                    if param == "couple_deg" and spec_couple_deg is not None:
                                        if current_val > spec_couple_deg:
                                            shift_occurred_spec = True
                                    if param == "botbul_mm" and spec_bul_botbul_mm is not None:
                                        if current_val > spec_bul_botbul_mm:
                                            shift_occurred_spec = True
                                    if param == "topbul_mm" and spec_bul_topbul_mm is not None:
                                        if current_val > spec_bul_topbul_mm:
                                            shift_occurred_spec = True
                                    if param == "botbul_deg" and spec_bul_botbul_deg is not None:
                                        if current_val > spec_bul_botbul_deg:
                                            shift_occurred_spec = True
                                    if param == "topdep_deg" and spec_bul_topdep_deg is not None:
                                        if current_val > spec_bul_topdep_deg:
                                            shift_occurred_spec = True
                                    if param == "botdep_mm" and spec_dep_botdep_mm is not None:
                                        if current_val > spec_dep_botdep_mm:
                                            shift_occurred_spec = True
                                    if param == "topdep_mm" and spec_dep_topdep_mm is not None:
                                        if current_val > spec_dep_topdep_mm:
                                            shift_occurred_spec = True
                                    if param == "botdep_deg" and spec_dep_botdep_deg is not None:
                                        if current_val > spec_dep_botdep_deg:
                                            shift_occurred_spec = True
                                    if param == "topbul_deg" and spec_dep_topbul_deg is not None:
                                        if current_val > spec_dep_topbul_deg:
                                            shift_occurred_spec = True
                                    if param == "crro_mm" and spec_rro_crro_mm is not None:
                                        if current_val > spec_rro_crro_mm:
                                            shift_occurred_spec = True
                                    if param == "crroh1_mm" and spec_rro_crroh1_mm is not None:
                                        if current_val > spec_rro_crroh1_mm:
                                            shift_occurred_spec = True
                                    if param == "blro_mm" and spec_lro_blro_mm is not None:
                                        if current_val > spec_lro_blro_mm:
                                            shift_occurred_spec = True
                                    if param == "blroh1_mm" and spec_lro_blroh1_mm is not None:
                                        if current_val > spec_lro_blroh1_mm:
                                            shift_occurred_spec = True
                                    if param == "tlro_mm" and spec_lro_tlro_mm is not None:
                                        if current_val > spec_lro_tlro_mm:
                                            shift_occurred_spec = True
                                    if param == "tlroh1_mm" and spec_lro_tlroh1_mm is not None:
                                        if current_val > spec_lro_tlroh1_mm:
                                            shift_occurred_spec = True
                                    if param == "crro_deg" and spec_rro_crro_deg is not None:
                                        if current_val > spec_rro_crro_deg:
                                            shift_occurred_spec = True
                                    if param == "crroh1_deg" and spec_rro_crroh1_deg is not None:
                                        if current_val > spec_rro_crroh1_deg:
                                            shift_occurred_spec = True
                                    if param == "blro_deg" and spec_lro_blro_deg is not None:
                                        if current_val > spec_lro_blro_deg:
                                            shift_occurred_spec = True
                                    if param == "blroh1_deg" and spec_lro_blroh1_deg is not None:
                                        if current_val > spec_lro_blroh1_deg:
                                            shift_occurred_spec = True
                                    if param == "tlro_deg" and spec_lro_tlro_deg is not None:
                                        if current_val > spec_lro_tlro_deg:
                                            shift_occurred_spec = True
                                    if param == "tlroh1_deg" and spec_lro_tlroh1_deg is not None:
                                        if current_val > spec_lro_tlroh1_deg:
                                            shift_occurred_spec = True
                                    if param == "botplane_g" and spec_dynamic_botplane_g is not None:
                                        if current_val > spec_dynamic_botplane_g:
                                            shift_occurred_spec = True
                                    if param == "topplane_g" and spec_dynamic_topplane_g is not None:
                                        if current_val > spec_dynamic_topplane_g:
                                            shift_occurred_spec = True
                                    if param == "botplane_deg" and spec_dynamic_botplane_deg is not None:
                                        if current_val > spec_dynamic_botplane_deg:
                                            shift_occurred_spec = True
                                    if param == "topplane_deg" and spec_dynamic_topplane_deg is not None:
                                        if current_val > spec_dynamic_topplane_deg:
                                            shift_occurred_spec = True
                                    
                                    if param in yield_params:
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if current_val < spec_yield:
                                                shift_occurred_spec = True
                                
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
                                parameter_alerts.append(alert_dict)
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
                                "gt_code": model_number,
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
                            # logging.info(f"Prepared {len(deviated_parameter_alerts)} derived alerts for machine {machine_no}, lot {unique_lot_id_current}")

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

                        batch_data["tbm_shift"] = batch_data["date_of_tbm"].apply(get_shift)
                        shift_groups = batch_data.groupby('tbm_shift')
                        shifts_data_list = []
                        for shift_name, shift_data in shift_groups:
                            shift_start_time = shift_data["date_of_tbm"].iloc[0]
                            shift_end_time = shift_data["date_of_tbm"].iloc[-1]
                            shifts_data_list.append({
                                "shift": shift_name,
                                "barcode_count": len(shift_data["barcode"]) if "barcode" in shift_data.columns else 0,
                                "machine_shift_tbm_start_time": shift_start_time,
                                "machine_shift_tbm_end_time": shift_end_time,
                                "barcodes": shift_data["barcode"].tolist() if "barcode" in shift_data.columns else []
                            })

                        any_shift_flag = any(alert["shift_occurred"] for alert in parameter_alerts)

                        averages_doc = {
                            "gt_code": model_number,
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
                            "gt_code": model_number,
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
                            "oe_yield": oe_yield(batch_data),
                            "balancing_oe_yield": balancing_oe_yield(batch_data),
                            "geometry_oe_yield": geometry_oe_yield(batch_data),
                            "warehouse_yield": warehouse_yield(batch_data),
                            "visual_yield": visual_yield(batch_data),
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
                                "gt_code": model_number,
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
                        "gt_code": alert_obj["gt_code"],
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

                for alert_key, alert_obj in alert_map.items():
                    true_machine_alerts = [ma for ma in alert_obj["machine_alerts"] if ma.get("tbm_shift_occurred", False)]
                    # logging.info("Model %s, Lot %s: %s true machine alerts for triggering" % (alert_obj['gt_code'], alert_obj['lot_id'], len(true_machine_alerts)))
                    if true_machine_alerts:
                        doc_trigger = {
                            "alert_id_tbm_ufm": alert_obj["alert_id_tbm_ufm"],
                            "gt_code": alert_obj["gt_code"],
                            "lot_id": alert_obj["lot_id"],
                            "alert_flag": False,
                            "dashboard_flag": False,
                            "alert_status_check": "o",
                            "alert_status_timestamp": None,
                            "alert_status_comment": [],
                            "alert_assign_user": [],  # Add 05-07
                            "machine_alerts": true_machine_alerts,
                            "first_barcode_model": alert_obj["first_barcode_model"],
                            "last_barcode_model": alert_obj["last_barcode_model"],
                            "timestamp_alert_generated": some_function()
                        }
                        triggering_bulk_operations.append(pymongo.InsertOne(doc_trigger))

        except Exception as e:
            logging.error(f"Error processing TBM model {model_number}: {e}")
            continue

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
            # Write to alert_db_virgin_yield
            result = alert_triggering_collect.bulk_write(triggering_bulk_operations, ordered=False)
            inserted_counts["alert_triggering_tbm_with_lot_size"] = result.inserted_count
            output_messages.append(f"✅ TBM Inserted {result.inserted_count} machine_alert documents into alert_db_virgin_yield.alert_triggering_tbm_with_lot_size.")
            # NEW: Write to alert_db_final_yield
            result_dashboard = alert_triggering_collect_dashboard.bulk_write(triggering_bulk_operations, ordered=False)
            inserted_counts["alert_triggering_tbm_dashboard"] = result_dashboard.inserted_count
            output_messages.append(f"✅ TBM Inserted {result_dashboard.inserted_count} machine_alert documents into alert_db_final_yield.alert_triggering_tbm_with_lot_size.")
        except Exception as e:
            error_message = f"❌ TBM Error performing bulk write to alert_triggering collections: {e}"
            output_messages.append(error_message)
    else:
        output_messages.append("No TBM machine_alert documents to insert into alert_triggering collections.")

    if deviated_triggering_bulk_operations:
        try:
            # Write to alert_deviated_final_yield
            result = alert_triggering_deviated_collect.bulk_write(deviated_triggering_bulk_operations, ordered=False)
            inserted_counts["alert_tbm_with_lot_size"] = result.inserted_count
            output_messages.append(f"✅ TBM Inserted {result.inserted_count} deviated alert documents into alert_deviated_final_yield.alert_tbm_with_lot_size.")
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
    final_message = f"✅ TBM Lot and Alert Process completed successfully in {execution_time:.2f} seconds!"
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

def process_curing_data_and_generate_lots_alerts():
    start_time = time.time()
    output_messages = []
    # WITH lot size: restrict processing to active curing presses only.
    _restrict_active_presses = True
    inserted_counts = {}
    updated_counts = {}
    logging.info("process_curing_data_and_generate_lots_alerts function is just started")

    try:
        merge_collect = client[UNIFORMITY_BALANCING_GEOMETRY_DB][COMBINED_COLLECTION_NAME]
        channel_collect_curing = client[config_databse][channel_no_collect_curing]
        channel_collect_ufm = client[config_databse][channel_no_collect]
        lot_curing_collect = client[lot_db][lot_curing_collect_name]
        alert_collect = client[alert_table_final_yield][alert_curing_collect_name]
        averages_collect = client[average_db][averages_curing_table_name]
        machine_collect = client[config_databse][machine_no_collect]
        curing_press_collect = client[config_databse][curing_press_dict_collect]
        alert_triggering_collect = client[alert_db_virgin_yield][alert_triggering_curing_collect_name]
        alert_triggering_deviated_collect = client[alert_deviated_final_yield][alert_triggering_deviated_curing]
        alert_triggering_collect_dashboard_curing = client[alert_db_final_yield][alert_triggering_curing_collect_name]
        curing_collect_data = client[model_db][curing_collection]
        curing_gt_code_collect = client[model_db][curing_collection_machine]
        output_messages.append("✅ TBM Collections loaded successfully for lot and alert processing in both alert_db_virgin_yield and alert_db_final_yield, and model_workcenter_db.")
    except pymongo.errors.OperationFailure as e:
        error_message = f"❌ Curing Database or collection error: {e}"
        output_messages.append(error_message)
        return {"status": "error", "message": error_message, "output_log": output_messages}, 500

    try:
        active_curing_press_ids = set(curing_gt_code_collect.distinct("curing_press", {"status": "active"}))
        output_messages.append(f"✅ Fetched {len(active_curing_press_ids)} active curing_press values: {active_curing_press_ids}")
    except Exception as e:
        error_message = f"❌ Error fetching active curing_press from curing_modelno: {e}"
        output_messages.append(error_message)
        return {"status": "error", "message": "Failed to fetch active curing_press", "output_log": output_messages}, 500

    # Fetch active gt_code numbers
    try:
        # Step 1: Fetch distinct active gt_code (flatten from list fields)
        active_gtcodes = set()
        cursor = curing_collect_data.find({"status": "active"}, {"gt_code": 1})
        for doc in cursor:
            active_gtcodes.update(doc.get("gt_code", []))  # flatten the gt_code list
        output_messages.append(f"✅ Fetched {len(active_gtcodes)} active model numbers: {active_gtcodes}")
    except Exception as e:
        output_messages.append(f"❌ Error fetching active model numbers: {e}")
        return {"status": "error", "message": "Failed to fetch active model numbers", "output_log": output_messages}, 500

    try:
        merge1 = list(merge_collect.find({}))
        merge_df = pd.DataFrame(merge1)
        channel_data_curing = list(channel_collect_curing.find({"gt_code": {"$in": list(active_gtcodes)}}))
        channel_data_curing = list(channel_collect_curing.find({}))
        channel_df = pd.DataFrame(channel_data_curing)
        channel_data_ufm = list(channel_collect_ufm.find({}))
        channel_df_ufm = pd.DataFrame(channel_data_ufm)
        machine_data = list(machine_collect.find({}))
        machine_df = pd.DataFrame(machine_data)
        curing_press_data = list(curing_press_collect.find({"curing_press": {"$in": list(active_curing_press_ids)}}))
        curing_press_data = list(curing_press_collect.find({}))
        curing_df = pd.DataFrame(curing_press_data)
        output_messages.append("✅ Curing Data loaded from collections into DataFrames.")
    except Exception as e:
        error_message = f"❌ Curing Error loading data: {e}"
        output_messages.append(error_message)
        return {"status": "error", "message": error_message, "output_log": output_messages}, 500

    # MODIFIED: Check for tbm column before adding tbm_machine column
    if 'tbm' in merge_df.columns and 'tbm' in machine_df.columns and 'tbm' in machine_df.columns:
        try:
            merge_df["tbm_machine"] = None  # Initialize tbm_machine column
            for index, row in merge_df.iterrows():
                tbm_value = row.get("tbm")
                if tbm_value:
                    matching_channel = machine_df[machine_df["tbm"] == tbm_value]
                    if not matching_channel.empty:
                        merge_df.at[index, "tbm_machine"] = matching_channel["tbm"].iloc[0]  # NEW: Use machine column
            output_messages.append("✅ Added tbm_machine column to merge_df based on matching tbm values.")
        except Exception as e:
            output_messages.append(f"⚠️ Warning: Failed to add tbm_machine column to merge_df: {e}. Continuing without tbm_machine.")
    else:
        output_messages.append("⚠️ Warning: 'tbm' column not found in merge_df or machine_df. Skipping tbm_machine column creation.")

    # MODIFIED: Check for tbm column before updating machine_df
    if 'tbm' in machine_df.columns and 'tbm' in merge_df.columns and 'tbm_machine' in merge_df.columns:
        try:
            for index, row in machine_df.iterrows():
                tbm_value = row.get("tbm")
                if tbm_value:
                    matching_merge = merge_df[merge_df["tbm"] == tbm_value]
                    if not matching_merge.empty and "tbm_machine" in matching_merge.columns:
                        machine_df.at[index, "tbm"] = matching_merge["tbm_machine"].iloc[0]
            output_messages.append("✅ Updated machine_df machine column with tbm_machine values from merge_df.")
        except Exception as e:
            output_messages.append(f"⚠️ Warning: Failed to update machine_df machine column: {e}. Continuing without updating channel_df machine.")
    else:
        output_messages.append("⚠️ Warning: 'tbm' column not found in merge_df or machine_df. Skipping machine_df machine column update.")


    # Get distinct model numbers
    gt_code_lists = channel_collect_curing.distinct("gt_code")
    if merge_df.empty or "gt_code" not in merge_df.columns:
        output_messages.append("merge_df is empty or missing 'gt_code'. Skipping Curing processing.")
        return {"status": "success", "message": "No Curing data to process.", "output_log": output_messages}, 200
    if "alert_id_curing_ufm" not in merge_df.columns:
        merge_df["alert_id_curing_ufm"] = None
    merge_df = merge_df[merge_df["gt_code"].isin(gt_code_lists)]
    merge_df = merge_df[merge_df["alert_id_curing_ufm"].isna()]
    if merge_df.empty:
        output_messages.append("No Curing data after filtering. Skipping.")
        return {"status": "success", "message": "No Curing data after filtering.", "output_log": output_messages}, 200

    merge_df["date_of_tbm"] = merge_df["tbm_buildtime"]
    merge_df["date_of_curing"] = merge_df["curing_infeedtime"]

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

    try:
        merge_df["date_of_ufm"] = convert_datetime_with_invalid_mask(merge_df["date_of_ufm"], "date_of_ufm")
        merge_df["date_of_ufm"] = merge_df["date_of_ufm"].astype(object).where(merge_df["date_of_ufm"].notnull(), "No Date")
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
        output_messages.append(f"❌ Error during datetime conversion: {e}")
        return {"status": "error", "message": f"Error during datetime conversion: {e}", "output_log": output_messages}, 500

    # merge_df["tbm_shift"] = merge_df["date_of_tbm"].apply(get_shift)
    merge_df["curing_shift"] = merge_df["date_of_curing"].apply(get_shift)

    try:
        last_alert = alert_collect.find_one(sort=[("alert_id_curing_ufm", -1)])
        last_alert_id = int(last_alert["alert_id_curing_ufm"]) if last_alert and "alert_id_curing_ufm" in last_alert else 0
    except Exception as e:
        error_message = f"❌ Curing Error fetching last alert ID: {e}"
        output_messages.append(error_message)
        last_alert_id = 0

    try:
        last_alert = alert_collect.find_one(sort=[("alert_id_tbm_ufm", -1)])
        last_alert_id = int(last_alert["alert_id_tbm_ufm"]) if last_alert and "alert_id_tbm_ufm" in last_alert else 0
    except Exception as e:
        error_message = f"❌ Curing Error fetching last alert ID for lot/alert processing: {e}"
        output_messages.append(error_message)
        last_alert_id = 0

    try:
        last_lots = list(lot_curing_collect.aggregate([
            {"$match": {"lot_id": {"$regex": "^Curing_[^_]+-[^_]*_[^_]+_\\d+$"},
                        "gt_code": {"$in": list(active_gtcodes)}}},
            {"$addFields": {
                "lot_number": {"$toInt": {"$arrayElemAt": [{"$split": ["$lot_id", "_"]}, -1]}},
                "gt_code": "$gt_code",
                "machine": "$machine"
            }},
            {"$sort": {"lot_number": -1}},
            {"$group": {
                "_id": {"gt_code": "$gt_code", "machine": "$machine"},
                "max_lot_number": {"$first": "$lot_number"},
                "last_lot_id": {"$first": "$lot_id"}
            }}
        ]))
        last_lot_dict = {
            (item["_id"]["gt_code"], item["_id"]["machine"]): {
                "max_lot_number": item["max_lot_number"],
                "last_lot_id": item["last_lot_id"]
            } for item in last_lots
        }
        logging.info(f"ℹ️ Fetched last lot numbers: {len(last_lot_dict)} entries")
        output_messages.append(f"ℹ️ Fetched last lot numbers: {len(last_lot_dict)} entries")
    except Exception as e:
        error_message = f"❌ Curing Error fetching last lot IDs: {e}"
        logging.error(error_message)
        output_messages.append(error_message)
        last_lot_dict = {}
    
    # Prepare merge_df
    merge_df["curing_press"] = merge_df["curing_press"].astype(str)
    curing_df["curing_press"] = curing_df["curing_press"].astype(str)  # Changed from modelnumber to gt_code is handled as array later
    # Filter merge_df to include only active curing presses (only when restricting).
    if _restrict_active_presses:
        merge_df = merge_df[merge_df["curing_press"].isin(active_curing_press_ids)]
    if merge_df.empty:
        output_messages.append("❌ No data in merge_df after filtering for active machines. Exiting.")
        return {"status": "error", "message": "No data after active tbm filtering", "output_log": output_messages}, 500

    # Ensure lot-tracking columns are object dtype so string lot IDs can be assigned
    # (they may pre-exist as float64/NaN, which raises "Invalid value ... for dtype 'float64'")
    for _lot_col in ["lot_id", "previous_lot_id", "curing_machine_model_lot_id", "curing_previous_machine_model_lot_id", "alert_id_curing_ufm"]:
        if _lot_col not in merge_df.columns:
            merge_df[_lot_col] = None
        merge_df[_lot_col] = merge_df[_lot_col].astype(object)

    curing_previous_machine_model_lot_ids = {}
    machine_lot_avg_map = {}
    lot_bulk_operations = []
    alert_bulk_operations = []
    averages_bulk_operations = []
    deviated_triggering_bulk_operations = []
    triggering_bulk_operations = []
    bulk_operations = []

    try:
        last_averages = list(averages_collect.aggregate([
            {"$match": {"curing_machine_model_lot_id": {"$regex": "^[^_]+-[^_]*_[^_]+_Curing_[^_]+-[^_]*_[^_]+_\\d+_\\d+$"},
                        "gt_code": {"$in": list(active_gtcodes)}}},
            {"$sort": {"curing_machine_model_lot_id": -1}},
            {"$group": {
                "_id": {"gt_code": "$gt_code", "machine": "$machine"},
                "last_curing_lot_id": {"$first": "$curing_machine_model_lot_id"},
                "last_averages": {"$first": "$current_lot_averages"}
            }}
        ]))
        for item in last_averages:
            gt_code = item["_id"]["gt_code"]
            machine = item["_id"]["machine"]
            curing_lot_id = item["last_curing_lot_id"]
            averages = item["last_averages"]
            curing_previous_machine_model_lot_ids[(gt_code, machine)] = curing_lot_id
            if averages:
                machine_lot_avg_map[(gt_code, machine, curing_lot_id)] = averages
                logging.info(f"Pre-populated machine_lot_avg_map with averages for {curing_lot_id}")
            else:
                logging.warning(f"No averages found for {curing_lot_id} in averages_collect")
        logging.info(f"Initialized curing_previous_machine_model_lot_ids with {len(curing_previous_machine_model_lot_ids)} entries")
        output_messages.append(f"Initialized curing_previous_machine_model_lot_ids with {len(curing_previous_machine_model_lot_ids)} entries")
    except Exception as e:
        logging.error(f"❌ Error pre-populating machine_lot_avg_map and curing_previous_machine_model_lot_ids: {e}")
        output_messages.append(f"❌ Error pre-populating machine_lot_avg_map and curing_previous_machine_model_lot_ids: {e}")

    conicity_params = [
        'rfpp_cw_kg','rfpp_ccw_kg','rfh1_cw_kg','rfh1_ccw_kg','rfh2_cw_kg','rfh2_ccw_kg','lfpp_cw_kg','lfpp_ccw_kg',
        'lsft_cw_kg','lsft_ccw_kg','lfh1_cw_kg','lfh1_ccw_kg','plysteer_kg','cony_kg','rfmxa_ccw_deg',
        'rfa1_cw_deg','rfa1_ccw_deg','rfa2_cw_deg','rfa2_ccw_deg','lfmxa_cw_deg','lfmxa_ccw_deg','lfa1_cw_deg',
        'lfa1_ccw_deg','botplane_g','topplane_g','couple_g','static_g','botplane_deg','topplane_deg','couple_deg',
        'static_deg','botbul_mm','botdep_mm','topbul_mm','topdep_mm','blro_mm','blroh1_mm','crro_mm','crroh1_mm',
        'tlro_mm','tlroh1_mm','botbul_deg','botdep_deg','topbul_deg','topdep_deg','blro_deg','blroh1_deg','crro_deg',
        'crroh1_deg','tlro_deg','tlroh1_deg'
        ]
    yield_params = [
        "rfv_yield", "rfh1_yield", "rfh2_yield", "lfv_yield", "lsft_yield", "lfh1_yield", "conicity_yield", "oe_yield",
        "rfmxa_yield", "rfa1_yield", "rfa2_yield", "lfxma_yield", "lfa1_yield", "central_radial_runout_yield", "central_radial_runout_first_harmonic_yield",
        "lateral_run_out_top_over_yield", "lateral_run_out_bottom_over_yield", "lateral_run_out_yield", "lateral_run_out_bottom_first_harmonic_yield", "lateral_run_out_top_first_harmonic_yield", 
        "lateral_run_out_harmonic_yield", "bulge_top_over_yield", "bulge_bottom_over_yield", "bulge_yield", "dent_top_over_yield", "dent_bottom_over_yield", "dent_yield", "geometry_oe_yield",
        "lower_yield", "upper_yield", "static_yield", "couple_yield", "balancing_oe_yield", "warehouse_yield", "visual_yield"
        ]
    # Define rank parameter mappings
    rank_param_mappings = {
        "rfv_yield": ["tu_rfpp_cw_grade", "tu_rfpp_ccw_grade"],
        "rfh1_yield": ["tu_rfh1_cw_grade", "tu_rfh1_ccw_grade"], 
        "rfh2_yield": ["tu_rfh2_cw_grade", "tu_rfh2_ccw_grade"],
        "lfv_yield": ["tu_lfpp_cw_grade", "tu_lfpp_ccw_grade"],
        "lsft_yield": ["tu_lsft_cw_grade", "tu_lsft_ccw_grade"],
        "lfh1_yield": ["tu_lfh1_cw_grad", "tu_lfh1_ccw_grad"],
        "conicity_yield": ["tu_cony_grade"],
        "oe_yield": ["tu_class"],
        "rfmxa_yield": ["tu_rfmxa_ccw_grade"],
        "rfa1_yield": ["tu_rfa1_cw_grade", "tu_rfa1_ccw_grade"],
        "rfa2_yield": ["tu_rfa2_cw_grade", "tu_rfa2_ccw_grade"],
        "lfxma_yield": ["tu_lfmxa_cw_grade", "tu_lfmxa_ccw_grade"],
        "lfa1_yield": ["tu_lfa1_cw_grade", "tu_lfa1_ccw_grade"],
        "central_radial_runout_yield": ["tg_crro_grade"],
        "central_radial_runout_first_harmonic_yield": ["tg_crroh1_grade"],
        "lateral_run_out_top_over_yield": ["tg_tlro_grade"],
        "lateral_run_out_bottom_over_yield": ["tg_blro_grade"],
        "lateral_run_out_yield": ["tg_tlro_grade", "tg_blro_grade"],
        "lateral_run_out_bottom_first_harmonic_yield": ["tg_blroh1_grade"],
        "lateral_run_out_top_first_harmonic_yield": ["tg_tlroh1_grade"],
        "lateral_run_out_harmonic_yield": ["tg_tlroh1_grade", "tg_blroh1_grade"],
        "bulge_top_over_yield": ["tg_topbul_grade"],
        "bulge_bottom_over_yield": ["tg_botbul_grade"],
        "bulge_yield": ["tg_botbul_grade", "tg_topbul_grade"],
        "dent_top_over_yield": ["tg_topdep_grade"],
        "dent_bottom_over_yield": ["tg_botdep_grade"],
        "dent_yield": ["tg_topdep_grade", "tg_botdep_grade"],
        "geometry_oe_yield": ["tg_class"],
        "lower_yield": ["tb_botplane_grade"],
        "upper_yield": ["tb_topplane_grade"],
        "static_yield": ["tb_static_grade"],
        "couple_yield": ["tb_couple_grade"],
        "balancing_oe_yield": ["tb_class"]
        }
    for _, row in channel_df.iterrows():
        try:
            # model_number = str(row.get("modelno", "")).strip()
            gt_code_list = row.get("gt_code", [])
            model_number = gt_code_list[0].strip() if isinstance(gt_code_list, list) and gt_code_list else ""
            if model_number not in active_gtcodes:
                output_messages.append(f"Skipping CURING model {model_number} as it is not active.")
                continue
            gt_codes = row.get("gt_code", [])

            if not isinstance(gt_codes, list):
                gt_codes = [gt_codes]

            target_lot_size = int(row.get("lot_size", 0))

            # spec_con = float(row.get("spec_con", 0))
            sizecode = str(row.get("sizecode", ""))
            spec_rfpp_cw_kg = float(row.get("spec_rfpp_cw_kg", 0))
            spec_rfpp_ccw_kg = float(row.get("spec_rfpp_ccw_kg", 0))
            spec_rfh1_cw_kg = float(row.get("spec_rfh1_cw_kg", 0))
            spec_rfh1_ccw_kg = float(row.get("spec_rfh1_ccw_kg", 0))
            spec_rfh2_cw_kg = float(row.get("spec_rfh2_cw_kg", 0))
            spec_rfh2_ccw_kg = float(row.get("spec_rfh2_ccw_kg", 0))
            spec_lfpp_cw_kg = float(row.get("spec_lfpp_cw_kg", 0))
            spec_lfpp_ccw_kg = float(row.get("spec_lfpp_ccw_kg", 0))
            spec_lsft_cw_kg = float(row.get("spec_lsft_cw_kg", 0))
            spec_lsft_ccw_kg = float(row.get("spec_lsft_ccw_kg", 0))
            spec_lfh1_cw_kg = float(row.get("spec_lfh1_cw_kg", 0))
            spec_lfh1_ccw_kg = float(row.get("spec_lfh1_ccw_kg", 0))
            spec_plysteer_kg = float(row.get("spec_plysteer_kg", 0))
            spec_rfmxa_ccw_deg = float(row.get("spec_rfmxa_ccw_deg", 0))
            spec_rfa1_cw_deg = float(row.get("spec_rfa1_cw_deg", 0))
            spec_rfa1_ccw_deg = float(row.get("spec_rfa1_ccw_deg", 0))
            spec_rfa2_cw_deg = float(row.get("spec_rfa2_cw_deg", 0))
            spec_rfa2_ccw_deg = float(row.get("spec_rfa2_ccw_deg", 0))
            spec_lfmxa_cw_deg = float(row.get("spec_lfmxa_cw_deg", 0))
            spec_lfmxa_ccw_deg = float(row.get("spec_lfmxa_ccw_deg", 0))
            spec_lfa1_cw_deg = float(row.get("spec_lfa1_cw_deg", 0))
            spec_lfa1_ccw_deg = float(row.get("spec_lfa1_ccw_deg", 0))
            spec_cony_kg = float(row.get("spec_cony_kg", 0))
            spec_static_g = float(row.get("spec_static_g", 0))
            spec_static_deg = float(row.get("spec_static_deg", 0))
            spec_couple_g = float(row.get("spec_couple_g", 0))
            spec_couple_deg = float(row.get("spec_couple_deg", 0))
            spec_bul_botbul_mm = float(row.get("spec_bul_botbul_mm", 0))
            spec_bul_topbul_mm = float(row.get("spec_bul_topbul_mm", 0))
            spec_bul_botbul_deg = float(row.get("spec_bul_botbul_deg", 0))
            spec_bul_topdep_deg = float(row.get("spec_bul_topdep_deg", 0))
            spec_dep_botdep_mm = float(row.get("spec_dep_botdep_mm", 0))
            spec_dep_topdep_mm = float(row.get("spec_dep_topdep_mm", 0))
            spec_dep_botdep_deg = float(row.get("spec_dep_botdep_deg", 0))
            spec_dep_topbul_deg = float(row.get("spec_dep_topbul_deg", 0))
            spec_rro_crro_mm = float(row.get("spec_rro_crro_mm", 0))
            spec_rro_crroh1_mm = float(row.get("spec_rro_crroh1_mm", 0))
            spec_lro_blro_mm = float(row.get("spec_lro_blro_mm", 0))
            spec_lro_blroh1_mm = float(row.get("spec_lro_blroh1_mm", 0))
            spec_lro_tlro_mm = float(row.get("spec_lro_tlro_mm", 0))
            spec_lro_tlroh1_mm = float(row.get("spec_lro_tlroh1_mm", 0))
            spec_rro_crro_deg = float(row.get("spec_rro_crro_deg", 0))
            spec_rro_crroh1_deg = float(row.get("spec_rro_crroh1_deg", 0))
            spec_lro_blro_deg = float(row.get("spec_lro_blro_deg", 0))
            spec_lro_blroh1_deg = float(row.get("spec_lro_blroh1_deg", 0))
            spec_lro_tlro_deg = float(row.get("spec_lro_tlro_deg", 0))
            spec_lro_tlroh1_deg = float(row.get("spec_lro_tlroh1_deg", 0))
            spec_dynamic_botplane_g = float(row.get("spec_dynamic_botplane_g", 0))
            spec_dynamic_topplane_g = float(row.get("spec_dynamic_topplane_g", 0))
            spec_dynamic_botplane_deg = float(row.get("spec_dynamic_botplane_deg", 0))
            spec_dynamic_topplane_deg = float(row.get("spec_dynamic_topplane_deg", 0))

            rfv_yield_spec_yield = float(row.get("rfv_yield_spec_yield", 0))
            rfh1_yield_spec_yield = float(row.get("rfh1_yield_spec_yield", 0))
            rfh2_yield_spec_yield = float(row.get("rfh2_yield_spec_yield", 0))
            lfv_yield_spec_yield = float(row.get("lfv_yield_spec_yield", 0))
            lsft_yield_spec_yield = float(row.get("lsft_yield_spec_yield", 0))
            lfh1_yield_spec_yield = float(row.get("lfh1_yield_spec_yield", 0))
            conicity_yield_spec_yield = float(row.get("conicity_yield_spec_yield", 0))
            oe_yield_spec_yield = float(row.get("oe_yield_spec_yield", 0))
            rfmxa_yield_spec_yield = float(row.get("rfmxa_yield_spec_yield", 0))
            rfa1_yield_spec_yield = float(row.get("rfa1_yield_spec_yield", 0))
            rfa2_yield_spec_yield = float(row.get("rfa2_yield_spec_yield", 0))
            lfxma_yield_spec_yield = float(row.get("lfxma_yield_spec_yield", 0))
            lfa1_yield_spec_yield = float(row.get("lfa1_yield_spec_yield", 0))
            central_radial_runout_yield_spec_yield = float(row.get("central_radial_runout_yield_spec_yield", 0))
            central_radial_runout_first_harmonic_yield_spec_yield = float(row.get("central_radial_runout_first_harmonic_yield_spec_yield", 0))
            lateral_run_out_top_over_yield_spec_yield = float(row.get("lateral_run_out_top_over_yield_spec_yield", 0))
            lateral_run_out_bottom_over_yield_spec_yield = float(row.get("lateral_run_out_bottom_over_yield_spec_yield", 0))
            lateral_run_out_yield_spec_yield = float(row.get("lateral_run_out_yield_spec_yield", 0))
            lateral_run_out_bottom_first_harmonic_yield_spec_yield = float(row.get("lateral_run_out_bottom_first_harmonic_yield_spec_yield", 0))
            lateral_run_out_top_first_harmonic_yield_spec_yield = float(row.get("lateral_run_out_top_first_harmonic_yield_spec_yield", 0))
            lateral_run_out_harmonic_yield_spec_yield = float(row.get("lateral_run_out_harmonic_yield_spec_yield", 0))
            bulge_top_over_yield_spec_yield = float(row.get("bulge_top_over_yield_spec_yield", 0))
            bulge_bottom_over_yield_spec_yield = float(row.get("bulge_bottom_over_yield_spec_yield", 0))
            bulge_yield_spec_yield = float(row.get("bulge_yield_spec_yield", 0))
            dent_top_over_yield_spec_yield = float(row.get("dent_top_over_yield_spec_yield", 0))
            dent_bottom_over_yield_spec_yield = float(row.get("dent_bottom_over_yield_spec_yield", 0))
            dent_yield_spec_yield = float(row.get("dent_yield_spec_yield", 0))
            geometry_oe_yield_spec_yield = float(row.get("geometry_oe_yield_spec_yield", 0))
            lower_yield_spec_yield = float(row.get("lower_yield_spec_yield", 0))
            upper_yield_spec_yield = float(row.get("upper_yield_spec_yield", 0))
            static_yield_spec_yield = float(row.get("static_yield_spec_yield", 0))
            couple_yield_spec_yield = float(row.get("couple_yield_spec_yield", 0))
            balancing_oe_yield_spec_yield = float(row.get("balancing_oe_yield_spec_yield", 0))
            warehouse_yield_spec_yield = float(row.get("warehouse_yield_spec_yield", 0))
            visual_yield_spec_yield = float(row.get("visual_yield_spec_yield", 0))
            
            class_spec_yield = float(row.get("class_spec_yield", 0))
            balancing_spec_yield = float(row.get("balancing_spec_yield", 0))
            geometry_spec_yield = float(row.get("geometry_spec_yield", 0))
            
            threshold_value_rfpp_cw_kg = float(row.get("threshold_value_rfpp_cw_kg", 5))
            threshold_value_rfpp_ccw_kg = float(row.get("threthreshold_value_rfpp_ccw_kgshold_value_bal", 5))
            threshold_value_rfh1_cw_kg = float(row.get("threshold_value_rfh1_cw_kg", 5))
            threshold_value_rfh1_ccw_kg = float(row.get("threshold_value_rfh1_ccw_kg", 5))
            threshold_value_rfh2_cw_kg = float(row.get("threshold_value_rfh2_cw_kg", 5))
            threshold_value_rfh2_ccw_kg = float(row.get("threshold_value_rfh2_ccw_kg", 5))
            threshold_value_lfpp_cw_kg = float(row.get("threshold_value_lfpp_cw_kg", 5))
            threshold_value_lfpp_ccw_kg = float(row.get("threshold_value_lfpp_ccw_kg", 5))
            threshold_value_lsft_cw_kg = float(row.get("threshold_value_lsft_cw_kg", 5))
            threshold_value_lsft_ccw_kg = float(row.get("threshold_value_lsft_ccw_kg", 5))
            threshold_value_lfh1_cw_kg = float(row.get("threshold_value_lfh1_cw_kg", 5))
            threshold_value_lfh1_ccw_kg = float(row.get("threshold_value_lfh1_ccw_kg", 5))
            threshold_value_plysteer_kg = float(row.get("threshold_value_plysteer_kg", 5))
            threshold_value_rfmxa_cw_deg = float(row.get("threshold_value_rfmxa_cw_deg", 5))
            threshold_value_rfmxa_ccw_deg = float(row.get("threshold_value_rfmxa_ccw_deg", 5))
            threshold_value_rfa1_cw_deg = float(row.get("threshold_value_rfa1_cw_deg", 5))
            threshold_value_rfa1_ccw_deg = float(row.get("threshold_value_rfa1_ccw_deg", 5))
            threshold_value_rfa2_cw_deg = float(row.get("threshold_value_rfa2_cw_deg", 5))
            threshold_value_rfa2_ccw_deg = float(row.get("threshold_value_rfa2_ccw_deg", 5))
            threshold_value_lfmxa_cw_deg = float(row.get("threshold_value_lfmxa_cw_deg", 5))
            threshold_value_lfmxa_ccw_deg = float(row.get("threshold_value_lfmxa_ccw_deg", 5))
            threshold_value_lfa1_cw_deg = float(row.get("threshold_value_lfa1_cw_deg", 5))
            threshold_value_lfa1_ccw_deg = float(row.get("threshold_value_lfa1_ccw_deg", 5))
            threshold_value_cony_kg = float(row.get("threshold_value_cony_kg", 5))
            threshold_value_bal_static_g = float(row.get("threshold_value_bal_static_g", 5))
            threshold_value_bal_static_deg = float(row.get("threshold_value_bal_static_deg", 5))
            threshold_value_bal_couple_g = float(row.get("threshold_value_bal_couple_g", 5))
            threshold_value_bal_couple_deg = float(row.get("threshold_value_bal_couple_deg", 5))
            threshold_value_bal_dynamic_botplane_g = float(row.get("threshold_value_bal_dynamic_botplane_g", 5))
            threshold_value_bal_dynamic_topplane_g = float(row.get("threshold_value_bal_dynamic_topplane_g", 5))
            threshold_value_bal_dynamic_botplane_deg = float(row.get("threshold_value_bal_dynamic_botplane_deg", 5))
            threshold_value_bal_dynamic_topplane_deg = float(row.get("threshold_value_bal_dynamic_topplane_deg", 5))
            threshold_value_geo_bul_botbul_mm = float(row.get("threshold_value_geo_bul_botbul_mm", 5))
            threshold_value_geo_bul_topbul_mm = float(row.get("threshold_value_geo_bul_topbul_mm", 5))
            threshold_value_geo_bul_botbul_deg = float(row.get("threshold_value_geo_bul_botbul_deg", 5))
            threshold_value_geo_bul_topdep_deg = float(row.get("threshold_value_geo_bul_topdep_deg", 5))
            threshold_value_geo_dep_botdep_mm = float(row.get("threshold_value_geo_dep_botdep_mm", 5))
            threshold_value_geo_dep_topdep_mm = float(row.get("threshold_value_geo_dep_topdep_mm", 5))
            threshold_value_geo_dep_botdep_deg = float(row.get("threshold_value_geo_dep_botdep_deg", 5))
            threshold_value_geo_dep_topbul_deg = float(row.get("threshold_value_geo_dep_topbul_deg", 5))
            threshold_value_geo_rro_crro_mm = float(row.get("threshold_value_geo_rro_crro_mm", 5))
            threshold_value_geo_rro_crroh1_mm = float(row.get("threshold_value_geo_rro_crroh1_mm", 5))
            threshold_value_geo_lro_blro_mm = float(row.get("threshold_value_geo_lro_blro_mm", 5))
            threshold_value_geo_lro_blroh1_mm = float(row.get("threshold_value_geo_lro_blroh1_mm", 5))
            threshold_value_geo_lro_tlro_mm = float(row.get("threshold_value_geo_lro_tlro_mm", 5))
            threshold_value_geo_lro_tlroh1_mm = float(row.get("threshold_value_geo_lro_tlroh1_mm", 5))
            threshold_value_geo_rro_crro_deg = float(row.get("threshold_value_geo_rro_crro_deg", 5))
            threshold_value_geo_rro_crroh1_deg = float(row.get("threshold_value_geo_rro_crroh1_deg", 5))
            threshold_value_geo_lro_blro_deg = float(row.get("threshold_value_geo_lro_blro_deg", 5))
            threshold_value_geo_lro_blroh1_deg = float(row.get("threshold_value_geo_lro_blroh1_deg", 5))
            threshold_value_geo_lro_tlro_deg = float(row.get("threshold_value_geo_lro_tlro_deg", 5))
            threshold_value_geo_lro_tlroh1_deg = float(row.get("threshold_value_geo_lro_tlroh1_deg", 5))

            threshold_value_yield_rfv_yield = float(row.get("threshold_value_yield_rfv_yield", 5))
            threshold_value_yield_rfh1_yield = float(row.get("threshold_value_yield_rfh1_yield", 5))
            threshold_value_yield_rfh2_yield = float(row.get("threshold_value_yield_rfh2_yield", 5))
            threshold_value_yield_lfv_yield = float(row.get("threshold_value_yield_lfv_yield", 5))
            threshold_value_yield_lsft_yield = float(row.get("threshold_value_yield_lsft_yield", 5))
            threshold_value_yield_lfh1_yield = float(row.get("threshold_value_yield_lfh1_yield", 5))
            threshold_value_yield_conicity_yield = float(row.get("threshold_value_yield_conicity_yield", 5))
            threshold_value_yield_oe_yield = float(row.get("threshold_value_yield_oe_yield", 5))
            threshold_value_yield_rfmxa_yield = float(row.get("threshold_value_yield_rfmxa_yield", 5))
            threshold_value_yield_rfa1_yield = float(row.get("threshold_value_yield_rfa1_yield", 5))
            threshold_value_yield_rfa2_yield = float(row.get("threshold_value_yield_rfa2_yield", 5))
            threshold_value_yield_lfxma_yield = float(row.get("threshold_value_yield_lfxma_yield", 5))
            threshold_value_yield_lfa1_yield = float(row.get("threshold_value_yield_lfa1_yield", 5))
            threshold_value_yield_central_radial_runout_yield = float(row.get("threshold_value_yield_central_radial_runout_yield", 5))
            threshold_value_yield_central_radial_runout_first_harmonic_yield = float(row.get("threshold_value_yield_central_radial_runout_first_harmonic_yield", 5))
            threshold_value_yield_lateral_run_out_top_over_yield = float(row.get("threshold_value_yield_lateral_run_out_top_over_yield", 5))
            threshold_value_yield_lateral_run_out_bottom_over_yield = float(row.get("threshold_value_yield_lateral_run_out_bottom_over_yield", 5))
            threshold_value_yield_lateral_run_out_yield = float(row.get("threshold_value_yield_lateral_run_out_yield", 5))
            threshold_value_yield_lateral_run_out_bottom_first_harmonic_yield = float(row.get("threshold_value_yield_lateral_run_out_bottom_first_harmonic_yield", 5))
            threshold_value_yield_lateral_run_out_top_first_harmonic_yield = float(row.get("threshold_value_yield_lateral_run_out_top_first_harmonic_yield", 5))
            threshold_value_yield_lateral_run_out_harmonic_yield = float(row.get("threshold_value_yield_lateral_run_out_harmonic_yield", 5))
            threshold_value_yield_bulge_top_over_yield = float(row.get("threshold_value_yield_bulge_top_over_yield", 5))
            threshold_value_yield_bulge_bottom_over_yield = float(row.get("threshold_value_yield_bulge_bottom_over_yield", 5))
            threshold_value_yield_bulge_yield = float(row.get("threshold_value_yield_bulge_yield", 5))
            threshold_value_yield_dent_top_over_yield = float(row.get("threshold_value_yield_dent_top_over_yield", 5))
            threshold_value_yield_dent_bottom_over_yield = float(row.get("threshold_value_yield_dent_bottom_over_yield", 5))
            threshold_value_yield_dent_yield = float(row.get("threshold_value_yield_dent_yield", 5))
            threshold_value_yield_geometry_oe_yield = float(row.get("threshold_value_yield_geometry_oe_yield", 5))
            threshold_value_yield_lower_yield = float(row.get("threshold_value_yield_lower_yield", 5))
            threshold_value_yield_upper_yield = float(row.get("threshold_value_yield_upper_yield", 5))
            threshold_value_yield_static_yield = float(row.get("threshold_value_yield_static_yield", 5))
            threshold_value_yield_couple_yield = float(row.get("threshold_value_yield_couple_yield", 5))
            threshold_value_yield_balancing_oe_yield = float(row.get("threshold_value_yield_balancing_oe_yield", 5))
            threshold_value_yield_warehouse_yield = float(row.get("threshold_value_yield_warehouse_yield", 5))
            threshold_value_yield_visual_yield = float(row.get("threshold_value_yield_visual_yield", 5))
            threshold_value_yield = float(row.get("threshold_value_yield", 5))

            class_threshold_value_yield_class_yield = float(row.get("class_threshold_value_yield_class_yield", 5))
            threshold_value_yield_balancing_yield = float(row.get("threshold_value_yield_balancing_yield", 5))
            threshold_value_yield_geometry_yield = float(row.get("threshold_value_yield_geometry_yield", 5))
            
            for gt_code in gt_codes:
                gt_code = str(gt_code).strip()
                model_data = merge_df[
                    (merge_df["gt_code"] == model_number) &
                    (merge_df["curing_press"].isin(active_curing_press_ids))
                ].copy()

                total_barcodes_model = len(model_data)
                model_data.sort_values(["date_of_curing"], ascending=True, inplace=True)
                alert_map = {}

                # Virgin/Final yield split: store raw barcode rows, then continue main
                # processing on virgin data (first occurrence per barcode).
                virgin_data, final_data, _vf_msgs = split_and_store_virgin_final(model_data, "date_of_curing", "CURING")
                output_messages.extend(_vf_msgs)
                model_data = virgin_data.copy()
                total_barcodes_model = len(model_data)

                if total_barcodes_model < target_lot_size:
                    output_messages.append(f"Skipping Curing model {model_number} due to insufficient model barcodes.")
                    continue

                for curing_value, curing_data in model_data.groupby(["curing_press", "curing_mouldid"]):
                    curing_data = curing_data[curing_data["curing_machine_model_lot_id"].isnull()].copy()
                    if curing_data.empty:
                        continue

                    barcode_counter = 0
                    total_curing_barcodes = len(curing_data)
                    curing_press_value, curing_mouldid_value = curing_value
                    curing_press_value = str(curing_press_value).replace("_", "-")
                    curing_mouldid_value = str(curing_mouldid_value).replace("_", "-")
                    machine_no = f"{curing_press_value}-{curing_mouldid_value}"
                    machine_press_value = machine_no.split('-')[0]

                    # MODIFIED: Only access tbm_machine if it exists
                    tbm_machine_value = curing_data["tbm_machine"].iloc[0] if "tbm_machine" in curing_data.columns and not curing_data["tbm_machine"].isna().all() else None
                    # output_messages.append(f"DEBUG: Model {model_number}, Machine {machine_no}, tbm_machine: {tbm_machine_value}, Found {total_curing_barcodes} barcodes.")

                    machine_row = curing_df[curing_df["curing_press"] == machine_press_value]
                    if machine_row.empty:
                        output_messages.append(f"Skipping Curing machine {machine_no} for model {model_number} due to unknown machine target")
                        continue
                    if total_curing_barcodes < target_lot_size:
                        output_messages.append(f"DEBUG: Skipping machine {machine_no} - Found {total_curing_barcodes}, Need {target_lot_size}.")
                        continue

                    barcode_counter = 0

                    while barcode_counter < total_curing_barcodes:
                        remaining_model = total_curing_barcodes - barcode_counter
                        if remaining_model < target_lot_size:
                            break
                        batch_size = target_lot_size
                        batch_indices = curing_data.iloc[barcode_counter: barcode_counter + batch_size].index
                        batch_data = curing_data.iloc[barcode_counter: barcode_counter + batch_size].copy()

                        machine_model_key = (model_number, machine_no)
                        if machine_model_key not in last_lot_dict:
                            last_lot_dict[machine_model_key] = {
                                "max_lot_number": 0,
                                "last_lot_id": None
                            }
                        last_lot_number = last_lot_dict[(model_number, machine_no)]["max_lot_number"] + 1

                        unique_lot_id_current = f"Curing_{machine_no}_{model_number}_{last_lot_number}"
                        current_barcode_counter = barcode_counter + batch_size
                        current_curing_machine_model_lot_id = f"{machine_no}_{model_number}_Curing_{machine_no}_{model_number}_{last_lot_number}_{current_barcode_counter}"
                        curing_previous_machine_model_lot_id = curing_previous_machine_model_lot_ids.get((model_number, machine_no))

                        last_lot_dict[(model_number, machine_no)]["max_lot_number"] = last_lot_number
                        last_lot_dict[(model_number, machine_no)]["last_lot_id"] = unique_lot_id_current

                        if not re.match(r"^Curing_[^_]+_[^_]+_\d+$", unique_lot_id_current):
                            logging.error(f"Invalid lot_id format: {unique_lot_id_current}")
                            continue
                        if not re.match(r"^[^_]+_[^_]+_Curing_[^_]+_[^_]+_\d+_\d+$", current_curing_machine_model_lot_id):
                            logging.error(f"Invalid curing_machine_model_lot_id format: {current_curing_machine_model_lot_id}")
                            continue

                        curing_previous_machine_model_lot_id = curing_previous_machine_model_lot_ids.get(machine_model_key)
                        # logging.info(f"Initial curing_previous_machine_model_lot_id: {curing_previous_machine_model_lot_id} for model {model_number}, machine {machine_no}")
                        if curing_previous_machine_model_lot_id is None and last_lot_number > 1:
                            prev_lot_number = last_lot_number - 1
                            prev_lot_id = f"Curing_{machine_no}_{model_number}_{prev_lot_number}"
                            logging.info(f"Fetching previous lot for lot_id: {prev_lot_id}")
                            try:
                                prev_lot = averages_collect.find_one(
                                    {
                                        "gt_code": model_number,
                                        "machine": machine_no,
                                        "lot_id": prev_lot_id
                                    },
                                    sort=[("curing_machine_model_lot_id", -1)]
                                )
                                if prev_lot:
                                    curing_previous_machine_model_lot_id = prev_lot["curing_machine_model_lot_id"]
                                    logging.info(f"Found previous lot: {curing_previous_machine_model_lot_id}")
                                else:
                                    logging.warning(f"No previous lot found for lot_id: {prev_lot_id}")
                                    curing_previous_machine_model_lot_id = None
                            except Exception as e:
                                logging.error(f"Error fetching previous curing_machine_model_lot_id for lot {prev_lot_number}: {e}")
                                curing_previous_machine_model_lot_id = None

                        previous_lot_id = f"Curing_{machine_no}_{model_number}_{last_lot_number - 1}" if last_lot_number > 1 else None
                        merge_df.loc[batch_indices, "lot_id"] = unique_lot_id_current
                        merge_df.loc[batch_indices, "previous_lot_id"] = previous_lot_id
                        merge_df.loc[batch_indices, "curing_machine_model_lot_id"] = current_curing_machine_model_lot_id
                        merge_df.loc[batch_indices, "curing_previous_machine_model_lot_id"] = curing_previous_machine_model_lot_id

                        prev_key = (model_number, machine_no, curing_previous_machine_model_lot_id)
                        prev_lot_avg = machine_lot_avg_map.get(prev_key, None)
                        if prev_lot_avg is None and curing_previous_machine_model_lot_id is not None:
                            try:
                                prev_lot_doc = averages_collect.find_one(
                                    {
                                        "gt_code": model_number,
                                        "machine": machine_no,
                                        "curing_machine_model_lot_id": curing_previous_machine_model_lot_id
                                    },
                                    {"current_lot_averages": 1}
                                )
                                prev_lot_avg = prev_lot_doc.get("current_lot_averages", {}) if prev_lot_doc else {}
                                if prev_lot_avg:
                                    machine_lot_avg_map[prev_key] = prev_lot_avg
                                    logging.info(f"Fetched previous lot averages for {curing_previous_machine_model_lot_id} from database")
                                else:
                                    logging.warning(f"No previous lot averages found for {curing_previous_machine_model_lot_id} in database")
                            except Exception as e:
                                logging.error(f"Error fetching previous lot averages for {curing_previous_machine_model_lot_id}: {e}")
                                prev_lot_avg = {}
                        if last_lot_number == 1:
                            prev_lot_avg = {}
                            # logging.info(f"First lot for model {model_number}, machine {machine_no}. Setting previous_lot_averages to empty.")
                        # logging.info(f"prev_key: {prev_key}, prev_lot_avg: {prev_lot_avg}")

                        last_lot_dict[machine_model_key]["max_lot_number"] = last_lot_number
                        last_lot_dict[machine_model_key]["last_lot_id"] = unique_lot_id_current

                        lot_doc = {
                            "gt_code": model_number,
                            "machine": machine_no,
                            "lot_id": unique_lot_id_current,
                            "timestamp": some_function()
                        }
                        try:
                            if not lot_curing_collect.find_one({"lot_id": unique_lot_id_current}):
                                lot_curing_collect.insert_one(lot_doc)
                                output_messages.append(f"Inserted lot_id {unique_lot_id_current} into lot_curing_collect")
                        except Exception as e:
                            logging.error(f"Error inserting lot_id {unique_lot_id_current} into lot_curing_collect: {e}")
                            output_messages.append(f"❌ Error inserting lot_id {unique_lot_id_current}: {e}")

                        # logging.info(f"Assigned lot_id: {unique_lot_id_current}, curing_machine_model_lot_id: {current_curing_machine_model_lot_id}, curing_previous_machine_model_lot_id: {curing_previous_machine_model_lot_id}")

                        for param in conicity_params:
                            if param in batch_data.columns:
                                batch_data[param] = pd.to_numeric(batch_data[param], errors='coerce')

                        processed_machine_barcodes = batch_size
                        balance_machine_barcodes = total_curing_barcodes - (barcode_counter + batch_size)
                        balance_barcodes = total_barcodes_model - (barcode_counter + batch_size)

                        lot_averages = {}
                        for param in conicity_params:
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
                            "lsft_yield": lsft_yield(batch_data),
                            "lfh1_yield": lfh1_yield(batch_data),
                            "conicity_yield": conicity_yield(batch_data),
                            "oe_yield": oe_yield(batch_data),
                            "rfmxa_yield": rfmxa_yield(batch_data),
                            "rfa1_yield": rfa1_yield(batch_data),
                            "rfa2_yield": rfa2_yield(batch_data),
                            "lfxma_yield": lfxma_yield(batch_data),
                            "lfa1_yield": lfa1_yield(batch_data),
                            "central_radial_runout_yield": central_radial_runout_yield(batch_data),
                            "central_radial_runout_first_harmonic_yield": central_radial_runout_first_harmonic_yield(batch_data),
                            "lateral_run_out_top_over_yield": lateral_run_out_top_over_yield(batch_data),
                            "lateral_run_out_bottom_over_yield": lateral_run_out_bottom_over_yield(batch_data),
                            "lateral_run_out_yield": lateral_run_out_yield(batch_data),
                            "lateral_run_out_bottom_first_harmonic_yield": lateral_run_out_bottom_first_harmonic_yield(batch_data),
                            "lateral_run_out_top_first_harmonic_yield": lateral_run_out_top_first_harmonic_yield(batch_data),
                            "lateral_run_out_harmonic_yield": lateral_run_out_harmonic_yield(batch_data),
                            "bulge_top_over_yield": bulge_top_over_yield(batch_data),
                            "bulge_bottom_over_yield": bulge_bottom_over_yield(batch_data),
                            "bulge_yield": bulge_yield(batch_data),
                            "dent_top_over_yield": dent_top_over_yield(batch_data),
                            "dent_bottom_over_yield": dent_bottom_over_yield(batch_data),
                            "dent_yield": dent_yield(batch_data),
                            "geometry_oe_yield": geometry_oe_yield(batch_data),
                            "lower_yield": lower_yield(batch_data),
                            "upper_yield": upper_yield(batch_data),
                            "static_yield": static_yield(batch_data),
                            "couple_yield": couple_yield(batch_data),
                            "balancing_oe_yield": balancing_oe_yield(batch_data),
                            "warehouse_yield": warehouse_yield(batch_data),
                            "visual_yield": visual_yield(batch_data)})
                                                                                

                        grouped_averages = {}
                        param_spec_map = {
                            "cony_kg": spec_cony_kg,
                            "rfpp_cw_kg": spec_rfpp_cw_kg,
                            "rfpp_ccw_kg": spec_rfpp_ccw_kg,
                            "rfh1_cw_kg": spec_rfh1_cw_kg,
                            "rfh1_ccw_kg": spec_rfh1_ccw_kg,
                            "lfpp_cw_kg": spec_lfpp_cw_kg,
                            "lfpp_ccw_kg": spec_lfpp_ccw_kg,
                            'rfh2_cw_kg': spec_rfh2_cw_kg,
                            'rfh2_ccw_kg': spec_rfh2_ccw_kg,
                            'lsft_cw_kg': spec_lsft_cw_kg,
                            'lsft_ccw_kg': spec_lsft_ccw_kg,
                            'lfh1_cw_kg': spec_lfh1_cw_kg,
                            'lfh1_ccw_kg': spec_lfh1_ccw_kg,
                            'plysteer_kg': spec_plysteer_kg,
                            'rfmxa_ccw_deg':spec_rfmxa_ccw_deg,
                            'rfa1_cw_deg': spec_rfa1_cw_deg,
                            'rfa1_ccw_deg': spec_rfa1_ccw_deg,
                            'rfa2_cw_deg': spec_rfa2_cw_deg,
                            'rfa2_ccw_deg': spec_rfa2_ccw_deg,
                            'lfmxa_cw_deg': spec_lfmxa_cw_deg,
                            'lfmxa_ccw_deg': spec_lfmxa_ccw_deg,
                            'lfa1_cw_deg': spec_lfa1_cw_deg,
                            'lfa1_ccw_deg': spec_lfa1_ccw_deg,
                            'couple_g': spec_couple_g,
                            'botplane_deg': spec_dynamic_botplane_deg,
                            'topplane_deg': spec_dynamic_topplane_deg,
                            'couple_deg': spec_couple_deg,
                            'static_deg': spec_static_deg,
                            'botbul_mm': spec_bul_botbul_mm,
                            'botdep_mm': spec_dep_botdep_mm,
                            'topbul_mm': spec_bul_topbul_mm,
                            'topdep_mm': spec_dep_topdep_mm,
                            'blro_mm': spec_lro_blro_mm,
                            'blroh1_mm': spec_lro_blroh1_mm,
                            'crro_mm': spec_rro_crro_mm,
                            'crroh1_mm': spec_rro_crroh1_mm,
                            'tlro_mm': spec_lro_tlro_mm,
                            'tlroh1_mm': spec_lro_tlroh1_mm,
                            'botbul_deg': spec_bul_botbul_deg,
                            'botdep_deg': spec_dep_botdep_deg,
                            'topbul_deg': spec_dep_topbul_deg,
                            'topdep_deg': spec_bul_topdep_deg,
                            'blro_deg': spec_lro_blro_deg,
                            'blroh1_deg': spec_lro_blroh1_deg,
                            'crro_deg': spec_rro_crro_deg,
                            'crroh1_deg': spec_rro_crroh1_deg,
                            'tlro_deg': spec_lro_tlro_deg,
                            'tlroh1_deg': spec_lro_tlroh1_deg,
                            'botplane_g': spec_dynamic_botplane_g,
                            'topplane_g': spec_dynamic_topplane_g,
                            "static_g": spec_static_g,

                            "rfv_yield": rfv_yield_spec_yield,
                            "rfh1_yield": rfh1_yield_spec_yield,
                            "rfh2_yield": rfh2_yield_spec_yield,
                            "lfv_yield": lfv_yield_spec_yield,
                            "lsft_yield": lsft_yield_spec_yield,
                            "lfh1_yield": lfh1_yield_spec_yield,
                            "conicity_yield": conicity_yield_spec_yield,
                            "oe_yield": oe_yield_spec_yield,
                            "rfmxa_yield": rfmxa_yield_spec_yield,
                            "rfa1_yield": rfa1_yield_spec_yield,
                            "rfa2_yield": rfa2_yield_spec_yield,
                            "lfxma_yield": lfxma_yield_spec_yield,
                            "lfa1_yield": lfa1_yield_spec_yield,
                            "central_radial_runout_yield": central_radial_runout_yield_spec_yield,
                            "central_radial_runout_first_harmonic_yield": central_radial_runout_first_harmonic_yield_spec_yield,
                            "lateral_run_out_top_over_yield": lateral_run_out_top_over_yield_spec_yield,
                            "lateral_run_out_bottom_over_yield": lateral_run_out_bottom_over_yield_spec_yield,
                            "lateral_run_out_yield": lateral_run_out_yield_spec_yield,
                            "lateral_run_out_bottom_first_harmonic_yield": lateral_run_out_bottom_first_harmonic_yield_spec_yield,
                            "lateral_run_out_top_first_harmonic_yield": lateral_run_out_top_first_harmonic_yield_spec_yield,
                            "lateral_run_out_harmonic_yield": lateral_run_out_harmonic_yield_spec_yield,
                            "bulge_top_over_yield": bulge_top_over_yield_spec_yield,
                            "bulge_bottom_over_yield": bulge_bottom_over_yield_spec_yield,
                            "bulge_yield": bulge_yield_spec_yield,
                            "dent_top_over_yield": dent_top_over_yield_spec_yield,
                            "dent_bottom_over_yield": dent_bottom_over_yield_spec_yield,
                            "dent__yield": dent_yield_spec_yield,
                            "geometry_oe_yield": geometry_oe_yield_spec_yield,
                            "lower_yield": lower_yield_spec_yield,
                            "upper_yield": upper_yield_spec_yield,
                            "static_yield": static_yield_spec_yield,
                            "couple_yield": couple_yield_spec_yield,
                            "balancing_oe_yield": balancing_oe_yield_spec_yield,
                            "warehouse_yield": warehouse_yield_spec_yield,
                            "visual_yield": visual_yield_spec_yield
                        }

                        try:
                            batch_data["ubno"] = batch_data["ubno"].astype(str).str.strip()
                            batch_data.sort_values("date_of_ufm", ascending=True, inplace=True)
                            ubno_grouped = batch_data.groupby("ubno")
                            ubno_records = []
                            for ubno, group in ubno_grouped:
                                group.sort_values("date_of_ufm", ascending=True)
                                record = {
                                    "ubno": ubno,
                                    "barcode_count": len(group["barcode"]),
                                    "tub_date_start_time": group["date_of_ufm"].iloc[0] if not group["date_of_ufm"].empty else None,
                                    "tub_date_end_time": group["date_of_ufm"].iloc[-1] if not group["date_of_ufm"].empty else None,
                                    "barcodes": group["barcode"].tolist() if "barcode" in group.columns else []
                                }
                                parameters = []
                                for param in conicity_params:
                                    try:
                                        if param in group.columns:
                                            valid_vals = group[param].dropna()
                                            avg_val = float(valid_vals.mean()) if not valid_vals.empty else None
                                        else:
                                            avg_val = None
                                        record[f"avg_{param}"] = avg_val
                                        spec = param_spec_map.get(param)
                                        exceeded = False
                                        if param == "cony_kg" and spec_cony_kg is not None and avg_val is not None:
                                            exceeded = not (-spec_cony_kg <= avg_val <= spec_cony_kg)
                                        if spec is not None and param not in ["cony_kg"] and avg_val is not None:
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
                                        exceeded = avg_val < spec if spec is not None else False
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
                            grouped_averages["ubno"] = ubno_records
                        except:
                            grouped_averages["ubno"] = []

                        batch_data1 = batch_data.copy()
                        # batch_data.sort_values("date_of_tbm", ascending=True, inplace=True)
                        batch_data1.sort_values("date_of_ufm", ascending=True, inplace=True)
                        batch_data.sort_values("date_of_curing", ascending=True, inplace=True)
                        batch_data1.sort_values("date_of_ufm", ascending=True, inplace=True)
                        machine_lot_start_build_time = batch_data["date_of_tbm"].iloc[0]
                        machine_lot_end_build_time = batch_data["date_of_tbm"].iloc[-1]
                        machine_lot_start_curing_time = batch_data["date_of_curing"].iloc[0]
                        machine_lot_end_curing_time = batch_data["date_of_curing"].iloc[-1]
                        tub_date_start_time = batch_data1["date_of_ufm"].iloc[0]
                        tub_date_end_time = batch_data1["date_of_ufm"].iloc[-1]
                        lot_start_time = some_function()
                        lot_end_time = some_function()

                        parameter_alerts = []
                        overall_shift_occurred = False
                        prev_key = (model_number, machine_no, curing_previous_machine_model_lot_id)
                        prev_lot_avg = machine_lot_avg_map.get(prev_key, None)

                        if prev_lot_avg:
                            for param in conicity_params + yield_params:
                                shift = False
                                shift_occurred_spec = False
                                current_val = lot_averages.get(param)
                                prev_val = prev_lot_avg.get(param)
                                if current_val is not None and prev_val is not None:
                                    if param == "cony_kg" and spec_cony_kg is not None:
                                        if abs(current_val - prev_val) > threshold_value_cony_kg:
                                            shift = True
                                        if not (-spec_cony_kg <= current_val <= spec_cony_kg):
                                            shift_occurred_spec = True
                                    if param == "rfpp_cw_kg" and spec_rfpp_cw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_rfpp_cw_kg:
                                            shift = True
                                        if current_val > spec_rfpp_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfpp_ccw_kg" and spec_rfpp_ccw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_rfpp_ccw_kg:
                                            shift = True
                                        if current_val > spec_rfpp_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfh1_cw_kg" and spec_rfh1_cw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_rfh1_cw_kg:
                                            shift = True
                                        if current_val > spec_rfh1_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfh1_ccw_kg" and spec_rfh1_ccw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_rfh1_ccw_kg:
                                            shift = True
                                        if current_val > spec_rfh1_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfh2_cw_kg" and spec_rfh2_cw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_rfh2_cw_kg:
                                            shift = True
                                        if current_val > spec_rfh2_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfh2_ccw_kg" and spec_rfh2_ccw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_rfh2_ccw_kg:
                                            shift = True
                                        if current_val > spec_rfh2_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "lfpp_cw_kg" and spec_lfpp_cw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_lfpp_cw_kg:
                                            shift = True
                                        if current_val > spec_lfpp_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "lfpp_ccw_kg" and spec_lfpp_ccw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_lfpp_ccw_kg:
                                            shift = True
                                        if current_val > spec_lfpp_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "lsft_cw_kg" and spec_lsft_cw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_lsft_cw_kg:
                                            shift = True
                                        if current_val > spec_lsft_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "lsft_ccw_kg" and spec_lsft_ccw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_lsft_ccw_kg:
                                            shift = True
                                        if current_val > spec_lsft_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "lfh1_cw_kg" and spec_lfh1_cw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_lfh1_cw_kg:
                                            shift = True
                                        if current_val > spec_lfh1_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "lfh1_ccw_kg" and spec_lfh1_ccw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_lfh1_ccw_kg:
                                            shift = True
                                        if current_val > spec_lfh1_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "plysteer_kg" and spec_plysteer_kg is not None:
                                        if (current_val - prev_val) > threshold_value_plysteer_kg:
                                            shift = True
                                        if current_val > spec_plysteer_kg:
                                            shift_occurred_spec = True
                                    if param == "rfmxa_ccw_deg" and spec_rfmxa_ccw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_rfmxa_ccw_deg:
                                            shift = True
                                        if current_val > spec_rfmxa_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "rfa1_cw_deg" and spec_rfa1_cw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_rfa1_cw_deg:
                                            shift = True
                                        if current_val > spec_rfa1_cw_deg:
                                            shift_occurred_spec = True
                                    if param == "rfa1_ccw_deg" and spec_rfa1_ccw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_rfa1_ccw_deg:
                                            shift = True
                                        if current_val > spec_rfa1_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "rfa2_cw_deg" and spec_rfa2_cw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_rfa2_cw_deg:
                                            shift = True
                                        if current_val > spec_rfa2_cw_deg:
                                            shift_occurred_spec = True
                                    if param == "rfa2_ccw_deg" and spec_rfa2_ccw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_rfa2_ccw_deg:
                                            shift = True
                                        if current_val > spec_rfa2_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "lfmxa_cw_deg" and spec_lfmxa_cw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_lfmxa_cw_deg:
                                            shift = True
                                        if current_val > spec_lfmxa_cw_deg:
                                            shift_occurred_spec = True
                                    if param == "lfmxa_ccw_deg" and spec_lfmxa_ccw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_lfmxa_ccw_deg:
                                            shift = True
                                        if current_val > spec_lfmxa_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "lfa1_cw_deg" and spec_lfa1_cw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_lfa1_cw_deg:
                                            shift = True
                                        if current_val > spec_lfa1_cw_deg:
                                            shift_occurred_spec = True
                                    if param == "lfa1_ccw_deg" and spec_lfa1_ccw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_lfa1_ccw_deg:
                                            shift = True
                                        if current_val > spec_lfa1_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "static_g" and spec_static_g is not None:
                                        if (current_val - prev_val) > threshold_value_bal_static_g:
                                            shift = True
                                        if current_val > spec_static_g:
                                            shift_occurred_spec = True
                                    if param == "static_deg" and spec_static_deg is not None:
                                        if (current_val - prev_val) > threshold_value_bal_static_deg:
                                            shift = True
                                        if current_val > spec_static_deg:
                                            shift_occurred_spec = True
                                    if param == "couple_g" and spec_couple_g is not None:
                                        if (current_val - prev_val) > threshold_value_bal_couple_g:
                                            shift = True
                                        if current_val > spec_couple_g:
                                            shift_occurred_spec = True
                                    if param == "couple_deg" and spec_couple_deg is not None:
                                        if (current_val - prev_val) > threshold_value_bal_couple_deg:
                                            shift = True
                                        if current_val > spec_couple_deg:
                                            shift_occurred_spec = True
                                    if param == "botbul_mm" and spec_bul_botbul_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_bul_botbul_mm:
                                            shift = True
                                        if current_val > spec_bul_botbul_mm:
                                            shift_occurred_spec = True
                                    if param == "topbul_mm" and spec_bul_topbul_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_bul_topbul_mm:
                                            shift = True
                                        if current_val > spec_bul_topbul_mm:
                                            shift_occurred_spec = True
                                    if param == "botbul_deg" and spec_bul_botbul_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_bul_botbul_deg:
                                            shift = True
                                        if current_val > spec_bul_botbul_deg:
                                            shift_occurred_spec = True
                                    if param == "topdep_deg" and spec_bul_topdep_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_bul_topdep_deg:
                                            shift = True
                                        if current_val > spec_bul_topdep_deg:
                                            shift_occurred_spec = True
                                    if param == "botdep_mm" and spec_dep_botdep_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_dep_botdep_mm:
                                            shift = True
                                        if current_val > spec_dep_botdep_mm:
                                            shift_occurred_spec = True
                                    if param == "topdep_mm" and spec_dep_topdep_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_dep_topdep_mm:
                                            shift = True
                                        if current_val > spec_dep_topdep_mm:
                                            shift_occurred_spec = True
                                    if param == "botdep_deg" and spec_dep_botdep_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_dep_botdep_deg:
                                            shift = True
                                        if current_val > spec_dep_botdep_deg:
                                            shift_occurred_spec = True
                                    if param == "topbul_deg" and spec_dep_topbul_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_dep_topbul_deg:
                                            shift = True
                                        if current_val > spec_dep_topbul_deg:
                                            shift_occurred_spec = True
                                    if param == "crro_mm" and spec_rro_crro_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_rro_crro_mm:
                                            shift = True
                                        if current_val > spec_rro_crro_mm:
                                            shift_occurred_spec = True
                                    if param == "crroh1_mm" and spec_rro_crroh1_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_rro_crroh1_mm:
                                            shift = True
                                        if current_val > spec_rro_crroh1_mm:
                                            shift_occurred_spec = True
                                    if param == "blro_mm" and spec_lro_blro_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_lro_blro_mm:
                                            shift = True
                                        if current_val > spec_lro_blro_mm:
                                            shift_occurred_spec = True
                                    if param == "blroh1_mm" and spec_lro_blroh1_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_lro_blroh1_mm:
                                            shift = True
                                        if current_val > spec_lro_blroh1_mm:
                                            shift_occurred_spec = True
                                    if param == "tlro_mm" and spec_lro_tlro_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_lro_tlro_mm:
                                            shift = True
                                        if current_val > spec_lro_tlro_mm:
                                            shift_occurred_spec = True
                                    if param == "tlroh1_mm" and spec_lro_tlroh1_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_lro_tlroh1_mm:
                                            shift = True
                                        if current_val > spec_lro_tlroh1_mm:
                                            shift_occurred_spec = True
                                    if param == "crro_deg" and spec_rro_crro_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_rro_crro_deg:
                                            shift = True
                                        if current_val > spec_rro_crro_deg:
                                            shift_occurred_spec = True
                                    if param == "crroh1_deg" and spec_rro_crroh1_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_rro_crroh1_deg:
                                            shift = True
                                        if current_val > spec_rro_crroh1_deg:
                                            shift_occurred_spec = True
                                    if param == "blro_deg" and spec_lro_blro_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_lro_blro_deg:
                                            shift = True
                                        if current_val > spec_lro_blro_deg:
                                            shift_occurred_spec = True
                                    if param == "blroh1_deg" and spec_lro_blroh1_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_lro_blroh1_deg:
                                            shift = True
                                        if current_val > spec_lro_blroh1_deg:
                                            shift_occurred_spec = True
                                    if param == "tlro_deg" and spec_lro_tlro_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_lro_tlro_deg:
                                            shift = True
                                        if current_val > spec_lro_tlro_deg:
                                            shift_occurred_spec = True
                                    if param == "tlroh1_deg" and spec_lro_tlroh1_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_lro_tlroh1_deg:
                                            shift = True
                                        if current_val > spec_lro_tlroh1_deg:
                                            shift_occurred_spec = True
                                    if param == "botplane_g" and spec_dynamic_botplane_g is not None:
                                        if (current_val - prev_val) > threshold_value_bal_dynamic_botplane_g:
                                            shift = True
                                        if current_val > spec_dynamic_botplane_g:
                                            shift_occurred_spec = True
                                    if param == "topplane_g" and spec_dynamic_topplane_g is not None:
                                        if (current_val - prev_val) > threshold_value_bal_dynamic_topplane_g:
                                            shift = True
                                        if current_val > spec_dynamic_topplane_g:
                                            shift_occurred_spec = True
                                    if param == "botplane_deg" and spec_dynamic_botplane_deg is not None:
                                        if (current_val - prev_val) > threshold_value_bal_dynamic_botplane_deg:
                                            shift = True
                                        if current_val > spec_dynamic_botplane_deg:
                                            shift_occurred_spec = True
                                    if param == "topplane_deg" and spec_dynamic_topplane_deg is not None:
                                        if (current_val - prev_val) > threshold_value_bal_dynamic_topplane_deg:
                                            shift = True
                                        if current_val > spec_dynamic_topplane_deg:
                                            shift_occurred_spec = True

                                    if param in yield_params:
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True

                                    # if param == "oe_yield":
                                    #     spec_yield = param_spec_map.get(param)
                                    #     if spec_yield is not None:
                                    #         if (prev_val > current_val) and (abs(current_val - prev_val) > (class_threshold_value_yield_class_yield)):
                                    #             shift = True
                                    #         if abs(current_val < spec_yield):
                                    #             shift_occurred_spec = True
                                    # if param == "balancing_oe_yield":
                                    #     spec_yield = param_spec_map.get(param)
                                    #     if spec_yield is not None:
                                    #         if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_balancing_yield)):
                                    #             shift = True
                                    #         if abs(current_val < spec_yield):
                                    #             shift_occurred_spec = True
                                    # if param == "geometry_oe_yield":
                                    #     spec_yield = param_spec_map.get(param)
                                    #     if spec_yield is not None:
                                    #         if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_geometry_yield)):
                                    #             shift = True
                                    #         if abs(current_val < spec_yield):
                                    #             shift_occurred_spec = True
                                    
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
                                parameter_alerts.append(alert_dict)
                                if shift_occurred:
                                    overall_shift_occurred = True
                        else:
                            for param in conicity_params + yield_params:
                                shift_occurred_thershold = False
                                shift_occurred_spec = False
                                current_val = lot_averages.get(param)
                                if current_val is not None:
                                    if param == "cony_kg" and spec_cony_kg is not None:
                                        if not (-spec_cony_kg <= current_val <= spec_cony_kg):
                                            shift_occurred_spec = True
                                    if param == "rfpp_cw_kg" and spec_rfpp_cw_kg is not None:
                                        if current_val > spec_rfpp_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfpp_ccw_kg" and spec_rfpp_ccw_kg is not None:
                                        if current_val > spec_rfpp_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfh1_cw_kg" and spec_rfh1_cw_kg is not None:
                                        if current_val > spec_rfh1_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfh1_ccw_kg" and spec_rfh1_ccw_kg is not None:
                                        if current_val > spec_rfh1_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfh2_cw_kg" and spec_rfh2_cw_kg is not None:
                                        if current_val > spec_rfh2_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfh2_ccw_kg" and spec_rfh2_ccw_kg is not None:
                                        if current_val > spec_rfh2_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "lfpp_cw_kg" and spec_lfpp_cw_kg is not None:
                                        if current_val > spec_lfpp_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "lfpp_ccw_kg" and spec_lfpp_ccw_kg is not None:
                                        if current_val > spec_lfpp_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "lsft_cw_kg" and spec_lsft_cw_kg is not None:
                                        if current_val > spec_lsft_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "lsft_ccw_kg" and spec_lsft_ccw_kg is not None:
                                        if current_val > spec_lsft_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "lfh1_cw_kg" and spec_lfh1_cw_kg is not None:
                                        if current_val > spec_lfh1_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "lfh1_ccw_kg" and spec_lfh1_ccw_kg is not None:
                                        if current_val > spec_lfh1_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "plysteer_kg" and spec_plysteer_kg is not None:
                                        if current_val > spec_plysteer_kg:
                                            shift_occurred_spec = True
                                    if param == "rfmxa_ccw_deg" and spec_rfmxa_ccw_deg is not None:
                                        if current_val > spec_rfmxa_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "rfa1_cw_deg" and spec_rfa1_cw_deg is not None:
                                        if current_val > spec_rfa1_cw_deg:
                                            shift_occurred_spec = True
                                    if param == "rfa1_ccw_deg" and spec_rfa1_ccw_deg is not None:
                                        if current_val > spec_rfa1_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "rfa2_cw_deg" and spec_rfa2_cw_deg is not None:
                                        if current_val > spec_rfa2_cw_deg:
                                            shift_occurred_spec = True
                                    if param == "rfa2_ccw_deg" and spec_rfa2_ccw_deg is not None:
                                        if current_val > spec_rfa2_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "lfmxa_cw_deg" and spec_lfmxa_cw_deg is not None:
                                        if current_val > spec_lfmxa_cw_deg:
                                            shift_occurred_spec = True
                                    if param == "lfmxa_ccw_deg" and spec_lfmxa_ccw_deg is not None:
                                        if current_val > spec_lfmxa_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "lfa1_cw_deg" and spec_lfa1_cw_deg is not None:
                                        if current_val > spec_lfa1_cw_deg:
                                            shift_occurred_spec = True
                                    if param == "lfa1_ccw_deg" and spec_lfa1_ccw_deg is not None:
                                        if current_val > spec_lfa1_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "static_g" and spec_static_g is not None:
                                        if current_val > spec_static_g:
                                            shift_occurred_spec = True
                                    if param == "static_deg" and spec_static_deg is not None:
                                        if current_val > spec_static_deg:
                                            shift_occurred_spec = True
                                    if param == "couple_g" and spec_couple_deg is not None:
                                        if current_val > spec_couple_deg:
                                            shift_occurred_spec = True
                                    if param == "couple_deg" and spec_couple_deg is not None:
                                        if current_val > spec_couple_deg:
                                            shift_occurred_spec = True
                                    if param == "botbul_mm" and spec_bul_botbul_mm is not None:
                                        if current_val > spec_bul_botbul_mm:
                                            shift_occurred_spec = True
                                    if param == "topbul_mm" and spec_bul_topbul_mm is not None:
                                        if current_val > spec_bul_topbul_mm:
                                            shift_occurred_spec = True
                                    if param == "botbul_deg" and spec_bul_botbul_deg is not None:
                                        if current_val > spec_bul_botbul_deg:
                                            shift_occurred_spec = True
                                    if param == "topdep_deg" and spec_bul_topdep_deg is not None:
                                        if current_val > spec_bul_topdep_deg:
                                            shift_occurred_spec = True
                                    if param == "botdep_mm" and spec_dep_botdep_mm is not None:
                                        if current_val > spec_dep_botdep_mm:
                                            shift_occurred_spec = True
                                    if param == "topdep_mm" and spec_dep_topdep_mm is not None:
                                        if current_val > spec_dep_topdep_mm:
                                            shift_occurred_spec = True
                                    if param == "botdep_deg" and spec_dep_botdep_deg is not None:
                                        if current_val > spec_dep_botdep_deg:
                                            shift_occurred_spec = True
                                    if param == "topbul_deg" and spec_dep_topbul_deg is not None:
                                        if current_val > spec_dep_topbul_deg:
                                            shift_occurred_spec = True
                                    if param == "crro_mm" and spec_rro_crro_mm is not None:
                                        if current_val > spec_rro_crro_mm:
                                            shift_occurred_spec = True
                                    if param == "crroh1_mm" and spec_rro_crroh1_mm is not None:
                                        if current_val > spec_rro_crroh1_mm:
                                            shift_occurred_spec = True
                                    if param == "blro_mm" and spec_lro_blro_mm is not None:
                                        if current_val > spec_lro_blro_mm:
                                            shift_occurred_spec = True
                                    if param == "blroh1_mm" and spec_lro_blroh1_mm is not None:
                                        if current_val > spec_lro_blroh1_mm:
                                            shift_occurred_spec = True
                                    if param == "tlro_mm" and spec_lro_tlro_mm is not None:
                                        if current_val > spec_lro_tlro_mm:
                                            shift_occurred_spec = True
                                    if param == "tlroh1_mm" and spec_lro_tlroh1_mm is not None:
                                        if current_val > spec_lro_tlroh1_mm:
                                            shift_occurred_spec = True
                                    if param == "crro_deg" and spec_rro_crro_deg is not None:
                                        if current_val > spec_rro_crro_deg:
                                            shift_occurred_spec = True
                                    if param == "crroh1_deg" and spec_rro_crroh1_deg is not None:
                                        if current_val > spec_rro_crroh1_deg:
                                            shift_occurred_spec = True
                                    if param == "blro_deg" and spec_lro_blro_deg is not None:
                                        if current_val > spec_lro_blro_deg:
                                            shift_occurred_spec = True
                                    if param == "blroh1_deg" and spec_lro_blroh1_deg is not None:
                                        if current_val > spec_lro_blroh1_deg:
                                            shift_occurred_spec = True
                                    if param == "tlro_deg" and spec_lro_tlro_deg is not None:
                                        if current_val > spec_lro_tlro_deg:
                                            shift_occurred_spec = True
                                    if param == "tlroh1_deg" and spec_lro_tlroh1_deg is not None:
                                        if current_val > spec_lro_tlroh1_deg:
                                            shift_occurred_spec = True
                                    if param == "botplane_g" and spec_dynamic_botplane_g is not None:
                                        if current_val > spec_dynamic_botplane_g:
                                            shift_occurred_spec = True
                                    if param == "topplane_g" and spec_dynamic_topplane_g is not None:
                                        if current_val > spec_dynamic_topplane_g:
                                            shift_occurred_spec = True
                                    if param == "botplane_deg" and spec_dynamic_botplane_deg is not None:
                                        if current_val > spec_dynamic_botplane_deg:
                                            shift_occurred_spec = True
                                    if param == "topplane_deg" and spec_dynamic_topplane_deg is not None:
                                        if current_val > spec_dynamic_topplane_deg:
                                            shift_occurred_spec = True
                                    if param in yield_params:
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if current_val < spec_yield:
                                                shift_occurred_spec = True

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

                        shift_alerts = [alert for alert in parameter_alerts if alert["shift_occurred"]]
                        logging.debug(f"Model {model_number}, Machine {machine_no}, Lot {current_curing_machine_model_lot_id}: {len(shift_alerts)} parameter alerts with shift_occurred=True")
                        if shift_alerts:
                            logging.debug(f"Shift alerts: {[alert['parameter'] for alert in shift_alerts]}")

                        overall_shift_occurred = bool(overall_shift_occurred)

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
                                        if col in batch_data.columns:
                                            value_counts = batch_data[col].value_counts().to_dict()
                                            for rank_value, count in value_counts.items():
                                                rank_value = int(rank_value) if isinstance(rank_value, (int, float)) else rank_value
                                                rank_counts[f"{col}_{rank_value}"] = int(count)
                                    alert["rank_counts"] = rank_counts

                        if deviated_parameter_alerts:
                            deviated_doc = {
                                "alert_id_tbm_ufm": last_alert_id + 1,
                                "gt_code": model_number,
                                "lot_id": unique_lot_id_current,
                                "machine": machine_no,
                                "comments": [],
                                "alert_assign_user": [],
                                "curing_machine_model_lot_id": current_curing_machine_model_lot_id,
                                "curing_previous_machine_model_lot_id": curing_previous_machine_model_lot_id,
                                "parameter_alerts": deviated_parameter_alerts,
                                "timestamp_alert_generated": some_function()
                            }
                            deviated_triggering_bulk_operations.append(pymongo.InsertOne(deviated_doc))
                            # logging.info(f"Prepared {len(deviated_parameter_alerts)} derived alerts for machine {machine_no}, lot {unique_lot_id_current}")

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
                                        if col in batch_data.columns:
                                            value_counts = batch_data[col].value_counts().to_dict()
                                            for rank_value, count in value_counts.items():
                                                rank_value = int(rank_value) if isinstance(rank_value, (int, float)) else rank_value
                                                rank_counts[f"{col}_{rank_value}"] = int(count)
                                    alert["rank_counts"] = rank_counts


                        batch_data["tbm_shift"] = batch_data["date_of_tbm"].apply(get_shift)
                        shift_groups = batch_data.groupby('tbm_shift')
                        shifts_data_list_tbm = []
                        for shift_name, shift_data in shift_groups:
                            shift_start_time = shift_data["date_of_tbm"].iloc[0]
                            shift_end_time = shift_data["date_of_tbm"].iloc[-1]
                            shifts_data_list_tbm.append({
                                "shift": shift_name,
                                "barcode_count": len(shift_data["barcode"]) if "barcode" in shift_data.columns else 0,
                                "machine_shift_tbm_start_time": shift_start_time,
                                "machine_shift_tbm_end_time": shift_end_time,
                                "barcodes": shift_data["barcode"].tolist() if "barcode" in shift_data.columns else []
                            })


                        batch_data["curing_shift"] = batch_data["date_of_curing"].apply(get_shift)

                        shift_groups = batch_data.groupby('curing_shift')
                        shifts_data_list = []
                        for shift_name, shift_data in shift_groups:
                            shift_start_time = shift_data["date_of_curing"].iloc[0]
                            shift_end_time = shift_data["date_of_curing"].iloc[-1]
                            shifts_data_list.append({
                                "shifts": shift_name,
                                "barcode_count": len(shift_data["barcode"]),
                                "machine_shift_curing_start_time": shift_start_time,
                                "machine_shift_curing_end_time": shift_end_time,
                                "barcodes": shift_data["barcode"].tolist() if "barcode" in shift_data.columns else []
                            })

                        any_shift_flag = any(alert["shift_occurred"] for alert in parameter_alerts)

                        averages_doc = {
                            "gt_code": model_number,
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
                            "gt_code": model_number,
                            "lot_id": unique_lot_id_current,
                            "curing_machine_model_lot_id": current_curing_machine_model_lot_id
                        }
                        if not averages_collect.find_one(averages_query):
                            averages_bulk_operations.append(pymongo.InsertOne(averages_doc))

                        current_key = (model_number, machine_no, current_curing_machine_model_lot_id)
                        machine_lot_avg_map[current_key] = lot_averages
                        curing_previous_machine_model_lot_ids[(model_number, machine_no)] = current_curing_machine_model_lot_id

                        any_shift_flag = any(alert["shift_occurred"] for alert in parameter_alerts)
                        machine_alert = {
                            "machine": machine_no,
                            "tbm_machine": tbm_machine_value,  # MODIFIED: tbm_machine may be None if not created
                            "curing_shift_occured": any_shift_flag,
                            "shifts_data": shifts_data_list,
                            "oe_yield": oe_yield(batch_data),
                            "balancing_oe_yield": balancing_oe_yield(batch_data),
                            "geometry_oe_yield": geometry_oe_yield(batch_data),
                            "warehouse_yield": warehouse_yield(batch_data),
                            "visual_yield": visual_yield(batch_data),
                            "shifts_data_tbm": shifts_data_list_tbm,
                            "sizecode": sizecode,
                            "gt_code": str(gt_code),
                            "timestamp_lot_started": lot_start_time,
                            "timestamp_lot_ended": lot_end_time,
                            "target_machine_lot_size": target_lot_size,
                            "processed_machine_barcodes": int(processed_machine_barcodes),
                            "balance_machine_barcodes": int(balance_machine_barcodes),
                            "total_machine_barcodes": int(total_curing_barcodes),
                            "machine_lot_start_build_time": machine_lot_start_build_time,
                            "machine_lot_end_build_time": machine_lot_end_build_time,
                            "machine_lot_start_curing_time": machine_lot_start_curing_time,
                            "machine_lot_end_curing_time": machine_lot_end_curing_time,
                            "tub_date_start_time": tub_date_start_time,
                            "tub_date_end_time": tub_date_end_time,
                            "parameter_alerts": parameter_alerts,
                            "lot_averages": lot_averages,
                            "grouped_averages": grouped_averages,
                            "previous_lot_averages": machine_lot_avg_map.get((model_number, machine_no, curing_previous_machine_model_lot_id), {}),
                            "first_barcode_machine": batch_data.iloc[0]["barcode"] if "barcode" in batch_data.columns else None,
                            "last_barcode_machine": batch_data.iloc[-1]["barcode"] if "barcode" in batch_data.columns else None,
                            "curing_machine_model_lot_id": current_curing_machine_model_lot_id,
                            "curing_previous_machine_model_lot_id": curing_previous_machine_model_lot_id,
                            "rank_columns": get_rank_columns(batch_data),
                            "barcodes": batch_data["barcode"].tolist()
                        }

                        alert_key = (model_number, unique_lot_id_current)
                        if alert_key not in alert_map:
                            last_alert_id += 1
                            alert_map[alert_key] = {
                                "alert_id_curing_ufm": last_alert_id,
                                "gt_code": model_number,
                                "lot_id": unique_lot_id_current,
                                "machine_alerts": [],
                                "first_barcode_model": None,
                                "last_barcode_model": None
                            }
                        alert_map[alert_key]["machine_alerts"].append(machine_alert)
                        if machine_alert["curing_shift_occured"]:
                            alert_map[alert_key]["shift_occurred"] = True
                        if alert_map[alert_key]["first_barcode_model"] is None:
                            alert_map[alert_key]["first_barcode_model"] = machine_alert["first_barcode_machine"]
                        alert_map[alert_key]["last_barcode_model"] = machine_alert["last_barcode_machine"]

                        if overall_shift_occurred:
                            merge_df.loc[batch_indices, "alert_id_curing_ufm"] = last_alert_id

                        barcode_counter += batch_size

                for alert_key, alert_obj in alert_map.items():
                    final_alert = {
                        "alert_id_curing_ufm": alert_obj["alert_id_curing_ufm"],
                        "gt_code": alert_obj["gt_code"],
                        "lot_id": alert_obj["lot_id"],
                        "alert_flag": False,
                        "dashboard_flag": False,
                        "alert_status_check":"o",
                        "alert_status_timestamp": None,
                        "alert_status_comment": [],
                        "alert_assign_user": [],
                        "machine_alerts": alert_obj["machine_alerts"],
                        "first_barcode_model": alert_obj["first_barcode_model"],
                        "last_barcode_model": alert_obj["last_barcode_model"],
                        "timestamp_alert_generated": some_function()
                    }
                    alert_bulk_operations.append(pymongo.InsertOne(final_alert))

                for alert_key, alert_obj in alert_map.items():
                    true_machine_alerts = [ma for ma in alert_obj["machine_alerts"] if ma.get("curing_shift_occured", False)]
                    if true_machine_alerts:
                        doc_trigger = {
                            "alert_id_curing_ufm": alert_obj["alert_id_curing_ufm"],
                            "gt_code": alert_obj["gt_code"],
                            "lot_id": alert_obj["lot_id"],
                            "alert_flag": False,
                            "dashboard_flag": False,
                            "alert_status_check":"o",
                            "alert_status_timestamp": None,
                            "alert_status_comment": [],
                            "alert_assign_user": [],
                            "machine_alerts": true_machine_alerts,
                            "first_barcode_model": alert_obj["first_barcode_model"],
                            "last_barcode_model": alert_obj["last_barcode_model"],
                            "timestamp_alert_generated": some_function()
                        }
                        triggering_bulk_operations.append(pymongo.InsertOne(doc_trigger))

        except Exception as e:
            error_message = f"❌ Curing Error processing model number {model_number}: {e}"
            output_messages.append(error_message)

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
            inserted_counts["alert_curing_table_with_lot_size"] = result.inserted_count
            output_messages.append(f"✅ Curing Inserted {result.inserted_count} alert documents into alert_curing_table_with_lot_size.")
        except Exception as e:
            error_message = f"❌ Curing Error performing bulk write to alert_curing_table_with_lot_size: {e}"
            output_messages.append(error_message)
    else:
        output_messages.append("No Curing alert documents to insert into alert_curing_table_with_lot_size.")

    if deviated_triggering_bulk_operations:
        try:
            result = alert_triggering_deviated_collect.bulk_write(deviated_triggering_bulk_operations, ordered=False)
            inserted_counts["alert_curing_with_lot_size"] = result.inserted_count
            output_messages.append(f"✅ TBM Inserted {result.inserted_count} deviated alert documents into alert_deviated_final_yield.alert_curing_with_lot_size.")
        except Exception as e:
            error_message = f"❌ TBM Error performing bulk write to alert_deviated_final_yield.alert_curing_with_lot_size: {e}"
            output_messages.append(error_message)
    else:
        output_messages.append("No TBM deviated alert documents to insert into alert_deviated_final_yield.alert_curing_with_lot_size.")

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

    end_time = time.time()
    execution_time = end_time - start_time

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
        # MODIFIED: Only add tbm_machine to update if it exists in merge_df
        if "tbm_machine" in merge_df.columns:
            update["$set"]["tbm_machine"] = row.get("tbm_machine")
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

    if triggering_bulk_operations:
        try:
            triggering_result = alert_triggering_collect.bulk_write(triggering_bulk_operations, ordered=False)
            inserted_counts["alert_triggering_curing_with_lot_size"] = triggering_result.inserted_count
            output_messages.append(f"✅ Curing Inserted {triggering_result.inserted_count} machine_alert documents into alert_triggering_curing_with_lot_size.")
            result_dashboard = alert_triggering_collect_dashboard_curing.bulk_write(triggering_bulk_operations, ordered=False)
            inserted_counts["alert_triggering_curing_dashboard"] = result_dashboard.inserted_count
            output_messages.append(f"✅ TBM Inserted {result_dashboard.inserted_count} machine_alert documents into alert_db_final_yield.alert_triggering_curing_with_lot_size.")
        except Exception as e:
            error_message = f"❌ Curing Error performing bulk write to alert_triggering_curing_with_lot_size collection: {e}"
            output_messages.append(error_message)
    else:
        output_messages.append("No Curing machine_alert documents to insert into alert_triggering_curing_with_lot_size collection.")

    final_message = f"✅ Curing Process completed successfully in {execution_time:.2f} seconds!"
    output_messages.append(final_message)

    logging.info("process_curing_data_and_generate_lots_alerts function is completed")
    return {"status": "success", "message": final_message, "output_log": output_messages,
            "inserted_counts": inserted_counts, "updated_counts": updated_counts, "execution_time": execution_time}, 200

def process_lot_and_alert_data():
    process_start_time = time.time()
    output_messages = []
    inserted_counts = {}
    updated_counts = {}
    logging.info("process_lot_and_alert_data function is just started")
    try:
        # UFM Specific Collections
        merge_collect = client[UNIFORMITY_BALANCING_GEOMETRY_DB][COMBINED_COLLECTION_NAME]
        channel_collect = client[config_databse][channel_no_collect]
        machine_collect = client[config_databse][machine_no_collect]
        lot_ufm_collect = client[lot_db][lot_table_collect]
        alert_collect = client[alert_table_final_yield][alert_ufm_collect_name]
        averages_ufm_collect = client[average_db][averages_ufm_table]
        ub_machine_collect = client[config_databse][ub_machine_no_collect]
        alert_ufm_collect = client[alert_db_virgin_yield][alert_triggering_ubno_collect_name]
        alert_triggering_deviated_collect = client[alert_deviated_final_yield][alert_triggering_deviated_ufm]
        alert_triggering_collect_dashboard_ufm = client[alert_db_final_yield][alert_triggering_ubno_collect_name]
        ufm_collect_data = client[model_db][ufm_collection]
        ufm_machine_collect = client[model_db][ufm_collection_machine]
        logging.info("✅ UFM Collections loaded successfully for alert_db_virgin_yield and alert_db_final_yield.")
        output_messages.append("✅ UFM Collections loaded successfully for lot and alert processing in alert_db_virgin_yield and alert_db_final_yield.")
    except pymongo.errors.OperationFailure as e:
        error_message = f"❌ UFM Database or collection error during processing: {e}"
        logging.error(error_message)
        output_messages.append(error_message)
        return {"status": "error", "message": error_message, "output_log": output_messages}, 500

    # Fetch active machines
    try:
        active_machines = set(ufm_machine_collect.distinct("ubno", {"status": "active"}))
        output_messages.append(f"✅ Fetched {len(active_machines)} active machines: {active_machines}")
        if not active_machines:
            output_messages.append("❌ No active machines found in model_workcenter_db.ufm_machine. Exiting.")
            return {"status": "error", "message": "No active machines found", "output_log": output_messages}, 500
    except Exception as e:
        output_messages.append(f"❌ Error fetching active machines: {e}")
        return {"status": "error", "message": "Failed to fetch active machines", "output_log": output_messages}, 500

    # Fetch active gt_code numbers
    try:
        active_gtcodes = set()
        cursor = ufm_collect_data.find({"status": "active"}, {"gt_code": 1})
        for doc in cursor:
            active_gtcodes.update(doc.get("gt_code", []))
        output_messages.append(f"✅ Fetched {len(active_gtcodes)} active model numbers: {active_gtcodes}")
    except Exception as e:
        output_messages.append(f"❌ Error fetching active model numbers: {e}")
        return {"status": "error", "message": "Failed to fetch active model numbers", "output_log": output_messages}, 500

    # Create unique indexes
    try:
        lot_ufm_collect.create_index(
            [
                ("modelno", pymongo.ASCENDING),
                ("machine", pymongo.ASCENDING),
                ("lot_id", pymongo.ASCENDING),
                ("ufm_machine_model_lot_id", pymongo.ASCENDING)
            ],
            unique=True,
            name="ufm_lot_unique_idx"
        )
        alert_ufm_collect.create_index(
            [
                ("modelno", pymongo.ASCENDING),
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
        channel_data = list(channel_collect.find({"gt_code": {"$in": list(active_gtcodes)}}))
        channel_df = pd.DataFrame(channel_data)
        machine_data = list(machine_collect.find({}))
        machine_df = pd.DataFrame(machine_data)
        ub_machine_data = list(ub_machine_collect.find({"ubno": {"$in": list(active_machines)}}))
        ub_machine_df = pd.DataFrame(ub_machine_data)
        logging.info("✅ UFM Data loaded into DataFrames.")
        output_messages.append("✅ UFM Data loaded into DataFrames.")
    except Exception as e:
        error_message = f"❌ UFM Error loading data: {e}"
        logging.error(error_message)
        output_messages.append(error_message)
        return {"status": "error", "message": error_message, "output_log": output_messages}, 500
    
    # MODIFIED: Check for cimcorp_tbm_tub column before adding tbm_machine column
    if 'tbm' in merge_df.columns and 'tbm' in machine_df.columns and 'tbm' in machine_df.columns:
        try:
            merge_df["tbm_machine"] = None  # Initialize tbm_machine column
            for index, row in merge_df.iterrows():
                tbm_value = row.get("tbm")
                if tbm_value:
                    matching_channel = machine_df[machine_df["tbm"] == tbm_value]
                    if not matching_channel.empty:
                        merge_df.at[index, "tbm_machine"] = matching_channel["tbm"].iloc[0]  # NEW: Use machine column
            output_messages.append("✅ Added tbm_machine column to merge_df based on matching cimcorp_tbm_tub values.")
        except Exception as e:
            output_messages.append(f"⚠️ Warning: Failed to add tbm_machine column to merge_df: {e}. Continuing without tbm_machine.")
    else:
        output_messages.append("⚠️ Warning: 'tbm' column not found in merge_df or machine_df. Skipping tbm_machine column creation.")

    # MODIFIED: Check for tbm column before updating machine_df
    if 'tbm' in machine_df.columns and 'tbm' in merge_df.columns and 'tbm_machine' in merge_df.columns:
        try:
            for index, row in machine_df.iterrows():
                tbm_value = row.get("tbm")
                if tbm_value:
                    matching_merge = merge_df[merge_df["tbm"] == tbm_value]
                    if not matching_merge.empty and "tbm_machine" in matching_merge.columns:
                        machine_df.at[index, "tbm"] = matching_merge["tbm_machine"].iloc[0]
            output_messages.append("✅ Updated machine_df machine column with tbm_machine values from merge_df.")
        except Exception as e:
            output_messages.append(f"⚠️ Warning: Failed to update machine_df machine column: {e}. Continuing without updating channel_df machine.")
    else:
        output_messages.append("⚠️ Warning: 'cimcorp_tbm_tub' column not found in merge_df or machine_df. Skipping machine_df machine column update.")


    # Filter data
    try:
        gt_code_lists = ufm_collect_data.distinct("gt_code")
        if "gt_code" not in merge_df.columns:
            output_messages.append("'gt_code' column missing in merge_df. Skipping UFM processing.")
            return {"status": "success", "message": "'gt_code' column missing. Skipping UFM processing.", "output_log": output_messages}, 200
        # Initialize alert-tracking columns if they don't exist yet (first run)
        if "alert_id_ufm_ufm" not in merge_df.columns:
            merge_df["alert_id_ufm_ufm"] = None
        if "ufm_machine_model_lot_id" not in merge_df.columns:
            merge_df["ufm_machine_model_lot_id"] = None

        merge_df = merge_df[merge_df["gt_code"].isin(gt_code_lists)]
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
            return {"status": "success", "message": "No new data to process UFM.", "output_log": output_messages}, 200

    except KeyError as e:
        error_message = f"Failed to filter UFM: Error - DataFrame issue: Column missing {e}. Ensure 'alert_id_ufm_ufm' and 'ufm_machine_model_lot_id' are present in '{uniformity_collection_name}'."
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
        merge_df["date_of_ufm"] = pd.to_datetime(merge_df["date_of_ufm"], errors="coerce").dt.strftime('%Y-%m-%d %H:%M:%S').fillna("No Date")
        merge_df["date_of_tbm"] = merge_df["tbm_buildtime"]

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
        merge_df["shift"] = merge_df["date_of_ufm"].apply(get_shift)

        merge_df["ubno"] = merge_df["ubno"].astype(str)
        ub_machine_df["ubno"] = ub_machine_df["ubno"].astype(str)
        merge_df = merge_df[merge_df["ubno"].isin(active_machines)]
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
                        "gt_code": {"$in": list(active_gtcodes)},
                        "machine": {"$in": list(active_machines)}}},
            {"$addFields": {
                "lot_number": {
                    "$toInt": {"$arrayElemAt": [{"$split": ["$lot_id", "_"]}, -1]}
                },
                "gt_code": "$gt_code",
                "machine": "$machine"
            }},
            {"$sort": {"lot_number": -1}},
            {"$group": {
                "_id": {"gt_code": "$gt_code", "machine": "$machine"},
                "max_lot_number": {"$first": "$lot_number"},
                "last_lot_id": {"$first": "$lot_id"}
            }}
        ]))
        last_lot_dict = {
            (item["_id"]["gt_code"], item["_id"]["machine"]): {
                "max_lot_number": item["max_lot_number"],
                "last_lot_id": item["last_lot_id"]
            } for item in last_lots_agg
        }
        logging.info("ℹ️ Last UFM Lot numbers fetched: %s", last_lot_dict)
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
                    "gt_code": {"$in": list(active_gtcodes)},
                    "machine": {"$in": list(active_machines)}
                }
            },
            {
                "$sort": {"ufm_machine_model_lot_id": -1}
            },
            {
                "$group": {
                    "_id": {"gt_code": "$gt_code", "machine": "$machine"},
                    "last_ufm_lot_id": {"$first": "$ufm_machine_model_lot_id"},
                    "last_averages": {"$first": "$current_lot_averages"}
                }
            }
        ]))
        for item in last_averages:
            gt_code = item["_id"]["gt_code"]
            machine = item["_id"]["machine"]
            ufm_lot_id = item["last_ufm_lot_id"]
            averages = item["last_averages"]
            ufm_previous_machine_model_lot_ids[(gt_code, machine)] = ufm_lot_id
            if averages:
                ufm_machine_lot_avg_map[(gt_code, machine, ufm_lot_id)] = averages
                logging.info(f"Pre-populated ufm_machine_lot_avg_map with averages for {ufm_lot_id}")
            else:
                logging.warning(f"No averages found for {ufm_lot_id} in averages_ufm_collect")
        logging.info(f"Initialized ufm_previous_machine_model_lot_ids: {ufm_previous_machine_model_lot_ids}")
        output_messages.append(f"Initialized ufm_previous_machine_model_lot_ids with {len(ufm_previous_machine_model_lot_ids)} entries")
    except Exception as e:
        logging.error(f"Error pre-populating ufm_machine_lot_avg_map and ufm_previous_machine_model_lot_ids: {e}")
        output_messages.append(f"❌ Error pre-populating ufm_machine_lot_avg_map and ufm_previous_machine_model_lot_ids: {e}")

    ufm_params = [
        'rfpp_cw_kg','rfpp_ccw_kg','rfh1_cw_kg','rfh1_ccw_kg','rfh2_cw_kg','rfh2_ccw_kg','lfpp_cw_kg','lfpp_ccw_kg',
        'lsft_cw_kg','lsft_ccw_kg','lfh1_cw_kg','lfh1_ccw_kg','plysteer_kg','cony_kg','rfmxa_ccw_deg',
        'rfa1_cw_deg','rfa1_ccw_deg','rfa2_cw_deg','rfa2_ccw_deg','lfmxa_cw_deg','lfmxa_ccw_deg','lfa1_cw_deg',
        'lfa1_ccw_deg','botplane_g','topplane_g','couple_g','static_g','botplane_deg','topplane_deg','couple_deg',
        'static_deg','botbul_mm','botdep_mm','topbul_mm','topdep_mm','blro_mm','blroh1_mm','crro_mm','crroh1_mm',
        'tlro_mm','tlroh1_mm','botbul_deg','botdep_deg','topbul_deg','topdep_deg','blro_deg','blroh1_deg','crro_deg',
        'crroh1_deg','tlro_deg','tlroh1_deg'
        ]
    yield_params = [
        "rfv_yield", "rfh1_yield", "rfh2_yield", "lfv_yield", "lsft_yield", "lfh1_yield", "conicity_yield", "oe_yield",
        "rfmxa_yield", "rfa1_yield", "rfa2_yield", "lfxma_yield", "lfa1_yield", "central_radial_runout_yield", "central_radial_runout_first_harmonic_yield",
        "lateral_run_out_top_over_yield", "lateral_run_out_bottom_over_yield", "lateral_run_out_yield", "lateral_run_out_bottom_first_harmonic_yield", "lateral_run_out_top_first_harmonic_yield", 
        "lateral_run_out_harmonic_yield", "bulge_top_over_yield", "bulge_bottom_over_yield", "bulge_yield", "dent_top_over_yield", "dent_bottom_over_yield", "dent_yield", "geometry_oe_yield",
        "lower_yield", "upper_yield", "static_yield", "couple_yield", "balancing_oe_yield", "warehouse_yield", "visual_yield"
        ]
    rank_param_mappings = {
        "rfv_yield": ["tu_rfpp_cw_grade", "tu_rfpp_ccw_grade"],
        "rfh1_yield": ["tu_rfh1_cw_grade", "tu_rfh1_ccw_grade"], 
        "rfh2_yield": ["tu_rfh2_cw_grade", "tu_rfh2_ccw_grade"],
        "lfv_yield": ["tu_lfpp_cw_grade", "tu_lfpp_ccw_grade"],
        "lsft_yield": ["tu_lsft_cw_grade", "tu_lsft_ccw_grade"],
        "lfh1_yield": ["tu_lfh1_cw_grad", "tu_lfh1_ccw_grad"],
        "conicity_yield": ["tu_cony_grade"],
        "oe_yield": ["tu_class"],
        "rfmxa_yield": ["tu_rfmxa_ccw_grade"],
        "rfa1_yield": ["tu_rfa1_cw_grade", "tu_rfa1_ccw_grade"],
        "rfa2_yield": ["tu_rfa2_cw_grade", "tu_rfa2_ccw_grade"],
        "lfxma_yield": ["tu_lfmxa_cw_grade", "tu_lfmxa_ccw_grade"],
        "lfa1_yield": ["tu_lfa1_cw_grade", "tu_lfa1_ccw_grade"],
        "central_radial_runout_yield": ["tg_crro_grade"],
        "central_radial_runout_first_harmonic_yield": ["tg_crroh1_grade"],
        "lateral_run_out_top_over_yield": ["tg_tlro_grade"],
        "lateral_run_out_bottom_over_yield": ["tg_blro_grade"],
        "lateral_run_out_yield": ["tg_tlro_grade", "tg_blro_grade"],
        "lateral_run_out_bottom_first_harmonic_yield": ["tg_blroh1_grade"],
        "lateral_run_out_top_first_harmonic_yield": ["tg_tlroh1_grade"],
        "lateral_run_out_harmonic_yield": ["tg_tlroh1_grade", "tg_blroh1_grade"],
        "bulge_top_over_yield": ["tg_topbul_grade"],
        "bulge_bottom_over_yield": ["tg_botbul_grade"],
        "bulge_yield": ["tg_botbul_grade", "tg_topbul_grade"],
        "dent_top_over_yield": ["tg_topdep_grade"],
        "dent_bottom_over_yield": ["tg_botdep_grade"],
        "dent_yield": ["tg_topdep_grade", "tg_botdep_grade"],
        "geometry_oe_yield": ["tg_class"],
        "lower_yield": ["tb_botplane_grade"],
        "upper_yield": ["tb_topplane_grade"],
        "static_yield": ["tb_static_grade"],
        "couple_yield": ["tb_couple_grade"],
        "balancing_oe_yield": ["tb_class"]
        }

    for _, channel_row_data in channel_df.iterrows():
        try:
            gt_code_list = channel_row_data.get("gt_code", [])
            model_number = gt_code_list[0].strip() if isinstance(gt_code_list, list) and gt_code_list else ""
            if model_number not in active_gtcodes:
                output_messages.append(f"Skipping CURING model {model_number} as it is not active.")
                continue
            gt_codes = channel_row_data.get("gt_code", [])
            if not isinstance(gt_codes, list):
                gt_codes = [gt_codes]

            for gt_code in gt_codes:
                gt_code = str(gt_code).strip()
                model_data = merge_df[
                    (merge_df["gt_code"] == model_number) &
                    (merge_df["ubno"].isin(active_machines))
                ].copy()

                target_lot_size = int(channel_row_data.get("lot_size", 0))
                if target_lot_size == 0:
                    logging.info(f"Skipping UFM model {model_number} due to invalid lot size.")
                    output_messages.append(f"Skipping UFM model {model_number} due to invalid lot size.")
                    continue

                # # Validate specification columns
                # required_specs = [
                #     "spec_con", "spec_repp", "spec_rfh1", "spec_lfpp", "spec_static",
                #     "spec_bul", "spec_dep", "spec_lro", "spec_rro", "spec_dynamic",
                #     "class_spec_yield", "threshold_value", "threshold_value_bal"
                # ]
                # missing_specs = [spec for spec in required_specs if spec not in channel_row_data]
                # if missing_specs:
                #     logging.warning(f"Missing specification columns for model {model_number}: {missing_specs}. Using defaults.")
                #     output_messages.append(f"⚠️ Missing specification columns for model {model_number}: {missing_specs}")

                # spec_con = float(channel_row_data.get("spec_con", 0))
                sizecode = str(channel_row_data.get("sizecode", ""))
                spec_rfpp_cw_kg = float(channel_row_data.get("spec_rfpp_cw_kg", 0))
                spec_rfpp_ccw_kg = float(channel_row_data.get("spec_rfpp_ccw_kg", 0))
                spec_rfh1_cw_kg = float(channel_row_data.get("spec_rfh1_cw_kg", 0))
                spec_rfh1_ccw_kg = float(channel_row_data.get("spec_rfh1_ccw_kg", 0))
                spec_rfh2_cw_kg = float(channel_row_data.get("spec_rfh2_cw_kg", 0))
                spec_rfh2_ccw_kg = float(channel_row_data.get("spec_rfh2_ccw_kg", 0))
                spec_lfpp_cw_kg = float(channel_row_data.get("spec_lfpp_cw_kg", 0))
                spec_lfpp_ccw_kg = float(channel_row_data.get("spec_lfpp_ccw_kg", 0))
                spec_lsft_cw_kg = float(channel_row_data.get("spec_lsft_cw_kg", 0))
                spec_lsft_ccw_kg = float(channel_row_data.get("spec_lsft_ccw_kg", 0))
                spec_lfh1_cw_kg = float(channel_row_data.get("spec_lfh1_cw_kg", 0))
                spec_lfh1_ccw_kg = float(channel_row_data.get("spec_lfh1_ccw_kg", 0))
                spec_plysteer_kg = float(channel_row_data.get("spec_plysteer_kg", 0))
                spec_rfmxa_ccw_deg = float(channel_row_data.get("spec_rfmxa_ccw_deg", 0))
                spec_rfa1_cw_deg = float(channel_row_data.get("spec_rfa1_cw_deg", 0))
                spec_rfa1_ccw_deg = float(channel_row_data.get("spec_rfa1_ccw_deg", 0))
                spec_rfa2_cw_deg = float(channel_row_data.get("spec_rfa2_cw_deg", 0))
                spec_rfa2_ccw_deg = float(channel_row_data.get("spec_rfa2_ccw_deg", 0))
                spec_lfmxa_cw_deg = float(channel_row_data.get("spec_lfmxa_cw_deg", 0))
                spec_lfmxa_ccw_deg = float(channel_row_data.get("spec_lfmxa_ccw_deg", 0))
                spec_lfa1_cw_deg = float(channel_row_data.get("spec_lfa1_cw_deg", 0))
                spec_lfa1_ccw_deg = float(channel_row_data.get("spec_lfa1_ccw_deg", 0))
                spec_cony_kg = float(channel_row_data.get("spec_cony_kg", 0))
                spec_static_g = float(channel_row_data.get("spec_static_g", 0))
                spec_static_deg = float(channel_row_data.get("spec_static_deg", 0))
                spec_couple_g = float(channel_row_data.get("spec_couple_g", 0))
                spec_couple_deg = float(channel_row_data.get("spec_couple_deg", 0))
                spec_bul_botbul_mm = float(channel_row_data.get("spec_bul_botbul_mm", 0))
                spec_bul_topbul_mm = float(channel_row_data.get("spec_bul_topbul_mm", 0))
                spec_bul_botbul_deg = float(channel_row_data.get("spec_bul_botbul_deg", 0))
                spec_bul_topdep_deg = float(channel_row_data.get("spec_bul_topdep_deg", 0))
                spec_dep_botdep_mm = float(channel_row_data.get("spec_dep_botdep_mm", 0))
                spec_dep_topdep_mm = float(channel_row_data.get("spec_dep_topdep_mm", 0))
                spec_dep_botdep_deg = float(channel_row_data.get("spec_dep_botdep_deg", 0))
                spec_dep_topbul_deg = float(channel_row_data.get("spec_dep_topbul_deg", 0))
                spec_rro_crro_mm = float(channel_row_data.get("spec_rro_crro_mm", 0))
                spec_rro_crroh1_mm = float(channel_row_data.get("spec_rro_crroh1_mm", 0))
                spec_lro_blro_mm = float(channel_row_data.get("spec_lro_blro_mm", 0))
                spec_lro_blroh1_mm = float(channel_row_data.get("spec_lro_blroh1_mm", 0))
                spec_lro_tlro_mm = float(channel_row_data.get("spec_lro_tlro_mm", 0))
                spec_lro_tlroh1_mm = float(channel_row_data.get("spec_lro_tlroh1_mm", 0))
                spec_rro_crro_deg = float(channel_row_data.get("spec_rro_crro_deg", 0))
                spec_rro_crroh1_deg = float(channel_row_data.get("spec_rro_crroh1_deg", 0))
                spec_lro_blro_deg = float(channel_row_data.get("spec_lro_blro_deg", 0))
                spec_lro_blroh1_deg = float(channel_row_data.get("spec_lro_blroh1_deg", 0))
                spec_lro_tlro_deg = float(channel_row_data.get("spec_lro_tlro_deg", 0))
                spec_lro_tlroh1_deg = float(channel_row_data.get("spec_lro_tlroh1_deg", 0))
                spec_dynamic_botplane_g = float(channel_row_data.get("spec_dynamic_botplane_g", 0))
                spec_dynamic_topplane_g = float(channel_row_data.get("spec_dynamic_topplane_g", 0))
                spec_dynamic_botplane_deg = float(channel_row_data.get("spec_dynamic_botplane_deg", 0))
                spec_dynamic_topplane_deg = float(channel_row_data.get("spec_dynamic_topplane_deg", 0))

                rfv_yield_spec_yield = float(channel_row_data.get("rfv_yield_spec_yield", 0))
                rfh1_yield_spec_yield = float(channel_row_data.get("rfh1_yield_spec_yield", 0))
                rfh2_yield_spec_yield = float(channel_row_data.get("rfh2_yield_spec_yield", 0))
                lfv_yield_spec_yield = float(channel_row_data.get("lfv_yield_spec_yield", 0))
                lsft_yield_spec_yield = float(channel_row_data.get("lsft_yield_spec_yield", 0))
                lfh1_yield_spec_yield = float(channel_row_data.get("lfh1_yield_spec_yield", 0))
                conicity_yield_spec_yield = float(channel_row_data.get("conicity_yield_spec_yield", 0))
                oe_yield_spec_yield = float(channel_row_data.get("oe_yield_spec_yield", 0))
                rfmxa_yield_spec_yield = float(channel_row_data.get("rfmxa_yield_spec_yield", 0))
                rfa1_yield_spec_yield = float(channel_row_data.get("rfa1_yield_spec_yield", 0))
                rfa2_yield_spec_yield = float(channel_row_data.get("rfa2_yield_spec_yield", 0))
                lfxma_yield_spec_yield = float(channel_row_data.get("lfxma_yield_spec_yield", 0))
                lfa1_yield_spec_yield = float(channel_row_data.get("lfa1_yield_spec_yield", 0))
                central_radial_runout_yield_spec_yield = float(channel_row_data.get("central_radial_runout_yield_spec_yield", 0))
                central_radial_runout_first_harmonic_yield_spec_yield = float(channel_row_data.get("central_radial_runout_first_harmonic_yield_spec_yield", 0))
                lateral_run_out_top_over_yield_spec_yield = float(channel_row_data.get("lateral_run_out_top_over_yield_spec_yield", 0))
                lateral_run_out_bottom_over_yield_spec_yield = float(channel_row_data.get("lateral_run_out_bottom_over_yield_spec_yield", 0))
                lateral_run_out_yield_spec_yield = float(channel_row_data.get("lateral_run_out_yield_spec_yield", 0))
                lateral_run_out_bottom_first_harmonic_yield_spec_yield = float(channel_row_data.get("lateral_run_out_bottom_first_harmonic_yield_spec_yield", 0))
                lateral_run_out_top_first_harmonic_yield_spec_yield = float(channel_row_data.get("lateral_run_out_top_first_harmonic_yield_spec_yield", 0))
                lateral_run_out_harmonic_yield_spec_yield = float(channel_row_data.get("lateral_run_out_harmonic_yield_spec_yield", 0))
                bulge_top_over_yield_spec_yield = float(channel_row_data.get("bulge_top_over_yield_spec_yield", 0))
                bulge_bottom_over_yield_spec_yield = float(channel_row_data.get("bulge_bottom_over_yield_spec_yield", 0))
                bulge_yield_spec_yield = float(channel_row_data.get("bulge_yield_spec_yield", 0))
                dent_top_over_yield_spec_yield = float(channel_row_data.get("dent_top_over_yield_spec_yield", 0))
                dent_bottom_over_yield_spec_yield = float(channel_row_data.get("dent_bottom_over_yield_spec_yield", 0))
                dent_yield_spec_yield = float(channel_row_data.get("dent_yield_spec_yield", 0))
                geometry_oe_yield_spec_yield = float(channel_row_data.get("geometry_oe_yield_spec_yield", 0))
                lower_yield_spec_yield = float(channel_row_data.get("lower_yield_spec_yield", 0))
                upper_yield_spec_yield = float(channel_row_data.get("upper_yield_spec_yield", 0))
                static_yield_spec_yield = float(channel_row_data.get("static_yield_spec_yield", 0))
                couple_yield_spec_yield = float(channel_row_data.get("couple_yield_spec_yield", 0))
                balancing_oe_yield_spec_yield = float(channel_row_data.get("balancing_oe_yield_spec_yield", 0))
                warehouse_yield_spec_yield = float(channel_row_data.get("warehouse_yield_spec_yield", 0))
                visual_yield_spec_yield = float(channel_row_data.get("visual_yield_spec_yield", 0))

                class_spec_yield = float(channel_row_data.get("class_spec_yield", 0))
                balancing_spec_yield = float(channel_row_data.get("balancing_spec_yield", 0))
                geometry_spec_yield = float(channel_row_data.get("geometry_spec_yield", 0))

                threshold_value_rfpp_cw_kg = float(channel_row_data.get("threshold_value_rfpp_cw_kg", 5))
                threshold_value_rfpp_ccw_kg = float(channel_row_data.get("threthreshold_value_rfpp_ccw_kgshold_value_bal", 5))
                threshold_value_rfh1_cw_kg = float(channel_row_data.get("threshold_value_rfh1_cw_kg", 5))
                threshold_value_rfh1_ccw_kg = float(channel_row_data.get("threshold_value_rfh1_ccw_kg", 5))
                threshold_value_rfh2_cw_kg = float(channel_row_data.get("threshold_value_rfh2_cw_kg", 5))
                threshold_value_rfh2_ccw_kg = float(channel_row_data.get("threshold_value_rfh2_ccw_kg", 5))
                threshold_value_lfpp_cw_kg = float(channel_row_data.get("threshold_value_lfpp_cw_kg", 5))
                threshold_value_lfpp_ccw_kg = float(channel_row_data.get("threshold_value_lfpp_ccw_kg", 5))
                threshold_value_lsft_cw_kg = float(channel_row_data.get("threshold_value_lsft_cw_kg", 5))
                threshold_value_lsft_ccw_kg = float(channel_row_data.get("threshold_value_lsft_ccw_kg", 5))
                threshold_value_lfh1_cw_kg = float(channel_row_data.get("threshold_value_lfh1_cw_kg", 5))
                threshold_value_lfh1_ccw_kg = float(channel_row_data.get("threshold_value_lfh1_ccw_kg", 5))
                threshold_value_plysteer_kg = float(channel_row_data.get("threshold_value_plysteer_kg", 5))
                threshold_value_rfmxa_cw_deg = float(channel_row_data.get("threshold_value_rfmxa_cw_deg", 5))
                threshold_value_rfmxa_ccw_deg = float(channel_row_data.get("threshold_value_rfmxa_ccw_deg", 5))
                threshold_value_rfa1_cw_deg = float(channel_row_data.get("threshold_value_rfa1_cw_deg", 5))
                threshold_value_rfa1_ccw_deg = float(channel_row_data.get("threshold_value_rfa1_ccw_deg", 5))
                threshold_value_rfa2_cw_deg = float(channel_row_data.get("threshold_value_rfa2_cw_deg", 5))
                threshold_value_rfa2_ccw_deg = float(channel_row_data.get("threshold_value_rfa2_ccw_deg", 5))
                threshold_value_lfmxa_cw_deg = float(channel_row_data.get("threshold_value_lfmxa_cw_deg", 5))
                threshold_value_lfmxa_ccw_deg = float(channel_row_data.get("threshold_value_lfmxa_ccw_deg", 5))
                threshold_value_lfa1_cw_deg = float(channel_row_data.get("threshold_value_lfa1_cw_deg", 5))
                threshold_value_lfa1_ccw_deg = float(channel_row_data.get("threshold_value_lfa1_ccw_deg", 5))
                threshold_value_cony_kg = float(channel_row_data.get("threshold_value_cony_kg", 5))
                threshold_value_bal_static_g = float(channel_row_data.get("threshold_value_bal_static_g", 5))
                threshold_value_bal_static_deg = float(channel_row_data.get("threshold_value_bal_static_deg", 5))
                threshold_value_bal_couple_g = float(channel_row_data.get("threshold_value_bal_couple_g", 5))
                threshold_value_bal_couple_deg = float(channel_row_data.get("threshold_value_bal_couple_deg", 5))
                threshold_value_bal_dynamic_botplane_g = float(channel_row_data.get("threshold_value_bal_dynamic_botplane_g", 5))
                threshold_value_bal_dynamic_topplane_g = float(channel_row_data.get("threshold_value_bal_dynamic_topplane_g", 5))
                threshold_value_bal_dynamic_botplane_deg = float(channel_row_data.get("threshold_value_bal_dynamic_botplane_deg", 5))
                threshold_value_bal_dynamic_topplane_deg = float(channel_row_data.get("threshold_value_bal_dynamic_topplane_deg", 5))
                threshold_value_geo_bul_botbul_mm = float(channel_row_data.get("threshold_value_geo_bul_botbul_mm", 5))
                threshold_value_geo_bul_topbul_mm = float(channel_row_data.get("threshold_value_geo_bul_topbul_mm", 5))
                threshold_value_geo_bul_botbul_deg = float(channel_row_data.get("threshold_value_geo_bul_botbul_deg", 5))
                threshold_value_geo_bul_topdep_deg = float(channel_row_data.get("threshold_value_geo_bul_topdep_deg", 5))
                threshold_value_geo_dep_botdep_mm = float(channel_row_data.get("threshold_value_geo_dep_botdep_mm", 5))
                threshold_value_geo_dep_topdep_mm = float(channel_row_data.get("threshold_value_geo_dep_topdep_mm", 5))
                threshold_value_geo_dep_botdep_deg = float(channel_row_data.get("threshold_value_geo_dep_botdep_deg", 5))
                threshold_value_geo_dep_topbul_deg = float(channel_row_data.get("threshold_value_geo_dep_topbul_deg", 5))
                threshold_value_geo_rro_crro_mm = float(channel_row_data.get("threshold_value_geo_rro_crro_mm", 5))
                threshold_value_geo_rro_crroh1_mm = float(channel_row_data.get("threshold_value_geo_rro_crroh1_mm", 5))
                threshold_value_geo_lro_blro_mm = float(channel_row_data.get("threshold_value_geo_lro_blro_mm", 5))
                threshold_value_geo_lro_blroh1_mm = float(channel_row_data.get("threshold_value_geo_lro_blroh1_mm", 5))
                threshold_value_geo_lro_tlro_mm = float(channel_row_data.get("threshold_value_geo_lro_tlro_mm", 5))
                threshold_value_geo_lro_tlroh1_mm = float(channel_row_data.get("threshold_value_geo_lro_tlroh1_mm", 5))
                threshold_value_geo_rro_crro_deg = float(channel_row_data.get("threshold_value_geo_rro_crro_deg", 5))
                threshold_value_geo_rro_crroh1_deg = float(channel_row_data.get("threshold_value_geo_rro_crroh1_deg", 5))
                threshold_value_geo_lro_blro_deg = float(channel_row_data.get("threshold_value_geo_lro_blro_deg", 5))
                threshold_value_geo_lro_blroh1_deg = float(channel_row_data.get("threshold_value_geo_lro_blroh1_deg", 5))
                threshold_value_geo_lro_tlro_deg = float(channel_row_data.get("threshold_value_geo_lro_tlro_deg", 5))
                threshold_value_geo_lro_tlroh1_deg = float(channel_row_data.get("threshold_value_geo_lro_tlroh1_deg", 5))

                threshold_value_yield_rfv_yield = float(channel_row_data.get("threshold_value_yield_rfv_yield", 5))
                threshold_value_yield_rfh1_yield = float(channel_row_data.get("threshold_value_yield_rfh1_yield", 5))
                threshold_value_yield_rfh2_yield = float(channel_row_data.get("threshold_value_yield_rfh2_yield", 5))
                threshold_value_yield_lfv_yield = float(channel_row_data.get("threshold_value_yield_lfv_yield", 5))
                threshold_value_yield_lsft_yield = float(channel_row_data.get("threshold_value_yield_lsft_yield", 5))
                threshold_value_yield_lfh1_yield = float(channel_row_data.get("threshold_value_yield_lfh1_yield", 5))
                threshold_value_yield_conicity_yield = float(channel_row_data.get("threshold_value_yield_conicity_yield", 5))
                threshold_value_yield_oe_yield = float(channel_row_data.get("threshold_value_yield_oe_yield", 5))
                threshold_value_yield_rfmxa_yield = float(channel_row_data.get("threshold_value_yield_rfmxa_yield", 5))
                threshold_value_yield_rfa1_yield = float(channel_row_data.get("threshold_value_yield_rfa1_yield", 5))
                threshold_value_yield_rfa2_yield = float(channel_row_data.get("threshold_value_yield_rfa2_yield", 5))
                threshold_value_yield_lfxma_yield = float(channel_row_data.get("threshold_value_yield_lfxma_yield", 5))
                threshold_value_yield_lfa1_yield = float(channel_row_data.get("threshold_value_yield_lfa1_yield", 5))
                threshold_value_yield_central_radial_runout_yield = float(channel_row_data.get("threshold_value_yield_central_radial_runout_yield", 5))
                threshold_value_yield_central_radial_runout_first_harmonic_yield = float(channel_row_data.get("threshold_value_yield_central_radial_runout_first_harmonic_yield", 5))
                threshold_value_yield_lateral_run_out_top_over_yield = float(channel_row_data.get("threshold_value_yield_lateral_run_out_top_over_yield", 5))
                threshold_value_yield_lateral_run_out_bottom_over_yield = float(channel_row_data.get("threshold_value_yield_lateral_run_out_bottom_over_yield", 5))
                threshold_value_yield_lateral_run_out_yield = float(channel_row_data.get("threshold_value_yield_lateral_run_out_yield", 5))
                threshold_value_yield_lateral_run_out_bottom_first_harmonic_yield = float(channel_row_data.get("threshold_value_yield_lateral_run_out_bottom_first_harmonic_yield", 5))
                threshold_value_yield_lateral_run_out_top_first_harmonic_yield = float(channel_row_data.get("threshold_value_yield_lateral_run_out_top_first_harmonic_yield", 5))
                threshold_value_yield_lateral_run_out_harmonic_yield = float(channel_row_data.get("threshold_value_yield_lateral_run_out_harmonic_yield", 5))
                threshold_value_yield_bulge_top_over_yield = float(channel_row_data.get("threshold_value_yield_bulge_top_over_yield", 5))
                threshold_value_yield_bulge_bottom_over_yield = float(channel_row_data.get("threshold_value_yield_bulge_bottom_over_yield", 5))
                threshold_value_yield_bulge_yield = float(channel_row_data.get("threshold_value_yield_bulge_yield", 5))
                threshold_value_yield_dent_top_over_yield = float(channel_row_data.get("threshold_value_yield_dent_top_over_yield", 5))
                threshold_value_yield_dent_bottom_over_yield = float(channel_row_data.get("threshold_value_yield_dent_bottom_over_yield", 5))
                threshold_value_yield_dent_yield = float(channel_row_data.get("threshold_value_yield_dent_yield", 5))
                threshold_value_yield_geometry_oe_yield = float(channel_row_data.get("threshold_value_yield_geometry_oe_yield", 5))
                threshold_value_yield_lower_yield = float(channel_row_data.get("threshold_value_yield_lower_yield", 5))
                threshold_value_yield_upper_yield = float(channel_row_data.get("threshold_value_yield_upper_yield", 5))
                threshold_value_yield_static_yield = float(channel_row_data.get("threshold_value_yield_static_yield", 5))
                threshold_value_yield_couple_yield = float(channel_row_data.get("threshold_value_yield_couple_yield", 5))
                threshold_value_yield_balancing_oe_yield = float(channel_row_data.get("threshold_value_yield_balancing_oe_yield", 5))
                threshold_value_yield_warehouse_yield = float(channel_row_data.get("threshold_value_yield_warehouse_yield", 5))
                threshold_value_yield_visual_yield = float(channel_row_data.get("threshold_value_yield_visual_yield", 5))
                threshold_value_yield = float(channel_row_data.get("threshold_value_yield", 5))

                class_threshold_value_yield_class_yield = float(channel_row_data.get("class_threshold_value_yield_class_yield", 5))
                threshold_value_yield_balancing_yield = float(channel_row_data.get("threshold_value_yield_balancing_yield", 5))
                threshold_value_yield_geometry_yield = float(channel_row_data.get("threshold_value_yield_geometry_yield", 5))

                if model_data.empty:
                    # logging.info(f"Skipping UFM model {model_number} gt_code {gt_code}: No data available.")
                    output_messages.append(f"Skipping UFM model {model_number} gt_code {gt_code}: No data available.")
                    continue

                total_barcodes_model_processable = len(model_data)
                if total_barcodes_model_processable < target_lot_size:
                    logging.info(f"Skipping UFM model {model_number} gt_code {gt_code}: Insufficient barcodes ({total_barcodes_model_processable} < {target_lot_size}).")
                    output_messages.append(f"Skipping UFM model {model_number} gt_code {gt_code}: Insufficient barcodes ({total_barcodes_model_processable} < {target_lot_size}).")
                    continue

                model_data.sort_values("date_of_ufm", ascending=True, inplace=True)

                # Virgin/Final yield split: store raw barcode rows, then continue main
                # processing on virgin data (first occurrence per barcode).
                virgin_data, final_data, _vf_msgs = split_and_store_virgin_final(model_data, "date_of_ufm", "UFM")
                output_messages.extend(_vf_msgs)
                model_data = virgin_data.copy()

                for ufm_machine_id, ufm_machine_data in model_data.groupby("ubno"):
                    try:
                        machine_no = str(ufm_machine_id)
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
                        if total_ufm_machine_barcodes < target_lot_size:
                            logging.info(f"Skipping UFM machine {machine_no} for model {model_number}: Insufficient barcodes ({total_ufm_machine_barcodes} < {target_lot_size}).")
                            output_messages.append(f"Skipping UFM machine {machine_no} for model {model_number}: Insufficient barcodes ({total_ufm_machine_barcodes} < {target_lot_size}).")
                            continue

                        machine_barcode_counter = 0
                        machine_model_key = (model_number, machine_no)

                        while machine_barcode_counter < total_ufm_machine_barcodes:
                            if (total_ufm_machine_barcodes - machine_barcode_counter) < target_lot_size:
                                # logging.info(f"Insufficient remaining barcodes on UFM machine {machine_no} for model {model_number}: {total_ufm_machine_barcodes - machine_barcode_counter}")
                                break
                            if (model_number, machine_no) not in last_lot_dict:
                                last_lot_dict[(model_number, machine_no)] = {
                                    "max_lot_number": 0,
                                    "last_lot_id": None
                                }

                            current_last_lot_number = last_lot_dict[(model_number, machine_no)]["max_lot_number"] + 1
                            current_model_lot_id = f"UFM_{machine_no}_{model_number}_{current_last_lot_number}"
                            current_barcode_counter = machine_barcode_counter + target_lot_size
                            current_ufm_machine_lot_id = f"{machine_no}_{model_number}_UFM_{machine_no}_{model_number}_{current_last_lot_number}_{current_barcode_counter}"

                            if not re.match(r"^UFM_[^_]+_[^_]+_\d+$", current_model_lot_id):
                                logging.error(f"Invalid lot_id format: {current_model_lot_id}")
                                continue
                            if not re.match(r"^[^_]+_[^_]+_UFM_[^_]+_[^_]+_\d+_\d+$", current_ufm_machine_lot_id):
                                logging.error(f"Invalid ufm_machine_model_lot_id format: {current_ufm_machine_lot_id}")
                                continue

                            sub_batch_start_index = machine_barcode_counter
                            sub_batch_end_index = machine_barcode_counter + target_lot_size
                            machine_batch_data = ufm_machine_data.iloc[sub_batch_start_index:sub_batch_end_index].copy()
                            machine_batch_indices = machine_batch_data.index

                            if machine_batch_data.empty:
                                logging.info(f"Empty batch data for machine {machine_no}, model {model_number}, lot {current_model_lot_id}. Skipping.")
                                continue

                            ufm_previous_machine_model_lot_id = ufm_previous_machine_model_lot_ids.get(machine_model_key)
                            # logging.info(f"Initial ufm_previous_machine_model_lot_id: {ufm_previous_machine_model_lot_id} for model {model_number}, machine {machine_no}")
                            if ufm_previous_machine_model_lot_id is None and current_last_lot_number > 1:
                                prev_lot_number = current_last_lot_number - 1
                                prev_lot_id = f"UFM_{machine_no}_{model_number}_{prev_lot_number}"
                                logging.info(f"Fetching previous lot for lot_id: {prev_lot_id}")
                                try:
                                    prev_lot = averages_ufm_collect.find_one(
                                        {
                                            "gt_code": model_number,
                                            "machine": machine_no,
                                            "lot_id": prev_lot_id
                                        },
                                        sort=[("ufm_machine_model_lot_id", -1)]
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

                            model_level_previous_lot_id = f"UFM_{machine_no}_{model_number}_{current_last_lot_number - 1}" if current_last_lot_number > 1 else None
                            merge_df.loc[machine_batch_indices, "lot_id"] = current_model_lot_id
                            merge_df.loc[machine_batch_indices, "previous_lot_id"] = model_level_previous_lot_id
                            merge_df.loc[machine_batch_indices, "ufm_machine_model_lot_id"] = current_ufm_machine_lot_id
                            merge_df.loc[machine_batch_indices, "ufm_previous_machine_model_lot_id"] = ufm_previous_machine_model_lot_id

                            prev_avg_key = (model_number, machine_no, ufm_previous_machine_model_lot_id)
                            prev_lot_avg = ufm_machine_lot_avg_map.get(prev_avg_key, None)
                            if prev_lot_avg is None and ufm_previous_machine_model_lot_id is not None:
                                try:
                                    prev_lot_doc = averages_ufm_collect.find_one(
                                        {
                                            "gt_code": model_number,
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
                                # logging.info(f"First lot for model {model_number}, machine {machine_no}. Setting previous_lot_averages to empty.")
                            # logging.info(f"prev_key: {prev_avg_key}, prev_lot_avg: {prev_lot_avg}")

                            last_lot_dict[machine_model_key] = {
                                "max_lot_number": current_last_lot_number,
                                "last_ufm_machine_model_lot_id": current_ufm_machine_lot_id
                            }

                            lot_doc_data = {
                                "_id": f"lot_{current_ufm_machine_lot_id}",
                                "gt_code": model_number,
                                "lot_id": current_model_lot_id,
                                "previous_lot_id": model_level_previous_lot_id,
                                "machine": machine_no,
                                "ufm_machine_model_lot_id": current_ufm_machine_lot_id,
                                "ufm_previous_machine_model_lot_id": ufm_previous_machine_model_lot_id,
                                "target_machine_lot_size": target_lot_size,
                                "processed_machine_barcodes": target_lot_size,
                                "timestamp_lot_generated": some_function(),
                                "first_barcode": machine_batch_data['barcode'].iloc[0] if not machine_batch_data.empty and 'barcode' in machine_batch_data else None,
                                "last_barcode": machine_batch_data['barcode'].iloc[-1] if not machine_batch_data.empty and 'barcode' in machine_batch_data else None,
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
                                    try:
                                        numeric_series = pd.to_numeric(machine_batch_data[param], errors='coerce').dropna()
                                        lot_averages[param] = float(numeric_series.mean()) if not numeric_series.empty else None
                                    except Exception as e:
                                        logging.warning(f"Error calculating mean for {param}: {e}")
                                        lot_averages[param] = None
                                else:
                                    lot_averages[param] = None

                            lot_averages.update({
                                "rfv_yield": rfv_yield(machine_batch_data), 
                                "rfh1_yield": rfh1_yield(machine_batch_data),
                                "rfh2_yield": rfh2_yield(machine_batch_data),
                                "lfv_yield": lfv_yield(machine_batch_data),
                                "lsft_yield": lsft_yield(machine_batch_data),
                                "lfh1_yield": lfh1_yield(machine_batch_data),
                                "conicity_yield": conicity_yield(machine_batch_data),
                                "oe_yield": oe_yield(machine_batch_data),
                                "rfmxa_yield": rfmxa_yield(machine_batch_data),
                                "rfa1_yield": rfa1_yield(machine_batch_data),
                                "rfa2_yield": rfa2_yield(machine_batch_data),
                                "lfxma_yield": lfxma_yield(machine_batch_data),
                                "lfa1_yield": lfa1_yield(machine_batch_data),
                                "central_radial_runout_yield": central_radial_runout_yield(machine_batch_data),
                                "central_radial_runout_first_harmonic_yield": central_radial_runout_first_harmonic_yield(machine_batch_data),
                                "lateral_run_out_top_over_yield": lateral_run_out_top_over_yield(machine_batch_data),
                                "lateral_run_out_bottom_over_yield": lateral_run_out_bottom_over_yield(machine_batch_data),
                                "lateral_run_out_yield": lateral_run_out_yield(machine_batch_data),
                                "lateral_run_out_bottom_first_harmonic_yield": lateral_run_out_bottom_first_harmonic_yield(machine_batch_data),
                                "lateral_run_out_top_first_harmonic_yield": lateral_run_out_top_first_harmonic_yield(machine_batch_data),
                                "lateral_run_out_harmonic_yield": lateral_run_out_harmonic_yield(machine_batch_data),
                                "bulge_top_over_yield": bulge_top_over_yield(machine_batch_data),
                                "bulge_bottom_over_yield": bulge_bottom_over_yield(machine_batch_data),
                                "bulge_yield": bulge_yield(machine_batch_data),
                                "dent_top_over_yield": dent_top_over_yield(machine_batch_data),
                                "dent_bottom_over_yield": dent_bottom_over_yield(machine_batch_data),
                                "dent_yield": dent_yield(machine_batch_data),
                                "geometry_oe_yield": geometry_oe_yield(machine_batch_data),
                                "lower_yield": lower_yield(machine_batch_data),
                                "upper_yield": upper_yield(machine_batch_data),
                                "static_yield": static_yield(machine_batch_data),
                                "couple_yield": couple_yield(machine_batch_data),
                                "balancing_oe_yield": balancing_oe_yield(machine_batch_data),
                                "warehouse_yield": warehouse_yield(machine_batch_data),
                                "visual_yield": visual_yield(machine_batch_data)})

                            parameter_alerts = []
                            machine_lot_overall_shift_occurred = False
                            yield_spec_mapping = {
                            "cony_kg": spec_cony_kg,
                            "rfpp_cw_kg": spec_rfpp_cw_kg,
                            "rfpp_ccw_kg": spec_rfpp_ccw_kg,
                            "rfh1_cw_kg": spec_rfh1_cw_kg,
                            "rfh1_ccw_kg": spec_rfh1_ccw_kg,
                            "lfpp_cw_kg": spec_lfpp_cw_kg,
                            "lfpp_ccw_kg": spec_lfpp_ccw_kg,
                            'rfh2_cw_kg': spec_rfh2_cw_kg,
                            'rfh2_ccw_kg': spec_rfh2_ccw_kg,
                            'lsft_cw_kg': spec_lsft_cw_kg,
                            'lsft_ccw_kg': spec_lsft_ccw_kg,
                            'lfh1_cw_kg': spec_lfh1_cw_kg,
                            'lfh1_ccw_kg': spec_lfh1_ccw_kg,
                            'plysteer_kg': spec_plysteer_kg,
                            'rfmxa_ccw_deg':spec_rfmxa_ccw_deg,
                            'rfa1_cw_deg': spec_rfa1_cw_deg,
                            'rfa1_ccw_deg': spec_rfa1_ccw_deg,
                            'rfa2_cw_deg': spec_rfa2_cw_deg,
                            'rfa2_ccw_deg': spec_rfa2_ccw_deg,
                            'lfmxa_cw_deg': spec_lfmxa_cw_deg,
                            'lfmxa_ccw_deg': spec_lfmxa_ccw_deg,
                            'lfa1_cw_deg': spec_lfa1_cw_deg,
                            'lfa1_ccw_deg': spec_lfa1_ccw_deg,
                            'couple_g': spec_couple_g,
                            'botplane_deg': spec_dynamic_botplane_deg,
                            'topplane_deg': spec_dynamic_topplane_deg,
                            'couple_deg': spec_couple_deg,
                            'static_deg': spec_static_deg,
                            'botbul_mm': spec_bul_botbul_mm,
                            'botdep_mm': spec_dep_botdep_mm,
                            'topbul_mm': spec_bul_topbul_mm,
                            'topdep_mm': spec_dep_topdep_mm,
                            'blro_mm': spec_lro_blro_mm,
                            'blroh1_mm': spec_lro_blroh1_mm,
                            'crro_mm': spec_rro_crro_mm,
                            'crroh1_mm': spec_rro_crroh1_mm,
                            'tlro_mm': spec_lro_tlro_mm,
                            'tlroh1_mm': spec_lro_tlroh1_mm,
                            'botbul_deg': spec_bul_botbul_deg,
                            'botdep_deg': spec_dep_botdep_deg,
                            'topbul_deg': spec_dep_topbul_deg,
                            'topdep_deg': spec_bul_topdep_deg,
                            'blro_deg': spec_lro_blro_deg,
                            'blroh1_deg': spec_lro_blroh1_deg,
                            'crro_deg': spec_rro_crro_deg,
                            'crroh1_deg': spec_rro_crroh1_deg,
                            'tlro_deg': spec_lro_tlro_deg,
                            'tlroh1_deg': spec_lro_tlroh1_deg,
                            'botplane_g': spec_dynamic_botplane_g,
                            'topplane_g': spec_dynamic_topplane_g,
                            "static_g": spec_static_g,

                            "rfv_yield": rfv_yield_spec_yield,
                            "rfh1_yield": rfh1_yield_spec_yield,
                            "rfh2_yield": rfh2_yield_spec_yield,
                            "lfv_yield": lfv_yield_spec_yield,
                            "lsft_yield": lsft_yield_spec_yield,
                            "lfh1_yield": lfh1_yield_spec_yield,
                            "conicity_yield": conicity_yield_spec_yield,
                            "oe_yield": oe_yield_spec_yield,
                            "rfmxa_yield": rfmxa_yield_spec_yield,
                            "rfa1_yield": rfa1_yield_spec_yield,
                            "rfa2_yield": rfa2_yield_spec_yield,
                            "lfxma_yield": lfxma_yield_spec_yield,
                            "lfa1_yield": lfa1_yield_spec_yield,
                            "central_radial_runout_yield": central_radial_runout_yield_spec_yield,
                            "central_radial_runout_first_harmonic_yield": central_radial_runout_first_harmonic_yield_spec_yield,
                            "lateral_run_out_top_over_yield": lateral_run_out_top_over_yield_spec_yield,
                            "lateral_run_out_bottom_over_yield": lateral_run_out_bottom_over_yield_spec_yield,
                            "lateral_run_out_yield": lateral_run_out_yield_spec_yield,
                            "lateral_run_out_bottom_first_harmonic_yield": lateral_run_out_bottom_first_harmonic_yield_spec_yield,
                            "lateral_run_out_top_first_harmonic_yield": lateral_run_out_top_first_harmonic_yield_spec_yield,
                            "lateral_run_out_harmonic_yield": lateral_run_out_harmonic_yield_spec_yield,
                            "bulge_top_over_yield": bulge_top_over_yield_spec_yield,
                            "bulge_bottom_over_yield": bulge_bottom_over_yield_spec_yield,
                            "bulge_yield": bulge_yield_spec_yield,
                            "dent_top_over_yield": dent_top_over_yield_spec_yield,
                            "dent_bottom_over_yield": dent_bottom_over_yield_spec_yield,
                            "dent__yield": dent_yield_spec_yield,
                            "geometry_oe_yield": geometry_oe_yield_spec_yield,
                            "lower_yield": lower_yield_spec_yield,
                            "upper_yield": upper_yield_spec_yield,
                            "static_yield": static_yield_spec_yield,
                            "couple_yield": couple_yield_spec_yield,
                            "balancing_oe_yield": balancing_oe_yield_spec_yield,
                            "warehouse_yield": warehouse_yield_spec_yield,
                            "visual_yield": visual_yield_spec_yield
                            }
                            # Log warnings for parameters without specs
                            for param in ufm_params:
                                if param not in yield_spec_mapping and param not in yield_params:
                                    logging.warning(f"No specification defined for parameter {param}")

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
                                    spec_val = yield_spec_mapping.get(param)

                                    if param == "cony_kg" and spec_cony_kg:
                                        shift_occurred_thershold = abs(current_val - prev_val) > threshold_value_cony_kg
                                        shift_occurred_spec = not (-spec_cony_kg <= current_val <= spec_cony_kg)
                                        spec_val = spec_cony_kg
                                    elif param == "rfpp_cw_kg" and spec_rfpp_cw_kg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_rfpp_cw_kg
                                        shift_occurred_spec = current_val > spec_rfpp_cw_kg
                                        spec_val = spec_rfpp_cw_kg
                                    elif param == "rfpp_ccw_kg" and spec_rfpp_ccw_kg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_rfpp_ccw_kg
                                        shift_occurred_spec = current_val > spec_rfpp_ccw_kg
                                        spec_val = spec_rfpp_ccw_kg
                                    elif param == "rfh1_cw_kg" and spec_rfh1_cw_kg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_rfh1_cw_kg
                                        shift_occurred_spec = current_val > spec_rfh1_cw_kg
                                        spec_val = spec_rfh1_cw_kg
                                    elif param == "rfh1_ccw_kg" and spec_rfh1_ccw_kg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_rfh1_ccw_kg
                                        shift_occurred_spec = current_val > spec_rfh1_ccw_kg
                                        spec_val = spec_rfh1_ccw_kg
                                    elif param == "rfh2_cw_kg" and spec_rfh2_cw_kg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_rfh2_cw_kg
                                        shift_occurred_spec = current_val > spec_rfh2_cw_kg
                                        spec_val = spec_rfh2_cw_kg
                                    elif param == "rfh2_ccw_kg" and spec_rfh2_ccw_kg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_rfh2_ccw_kg
                                        shift_occurred_spec = current_val > spec_rfh2_ccw_kg
                                        spec_val = spec_rfh2_ccw_kg
                                    elif param == "lfpp_cw_kg" and spec_lfpp_cw_kg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_lfpp_cw_kg
                                        shift_occurred_spec = current_val > spec_lfpp_cw_kg
                                        spec_val = spec_lfpp_cw_kg
                                    elif param == "lfpp_ccw_kg" and spec_lfpp_ccw_kg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_lfpp_ccw_kg
                                        shift_occurred_spec = current_val > spec_lfpp_ccw_kg
                                        spec_val = spec_lfpp_ccw_kg
                                    elif param == "lsft_cw_kg" and spec_lsft_cw_kg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_lsft_cw_kg
                                        shift_occurred_spec = current_val > spec_lsft_cw_kg
                                        spec_val = spec_lsft_cw_kg
                                    elif param == "lsft_ccw_kg" and spec_lsft_ccw_kg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_lsft_ccw_kg
                                        shift_occurred_spec = current_val > spec_lsft_ccw_kg
                                        spec_val = spec_lsft_ccw_kg
                                    elif param == "lfh1_cw_kg" and spec_lfh1_cw_kg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_lfh1_cw_kg
                                        shift_occurred_spec = current_val > spec_lfh1_cw_kg
                                        spec_val = spec_lfh1_cw_kg
                                    elif param == "lfh1_ccw_kg" and spec_lfh1_ccw_kg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_lfh1_ccw_kg
                                        shift_occurred_spec = current_val > spec_lfh1_ccw_kg
                                        spec_val = spec_lfh1_ccw_kg
                                    elif param == "plysteer_kg" and spec_plysteer_kg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_plysteer_kg
                                        shift_occurred_spec = current_val > spec_plysteer_kg
                                        spec_val = spec_plysteer_kg
                                    elif param == "rfmxa_ccw_deg" and spec_rfmxa_ccw_deg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_rfmxa_ccw_deg
                                        shift_occurred_spec = current_val > spec_rfmxa_ccw_deg
                                        spec_val = spec_rfmxa_ccw_deg
                                    elif param == "rfa1_cw_deg" and spec_rfa1_cw_deg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_rfa1_cw_deg
                                        shift_occurred_spec = current_val > spec_rfa1_cw_deg
                                        spec_val = spec_rfa1_cw_deg
                                    elif param == "rfa1_ccw_deg" and spec_rfa1_ccw_deg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_rfa1_ccw_deg
                                        shift_occurred_spec = current_val > spec_rfa1_ccw_deg
                                        spec_val = spec_rfa1_ccw_deg
                                    elif param == "rfa2_cw_deg" and spec_rfa2_cw_deg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_rfa2_cw_deg
                                        shift_occurred_spec = current_val > spec_rfa2_cw_deg
                                        spec_val = spec_rfa2_cw_deg
                                    elif param == "rfa2_ccw_deg" and spec_rfa2_ccw_deg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_rfa2_ccw_deg
                                        shift_occurred_spec = current_val > spec_rfa2_ccw_deg
                                        spec_val = spec_rfa2_ccw_deg
                                    elif param == "lfmxa_cw_deg" and spec_lfmxa_cw_deg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_lfmxa_cw_deg
                                        shift_occurred_spec = current_val > spec_lfmxa_cw_deg
                                        spec_val = spec_lfmxa_cw_deg
                                    elif param == "lfmxa_ccw_deg" and spec_lfmxa_ccw_deg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_lfmxa_ccw_deg
                                        shift_occurred_spec = current_val > spec_lfmxa_ccw_deg
                                        spec_val = spec_lfmxa_ccw_deg
                                    elif param == "lfa1_cw_deg" and spec_lfa1_cw_deg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_lfa1_cw_deg
                                        shift_occurred_spec = current_val > spec_lfa1_cw_deg
                                        spec_val = spec_lfa1_cw_deg
                                    elif param == "lfa1_ccw_deg" and spec_lfa1_ccw_deg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_lfa1_ccw_deg
                                        shift_occurred_spec = current_val > spec_lfa1_ccw_deg
                                        spec_val = spec_lfa1_ccw_deg
                                    elif param == "static_g" and spec_static_g:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_bal_static_g
                                        shift_occurred_spec = current_val > spec_static_g
                                        spec_val = spec_static_g
                                    elif param == "static_deg" and spec_static_deg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_bal_static_deg
                                        shift_occurred_spec = current_val > spec_static_deg
                                        spec_val = spec_static_deg
                                    elif param == "couple_g" and spec_couple_g:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_bal_couple_g
                                        shift_occurred_spec = current_val > spec_couple_g
                                        spec_val = spec_couple_g
                                    elif param == "couple_deg" and spec_couple_deg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_bal_couple_deg
                                        shift_occurred_spec = current_val > spec_couple_deg
                                        spec_val = spec_couple_deg
                                    elif param == "botbul_mm" and spec_bul_botbul_mm:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_bul_botbul_mm
                                        shift_occurred_spec = current_val > spec_bul_botbul_mm
                                        spec_val = spec_bul_botbul_mm
                                    elif param == "topbul_mm" and spec_bul_topbul_mm:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_bul_topbul_mm
                                        shift_occurred_spec = current_val > spec_bul_topbul_mm
                                        spec_val = spec_bul_topbul_mm
                                    elif param == "botbul_deg" and spec_bul_botbul_deg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_bul_botbul_deg
                                        shift_occurred_spec = current_val > spec_bul_botbul_deg
                                        spec_val = spec_bul_botbul_deg
                                    elif param == "topdep_deg" and spec_bul_topdep_deg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_bul_topdep_deg
                                        shift_occurred_spec = current_val > spec_bul_topdep_deg
                                        spec_val = spec_bul_topdep_deg
                                    elif param == "botdep_mm" and spec_dep_botdep_mm:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_dep_botdep_mm
                                        shift_occurred_spec = current_val > spec_dep_botdep_mm
                                        spec_val = spec_dep_botdep_mm
                                    elif param == "topdep_mm" and spec_dep_topdep_mm:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_dep_topdep_mm
                                        shift_occurred_spec = current_val > spec_dep_topdep_mm
                                        spec_val = spec_dep_topdep_mm
                                    elif param == "botdep_deg" and spec_dep_botdep_deg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_dep_botdep_deg
                                        shift_occurred_spec = current_val > spec_dep_botdep_deg
                                        spec_val = spec_dep_botdep_deg
                                    elif param == "topbul_deg" and spec_dep_topbul_deg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_dep_topbul_deg
                                        shift_occurred_spec = current_val > spec_dep_topbul_deg
                                        spec_val = spec_dep_topbul_deg
                                    elif param == "crro_mm" and spec_rro_crro_mm:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_rro_crro_mm
                                        shift_occurred_spec = current_val > spec_rro_crro_mm
                                        spec_val = spec_rro_crro_mm
                                    elif param == "crroh1_mm" and spec_rro_crroh1_mm:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_rro_crroh1_mm
                                        shift_occurred_spec = current_val > spec_rro_crroh1_mm
                                        spec_val = spec_rro_crroh1_mm
                                    elif param == "blro_mm" and spec_lro_blro_mm:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_lro_blro_mm
                                        shift_occurred_spec = current_val > spec_lro_blro_mm
                                        spec_val = spec_lro_blro_mm
                                    elif param == "blroh1_mm" and spec_lro_blroh1_mm:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_lro_blroh1_mm
                                        shift_occurred_spec = current_val > spec_lro_blroh1_mm
                                        spec_val = spec_lro_blroh1_mm
                                    elif param == "tlro_mm" and spec_lro_tlro_mm:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_lro_tlro_mm
                                        shift_occurred_spec = current_val > spec_lro_tlro_mm
                                        spec_val = spec_lro_tlro_mm
                                    elif param == "tlroh1_mm" and spec_lro_tlroh1_mm:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_lro_tlroh1_mm
                                        shift_occurred_spec = current_val > spec_lro_tlroh1_mm
                                        spec_val = spec_lro_tlroh1_mm
                                    elif param == "crro_deg" and spec_rro_crro_deg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_rro_crro_deg
                                        shift_occurred_spec = current_val > spec_rro_crro_deg
                                        spec_val = spec_rro_crro_deg
                                    elif param == "crroh1_deg" and spec_rro_crroh1_deg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_rro_crroh1_deg
                                        shift_occurred_spec = current_val > spec_rro_crroh1_deg
                                        spec_val = spec_rro_crroh1_deg
                                    elif param == "blro_deg" and spec_lro_blro_deg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_lro_blro_deg
                                        shift_occurred_spec = current_val > spec_lro_blro_deg
                                        spec_val = spec_lro_blro_deg
                                    elif param == "blroh1_deg" and spec_lro_blroh1_deg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_lro_blroh1_deg
                                        shift_occurred_spec = current_val > spec_lro_blroh1_deg
                                        spec_val = spec_lro_blroh1_deg
                                    elif param == "tlro_deg" and spec_lro_tlro_deg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_lro_tlro_deg
                                        shift_occurred_spec = current_val > spec_lro_tlro_deg
                                        spec_val = spec_lro_tlro_deg
                                    elif param == "tlroh1_deg" and spec_lro_tlroh1_deg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_lro_tlroh1_deg
                                        shift_occurred_spec = current_val > spec_lro_tlroh1_deg
                                        spec_val = spec_lro_tlroh1_deg
                                    elif param == "botplane_g" and spec_dynamic_botplane_g:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_bal_dynamic_botplane_g
                                        shift_occurred_spec = current_val > spec_dynamic_botplane_g
                                        spec_val = spec_dynamic_botplane_g
                                    elif param == "topplane_g" and spec_dynamic_topplane_g:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_bal_dynamic_topplane_g
                                        shift_occurred_spec = current_val > spec_dynamic_topplane_g
                                        spec_val = spec_dynamic_topplane_g
                                    elif param == "botplane_deg" and spec_dynamic_botplane_deg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_bal_dynamic_botplane_deg
                                        shift_occurred_spec = current_val > spec_dynamic_botplane_deg
                                        spec_val = spec_dynamic_botplane_deg
                                    elif param == "topplane_deg" and spec_dynamic_topplane_deg:
                                        shift_occurred_thershold = (current_val - prev_val) > threshold_value_bal_dynamic_topplane_deg
                                        shift_occurred_spec = current_val > spec_dynamic_topplane_deg
                                        spec_val = spec_dynamic_topplane_deg
                                    
                                    # elif param == "oe_yield":
                                    #     spec_yield = yield_spec_mapping.get(param, 0)
                                    #     shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > class_threshold_value_yield_class_yield)
                                    #     shift_occurred_spec = current_val < spec_yield
                                    #     spec_val = spec_yield
                                    # elif param == "balancing_oe_yield":
                                    #     spec_yield = yield_spec_mapping.get(param, 0)
                                    #     shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_balancing_yield)
                                    #     shift_occurred_spec = current_val < spec_yield
                                    #     spec_val = spec_yield
                                    # elif param == "geometry_oe_yield":
                                    #     spec_yield = yield_spec_mapping.get(param, 0)
                                    #     shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_geometry_yield)
                                    #     shift_occurred_spec = current_val < spec_yield
                                    #     spec_val = spec_yield

                                    elif param in yield_params:
                                        spec_yield = yield_spec_mapping.get(param, 0)
                                        shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield)
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
                                    spec_val = yield_spec_mapping.get(param)

                                    if param == "cony_kg" and spec_cony_kg:
                                        shift_occurred_spec = not (-spec_cony_kg <= current_val <= spec_cony_kg)
                                        spec_val = spec_cony_kg
                                    elif param == "rfpp_cw_kg" and spec_rfpp_cw_kg:
                                        shift_occurred_spec = current_val > spec_rfpp_cw_kg
                                        spec_val = spec_rfpp_cw_kg
                                    elif param == "rfpp_ccw_kg" and spec_rfpp_ccw_kg:
                                        shift_occurred_spec = current_val > spec_rfpp_ccw_kg
                                        spec_val = spec_rfpp_ccw_kg
                                    elif param == "rfh1_cw_kg" and spec_rfh1_cw_kg:
                                        shift_occurred_spec = current_val > spec_rfh1_cw_kg
                                        spec_val = spec_rfh1_cw_kg
                                    elif param == "rfh1_ccw_kg" and spec_rfh1_ccw_kg:
                                        shift_occurred_spec = current_val > spec_rfh1_ccw_kg
                                        spec_val = spec_rfh1_ccw_kg
                                    elif param == "rfh2_cw_kg" and spec_rfh2_cw_kg:
                                        shift_occurred_spec = current_val > spec_rfh2_cw_kg
                                        spec_val = spec_rfh2_cw_kg
                                    elif param == "rfh2_ccw_kg" and spec_rfh2_ccw_kg:
                                        shift_occurred_spec = current_val > spec_rfh2_ccw_kg
                                        spec_val = spec_rfh2_ccw_kg
                                    elif param == "lfpp_cw_kg" and spec_lfpp_cw_kg:
                                        shift_occurred_spec = current_val > spec_lfpp_cw_kg
                                        spec_val = spec_lfpp_cw_kg
                                    elif param == "lfpp_ccw_kg" and spec_lfpp_ccw_kg:
                                        shift_occurred_spec = current_val > spec_lfpp_ccw_kg
                                        spec_val = spec_lfpp_ccw_kg
                                    elif param == "lsft_cw_kg" and spec_lsft_cw_kg:
                                        shift_occurred_spec = current_val > spec_lsft_cw_kg
                                        spec_val = spec_lsft_cw_kg
                                    elif param == "lsft_ccw_kg" and spec_lsft_ccw_kg:
                                        shift_occurred_spec = current_val > spec_lsft_ccw_kg
                                        spec_val = spec_lsft_ccw_kg
                                    elif param == "lfh1_cw_kg" and spec_lfh1_cw_kg:
                                        shift_occurred_spec = current_val > spec_lfh1_cw_kg
                                        spec_val = spec_lfh1_cw_kg
                                    elif param == "lfh1_ccw_kg" and spec_lfh1_ccw_kg:
                                        shift_occurred_spec = current_val > spec_lfh1_ccw_kg
                                        spec_val = spec_lfh1_ccw_kg
                                    elif param == "plysteer_kg" and spec_plysteer_kg:
                                        shift_occurred_spec = current_val > spec_plysteer_kg
                                        spec_val = spec_plysteer_kg
                                    elif param == "rfmxa_ccw_deg" and spec_rfmxa_ccw_deg:
                                        shift_occurred_spec = current_val > spec_rfmxa_ccw_deg
                                        spec_val = spec_rfmxa_ccw_deg
                                    elif param == "rfa1_cw_deg" and spec_rfa1_cw_deg:
                                        shift_occurred_spec = current_val > spec_rfa1_cw_deg
                                        spec_val = spec_rfa1_cw_deg
                                    elif param == "rfa1_ccw_deg" and spec_rfa1_ccw_deg:
                                        shift_occurred_spec = current_val > spec_rfa1_ccw_deg
                                        spec_val = spec_rfa1_ccw_deg
                                    elif param == "rfa2_cw_deg" and spec_rfa2_cw_deg:
                                        shift_occurred_spec = current_val > spec_rfa2_cw_deg
                                        spec_val = spec_rfa2_cw_deg
                                    elif param == "rfa2_ccw_deg" and spec_rfa2_ccw_deg:
                                        shift_occurred_spec = current_val > spec_rfa2_ccw_deg
                                        spec_val = spec_rfa2_ccw_deg
                                    elif param == "lfmxa_cw_deg" and spec_lfmxa_cw_deg:
                                        shift_occurred_spec = current_val > spec_lfmxa_cw_deg
                                        spec_val = spec_lfmxa_cw_deg
                                    elif param == "lfmxa_ccw_deg" and spec_lfmxa_ccw_deg:
                                        shift_occurred_spec = current_val > spec_lfmxa_ccw_deg
                                        spec_val = spec_lfmxa_ccw_deg
                                    elif param == "lfa1_cw_deg" and spec_lfa1_cw_deg:
                                        shift_occurred_spec = current_val > spec_lfa1_cw_deg
                                        spec_val = spec_lfa1_cw_deg
                                    elif param == "lfa1_ccw_deg" and spec_lfa1_ccw_deg:
                                        shift_occurred_spec = current_val > spec_lfa1_ccw_deg
                                        spec_val = spec_lfa1_ccw_deg
                                    elif param == "static_g" and spec_static_g:
                                        shift_occurred_spec = current_val > spec_static_g
                                        spec_val = spec_static_g
                                    elif param == "static_deg" and spec_static_deg:
                                        shift_occurred_spec = current_val > spec_static_deg
                                        spec_val = spec_static_deg
                                    elif param == "couple_g" and spec_couple_g:
                                        shift_occurred_spec = current_val > spec_couple_g
                                        spec_val = spec_couple_g
                                    elif param == "couple_deg" and spec_couple_deg:
                                        shift_occurred_spec = current_val > spec_couple_deg
                                        spec_val = spec_couple_deg
                                    elif param == "botbul_mm" and spec_bul_botbul_mm:
                                        shift_occurred_spec = current_val > spec_bul_botbul_mm
                                        spec_val = spec_bul_botbul_mm
                                    elif param == "topbul_mm" and spec_bul_topbul_mm:
                                        shift_occurred_spec = current_val > spec_bul_topbul_mm
                                        spec_val = spec_bul_topbul_mm
                                    elif param == "botbul_deg" and spec_bul_botbul_deg:
                                        shift_occurred_spec = current_val > spec_bul_botbul_deg
                                        spec_val = spec_bul_botbul_deg
                                    elif param == "topdep_deg" and spec_bul_topdep_deg:
                                        shift_occurred_spec = current_val > spec_bul_topdep_deg
                                        spec_val = spec_bul_topdep_deg
                                    elif param == "botdep_mm" and spec_dep_botdep_mm:
                                        shift_occurred_spec = current_val > spec_dep_botdep_mm
                                        spec_val = spec_dep_botdep_mm
                                    elif param == "topdep_mm" and spec_dep_topdep_mm:
                                        shift_occurred_spec = current_val > spec_dep_topdep_mm
                                        spec_val = spec_dep_topdep_mm
                                    elif param == "botdep_deg" and spec_dep_botdep_deg:
                                        shift_occurred_spec = current_val > spec_dep_botdep_deg
                                        spec_val = spec_dep_botdep_deg
                                    elif param == "topbul_deg" and spec_dep_topbul_deg:
                                        shift_occurred_spec = current_val > spec_dep_topbul_deg
                                        spec_val = spec_dep_topbul_deg
                                    elif param == "crro_mm" and spec_rro_crro_mm:
                                        shift_occurred_spec = current_val > spec_rro_crro_mm
                                        spec_val = spec_rro_crro_mm
                                    elif param == "crroh1_mm" and spec_rro_crroh1_mm:
                                        shift_occurred_spec = current_val > spec_rro_crroh1_mm
                                        spec_val = spec_rro_crroh1_mm
                                    elif param == "blro_mm" and spec_lro_blro_mm:
                                        shift_occurred_spec = current_val > spec_lro_blro_mm
                                        spec_val = spec_lro_blro_mm
                                    elif param == "blroh1_mm" and spec_lro_blroh1_mm:
                                        shift_occurred_spec = current_val > spec_lro_blroh1_mm
                                        spec_val = spec_lro_blroh1_mm
                                    elif param == "tlro_mm" and spec_lro_tlro_mm:
                                        shift_occurred_spec = current_val > spec_lro_tlro_mm
                                        spec_val = spec_lro_tlro_mm
                                    elif param == "tlroh1_mm" and spec_lro_tlroh1_mm:
                                        shift_occurred_spec = current_val > spec_lro_tlroh1_mm
                                        spec_val = spec_lro_tlroh1_mm
                                    elif param == "crro_deg" and spec_rro_crro_deg:
                                        shift_occurred_spec = current_val > spec_rro_crro_deg
                                        spec_val = spec_rro_crro_deg
                                    elif param == "crroh1_deg" and spec_rro_crroh1_deg:
                                        shift_occurred_spec = current_val > spec_rro_crroh1_deg
                                        spec_val = spec_rro_crroh1_deg
                                    elif param == "blro_deg" and spec_lro_blro_deg:
                                        shift_occurred_spec = current_val > spec_lro_blro_deg
                                        spec_val = spec_lro_blro_deg
                                    elif param == "blroh1_deg" and spec_lro_blroh1_deg:
                                        shift_occurred_spec = current_val > spec_lro_blroh1_deg
                                        spec_val = spec_lro_blroh1_deg
                                    elif param == "tlro_deg" and spec_lro_tlro_deg:
                                        shift_occurred_spec = current_val > spec_lro_tlro_deg
                                        spec_val = spec_lro_tlro_deg
                                    elif param == "tlroh1_deg" and spec_lro_tlroh1_deg:
                                        shift_occurred_spec = current_val > spec_lro_tlroh1_deg
                                        spec_val = spec_lro_tlroh1_deg
                                    elif param == "botplane_g" and spec_dynamic_botplane_g:
                                        shift_occurred_spec = current_val > spec_dynamic_botplane_g
                                        spec_val = spec_dynamic_botplane_g
                                    elif param == "topplane_g" and spec_dynamic_topplane_g:
                                        shift_occurred_spec = current_val > spec_dynamic_topplane_g
                                        spec_val = spec_dynamic_topplane_g
                                    elif param == "botplane_deg" and spec_dynamic_botplane_deg:
                                        shift_occurred_spec = current_val > spec_dynamic_botplane_deg
                                        spec_val = spec_dynamic_botplane_deg
                                    elif param == "topplane_deg" and spec_dynamic_topplane_deg:
                                        shift_occurred_spec = current_val > spec_dynamic_topplane_deg
                                        spec_val = spec_dynamic_topplane_deg
                                    

                                    # elif param == "oe_yield":
                                    #     spec_yield = yield_spec_mapping.get(param, 0)
                                    #     shift_occurred_spec = current_val < spec_yield
                                    #     spec_val = spec_yield
                                    # elif param == "balancing_oe_yield":
                                    #     spec_yield = yield_spec_mapping.get(param, 0)
                                    #     shift_occurred_spec = current_val < spec_yield
                                    #     spec_val = spec_yield
                                    # elif param == "geometry_oe_yield":
                                    #     spec_yield = yield_spec_mapping.get(param, 0)
                                    #     shift_occurred_spec = current_val < spec_yield
                                    #     spec_val = spec_yield

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
                                    "gt_code": model_number,
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
                                # logging.info(f"Prepared {len(deviated_parameter_alerts)} derived alerts for machine {machine_no}, lot {current_model_lot_id}")

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
                                "gt_code": model_number,
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

                            machine_batch_data["tbm_shift"] = machine_batch_data["date_of_tbm"].apply(get_shift)
                            shift_groups = machine_batch_data.groupby('tbm_shift')
                            shifts_data_list_tbm = []
                            for shift_name, shift_data in shift_groups:
                                shift_start_time = shift_data["date_of_tbm"].iloc[0]
                                shift_end_time = shift_data["date_of_tbm"].iloc[-1]
                                shifts_data_list_tbm.append({
                                    "shift": shift_name,
                                    "barcode_count": len(shift_data["barcode"]) if "barcode" in shift_data.columns else 0,
                                    "machine_shift_tbm_start_time": shift_start_time,
                                    "machine_shift_tbm_end_time": shift_end_time,
                                    "barcodes": shift_data["barcode"].tolist() if "barcode" in shift_data.columns else []
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
                                "oe_yield": oe_yield(machine_batch_data),
                                "balancing_oe_yield": balancing_oe_yield(machine_batch_data),
                                "geometry_oe_yield": geometry_oe_yield(machine_batch_data),
                                "warehouse_yield": warehouse_yield(machine_batch_data),
                                "visual_yield": visual_yield(machine_batch_data),
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
                                "rank_columns": get_rank_columns(machine_batch_data),
                                "barcodes": machine_batch_data["barcode"].tolist() if "barcode" in machine_batch_data.columns else []
                            }

                            alert_doc_main = {
                                "_id": f"main_alert_{last_alert_id}",
                                "alert_id_ufm_ufm": last_alert_id,
                                "gt_code": model_number,
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
                            if not alert_ufm_collect.find_one({"alert_id_ufm_ufm": last_alert_id}):
                                ufm_alert_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(alert_doc_main)))
                                output_messages.append(f"Prepared alert_id_ufm_ufm {last_alert_id} for insertion into alert_ufm_collect")

                            trigger_doc = {
                                "_id": f"trigger_alert_{last_alert_id}",
                                "alert_id_ufm_ufm": last_alert_id,
                                "gt_code": model_number,
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
                            if not alert_triggering_collect_dashboard_ufm.find_one({"alert_id_ufm_ufm": last_alert_id}):
                                ufm_triggering_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(trigger_doc)))
                                output_messages.append(f"Prepared trigger alert_id_ufm_ufm {last_alert_id} for insertion into alert_triggering_collect_dashboard_ufm")
                            if not alert_collect.find_one({"alert_id_ufm_ufm": last_alert_id}):
                                ufm_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(trigger_doc)))
                                output_messages.append(f"Prepared trigger alert_id_ufm_ufm {last_alert_id} for insertion into alert_collect")

                            machine_barcode_counter += target_lot_size

                    except Exception as e:
                        logging.error("❌ UFM Error processing machine %s for model %s: %s", ufm_machine_id, model_number, e)
                        output_messages.append(f"❌ UFM Error processing machine {ufm_machine_id} for model {model_number}: {e}")
                        continue

        except Exception as e:
            logging.error("❌ UFM Error processing model %s: %s", model_number, e)
            output_messages.append(f"❌ UFM Error processing model {model_number}: {e}")
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
            output_messages.append(f"✅ UFM Inserted {result.inserted_count} deviated alert documents into alert_deviated_final_yield.alert_ufm_with_lot_size.")
        except Exception as e:
            error_message = f"❌ UFM Error performing bulk write to deviated alert collections: {e}"
            output_messages.append(error_message)
    else:
        output_messages.append("No UFM deviated alert documents to insert into deviated alert collections.")

    if ufm_alert_bulk_operations:
        try:
            result = alert_ufm_collect.bulk_write(ufm_alert_bulk_operations, ordered=False)
            inserted_counts["alert_ufm_collect"] = result.inserted_count
            final_log_messages.append(f"✅ UFM Inserted {result.inserted_count} documents into UFM alert collection (alert_db_virgin_yield.{alert_triggering_ubno_collect_name}).")
        except pymongo.errors.BulkWriteError as bwe:
            inserted_counts["alert_ufm_collect"] = bwe.details['nInserted']
            final_log_messages.append(f"⚠️ UFM Alert Bulk Write Errors: Inserted {bwe.details['nInserted']}. Errors: {bwe.details['writeErrors']}")
        except Exception as e:
            final_log_messages.append(f"❌ UFM Error performing bulk write to UFM alert collection: {e}")

    if ufm_triggering_bulk_operations:
        try:
            result_dashboard = alert_triggering_collect_dashboard_ufm.bulk_write(ufm_triggering_bulk_operations, ordered=False)
            inserted_counts["alert_triggering_collect_dashboard_ufm"] = result_dashboard.inserted_count
            output_messages.append(f"✅ UFM Inserted {result_dashboard.inserted_count} machine_alert documents into alert_db_final_yield.{alert_triggering_ubno_collect_name}.")
        except pymongo.errors.BulkWriteError as bwe:
            inserted_counts["alert_triggering_collect_dashboard_ufm"] = bwe.details['nInserted']
            output_messages.append(f"⚠️ UFM Triggering Alert Bulk Write Errors in alert_db_final_yield.{alert_triggering_ubno_collect_name}: Inserted {bwe.details['nInserted']}. Errors: {bwe.details['writeErrors']}")
        except Exception as e:
            output_messages.append(f"❌ UFM Error performing bulk write to alert_db_final_yield.{alert_triggering_ubno_collect_name}: {e}")
    else:
        output_messages.append(f"No UFM triggering alert documents to insert into alert_db_final_yield.{alert_triggering_ubno_collect_name}.")

    if ufm_bulk_operations:
        try:
            result = alert_collect.bulk_write(ufm_bulk_operations, ordered=False)
            inserted_counts["alert_collect"] = result.inserted_count
            final_log_messages.append(f"✅ UFM Inserted {result.inserted_count} documents into UFM triggering alert collection (alert_db_virgin_yield.{alert_ufm_collect_name}).")
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
    # WITHOUT lot size: assign a lot id to ALL barcodes for every machine present in
    # the data (do not restrict to active machines). This ensures no barcode is left
    # with a null tbm_machine_model_lot_id in new_data_merged_combined_uniform_balan_geo.
    _restrict_active_machines = False
    inserted_counts = {}
    updated_counts = {}
    # tbm_debug_csv_filename = "tbm_process_debug_data.csv"

    # Initialize MongoDB collections
    try:
        merge_collect = client[UNIFORMITY_BALANCING_GEOMETRY_DB][new_COMBINED_COLLECTION_NAME]
        channel_collect_tbm = client[config_databse][channel_no_collect_tbm]
        channel_collect_ufm = client[config_databse][channel_no_collect]
        lot_tbm_collect = client[lot_db][lot_tbm_collect_name_new]
        alert_collect = client[alert_table_final_yield][alert_tbm_collect_name_new]
        averages_collect = client[average_db][averages_tbm_table_new]
        machine_collect = client[config_databse][machine_no_collect]
        curing_press_collect = client[config_databse][curing_press_dict_collect]
        alert_triggering_collect = client[alert_db_virgin_yield][alert_triggering_collect_name_new]
        summary_triggering_collect = client[summary_db][summary_triggering_collect_name_tbm]
        alert_triggering_deviated_collect = client[alert_deviated_final_yield][alert_triggering_deviated_tbm_new]
        # NEW: Initialize collections for alert_db_final_yield with the same collection names
        alert_triggering_collect_dashboard = client[alert_db_final_yield][alert_triggering_collect_name_new]
        # NEW: Initialize collections for model_workcenter_db
        tbm_gtcodes_collect = client[model_db][tbm_collection]
        tbm_machine_collect = client[model_db][tbm_collection_machine]
        output_messages.append("✅ TBM Collections loaded successfully for lot and alert processing in both alert_db_virgin_yield and alert_db_final_yield.")
    except Exception as e:
        return {"status": "error", "message": "Database connection failed", "output_log": output_messages}, 500
    
    # Fetch active machines from model_workcenter_db.tbm_machine
    try:
        active_machines = set(tbm_machine_collect.distinct("tbm", {"status": "active"}))
        output_messages.append(f"✅ Fetched {len(active_machines)} active machines: {active_machines}")
        if not active_machines:
            output_messages.append("❌ No active machines found in model_workcenter_db.tbm_machine. Exiting.")
            return {"status": "error", "message": "No active machines found", "output_log": output_messages}, 500
    except Exception as e:
        output_messages.append(f"❌ Error fetching active machines: {e}")
        return {"status": "error", "message": "Failed to fetch active machines", "output_log": output_messages}, 500

    # Fetch active gt_code numbers
    try:
        # Step 1: Fetch distinct active gt_codes (flatten from list fields)
        active_gtcodes = set()
        cursor = tbm_gtcodes_collect.find({"status": "active"}, {"gt_code": 1})
        for doc in cursor:
            active_gtcodes.update(doc.get("gt_code", []))  # flatten the gt_code list
        output_messages.append(f"✅ Fetched {len(active_gtcodes)} active model numbers: {active_gtcodes}")
    except Exception as e:
        output_messages.append(f"❌ Error fetching active gt_code numbers: {e}")
        return {"status": "error", "message": "Failed to fetch active model numbers", "output_log": output_messages}, 500

    # Load data into DataFrames
    try:
        merge1 = list(merge_collect.find({}))
        merge_df = pd.DataFrame(merge1)
        channel_data_tbm = list(channel_collect_tbm.find({"gt_code": {"$in": list(active_gtcodes)}}))
        channel_data_tbm = list(channel_collect_tbm.find({}))
        channel_df = pd.DataFrame(channel_data_tbm)
        channel_data_ufm = list(channel_collect_ufm.find({}))
        channel_df_ufm = pd.DataFrame(channel_data_ufm)
        machine_data = list(machine_collect.find({"tbm": {"$in": list(active_machines)}}))
        machine_data = list(machine_collect.find({}))
        machine_df = pd.DataFrame(machine_data)
        output_messages.append(f"✅ TBM Data loaded from collections into DataFrames for processing (filtered for active model numbers: {active_gtcodes}).")
    except Exception as e:
        return {"status": "error", "message": "Data loading failed", "output_log": output_messages}, 500
    
    # Get distinct model numbers
    gt_code_lists = channel_collect_tbm.distinct("gt_code")

    # Check required columns — initialize any missing ones instead of exiting
    if merge_df.empty:
        output_messages.append("merge_df is empty. No TBM data to process.")
        return {"status": "success", "message": "No TBM data to process.", "output_log": output_messages, "inserted_counts": inserted_counts, "updated_counts": updated_counts}, 200
    if "gt_code" not in merge_df.columns:
        output_messages.append("'gt_code' column missing in merge_df. Skipping TBM processing.")
        return {"status": "success", "message": "'gt_code' column missing. Skipping TBM processing.", "output_log": output_messages, "inserted_counts": inserted_counts, "updated_counts": updated_counts}, 200
    if "alert_id_tbm_ufm" not in merge_df.columns:
        merge_df["alert_id_tbm_ufm"] = None

    # Filter data
    filtered_df = merge_df[merge_df["gt_code"].isin(gt_code_lists) & merge_df["alert_id_tbm_ufm"].isna()]  # Changed from modelno to gt_code
    if not filtered_df.empty:
        merge_df = filtered_df
    else:
        output_messages.append("No TBM data after filtering or condition not satisfied. Skipping.")
        return {"status": "success", "message": "No TBM data after filtering.", "output_log": output_messages, "inserted_counts": inserted_counts, "updated_counts": updated_counts}, 200

    # Assign date columns
    merge_df["date_of_tbm"] = merge_df.get("tbm_buildtime", pd.Series([None] * len(merge_df)))
    merge_df["date_of_curing"] = merge_df.get("curing_infeedtime", pd.Series([None] * len(merge_df)))

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
        merge_df["date_of_ufm"] = convert_datetime_with_invalid_mask(merge_df["date_of_ufm"], "date_of_ufm")
        merge_df["date_of_ufm"] = merge_df["date_of_ufm"].astype(object).where(merge_df["date_of_ufm"].notnull(), "No Date")
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

    # Fetch last lot IDs per tbm and model
    try:
        last_lots = list(lot_tbm_collect.aggregate([
            {
                "$match": {
                    "lot_id": {"$regex": "^TBM_[^_]+_[^_]+_\\d+$"},
                    "gt_code": {"$in": list(active_gtcodes)},
                    "tbm": {"$in": list(active_machines)}
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
                    "gt_code": "$gt_code",
                    "tbm": "$tbm"
                }
            },
            {
                "$sort": {"lot_number": -1}
            },
            {
                "$group": {
                    "_id": {"gt_code": "$gt_code", "tbm": "$tbm"},
                    "max_lot_number": {"$first": "$lot_number"},
                    "last_lot_id": {"$first": "$lot_id"}
                }
            }
        ]))
        last_lot_dict = {
            (item["_id"]["gt_code"], item["_id"]["tbm"]): {
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
    merge_df["tbm"] = merge_df["tbm"].astype(str)
    machine_df["tbm"] = machine_df["tbm"].astype(str)  # Changed from modelnumber to gt_code is handled as array later
    # Filter merge_df to include only active machines (only when restricting).
    if _restrict_active_machines:
        merge_df = merge_df[merge_df["tbm"].isin(active_machines)]
    if merge_df.empty:
        output_messages.append("❌ No data in merge_df after filtering for active machines. Exiting.")
        return {"status": "error", "message": "No data after active tbm filtering", "output_log": output_messages}, 500

    # Ensure lot-tracking columns are object dtype so string lot IDs can be assigned
    # (they may pre-exist as float64/NaN, which raises "Invalid value ... for dtype 'float64'")
    for _lot_col in ["lot_id", "previous_lot_id", "tbm_machine_model_lot_id", "tbm_previous_machine_model_lot_id", "alert_id_tbm_ufm"]:
        if _lot_col not in merge_df.columns:
            merge_df[_lot_col] = None
        merge_df[_lot_col] = merge_df[_lot_col].astype(object)

    # Initialize tracking variables
    machine_lot_avg_map = {}
    tbm_previous_machine_model_lot_ids = {}
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
                "gt_code": {"$in": list(active_gtcodes)},
                "machine": {"$in": list(active_machines)}
            }
            },
            {
                "$sort": {"tbm_machine_model_lot_id": -1}
            },
            {
                "$group": {
                    "_id": {"gt_code": "$gt_code", "machine": "$machine"},
                    "last_tbm_lot_id": {"$first": "$tbm_machine_model_lot_id"},
                    "last_averages": {"$first": "$current_lot_averages"}
                }
            }
        ]))
        for item in last_averages:
            gt_code = item["_id"]["gt_code"]
            tbm = item["_id"]["machine"]
            tbm_lot_id = item["last_tbm_lot_id"]
            averages = item["last_averages"]
            tbm_previous_machine_model_lot_ids[(gt_code, tbm)] = tbm_lot_id
            if averages:
                machine_lot_avg_map[(gt_code, tbm, tbm_lot_id)] = averages
                logging.info(f"Pre-populated machine_lot_avg_map with averages for {tbm_lot_id}")
            else:
                logging.warning(f"No averages found for {tbm_lot_id} in averages_collect")
        logging.info(f"Initialized tbm_previous_machine_model_lot_ids: {tbm_previous_machine_model_lot_ids}")
    except Exception as e:
        logging.error(f"Error pre-populating machine_lot_avg_map and tbm_previous_machine_model_lot_ids: {e}")
        output_messages.append(f"❌ Error pre-populating machine_lot_avg_map and tbm_previous_machine_model_lot_ids: {e}")

    numeric_params = [
        'rfpp_cw_kg','rfpp_ccw_kg','rfh1_cw_kg','rfh1_ccw_kg','rfh2_cw_kg','rfh2_ccw_kg','lfpp_cw_kg','lfpp_ccw_kg',
        'lsft_cw_kg','lsft_ccw_kg','lfh1_cw_kg','lfh1_ccw_kg','plysteer_kg','cony_kg','rfmxa_ccw_deg',
        'rfa1_cw_deg','rfa1_ccw_deg','rfa2_cw_deg','rfa2_ccw_deg','lfmxa_cw_deg','lfmxa_ccw_deg','lfa1_cw_deg',
        'lfa1_ccw_deg','botplane_g','topplane_g','couple_g','static_g','botplane_deg','topplane_deg','couple_deg',
        'static_deg','botbul_mm','botdep_mm','topbul_mm','topdep_mm','blro_mm','blroh1_mm','crro_mm','crroh1_mm',
        'tlro_mm','tlroh1_mm','botbul_deg','botdep_deg','topbul_deg','topdep_deg','blro_deg','blroh1_deg','crro_deg',
        'crroh1_deg','tlro_deg','tlroh1_deg'
        ]
    yield_params = [
        "rfv_yield", "rfh1_yield", "rfh2_yield", "lfv_yield", "lsft_yield", "lfh1_yield", "conicity_yield", "oe_yield",
        "rfmxa_yield", "rfa1_yield", "rfa2_yield", "lfxma_yield", "lfa1_yield", "central_radial_runout_yield", "central_radial_runout_first_harmonic_yield",
        "lateral_run_out_top_over_yield", "lateral_run_out_bottom_over_yield", "lateral_run_out_yield", "lateral_run_out_bottom_first_harmonic_yield", "lateral_run_out_top_first_harmonic_yield", 
        "lateral_run_out_harmonic_yield", "bulge_top_over_yield", "bulge_bottom_over_yield", "bulge_yield", "dent_top_over_yield", "dent_bottom_over_yield", "dent_yield", "geometry_oe_yield",
        "lower_yield", "upper_yield", "static_yield", "couple_yield", "balancing_oe_yield", "warehouse_yield", "visual_yield"
        ]
    
    # Define rank parameter mappings
    rank_param_mappings = {
        "rfv_yield": ["tu_rfpp_cw_grade", "tu_rfpp_ccw_grade"],
        "rfh1_yield": ["tu_rfh1_cw_grade", "tu_rfh1_ccw_grade"], 
        "rfh2_yield": ["tu_rfh2_cw_grade", "tu_rfh2_ccw_grade"],
        "lfv_yield": ["tu_lfpp_cw_grade", "tu_lfpp_ccw_grade"],
        "lsft_yield": ["tu_lsft_cw_grade", "tu_lsft_ccw_grade"],
        "lfh1_yield": ["tu_lfh1_cw_grad", "tu_lfh1_ccw_grad"],
        "conicity_yield": ["tu_cony_grade"],
        "oe_yield": ["tu_class"],
        "rfmxa_yield": ["tu_rfmxa_ccw_grade"],
        "rfa1_yield": ["tu_rfa1_cw_grade", "tu_rfa1_ccw_grade"],
        "rfa2_yield": ["tu_rfa2_cw_grade", "tu_rfa2_ccw_grade"],
        "lfxma_yield": ["tu_lfmxa_cw_grade", "tu_lfmxa_ccw_grade"],
        "lfa1_yield": ["tu_lfa1_cw_grade", "tu_lfa1_ccw_grade"],
        "central_radial_runout_yield": ["tg_crro_grade"],
        "central_radial_runout_first_harmonic_yield": ["tg_crroh1_grade"],
        "lateral_run_out_top_over_yield": ["tg_tlro_grade"],
        "lateral_run_out_bottom_over_yield": ["tg_blro_grade"],
        "lateral_run_out_yield": ["tg_tlro_grade", "tg_blro_grade"],
        "lateral_run_out_bottom_first_harmonic_yield": ["tg_blroh1_grade"],
        "lateral_run_out_top_first_harmonic_yield": ["tg_tlroh1_grade"],
        "lateral_run_out_harmonic_yield": ["tg_tlroh1_grade", "tg_blroh1_grade"],
        "bulge_top_over_yield": ["tg_topbul_grade"],
        "bulge_bottom_over_yield": ["tg_botbul_grade"],
        "bulge_yield": ["tg_botbul_grade", "tg_topbul_grade"],
        "dent_top_over_yield": ["tg_topdep_grade"],
        "dent_bottom_over_yield": ["tg_botdep_grade"],
        "dent_yield": ["tg_topdep_grade", "tg_botdep_grade"],
        "geometry_oe_yield": ["tg_class"],
        "lower_yield": ["tb_botplane_grade"],
        "upper_yield": ["tb_topplane_grade"],
        "static_yield": ["tb_static_grade"],
        "couple_yield": ["tb_couple_grade"],
        "balancing_oe_yield": ["tb_class"]
        }
    # TBM tbm-wise processing
    for _, row in channel_df.iterrows():
        try:
            gt_code_list = row.get("gt_code", [])
            model_number = gt_code_list[0].strip() if isinstance(gt_code_list, list) and gt_code_list else ""

            if model_number not in active_gtcodes:
                output_messages.append(f"Skipping TBM model {model_number} as it is not active.")
                continue
            gt_codes = row.get("gt_code", [])

            # Ensure gt_codes is a list
            if not isinstance(gt_codes, list):
                gt_codes = [gt_codes]
            
            # spec_con = float(row.get("spec_con", 0))
            sizecode = str(row.get("sizecode", ""))
            spec_rfpp_cw_kg = float(row.get("spec_rfpp_cw_kg", 0))
            spec_rfpp_ccw_kg = float(row.get("spec_rfpp_ccw_kg", 0))
            spec_rfh1_cw_kg = float(row.get("spec_rfh1_cw_kg", 0))
            spec_rfh1_ccw_kg = float(row.get("spec_rfh1_ccw_kg", 0))
            spec_rfh2_cw_kg = float(row.get("spec_rfh2_cw_kg", 0))
            spec_rfh2_ccw_kg = float(row.get("spec_rfh2_ccw_kg", 0))
            spec_lfpp_cw_kg = float(row.get("spec_lfpp_cw_kg", 0))
            spec_lfpp_ccw_kg = float(row.get("spec_lfpp_ccw_kg", 0))
            spec_lsft_cw_kg = float(row.get("spec_lsft_cw_kg", 0))
            spec_lsft_ccw_kg = float(row.get("spec_lsft_ccw_kg", 0))
            spec_lfh1_cw_kg = float(row.get("spec_lfh1_cw_kg", 0))
            spec_lfh1_ccw_kg = float(row.get("spec_lfh1_ccw_kg", 0))
            spec_plysteer_kg = float(row.get("spec_plysteer_kg", 0))
            spec_rfmxa_ccw_deg = float(row.get("spec_rfmxa_ccw_deg", 0))
            spec_rfa1_cw_deg = float(row.get("spec_rfa1_cw_deg", 0))
            spec_rfa1_ccw_deg = float(row.get("spec_rfa1_ccw_deg", 0))
            spec_rfa2_cw_deg = float(row.get("spec_rfa2_cw_deg", 0))
            spec_rfa2_ccw_deg = float(row.get("spec_rfa2_ccw_deg", 0))
            spec_lfmxa_cw_deg = float(row.get("spec_lfmxa_cw_deg", 0))
            spec_lfmxa_ccw_deg = float(row.get("spec_lfmxa_ccw_deg", 0))
            spec_lfa1_cw_deg = float(row.get("spec_lfa1_cw_deg", 0))
            spec_lfa1_ccw_deg = float(row.get("spec_lfa1_ccw_deg", 0))
            spec_cony_kg = float(row.get("spec_cony_kg", 0))
            spec_static_g = float(row.get("spec_static_g", 0))
            spec_static_deg = float(row.get("spec_static_deg", 0))
            spec_couple_g = float(row.get("spec_couple_g", 0))
            spec_couple_deg = float(row.get("spec_couple_deg", 0))
            spec_bul_botbul_mm = float(row.get("spec_bul_botbul_mm", 0))
            spec_bul_topbul_mm = float(row.get("spec_bul_topbul_mm", 0))
            spec_bul_botbul_deg = float(row.get("spec_bul_botbul_deg", 0))
            spec_bul_topdep_deg = float(row.get("spec_bul_topdep_deg", 0))
            spec_dep_botdep_mm = float(row.get("spec_dep_botdep_mm", 0))
            spec_dep_topdep_mm = float(row.get("spec_dep_topdep_mm", 0))
            spec_dep_botdep_deg = float(row.get("spec_dep_botdep_deg", 0))
            spec_dep_topbul_deg = float(row.get("spec_dep_topbul_deg", 0))
            spec_rro_crro_mm = float(row.get("spec_rro_crro_mm", 0))
            spec_rro_crroh1_mm = float(row.get("spec_rro_crroh1_mm", 0))
            spec_lro_blro_mm = float(row.get("spec_lro_blro_mm", 0))
            spec_lro_blroh1_mm = float(row.get("spec_lro_blroh1_mm", 0))
            spec_lro_tlro_mm = float(row.get("spec_lro_tlro_mm", 0))
            spec_lro_tlroh1_mm = float(row.get("spec_lro_tlroh1_mm", 0))
            spec_rro_crro_deg = float(row.get("spec_rro_crro_deg", 0))
            spec_rro_crroh1_deg = float(row.get("spec_rro_crroh1_deg", 0))
            spec_lro_blro_deg = float(row.get("spec_lro_blro_deg", 0))
            spec_lro_blroh1_deg = float(row.get("spec_lro_blroh1_deg", 0))
            spec_lro_tlro_deg = float(row.get("spec_lro_tlro_deg", 0))
            spec_lro_tlroh1_deg = float(row.get("spec_lro_tlroh1_deg", 0))
            spec_dynamic_botplane_g = float(row.get("spec_dynamic_botplane_g", 0))
            spec_dynamic_topplane_g = float(row.get("spec_dynamic_topplane_g", 0))
            spec_dynamic_botplane_deg = float(row.get("spec_dynamic_botplane_deg", 0))
            spec_dynamic_topplane_deg = float(row.get("spec_dynamic_topplane_deg", 0))

            rfv_yield_spec_yield = float(row.get("rfv_yield_spec_yield", 0))
            rfh1_yield_spec_yield = float(row.get("rfh1_yield_spec_yield", 0))
            rfh2_yield_spec_yield = float(row.get("rfh2_yield_spec_yield", 0))
            lfv_yield_spec_yield = float(row.get("lfv_yield_spec_yield", 0))
            lsft_yield_spec_yield = float(row.get("lsft_yield_spec_yield", 0))
            lfh1_yield_spec_yield = float(row.get("lfh1_yield_spec_yield", 0))
            conicity_yield_spec_yield = float(row.get("conicity_yield_spec_yield", 0))
            oe_yield_spec_yield = float(row.get("oe_yield_spec_yield", 0))
            rfmxa_yield_spec_yield = float(row.get("rfmxa_yield_spec_yield", 0))
            rfa1_yield_spec_yield = float(row.get("rfa1_yield_spec_yield", 0))
            rfa2_yield_spec_yield = float(row.get("rfa2_yield_spec_yield", 0))
            lfxma_yield_spec_yield = float(row.get("lfxma_yield_spec_yield", 0))
            lfa1_yield_spec_yield = float(row.get("lfa1_yield_spec_yield", 0))
            central_radial_runout_yield_spec_yield = float(row.get("central_radial_runout_yield_spec_yield", 0))
            central_radial_runout_first_harmonic_yield_spec_yield = float(row.get("central_radial_runout_first_harmonic_yield_spec_yield", 0))
            lateral_run_out_top_over_yield_spec_yield = float(row.get("lateral_run_out_top_over_yield_spec_yield", 0))
            lateral_run_out_bottom_over_yield_spec_yield = float(row.get("lateral_run_out_bottom_over_yield_spec_yield", 0))
            lateral_run_out_yield_spec_yield = float(row.get("lateral_run_out_yield_spec_yield", 0))
            lateral_run_out_bottom_first_harmonic_yield_spec_yield = float(row.get("lateral_run_out_bottom_first_harmonic_yield_spec_yield", 0))
            lateral_run_out_top_first_harmonic_yield_spec_yield = float(row.get("lateral_run_out_top_first_harmonic_yield_spec_yield", 0))
            lateral_run_out_harmonic_yield_spec_yield = float(row.get("lateral_run_out_harmonic_yield_spec_yield", 0))
            bulge_top_over_yield_spec_yield = float(row.get("bulge_top_over_yield_spec_yield", 0))
            bulge_bottom_over_yield_spec_yield = float(row.get("bulge_bottom_over_yield_spec_yield", 0))
            bulge_yield_spec_yield = float(row.get("bulge_yield_spec_yield", 0))
            dent_top_over_yield_spec_yield = float(row.get("dent_top_over_yield_spec_yield", 0))
            dent_bottom_over_yield_spec_yield = float(row.get("dent_bottom_over_yield_spec_yield", 0))
            dent_yield_spec_yield = float(row.get("dent_yield_spec_yield", 0))
            geometry_oe_yield_spec_yield = float(row.get("geometry_oe_yield_spec_yield", 0))
            lower_yield_spec_yield = float(row.get("lower_yield_spec_yield", 0))
            upper_yield_spec_yield = float(row.get("upper_yield_spec_yield", 0))
            static_yield_spec_yield = float(row.get("static_yield_spec_yield", 0))
            couple_yield_spec_yield = float(row.get("couple_yield_spec_yield", 0))
            balancing_oe_yield_spec_yield = float(row.get("balancing_oe_yield_spec_yield", 0))
            warehouse_yield_spec_yield = float(row.get("warehouse_yield_spec_yield", 0))
            visual_yield_spec_yield = float(row.get("visual_yield_spec_yield", 0))
            
            class_spec_yield = float(row.get("class_spec_yield", 0))
            balancing_spec_yield = float(row.get("balancing_spec_yield", 0))
            geometry_spec_yield = float(row.get("geometry_spec_yield", 0))
            
            threshold_value_rfpp_cw_kg = float(row.get("threshold_value_rfpp_cw_kg", 5))
            threshold_value_rfpp_ccw_kg = float(row.get("threthreshold_value_rfpp_ccw_kgshold_value_bal", 5))
            threshold_value_rfh1_cw_kg = float(row.get("threshold_value_rfh1_cw_kg", 5))
            threshold_value_rfh1_ccw_kg = float(row.get("threshold_value_rfh1_ccw_kg", 5))
            threshold_value_rfh2_cw_kg = float(row.get("threshold_value_rfh2_cw_kg", 5))
            threshold_value_rfh2_ccw_kg = float(row.get("threshold_value_rfh2_ccw_kg", 5))
            threshold_value_lfpp_cw_kg = float(row.get("threshold_value_lfpp_cw_kg", 5))
            threshold_value_lfpp_ccw_kg = float(row.get("threshold_value_lfpp_ccw_kg", 5))
            threshold_value_lsft_cw_kg = float(row.get("threshold_value_lsft_cw_kg", 5))
            threshold_value_lsft_ccw_kg = float(row.get("threshold_value_lsft_ccw_kg", 5))
            threshold_value_lfh1_cw_kg = float(row.get("threshold_value_lfh1_cw_kg", 5))
            threshold_value_lfh1_ccw_kg = float(row.get("threshold_value_lfh1_ccw_kg", 5))
            threshold_value_plysteer_kg = float(row.get("threshold_value_plysteer_kg", 5))
            threshold_value_rfmxa_cw_deg = float(row.get("threshold_value_rfmxa_cw_deg", 5))
            threshold_value_rfmxa_ccw_deg = float(row.get("threshold_value_rfmxa_ccw_deg", 5))
            threshold_value_rfa1_cw_deg = float(row.get("threshold_value_rfa1_cw_deg", 5))
            threshold_value_rfa1_ccw_deg = float(row.get("threshold_value_rfa1_ccw_deg", 5))
            threshold_value_rfa2_cw_deg = float(row.get("threshold_value_rfa2_cw_deg", 5))
            threshold_value_rfa2_ccw_deg = float(row.get("threshold_value_rfa2_ccw_deg", 5))
            threshold_value_lfmxa_cw_deg = float(row.get("threshold_value_lfmxa_cw_deg", 5))
            threshold_value_lfmxa_ccw_deg = float(row.get("threshold_value_lfmxa_ccw_deg", 5))
            threshold_value_lfa1_cw_deg = float(row.get("threshold_value_lfa1_cw_deg", 5))
            threshold_value_lfa1_ccw_deg = float(row.get("threshold_value_lfa1_ccw_deg", 5))
            threshold_value_cony_kg = float(row.get("threshold_value_cony_kg", 5))
            threshold_value_bal_static_g = float(row.get("threshold_value_bal_static_g", 5))
            threshold_value_bal_static_deg = float(row.get("threshold_value_bal_static_deg", 5))
            threshold_value_bal_couple_g = float(row.get("threshold_value_bal_couple_g", 5))
            threshold_value_bal_couple_deg = float(row.get("threshold_value_bal_couple_deg", 5))
            threshold_value_bal_dynamic_botplane_g = float(row.get("threshold_value_bal_dynamic_botplane_g", 5))
            threshold_value_bal_dynamic_topplane_g = float(row.get("threshold_value_bal_dynamic_topplane_g", 5))
            threshold_value_bal_dynamic_botplane_deg = float(row.get("threshold_value_bal_dynamic_botplane_deg", 5))
            threshold_value_bal_dynamic_topplane_deg = float(row.get("threshold_value_bal_dynamic_topplane_deg", 5))
            threshold_value_geo_bul_botbul_mm = float(row.get("threshold_value_geo_bul_botbul_mm", 5))
            threshold_value_geo_bul_topbul_mm = float(row.get("threshold_value_geo_bul_topbul_mm", 5))
            threshold_value_geo_bul_botbul_deg = float(row.get("threshold_value_geo_bul_botbul_deg", 5))
            threshold_value_geo_bul_topdep_deg = float(row.get("threshold_value_geo_bul_topdep_deg", 5))
            threshold_value_geo_dep_botdep_mm = float(row.get("threshold_value_geo_dep_botdep_mm", 5))
            threshold_value_geo_dep_topdep_mm = float(row.get("threshold_value_geo_dep_topdep_mm", 5))
            threshold_value_geo_dep_botdep_deg = float(row.get("threshold_value_geo_dep_botdep_deg", 5))
            threshold_value_geo_dep_topbul_deg = float(row.get("threshold_value_geo_dep_topbul_deg", 5))
            threshold_value_geo_rro_crro_mm = float(row.get("threshold_value_geo_rro_crro_mm", 5))
            threshold_value_geo_rro_crroh1_mm = float(row.get("threshold_value_geo_rro_crroh1_mm", 5))
            threshold_value_geo_lro_blro_mm = float(row.get("threshold_value_geo_lro_blro_mm", 5))
            threshold_value_geo_lro_blroh1_mm = float(row.get("threshold_value_geo_lro_blroh1_mm", 5))
            threshold_value_geo_lro_tlro_mm = float(row.get("threshold_value_geo_lro_tlro_mm", 5))
            threshold_value_geo_lro_tlroh1_mm = float(row.get("threshold_value_geo_lro_tlroh1_mm", 5))
            threshold_value_geo_rro_crro_deg = float(row.get("threshold_value_geo_rro_crro_deg", 5))
            threshold_value_geo_rro_crroh1_deg = float(row.get("threshold_value_geo_rro_crroh1_deg", 5))
            threshold_value_geo_lro_blro_deg = float(row.get("threshold_value_geo_lro_blro_deg", 5))
            threshold_value_geo_lro_blroh1_deg = float(row.get("threshold_value_geo_lro_blroh1_deg", 5))
            threshold_value_geo_lro_tlro_deg = float(row.get("threshold_value_geo_lro_tlro_deg", 5))
            threshold_value_geo_lro_tlroh1_deg = float(row.get("threshold_value_geo_lro_tlroh1_deg", 5))

            threshold_value_yield_rfv_yield = float(row.get("threshold_value_yield_rfv_yield", 5))
            threshold_value_yield_rfh1_yield = float(row.get("threshold_value_yield_rfh1_yield", 5))
            threshold_value_yield_rfh2_yield = float(row.get("threshold_value_yield_rfh2_yield", 5))
            threshold_value_yield_lfv_yield = float(row.get("threshold_value_yield_lfv_yield", 5))
            threshold_value_yield_lsft_yield = float(row.get("threshold_value_yield_lsft_yield", 5))
            threshold_value_yield_lfh1_yield = float(row.get("threshold_value_yield_lfh1_yield", 5))
            threshold_value_yield_conicity_yield = float(row.get("threshold_value_yield_conicity_yield", 5))
            threshold_value_yield_oe_yield = float(row.get("threshold_value_yield_oe_yield", 5))
            threshold_value_yield_rfmxa_yield = float(row.get("threshold_value_yield_rfmxa_yield", 5))
            threshold_value_yield_rfa1_yield = float(row.get("threshold_value_yield_rfa1_yield", 5))
            threshold_value_yield_rfa2_yield = float(row.get("threshold_value_yield_rfa2_yield", 5))
            threshold_value_yield_lfxma_yield = float(row.get("threshold_value_yield_lfxma_yield", 5))
            threshold_value_yield_lfa1_yield = float(row.get("threshold_value_yield_lfa1_yield", 5))
            threshold_value_yield_central_radial_runout_yield = float(row.get("threshold_value_yield_central_radial_runout_yield", 5))
            threshold_value_yield_central_radial_runout_first_harmonic_yield = float(row.get("threshold_value_yield_central_radial_runout_first_harmonic_yield", 5))
            threshold_value_yield_lateral_run_out_top_over_yield = float(row.get("threshold_value_yield_lateral_run_out_top_over_yield", 5))
            threshold_value_yield_lateral_run_out_bottom_over_yield = float(row.get("threshold_value_yield_lateral_run_out_bottom_over_yield", 5))
            threshold_value_yield_lateral_run_out_yield = float(row.get("threshold_value_yield_lateral_run_out_yield", 5))
            threshold_value_yield_lateral_run_out_bottom_first_harmonic_yield = float(row.get("threshold_value_yield_lateral_run_out_bottom_first_harmonic_yield", 5))
            threshold_value_yield_lateral_run_out_top_first_harmonic_yield = float(row.get("threshold_value_yield_lateral_run_out_top_first_harmonic_yield", 5))
            threshold_value_yield_lateral_run_out_harmonic_yield = float(row.get("threshold_value_yield_lateral_run_out_harmonic_yield", 5))
            threshold_value_yield_bulge_top_over_yield = float(row.get("threshold_value_yield_bulge_top_over_yield", 5))
            threshold_value_yield_bulge_bottom_over_yield = float(row.get("threshold_value_yield_bulge_bottom_over_yield", 5))
            threshold_value_yield_bulge_yield = float(row.get("threshold_value_yield_bulge_yield", 5))
            threshold_value_yield_dent_top_over_yield = float(row.get("threshold_value_yield_dent_top_over_yield", 5))
            threshold_value_yield_dent_bottom_over_yield = float(row.get("threshold_value_yield_dent_bottom_over_yield", 5))
            threshold_value_yield_dent_yield = float(row.get("threshold_value_yield_dent_yield", 5))
            threshold_value_yield_geometry_oe_yield = float(row.get("threshold_value_yield_geometry_oe_yield", 5))
            threshold_value_yield_lower_yield = float(row.get("threshold_value_yield_lower_yield", 5))
            threshold_value_yield_upper_yield = float(row.get("threshold_value_yield_upper_yield", 5))
            threshold_value_yield_static_yield = float(row.get("threshold_value_yield_static_yield", 5))
            threshold_value_yield_couple_yield = float(row.get("threshold_value_yield_couple_yield", 5))
            threshold_value_yield_balancing_oe_yield = float(row.get("threshold_value_yield_balancing_oe_yield", 5))
            threshold_value_yield_warehouse_yield = float(row.get("threshold_value_yield_warehouse_yield", 5))
            threshold_value_yield_visual_yield = float(row.get("threshold_value_yield_visual_yield", 5))
            threshold_value_yield = float(row.get("threshold_value_yield", 5))

            class_threshold_value_yield_class_yield = float(row.get("class_threshold_value_yield_class_yield", 5))
            threshold_value_yield_balancing_yield = float(row.get("threshold_value_yield_balancing_yield", 5))
            threshold_value_yield_geometry_yield = float(row.get("threshold_value_yield_geometry_yield", 5))
            
            for gt_code in gt_codes:
                gt_code = str(gt_code).strip()

                # Filter merge_df for each gt_code individually.
                # WITHOUT lot size: assign a lot id to ALL barcodes for the machine,
                # so we do NOT restrict to active machines here (every machine present
                # in the data must get a lot id, never null).
                model_data = merge_df[
                    (merge_df["gt_code"] == model_number)  # Changed from modelno to gt_code
                ].copy()

                total_barcodes_model = len(model_data)
                model_data.sort_values(["date_of_tbm"], ascending=True, inplace=True)
                alert_map = {}

                # Virgin/Final yield split: store raw barcode rows, then continue main
                # processing on virgin data (first occurrence per barcode).
                virgin_data, final_data, _vf_msgs = split_and_store_virgin_final(model_data, "date_of_tbm", "TBM")
                output_messages.extend(_vf_msgs)
                model_data = virgin_data.copy()
                total_barcodes_model = len(model_data)

                for tbm_value, tbm_data in model_data.groupby("tbm"):
                    # WITHOUT lot size: process every machine group so all barcodes
                    # get a lot id. Do NOT skip machines missing from machine_df config.
                    if tbm_data.empty:
                        continue
                    total_tbm_barcodes = len(tbm_data)
                    machine_no = str(tbm_value)

                    barcode_counter = 0

                    while barcode_counter < total_tbm_barcodes:
                        remaining_model = total_tbm_barcodes - barcode_counter
                        batch_size = remaining_model
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
                        # logging.info(f"Initial tbm_previous_machine_model_lot_id: {tbm_previous_machine_model_lot_id} for model {model_number}, machine {machine_no}")
                        if tbm_previous_machine_model_lot_id is None and last_lot_number > 1:
                            prev_lot_number = last_lot_number - 1
                            prev_lot_id = f"TBM_{machine_no}_{model_number}_{prev_lot_number}"
                            logging.info(f"Fetching previous lot for lot_id: {prev_lot_id}")
                            try:
                                prev_lot = averages_collect.find_one(
                                    {
                                        "gt_code": model_number,
                                        "machine": machine_no,
                                        "lot_id": prev_lot_id
                                    },
                                    sort=[("tbm_machine_model_lot_id", -1)]
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
                                        "gt_code": model_number,
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
                            # logging.info(f"First lot for model {model_number}, machine {machine_no}. Setting previous_lot_averages to empty.")
                        # logging.info(f"prev_key: {prev_key}, prev_lot_avg: {prev_lot_avg}")

                        lot_doc = {
                            "gt_code": model_number,
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
                            "lsft_yield": lsft_yield(batch_data),
                            "lfh1_yield": lfh1_yield(batch_data),
                            "conicity_yield": conicity_yield(batch_data),
                            "oe_yield": oe_yield(batch_data),
                            "rfmxa_yield": rfmxa_yield(batch_data),
                            "rfa1_yield": rfa1_yield(batch_data),
                            "rfa2_yield": rfa2_yield(batch_data),
                            "lfxma_yield": lfxma_yield(batch_data),
                            "lfa1_yield": lfa1_yield(batch_data),
                            "central_radial_runout_yield": central_radial_runout_yield(batch_data),
                            "central_radial_runout_first_harmonic_yield": central_radial_runout_first_harmonic_yield(batch_data),
                            "lateral_run_out_top_over_yield": lateral_run_out_top_over_yield(batch_data),
                            "lateral_run_out_bottom_over_yield": lateral_run_out_bottom_over_yield(batch_data),
                            "lateral_run_out_yield": lateral_run_out_yield(batch_data),
                            "lateral_run_out_bottom_first_harmonic_yield": lateral_run_out_bottom_first_harmonic_yield(batch_data),
                            "lateral_run_out_top_first_harmonic_yield": lateral_run_out_top_first_harmonic_yield(batch_data),
                            "lateral_run_out_harmonic_yield": lateral_run_out_harmonic_yield(batch_data),
                            "bulge_top_over_yield": bulge_top_over_yield(batch_data),
                            "bulge_bottom_over_yield": bulge_bottom_over_yield(batch_data),
                            "bulge_yield": bulge_yield(batch_data),
                            "dent_top_over_yield": dent_top_over_yield(batch_data),
                            "dent_bottom_over_yield": dent_bottom_over_yield(batch_data),
                            "dent_yield": dent_yield(batch_data),
                            "geometry_oe_yield": geometry_oe_yield(batch_data),
                            "lower_yield": lower_yield(batch_data),
                            "upper_yield": upper_yield(batch_data),
                            "static_yield": static_yield(batch_data),
                            "couple_yield": couple_yield(batch_data),
                            "balancing_oe_yield": balancing_oe_yield(batch_data),
                            "warehouse_yield": warehouse_yield(batch_data),
                            "visual_yield": visual_yield(batch_data)})
                                                                                

                        grouped_averages = {}
                        param_spec_map = {
                            "cony_kg": spec_cony_kg,
                            "rfpp_cw_kg": spec_rfpp_cw_kg,
                            "rfpp_ccw_kg": spec_rfpp_ccw_kg,
                            "rfh1_cw_kg": spec_rfh1_cw_kg,
                            "rfh1_ccw_kg": spec_rfh1_ccw_kg,
                            "lfpp_cw_kg": spec_lfpp_cw_kg,
                            "lfpp_ccw_kg": spec_lfpp_ccw_kg,
                            'rfh2_cw_kg': spec_rfh2_cw_kg,
                            'rfh2_ccw_kg': spec_rfh2_ccw_kg,
                            'lsft_cw_kg': spec_lsft_cw_kg,
                            'lsft_ccw_kg': spec_lsft_ccw_kg,
                            'lfh1_cw_kg': spec_lfh1_cw_kg,
                            'lfh1_ccw_kg': spec_lfh1_ccw_kg,
                            'plysteer_kg': spec_plysteer_kg,
                            'rfmxa_ccw_deg':spec_rfmxa_ccw_deg,
                            'rfa1_cw_deg': spec_rfa1_cw_deg,
                            'rfa1_ccw_deg': spec_rfa1_ccw_deg,
                            'rfa2_cw_deg': spec_rfa2_cw_deg,
                            'rfa2_ccw_deg': spec_rfa2_ccw_deg,
                            'lfmxa_cw_deg': spec_lfmxa_cw_deg,
                            'lfmxa_ccw_deg': spec_lfmxa_ccw_deg,
                            'lfa1_cw_deg': spec_lfa1_cw_deg,
                            'lfa1_ccw_deg': spec_lfa1_ccw_deg,
                            'couple_g': spec_couple_g,
                            'botplane_deg': spec_dynamic_botplane_deg,
                            'topplane_deg': spec_dynamic_topplane_deg,
                            'couple_deg': spec_couple_deg,
                            'static_deg': spec_static_deg,
                            'botbul_mm': spec_bul_botbul_mm,
                            'botdep_mm': spec_dep_botdep_mm,
                            'topbul_mm': spec_bul_topbul_mm,
                            'topdep_mm': spec_dep_topdep_mm,
                            'blro_mm': spec_lro_blro_mm,
                            'blroh1_mm': spec_lro_blroh1_mm,
                            'crro_mm': spec_rro_crro_mm,
                            'crroh1_mm': spec_rro_crroh1_mm,
                            'tlro_mm': spec_lro_tlro_mm,
                            'tlroh1_mm': spec_lro_tlroh1_mm,
                            'botbul_deg': spec_bul_botbul_deg,
                            'botdep_deg': spec_dep_botdep_deg,
                            'topbul_deg': spec_dep_topbul_deg,
                            'topdep_deg': spec_bul_topdep_deg,
                            'blro_deg': spec_lro_blro_deg,
                            'blroh1_deg': spec_lro_blroh1_deg,
                            'crro_deg': spec_rro_crro_deg,
                            'crroh1_deg': spec_rro_crroh1_deg,
                            'tlro_deg': spec_lro_tlro_deg,
                            'tlroh1_deg': spec_lro_tlroh1_deg,
                            'botplane_g': spec_dynamic_botplane_g,
                            'topplane_g': spec_dynamic_topplane_g,
                            "static_g": spec_static_g,

                            "rfv_yield": rfv_yield_spec_yield,
                            "rfh1_yield": rfh1_yield_spec_yield,
                            "rfh2_yield": rfh2_yield_spec_yield,
                            "lfv_yield": lfv_yield_spec_yield,
                            "lsft_yield": lsft_yield_spec_yield,
                            "lfh1_yield": lfh1_yield_spec_yield,
                            "conicity_yield": conicity_yield_spec_yield,
                            "oe_yield": oe_yield_spec_yield,
                            "rfmxa_yield": rfmxa_yield_spec_yield,
                            "rfa1_yield": rfa1_yield_spec_yield,
                            "rfa2_yield": rfa2_yield_spec_yield,
                            "lfxma_yield": lfxma_yield_spec_yield,
                            "lfa1_yield": lfa1_yield_spec_yield,
                            "central_radial_runout_yield": central_radial_runout_yield_spec_yield,
                            "central_radial_runout_first_harmonic_yield": central_radial_runout_first_harmonic_yield_spec_yield,
                            "lateral_run_out_top_over_yield": lateral_run_out_top_over_yield_spec_yield,
                            "lateral_run_out_bottom_over_yield": lateral_run_out_bottom_over_yield_spec_yield,
                            "lateral_run_out_yield": lateral_run_out_yield_spec_yield,
                            "lateral_run_out_bottom_first_harmonic_yield": lateral_run_out_bottom_first_harmonic_yield_spec_yield,
                            "lateral_run_out_top_first_harmonic_yield": lateral_run_out_top_first_harmonic_yield_spec_yield,
                            "lateral_run_out_harmonic_yield": lateral_run_out_harmonic_yield_spec_yield,
                            "bulge_top_over_yield": bulge_top_over_yield_spec_yield,
                            "bulge_bottom_over_yield": bulge_bottom_over_yield_spec_yield,
                            "bulge_yield": bulge_yield_spec_yield,
                            "dent_top_over_yield": dent_top_over_yield_spec_yield,
                            "dent_bottom_over_yield": dent_bottom_over_yield_spec_yield,
                            "dent__yield": dent_yield_spec_yield,
                            "geometry_oe_yield": geometry_oe_yield_spec_yield,
                            "lower_yield": lower_yield_spec_yield,
                            "upper_yield": upper_yield_spec_yield,
                            "static_yield": static_yield_spec_yield,
                            "couple_yield": couple_yield_spec_yield,
                            "balancing_oe_yield": balancing_oe_yield_spec_yield,
                            "warehouse_yield": warehouse_yield_spec_yield,
                            "visual_yield": visual_yield_spec_yield
                        }
                        try:
                            batch_data["ubno"] = batch_data["ubno"].astype(str).str.strip()
                            batch_data.sort_values("date_of_ufm", ascending=True, inplace=True)
                            ubno_grouped = batch_data.groupby("ubno")
                            ubno_records = []
                            for ubno, group in ubno_grouped:
                                group.sort_values("date_of_ufm", ascending=True)
                                record = {
                                    "ubno": ubno,
                                    "barcode_count": len(group["barcode"]) if "barcode" in group.columns else 0,
                                    "tub_date_start_time": group["date_of_ufm"].iloc[0] if not group["date_of_ufm"].empty else None,
                                    "tub_date_end_time": group["date_of_ufm"].iloc[-1] if not group["date_of_ufm"].empty else None,
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
                                        if param == "cony_kg" and spec_cony_kg is not None and avg_val is not None:
                                            exceeded = not (-spec_cony_kg <= avg_val <= spec_cony_kg)
                                        if spec is not None and param not in ["cony_kg"] and avg_val is not None:
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
                            grouped_averages["ubno"] = ubno_records
                        except Exception as e:
                            logging.error(f"Error processing ubno grouped averages: {e}")
                            grouped_averages["ubno"] = []

                        batch_data1 = batch_data.copy()
                        batch_data.sort_values("date_of_tbm", ascending=True, inplace=True)
                        batch_data1.sort_values("date_of_ufm", ascending=True, inplace=True)
                        machine_lot_start_build_time = batch_data["date_of_tbm"].iloc[0]
                        machine_lot_end_build_time = batch_data["date_of_tbm"].iloc[-1]
                        tub_date_start_time = batch_data1["date_of_ufm"].iloc[0]
                        tub_date_end_time = batch_data1["date_of_ufm"].iloc[-1]
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
                                    if param == "cony_kg" and spec_cony_kg is not None:
                                        if abs(current_val - prev_val) > threshold_value_cony_kg:
                                            shift = True
                                        if not (-spec_cony_kg <= current_val <= spec_cony_kg):
                                            shift_occurred_spec = True
                                    if param == "rfpp_cw_kg" and spec_rfpp_cw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_rfpp_cw_kg:
                                            shift = True
                                        if current_val > spec_rfpp_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfpp_ccw_kg" and spec_rfpp_ccw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_rfpp_ccw_kg:
                                            shift = True
                                        if current_val > spec_rfpp_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfh1_cw_kg" and spec_rfh1_cw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_rfh1_cw_kg:
                                            shift = True
                                        if current_val > spec_rfh1_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfh1_ccw_kg" and spec_rfh1_ccw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_rfh1_ccw_kg:
                                            shift = True
                                        if current_val > spec_rfh1_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfh2_cw_kg" and spec_rfh2_cw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_rfh2_cw_kg:
                                            shift = True
                                        if current_val > spec_rfh2_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfh2_ccw_kg" and spec_rfh2_ccw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_rfh2_ccw_kg:
                                            shift = True
                                        if current_val > spec_rfh2_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "lfpp_cw_kg" and spec_lfpp_cw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_lfpp_cw_kg:
                                            shift = True
                                        if current_val > spec_lfpp_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "lfpp_ccw_kg" and spec_lfpp_ccw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_lfpp_ccw_kg:
                                            shift = True
                                        if current_val > spec_lfpp_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "lsft_cw_kg" and spec_lsft_cw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_lsft_cw_kg:
                                            shift = True
                                        if current_val > spec_lsft_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "lsft_ccw_kg" and spec_lsft_ccw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_lsft_ccw_kg:
                                            shift = True
                                        if current_val > spec_lsft_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "lfh1_cw_kg" and spec_lfh1_cw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_lfh1_cw_kg:
                                            shift = True
                                        if current_val > spec_lfh1_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "lfh1_ccw_kg" and spec_lfh1_ccw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_lfh1_ccw_kg:
                                            shift = True
                                        if current_val > spec_lfh1_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "plysteer_kg" and spec_plysteer_kg is not None:
                                        if (current_val - prev_val) > threshold_value_plysteer_kg:
                                            shift = True
                                        if current_val > spec_plysteer_kg:
                                            shift_occurred_spec = True
                                    if param == "rfmxa_ccw_deg" and spec_rfmxa_ccw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_rfmxa_ccw_deg:
                                            shift = True
                                        if current_val > spec_rfmxa_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "rfa1_cw_deg" and spec_rfa1_cw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_rfa1_cw_deg:
                                            shift = True
                                        if current_val > spec_rfa1_cw_deg:
                                            shift_occurred_spec = True
                                    if param == "rfa1_ccw_deg" and spec_rfa1_ccw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_rfa1_ccw_deg:
                                            shift = True
                                        if current_val > spec_rfa1_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "rfa2_cw_deg" and spec_rfa2_cw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_rfa2_cw_deg:
                                            shift = True
                                        if current_val > spec_rfa2_cw_deg:
                                            shift_occurred_spec = True
                                    if param == "rfa2_ccw_deg" and spec_rfa2_ccw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_rfa2_ccw_deg:
                                            shift = True
                                        if current_val > spec_rfa2_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "lfmxa_cw_deg" and spec_lfmxa_cw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_lfmxa_cw_deg:
                                            shift = True
                                        if current_val > spec_lfmxa_cw_deg:
                                            shift_occurred_spec = True
                                    if param == "lfmxa_ccw_deg" and spec_lfmxa_ccw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_lfmxa_ccw_deg:
                                            shift = True
                                        if current_val > spec_lfmxa_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "lfa1_cw_deg" and spec_lfa1_cw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_lfa1_cw_deg:
                                            shift = True
                                        if current_val > spec_lfa1_cw_deg:
                                            shift_occurred_spec = True
                                    if param == "lfa1_ccw_deg" and spec_lfa1_ccw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_lfa1_ccw_deg:
                                            shift = True
                                        if current_val > spec_lfa1_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "static_g" and spec_static_g is not None:
                                        if (current_val - prev_val) > threshold_value_bal_static_g:
                                            shift = True
                                        if current_val > spec_static_g:
                                            shift_occurred_spec = True
                                    if param == "static_deg" and spec_static_deg is not None:
                                        if (current_val - prev_val) > threshold_value_bal_static_deg:
                                            shift = True
                                        if current_val > spec_static_deg:
                                            shift_occurred_spec = True
                                    if param == "couple_g" and spec_couple_g is not None:
                                        if (current_val - prev_val) > threshold_value_bal_couple_g:
                                            shift = True
                                        if current_val > spec_couple_g:
                                            shift_occurred_spec = True
                                    if param == "couple_deg" and spec_couple_deg is not None:
                                        if (current_val - prev_val) > threshold_value_bal_couple_deg:
                                            shift = True
                                        if current_val > spec_couple_deg:
                                            shift_occurred_spec = True
                                    if param == "botbul_mm" and spec_bul_botbul_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_bul_botbul_mm:
                                            shift = True
                                        if current_val > spec_bul_botbul_mm:
                                            shift_occurred_spec = True
                                    if param == "topbul_mm" and spec_bul_topbul_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_bul_topbul_mm:
                                            shift = True
                                        if current_val > spec_bul_topbul_mm:
                                            shift_occurred_spec = True
                                    if param == "botbul_deg" and spec_bul_botbul_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_bul_botbul_deg:
                                            shift = True
                                        if current_val > spec_bul_botbul_deg:
                                            shift_occurred_spec = True
                                    if param == "topdep_deg" and spec_bul_topdep_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_bul_topdep_deg:
                                            shift = True
                                        if current_val > spec_bul_topdep_deg:
                                            shift_occurred_spec = True
                                    if param == "botdep_mm" and spec_dep_botdep_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_dep_botdep_mm:
                                            shift = True
                                        if current_val > spec_dep_botdep_mm:
                                            shift_occurred_spec = True
                                    if param == "topdep_mm" and spec_dep_topdep_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_dep_topdep_mm:
                                            shift = True
                                        if current_val > spec_dep_topdep_mm:
                                            shift_occurred_spec = True
                                    if param == "botdep_deg" and spec_dep_botdep_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_dep_botdep_deg:
                                            shift = True
                                        if current_val > spec_dep_botdep_deg:
                                            shift_occurred_spec = True
                                    if param == "topbul_deg" and spec_dep_topbul_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_dep_topbul_deg:
                                            shift = True
                                        if current_val > spec_dep_topbul_deg:
                                            shift_occurred_spec = True
                                    if param == "crro_mm" and spec_rro_crro_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_rro_crro_mm:
                                            shift = True
                                        if current_val > spec_rro_crro_mm:
                                            shift_occurred_spec = True
                                    if param == "crroh1_mm" and spec_rro_crroh1_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_rro_crroh1_mm:
                                            shift = True
                                        if current_val > spec_rro_crroh1_mm:
                                            shift_occurred_spec = True
                                    if param == "blro_mm" and spec_lro_blro_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_lro_blro_mm:
                                            shift = True
                                        if current_val > spec_lro_blro_mm:
                                            shift_occurred_spec = True
                                    if param == "blroh1_mm" and spec_lro_blroh1_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_lro_blroh1_mm:
                                            shift = True
                                        if current_val > spec_lro_blroh1_mm:
                                            shift_occurred_spec = True
                                    if param == "tlro_mm" and spec_lro_tlro_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_lro_tlro_mm:
                                            shift = True
                                        if current_val > spec_lro_tlro_mm:
                                            shift_occurred_spec = True
                                    if param == "tlroh1_mm" and spec_lro_tlroh1_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_lro_tlroh1_mm:
                                            shift = True
                                        if current_val > spec_lro_tlroh1_mm:
                                            shift_occurred_spec = True
                                    if param == "crro_deg" and spec_rro_crro_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_rro_crro_deg:
                                            shift = True
                                        if current_val > spec_rro_crro_deg:
                                            shift_occurred_spec = True
                                    if param == "crroh1_deg" and spec_rro_crroh1_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_rro_crroh1_deg:
                                            shift = True
                                        if current_val > spec_rro_crroh1_deg:
                                            shift_occurred_spec = True
                                    if param == "blro_deg" and spec_lro_blro_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_lro_blro_deg:
                                            shift = True
                                        if current_val > spec_lro_blro_deg:
                                            shift_occurred_spec = True
                                    if param == "blroh1_deg" and spec_lro_blroh1_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_lro_blroh1_deg:
                                            shift = True
                                        if current_val > spec_lro_blroh1_deg:
                                            shift_occurred_spec = True
                                    if param == "tlro_deg" and spec_lro_tlro_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_lro_tlro_deg:
                                            shift = True
                                        if current_val > spec_lro_tlro_deg:
                                            shift_occurred_spec = True
                                    if param == "tlroh1_deg" and spec_lro_tlroh1_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_lro_tlroh1_deg:
                                            shift = True
                                        if current_val > spec_lro_tlroh1_deg:
                                            shift_occurred_spec = True
                                    if param == "botplane_g" and spec_dynamic_botplane_g is not None:
                                        if (current_val - prev_val) > threshold_value_bal_dynamic_botplane_g:
                                            shift = True
                                        if current_val > spec_dynamic_botplane_g:
                                            shift_occurred_spec = True
                                    if param == "topplane_g" and spec_dynamic_topplane_g is not None:
                                        if (current_val - prev_val) > threshold_value_bal_dynamic_topplane_g:
                                            shift = True
                                        if current_val > spec_dynamic_topplane_g:
                                            shift_occurred_spec = True
                                    if param == "botplane_deg" and spec_dynamic_botplane_deg is not None:
                                        if (current_val - prev_val) > threshold_value_bal_dynamic_botplane_deg:
                                            shift = True
                                        if current_val > spec_dynamic_botplane_deg:
                                            shift_occurred_spec = True
                                    if param == "topplane_deg" and spec_dynamic_topplane_deg is not None:
                                        if (current_val - prev_val) > threshold_value_bal_dynamic_topplane_deg:
                                            shift = True
                                        if current_val > spec_dynamic_topplane_deg:
                                            shift_occurred_spec = True

                                    if param in yield_params:
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True

                                    # if param == "oe_yield":
                                    #     spec_yield = param_spec_map.get(param)
                                    #     if spec_yield is not None:
                                    #         if (prev_val > current_val) and (abs(current_val - prev_val) > (class_threshold_value_yield_class_yield)):
                                    #             shift = True
                                    #         if abs(current_val < spec_yield):
                                    #             shift_occurred_spec = True
                                    # if param == "balancing_oe_yield":
                                    #     spec_yield = param_spec_map.get(param)
                                    #     if spec_yield is not None:
                                    #         if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_balancing_yield)):
                                    #             shift = True
                                    #         if abs(current_val < spec_yield):
                                    #             shift_occurred_spec = True
                                    # if param == "geometry_oe_yield":
                                    #     spec_yield = param_spec_map.get(param)
                                    #     if spec_yield is not None:
                                    #         if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_geometry_yield)):
                                    #             shift = True
                                    #         if abs(current_val < spec_yield):
                                    #             shift_occurred_spec = True
                                    
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
                                parameter_alerts.append(alert_dict)
                                if shift_occurred:
                                    overall_shift_occurred = True
                        else:
                            for param in numeric_params + yield_params:
                                shift_occurred_thershold = False
                                shift_occurred_spec = False
                                current_val = lot_averages.get(param)
                                if current_val is not None:
                                    if param == "cony_kg" and spec_cony_kg is not None:
                                        if not (-spec_cony_kg <= current_val <= spec_cony_kg):
                                            shift_occurred_spec = True
                                    if param == "rfpp_cw_kg" and spec_rfpp_cw_kg is not None:
                                        if current_val > spec_rfpp_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfpp_ccw_kg" and spec_rfpp_ccw_kg is not None:
                                        if current_val > spec_rfpp_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfh1_cw_kg" and spec_rfh1_cw_kg is not None:
                                        if current_val > spec_rfh1_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfh1_ccw_kg" and spec_rfh1_ccw_kg is not None:
                                        if current_val > spec_rfh1_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfh2_cw_kg" and spec_rfh2_cw_kg is not None:
                                        if current_val > spec_rfh2_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfh2_ccw_kg" and spec_rfh2_ccw_kg is not None:
                                        if current_val > spec_rfh2_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "lfpp_cw_kg" and spec_lfpp_cw_kg is not None:
                                        if current_val > spec_lfpp_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "lfpp_ccw_kg" and spec_lfpp_ccw_kg is not None:
                                        if current_val > spec_lfpp_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "lsft_cw_kg" and spec_lsft_cw_kg is not None:
                                        if current_val > spec_lsft_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "lsft_ccw_kg" and spec_lsft_ccw_kg is not None:
                                        if current_val > spec_lsft_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "lfh1_cw_kg" and spec_lfh1_cw_kg is not None:
                                        if current_val > spec_lfh1_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "lfh1_ccw_kg" and spec_lfh1_ccw_kg is not None:
                                        if current_val > spec_lfh1_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "plysteer_kg" and spec_plysteer_kg is not None:
                                        if current_val > spec_plysteer_kg:
                                            shift_occurred_spec = True
                                    if param == "rfmxa_ccw_deg" and spec_rfmxa_ccw_deg is not None:
                                        if current_val > spec_rfmxa_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "rfa1_cw_deg" and spec_rfa1_cw_deg is not None:
                                        if current_val > spec_rfa1_cw_deg:
                                            shift_occurred_spec = True
                                    if param == "rfa1_ccw_deg" and spec_rfa1_ccw_deg is not None:
                                        if current_val > spec_rfa1_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "rfa2_cw_deg" and spec_rfa2_cw_deg is not None:
                                        if current_val > spec_rfa2_cw_deg:
                                            shift_occurred_spec = True
                                    if param == "rfa2_ccw_deg" and spec_rfa2_ccw_deg is not None:
                                        if current_val > spec_rfa2_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "lfmxa_cw_deg" and spec_lfmxa_cw_deg is not None:
                                        if current_val > spec_lfmxa_cw_deg:
                                            shift_occurred_spec = True
                                    if param == "lfmxa_ccw_deg" and spec_lfmxa_ccw_deg is not None:
                                        if current_val > spec_lfmxa_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "lfa1_cw_deg" and spec_lfa1_cw_deg is not None:
                                        if current_val > spec_lfa1_cw_deg:
                                            shift_occurred_spec = True
                                    if param == "lfa1_ccw_deg" and spec_lfa1_ccw_deg is not None:
                                        if current_val > spec_lfa1_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "static_g" and spec_static_g is not None:
                                        if current_val > spec_static_g:
                                            shift_occurred_spec = True
                                    if param == "static_deg" and spec_static_deg is not None:
                                        if current_val > spec_static_deg:
                                            shift_occurred_spec = True
                                    if param == "couple_g" and spec_couple_deg is not None:
                                        if current_val > spec_couple_deg:
                                            shift_occurred_spec = True
                                    if param == "couple_deg" and spec_couple_deg is not None:
                                        if current_val > spec_couple_deg:
                                            shift_occurred_spec = True
                                    if param == "botbul_mm" and spec_bul_botbul_mm is not None:
                                        if current_val > spec_bul_botbul_mm:
                                            shift_occurred_spec = True
                                    if param == "topbul_mm" and spec_bul_topbul_mm is not None:
                                        if current_val > spec_bul_topbul_mm:
                                            shift_occurred_spec = True
                                    if param == "botbul_deg" and spec_bul_botbul_deg is not None:
                                        if current_val > spec_bul_botbul_deg:
                                            shift_occurred_spec = True
                                    if param == "topdep_deg" and spec_bul_topdep_deg is not None:
                                        if current_val > spec_bul_topdep_deg:
                                            shift_occurred_spec = True
                                    if param == "botdep_mm" and spec_dep_botdep_mm is not None:
                                        if current_val > spec_dep_botdep_mm:
                                            shift_occurred_spec = True
                                    if param == "topdep_mm" and spec_dep_topdep_mm is not None:
                                        if current_val > spec_dep_topdep_mm:
                                            shift_occurred_spec = True
                                    if param == "botdep_deg" and spec_dep_botdep_deg is not None:
                                        if current_val > spec_dep_botdep_deg:
                                            shift_occurred_spec = True
                                    if param == "topbul_deg" and spec_dep_topbul_deg is not None:
                                        if current_val > spec_dep_topbul_deg:
                                            shift_occurred_spec = True
                                    if param == "crro_mm" and spec_rro_crro_mm is not None:
                                        if current_val > spec_rro_crro_mm:
                                            shift_occurred_spec = True
                                    if param == "crroh1_mm" and spec_rro_crroh1_mm is not None:
                                        if current_val > spec_rro_crroh1_mm:
                                            shift_occurred_spec = True
                                    if param == "blro_mm" and spec_lro_blro_mm is not None:
                                        if current_val > spec_lro_blro_mm:
                                            shift_occurred_spec = True
                                    if param == "blroh1_mm" and spec_lro_blroh1_mm is not None:
                                        if current_val > spec_lro_blroh1_mm:
                                            shift_occurred_spec = True
                                    if param == "tlro_mm" and spec_lro_tlro_mm is not None:
                                        if current_val > spec_lro_tlro_mm:
                                            shift_occurred_spec = True
                                    if param == "tlroh1_mm" and spec_lro_tlroh1_mm is not None:
                                        if current_val > spec_lro_tlroh1_mm:
                                            shift_occurred_spec = True
                                    if param == "crro_deg" and spec_rro_crro_deg is not None:
                                        if current_val > spec_rro_crro_deg:
                                            shift_occurred_spec = True
                                    if param == "crroh1_deg" and spec_rro_crroh1_deg is not None:
                                        if current_val > spec_rro_crroh1_deg:
                                            shift_occurred_spec = True
                                    if param == "blro_deg" and spec_lro_blro_deg is not None:
                                        if current_val > spec_lro_blro_deg:
                                            shift_occurred_spec = True
                                    if param == "blroh1_deg" and spec_lro_blroh1_deg is not None:
                                        if current_val > spec_lro_blroh1_deg:
                                            shift_occurred_spec = True
                                    if param == "tlro_deg" and spec_lro_tlro_deg is not None:
                                        if current_val > spec_lro_tlro_deg:
                                            shift_occurred_spec = True
                                    if param == "tlroh1_deg" and spec_lro_tlroh1_deg is not None:
                                        if current_val > spec_lro_tlroh1_deg:
                                            shift_occurred_spec = True
                                    if param == "botplane_g" and spec_dynamic_botplane_g is not None:
                                        if current_val > spec_dynamic_botplane_g:
                                            shift_occurred_spec = True
                                    if param == "topplane_g" and spec_dynamic_topplane_g is not None:
                                        if current_val > spec_dynamic_topplane_g:
                                            shift_occurred_spec = True
                                    if param == "botplane_deg" and spec_dynamic_botplane_deg is not None:
                                        if current_val > spec_dynamic_botplane_deg:
                                            shift_occurred_spec = True
                                    if param == "topplane_deg" and spec_dynamic_topplane_deg is not None:
                                        if current_val > spec_dynamic_topplane_deg:
                                            shift_occurred_spec = True

                                    if param in yield_params:
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if current_val < spec_yield:
                                                shift_occurred_spec = True
                                
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
                                parameter_alerts.append(alert_dict)
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
                                "gt_code": model_number,
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
                            # logging.info(f"Prepared {len(deviated_parameter_alerts)} derived alerts for machine {machine_no}, lot {unique_lot_id_current}")

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

                        batch_data["tbm_shift"] = batch_data["date_of_tbm"].apply(get_shift)
                        shift_groups = batch_data.groupby('tbm_shift')
                        shifts_data_list = []
                        for shift_name, shift_data in shift_groups:
                            shift_start_time = shift_data["date_of_tbm"].iloc[0]
                            shift_end_time = shift_data["date_of_tbm"].iloc[-1]
                            shifts_data_list.append({
                                "shift": shift_name,
                                "barcode_count": len(shift_data["barcode"]) if "barcode" in shift_data.columns else 0,
                                "machine_shift_tbm_start_time": shift_start_time,
                                "machine_shift_tbm_end_time": shift_end_time,
                                "barcodes": shift_data["barcode"].tolist() if "barcode" in shift_data.columns else []
                            })

                        any_shift_flag = any(alert["shift_occurred"] for alert in parameter_alerts)

                        averages_doc = {
                            "gt_code": model_number,
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
                            "target_machine_lot_size": batch_size,
                            "balance_machine_barcodes": int(balance_machine_barcodes),
                            "model_processed_barcodes": int(batch_size),
                            "model_target_lot_size": batch_size,
                            "model_balance_barcodes": int(balance_barcodes),
                            "model_total_barcodes": int(total_barcodes_model),
                            "batch_indices": list(batch_indices),
                            "timestamp_alert_generated": some_function()
                        }
                        averages_query = {
                            "gt_code": model_number,
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
                            "oe_yield": oe_yield(batch_data),
                            "balancing_oe_yield": balancing_oe_yield(batch_data),
                            "geometry_oe_yield": geometry_oe_yield(batch_data),
                            "warehouse_yield": warehouse_yield(batch_data),
                            "visual_yield": visual_yield(batch_data),
                            "sizecode": sizecode,
                            "gt_code": str(gt_code),    # Add 16-07
                            "timestamp_lot_started": lot_start_time,
                            "timestamp_lot_ended": lot_end_time,
                            "target_machine_lot_size": batch_size,
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
                                "gt_code": model_number,
                                "lot_id": unique_lot_id_current,
                                "first_barcode_model": None,
                                "last_barcode_model": None,
                                "shift_occurred": False,
                                "machine_alerts": []
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
                        "gt_code": alert_obj["gt_code"],
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

                for alert_key, alert_obj in alert_map.items():
                    true_machine_alerts = [ma for ma in alert_obj["machine_alerts"] if ma.get("tbm_shift_occurred", False)]
                    # logging.info("Model %s, Lot %s: %s true machine alerts for triggering" % (alert_obj['gt_code'], alert_obj['lot_id'], len(true_machine_alerts)))
                    if true_machine_alerts:
                        doc_trigger = {
                            "alert_id_tbm_ufm": alert_obj["alert_id_tbm_ufm"],
                            "gt_code": alert_obj["gt_code"],
                            "lot_id": alert_obj["lot_id"],
                            "alert_flag": False,
                            "dashboard_flag": False,
                            "alert_status_check": "o",
                            "alert_status_timestamp": None,
                            "alert_status_comment": [],
                            "alert_assign_user": [],  # Add 05-07
                            "machine_alerts": true_machine_alerts,
                            "first_barcode_model": alert_obj["first_barcode_model"],
                            "last_barcode_model": alert_obj["last_barcode_model"],
                            "timestamp_alert_generated": some_function()
                        }
                        triggering_bulk_operations.append(pymongo.InsertOne(doc_trigger))

        except Exception as e:
            logging.error(f"Error processing TBM model {model_number}: {e}")
            continue

    # Execute bulk operations
    if averages_bulk_operations:
        try:
            result = averages_collect.bulk_write(averages_bulk_operations, ordered=False)
            inserted_counts["averages_tbm_table_new"] = result.inserted_count
            output_messages.append(f"✅ TBM Inserted {result.inserted_count} averages documents into averages_tbm_table_new.")
        except Exception as e:
            error_message = f"❌ TBM Error performing bulk write to averages_tbm_table_new: {e}"
            output_messages.append(error_message)
    else:
        output_messages.append("No TBM averages documents to insert.")

    if alert_bulk_operations:
        try:
            result = alert_collect.bulk_write(alert_bulk_operations, ordered=False)
            inserted_counts["alert_tbm_table_without_lot_size"] = result.inserted_count
            output_messages.append(f"✅ TBM Inserted {result.inserted_count} alert documents into alert_tbm_table_without_lot_size.")
        except Exception as e:
            error_message = f"❌ TBM Error performing bulk write to alert_tbm_table_without_lot_size: {e}"
            output_messages.append(error_message)
    else:
        output_messages.append("No TBM alert documents to insert.")

    if triggering_bulk_operations:
        try:
            # Write to alert_db_virgin_yield
            result = alert_triggering_collect.bulk_write(triggering_bulk_operations, ordered=False)
            inserted_counts["alert_triggering_tbm_without_lot_size"] = result.inserted_count
            output_messages.append(f"✅ TBM Inserted {result.inserted_count} machine_alert documents into alert_db_virgin_yield.alert_triggering_tbm_without_lot_size.")
            # NEW: Write to alert_db_final_yield
            result_dashboard = alert_triggering_collect_dashboard.bulk_write(triggering_bulk_operations, ordered=False)
            inserted_counts["alert_triggering_tbm_dashboard"] = result_dashboard.inserted_count
            output_messages.append(f"✅ TBM Inserted {result_dashboard.inserted_count} machine_alert documents into alert_db_final_yield.alert_triggering_tbm_without_lot_size.")
        except Exception as e:
            error_message = f"❌ TBM Error performing bulk write to alert_triggering collections: {e}"
            output_messages.append(error_message)
    else:
        output_messages.append("No TBM machine_alert documents to insert into alert_triggering collections.")

    if deviated_triggering_bulk_operations:
        try:
            # Write to alert_deviated_final_yield
            result = alert_triggering_deviated_collect.bulk_write(deviated_triggering_bulk_operations, ordered=False)
            inserted_counts["alert_tbm_without_lot_size"] = result.inserted_count
            output_messages.append(f"✅ TBM Inserted {result.inserted_count} deviated alert documents into alert_deviated_final_yield.alert_tbm_without_lot_size.")
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
            updated_counts["data_merged_combined_uniform_balan_geo"] = result.modified_count
            output_messages.append(f"✅ TBM Updated {result.modified_count} documents in data_mereged_dashboard.")
        except Exception as e:
            error_message = f"❌ TBM Error updating data_merged_combined_uniform_balan_geo: {e}"
            output_messages.append(error_message)
    else:
        output_messages.append("No TBM documents to update in data_merged_combined_uniform_balan_geo.")

    end_time = time.time()
    execution_time = end_time - start_time
    final_message = f"✅ TBM Lot and Alert Process completed successfully in {execution_time:.2f} seconds!"
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

def process_curing_data_and_generate_lots_alerts_without_lot_size():
    start_time = time.time()
    output_messages = []
    # WITHOUT lot size: assign a lot id to ALL barcodes for every curing press present
    # in the data (do not restrict to active presses). Ensures no barcode is left with
    # a null curing_machine_model_lot_id in new_data_merged_combined_uniform_balan_geo.
    _restrict_active_presses = False
    inserted_counts = {}
    updated_counts = {}
    logging.info("process_curing_data_and_generate_lots_alerts_without_lot_size function is just started")

    try:
        merge_collect = client[UNIFORMITY_BALANCING_GEOMETRY_DB][new_COMBINED_COLLECTION_NAME]
        channel_collect_curing = client[config_databse][channel_no_collect_curing]
        channel_collect_ufm = client[config_databse][channel_no_collect]
        lot_curing_collect = client[lot_db][lot_curing_collect_name_new]
        alert_collect = client[alert_table_final_yield][alert_curing_collect_name_new]
        averages_collect = client[average_db][averages_curing_table_name_new]
        machine_collect = client[config_databse][machine_no_collect]
        curing_press_collect = client[config_databse][curing_press_dict_collect]
        alert_triggering_collect = client[alert_db_virgin_yield][alert_triggering_curing_collect_name_new]
        alert_triggering_deviated_collect = client[alert_deviated_final_yield][alert_triggering_deviated_curing_new]
        alert_triggering_collect_dashboard_curing = client[alert_db_final_yield][alert_triggering_curing_collect_name_new]
        curing_collect_data = client[model_db][curing_collection]
        curing_gt_code_collect = client[model_db][curing_collection_machine]
        output_messages.append("✅ TBM Collections loaded successfully for lot and alert processing in both alert_db_virgin_yield and alert_db_final_yield, and model_workcenter_db.")
    except pymongo.errors.OperationFailure as e:
        error_message = f"❌ Curing Database or collection error: {e}"
        output_messages.append(error_message)
        return {"status": "error", "message": error_message, "output_log": output_messages}, 500

    try:
        active_curing_press_ids = set(curing_gt_code_collect.distinct("curing_press", {"status": "active"}))
        output_messages.append(f"✅ Fetched {len(active_curing_press_ids)} active curing_press values: {active_curing_press_ids}")
    except Exception as e:
        error_message = f"❌ Error fetching active curing_press from curing_modelno: {e}"
        output_messages.append(error_message)
        return {"status": "error", "message": "Failed to fetch active curing_press", "output_log": output_messages}, 500

    # Fetch active gt_code numbers
    try:
        # Step 1: Fetch distinct active gt_code (flatten from list fields)
        active_gtcodes = set()
        cursor = curing_collect_data.find({"status": "active"}, {"gt_code": 1})
        for doc in cursor:
            active_gtcodes.update(doc.get("gt_code", []))  # flatten the gt_code list
        output_messages.append(f"✅ Fetched {len(active_gtcodes)} active model numbers: {active_gtcodes}")
    except Exception as e:
        output_messages.append(f"❌ Error fetching active model numbers: {e}")
        return {"status": "error", "message": "Failed to fetch active model numbers", "output_log": output_messages}, 500

    try:
        merge1 = list(merge_collect.find({}))
        merge_df = pd.DataFrame(merge1)
        channel_data_curing = list(channel_collect_curing.find({"gt_code": {"$in": list(active_gtcodes)}}))
        channel_data_curing = list(channel_collect_curing.find({}))
        channel_df = pd.DataFrame(channel_data_curing)
        channel_data_ufm = list(channel_collect_ufm.find({}))
        channel_df_ufm = pd.DataFrame(channel_data_ufm)
        machine_data = list(machine_collect.find({}))
        machine_df = pd.DataFrame(machine_data)
        curing_press_data = list(curing_press_collect.find({"curing_press": {"$in": list(active_curing_press_ids)}}))
        curing_press_data = list(curing_press_collect.find({}))
        curing_df = pd.DataFrame(curing_press_data)
        output_messages.append("✅ Curing Data loaded from collections into DataFrames.")
    except Exception as e:
        error_message = f"❌ Curing Error loading data: {e}"
        output_messages.append(error_message)
        return {"status": "error", "message": error_message, "output_log": output_messages}, 500

    # Get distinct model numbers
    gt_code_lists = channel_collect_curing.distinct("gt_code")
    if merge_df.empty or "gt_code" not in merge_df.columns:
        output_messages.append("merge_df is empty or missing 'gt_code'. Skipping Curing processing.")
        return {"status": "success", "message": "No Curing data to process.", "output_log": output_messages}, 200
    if "alert_id_curing_ufm" not in merge_df.columns:
        merge_df["alert_id_curing_ufm"] = None
    merge_df = merge_df[merge_df["gt_code"].isin(gt_code_lists)]
    merge_df = merge_df[merge_df["alert_id_curing_ufm"].isna()]
    if merge_df.empty:
        output_messages.append("No Curing data after filtering. Skipping.")
        return {"status": "success", "message": "No Curing data after filtering.", "output_log": output_messages}, 200

    # merge_df["date_of_tbm"] = merge_df["tbm_buildtime"]
    merge_df["date_of_curing"] = merge_df["curing_infeedtime"]

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

    try:
        merge_df["date_of_ufm"] = convert_datetime_with_invalid_mask(merge_df["date_of_ufm"], "date_of_ufm")
        merge_df["date_of_ufm"] = merge_df["date_of_ufm"].astype(object).where(merge_df["date_of_ufm"].notnull(), "No Date")
        # merge_df["date_of_tbm"] = convert_datetime_with_invalid_mask(merge_df["date_of_tbm"], "date_of_tbm")
        # merge_df["date_of_tbm"] = merge_df["date_of_tbm"].astype(object).where(merge_df["date_of_tbm"].notnull(), "No Date")
        merge_df["date_of_curing"] = convert_datetime_with_invalid_mask(merge_df["date_of_curing"], "date_of_curing")
        merge_df["date_of_curing"] = merge_df["date_of_curing"].astype(object).where(merge_df["date_of_curing"].notnull(), "No Date")
        # merge_df["time_of_ufm"] = pd.to_datetime(merge_df["time"], errors="coerce").dt.strftime("%H:%M:%S")
        # time_invalid_mask = merge_df["time_of_ufm"].isna() & merge_df["time"].notna()
        # if time_invalid_mask.any():
        #     logging.info("⚠️ Found %s invalid time values", time_invalid_mask.sum())
    except Exception as e:
        logging.info("❌ Error during datetime conversion: %s", e)
        output_messages.append(f"❌ Error during datetime conversion: {e}")
        return {"status": "error", "message": f"Error during datetime conversion: {e}", "output_log": output_messages}, 500

    # merge_df["tbm_shift"] = merge_df["date_of_tbm"].apply(get_shift)
    merge_df["curing_shift"] = merge_df["date_of_curing"].apply(get_shift)

    try:
        last_alert = alert_collect.find_one(sort=[("alert_id_curing_ufm", -1)])
        last_alert_id = int(last_alert["alert_id_curing_ufm"]) if last_alert and "alert_id_curing_ufm" in last_alert else 0
    except Exception as e:
        error_message = f"❌ Curing Error fetching last alert ID: {e}"
        output_messages.append(error_message)
        last_alert_id = 0

    try:
        last_alert = alert_collect.find_one(sort=[("alert_id_tbm_ufm", -1)])
        last_alert_id = int(last_alert["alert_id_tbm_ufm"]) if last_alert and "alert_id_tbm_ufm" in last_alert else 0
    except Exception as e:
        error_message = f"❌ Curing Error fetching last alert ID for lot/alert processing: {e}"
        output_messages.append(error_message)
        last_alert_id = 0

    try:
        last_lots = list(lot_curing_collect.aggregate([
            {"$match": {"lot_id": {"$regex": "^Curing_[^_]+-[^_]*_[^_]+_\\d+$"},
                        "gt_code": {"$in": list(active_gtcodes)}}},
            {"$addFields": {
                "lot_number": {"$toInt": {"$arrayElemAt": [{"$split": ["$lot_id", "_"]}, -1]}},
                "gt_code": "$gt_code",
                "machine": "$machine"
            }},
            {"$sort": {"lot_number": -1}},
            {"$group": {
                "_id": {"gt_code": "$gt_code", "machine": "$machine"},
                "max_lot_number": {"$first": "$lot_number"},
                "last_lot_id": {"$first": "$lot_id"}
            }}
        ]))
        last_lot_dict = {
            (item["_id"]["gt_code"], item["_id"]["machine"]): {
                "max_lot_number": item["max_lot_number"],
                "last_lot_id": item["last_lot_id"]
            } for item in last_lots
        }
        logging.info(f"ℹ️ Fetched last lot numbers: {len(last_lot_dict)} entries")
        output_messages.append(f"ℹ️ Fetched last lot numbers: {len(last_lot_dict)} entries")
    except Exception as e:
        error_message = f"❌ Curing Error fetching last lot IDs: {e}"
        logging.error(error_message)
        output_messages.append(error_message)
        last_lot_dict = {}
    
    # Prepare merge_df
    merge_df["curing_press"] = merge_df["curing_press"].astype(str)
    curing_df["curing_press"] = curing_df["curing_press"].astype(str)  # Changed from modelnumber to gt_code is handled as array later
    # Filter merge_df to include only active curing presses (only when restricting).
    if _restrict_active_presses:
        merge_df = merge_df[merge_df["curing_press"].isin(active_curing_press_ids)]
    if merge_df.empty:
        output_messages.append("❌ No data in merge_df after filtering for active machines. Exiting.")
        return {"status": "error", "message": "No data after active tbm filtering", "output_log": output_messages}, 500

    # Ensure lot-tracking columns are object dtype so string lot IDs can be assigned
    # (they may pre-exist as float64/NaN, which raises "Invalid value ... for dtype 'float64'")
    for _lot_col in ["lot_id", "previous_lot_id", "curing_machine_model_lot_id", "curing_previous_machine_model_lot_id", "alert_id_curing_ufm"]:
        if _lot_col not in merge_df.columns:
            merge_df[_lot_col] = None
        merge_df[_lot_col] = merge_df[_lot_col].astype(object)

    curing_previous_machine_model_lot_ids = {}
    machine_lot_avg_map = {}
    lot_bulk_operations = []
    alert_bulk_operations = []
    averages_bulk_operations = []
    deviated_triggering_bulk_operations = []
    triggering_bulk_operations = []
    bulk_operations = []

    try:
        last_averages = list(averages_collect.aggregate([
            {"$match": {"curing_machine_model_lot_id": {"$regex": "^[^_]+-[^_]*_[^_]+_Curing_[^_]+-[^_]*_[^_]+_\\d+_\\d+$"},
                        "gt_code": {"$in": list(active_gtcodes)}}},
            {"$sort": {"curing_machine_model_lot_id": -1}},
            {"$group": {
                "_id": {"gt_code": "$gt_code", "machine": "$machine"},
                "last_curing_lot_id": {"$first": "$curing_machine_model_lot_id"},
                "last_averages": {"$first": "$current_lot_averages"}
            }}
        ]))
        for item in last_averages:
            gt_code = item["_id"]["gt_code"]
            machine = item["_id"]["machine"]
            curing_lot_id = item["last_curing_lot_id"]
            averages = item["last_averages"]
            curing_previous_machine_model_lot_ids[(gt_code, machine)] = curing_lot_id
            if averages:
                machine_lot_avg_map[(gt_code, machine, curing_lot_id)] = averages
                logging.info(f"Pre-populated machine_lot_avg_map with averages for {curing_lot_id}")
            else:
                logging.warning(f"No averages found for {curing_lot_id} in averages_collect")
        logging.info(f"Initialized curing_previous_machine_model_lot_ids with {len(curing_previous_machine_model_lot_ids)} entries")
        output_messages.append(f"Initialized curing_previous_machine_model_lot_ids with {len(curing_previous_machine_model_lot_ids)} entries")
    except Exception as e:
        logging.error(f"❌ Error pre-populating machine_lot_avg_map and curing_previous_machine_model_lot_ids: {e}")
        output_messages.append(f"❌ Error pre-populating machine_lot_avg_map and curing_previous_machine_model_lot_ids: {e}")

    conicity_params = [
        'rfpp_cw_kg','rfpp_ccw_kg','rfh1_cw_kg','rfh1_ccw_kg','rfh2_cw_kg','rfh2_ccw_kg','lfpp_cw_kg','lfpp_ccw_kg',
        'lsft_cw_kg','lsft_ccw_kg','lfh1_cw_kg','lfh1_ccw_kg','plysteer_kg','cony_kg','rfmxa_ccw_deg',
        'rfa1_cw_deg','rfa1_ccw_deg','rfa2_cw_deg','rfa2_ccw_deg','lfmxa_cw_deg','lfmxa_ccw_deg','lfa1_cw_deg',
        'lfa1_ccw_deg','botplane_g','topplane_g','couple_g','static_g','botplane_deg','topplane_deg','couple_deg',
        'static_deg','botbul_mm','botdep_mm','topbul_mm','topdep_mm','blro_mm','blroh1_mm','crro_mm','crroh1_mm',
        'tlro_mm','tlroh1_mm','botbul_deg','botdep_deg','topbul_deg','topdep_deg','blro_deg','blroh1_deg','crro_deg',
        'crroh1_deg','tlro_deg','tlroh1_deg'
        ]

    yield_params = [
        "rfv_yield", "rfh1_yield", "rfh2_yield", "lfv_yield", "lsft_yield", "lfh1_yield", "conicity_yield", "oe_yield",
        "rfmxa_yield", "rfa1_yield", "rfa2_yield", "lfxma_yield", "lfa1_yield", "central_radial_runout_yield", "central_radial_runout_first_harmonic_yield",
        "lateral_run_out_top_over_yield", "lateral_run_out_bottom_over_yield", "lateral_run_out_yield", "lateral_run_out_bottom_first_harmonic_yield", "lateral_run_out_top_first_harmonic_yield", 
        "lateral_run_out_harmonic_yield", "bulge_top_over_yield", "bulge_bottom_over_yield", "bulge_yield", "dent_top_over_yield", "dent_bottom_over_yield", "dent_yield", "geometry_oe_yield",
        "lower_yield", "upper_yield", "static_yield", "couple_yield", "balancing_oe_yield", "warehouse_yield", "visual_yield"
        ]

    # Define rank parameter mappings
    rank_param_mappings = {
        "rfv_yield": ["tu_rfpp_cw_grade", "tu_rfpp_ccw_grade"],
        "rfh1_yield": ["tu_rfh1_cw_grade", "tu_rfh1_ccw_grade"], 
        "rfh2_yield": ["tu_rfh2_cw_grade", "tu_rfh2_ccw_grade"],
        "lfv_yield": ["tu_lfpp_cw_grade", "tu_lfpp_ccw_grade"],
        "lsft_yield": ["tu_lsft_cw_grade", "tu_lsft_ccw_grade"],
        "lfh1_yield": ["tu_lfh1_cw_grad", "tu_lfh1_ccw_grad"],
        "conicity_yield": ["tu_cony_grade"],
        "oe_yield": ["tu_class"],
        "rfmxa_yield": ["tu_rfmxa_ccw_grade"],
        "rfa1_yield": ["tu_rfa1_cw_grade", "tu_rfa1_ccw_grade"],
        "rfa2_yield": ["tu_rfa2_cw_grade", "tu_rfa2_ccw_grade"],
        "lfxma_yield": ["tu_lfmxa_cw_grade", "tu_lfmxa_ccw_grade"],
        "lfa1_yield": ["tu_lfa1_cw_grade", "tu_lfa1_ccw_grade"],
        "central_radial_runout_yield": ["tg_crro_grade"],
        "central_radial_runout_first_harmonic_yield": ["tg_crroh1_grade"],
        "lateral_run_out_top_over_yield": ["tg_tlro_grade"],
        "lateral_run_out_bottom_over_yield": ["tg_blro_grade"],
        "lateral_run_out_yield": ["tg_tlro_grade", "tg_blro_grade"],
        "lateral_run_out_bottom_first_harmonic_yield": ["tg_blroh1_grade"],
        "lateral_run_out_top_first_harmonic_yield": ["tg_tlroh1_grade"],
        "lateral_run_out_harmonic_yield": ["tg_tlroh1_grade", "tg_blroh1_grade"],
        "bulge_top_over_yield": ["tg_topbul_grade"],
        "bulge_bottom_over_yield": ["tg_botbul_grade"],
        "bulge_yield": ["tg_botbul_grade", "tg_topbul_grade"],
        "dent_top_over_yield": ["tg_topdep_grade"],
        "dent_bottom_over_yield": ["tg_botdep_grade"],
        "dent_yield": ["tg_topdep_grade", "tg_botdep_grade"],
        "geometry_oe_yield": ["tg_class"],
        "lower_yield": ["tb_botplane_grade"],
        "upper_yield": ["tb_topplane_grade"],
        "static_yield": ["tb_static_grade"],
        "couple_yield": ["tb_couple_grade"],
        "balancing_oe_yield": ["tb_class"]
        }

    for _, row in channel_df.iterrows():
        try:
            # model_number = str(row.get("modelno", "")).strip()
            gt_code_list = row.get("gt_code", [])
            model_number = gt_code_list[0].strip() if isinstance(gt_code_list, list) and gt_code_list else ""
            if model_number not in active_gtcodes:
                output_messages.append(f"Skipping CURING model {model_number} as it is not active.")
                continue
            gt_codes = row.get("gt_code", [])

            if not isinstance(gt_codes, list):
                gt_codes = [gt_codes]

            # spec_con = float(row.get("spec_con", 0))
            sizecode = str(row.get("sizecode", ""))
            spec_rfpp_cw_kg = float(row.get("spec_rfpp_cw_kg", 0))
            spec_rfpp_ccw_kg = float(row.get("spec_rfpp_ccw_kg", 0))
            spec_rfh1_cw_kg = float(row.get("spec_rfh1_cw_kg", 0))
            spec_rfh1_ccw_kg = float(row.get("spec_rfh1_ccw_kg", 0))
            spec_rfh2_cw_kg = float(row.get("spec_rfh2_cw_kg", 0))
            spec_rfh2_ccw_kg = float(row.get("spec_rfh2_ccw_kg", 0))
            spec_lfpp_cw_kg = float(row.get("spec_lfpp_cw_kg", 0))
            spec_lfpp_ccw_kg = float(row.get("spec_lfpp_ccw_kg", 0))
            spec_lsft_cw_kg = float(row.get("spec_lsft_cw_kg", 0))
            spec_lsft_ccw_kg = float(row.get("spec_lsft_ccw_kg", 0))
            spec_lfh1_cw_kg = float(row.get("spec_lfh1_cw_kg", 0))
            spec_lfh1_ccw_kg = float(row.get("spec_lfh1_ccw_kg", 0))
            spec_plysteer_kg = float(row.get("spec_plysteer_kg", 0))
            spec_rfmxa_ccw_deg = float(row.get("spec_rfmxa_ccw_deg", 0))
            spec_rfa1_cw_deg = float(row.get("spec_rfa1_cw_deg", 0))
            spec_rfa1_ccw_deg = float(row.get("spec_rfa1_ccw_deg", 0))
            spec_rfa2_cw_deg = float(row.get("spec_rfa2_cw_deg", 0))
            spec_rfa2_ccw_deg = float(row.get("spec_rfa2_ccw_deg", 0))
            spec_lfmxa_cw_deg = float(row.get("spec_lfmxa_cw_deg", 0))
            spec_lfmxa_ccw_deg = float(row.get("spec_lfmxa_ccw_deg", 0))
            spec_lfa1_cw_deg = float(row.get("spec_lfa1_cw_deg", 0))
            spec_lfa1_ccw_deg = float(row.get("spec_lfa1_ccw_deg", 0))
            spec_cony_kg = float(row.get("spec_cony_kg", 0))
            spec_static_g = float(row.get("spec_static_g", 0))
            spec_static_deg = float(row.get("spec_static_deg", 0))
            spec_couple_g = float(row.get("spec_couple_g", 0))
            spec_couple_deg = float(row.get("spec_couple_deg", 0))
            spec_bul_botbul_mm = float(row.get("spec_bul_botbul_mm", 0))
            spec_bul_topbul_mm = float(row.get("spec_bul_topbul_mm", 0))
            spec_bul_botbul_deg = float(row.get("spec_bul_botbul_deg", 0))
            spec_bul_topdep_deg = float(row.get("spec_bul_topdep_deg", 0))
            spec_dep_botdep_mm = float(row.get("spec_dep_botdep_mm", 0))
            spec_dep_topdep_mm = float(row.get("spec_dep_topdep_mm", 0))
            spec_dep_botdep_deg = float(row.get("spec_dep_botdep_deg", 0))
            spec_dep_topbul_deg = float(row.get("spec_dep_topbul_deg", 0))
            spec_rro_crro_mm = float(row.get("spec_rro_crro_mm", 0))
            spec_rro_crroh1_mm = float(row.get("spec_rro_crroh1_mm", 0))
            spec_lro_blro_mm = float(row.get("spec_lro_blro_mm", 0))
            spec_lro_blroh1_mm = float(row.get("spec_lro_blroh1_mm", 0))
            spec_lro_tlro_mm = float(row.get("spec_lro_tlro_mm", 0))
            spec_lro_tlroh1_mm = float(row.get("spec_lro_tlroh1_mm", 0))
            spec_rro_crro_deg = float(row.get("spec_rro_crro_deg", 0))
            spec_rro_crroh1_deg = float(row.get("spec_rro_crroh1_deg", 0))
            spec_lro_blro_deg = float(row.get("spec_lro_blro_deg", 0))
            spec_lro_blroh1_deg = float(row.get("spec_lro_blroh1_deg", 0))
            spec_lro_tlro_deg = float(row.get("spec_lro_tlro_deg", 0))
            spec_lro_tlroh1_deg = float(row.get("spec_lro_tlroh1_deg", 0))
            spec_dynamic_botplane_g = float(row.get("spec_dynamic_botplane_g", 0))
            spec_dynamic_topplane_g = float(row.get("spec_dynamic_topplane_g", 0))
            spec_dynamic_botplane_deg = float(row.get("spec_dynamic_botplane_deg", 0))
            spec_dynamic_topplane_deg = float(row.get("spec_dynamic_topplane_deg", 0))

            rfv_yield_spec_yield = float(row.get("rfv_yield_spec_yield", 0))
            rfh1_yield_spec_yield = float(row.get("rfh1_yield_spec_yield", 0))
            rfh2_yield_spec_yield = float(row.get("rfh2_yield_spec_yield", 0))
            lfv_yield_spec_yield = float(row.get("lfv_yield_spec_yield", 0))
            lsft_yield_spec_yield = float(row.get("lsft_yield_spec_yield", 0))
            lfh1_yield_spec_yield = float(row.get("lfh1_yield_spec_yield", 0))
            conicity_yield_spec_yield = float(row.get("conicity_yield_spec_yield", 0))
            oe_yield_spec_yield = float(row.get("oe_yield_spec_yield", 0))
            rfmxa_yield_spec_yield = float(row.get("rfmxa_yield_spec_yield", 0))
            rfa1_yield_spec_yield = float(row.get("rfa1_yield_spec_yield", 0))
            rfa2_yield_spec_yield = float(row.get("rfa2_yield_spec_yield", 0))
            lfxma_yield_spec_yield = float(row.get("lfxma_yield_spec_yield", 0))
            lfa1_yield_spec_yield = float(row.get("lfa1_yield_spec_yield", 0))
            central_radial_runout_yield_spec_yield = float(row.get("central_radial_runout_yield_spec_yield", 0))
            central_radial_runout_first_harmonic_yield_spec_yield = float(row.get("central_radial_runout_first_harmonic_yield_spec_yield", 0))
            lateral_run_out_top_over_yield_spec_yield = float(row.get("lateral_run_out_top_over_yield_spec_yield", 0))
            lateral_run_out_bottom_over_yield_spec_yield = float(row.get("lateral_run_out_bottom_over_yield_spec_yield", 0))
            lateral_run_out_yield_spec_yield = float(row.get("lateral_run_out_yield_spec_yield", 0))
            lateral_run_out_bottom_first_harmonic_yield_spec_yield = float(row.get("lateral_run_out_bottom_first_harmonic_yield_spec_yield", 0))
            lateral_run_out_top_first_harmonic_yield_spec_yield = float(row.get("lateral_run_out_top_first_harmonic_yield_spec_yield", 0))
            lateral_run_out_harmonic_yield_spec_yield = float(row.get("lateral_run_out_harmonic_yield_spec_yield", 0))
            bulge_top_over_yield_spec_yield = float(row.get("bulge_top_over_yield_spec_yield", 0))
            bulge_bottom_over_yield_spec_yield = float(row.get("bulge_bottom_over_yield_spec_yield", 0))
            bulge_yield_spec_yield = float(row.get("bulge_yield_spec_yield", 0))
            dent_top_over_yield_spec_yield = float(row.get("dent_top_over_yield_spec_yield", 0))
            dent_bottom_over_yield_spec_yield = float(row.get("dent_bottom_over_yield_spec_yield", 0))
            dent_yield_spec_yield = float(row.get("dent_yield_spec_yield", 0))
            geometry_oe_yield_spec_yield = float(row.get("geometry_oe_yield_spec_yield", 0))
            lower_yield_spec_yield = float(row.get("lower_yield_spec_yield", 0))
            upper_yield_spec_yield = float(row.get("upper_yield_spec_yield", 0))
            static_yield_spec_yield = float(row.get("static_yield_spec_yield", 0))
            couple_yield_spec_yield = float(row.get("couple_yield_spec_yield", 0))
            balancing_oe_yield_spec_yield = float(row.get("balancing_oe_yield_spec_yield", 0))
            warehouse_yield_spec_yield = float(row.get("warehouse_yield_spec_yield", 0))
            visual_yield_spec_yield = float(row.get("visual_yield_spec_yield", 0))
            
            class_spec_yield = float(row.get("class_spec_yield", 0))
            balancing_spec_yield = float(row.get("balancing_spec_yield", 0))
            geometry_spec_yield = float(row.get("geometry_spec_yield", 0))
            
            threshold_value_rfpp_cw_kg = float(row.get("threshold_value_rfpp_cw_kg", 5))
            threshold_value_rfpp_ccw_kg = float(row.get("threthreshold_value_rfpp_ccw_kgshold_value_bal", 5))
            threshold_value_rfh1_cw_kg = float(row.get("threshold_value_rfh1_cw_kg", 5))
            threshold_value_rfh1_ccw_kg = float(row.get("threshold_value_rfh1_ccw_kg", 5))
            threshold_value_rfh2_cw_kg = float(row.get("threshold_value_rfh2_cw_kg", 5))
            threshold_value_rfh2_ccw_kg = float(row.get("threshold_value_rfh2_ccw_kg", 5))
            threshold_value_lfpp_cw_kg = float(row.get("threshold_value_lfpp_cw_kg", 5))
            threshold_value_lfpp_ccw_kg = float(row.get("threshold_value_lfpp_ccw_kg", 5))
            threshold_value_lsft_cw_kg = float(row.get("threshold_value_lsft_cw_kg", 5))
            threshold_value_lsft_ccw_kg = float(row.get("threshold_value_lsft_ccw_kg", 5))
            threshold_value_lfh1_cw_kg = float(row.get("threshold_value_lfh1_cw_kg", 5))
            threshold_value_lfh1_ccw_kg = float(row.get("threshold_value_lfh1_ccw_kg", 5))
            threshold_value_plysteer_kg = float(row.get("threshold_value_plysteer_kg", 5))
            threshold_value_rfmxa_cw_deg = float(row.get("threshold_value_rfmxa_cw_deg", 5))
            threshold_value_rfmxa_ccw_deg = float(row.get("threshold_value_rfmxa_ccw_deg", 5))
            threshold_value_rfa1_cw_deg = float(row.get("threshold_value_rfa1_cw_deg", 5))
            threshold_value_rfa1_ccw_deg = float(row.get("threshold_value_rfa1_ccw_deg", 5))
            threshold_value_rfa2_cw_deg = float(row.get("threshold_value_rfa2_cw_deg", 5))
            threshold_value_rfa2_ccw_deg = float(row.get("threshold_value_rfa2_ccw_deg", 5))
            threshold_value_lfmxa_cw_deg = float(row.get("threshold_value_lfmxa_cw_deg", 5))
            threshold_value_lfmxa_ccw_deg = float(row.get("threshold_value_lfmxa_ccw_deg", 5))
            threshold_value_lfa1_cw_deg = float(row.get("threshold_value_lfa1_cw_deg", 5))
            threshold_value_lfa1_ccw_deg = float(row.get("threshold_value_lfa1_ccw_deg", 5))
            threshold_value_cony_kg = float(row.get("threshold_value_cony_kg", 5))
            threshold_value_bal_static_g = float(row.get("threshold_value_bal_static_g", 5))
            threshold_value_bal_static_deg = float(row.get("threshold_value_bal_static_deg", 5))
            threshold_value_bal_couple_g = float(row.get("threshold_value_bal_couple_g", 5))
            threshold_value_bal_couple_deg = float(row.get("threshold_value_bal_couple_deg", 5))
            threshold_value_bal_dynamic_botplane_g = float(row.get("threshold_value_bal_dynamic_botplane_g", 5))
            threshold_value_bal_dynamic_topplane_g = float(row.get("threshold_value_bal_dynamic_topplane_g", 5))
            threshold_value_bal_dynamic_botplane_deg = float(row.get("threshold_value_bal_dynamic_botplane_deg", 5))
            threshold_value_bal_dynamic_topplane_deg = float(row.get("threshold_value_bal_dynamic_topplane_deg", 5))
            threshold_value_geo_bul_botbul_mm = float(row.get("threshold_value_geo_bul_botbul_mm", 5))
            threshold_value_geo_bul_topbul_mm = float(row.get("threshold_value_geo_bul_topbul_mm", 5))
            threshold_value_geo_bul_botbul_deg = float(row.get("threshold_value_geo_bul_botbul_deg", 5))
            threshold_value_geo_bul_topdep_deg = float(row.get("threshold_value_geo_bul_topdep_deg", 5))
            threshold_value_geo_dep_botdep_mm = float(row.get("threshold_value_geo_dep_botdep_mm", 5))
            threshold_value_geo_dep_topdep_mm = float(row.get("threshold_value_geo_dep_topdep_mm", 5))
            threshold_value_geo_dep_botdep_deg = float(row.get("threshold_value_geo_dep_botdep_deg", 5))
            threshold_value_geo_dep_topbul_deg = float(row.get("threshold_value_geo_dep_topbul_deg", 5))
            threshold_value_geo_rro_crro_mm = float(row.get("threshold_value_geo_rro_crro_mm", 5))
            threshold_value_geo_rro_crroh1_mm = float(row.get("threshold_value_geo_rro_crroh1_mm", 5))
            threshold_value_geo_lro_blro_mm = float(row.get("threshold_value_geo_lro_blro_mm", 5))
            threshold_value_geo_lro_blroh1_mm = float(row.get("threshold_value_geo_lro_blroh1_mm", 5))
            threshold_value_geo_lro_tlro_mm = float(row.get("threshold_value_geo_lro_tlro_mm", 5))
            threshold_value_geo_lro_tlroh1_mm = float(row.get("threshold_value_geo_lro_tlroh1_mm", 5))
            threshold_value_geo_rro_crro_deg = float(row.get("threshold_value_geo_rro_crro_deg", 5))
            threshold_value_geo_rro_crroh1_deg = float(row.get("threshold_value_geo_rro_crroh1_deg", 5))
            threshold_value_geo_lro_blro_deg = float(row.get("threshold_value_geo_lro_blro_deg", 5))
            threshold_value_geo_lro_blroh1_deg = float(row.get("threshold_value_geo_lro_blroh1_deg", 5))
            threshold_value_geo_lro_tlro_deg = float(row.get("threshold_value_geo_lro_tlro_deg", 5))
            threshold_value_geo_lro_tlroh1_deg = float(row.get("threshold_value_geo_lro_tlroh1_deg", 5))

            threshold_value_yield_rfv_yield = float(row.get("threshold_value_yield_rfv_yield", 5))
            threshold_value_yield_rfh1_yield = float(row.get("threshold_value_yield_rfh1_yield", 5))
            threshold_value_yield_rfh2_yield = float(row.get("threshold_value_yield_rfh2_yield", 5))
            threshold_value_yield_lfv_yield = float(row.get("threshold_value_yield_lfv_yield", 5))
            threshold_value_yield_lsft_yield = float(row.get("threshold_value_yield_lsft_yield", 5))
            threshold_value_yield_lfh1_yield = float(row.get("threshold_value_yield_lfh1_yield", 5))
            threshold_value_yield_conicity_yield = float(row.get("threshold_value_yield_conicity_yield", 5))
            threshold_value_yield_oe_yield = float(row.get("threshold_value_yield_oe_yield", 5))
            threshold_value_yield_rfmxa_yield = float(row.get("threshold_value_yield_rfmxa_yield", 5))
            threshold_value_yield_rfa1_yield = float(row.get("threshold_value_yield_rfa1_yield", 5))
            threshold_value_yield_rfa2_yield = float(row.get("threshold_value_yield_rfa2_yield", 5))
            threshold_value_yield_lfxma_yield = float(row.get("threshold_value_yield_lfxma_yield", 5))
            threshold_value_yield_lfa1_yield = float(row.get("threshold_value_yield_lfa1_yield", 5))
            threshold_value_yield_central_radial_runout_yield = float(row.get("threshold_value_yield_central_radial_runout_yield", 5))
            threshold_value_yield_central_radial_runout_first_harmonic_yield = float(row.get("threshold_value_yield_central_radial_runout_first_harmonic_yield", 5))
            threshold_value_yield_lateral_run_out_top_over_yield = float(row.get("threshold_value_yield_lateral_run_out_top_over_yield", 5))
            threshold_value_yield_lateral_run_out_bottom_over_yield = float(row.get("threshold_value_yield_lateral_run_out_bottom_over_yield", 5))
            threshold_value_yield_lateral_run_out_yield = float(row.get("threshold_value_yield_lateral_run_out_yield", 5))
            threshold_value_yield_lateral_run_out_bottom_first_harmonic_yield = float(row.get("threshold_value_yield_lateral_run_out_bottom_first_harmonic_yield", 5))
            threshold_value_yield_lateral_run_out_top_first_harmonic_yield = float(row.get("threshold_value_yield_lateral_run_out_top_first_harmonic_yield", 5))
            threshold_value_yield_lateral_run_out_harmonic_yield = float(row.get("threshold_value_yield_lateral_run_out_harmonic_yield", 5))
            threshold_value_yield_bulge_top_over_yield = float(row.get("threshold_value_yield_bulge_top_over_yield", 5))
            threshold_value_yield_bulge_bottom_over_yield = float(row.get("threshold_value_yield_bulge_bottom_over_yield", 5))
            threshold_value_yield_bulge_yield = float(row.get("threshold_value_yield_bulge_yield", 5))
            threshold_value_yield_dent_top_over_yield = float(row.get("threshold_value_yield_dent_top_over_yield", 5))
            threshold_value_yield_dent_bottom_over_yield = float(row.get("threshold_value_yield_dent_bottom_over_yield", 5))
            threshold_value_yield_dent_yield = float(row.get("threshold_value_yield_dent_yield", 5))
            threshold_value_yield_geometry_oe_yield = float(row.get("threshold_value_yield_geometry_oe_yield", 5))
            threshold_value_yield_lower_yield = float(row.get("threshold_value_yield_lower_yield", 5))
            threshold_value_yield_upper_yield = float(row.get("threshold_value_yield_upper_yield", 5))
            threshold_value_yield_static_yield = float(row.get("threshold_value_yield_static_yield", 5))
            threshold_value_yield_couple_yield = float(row.get("threshold_value_yield_couple_yield", 5))
            threshold_value_yield_balancing_oe_yield = float(row.get("threshold_value_yield_balancing_oe_yield", 5))
            threshold_value_yield_warehouse_yield = float(row.get("threshold_value_yield_warehouse_yield", 5))
            threshold_value_yield_visual_yield = float(row.get("threshold_value_yield_visual_yield", 5))
            threshold_value_yield = float(row.get("threshold_value_yield", 5))

            class_threshold_value_yield_class_yield = float(row.get("class_threshold_value_yield_class_yield", 5))
            threshold_value_yield_balancing_yield = float(row.get("threshold_value_yield_balancing_yield", 5))
            threshold_value_yield_geometry_yield = float(row.get("threshold_value_yield_geometry_yield", 5))
            
            for gt_code in gt_codes:
                gt_code = str(gt_code).strip()
                # WITHOUT lot size: assign a lot id to ALL barcodes for the press, so we
                # do NOT restrict to active curing presses here.
                model_data = merge_df[
                    (merge_df["gt_code"] == model_number)
                ].copy()

                total_barcodes_model = len(model_data)
                model_data.sort_values(["date_of_curing"], ascending=True, inplace=True)
                alert_map = {}

                # Virgin/Final yield split: store raw barcode rows, then continue main
                # processing on virgin data (first occurrence per barcode).
                virgin_data, final_data, _vf_msgs = split_and_store_virgin_final(model_data, "date_of_curing", "CURING")
                output_messages.extend(_vf_msgs)
                model_data = virgin_data.copy()
                total_barcodes_model = len(model_data)

                for curing_value, curing_data in model_data.groupby(["curing_press", "curing_mouldid"]):
                    curing_data = curing_data[curing_data["curing_machine_model_lot_id"].isnull()].copy()
                    if curing_data.empty:
                        continue

                    total_curing_barcodes = len(curing_data)
                    curing_press_value, curing_mouldid_value = curing_value
                    curing_press_value = str(curing_press_value).replace("_", "-")
                    curing_mouldid_value = str(curing_mouldid_value).replace("_", "-")
                    machine_no = f"{curing_press_value}-{curing_mouldid_value}"
                    machine_press_value = machine_no.split('-')[0]

                    # MODIFIED: Only access tbm_machine if it exists
                    tbm_machine_value = curing_data["tbm_machine"].iloc[0] if "tbm_machine" in curing_data.columns and not curing_data["tbm_machine"].isna().all() else None
                    # output_messages.append(f"DEBUG: Model {model_number}, Machine {machine_no}, tbm_machine: {tbm_machine_value}, Found {total_curing_barcodes} barcodes.")

                    # WITHOUT lot size: process every press/mould group so all barcodes
                    # get a lot id. Do NOT skip presses missing from curing_df config.

                    if total_curing_barcodes > 0:
                        batch_size = total_curing_barcodes
                        batch_indices = curing_data.index
                        batch_data = curing_data.copy()

                        machine_model_key = (model_number, machine_no)
                        if machine_model_key not in last_lot_dict:
                            last_lot_dict[machine_model_key] = {
                                "max_lot_number": 0,
                                "last_lot_id": None
                            }
                        last_lot_number = last_lot_dict[(model_number, machine_no)]["max_lot_number"] + 1

                        unique_lot_id_current = f"Curing_{machine_no}_{model_number}_{last_lot_number}"
                        current_barcode_counter = batch_size
                        current_curing_machine_model_lot_id = f"{machine_no}_{model_number}_Curing_{machine_no}_{model_number}_{last_lot_number}_{current_barcode_counter}"
                        curing_previous_machine_model_lot_id = curing_previous_machine_model_lot_ids.get((model_number, machine_no))

                        last_lot_dict[(model_number, machine_no)]["max_lot_number"] = last_lot_number
                        last_lot_dict[(model_number, machine_no)]["last_lot_id"] = unique_lot_id_current

                        if not re.match(r"^Curing_[^_]+_[^_]+_\d+$", unique_lot_id_current):
                            logging.error(f"Invalid lot_id format: {unique_lot_id_current}")
                            continue
                        if not re.match(r"^[^_]+_[^_]+_Curing_[^_]+_[^_]+_\d+_\d+$", current_curing_machine_model_lot_id):
                            logging.error(f"Invalid curing_machine_model_lot_id format: {current_curing_machine_model_lot_id}")
                            continue

                        curing_previous_machine_model_lot_id = curing_previous_machine_model_lot_ids.get(machine_model_key)
                        # logging.info(f"Initial curing_previous_machine_model_lot_id: {curing_previous_machine_model_lot_id} for model {model_number}, machine {machine_no}")
                        if curing_previous_machine_model_lot_id is None and last_lot_number > 1:
                            prev_lot_number = last_lot_number - 1
                            prev_lot_id = f"Curing_{machine_no}_{model_number}_{prev_lot_number}"
                            logging.info(f"Fetching previous lot for lot_id: {prev_lot_id}")
                            try:
                                prev_lot = averages_collect.find_one(
                                    {
                                        "gt_code": model_number,
                                        "machine": machine_no,
                                        "lot_id": prev_lot_id
                                    },
                                    sort=[("curing_machine_model_lot_id", -1)]
                                )
                                if prev_lot:
                                    curing_previous_machine_model_lot_id = prev_lot["curing_machine_model_lot_id"]
                                    logging.info(f"Found previous lot: {curing_previous_machine_model_lot_id}")
                                else:
                                    logging.warning(f"No previous lot found for lot_id: {prev_lot_id}")
                                    curing_previous_machine_model_lot_id = None
                            except Exception as e:
                                logging.error(f"Error fetching previous curing_machine_model_lot_id for lot {prev_lot_number}: {e}")
                                curing_previous_machine_model_lot_id = None

                        previous_lot_id = f"Curing_{machine_no}_{model_number}_{last_lot_number - 1}" if last_lot_number > 1 else None
                        merge_df.loc[batch_indices, "lot_id"] = unique_lot_id_current
                        merge_df.loc[batch_indices, "previous_lot_id"] = previous_lot_id
                        merge_df.loc[batch_indices, "curing_machine_model_lot_id"] = current_curing_machine_model_lot_id
                        merge_df.loc[batch_indices, "curing_previous_machine_model_lot_id"] = curing_previous_machine_model_lot_id

                        prev_key = (model_number, machine_no, curing_previous_machine_model_lot_id)
                        prev_lot_avg = machine_lot_avg_map.get(prev_key, None)
                        if prev_lot_avg is None and curing_previous_machine_model_lot_id is not None:
                            try:
                                prev_lot_doc = averages_collect.find_one(
                                    {
                                        "gt_code": model_number,
                                        "machine": machine_no,
                                        "curing_machine_model_lot_id": curing_previous_machine_model_lot_id
                                    },
                                    {"current_lot_averages": 1}
                                )
                                prev_lot_avg = prev_lot_doc.get("current_lot_averages", {}) if prev_lot_doc else {}
                                if prev_lot_avg:
                                    machine_lot_avg_map[prev_key] = prev_lot_avg
                                    logging.info(f"Fetched previous lot averages for {curing_previous_machine_model_lot_id} from database")
                                else:
                                    logging.warning(f"No previous lot averages found for {curing_previous_machine_model_lot_id} in database")
                            except Exception as e:
                                logging.error(f"Error fetching previous lot averages for {curing_previous_machine_model_lot_id}: {e}")
                                prev_lot_avg = {}
                        if last_lot_number == 1:
                            prev_lot_avg = {}
                            # logging.info(f"First lot for model {model_number}, machine {machine_no}. Setting previous_lot_averages to empty.")
                        # logging.info(f"prev_key: {prev_key}, prev_lot_avg: {prev_lot_avg}")

                        last_lot_dict[machine_model_key]["max_lot_number"] = last_lot_number
                        last_lot_dict[machine_model_key]["last_lot_id"] = unique_lot_id_current

                        lot_doc = {
                            "gt_code": model_number,
                            "machine": machine_no,
                            "lot_id": unique_lot_id_current,
                            "timestamp": some_function()
                        }
                        try:
                            if not lot_curing_collect.find_one({"lot_id": unique_lot_id_current}):
                                lot_curing_collect.insert_one(lot_doc)
                                output_messages.append(f"Inserted lot_id {unique_lot_id_current} into lot_curing_collect")
                        except Exception as e:
                            logging.error(f"Error inserting lot_id {unique_lot_id_current} into lot_curing_collect: {e}")
                            output_messages.append(f"❌ Error inserting lot_id {unique_lot_id_current}: {e}")

                        # logging.info(f"Assigned lot_id: {unique_lot_id_current}, curing_machine_model_lot_id: {current_curing_machine_model_lot_id}, curing_previous_machine_model_lot_id: {curing_previous_machine_model_lot_id}")

                        for param in conicity_params:
                            if param in batch_data.columns:
                                batch_data[param] = pd.to_numeric(batch_data[param], errors='coerce')

                        processed_machine_barcodes = batch_size
                        balance_machine_barcodes = total_curing_barcodes - batch_size
                        balance_barcodes = total_barcodes_model - batch_size

                        lot_averages = {}
                        for param in conicity_params:
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
                            "lsft_yield": lsft_yield(batch_data),
                            "lfh1_yield": lfh1_yield(batch_data),
                            "conicity_yield": conicity_yield(batch_data),
                            "oe_yield": oe_yield(batch_data),
                            "rfmxa_yield": rfmxa_yield(batch_data),
                            "rfa1_yield": rfa1_yield(batch_data),
                            "rfa2_yield": rfa2_yield(batch_data),
                            "lfxma_yield": lfxma_yield(batch_data),
                            "lfa1_yield": lfa1_yield(batch_data),
                            "central_radial_runout_yield": central_radial_runout_yield(batch_data),
                            "central_radial_runout_first_harmonic_yield": central_radial_runout_first_harmonic_yield(batch_data),
                            "lateral_run_out_top_over_yield": lateral_run_out_top_over_yield(batch_data),
                            "lateral_run_out_bottom_over_yield": lateral_run_out_bottom_over_yield(batch_data),
                            "lateral_run_out_yield": lateral_run_out_yield(batch_data),
                            "lateral_run_out_bottom_first_harmonic_yield": lateral_run_out_bottom_first_harmonic_yield(batch_data),
                            "lateral_run_out_top_first_harmonic_yield": lateral_run_out_top_first_harmonic_yield(batch_data),
                            "lateral_run_out_harmonic_yield": lateral_run_out_harmonic_yield(batch_data),
                            "bulge_top_over_yield": bulge_top_over_yield(batch_data),
                            "bulge_bottom_over_yield": bulge_bottom_over_yield(batch_data),
                            "bulge_yield": bulge_yield(batch_data),
                            "dent_top_over_yield": dent_top_over_yield(batch_data),
                            "dent_bottom_over_yield": dent_bottom_over_yield(batch_data),
                            "dent_yield": dent_yield(batch_data),
                            "geometry_oe_yield": geometry_oe_yield(batch_data),
                            "lower_yield": lower_yield(batch_data),
                            "upper_yield": upper_yield(batch_data),
                            "static_yield": static_yield(batch_data),
                            "couple_yield": couple_yield(batch_data),
                            "balancing_oe_yield": balancing_oe_yield(batch_data),
                            "warehouse_yield": warehouse_yield(batch_data),
                            "visual_yield": visual_yield(batch_data)})
                                                                                

                        grouped_averages = {}
                        param_spec_map = {
                            "cony_kg": spec_cony_kg,
                            "rfpp_cw_kg": spec_rfpp_cw_kg,
                            "rfpp_ccw_kg": spec_rfpp_ccw_kg,
                            "rfh1_cw_kg": spec_rfh1_cw_kg,
                            "rfh1_ccw_kg": spec_rfh1_ccw_kg,
                            "lfpp_cw_kg": spec_lfpp_cw_kg,
                            "lfpp_ccw_kg": spec_lfpp_ccw_kg,
                            'rfh2_cw_kg': spec_rfh2_cw_kg,
                            'rfh2_ccw_kg': spec_rfh2_ccw_kg,
                            'lsft_cw_kg': spec_lsft_cw_kg,
                            'lsft_ccw_kg': spec_lsft_ccw_kg,
                            'lfh1_cw_kg': spec_lfh1_cw_kg,
                            'lfh1_ccw_kg': spec_lfh1_ccw_kg,
                            'plysteer_kg': spec_plysteer_kg,
                            'rfmxa_ccw_deg':spec_rfmxa_ccw_deg,
                            'rfa1_cw_deg': spec_rfa1_cw_deg,
                            'rfa1_ccw_deg': spec_rfa1_ccw_deg,
                            'rfa2_cw_deg': spec_rfa2_cw_deg,
                            'rfa2_ccw_deg': spec_rfa2_ccw_deg,
                            'lfmxa_cw_deg': spec_lfmxa_cw_deg,
                            'lfmxa_ccw_deg': spec_lfmxa_ccw_deg,
                            'lfa1_cw_deg': spec_lfa1_cw_deg,
                            'lfa1_ccw_deg': spec_lfa1_ccw_deg,
                            'couple_g': spec_couple_g,
                            'botplane_deg': spec_dynamic_botplane_deg,
                            'topplane_deg': spec_dynamic_topplane_deg,
                            'couple_deg': spec_couple_deg,
                            'static_deg': spec_static_deg,
                            'botbul_mm': spec_bul_botbul_mm,
                            'botdep_mm': spec_dep_botdep_mm,
                            'topbul_mm': spec_bul_topbul_mm,
                            'topdep_mm': spec_dep_topdep_mm,
                            'blro_mm': spec_lro_blro_mm,
                            'blroh1_mm': spec_lro_blroh1_mm,
                            'crro_mm': spec_rro_crro_mm,
                            'crroh1_mm': spec_rro_crroh1_mm,
                            'tlro_mm': spec_lro_tlro_mm,
                            'tlroh1_mm': spec_lro_tlroh1_mm,
                            'botbul_deg': spec_bul_botbul_deg,
                            'botdep_deg': spec_dep_botdep_deg,
                            'topbul_deg': spec_dep_topbul_deg,
                            'topdep_deg': spec_bul_topdep_deg,
                            'blro_deg': spec_lro_blro_deg,
                            'blroh1_deg': spec_lro_blroh1_deg,
                            'crro_deg': spec_rro_crro_deg,
                            'crroh1_deg': spec_rro_crroh1_deg,
                            'tlro_deg': spec_lro_tlro_deg,
                            'tlroh1_deg': spec_lro_tlroh1_deg,
                            'botplane_g': spec_dynamic_botplane_g,
                            'topplane_g': spec_dynamic_topplane_g,
                            "static_g": spec_static_g,

                            "rfv_yield": rfv_yield_spec_yield,
                            "rfh1_yield": rfh1_yield_spec_yield,
                            "rfh2_yield": rfh2_yield_spec_yield,
                            "lfv_yield": lfv_yield_spec_yield,
                            "lsft_yield": lsft_yield_spec_yield,
                            "lfh1_yield": lfh1_yield_spec_yield,
                            "conicity_yield": conicity_yield_spec_yield,
                            "oe_yield": oe_yield_spec_yield,
                            "rfmxa_yield": rfmxa_yield_spec_yield,
                            "rfa1_yield": rfa1_yield_spec_yield,
                            "rfa2_yield": rfa2_yield_spec_yield,
                            "lfxma_yield": lfxma_yield_spec_yield,
                            "lfa1_yield": lfa1_yield_spec_yield,
                            "central_radial_runout_yield": central_radial_runout_yield_spec_yield,
                            "central_radial_runout_first_harmonic_yield": central_radial_runout_first_harmonic_yield_spec_yield,
                            "lateral_run_out_top_over_yield": lateral_run_out_top_over_yield_spec_yield,
                            "lateral_run_out_bottom_over_yield": lateral_run_out_bottom_over_yield_spec_yield,
                            "lateral_run_out_yield": lateral_run_out_yield_spec_yield,
                            "lateral_run_out_bottom_first_harmonic_yield": lateral_run_out_bottom_first_harmonic_yield_spec_yield,
                            "lateral_run_out_top_first_harmonic_yield": lateral_run_out_top_first_harmonic_yield_spec_yield,
                            "lateral_run_out_harmonic_yield": lateral_run_out_harmonic_yield_spec_yield,
                            "bulge_top_over_yield": bulge_top_over_yield_spec_yield,
                            "bulge_bottom_over_yield": bulge_bottom_over_yield_spec_yield,
                            "bulge_yield": bulge_yield_spec_yield,
                            "dent_top_over_yield": dent_top_over_yield_spec_yield,
                            "dent_bottom_over_yield": dent_bottom_over_yield_spec_yield,
                            "dent__yield": dent_yield_spec_yield,
                            "geometry_oe_yield": geometry_oe_yield_spec_yield,
                            "lower_yield": lower_yield_spec_yield,
                            "upper_yield": upper_yield_spec_yield,
                            "static_yield": static_yield_spec_yield,
                            "couple_yield": couple_yield_spec_yield,
                            "balancing_oe_yield": balancing_oe_yield_spec_yield,
                            "warehouse_yield": warehouse_yield_spec_yield,
                            "visual_yield": visual_yield_spec_yield
                        }

                        try:
                            batch_data["ubno"] = batch_data["ubno"].astype(str).str.strip()
                            batch_data.sort_values("date_of_ufm", ascending=True, inplace=True)
                            ubno_grouped = batch_data.groupby("ubno")
                            ubno_records = []
                            for ubno, group in ubno_grouped:
                                group.sort_values("date_of_ufm", ascending=True)
                                record = {
                                    "ubno": ubno,
                                    "barcode_count": len(group["barcode"]),
                                    "tub_date_start_time": group["date_of_ufm"].iloc[0] if not group["date_of_ufm"].empty else None,
                                    "tub_date_end_time": group["date_of_ufm"].iloc[-1] if not group["date_of_ufm"].empty else None,
                                    "barcodes": group["barcode"].tolist() if "barcode" in group.columns else []
                                }
                                parameters = []
                                for param in conicity_params:
                                    try:
                                        if param in group.columns:
                                            valid_vals = group[param].dropna()
                                            avg_val = float(valid_vals.mean()) if not valid_vals.empty else None
                                        else:
                                            avg_val = None
                                        record[f"avg_{param}"] = avg_val
                                        spec = param_spec_map.get(param)
                                        exceeded = False
                                        if param == "cony_kg" and spec_cony_kg is not None and avg_val is not None:
                                            exceeded = not (-spec_cony_kg <= avg_val <= spec_cony_kg)
                                        if spec is not None and param not in ["cony_kg"] and avg_val is not None:
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
                                        exceeded = avg_val < spec if spec is not None else False
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
                            grouped_averages["ubno"] = ubno_records
                        except:
                            grouped_averages["ubno"] = []

                        batch_data1 = batch_data.copy()
                        # batch_data.sort_values("date_of_tbm", ascending=True, inplace=True)
                        batch_data1.sort_values("date_of_ufm", ascending=True, inplace=True)
                        batch_data.sort_values("date_of_curing", ascending=True, inplace=True)
                        batch_data1.sort_values("date_of_ufm", ascending=True, inplace=True)
                        # machine_lot_start_build_time = batch_data["date_of_tbm"].iloc[0]
                        # machine_lot_end_build_time = batch_data["date_of_tbm"].iloc[-1]
                        machine_lot_start_curing_time = batch_data["date_of_curing"].iloc[0]
                        machine_lot_end_curing_time = batch_data["date_of_curing"].iloc[-1]
                        tub_date_start_time = batch_data1["date_of_ufm"].iloc[0]
                        tub_date_end_time = batch_data1["date_of_ufm"].iloc[-1]
                        lot_start_time = some_function()
                        lot_end_time = some_function()

                        parameter_alerts = []
                        overall_shift_occurred = False
                        prev_key = (model_number, machine_no, curing_previous_machine_model_lot_id)
                        prev_lot_avg = machine_lot_avg_map.get(prev_key, None)

                        if prev_lot_avg:
                            for param in conicity_params + yield_params:
                                shift = False
                                shift_occurred_spec = False
                                current_val = lot_averages.get(param)
                                prev_val = prev_lot_avg.get(param)
                                if current_val is not None and prev_val is not None:
                                    if param == "cony_kg" and spec_cony_kg is not None:
                                        if abs(current_val - prev_val) > threshold_value_cony_kg:
                                            shift = True
                                        if not (-spec_cony_kg <= current_val <= spec_cony_kg):
                                            shift_occurred_spec = True
                                    if param == "rfpp_cw_kg" and spec_rfpp_cw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_rfpp_cw_kg:
                                            shift = True
                                        if current_val > spec_rfpp_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfpp_ccw_kg" and spec_rfpp_ccw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_rfpp_ccw_kg:
                                            shift = True
                                        if current_val > spec_rfpp_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfh1_cw_kg" and spec_rfh1_cw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_rfh1_cw_kg:
                                            shift = True
                                        if current_val > spec_rfh1_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfh1_ccw_kg" and spec_rfh1_ccw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_rfh1_ccw_kg:
                                            shift = True
                                        if current_val > spec_rfh1_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfh2_cw_kg" and spec_rfh2_cw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_rfh2_cw_kg:
                                            shift = True
                                        if current_val > spec_rfh2_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfh2_ccw_kg" and spec_rfh2_ccw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_rfh2_ccw_kg:
                                            shift = True
                                        if current_val > spec_rfh2_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "lfpp_cw_kg" and spec_lfpp_cw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_lfpp_cw_kg:
                                            shift = True
                                        if current_val > spec_lfpp_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "lfpp_ccw_kg" and spec_lfpp_ccw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_lfpp_ccw_kg:
                                            shift = True
                                        if current_val > spec_lfpp_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "lsft_cw_kg" and spec_lsft_cw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_lsft_cw_kg:
                                            shift = True
                                        if current_val > spec_lsft_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "lsft_ccw_kg" and spec_lsft_ccw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_lsft_ccw_kg:
                                            shift = True
                                        if current_val > spec_lsft_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "lfh1_cw_kg" and spec_lfh1_cw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_lfh1_cw_kg:
                                            shift = True
                                        if current_val > spec_lfh1_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "lfh1_ccw_kg" and spec_lfh1_ccw_kg is not None:
                                        if (current_val - prev_val) > threshold_value_lfh1_ccw_kg:
                                            shift = True
                                        if current_val > spec_lfh1_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "plysteer_kg" and spec_plysteer_kg is not None:
                                        if (current_val - prev_val) > threshold_value_plysteer_kg:
                                            shift = True
                                        if current_val > spec_plysteer_kg:
                                            shift_occurred_spec = True
                                    if param == "rfmxa_ccw_deg" and spec_rfmxa_ccw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_rfmxa_ccw_deg:
                                            shift = True
                                        if current_val > spec_rfmxa_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "rfa1_cw_deg" and spec_rfa1_cw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_rfa1_cw_deg:
                                            shift = True
                                        if current_val > spec_rfa1_cw_deg:
                                            shift_occurred_spec = True
                                    if param == "rfa1_ccw_deg" and spec_rfa1_ccw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_rfa1_ccw_deg:
                                            shift = True
                                        if current_val > spec_rfa1_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "rfa2_cw_deg" and spec_rfa2_cw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_rfa2_cw_deg:
                                            shift = True
                                        if current_val > spec_rfa2_cw_deg:
                                            shift_occurred_spec = True
                                    if param == "rfa2_ccw_deg" and spec_rfa2_ccw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_rfa2_ccw_deg:
                                            shift = True
                                        if current_val > spec_rfa2_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "lfmxa_cw_deg" and spec_lfmxa_cw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_lfmxa_cw_deg:
                                            shift = True
                                        if current_val > spec_lfmxa_cw_deg:
                                            shift_occurred_spec = True
                                    if param == "lfmxa_ccw_deg" and spec_lfmxa_ccw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_lfmxa_ccw_deg:
                                            shift = True
                                        if current_val > spec_lfmxa_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "lfa1_cw_deg" and spec_lfa1_cw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_lfa1_cw_deg:
                                            shift = True
                                        if current_val > spec_lfa1_cw_deg:
                                            shift_occurred_spec = True
                                    if param == "lfa1_ccw_deg" and spec_lfa1_ccw_deg is not None:
                                        if (current_val - prev_val) > threshold_value_lfa1_ccw_deg:
                                            shift = True
                                        if current_val > spec_lfa1_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "static_g" and spec_static_g is not None:
                                        if (current_val - prev_val) > threshold_value_bal_static_g:
                                            shift = True
                                        if current_val > spec_static_g:
                                            shift_occurred_spec = True
                                    if param == "static_deg" and spec_static_deg is not None:
                                        if (current_val - prev_val) > threshold_value_bal_static_deg:
                                            shift = True
                                        if current_val > spec_static_deg:
                                            shift_occurred_spec = True
                                    if param == "couple_g" and spec_couple_g is not None:
                                        if (current_val - prev_val) > threshold_value_bal_couple_g:
                                            shift = True
                                        if current_val > spec_couple_g:
                                            shift_occurred_spec = True
                                    if param == "couple_deg" and spec_couple_deg is not None:
                                        if (current_val - prev_val) > threshold_value_bal_couple_deg:
                                            shift = True
                                        if current_val > spec_couple_deg:
                                            shift_occurred_spec = True
                                    if param == "botbul_mm" and spec_bul_botbul_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_bul_botbul_mm:
                                            shift = True
                                        if current_val > spec_bul_botbul_mm:
                                            shift_occurred_spec = True
                                    if param == "topbul_mm" and spec_bul_topbul_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_bul_topbul_mm:
                                            shift = True
                                        if current_val > spec_bul_topbul_mm:
                                            shift_occurred_spec = True
                                    if param == "botbul_deg" and spec_bul_botbul_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_bul_botbul_deg:
                                            shift = True
                                        if current_val > spec_bul_botbul_deg:
                                            shift_occurred_spec = True
                                    if param == "topdep_deg" and spec_bul_topdep_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_bul_topdep_deg:
                                            shift = True
                                        if current_val > spec_bul_topdep_deg:
                                            shift_occurred_spec = True
                                    if param == "botdep_mm" and spec_dep_botdep_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_dep_botdep_mm:
                                            shift = True
                                        if current_val > spec_dep_botdep_mm:
                                            shift_occurred_spec = True
                                    if param == "topdep_mm" and spec_dep_topdep_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_dep_topdep_mm:
                                            shift = True
                                        if current_val > spec_dep_topdep_mm:
                                            shift_occurred_spec = True
                                    if param == "botdep_deg" and spec_dep_botdep_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_dep_botdep_deg:
                                            shift = True
                                        if current_val > spec_dep_botdep_deg:
                                            shift_occurred_spec = True
                                    if param == "topbul_deg" and spec_dep_topbul_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_dep_topbul_deg:
                                            shift = True
                                        if current_val > spec_dep_topbul_deg:
                                            shift_occurred_spec = True
                                    if param == "crro_mm" and spec_rro_crro_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_rro_crro_mm:
                                            shift = True
                                        if current_val > spec_rro_crro_mm:
                                            shift_occurred_spec = True
                                    if param == "crroh1_mm" and spec_rro_crroh1_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_rro_crroh1_mm:
                                            shift = True
                                        if current_val > spec_rro_crroh1_mm:
                                            shift_occurred_spec = True
                                    if param == "blro_mm" and spec_lro_blro_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_lro_blro_mm:
                                            shift = True
                                        if current_val > spec_lro_blro_mm:
                                            shift_occurred_spec = True
                                    if param == "blroh1_mm" and spec_lro_blroh1_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_lro_blroh1_mm:
                                            shift = True
                                        if current_val > spec_lro_blroh1_mm:
                                            shift_occurred_spec = True
                                    if param == "tlro_mm" and spec_lro_tlro_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_lro_tlro_mm:
                                            shift = True
                                        if current_val > spec_lro_tlro_mm:
                                            shift_occurred_spec = True
                                    if param == "tlroh1_mm" and spec_lro_tlroh1_mm is not None:
                                        if (current_val - prev_val) > threshold_value_geo_lro_tlroh1_mm:
                                            shift = True
                                        if current_val > spec_lro_tlroh1_mm:
                                            shift_occurred_spec = True
                                    if param == "crro_deg" and spec_rro_crro_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_rro_crro_deg:
                                            shift = True
                                        if current_val > spec_rro_crro_deg:
                                            shift_occurred_spec = True
                                    if param == "crroh1_deg" and spec_rro_crroh1_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_rro_crroh1_deg:
                                            shift = True
                                        if current_val > spec_rro_crroh1_deg:
                                            shift_occurred_spec = True
                                    if param == "blro_deg" and spec_lro_blro_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_lro_blro_deg:
                                            shift = True
                                        if current_val > spec_lro_blro_deg:
                                            shift_occurred_spec = True
                                    if param == "blroh1_deg" and spec_lro_blroh1_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_lro_blroh1_deg:
                                            shift = True
                                        if current_val > spec_lro_blroh1_deg:
                                            shift_occurred_spec = True
                                    if param == "tlro_deg" and spec_lro_tlro_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_lro_tlro_deg:
                                            shift = True
                                        if current_val > spec_lro_tlro_deg:
                                            shift_occurred_spec = True
                                    if param == "tlroh1_deg" and spec_lro_tlroh1_deg is not None:
                                        if (current_val - prev_val) > threshold_value_geo_lro_tlroh1_deg:
                                            shift = True
                                        if current_val > spec_lro_tlroh1_deg:
                                            shift_occurred_spec = True
                                    if param == "botplane_g" and spec_dynamic_botplane_g is not None:
                                        if (current_val - prev_val) > threshold_value_bal_dynamic_botplane_g:
                                            shift = True
                                        if current_val > spec_dynamic_botplane_g:
                                            shift_occurred_spec = True
                                    if param == "topplane_g" and spec_dynamic_topplane_g is not None:
                                        if (current_val - prev_val) > threshold_value_bal_dynamic_topplane_g:
                                            shift = True
                                        if current_val > spec_dynamic_topplane_g:
                                            shift_occurred_spec = True
                                    if param == "botplane_deg" and spec_dynamic_botplane_deg is not None:
                                        if (current_val - prev_val) > threshold_value_bal_dynamic_botplane_deg:
                                            shift = True
                                        if current_val > spec_dynamic_botplane_deg:
                                            shift_occurred_spec = True
                                    if param == "topplane_deg" and spec_dynamic_topplane_deg is not None:
                                        if (current_val - prev_val) > threshold_value_bal_dynamic_topplane_deg:
                                            shift = True
                                        if current_val > spec_dynamic_topplane_deg:
                                            shift_occurred_spec = True

                                    if param in yield_params:
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield)):
                                                shift = True
                                            if abs(current_val < spec_yield):
                                                shift_occurred_spec = True

                                    # if param == "oe_yield":
                                    #     spec_yield = param_spec_map.get(param)
                                    #     if spec_yield is not None:
                                    #         if (prev_val > current_val) and (abs(current_val - prev_val) > (class_threshold_value_yield_class_yield)):
                                    #             shift = True
                                    #         if abs(current_val < spec_yield):
                                    #             shift_occurred_spec = True
                                    # if param == "balancing_oe_yield":
                                    #     spec_yield = param_spec_map.get(param)
                                    #     if spec_yield is not None:
                                    #         if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_balancing_yield)):
                                    #             shift = True
                                    #         if abs(current_val < spec_yield):
                                    #             shift_occurred_spec = True
                                    # if param == "geometry_oe_yield":
                                    #     spec_yield = param_spec_map.get(param)
                                    #     if spec_yield is not None:
                                    #         if (prev_val > current_val) and (abs(current_val - prev_val) > (threshold_value_yield_geometry_yield)):
                                    #             shift = True
                                    #         if abs(current_val < spec_yield):
                                    #             shift_occurred_spec = True
                                    
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
                                parameter_alerts.append(alert_dict)
                                if shift_occurred:
                                    overall_shift_occurred = True
                        else:
                            for param in conicity_params + yield_params:
                                shift_occurred_thershold = False
                                shift_occurred_spec = False
                                current_val = lot_averages.get(param)
                                if current_val is not None:
                                    if param == "cony_kg" and spec_cony_kg is not None:
                                        if not (-spec_cony_kg <= current_val <= spec_cony_kg):
                                            shift_occurred_spec = True
                                    if param == "rfpp_cw_kg" and spec_rfpp_cw_kg is not None:
                                        if current_val > spec_rfpp_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfpp_ccw_kg" and spec_rfpp_ccw_kg is not None:
                                        if current_val > spec_rfpp_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfh1_cw_kg" and spec_rfh1_cw_kg is not None:
                                        if current_val > spec_rfh1_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfh1_ccw_kg" and spec_rfh1_ccw_kg is not None:
                                        if current_val > spec_rfh1_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfh2_cw_kg" and spec_rfh2_cw_kg is not None:
                                        if current_val > spec_rfh2_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "rfh2_ccw_kg" and spec_rfh2_ccw_kg is not None:
                                        if current_val > spec_rfh2_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "lfpp_cw_kg" and spec_lfpp_cw_kg is not None:
                                        if current_val > spec_lfpp_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "lfpp_ccw_kg" and spec_lfpp_ccw_kg is not None:
                                        if current_val > spec_lfpp_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "lsft_cw_kg" and spec_lsft_cw_kg is not None:
                                        if current_val > spec_lsft_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "lsft_ccw_kg" and spec_lsft_ccw_kg is not None:
                                        if current_val > spec_lsft_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "lfh1_cw_kg" and spec_lfh1_cw_kg is not None:
                                        if current_val > spec_lfh1_cw_kg:
                                            shift_occurred_spec = True
                                    if param == "lfh1_ccw_kg" and spec_lfh1_ccw_kg is not None:
                                        if current_val > spec_lfh1_ccw_kg:
                                            shift_occurred_spec = True
                                    if param == "plysteer_kg" and spec_plysteer_kg is not None:
                                        if current_val > spec_plysteer_kg:
                                            shift_occurred_spec = True
                                    if param == "rfmxa_ccw_deg" and spec_rfmxa_ccw_deg is not None:
                                        if current_val > spec_rfmxa_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "rfa1_cw_deg" and spec_rfa1_cw_deg is not None:
                                        if current_val > spec_rfa1_cw_deg:
                                            shift_occurred_spec = True
                                    if param == "rfa1_ccw_deg" and spec_rfa1_ccw_deg is not None:
                                        if current_val > spec_rfa1_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "rfa2_cw_deg" and spec_rfa2_cw_deg is not None:
                                        if current_val > spec_rfa2_cw_deg:
                                            shift_occurred_spec = True
                                    if param == "rfa2_ccw_deg" and spec_rfa2_ccw_deg is not None:
                                        if current_val > spec_rfa2_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "lfmxa_cw_deg" and spec_lfmxa_cw_deg is not None:
                                        if current_val > spec_lfmxa_cw_deg:
                                            shift_occurred_spec = True
                                    if param == "lfmxa_ccw_deg" and spec_lfmxa_ccw_deg is not None:
                                        if current_val > spec_lfmxa_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "lfa1_cw_deg" and spec_lfa1_cw_deg is not None:
                                        if current_val > spec_lfa1_cw_deg:
                                            shift_occurred_spec = True
                                    if param == "lfa1_ccw_deg" and spec_lfa1_ccw_deg is not None:
                                        if current_val > spec_lfa1_ccw_deg:
                                            shift_occurred_spec = True
                                    if param == "static_g" and spec_static_g is not None:
                                        if current_val > spec_static_g:
                                            shift_occurred_spec = True
                                    if param == "static_deg" and spec_static_deg is not None:
                                        if current_val > spec_static_deg:
                                            shift_occurred_spec = True
                                    if param == "couple_g" and spec_couple_deg is not None:
                                        if current_val > spec_couple_deg:
                                            shift_occurred_spec = True
                                    if param == "couple_deg" and spec_couple_deg is not None:
                                        if current_val > spec_couple_deg:
                                            shift_occurred_spec = True
                                    if param == "botbul_mm" and spec_bul_botbul_mm is not None:
                                        if current_val > spec_bul_botbul_mm:
                                            shift_occurred_spec = True
                                    if param == "topbul_mm" and spec_bul_topbul_mm is not None:
                                        if current_val > spec_bul_topbul_mm:
                                            shift_occurred_spec = True
                                    if param == "botbul_deg" and spec_bul_botbul_deg is not None:
                                        if current_val > spec_bul_botbul_deg:
                                            shift_occurred_spec = True
                                    if param == "topdep_deg" and spec_bul_topdep_deg is not None:
                                        if current_val > spec_bul_topdep_deg:
                                            shift_occurred_spec = True
                                    if param == "botdep_mm" and spec_dep_botdep_mm is not None:
                                        if current_val > spec_dep_botdep_mm:
                                            shift_occurred_spec = True
                                    if param == "topdep_mm" and spec_dep_topdep_mm is not None:
                                        if current_val > spec_dep_topdep_mm:
                                            shift_occurred_spec = True
                                    if param == "botdep_deg" and spec_dep_botdep_deg is not None:
                                        if current_val > spec_dep_botdep_deg:
                                            shift_occurred_spec = True
                                    if param == "topbul_deg" and spec_dep_topbul_deg is not None:
                                        if current_val > spec_dep_topbul_deg:
                                            shift_occurred_spec = True
                                    if param == "crro_mm" and spec_rro_crro_mm is not None:
                                        if current_val > spec_rro_crro_mm:
                                            shift_occurred_spec = True
                                    if param == "crroh1_mm" and spec_rro_crroh1_mm is not None:
                                        if current_val > spec_rro_crroh1_mm:
                                            shift_occurred_spec = True
                                    if param == "blro_mm" and spec_lro_blro_mm is not None:
                                        if current_val > spec_lro_blro_mm:
                                            shift_occurred_spec = True
                                    if param == "blroh1_mm" and spec_lro_blroh1_mm is not None:
                                        if current_val > spec_lro_blroh1_mm:
                                            shift_occurred_spec = True
                                    if param == "tlro_mm" and spec_lro_tlro_mm is not None:
                                        if current_val > spec_lro_tlro_mm:
                                            shift_occurred_spec = True
                                    if param == "tlroh1_mm" and spec_lro_tlroh1_mm is not None:
                                        if current_val > spec_lro_tlroh1_mm:
                                            shift_occurred_spec = True
                                    if param == "crro_deg" and spec_rro_crro_deg is not None:
                                        if current_val > spec_rro_crro_deg:
                                            shift_occurred_spec = True
                                    if param == "crroh1_deg" and spec_rro_crroh1_deg is not None:
                                        if current_val > spec_rro_crroh1_deg:
                                            shift_occurred_spec = True
                                    if param == "blro_deg" and spec_lro_blro_deg is not None:
                                        if current_val > spec_lro_blro_deg:
                                            shift_occurred_spec = True
                                    if param == "blroh1_deg" and spec_lro_blroh1_deg is not None:
                                        if current_val > spec_lro_blroh1_deg:
                                            shift_occurred_spec = True
                                    if param == "tlro_deg" and spec_lro_tlro_deg is not None:
                                        if current_val > spec_lro_tlro_deg:
                                            shift_occurred_spec = True
                                    if param == "tlroh1_deg" and spec_lro_tlroh1_deg is not None:
                                        if current_val > spec_lro_tlroh1_deg:
                                            shift_occurred_spec = True
                                    if param == "botplane_g" and spec_dynamic_botplane_g is not None:
                                        if current_val > spec_dynamic_botplane_g:
                                            shift_occurred_spec = True
                                    if param == "topplane_g" and spec_dynamic_topplane_g is not None:
                                        if current_val > spec_dynamic_topplane_g:
                                            shift_occurred_spec = True
                                    if param == "botplane_deg" and spec_dynamic_botplane_deg is not None:
                                        if current_val > spec_dynamic_botplane_deg:
                                            shift_occurred_spec = True
                                    if param == "topplane_deg" and spec_dynamic_topplane_deg is not None:
                                        if current_val > spec_dynamic_topplane_deg:
                                            shift_occurred_spec = True
                                    
                                    if param in yield_params:
                                        spec_yield = param_spec_map.get(param)
                                        if spec_yield is not None:
                                            if current_val < spec_yield:
                                                shift_occurred_spec = True
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
                                parameter_alerts.append(alert_dict)
                                if shift_occurred:
                                    overall_shift_occurred = True

                        shift_alerts = [alert for alert in parameter_alerts if alert["shift_occurred"]]
                        logging.debug(f"Model {model_number}, Machine {machine_no}, Lot {current_curing_machine_model_lot_id}: {len(shift_alerts)} parameter alerts with shift_occurred=True")
                        if shift_alerts:
                            logging.debug(f"Shift alerts: {[alert['parameter'] for alert in shift_alerts]}")

                        overall_shift_occurred = bool(overall_shift_occurred)

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
                                        if col in batch_data.columns:
                                            value_counts = batch_data[col].value_counts().to_dict()
                                            for rank_value, count in value_counts.items():
                                                rank_value = int(rank_value) if isinstance(rank_value, (int, float)) else rank_value
                                                rank_counts[f"{col}_{rank_value}"] = int(count)
                                    alert["rank_counts"] = rank_counts

                        if deviated_parameter_alerts:
                            deviated_doc = {
                                "alert_id_tbm_ufm": last_alert_id + 1,
                                "gt_code": model_number,
                                "lot_id": unique_lot_id_current,
                                "machine": machine_no,
                                "comments": [],
                                "alert_assign_user": [],
                                "curing_machine_model_lot_id": current_curing_machine_model_lot_id,
                                "curing_previous_machine_model_lot_id": curing_previous_machine_model_lot_id,
                                "parameter_alerts": deviated_parameter_alerts,
                                "timestamp_alert_generated": some_function()
                            }
                            deviated_triggering_bulk_operations.append(pymongo.InsertOne(deviated_doc))
                            # logging.info(f"Prepared {len(deviated_parameter_alerts)} derived alerts for machine {machine_no}, lot {unique_lot_id_current}")

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
                                        if col in batch_data.columns:
                                            value_counts = batch_data[col].value_counts().to_dict()
                                            for rank_value, count in value_counts.items():
                                                rank_value = int(rank_value) if isinstance(rank_value, (int, float)) else rank_value
                                                rank_counts[f"{col}_{rank_value}"] = int(count)
                                    alert["rank_counts"] = rank_counts


                        # batch_data["tbm_shift"] = batch_data["date_of_tbm"].apply(get_shift)
                        # shift_groups = batch_data.groupby('tbm_shift')
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


                        batch_data["curing_shift"] = batch_data["date_of_curing"].apply(get_shift)

                        shift_groups = batch_data.groupby('curing_shift')
                        shifts_data_list = []
                        for shift_name, shift_data in shift_groups:
                            shift_start_time = shift_data["date_of_curing"].iloc[0]
                            shift_end_time = shift_data["date_of_curing"].iloc[-1]
                            shifts_data_list.append({
                                "shifts": shift_name,
                                "barcode_count": len(shift_data["barcode"]),
                                "machine_shift_curing_start_time": shift_start_time,
                                "machine_shift_curing_end_time": shift_end_time,
                                "barcodes": shift_data["barcode"].tolist() if "barcode" in shift_data.columns else []
                            })

                        any_shift_flag = any(alert["shift_occurred"] for alert in parameter_alerts)

                        averages_doc = {
                            "gt_code": model_number,
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
                            "processed_machine_barcodes": int(processed_machine_barcodes),
                            "target_machine_lot_size": batch_size,
                            "balance_machine_barcodes": int(balance_machine_barcodes),
                            "model_processed_barcodes": int(batch_size),
                            "model_target_lot_size": batch_size,
                            "model_balance_barcodes": int(balance_barcodes),
                            "model_total_barcodes": int(total_barcodes_model),
                            "batch_indices": list(batch_indices),
                            "timestamp_alert_generated": some_function()
                        }

                        averages_query = {
                            "gt_code": model_number,
                            "lot_id": unique_lot_id_current,
                            "curing_machine_model_lot_id": current_curing_machine_model_lot_id
                        }
                        if not averages_collect.find_one(averages_query):
                            averages_bulk_operations.append(pymongo.InsertOne(averages_doc))

                        current_key = (model_number, machine_no, current_curing_machine_model_lot_id)
                        machine_lot_avg_map[current_key] = lot_averages
                        curing_previous_machine_model_lot_ids[(model_number, machine_no)] = current_curing_machine_model_lot_id

                        any_shift_flag = any(alert["shift_occurred"] for alert in parameter_alerts)
                        machine_alert = {
                            "machine": machine_no,
                            "tbm_machine": tbm_machine_value,  # MODIFIED: tbm_machine may be None if not created
                            "curing_shift_occured": any_shift_flag,
                            "shifts_data": shifts_data_list,
                            "oe_yield": oe_yield(batch_data),
                            "balancing_oe_yield": balancing_oe_yield(batch_data),
                            "geometry_oe_yield": geometry_oe_yield(batch_data),
                            "warehouse_yield": warehouse_yield(batch_data),
                            "visual_yield": visual_yield(batch_data),
                            "sizecode": sizecode,
                            "gt_code": str(gt_code),
                            "timestamp_lot_started": lot_start_time,
                            "timestamp_lot_ended": lot_end_time,
                            "target_machine_lot_size": batch_size,
                            "processed_machine_barcodes": int(processed_machine_barcodes),
                            "balance_machine_barcodes": int(balance_machine_barcodes),
                            "total_machine_barcodes": int(total_curing_barcodes),
                            # "machine_lot_start_build_time": machine_lot_start_build_time,
                            # "machine_lot_end_build_time": machine_lot_end_build_time,
                            "machine_lot_start_curing_time": machine_lot_start_curing_time,
                            "machine_lot_end_curing_time": machine_lot_end_curing_time,
                            "tub_date_start_time": tub_date_start_time,
                            "tub_date_end_time": tub_date_end_time,
                            "parameter_alerts": parameter_alerts,
                            "lot_averages": lot_averages,
                            "grouped_averages": grouped_averages,
                            "previous_lot_averages": machine_lot_avg_map.get((model_number, machine_no, curing_previous_machine_model_lot_id), {}),
                            "first_barcode_machine": batch_data.iloc[0]["barcode"] if "barcode" in batch_data.columns else None,
                            "last_barcode_machine": batch_data.iloc[-1]["barcode"] if "barcode" in batch_data.columns else None,
                            "curing_machine_model_lot_id": current_curing_machine_model_lot_id,
                            "curing_previous_machine_model_lot_id": curing_previous_machine_model_lot_id,
                            "rank_columns": get_rank_columns(batch_data),
                            "barcodes": batch_data["barcode"].tolist()
                        }

                        alert_key = (model_number, unique_lot_id_current)
                        if alert_key not in alert_map:
                            last_alert_id += 1
                            alert_map[alert_key] = {
                                "alert_id_curing_ufm": last_alert_id,
                                "gt_code": model_number,
                                "lot_id": unique_lot_id_current,
                                "machine_alerts": [],
                                "first_barcode_model": None,
                                "last_barcode_model": None
                            }
                        alert_map[alert_key]["machine_alerts"].append(machine_alert)
                        if machine_alert["curing_shift_occured"]:
                            alert_map[alert_key]["shift_occurred"] = True
                        if alert_map[alert_key]["first_barcode_model"] is None:
                            alert_map[alert_key]["first_barcode_model"] = machine_alert["first_barcode_machine"]
                        alert_map[alert_key]["last_barcode_model"] = machine_alert["last_barcode_machine"]

                        if overall_shift_occurred:
                            merge_df.loc[batch_indices, "alert_id_curing_ufm"] = last_alert_id

                for alert_key, alert_obj in alert_map.items():
                    final_alert = {
                        "alert_id_curing_ufm": alert_obj["alert_id_curing_ufm"],
                        "gt_code": alert_obj["gt_code"],
                        "lot_id": alert_obj["lot_id"],
                        "alert_flag": False,
                        "dashboard_flag": False,
                        "alert_status_check":"o",
                        "alert_status_timestamp": None,
                        "alert_status_comment": [],
                        "alert_assign_user": [],
                        "machine_alerts": alert_obj["machine_alerts"],
                        "first_barcode_model": alert_obj["first_barcode_model"],
                        "last_barcode_model": alert_obj["last_barcode_model"],
                        "timestamp_alert_generated": some_function()
                    }
                    alert_bulk_operations.append(pymongo.InsertOne(final_alert))

                for alert_key, alert_obj in alert_map.items():
                    true_machine_alerts = [ma for ma in alert_obj["machine_alerts"] if ma.get("curing_shift_occured", False)]
                    if true_machine_alerts:
                        doc_trigger = {
                            "alert_id_curing_ufm": alert_obj["alert_id_curing_ufm"],
                            "gt_code": alert_obj["gt_code"],
                            "lot_id": alert_obj["lot_id"],
                            "alert_flag": False,
                            "dashboard_flag": False,
                            "alert_status_check":"o",
                            "alert_status_timestamp": None,
                            "alert_status_comment": [],
                            "alert_assign_user": [],
                            "machine_alerts": true_machine_alerts,
                            "first_barcode_model": alert_obj["first_barcode_model"],
                            "last_barcode_model": alert_obj["last_barcode_model"],
                            "timestamp_alert_generated": some_function()
                        }
                        triggering_bulk_operations.append(pymongo.InsertOne(doc_trigger))

        except Exception as e:
            error_message = f"❌ Curing Error processing model number {model_number}: {e}"
            output_messages.append(error_message)

    if bulk_operations:
        try:
            result = lot_curing_collect.bulk_write(bulk_operations, ordered=False)
            inserted_counts["lot_curing_table_new"] = result.inserted_count
            output_messages.append(f"✅ Curing Inserted {result.inserted_count} documents into lot_curing_table_new.")
        except Exception as e:
            error_message = f"❌ Curing Error performing bulk write to lot_curing_table_new: {e}"
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

    if deviated_triggering_bulk_operations:
        try:
            result = alert_triggering_deviated_collect.bulk_write(deviated_triggering_bulk_operations, ordered=False)
            inserted_counts["alert_curing_without_lot_size"] = result.inserted_count
            output_messages.append(f"✅ TBM Inserted {result.inserted_count} deviated alert documents into alert_deviated_final_yield.alert_curing_without_lot_size.")
        except Exception as e:
            error_message = f"❌ TBM Error performing bulk write to alert_deviated_final_yield.alert_curing_without_lot_size: {e}"
            output_messages.append(error_message)
    else:
        output_messages.append("No TBM deviated alert documents to insert into alert_deviated_final_yield.alert_curing_without_lot_size.")

    if averages_bulk_operations:
        try:
            result = averages_collect.bulk_write(averages_bulk_operations, ordered=False)
            inserted_counts["averages_curing_table_new"] = result.inserted_count
            output_messages.append(f"✅ Curing Inserted {result.inserted_count} new averages documents into averages_curing_table_new.")
        except Exception as e:
            error_message = f"❌ Curing Error performing bulk write to averages_curing_table_new: {e}"
            output_messages.append(error_message)
    else:
        output_messages.append("No Curing averages documents to insert into averages_curing_table_new.")

    end_time = time.time()
    execution_time = end_time - start_time

    bulk_updates = []
    for index, row in merge_df.iterrows():
        query = {"_id": row["_id"]}
        update = {"$set": {
            "alert_id_curing_ufm": row["alert_id_curing_ufm"],
            "lot_id": row["lot_id"],
            "previous_lot_id": row["previous_lot_id"],
            "curing_machine_model_lot_id": row["curing_machine_model_lot_id"],
            "curing_previous_machine_model_lot_id": row["curing_previous_machine_model_lot_id"]
        }}
        # # MODIFIED: Only add tbm_machine to update if it exists in merge_df
        # if "tbm_machine" in merge_df.columns:
        #     update["$set"]["tbm_machine"] = row["tbm_machine"]
        bulk_updates.append(pymongo.UpdateOne(query, update))

    if bulk_updates:
        try:
            result = merge_collect.bulk_write(bulk_updates, ordered=False)
            updated_counts["new_data_merged_combined_uniform_balan_geo"] = result.modified_count
            output_messages.append(f"✅ Curing Updated {result.modified_count} documents in new_data_merged_combined_uniform_balan_geo collection.")
        except Exception as e:
            error_message = f"❌ Curing Error updating new_data_merged_combined_uniform_balan_geo collection: {e}"
            output_messages.append(error_message)
    else:
        output_messages.append("No Curing documents to update in new_data_merged_combined_uniform_balan_geo collection.")

    if triggering_bulk_operations:
        try:
            triggering_result = alert_triggering_collect.bulk_write(triggering_bulk_operations, ordered=False)
            inserted_counts["alert_triggering_curing_without_lot_size"] = triggering_result.inserted_count
            output_messages.append(f"✅ Curing Inserted {triggering_result.inserted_count} machine_alert documents into alert_triggering_curing_without_lot_size.")
            result_dashboard = alert_triggering_collect_dashboard_curing.bulk_write(triggering_bulk_operations, ordered=False)
            inserted_counts["alert_triggering_curing_dashboard"] = result_dashboard.inserted_count
            output_messages.append(f"✅ TBM Inserted {result_dashboard.inserted_count} machine_alert documents into alert_db_final_yield.alert_triggering_curing_without_lot_size.")
        except Exception as e:
            error_message = f"❌ Curing Error performing bulk write to alert_triggering_curing_without_lot_size collection: {e}"
            output_messages.append(error_message)
    else:
        output_messages.append("No Curing machine_alert documents to insert into alert_triggering_curing_without_lot_size collection.")

    final_message = f"✅ Curing Process completed successfully in {execution_time:.2f} seconds!"
    output_messages.append(final_message)

    logging.info("process_curing_data_and_generate_lots_alerts_without_lot_size function is completed")
    return {"status": "success", "message": final_message, "output_log": output_messages,
            "inserted_counts": inserted_counts, "updated_counts": updated_counts, "execution_time": execution_time}, 200

def process_ufm_lot_and_alert_data_without_lot_size():
    process_start_time = time.time()
    output_messages = []
    inserted_counts = {}
    updated_counts = {}
    logging.info("process_ufm_lot_and_alert_data_without_lot_size function is just started")
    try:
        # UFM Specific Collections
        merge_collect = client[UNIFORMITY_BALANCING_GEOMETRY_DB][new_COMBINED_COLLECTION_NAME]
        channel_collect = client[config_databse][channel_no_collect]
        machine_collect = client[config_databse][machine_no_collect]
        lot_ufm_collect = client[lot_db][lot_ufm_collect_name_new]
        alert_collect = client[alert_table_final_yield][alert_ufm_collect_name_new]
        averages_ufm_collect = client[average_db][averages_ufm_table_new]
        ub_machine_collect = client[config_databse][ub_machine_no_collect]
        alert_ufm_collect = client[alert_db_virgin_yield][alert_triggering_ubno_collect_name_new]
        alert_triggering_deviated_collect = client[alert_deviated_final_yield][alert_triggering_deviated_ufm_new]
        alert_triggering_collect_dashboard_ufm = client[alert_db_final_yield][alert_triggering_ubno_collect_name_new]
        ufm_collect_data = client[model_db][ufm_collection]
        ufm_machine_collect = client[model_db][ufm_collection_machine]
        logging.info("✅ UFM Collections loaded successfully for alert_db_virgin_yield and alert_db_final_yield.")
        output_messages.append("✅ UFM Collections loaded successfully for lot and alert processing in alert_db_virgin_yield and alert_db_final_yield.")
    except pymongo.errors.OperationFailure as e:
        error_message = f"❌ UFM Database or collection error during processing: {e}"
        logging.error(error_message)
        output_messages.append(error_message)
        return {"status": "error", "message": error_message, "output_log": output_messages}, 500

    # Fetch active machines
    try:
        active_machines = set(ufm_machine_collect.distinct("ubno", {"status": "active"}))
        output_messages.append(f"✅ Fetched {len(active_machines)} active machines: {active_machines}")
        if not active_machines:
            output_messages.append("❌ No active machines found in model_workcenter_db.ufm_machine. Exiting.")
            return {"status": "error", "message": "No active machines found", "output_log": output_messages}, 500
    except Exception as e:
        output_messages.append(f"❌ Error fetching active machines: {e}")
        return {"status": "error", "message": "Failed to fetch active machines", "output_log": output_messages}, 500

    # Fetch active gt_code numbers
    try:
        active_gtcodes = set()
        cursor = ufm_collect_data.find({"status": "active"}, {"gt_code": 1})
        for doc in cursor:
            active_gtcodes.update(doc.get("gt_code", []))
        output_messages.append(f"✅ Fetched {len(active_gtcodes)} active model numbers: {active_gtcodes}")
    except Exception as e:
        output_messages.append(f"❌ Error fetching active model numbers: {e}")
        return {"status": "error", "message": "Failed to fetch active model numbers", "output_log": output_messages}, 500

    # Create unique indexes
    try:
        lot_ufm_collect.create_index(
            [
                ("modelno", pymongo.ASCENDING),
                ("machine", pymongo.ASCENDING),
                ("lot_id", pymongo.ASCENDING),
                ("ufm_machine_model_lot_id", pymongo.ASCENDING)
            ],
            unique=True,
            name="ufm_lot_unique_idx"
        )
        alert_ufm_collect.create_index(
            [
                ("modelno", pymongo.ASCENDING),
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
        channel_data = list(channel_collect.find({"gt_code": {"$in": list(active_gtcodes)}}))
        channel_df = pd.DataFrame(channel_data)
        machine_data = list(machine_collect.find({}))
        machine_df = pd.DataFrame(machine_data)
        ub_machine_data = list(ub_machine_collect.find({"ubno": {"$in": list(active_machines)}}))
        ub_machine_df = pd.DataFrame(ub_machine_data)
        logging.info("✅ UFM Data loaded into DataFrames.")
        output_messages.append("✅ UFM Data loaded into DataFrames.")
    except Exception as e:
        error_message = f"❌ UFM Error loading data: {e}"
        logging.error(error_message)
        output_messages.append(error_message)
        return {"status": "error", "message": error_message, "output_log": output_messages}, 500

    # Filter data
    try:
        gt_code_lists = ufm_collect_data.distinct("gt_code")
        if "gt_code" not in merge_df.columns:
            output_messages.append("'gt_code' column missing in merge_df. Skipping UFM processing.")
            return {"status": "success", "message": "'gt_code' column missing. Skipping UFM processing.", "output_log": output_messages}, 200
        # Initialize alert-tracking columns if they don't exist yet (first run)
        if "alert_id_ufm_ufm" not in merge_df.columns:
            merge_df["alert_id_ufm_ufm"] = None
        if "ufm_machine_model_lot_id" not in merge_df.columns:
            merge_df["ufm_machine_model_lot_id"] = None

        merge_df = merge_df[merge_df["gt_code"].isin(gt_code_lists)]
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
            return {"status": "success", "message": "No new data to process UFM.", "output_log": output_messages}, 200

    except KeyError as e:
        error_message = f"Failed to filter UFM: Error - DataFrame issue: Column missing {e}. Ensure 'alert_id_ufm_ufm' and 'ufm_machine_model_lot_id' are present in '{uniformity_collection_name}'."
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
        merge_df["date_of_ufm"] = pd.to_datetime(merge_df["date_of_ufm"], errors="coerce").dt.strftime('%Y-%m-%d %H:%M:%S').fillna("No Date")

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
        merge_df["shift"] = merge_df["date_of_ufm"].apply(get_shift)

        merge_df["ubno"] = merge_df["ubno"].astype(str)
        ub_machine_df["ubno"] = ub_machine_df["ubno"].astype(str)
        merge_df = merge_df[merge_df["ubno"].isin(active_machines)]
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
                        "gt_code": {"$in": list(active_gtcodes)},
                        "machine": {"$in": list(active_machines)}}},
            {"$addFields": {
                "lot_number": {
                    "$toInt": {"$arrayElemAt": [{"$split": ["$lot_id", "_"]}, -1]}
                },
                "gt_code": "$gt_code",
                "machine": "$machine"
            }},
            {"$sort": {"lot_number": -1}},
            {"$group": {
                "_id": {"gt_code": "$gt_code", "machine": "$machine"},
                "max_lot_number": {"$first": "$lot_number"},
                "last_lot_id": {"$first": "$lot_id"}
            }}
        ]))
        last_lot_dict = {
            (item["_id"]["gt_code"], item["_id"]["machine"]): {
                "max_lot_number": item["max_lot_number"],
                "last_lot_id": item["last_lot_id"]
            } for item in last_lots_agg
        }
        logging.info("ℹ️ Last UFM Lot numbers fetched: %s", last_lot_dict)
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
                    "gt_code": {"$in": list(active_gtcodes)},
                    "machine": {"$in": list(active_machines)}
                }
            },
            {
                "$sort": {"ufm_machine_model_lot_id": -1}
            },
            {
                "$group": {
                    "_id": {"gt_code": "$gt_code", "machine": "$machine"},
                    "last_ufm_lot_id": {"$first": "$ufm_machine_model_lot_id"},
                    "last_averages": {"$first": "$current_lot_averages"}
                }
            }
        ]))
        for item in last_averages:
            gt_code = item["_id"]["gt_code"]
            machine = item["_id"]["machine"]
            ufm_lot_id = item["last_ufm_lot_id"]
            averages = item["last_averages"]
            ufm_previous_machine_model_lot_ids[(gt_code, machine)] = ufm_lot_id
            if averages:
                ufm_machine_lot_avg_map[(gt_code, machine, ufm_lot_id)] = averages
                logging.info(f"Pre-populated ufm_machine_lot_avg_map with averages for {ufm_lot_id}")
            else:
                logging.warning(f"No averages found for {ufm_lot_id} in averages_ufm_collect")
        logging.info(f"Initialized ufm_previous_machine_model_lot_ids: {ufm_previous_machine_model_lot_ids}")
        output_messages.append(f"Initialized ufm_previous_machine_model_lot_ids with {len(ufm_previous_machine_model_lot_ids)} entries")
    except Exception as e:
        logging.error(f"Error pre-populating ufm_machine_lot_avg_map and ufm_previous_machine_model_lot_ids: {e}")
        output_messages.append(f"❌ Error pre-populating ufm_machine_lot_avg_map and ufm_previous_machine_model_lot_ids: {e}")

    ufm_params = [
        'rfpp_cw_kg','rfpp_ccw_kg','rfh1_cw_kg','rfh1_ccw_kg','rfh2_cw_kg','rfh2_ccw_kg','lfpp_cw_kg','lfpp_ccw_kg',
        'lsft_cw_kg','lsft_ccw_kg','lfh1_cw_kg','lfh1_ccw_kg','plysteer_kg','cony_kg','rfmxa_ccw_deg',
        'rfa1_cw_deg','rfa1_ccw_deg','rfa2_cw_deg','rfa2_ccw_deg','lfmxa_cw_deg','lfmxa_ccw_deg','lfa1_cw_deg',
        'lfa1_ccw_deg','botplane_g','topplane_g','couple_g','static_g','botplane_deg','topplane_deg','couple_deg',
        'static_deg','botbul_mm','botdep_mm','topbul_mm','topdep_mm','blro_mm','blroh1_mm','crro_mm','crroh1_mm',
        'tlro_mm','tlroh1_mm','botbul_deg','botdep_deg','topbul_deg','topdep_deg','blro_deg','blroh1_deg','crro_deg',
        'crroh1_deg','tlro_deg','tlroh1_deg'
        ]
    yield_params = [
        "rfv_yield", "rfh1_yield", "rfh2_yield", "lfv_yield", "lsft_yield", "lfh1_yield", "conicity_yield", "oe_yield",
        "rfmxa_yield", "rfa1_yield", "rfa2_yield", "lfxma_yield", "lfa1_yield", "central_radial_runout_yield", "central_radial_runout_first_harmonic_yield",
        "lateral_run_out_top_over_yield", "lateral_run_out_bottom_over_yield", "lateral_run_out_yield", "lateral_run_out_bottom_first_harmonic_yield", "lateral_run_out_top_first_harmonic_yield", 
        "lateral_run_out_harmonic_yield", "bulge_top_over_yield", "bulge_bottom_over_yield", "bulge_yield", "dent_top_over_yield", "dent_bottom_over_yield", "dent_yield", "geometry_oe_yield",
        "lower_yield", "upper_yield", "static_yield", "couple_yield", "balancing_oe_yield", "warehouse_yield", "visual_yield"
        ]
    rank_param_mappings = {
        "rfv_yield": ["tu_rfpp_cw_grade", "tu_rfpp_ccw_grade"],
        "rfh1_yield": ["tu_rfh1_cw_grade", "tu_rfh1_ccw_grade"], 
        "rfh2_yield": ["tu_rfh2_cw_grade", "tu_rfh2_ccw_grade"],
        "lfv_yield": ["tu_lfpp_cw_grade", "tu_lfpp_ccw_grade"],
        "lsft_yield": ["tu_lsft_cw_grade", "tu_lsft_ccw_grade"],
        "lfh1_yield": ["tu_lfh1_cw_grad", "tu_lfh1_ccw_grad"],
        "conicity_yield": ["tu_cony_grade"],
        "oe_yield": ["tu_class"],
        "rfmxa_yield": ["tu_rfmxa_ccw_grade"],
        "rfa1_yield": ["tu_rfa1_cw_grade", "tu_rfa1_ccw_grade"],
        "rfa2_yield": ["tu_rfa2_cw_grade", "tu_rfa2_ccw_grade"],
        "lfxma_yield": ["tu_lfmxa_cw_grade", "tu_lfmxa_ccw_grade"],
        "lfa1_yield": ["tu_lfa1_cw_grade", "tu_lfa1_ccw_grade"],
        "central_radial_runout_yield": ["tg_crro_grade"],
        "central_radial_runout_first_harmonic_yield": ["tg_crroh1_grade"],
        "lateral_run_out_top_over_yield": ["tg_tlro_grade"],
        "lateral_run_out_bottom_over_yield": ["tg_blro_grade"],
        "lateral_run_out_yield": ["tg_tlro_grade", "tg_blro_grade"],
        "lateral_run_out_bottom_first_harmonic_yield": ["tg_blroh1_grade"],
        "lateral_run_out_top_first_harmonic_yield": ["tg_tlroh1_grade"],
        "lateral_run_out_harmonic_yield": ["tg_tlroh1_grade", "tg_blroh1_grade"],
        "bulge_top_over_yield": ["tg_topbul_grade"],
        "bulge_bottom_over_yield": ["tg_botbul_grade"],
        "bulge_yield": ["tg_botbul_grade", "tg_topbul_grade"],
        "dent_top_over_yield": ["tg_topdep_grade"],
        "dent_bottom_over_yield": ["tg_botdep_grade"],
        "dent_yield": ["tg_topdep_grade", "tg_botdep_grade"],
        "geometry_oe_yield": ["tg_class"],
        "lower_yield": ["tb_botplane_grade"],
        "upper_yield": ["tb_topplane_grade"],
        "static_yield": ["tb_static_grade"],
        "couple_yield": ["tb_couple_grade"],
        "balancing_oe_yield": ["tb_class"]
        }
    for _, channel_row_data in channel_df.iterrows():
        try:
            gt_code_list = channel_row_data.get("gt_code", [])
            model_number = gt_code_list[0].strip() if isinstance(gt_code_list, list) and gt_code_list else ""
            if model_number not in active_gtcodes:
                output_messages.append(f"Skipping CURING model {model_number} as it is not active.")
                continue
            gt_codes = channel_row_data.get("gt_code", [])
            if not isinstance(gt_codes, list):
                gt_codes = [gt_codes]

            for gt_code in gt_codes:
                gt_code = str(gt_code).strip()
                model_data = merge_df[
                    (merge_df["gt_code"] == model_number) &
                    (merge_df["ubno"].isin(active_machines))
                ].copy()

                # # Validate specification columns
                # required_specs = [
                #     "spec_con", "spec_repp", "spec_rfh1", "spec_lfpp", "spec_static",
                #     "spec_bul", "spec_dep", "spec_lro", "spec_rro", "spec_dynamic",
                #     "class_spec_yield", "threshold_value", "threshold_value_bal"
                # ]
                # missing_specs = [spec for spec in required_specs if spec not in channel_row_data]
                # if missing_specs:
                #     logging.warning(f"Missing specification columns for model {model_number}: {missing_specs}. Using defaults.")
                #     output_messages.append(f"⚠️ Missing specification columns for model {model_number}: {missing_specs}")

                # spec_con = float(channel_row_data.get("spec_con", 0))
                sizecode = str(channel_row_data.get("sizecode", ""))
                spec_rfpp_cw_kg = float(channel_row_data.get("spec_rfpp_cw_kg", 0))
                spec_rfpp_ccw_kg = float(channel_row_data.get("spec_rfpp_ccw_kg", 0))
                spec_rfh1_cw_kg = float(channel_row_data.get("spec_rfh1_cw_kg", 0))
                spec_rfh1_ccw_kg = float(channel_row_data.get("spec_rfh1_ccw_kg", 0))
                spec_rfh2_cw_kg = float(channel_row_data.get("spec_rfh2_cw_kg", 0))
                spec_rfh2_ccw_kg = float(channel_row_data.get("spec_rfh2_ccw_kg", 0))
                spec_lfpp_cw_kg = float(channel_row_data.get("spec_lfpp_cw_kg", 0))
                spec_lfpp_ccw_kg = float(channel_row_data.get("spec_lfpp_ccw_kg", 0))
                spec_lsft_cw_kg = float(channel_row_data.get("spec_lsft_cw_kg", 0))
                spec_lsft_ccw_kg = float(channel_row_data.get("spec_lsft_ccw_kg", 0))
                spec_lfh1_cw_kg = float(channel_row_data.get("spec_lfh1_cw_kg", 0))
                spec_lfh1_ccw_kg = float(channel_row_data.get("spec_lfh1_ccw_kg", 0))
                spec_plysteer_kg = float(channel_row_data.get("spec_plysteer_kg", 0))
                spec_rfmxa_ccw_deg = float(channel_row_data.get("spec_rfmxa_ccw_deg", 0))
                spec_rfa1_cw_deg = float(channel_row_data.get("spec_rfa1_cw_deg", 0))
                spec_rfa1_ccw_deg = float(channel_row_data.get("spec_rfa1_ccw_deg", 0))
                spec_rfa2_cw_deg = float(channel_row_data.get("spec_rfa2_cw_deg", 0))
                spec_rfa2_ccw_deg = float(channel_row_data.get("spec_rfa2_ccw_deg", 0))
                spec_lfmxa_cw_deg = float(channel_row_data.get("spec_lfmxa_cw_deg", 0))
                spec_lfmxa_ccw_deg = float(channel_row_data.get("spec_lfmxa_ccw_deg", 0))
                spec_lfa1_cw_deg = float(channel_row_data.get("spec_lfa1_cw_deg", 0))
                spec_lfa1_ccw_deg = float(channel_row_data.get("spec_lfa1_ccw_deg", 0))
                spec_cony_kg = float(channel_row_data.get("spec_cony_kg", 0))
                spec_static_g = float(channel_row_data.get("spec_static_g", 0))
                spec_static_deg = float(channel_row_data.get("spec_static_deg", 0))
                spec_couple_g = float(channel_row_data.get("spec_couple_g", 0))
                spec_couple_deg = float(channel_row_data.get("spec_couple_deg", 0))
                spec_bul_botbul_mm = float(channel_row_data.get("spec_bul_botbul_mm", 0))
                spec_bul_topbul_mm = float(channel_row_data.get("spec_bul_topbul_mm", 0))
                spec_bul_botbul_deg = float(channel_row_data.get("spec_bul_botbul_deg", 0))
                spec_bul_topdep_deg = float(channel_row_data.get("spec_bul_topdep_deg", 0))
                spec_dep_botdep_mm = float(channel_row_data.get("spec_dep_botdep_mm", 0))
                spec_dep_topdep_mm = float(channel_row_data.get("spec_dep_topdep_mm", 0))
                spec_dep_botdep_deg = float(channel_row_data.get("spec_dep_botdep_deg", 0))
                spec_dep_topbul_deg = float(channel_row_data.get("spec_dep_topbul_deg", 0))
                spec_rro_crro_mm = float(channel_row_data.get("spec_rro_crro_mm", 0))
                spec_rro_crroh1_mm = float(channel_row_data.get("spec_rro_crroh1_mm", 0))
                spec_lro_blro_mm = float(channel_row_data.get("spec_lro_blro_mm", 0))
                spec_lro_blroh1_mm = float(channel_row_data.get("spec_lro_blroh1_mm", 0))
                spec_lro_tlro_mm = float(channel_row_data.get("spec_lro_tlro_mm", 0))
                spec_lro_tlroh1_mm = float(channel_row_data.get("spec_lro_tlroh1_mm", 0))
                spec_rro_crro_deg = float(channel_row_data.get("spec_rro_crro_deg", 0))
                spec_rro_crroh1_deg = float(channel_row_data.get("spec_rro_crroh1_deg", 0))
                spec_lro_blro_deg = float(channel_row_data.get("spec_lro_blro_deg", 0))
                spec_lro_blroh1_deg = float(channel_row_data.get("spec_lro_blroh1_deg", 0))
                spec_lro_tlro_deg = float(channel_row_data.get("spec_lro_tlro_deg", 0))
                spec_lro_tlroh1_deg = float(channel_row_data.get("spec_lro_tlroh1_deg", 0))
                spec_dynamic_botplane_g = float(channel_row_data.get("spec_dynamic_botplane_g", 0))
                spec_dynamic_topplane_g = float(channel_row_data.get("spec_dynamic_topplane_g", 0))
                spec_dynamic_botplane_deg = float(channel_row_data.get("spec_dynamic_botplane_deg", 0))
                spec_dynamic_topplane_deg = float(channel_row_data.get("spec_dynamic_topplane_deg", 0))

                rfv_yield_spec_yield = float(channel_row_data.get("rfv_yield_spec_yield", 0))
                rfh1_yield_spec_yield = float(channel_row_data.get("rfh1_yield_spec_yield", 0))
                rfh2_yield_spec_yield = float(channel_row_data.get("rfh2_yield_spec_yield", 0))
                lfv_yield_spec_yield = float(channel_row_data.get("lfv_yield_spec_yield", 0))
                lsft_yield_spec_yield = float(channel_row_data.get("lsft_yield_spec_yield", 0))
                lfh1_yield_spec_yield = float(channel_row_data.get("lfh1_yield_spec_yield", 0))
                conicity_yield_spec_yield = float(channel_row_data.get("conicity_yield_spec_yield", 0))
                oe_yield_spec_yield = float(channel_row_data.get("oe_yield_spec_yield", 0))
                rfmxa_yield_spec_yield = float(channel_row_data.get("rfmxa_yield_spec_yield", 0))
                rfa1_yield_spec_yield = float(channel_row_data.get("rfa1_yield_spec_yield", 0))
                rfa2_yield_spec_yield = float(channel_row_data.get("rfa2_yield_spec_yield", 0))
                lfxma_yield_spec_yield = float(channel_row_data.get("lfxma_yield_spec_yield", 0))
                lfa1_yield_spec_yield = float(channel_row_data.get("lfa1_yield_spec_yield", 0))
                central_radial_runout_yield_spec_yield = float(channel_row_data.get("central_radial_runout_yield_spec_yield", 0))
                central_radial_runout_first_harmonic_yield_spec_yield = float(channel_row_data.get("central_radial_runout_first_harmonic_yield_spec_yield", 0))
                lateral_run_out_top_over_yield_spec_yield = float(channel_row_data.get("lateral_run_out_top_over_yield_spec_yield", 0))
                lateral_run_out_bottom_over_yield_spec_yield = float(channel_row_data.get("lateral_run_out_bottom_over_yield_spec_yield", 0))
                lateral_run_out_yield_spec_yield = float(channel_row_data.get("lateral_run_out_yield_spec_yield", 0))
                lateral_run_out_bottom_first_harmonic_yield_spec_yield = float(channel_row_data.get("lateral_run_out_bottom_first_harmonic_yield_spec_yield", 0))
                lateral_run_out_top_first_harmonic_yield_spec_yield = float(channel_row_data.get("lateral_run_out_top_first_harmonic_yield_spec_yield", 0))
                lateral_run_out_harmonic_yield_spec_yield = float(channel_row_data.get("lateral_run_out_harmonic_yield_spec_yield", 0))
                bulge_top_over_yield_spec_yield = float(channel_row_data.get("bulge_top_over_yield_spec_yield", 0))
                bulge_bottom_over_yield_spec_yield = float(channel_row_data.get("bulge_bottom_over_yield_spec_yield", 0))
                bulge_yield_spec_yield = float(channel_row_data.get("bulge_yield_spec_yield", 0))
                dent_top_over_yield_spec_yield = float(channel_row_data.get("dent_top_over_yield_spec_yield", 0))
                dent_bottom_over_yield_spec_yield = float(channel_row_data.get("dent_bottom_over_yield_spec_yield", 0))
                dent_yield_spec_yield = float(channel_row_data.get("dent_yield_spec_yield", 0))
                geometry_oe_yield_spec_yield = float(channel_row_data.get("geometry_oe_yield_spec_yield", 0))
                lower_yield_spec_yield = float(channel_row_data.get("lower_yield_spec_yield", 0))
                upper_yield_spec_yield = float(channel_row_data.get("upper_yield_spec_yield", 0))
                static_yield_spec_yield = float(channel_row_data.get("static_yield_spec_yield", 0))
                couple_yield_spec_yield = float(channel_row_data.get("couple_yield_spec_yield", 0))
                balancing_oe_yield_spec_yield = float(channel_row_data.get("balancing_oe_yield_spec_yield", 0))
                warehouse_yield_spec_yield = float(channel_row_data.get("warehouse_yield_spec_yield", 0))
                visual_yield_spec_yield = float(channel_row_data.get("visual_yield_spec_yield", 0))

                class_spec_yield = float(channel_row_data.get("class_spec_yield", 0))
                balancing_spec_yield = float(channel_row_data.get("balancing_spec_yield", 0))
                geometry_spec_yield = float(channel_row_data.get("geometry_spec_yield", 0))

                threshold_value_rfpp_cw_kg = float(channel_row_data.get("threshold_value_rfpp_cw_kg", 5))
                threshold_value_rfpp_ccw_kg = float(channel_row_data.get("threthreshold_value_rfpp_ccw_kgshold_value_bal", 5))
                threshold_value_rfh1_cw_kg = float(channel_row_data.get("threshold_value_rfh1_cw_kg", 5))
                threshold_value_rfh1_ccw_kg = float(channel_row_data.get("threshold_value_rfh1_ccw_kg", 5))
                threshold_value_rfh2_cw_kg = float(channel_row_data.get("threshold_value_rfh2_cw_kg", 5))
                threshold_value_rfh2_ccw_kg = float(channel_row_data.get("threshold_value_rfh2_ccw_kg", 5))
                threshold_value_lfpp_cw_kg = float(channel_row_data.get("threshold_value_lfpp_cw_kg", 5))
                threshold_value_lfpp_ccw_kg = float(channel_row_data.get("threshold_value_lfpp_ccw_kg", 5))
                threshold_value_lsft_cw_kg = float(channel_row_data.get("threshold_value_lsft_cw_kg", 5))
                threshold_value_lsft_ccw_kg = float(channel_row_data.get("threshold_value_lsft_ccw_kg", 5))
                threshold_value_lfh1_cw_kg = float(channel_row_data.get("threshold_value_lfh1_cw_kg", 5))
                threshold_value_lfh1_ccw_kg = float(channel_row_data.get("threshold_value_lfh1_ccw_kg", 5))
                threshold_value_plysteer_kg = float(channel_row_data.get("threshold_value_plysteer_kg", 5))
                threshold_value_rfmxa_cw_deg = float(channel_row_data.get("threshold_value_rfmxa_cw_deg", 5))
                threshold_value_rfmxa_ccw_deg = float(channel_row_data.get("threshold_value_rfmxa_ccw_deg", 5))
                threshold_value_rfa1_cw_deg = float(channel_row_data.get("threshold_value_rfa1_cw_deg", 5))
                threshold_value_rfa1_ccw_deg = float(channel_row_data.get("threshold_value_rfa1_ccw_deg", 5))
                threshold_value_rfa2_cw_deg = float(channel_row_data.get("threshold_value_rfa2_cw_deg", 5))
                threshold_value_rfa2_ccw_deg = float(channel_row_data.get("threshold_value_rfa2_ccw_deg", 5))
                threshold_value_lfmxa_cw_deg = float(channel_row_data.get("threshold_value_lfmxa_cw_deg", 5))
                threshold_value_lfmxa_ccw_deg = float(channel_row_data.get("threshold_value_lfmxa_ccw_deg", 5))
                threshold_value_lfa1_cw_deg = float(channel_row_data.get("threshold_value_lfa1_cw_deg", 5))
                threshold_value_lfa1_ccw_deg = float(channel_row_data.get("threshold_value_lfa1_ccw_deg", 5))
                threshold_value_cony_kg = float(channel_row_data.get("threshold_value_cony_kg", 5))
                threshold_value_bal_static_g = float(channel_row_data.get("threshold_value_bal_static_g", 5))
                threshold_value_bal_static_deg = float(channel_row_data.get("threshold_value_bal_static_deg", 5))
                threshold_value_bal_couple_g = float(channel_row_data.get("threshold_value_bal_couple_g", 5))
                threshold_value_bal_couple_deg = float(channel_row_data.get("threshold_value_bal_couple_deg", 5))
                threshold_value_bal_dynamic_botplane_g = float(channel_row_data.get("threshold_value_bal_dynamic_botplane_g", 5))
                threshold_value_bal_dynamic_topplane_g = float(channel_row_data.get("threshold_value_bal_dynamic_topplane_g", 5))
                threshold_value_bal_dynamic_botplane_deg = float(channel_row_data.get("threshold_value_bal_dynamic_botplane_deg", 5))
                threshold_value_bal_dynamic_topplane_deg = float(channel_row_data.get("threshold_value_bal_dynamic_topplane_deg", 5))
                threshold_value_geo_bul_botbul_mm = float(channel_row_data.get("threshold_value_geo_bul_botbul_mm", 5))
                threshold_value_geo_bul_topbul_mm = float(channel_row_data.get("threshold_value_geo_bul_topbul_mm", 5))
                threshold_value_geo_bul_botbul_deg = float(channel_row_data.get("threshold_value_geo_bul_botbul_deg", 5))
                threshold_value_geo_bul_topdep_deg = float(channel_row_data.get("threshold_value_geo_bul_topdep_deg", 5))
                threshold_value_geo_dep_botdep_mm = float(channel_row_data.get("threshold_value_geo_dep_botdep_mm", 5))
                threshold_value_geo_dep_topdep_mm = float(channel_row_data.get("threshold_value_geo_dep_topdep_mm", 5))
                threshold_value_geo_dep_botdep_deg = float(channel_row_data.get("threshold_value_geo_dep_botdep_deg", 5))
                threshold_value_geo_dep_topbul_deg = float(channel_row_data.get("threshold_value_geo_dep_topbul_deg", 5))
                threshold_value_geo_rro_crro_mm = float(channel_row_data.get("threshold_value_geo_rro_crro_mm", 5))
                threshold_value_geo_rro_crroh1_mm = float(channel_row_data.get("threshold_value_geo_rro_crroh1_mm", 5))
                threshold_value_geo_lro_blro_mm = float(channel_row_data.get("threshold_value_geo_lro_blro_mm", 5))
                threshold_value_geo_lro_blroh1_mm = float(channel_row_data.get("threshold_value_geo_lro_blroh1_mm", 5))
                threshold_value_geo_lro_tlro_mm = float(channel_row_data.get("threshold_value_geo_lro_tlro_mm", 5))
                threshold_value_geo_lro_tlroh1_mm = float(channel_row_data.get("threshold_value_geo_lro_tlroh1_mm", 5))
                threshold_value_geo_rro_crro_deg = float(channel_row_data.get("threshold_value_geo_rro_crro_deg", 5))
                threshold_value_geo_rro_crroh1_deg = float(channel_row_data.get("threshold_value_geo_rro_crroh1_deg", 5))
                threshold_value_geo_lro_blro_deg = float(channel_row_data.get("threshold_value_geo_lro_blro_deg", 5))
                threshold_value_geo_lro_blroh1_deg = float(channel_row_data.get("threshold_value_geo_lro_blroh1_deg", 5))
                threshold_value_geo_lro_tlro_deg = float(channel_row_data.get("threshold_value_geo_lro_tlro_deg", 5))
                threshold_value_geo_lro_tlroh1_deg = float(channel_row_data.get("threshold_value_geo_lro_tlroh1_deg", 5))

                threshold_value_yield_rfv_yield = float(channel_row_data.get("threshold_value_yield_rfv_yield", 5))
                threshold_value_yield_rfh1_yield = float(channel_row_data.get("threshold_value_yield_rfh1_yield", 5))
                threshold_value_yield_rfh2_yield = float(channel_row_data.get("threshold_value_yield_rfh2_yield", 5))
                threshold_value_yield_lfv_yield = float(channel_row_data.get("threshold_value_yield_lfv_yield", 5))
                threshold_value_yield_lsft_yield = float(channel_row_data.get("threshold_value_yield_lsft_yield", 5))
                threshold_value_yield_lfh1_yield = float(channel_row_data.get("threshold_value_yield_lfh1_yield", 5))
                threshold_value_yield_conicity_yield = float(channel_row_data.get("threshold_value_yield_conicity_yield", 5))
                threshold_value_yield_oe_yield = float(channel_row_data.get("threshold_value_yield_oe_yield", 5))
                threshold_value_yield_rfmxa_yield = float(channel_row_data.get("threshold_value_yield_rfmxa_yield", 5))
                threshold_value_yield_rfa1_yield = float(channel_row_data.get("threshold_value_yield_rfa1_yield", 5))
                threshold_value_yield_rfa2_yield = float(channel_row_data.get("threshold_value_yield_rfa2_yield", 5))
                threshold_value_yield_lfxma_yield = float(channel_row_data.get("threshold_value_yield_lfxma_yield", 5))
                threshold_value_yield_lfa1_yield = float(channel_row_data.get("threshold_value_yield_lfa1_yield", 5))
                threshold_value_yield_central_radial_runout_yield = float(channel_row_data.get("threshold_value_yield_central_radial_runout_yield", 5))
                threshold_value_yield_central_radial_runout_first_harmonic_yield = float(channel_row_data.get("threshold_value_yield_central_radial_runout_first_harmonic_yield", 5))
                threshold_value_yield_lateral_run_out_top_over_yield = float(channel_row_data.get("threshold_value_yield_lateral_run_out_top_over_yield", 5))
                threshold_value_yield_lateral_run_out_bottom_over_yield = float(channel_row_data.get("threshold_value_yield_lateral_run_out_bottom_over_yield", 5))
                threshold_value_yield_lateral_run_out_yield = float(channel_row_data.get("threshold_value_yield_lateral_run_out_yield", 5))
                threshold_value_yield_lateral_run_out_bottom_first_harmonic_yield = float(channel_row_data.get("threshold_value_yield_lateral_run_out_bottom_first_harmonic_yield", 5))
                threshold_value_yield_lateral_run_out_top_first_harmonic_yield = float(channel_row_data.get("threshold_value_yield_lateral_run_out_top_first_harmonic_yield", 5))
                threshold_value_yield_lateral_run_out_harmonic_yield = float(channel_row_data.get("threshold_value_yield_lateral_run_out_harmonic_yield", 5))
                threshold_value_yield_bulge_top_over_yield = float(channel_row_data.get("threshold_value_yield_bulge_top_over_yield", 5))
                threshold_value_yield_bulge_bottom_over_yield = float(channel_row_data.get("threshold_value_yield_bulge_bottom_over_yield", 5))
                threshold_value_yield_bulge_yield = float(channel_row_data.get("threshold_value_yield_bulge_yield", 5))
                threshold_value_yield_dent_top_over_yield = float(channel_row_data.get("threshold_value_yield_dent_top_over_yield", 5))
                threshold_value_yield_dent_bottom_over_yield = float(channel_row_data.get("threshold_value_yield_dent_bottom_over_yield", 5))
                threshold_value_yield_dent_yield = float(channel_row_data.get("threshold_value_yield_dent_yield", 5))
                threshold_value_yield_geometry_oe_yield = float(channel_row_data.get("threshold_value_yield_geometry_oe_yield", 5))
                threshold_value_yield_lower_yield = float(channel_row_data.get("threshold_value_yield_lower_yield", 5))
                threshold_value_yield_upper_yield = float(channel_row_data.get("threshold_value_yield_upper_yield", 5))
                threshold_value_yield_static_yield = float(channel_row_data.get("threshold_value_yield_static_yield", 5))
                threshold_value_yield_couple_yield = float(channel_row_data.get("threshold_value_yield_couple_yield", 5))
                threshold_value_yield_balancing_oe_yield = float(channel_row_data.get("threshold_value_yield_balancing_oe_yield", 5))
                threshold_value_yield_warehouse_yield = float(channel_row_data.get("threshold_value_yield_warehouse_yield", 5))
                threshold_value_yield_visual_yield = float(channel_row_data.get("threshold_value_yield_visual_yield", 5))
                threshold_value_yield = float(channel_row_data.get("threshold_value_yield", 5))

                class_threshold_value_yield_class_yield = float(channel_row_data.get("class_threshold_value_yield_class_yield", 5))
                threshold_value_yield_balancing_yield = float(channel_row_data.get("threshold_value_yield_balancing_yield", 5))
                threshold_value_yield_geometry_yield = float(channel_row_data.get("threshold_value_yield_geometry_yield", 5))

                if model_data.empty:
                    # logging.info(f"Skipping UFM model {model_number} gt_code {gt_code}: No data available.")
                    output_messages.append(f"Skipping UFM model {model_number} gt_code {gt_code}: No data available.")
                    continue

                model_data.sort_values("date_of_ufm", ascending=True, inplace=True)

                # Virgin/Final yield split: store raw barcode rows, then continue main
                # processing on virgin data (first occurrence per barcode).
                virgin_data, final_data, _vf_msgs = split_and_store_virgin_final(model_data, "date_of_ufm", "UFM")
                output_messages.extend(_vf_msgs)
                model_data = virgin_data.copy()

                for ufm_machine_id, ufm_machine_data in model_data.groupby("ubno"):
                    try:
                        machine_no = str(ufm_machine_id)
                        if ufm_machine_data.empty:
                            logging.info(f"Skipping UFM machine {machine_no} for model {model_number}: No unprocessed data.")
                            continue

                        machine_config_row = ub_machine_df[ub_machine_df["ubno"] == machine_no]
                        if machine_config_row.empty:
                            logging.info(f"Skipping UFM machine {machine_no} for model {model_number}: Unknown machine config.")
                            output_messages.append(f"Skipping UFM machine {machine_no} for model {model_number}: Unknown machine config.")
                            continue

                        total_ufm_machine_barcodes = len(ufm_machine_data)

                        machine_barcode_counter = 0
                        machine_model_key = (model_number, machine_no)

                        if (model_number, machine_no) not in last_lot_dict:
                            last_lot_dict[(model_number, machine_no)] = {
                                "max_lot_number": 0,
                                "last_lot_id": None
                            }

                        current_last_lot_number = last_lot_dict[(model_number, machine_no)]["max_lot_number"] + 1
                        current_model_lot_id = f"UFM_{machine_no}_{model_number}_{current_last_lot_number}"
                        current_barcode_counter = total_ufm_machine_barcodes
                        current_ufm_machine_lot_id = f"{machine_no}_{model_number}_UFM_{machine_no}_{model_number}_{current_last_lot_number}_{current_barcode_counter}"

                        if not re.match(r"^UFM_[^_]+_[^_]+_\d+$", current_model_lot_id):
                            logging.error(f"Invalid lot_id format: {current_model_lot_id}")
                            continue
                        if not re.match(r"^[^_]+_[^_]+_UFM_[^_]+_[^_]+_\d+_\d+$", current_ufm_machine_lot_id):
                            logging.error(f"Invalid ufm_machine_model_lot_id format: {current_ufm_machine_lot_id}")
                            continue

                        machine_batch_data = ufm_machine_data.copy()
                        machine_batch_indices = machine_batch_data.index

                        if machine_batch_data.empty:
                            logging.info(f"Empty batch data for machine {machine_no}, model {model_number}, lot {current_model_lot_id}. Skipping.")
                            continue

                        ufm_previous_machine_model_lot_id = ufm_previous_machine_model_lot_ids.get(machine_model_key)
                        # logging.info(f"Initial ufm_previous_machine_model_lot_id: {ufm_previous_machine_model_lot_id} for model {model_number}, machine {machine_no}")
                        if ufm_previous_machine_model_lot_id is None and current_last_lot_number > 1:
                            prev_lot_number = current_last_lot_number - 1
                            prev_lot_id = f"UFM_{machine_no}_{model_number}_{prev_lot_number}"
                            logging.info(f"Fetching previous lot for lot_id: {prev_lot_id}")
                            try:
                                prev_lot = averages_ufm_collect.find_one(
                                    {
                                        "gt_code": model_number,
                                        "machine": machine_no,
                                        "lot_id": prev_lot_id
                                    },
                                    sort=[("ufm_machine_model_lot_id", -1)]
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

                        model_level_previous_lot_id = f"UFM_{machine_no}_{model_number}_{current_last_lot_number - 1}" if current_last_lot_number > 1 else None
                        merge_df.loc[machine_batch_indices, "lot_id"] = current_model_lot_id
                        merge_df.loc[machine_batch_indices, "previous_lot_id"] = model_level_previous_lot_id
                        merge_df.loc[machine_batch_indices, "ufm_machine_model_lot_id"] = current_ufm_machine_lot_id
                        merge_df.loc[machine_batch_indices, "ufm_previous_machine_model_lot_id"] = ufm_previous_machine_model_lot_id

                        prev_avg_key = (model_number, machine_no, ufm_previous_machine_model_lot_id)
                        prev_lot_avg = ufm_machine_lot_avg_map.get(prev_avg_key, None)
                        if prev_lot_avg is None and ufm_previous_machine_model_lot_id is not None:
                            try:
                                prev_lot_doc = averages_ufm_collect.find_one(
                                    {
                                        "gt_code": model_number,
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
                            # logging.info(f"First lot for model {model_number}, machine {machine_no}. Setting previous_lot_averages to empty.")
                        # logging.info(f"prev_key: {prev_avg_key}, prev_lot_avg: {prev_lot_avg}")

                        last_lot_dict[machine_model_key] = {
                            "max_lot_number": current_last_lot_number,
                            "last_ufm_machine_model_lot_id": current_ufm_machine_lot_id
                        }

                        lot_doc_data = {
                            "_id": f"lot_{current_ufm_machine_lot_id}",
                            "gt_code": model_number,
                            "lot_id": current_model_lot_id,
                            "previous_lot_id": model_level_previous_lot_id,
                            "machine": machine_no,
                            "ufm_machine_model_lot_id": current_ufm_machine_lot_id,
                            "ufm_previous_machine_model_lot_id": ufm_previous_machine_model_lot_id,
                            "processed_machine_barcodes": total_ufm_machine_barcodes,
                            "timestamp_lot_generated": some_function(),
                            "first_barcode": machine_batch_data['barcode'].iloc[0] if not machine_batch_data.empty and 'barcode' in machine_batch_data else None,
                            "last_barcode": machine_batch_data['barcode'].iloc[-1] if not machine_batch_data.empty and 'barcode' in machine_batch_data else None,
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
                                try:
                                    numeric_series = pd.to_numeric(machine_batch_data[param], errors='coerce').dropna()
                                    lot_averages[param] = float(numeric_series.mean()) if not numeric_series.empty else None
                                except Exception as e:
                                    logging.warning(f"Error calculating mean for {param}: {e}")
                                    lot_averages[param] = None
                            else:
                                lot_averages[param] = None

                        lot_averages.update({
                                "rfv_yield": rfv_yield(machine_batch_data), 
                                "rfh1_yield": rfh1_yield(machine_batch_data),
                                "rfh2_yield": rfh2_yield(machine_batch_data),
                                "lfv_yield": lfv_yield(machine_batch_data),
                                "lsft_yield": lsft_yield(machine_batch_data),
                                "lfh1_yield": lfh1_yield(machine_batch_data),
                                "conicity_yield": conicity_yield(machine_batch_data),
                                "oe_yield": oe_yield(machine_batch_data),
                                "rfmxa_yield": rfmxa_yield(machine_batch_data),
                                "rfa1_yield": rfa1_yield(machine_batch_data),
                                "rfa2_yield": rfa2_yield(machine_batch_data),
                                "lfxma_yield": lfxma_yield(machine_batch_data),
                                "lfa1_yield": lfa1_yield(machine_batch_data),
                                "central_radial_runout_yield": central_radial_runout_yield(machine_batch_data),
                                "central_radial_runout_first_harmonic_yield": central_radial_runout_first_harmonic_yield(machine_batch_data),
                                "lateral_run_out_top_over_yield": lateral_run_out_top_over_yield(machine_batch_data),
                                "lateral_run_out_bottom_over_yield": lateral_run_out_bottom_over_yield(machine_batch_data),
                                "lateral_run_out_yield": lateral_run_out_yield(machine_batch_data),
                                "lateral_run_out_bottom_first_harmonic_yield": lateral_run_out_bottom_first_harmonic_yield(machine_batch_data),
                                "lateral_run_out_top_first_harmonic_yield": lateral_run_out_top_first_harmonic_yield(machine_batch_data),
                                "lateral_run_out_harmonic_yield": lateral_run_out_harmonic_yield(machine_batch_data),
                                "bulge_top_over_yield": bulge_top_over_yield(machine_batch_data),
                                "bulge_bottom_over_yield": bulge_bottom_over_yield(machine_batch_data),
                                "bulge_yield": bulge_yield(machine_batch_data),
                                "dent_top_over_yield": dent_top_over_yield(machine_batch_data),
                                "dent_bottom_over_yield": dent_bottom_over_yield(machine_batch_data),
                                "dent_yield": dent_yield(machine_batch_data),
                                "geometry_oe_yield": geometry_oe_yield(machine_batch_data),
                                "lower_yield": lower_yield(machine_batch_data),
                                "upper_yield": upper_yield(machine_batch_data),
                                "static_yield": static_yield(machine_batch_data),
                                "couple_yield": couple_yield(machine_batch_data),
                                "balancing_oe_yield": balancing_oe_yield(machine_batch_data),
                                "warehouse_yield": warehouse_yield(machine_batch_data),
                                "visual_yield": visual_yield(machine_batch_data)})

                        parameter_alerts = []
                        machine_lot_overall_shift_occurred = False
                        yield_spec_mapping = {
                            "cony_kg": spec_cony_kg,
                            "rfpp_cw_kg": spec_rfpp_cw_kg,
                            "rfpp_ccw_kg": spec_rfpp_ccw_kg,
                            "rfh1_cw_kg": spec_rfh1_cw_kg,
                            "rfh1_ccw_kg": spec_rfh1_ccw_kg,
                            "lfpp_cw_kg": spec_lfpp_cw_kg,
                            "lfpp_ccw_kg": spec_lfpp_ccw_kg,
                            'rfh2_cw_kg': spec_rfh2_cw_kg,
                            'rfh2_ccw_kg': spec_rfh2_ccw_kg,
                            'lsft_cw_kg': spec_lsft_cw_kg,
                            'lsft_ccw_kg': spec_lsft_ccw_kg,
                            'lfh1_cw_kg': spec_lfh1_cw_kg,
                            'lfh1_ccw_kg': spec_lfh1_ccw_kg,
                            'plysteer_kg': spec_plysteer_kg,
                            'rfmxa_ccw_deg':spec_rfmxa_ccw_deg,
                            'rfa1_cw_deg': spec_rfa1_cw_deg,
                            'rfa1_ccw_deg': spec_rfa1_ccw_deg,
                            'rfa2_cw_deg': spec_rfa2_cw_deg,
                            'rfa2_ccw_deg': spec_rfa2_ccw_deg,
                            'lfmxa_cw_deg': spec_lfmxa_cw_deg,
                            'lfmxa_ccw_deg': spec_lfmxa_ccw_deg,
                            'lfa1_cw_deg': spec_lfa1_cw_deg,
                            'lfa1_ccw_deg': spec_lfa1_ccw_deg,
                            'couple_g': spec_couple_g,
                            'botplane_deg': spec_dynamic_botplane_deg,
                            'topplane_deg': spec_dynamic_topplane_deg,
                            'couple_deg': spec_couple_deg,
                            'static_deg': spec_static_deg,
                            'botbul_mm': spec_bul_botbul_mm,
                            'botdep_mm': spec_dep_botdep_mm,
                            'topbul_mm': spec_bul_topbul_mm,
                            'topdep_mm': spec_dep_topdep_mm,
                            'blro_mm': spec_lro_blro_mm,
                            'blroh1_mm': spec_lro_blroh1_mm,
                            'crro_mm': spec_rro_crro_mm,
                            'crroh1_mm': spec_rro_crroh1_mm,
                            'tlro_mm': spec_lro_tlro_mm,
                            'tlroh1_mm': spec_lro_tlroh1_mm,
                            'botbul_deg': spec_bul_botbul_deg,
                            'botdep_deg': spec_dep_botdep_deg,
                            'topbul_deg': spec_dep_topbul_deg,
                            'topdep_deg': spec_bul_topdep_deg,
                            'blro_deg': spec_lro_blro_deg,
                            'blroh1_deg': spec_lro_blroh1_deg,
                            'crro_deg': spec_rro_crro_deg,
                            'crroh1_deg': spec_rro_crroh1_deg,
                            'tlro_deg': spec_lro_tlro_deg,
                            'tlroh1_deg': spec_lro_tlroh1_deg,
                            'botplane_g': spec_dynamic_botplane_g,
                            'topplane_g': spec_dynamic_topplane_g,
                            "static_g": spec_static_g,

                            "rfv_yield": rfv_yield_spec_yield,
                            "rfh1_yield": rfh1_yield_spec_yield,
                            "rfh2_yield": rfh2_yield_spec_yield,
                            "lfv_yield": lfv_yield_spec_yield,
                            "lsft_yield": lsft_yield_spec_yield,
                            "lfh1_yield": lfh1_yield_spec_yield,
                            "conicity_yield": conicity_yield_spec_yield,
                            "oe_yield": oe_yield_spec_yield,
                            "rfmxa_yield": rfmxa_yield_spec_yield,
                            "rfa1_yield": rfa1_yield_spec_yield,
                            "rfa2_yield": rfa2_yield_spec_yield,
                            "lfxma_yield": lfxma_yield_spec_yield,
                            "lfa1_yield": lfa1_yield_spec_yield,
                            "central_radial_runout_yield": central_radial_runout_yield_spec_yield,
                            "central_radial_runout_first_harmonic_yield": central_radial_runout_first_harmonic_yield_spec_yield,
                            "lateral_run_out_top_over_yield": lateral_run_out_top_over_yield_spec_yield,
                            "lateral_run_out_bottom_over_yield": lateral_run_out_bottom_over_yield_spec_yield,
                            "lateral_run_out_yield": lateral_run_out_yield_spec_yield,
                            "lateral_run_out_bottom_first_harmonic_yield": lateral_run_out_bottom_first_harmonic_yield_spec_yield,
                            "lateral_run_out_top_first_harmonic_yield": lateral_run_out_top_first_harmonic_yield_spec_yield,
                            "lateral_run_out_harmonic_yield": lateral_run_out_harmonic_yield_spec_yield,
                            "bulge_top_over_yield": bulge_top_over_yield_spec_yield,
                            "bulge_bottom_over_yield": bulge_bottom_over_yield_spec_yield,
                            "bulge_yield": bulge_yield_spec_yield,
                            "dent_top_over_yield": dent_top_over_yield_spec_yield,
                            "dent_bottom_over_yield": dent_bottom_over_yield_spec_yield,
                            "dent__yield": dent_yield_spec_yield,
                            "geometry_oe_yield": geometry_oe_yield_spec_yield,
                            "lower_yield": lower_yield_spec_yield,
                            "upper_yield": upper_yield_spec_yield,
                            "static_yield": static_yield_spec_yield,
                            "couple_yield": couple_yield_spec_yield,
                            "balancing_oe_yield": balancing_oe_yield_spec_yield,
                            "warehouse_yield": warehouse_yield_spec_yield,
                            "visual_yield": visual_yield_spec_yield
                            }
                        # Log warnings for parameters without specs
                        for param in ufm_params:
                            if param not in yield_spec_mapping and param not in yield_params:
                                logging.warning(f"No specification defined for parameter {param}")

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
                                spec_val = yield_spec_mapping.get(param)

                                if param == "cony_kg" and spec_cony_kg:
                                        shift_occurred_thershold = abs(current_val - prev_val) > threshold_value_cony_kg
                                        shift_occurred_spec = not (-spec_cony_kg <= current_val <= spec_cony_kg)
                                        spec_val = spec_cony_kg
                                elif param == "rfpp_cw_kg" and spec_rfpp_cw_kg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_rfpp_cw_kg
                                    shift_occurred_spec = current_val > spec_rfpp_cw_kg
                                    spec_val = spec_rfpp_cw_kg
                                elif param == "rfpp_ccw_kg" and spec_rfpp_ccw_kg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_rfpp_ccw_kg
                                    shift_occurred_spec = current_val > spec_rfpp_ccw_kg
                                    spec_val = spec_rfpp_ccw_kg
                                elif param == "rfh1_cw_kg" and spec_rfh1_cw_kg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_rfh1_cw_kg
                                    shift_occurred_spec = current_val > spec_rfh1_cw_kg
                                    spec_val = spec_rfh1_cw_kg
                                elif param == "rfh1_ccw_kg" and spec_rfh1_ccw_kg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_rfh1_ccw_kg
                                    shift_occurred_spec = current_val > spec_rfh1_ccw_kg
                                    spec_val = spec_rfh1_ccw_kg
                                elif param == "rfh2_cw_kg" and spec_rfh2_cw_kg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_rfh2_cw_kg
                                    shift_occurred_spec = current_val > spec_rfh2_cw_kg
                                    spec_val = spec_rfh2_cw_kg
                                elif param == "rfh2_ccw_kg" and spec_rfh2_ccw_kg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_rfh2_ccw_kg
                                    shift_occurred_spec = current_val > spec_rfh2_ccw_kg
                                    spec_val = spec_rfh2_ccw_kg
                                elif param == "lfpp_cw_kg" and spec_lfpp_cw_kg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_lfpp_cw_kg
                                    shift_occurred_spec = current_val > spec_lfpp_cw_kg
                                    spec_val = spec_lfpp_cw_kg
                                elif param == "lfpp_ccw_kg" and spec_lfpp_ccw_kg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_lfpp_ccw_kg
                                    shift_occurred_spec = current_val > spec_lfpp_ccw_kg
                                    spec_val = spec_lfpp_ccw_kg
                                elif param == "lsft_cw_kg" and spec_lsft_cw_kg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_lsft_cw_kg
                                    shift_occurred_spec = current_val > spec_lsft_cw_kg
                                    spec_val = spec_lsft_cw_kg
                                elif param == "lsft_ccw_kg" and spec_lsft_ccw_kg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_lsft_ccw_kg
                                    shift_occurred_spec = current_val > spec_lsft_ccw_kg
                                    spec_val = spec_lsft_ccw_kg
                                elif param == "lfh1_cw_kg" and spec_lfh1_cw_kg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_lfh1_cw_kg
                                    shift_occurred_spec = current_val > spec_lfh1_cw_kg
                                    spec_val = spec_lfh1_cw_kg
                                elif param == "lfh1_ccw_kg" and spec_lfh1_ccw_kg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_lfh1_ccw_kg
                                    shift_occurred_spec = current_val > spec_lfh1_ccw_kg
                                    spec_val = spec_lfh1_ccw_kg
                                elif param == "plysteer_kg" and spec_plysteer_kg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_plysteer_kg
                                    shift_occurred_spec = current_val > spec_plysteer_kg
                                    spec_val = spec_plysteer_kg
                                elif param == "rfmxa_ccw_deg" and spec_rfmxa_ccw_deg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_rfmxa_ccw_deg
                                    shift_occurred_spec = current_val > spec_rfmxa_ccw_deg
                                    spec_val = spec_rfmxa_ccw_deg
                                elif param == "rfa1_cw_deg" and spec_rfa1_cw_deg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_rfa1_cw_deg
                                    shift_occurred_spec = current_val > spec_rfa1_cw_deg
                                    spec_val = spec_rfa1_cw_deg
                                elif param == "rfa1_ccw_deg" and spec_rfa1_ccw_deg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_rfa1_ccw_deg
                                    shift_occurred_spec = current_val > spec_rfa1_ccw_deg
                                    spec_val = spec_rfa1_ccw_deg
                                elif param == "rfa2_cw_deg" and spec_rfa2_cw_deg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_rfa2_cw_deg
                                    shift_occurred_spec = current_val > spec_rfa2_cw_deg
                                    spec_val = spec_rfa2_cw_deg
                                elif param == "rfa2_ccw_deg" and spec_rfa2_ccw_deg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_rfa2_ccw_deg
                                    shift_occurred_spec = current_val > spec_rfa2_ccw_deg
                                    spec_val = spec_rfa2_ccw_deg
                                elif param == "lfmxa_cw_deg" and spec_lfmxa_cw_deg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_lfmxa_cw_deg
                                    shift_occurred_spec = current_val > spec_lfmxa_cw_deg
                                    spec_val = spec_lfmxa_cw_deg
                                elif param == "lfmxa_ccw_deg" and spec_lfmxa_ccw_deg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_lfmxa_ccw_deg
                                    shift_occurred_spec = current_val > spec_lfmxa_ccw_deg
                                    spec_val = spec_lfmxa_ccw_deg
                                elif param == "lfa1_cw_deg" and spec_lfa1_cw_deg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_lfa1_cw_deg
                                    shift_occurred_spec = current_val > spec_lfa1_cw_deg
                                    spec_val = spec_lfa1_cw_deg
                                elif param == "lfa1_ccw_deg" and spec_lfa1_ccw_deg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_lfa1_ccw_deg
                                    shift_occurred_spec = current_val > spec_lfa1_ccw_deg
                                    spec_val = spec_lfa1_ccw_deg
                                elif param == "static_g" and spec_static_g:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_bal_static_g
                                    shift_occurred_spec = current_val > spec_static_g
                                    spec_val = spec_static_g
                                elif param == "static_deg" and spec_static_deg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_bal_static_deg
                                    shift_occurred_spec = current_val > spec_static_deg
                                    spec_val = spec_static_deg
                                elif param == "couple_g" and spec_couple_g:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_bal_couple_g
                                    shift_occurred_spec = current_val > spec_couple_g
                                    spec_val = spec_couple_g
                                elif param == "couple_deg" and spec_couple_deg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_bal_couple_deg
                                    shift_occurred_spec = current_val > spec_couple_deg
                                    spec_val = spec_couple_deg
                                elif param == "botbul_mm" and spec_bul_botbul_mm:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_bul_botbul_mm
                                    shift_occurred_spec = current_val > spec_bul_botbul_mm
                                    spec_val = spec_bul_botbul_mm
                                elif param == "topbul_mm" and spec_bul_topbul_mm:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_bul_topbul_mm
                                    shift_occurred_spec = current_val > spec_bul_topbul_mm
                                    spec_val = spec_bul_topbul_mm
                                elif param == "botbul_deg" and spec_bul_botbul_deg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_bul_botbul_deg
                                    shift_occurred_spec = current_val > spec_bul_botbul_deg
                                    spec_val = spec_bul_botbul_deg
                                elif param == "topdep_deg" and spec_bul_topdep_deg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_bul_topdep_deg
                                    shift_occurred_spec = current_val > spec_bul_topdep_deg
                                    spec_val = spec_bul_topdep_deg
                                elif param == "botdep_mm" and spec_dep_botdep_mm:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_dep_botdep_mm
                                    shift_occurred_spec = current_val > spec_dep_botdep_mm
                                    spec_val = spec_dep_botdep_mm
                                elif param == "topdep_mm" and spec_dep_topdep_mm:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_dep_topdep_mm
                                    shift_occurred_spec = current_val > spec_dep_topdep_mm
                                    spec_val = spec_dep_topdep_mm
                                elif param == "botdep_deg" and spec_dep_botdep_deg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_dep_botdep_deg
                                    shift_occurred_spec = current_val > spec_dep_botdep_deg
                                    spec_val = spec_dep_botdep_deg
                                elif param == "topbul_deg" and spec_dep_topbul_deg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_dep_topbul_deg
                                    shift_occurred_spec = current_val > spec_dep_topbul_deg
                                    spec_val = spec_dep_topbul_deg
                                elif param == "crro_mm" and spec_rro_crro_mm:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_rro_crro_mm
                                    shift_occurred_spec = current_val > spec_rro_crro_mm
                                    spec_val = spec_rro_crro_mm
                                elif param == "crroh1_mm" and spec_rro_crroh1_mm:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_rro_crroh1_mm
                                    shift_occurred_spec = current_val > spec_rro_crroh1_mm
                                    spec_val = spec_rro_crroh1_mm
                                elif param == "blro_mm" and spec_lro_blro_mm:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_lro_blro_mm
                                    shift_occurred_spec = current_val > spec_lro_blro_mm
                                    spec_val = spec_lro_blro_mm
                                elif param == "blroh1_mm" and spec_lro_blroh1_mm:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_lro_blroh1_mm
                                    shift_occurred_spec = current_val > spec_lro_blroh1_mm
                                    spec_val = spec_lro_blroh1_mm
                                elif param == "tlro_mm" and spec_lro_tlro_mm:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_lro_tlro_mm
                                    shift_occurred_spec = current_val > spec_lro_tlro_mm
                                    spec_val = spec_lro_tlro_mm
                                elif param == "tlroh1_mm" and spec_lro_tlroh1_mm:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_lro_tlroh1_mm
                                    shift_occurred_spec = current_val > spec_lro_tlroh1_mm
                                    spec_val = spec_lro_tlroh1_mm
                                elif param == "crro_deg" and spec_rro_crro_deg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_rro_crro_deg
                                    shift_occurred_spec = current_val > spec_rro_crro_deg
                                    spec_val = spec_rro_crro_deg
                                elif param == "crroh1_deg" and spec_rro_crroh1_deg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_rro_crroh1_deg
                                    shift_occurred_spec = current_val > spec_rro_crroh1_deg
                                    spec_val = spec_rro_crroh1_deg
                                elif param == "blro_deg" and spec_lro_blro_deg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_lro_blro_deg
                                    shift_occurred_spec = current_val > spec_lro_blro_deg
                                    spec_val = spec_lro_blro_deg
                                elif param == "blroh1_deg" and spec_lro_blroh1_deg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_lro_blroh1_deg
                                    shift_occurred_spec = current_val > spec_lro_blroh1_deg
                                    spec_val = spec_lro_blroh1_deg
                                elif param == "tlro_deg" and spec_lro_tlro_deg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_lro_tlro_deg
                                    shift_occurred_spec = current_val > spec_lro_tlro_deg
                                    spec_val = spec_lro_tlro_deg
                                elif param == "tlroh1_deg" and spec_lro_tlroh1_deg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_geo_lro_tlroh1_deg
                                    shift_occurred_spec = current_val > spec_lro_tlroh1_deg
                                    spec_val = spec_lro_tlroh1_deg
                                elif param == "botplane_g" and spec_dynamic_botplane_g:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_bal_dynamic_botplane_g
                                    shift_occurred_spec = current_val > spec_dynamic_botplane_g
                                    spec_val = spec_dynamic_botplane_g
                                elif param == "topplane_g" and spec_dynamic_topplane_g:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_bal_dynamic_topplane_g
                                    shift_occurred_spec = current_val > spec_dynamic_topplane_g
                                    spec_val = spec_dynamic_topplane_g
                                elif param == "botplane_deg" and spec_dynamic_botplane_deg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_bal_dynamic_botplane_deg
                                    shift_occurred_spec = current_val > spec_dynamic_botplane_deg
                                    spec_val = spec_dynamic_botplane_deg
                                elif param == "topplane_deg" and spec_dynamic_topplane_deg:
                                    shift_occurred_thershold = (current_val - prev_val) > threshold_value_bal_dynamic_topplane_deg
                                    shift_occurred_spec = current_val > spec_dynamic_topplane_deg
                                    spec_val = spec_dynamic_topplane_deg

                                # elif param == "oe_yield":
                                #     spec_yield = yield_spec_mapping.get(param, 0)
                                #     shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > class_threshold_value_yield_class_yield)
                                #     shift_occurred_spec = current_val < spec_yield
                                #     spec_val = spec_yield
                                # elif param == "balancing_oe_yield":
                                #     spec_yield = yield_spec_mapping.get(param, 0)
                                #     shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_balancing_yield)
                                #     shift_occurred_spec = current_val < spec_yield
                                #     spec_val = spec_yield
                                # elif param == "geometry_oe_yield":
                                #     spec_yield = yield_spec_mapping.get(param, 0)
                                #     shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield_geometry_yield)
                                #     shift_occurred_spec = current_val < spec_yield
                                #     spec_val = spec_yield

                                elif param in yield_params:
                                    spec_yield = yield_spec_mapping.get(param, 0)
                                    shift_occurred_thershold = (prev_val > current_val) and (abs(current_val - prev_val) > threshold_value_yield)
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
                                spec_val = yield_spec_mapping.get(param)

                                if param == "cony_kg" and spec_cony_kg:
                                    shift_occurred_spec = not (-spec_cony_kg <= current_val <= spec_cony_kg)
                                    spec_val = spec_cony_kg
                                elif param == "rfpp_cw_kg" and spec_rfpp_cw_kg:
                                    shift_occurred_spec = current_val > spec_rfpp_cw_kg
                                    spec_val = spec_rfpp_cw_kg
                                elif param == "rfpp_ccw_kg" and spec_rfpp_ccw_kg:
                                    shift_occurred_spec = current_val > spec_rfpp_ccw_kg
                                    spec_val = spec_rfpp_ccw_kg
                                elif param == "rfh1_cw_kg" and spec_rfh1_cw_kg:
                                    shift_occurred_spec = current_val > spec_rfh1_cw_kg
                                    spec_val = spec_rfh1_cw_kg
                                elif param == "rfh1_ccw_kg" and spec_rfh1_ccw_kg:
                                    shift_occurred_spec = current_val > spec_rfh1_ccw_kg
                                    spec_val = spec_rfh1_ccw_kg
                                elif param == "rfh2_cw_kg" and spec_rfh2_cw_kg:
                                    shift_occurred_spec = current_val > spec_rfh2_cw_kg
                                    spec_val = spec_rfh2_cw_kg
                                elif param == "rfh2_ccw_kg" and spec_rfh2_ccw_kg:
                                    shift_occurred_spec = current_val > spec_rfh2_ccw_kg
                                    spec_val = spec_rfh2_ccw_kg
                                elif param == "lfpp_cw_kg" and spec_lfpp_cw_kg:
                                    shift_occurred_spec = current_val > spec_lfpp_cw_kg
                                    spec_val = spec_lfpp_cw_kg
                                elif param == "lfpp_ccw_kg" and spec_lfpp_ccw_kg:
                                    shift_occurred_spec = current_val > spec_lfpp_ccw_kg
                                    spec_val = spec_lfpp_ccw_kg
                                elif param == "lsft_cw_kg" and spec_lsft_cw_kg:
                                    shift_occurred_spec = current_val > spec_lsft_cw_kg
                                    spec_val = spec_lsft_cw_kg
                                elif param == "lsft_ccw_kg" and spec_lsft_ccw_kg:
                                    shift_occurred_spec = current_val > spec_lsft_ccw_kg
                                    spec_val = spec_lsft_ccw_kg
                                elif param == "lfh1_cw_kg" and spec_lfh1_cw_kg:
                                    shift_occurred_spec = current_val > spec_lfh1_cw_kg
                                    spec_val = spec_lfh1_cw_kg
                                elif param == "lfh1_ccw_kg" and spec_lfh1_ccw_kg:
                                    shift_occurred_spec = current_val > spec_lfh1_ccw_kg
                                    spec_val = spec_lfh1_ccw_kg
                                elif param == "plysteer_kg" and spec_plysteer_kg:
                                    shift_occurred_spec = current_val > spec_plysteer_kg
                                    spec_val = spec_plysteer_kg
                                elif param == "rfmxa_ccw_deg" and spec_rfmxa_ccw_deg:
                                    shift_occurred_spec = current_val > spec_rfmxa_ccw_deg
                                    spec_val = spec_rfmxa_ccw_deg
                                elif param == "rfa1_cw_deg" and spec_rfa1_cw_deg:
                                    shift_occurred_spec = current_val > spec_rfa1_cw_deg
                                    spec_val = spec_rfa1_cw_deg
                                elif param == "rfa1_ccw_deg" and spec_rfa1_ccw_deg:
                                    shift_occurred_spec = current_val > spec_rfa1_ccw_deg
                                    spec_val = spec_rfa1_ccw_deg
                                elif param == "rfa2_cw_deg" and spec_rfa2_cw_deg:
                                    shift_occurred_spec = current_val > spec_rfa2_cw_deg
                                    spec_val = spec_rfa2_cw_deg
                                elif param == "rfa2_ccw_deg" and spec_rfa2_ccw_deg:
                                    shift_occurred_spec = current_val > spec_rfa2_ccw_deg
                                    spec_val = spec_rfa2_ccw_deg
                                elif param == "lfmxa_cw_deg" and spec_lfmxa_cw_deg:
                                    shift_occurred_spec = current_val > spec_lfmxa_cw_deg
                                    spec_val = spec_lfmxa_cw_deg
                                elif param == "lfmxa_ccw_deg" and spec_lfmxa_ccw_deg:
                                    shift_occurred_spec = current_val > spec_lfmxa_ccw_deg
                                    spec_val = spec_lfmxa_ccw_deg
                                elif param == "lfa1_cw_deg" and spec_lfa1_cw_deg:
                                    shift_occurred_spec = current_val > spec_lfa1_cw_deg
                                    spec_val = spec_lfa1_cw_deg
                                elif param == "lfa1_ccw_deg" and spec_lfa1_ccw_deg:
                                    shift_occurred_spec = current_val > spec_lfa1_ccw_deg
                                    spec_val = spec_lfa1_ccw_deg
                                elif param == "static_g" and spec_static_g:
                                    shift_occurred_spec = current_val > spec_static_g
                                    spec_val = spec_static_g
                                elif param == "static_deg" and spec_static_deg:
                                    shift_occurred_spec = current_val > spec_static_deg
                                    spec_val = spec_static_deg
                                elif param == "couple_g" and spec_couple_g:
                                    shift_occurred_spec = current_val > spec_couple_g
                                    spec_val = spec_couple_g
                                elif param == "couple_deg" and spec_couple_deg:
                                    shift_occurred_spec = current_val > spec_couple_deg
                                    spec_val = spec_couple_deg
                                elif param == "botbul_mm" and spec_bul_botbul_mm:
                                    shift_occurred_spec = current_val > spec_bul_botbul_mm
                                    spec_val = spec_bul_botbul_mm
                                elif param == "topbul_mm" and spec_bul_topbul_mm:
                                    shift_occurred_spec = current_val > spec_bul_topbul_mm
                                    spec_val = spec_bul_topbul_mm
                                elif param == "botbul_deg" and spec_bul_botbul_deg:
                                    shift_occurred_spec = current_val > spec_bul_botbul_deg
                                    spec_val = spec_bul_botbul_deg
                                elif param == "topdep_deg" and spec_bul_topdep_deg:
                                    shift_occurred_spec = current_val > spec_bul_topdep_deg
                                    spec_val = spec_bul_topdep_deg
                                elif param == "botdep_mm" and spec_dep_botdep_mm:
                                    shift_occurred_spec = current_val > spec_dep_botdep_mm
                                    spec_val = spec_dep_botdep_mm
                                elif param == "topdep_mm" and spec_dep_topdep_mm:
                                    shift_occurred_spec = current_val > spec_dep_topdep_mm
                                    spec_val = spec_dep_topdep_mm
                                elif param == "botdep_deg" and spec_dep_botdep_deg:
                                    shift_occurred_spec = current_val > spec_dep_botdep_deg
                                    spec_val = spec_dep_botdep_deg
                                elif param == "topbul_deg" and spec_dep_topbul_deg:
                                    shift_occurred_spec = current_val > spec_dep_topbul_deg
                                    spec_val = spec_dep_topbul_deg
                                elif param == "crro_mm" and spec_rro_crro_mm:
                                    shift_occurred_spec = current_val > spec_rro_crro_mm
                                    spec_val = spec_rro_crro_mm
                                elif param == "crroh1_mm" and spec_rro_crroh1_mm:
                                    shift_occurred_spec = current_val > spec_rro_crroh1_mm
                                    spec_val = spec_rro_crroh1_mm
                                elif param == "blro_mm" and spec_lro_blro_mm:
                                    shift_occurred_spec = current_val > spec_lro_blro_mm
                                    spec_val = spec_lro_blro_mm
                                elif param == "blroh1_mm" and spec_lro_blroh1_mm:
                                    shift_occurred_spec = current_val > spec_lro_blroh1_mm
                                    spec_val = spec_lro_blroh1_mm
                                elif param == "tlro_mm" and spec_lro_tlro_mm:
                                    shift_occurred_spec = current_val > spec_lro_tlro_mm
                                    spec_val = spec_lro_tlro_mm
                                elif param == "tlroh1_mm" and spec_lro_tlroh1_mm:
                                    shift_occurred_spec = current_val > spec_lro_tlroh1_mm
                                    spec_val = spec_lro_tlroh1_mm
                                elif param == "crro_deg" and spec_rro_crro_deg:
                                    shift_occurred_spec = current_val > spec_rro_crro_deg
                                    spec_val = spec_rro_crro_deg
                                elif param == "crroh1_deg" and spec_rro_crroh1_deg:
                                    shift_occurred_spec = current_val > spec_rro_crroh1_deg
                                    spec_val = spec_rro_crroh1_deg
                                elif param == "blro_deg" and spec_lro_blro_deg:
                                    shift_occurred_spec = current_val > spec_lro_blro_deg
                                    spec_val = spec_lro_blro_deg
                                elif param == "blroh1_deg" and spec_lro_blroh1_deg:
                                    shift_occurred_spec = current_val > spec_lro_blroh1_deg
                                    spec_val = spec_lro_blroh1_deg
                                elif param == "tlro_deg" and spec_lro_tlro_deg:
                                    shift_occurred_spec = current_val > spec_lro_tlro_deg
                                    spec_val = spec_lro_tlro_deg
                                elif param == "tlroh1_deg" and spec_lro_tlroh1_deg:
                                    shift_occurred_spec = current_val > spec_lro_tlroh1_deg
                                    spec_val = spec_lro_tlroh1_deg
                                elif param == "botplane_g" and spec_dynamic_botplane_g:
                                    shift_occurred_spec = current_val > spec_dynamic_botplane_g
                                    spec_val = spec_dynamic_botplane_g
                                elif param == "topplane_g" and spec_dynamic_topplane_g:
                                    shift_occurred_spec = current_val > spec_dynamic_topplane_g
                                    spec_val = spec_dynamic_topplane_g
                                elif param == "botplane_deg" and spec_dynamic_botplane_deg:
                                    shift_occurred_spec = current_val > spec_dynamic_botplane_deg
                                    spec_val = spec_dynamic_botplane_deg
                                elif param == "topplane_deg" and spec_dynamic_topplane_deg:
                                    shift_occurred_spec = current_val > spec_dynamic_topplane_deg
                                    spec_val = spec_dynamic_topplane_deg

                                # elif param == "oe_yield":
                                #     spec_yield = yield_spec_mapping.get(param, 0)
                                #     shift_occurred_spec = current_val < spec_yield
                                #     spec_val = spec_yield
                                # elif param == "balancing_oe_yield":
                                #     spec_yield = yield_spec_mapping.get(param, 0)
                                #     shift_occurred_spec = current_val < spec_yield
                                #     spec_val = spec_yield
                                # elif param == "geometry_oe_yield":
                                #     spec_yield = yield_spec_mapping.get(param, 0)
                                #     shift_occurred_spec = current_val < spec_yield
                                #     spec_val = spec_yield

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
                                "gt_code": model_number,
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
                            # logging.info(f"Prepared {len(deviated_parameter_alerts)} derived alerts for machine {machine_no}, lot {current_model_lot_id}")

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
                            "gt_code": model_number,
                            "lot_id": current_model_lot_id,
                            "previous_lot_id": model_level_previous_lot_id,
                            "machine": machine_no,
                            "ufm_machine_model_lot_id": current_ufm_machine_lot_id,
                            "ufm_previous_machine_model_lot_id": ufm_previous_machine_model_lot_id,
                            "current_lot_averages": {k: round(v, 3) if v is not None and not pd.isna(v) else None for k, v in lot_averages.items()},
                            "previous_lot_averages": {k: round(v, 3) if v is not None and not pd.isna(v) else None for k, v in prev_lot_avg.items()} if prev_lot_avg else {},
                            "parameter_alerts": parameter_alerts,
                            "ufm_shift_occurred": machine_lot_overall_shift_occurred,
                            "processed_machine_barcodes": total_ufm_machine_barcodes,
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
                            "ufm_shift_occured": machine_lot_overall_shift_occurred,
                            "shifts_data": shifts_data_list,
                            "oe_yield": oe_yield(machine_batch_data),
                            "balancing_oe_yield": balancing_oe_yield(machine_batch_data),
                            "geometry_oe_yield": geometry_oe_yield(machine_batch_data),
                            "warehouse_yield": warehouse_yield(machine_batch_data),
                            "visual_yield": visual_yield(machine_batch_data),
                            "sizecode": sizecode,
                            "gt_code": str(gt_code),
                            "processed_machine_barcodes": total_ufm_machine_barcodes,
                            "machine_lot_start_ufm_time": machine_batch_data["date_of_ufm"].min() if not machine_batch_data["date_of_ufm"].empty else None,
                            "machine_lot_end_ufm_time": machine_batch_data["date_of_ufm"].max() if not machine_batch_data["date_of_ufm"].empty else None,
                            "parameter_alerts": parameter_alerts,
                            "lot_averages": {k: round(v, 3) if v is not None and not pd.isna(v) else None for k, v in lot_averages.items()},
                            "previous_lot_averages": {k: round(v, 3) if v is not None and not pd.isna(v) else None for k, v in prev_lot_avg.items()} if prev_lot_avg else {},
                            "first_barcode_machine": machine_batch_data.iloc[0]["barcode"] if not machine_batch_data.empty and "barcode" in machine_batch_data.columns else None,
                            "last_barcode_machine": machine_batch_data.iloc[-1]["barcode"] if not machine_batch_data.empty and "barcode" in machine_batch_data.columns else None,
                            "ufm_machine_model_lot_id": current_ufm_machine_lot_id,
                            "ufm_previous_machine_model_lot_id": ufm_previous_machine_model_lot_id,
                            "rank_columns": get_rank_columns(machine_batch_data),
                            "barcodes": machine_batch_data["barcode"].tolist() if "barcode" in machine_batch_data.columns else []
                        }

                        alert_doc_main = {
                            "_id": f"main_alert_{last_alert_id}",
                            "alert_id_ufm_ufm": last_alert_id,
                            "gt_code": model_number,
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
                        if not alert_ufm_collect.find_one({"alert_id_ufm_ufm": last_alert_id}):
                            ufm_alert_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(alert_doc_main)))
                            output_messages.append(f"Prepared alert_id_ufm_ufm {last_alert_id} for insertion into alert_ufm_collect")

                        trigger_doc = {
                            "_id": f"trigger_alert_{last_alert_id}",
                            "alert_id_ufm_ufm": last_alert_id,
                            "gt_code": model_number,
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
                        if not alert_triggering_collect_dashboard_ufm.find_one({"alert_id_ufm_ufm": last_alert_id}):
                            ufm_triggering_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(trigger_doc)))
                            output_messages.append(f"Prepared trigger alert_id_ufm_ufm {last_alert_id} for insertion into alert_triggering_collect_dashboard_ufm")
                        if not alert_collect.find_one({"alert_id_ufm_ufm": last_alert_id}):
                            ufm_bulk_operations.append(pymongo.InsertOne(convert_numpy_types(trigger_doc)))
                            output_messages.append(f"Prepared trigger alert_id_ufm_ufm {last_alert_id} for insertion into alert_collect")

                    except Exception as e:
                        logging.error("❌ UFM Error processing machine %s for model %s: %s", ufm_machine_id, model_number, e)
                        output_messages.append(f"❌ UFM Error processing machine {ufm_machine_id} for model {model_number}: {e}")
                        continue

        except Exception as e:
            logging.error("❌ UFM Error processing model %s: %s", model_number, e)
            output_messages.append(f"❌ UFM Error processing model {model_number}: {e}")
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
            inserted_counts["alert_ufm_without_lot_size"] = result.inserted_count
            output_messages.append(f"✅ UFM Inserted {result.inserted_count} deviated alert documents into alert_deviated_final_yield.alert_ufm_without_lot_size.")
        except Exception as e:
            error_message = f"❌ UFM Error performing bulk write to deviated alert collections: {e}"
            output_messages.append(error_message)
    else:
        output_messages.append("No UFM deviated alert documents to insert into deviated alert collections.")

    if ufm_alert_bulk_operations:
        try:
            result = alert_ufm_collect.bulk_write(ufm_alert_bulk_operations, ordered=False)
            inserted_counts["alert_ufm_collect"] = result.inserted_count
            final_log_messages.append(f"✅ UFM Inserted {result.inserted_count} documents into UFM alert collection (alert_db_virgin_yield.{alert_triggering_ubno_collect_name}).")
        except pymongo.errors.BulkWriteError as bwe:
            inserted_counts["alert_ufm_collect"] = bwe.details['nInserted']
            final_log_messages.append(f"⚠️ UFM Alert Bulk Write Errors: Inserted {bwe.details['nInserted']}. Errors: {bwe.details['writeErrors']}")
        except Exception as e:
            final_log_messages.append(f"❌ UFM Error performing bulk write to UFM alert collection: {e}")

    if ufm_triggering_bulk_operations:
        try:
            result_dashboard = alert_triggering_collect_dashboard_ufm.bulk_write(ufm_triggering_bulk_operations, ordered=False)
            inserted_counts["alert_triggering_collect_dashboard_ufm"] = result_dashboard.inserted_count
            output_messages.append(f"✅ UFM Inserted {result_dashboard.inserted_count} machine_alert documents into alert_db_final_yield.{alert_triggering_ubno_collect_name}.")
        except pymongo.errors.BulkWriteError as bwe:
            inserted_counts["alert_triggering_collect_dashboard_ufm"] = bwe.details['nInserted']
            output_messages.append(f"⚠️ UFM Triggering Alert Bulk Write Errors in alert_db_final_yield.{alert_triggering_ubno_collect_name}: Inserted {bwe.details['nInserted']}. Errors: {bwe.details['writeErrors']}")
        except Exception as e:
            output_messages.append(f"❌ UFM Error performing bulk write to alert_db_final_yield.{alert_triggering_ubno_collect_name}: {e}")
    else:
        output_messages.append(f"No UFM triggering alert documents to insert into alert_db_final_yield.{alert_triggering_ubno_collect_name}.")

    if ufm_bulk_operations:
        try:
            result = alert_collect.bulk_write(ufm_bulk_operations, ordered=False)
            inserted_counts["alert_collect"] = result.inserted_count
            final_log_messages.append(f"✅ UFM Inserted {result.inserted_count} documents into UFM triggering alert collection (alert_db_virgin_yield.{alert_ufm_collect_name}).")
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

def ensure_admin_collection_exists():  # Add 05-07
    logging.info("ensure_admin_collection_exists function is just started")
    try:
        db = client[alert_mail_db]
        collection = db[admin_table]
        alert_users_collection = db[alert_users]  # New collection for copying data                                                              
        # Check if collection exists or is empty
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
            alert_users_collection.insert_one(default_admin)  # Copy to alert_users                    
            logging.info("Default manager admin inserted.")
            return

        # Check if a manager role already exists
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
            alert_users_collection.insert_one(default_admin)  # Copy to alert_users                 
            logging.info("Default manager admin inserted into both admin_table and alert_users.")
        else:
            logging.info("At least one 'manager' user already exists in admin_table. No insert needed.")

    except Exception as e:
        logging.error(f"Error in ensure_admin_collection_exists: {str(e)}")

def main():
    # Run data cleaning
    # Insert raw data
    if not (df_balancing.empty or df_geometry.empty):
        insert_with_timestamp(df_uniformity, 'UNIFORMITY', 'UNIFORMITY')
        logging.info("Raw UNIFORMITY data inserted into UNIFORMITY database")
        insert_with_timestamp(df_balancing, 'BALANCING', 'BALANCING')
        logging.info("Raw BALANCING data inserted into BALANCING database")
        insert_with_timestamp(df_geometry, 'GEOMETRY', 'GEOMETRY')
        logging.info("Raw GEOMETRY data inserted into GEOMETRY database")
        data_processing_result = run_data_cleaning()
        lot_alert_result = process_lot_and_alert_data()
        if data_processing_result['status'] == 'success':
            print("Data cleaning completed successfully.")
        else:
            print("Data cleaning failed:", data_processing_result['message'])
            return

        # Process TBM lot and alert data
        print("Starting process_tbm_lot_and_alert_data")
        tbm_process_output_data = process_tbm_lot_and_alert_data()
        if tbm_process_output_data[0]['status'] == 'success':
            print("TBM lot and alert processing completed successfully.")
            output_messages = tbm_process_output_data[0]['output_log']
            tbm_lot_alert_results = tbm_process_output_data[0]
        else:
            print("TBM lot and alert processing failed:", tbm_process_output_data[0]['message'])
            return tbm_process_output_data
        
        # Process Curing lot and alert data
        logging.info("Starting process_curing_data_and_generate_lots_alerts")
        curing_process_output_data, curing_status_code = process_curing_data_and_generate_lots_alerts()
        if curing_process_output_data['status'] == 'success':
            logging.info("Curing lot and alert processing completed successfully.")
            output_messages.extend(curing_process_output_data['output_log'])
            curing_lot_alert_results = curing_process_output_data
        else:
            logging.info("TBM lot and alert processing failed: %s", tbm_process_output_data[0]['message'])
            return curing_process_output_data, curing_status_code
        
        logging.info("Starting process_tbm_lot_and_alert_data without lot size")
        tbm_process_output_data = process_tbm_lot_and_alert_data_without_lot_size()
        if tbm_process_output_data[0]['status'] == 'success':
            logging.info("process_tbm_lot_and_alert_data_without_lot_size and alert processing completed successfully.")
            output_messages = tbm_process_output_data[0]['output_log']
            tbm_lot_alert_results = tbm_process_output_data[0]
        else:
            logging.info("process_tbm_lot_and_alert_data_without_lot_size lot and alert processing failed: %s", tbm_process_output_data[0]['message'])
            return tbm_process_output_data
        
        # Process Curing lot and alert data
        logging.info("Starting process_curing_data_and_generate_lots_alerts_without_lot_size")
        curing_process_output_data, curing_status_code = process_curing_data_and_generate_lots_alerts_without_lot_size()
        if curing_process_output_data['status'] == 'success':
            logging.info("Curing lot and alert processing completed successfully.")
            output_messages.extend(curing_process_output_data['output_log'])
            curing_lot_alert_results = curing_process_output_data
        else:
            logging.info("process_curing_lot_and_alert_data_without_lot_size and alert processing failed: %s", curing_process_output_data[0]['message'])
            return curing_process_output_data, curing_status_code
        
        # Process ufm lot and alert data
        logging.info("Starting process_ufm_lot_and_alert_data without lot size")
        ufm_process_output_data = process_ufm_lot_and_alert_data_without_lot_size()
        if ufm_process_output_data[0]['status'] == 'success':
            logging.info("process_ufm_lot_and_alert_data_without_lot_size lot and alert processing completed successfully without lot size.")
            output_messages = ufm_process_output_data[0]['output_log']
            ufm_lot_alert_results = ufm_process_output_data[0]
        else:
            logging.info("UFM without lot and alert processing failed: %s", ufm_process_output_data[0]['message'])
            return ufm_process_output_data
    else:
        logging.info("BALANCING, GEOMETERY data is empty, skipping BALANCING, GEOMETERY processing.")

        # NOTE: The data cleaning + combined-collection rebuild (run_data_cleaning) is
        # intentionally executed ONLY ONCE above. Re-running insert_with_timestamp /
        # run_data_cleaning here would delete and rebuild
        # data_merged_combined_uniform_balan_geo, wiping the lot ids that the alert
        # functions (UFM/TBM/Curing) just stamped onto it and causing lot barcode
        # counts to shrink (e.g. a 100-barcode lot showing only ~44 barcodes).

        # Process ufm lot and alert data
        logging.info("Starting process_ufm_lot_and_alert_data without lot size")
        ufm_process_output_data = process_ufm_lot_and_alert_data_without_lot_size()
        if ufm_process_output_data[0]['status'] == 'success':
            logging.info("process_ufm_lot_and_alert_data_without_lot_size lot and alert processing completed successfully without lot size.")
            output_messages = ufm_process_output_data[0]['output_log']
            ufm_lot_alert_results = ufm_process_output_data[0]
        else:
            logging.info("process_ufm_lot_and_alert_data_without_lot_size lot and alert processing failed:", ufm_process_output_data[0]['message'])
            return ufm_process_output_data
    
    # Final success message
    print("All processes completed successfully.")
    return {
        "status": "success",
        "message": "All processes completed successfully.",
        # "output_log": output_messages,
        # "tbm_lot_alert_results": tbm_lot_alert_results
        # "curing_lot_alert_results": curing_lot_alert_results
    }

if __name__ == "__main__":
    configure_logging()
    warnings.filterwarnings("ignore")
    ensure_admin_collection_exists()
    result = main()
    logging.info(f"Processing result: {result}")
    client.close()