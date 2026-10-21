import logging
import pandas as pd
from datetime import datetime, timedelta,time as datetime_time ,timezone
import pytz
from collections import defaultdict
from pymongo import MongoClient
import numpy as np
import re
import os
# ---- Setup Logging ----
# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
# logger = logging.getLogger(__name__)
import pathlib

# ────────────────────────────────────────────────────────────────
#           Centralized Logging Setup with Daily Files
# ────────────────────────────────────────────────────────────────


def setup_dated_logging(log_dir="logs", logger_name=__name__):
    """
    Creates log folder and configures logger to write ONLY to file:
      • logs/tyre-yield-YYYY-MM-DD.log
    """

    # -------------------------------------------------
    # Create logs directory
    # -------------------------------------------------
    log_path = pathlib.Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    # -------------------------------------------------
    # Generate filename with current date
    # -------------------------------------------------
    today_str = datetime.now().strftime("%Y-%m-%d")
    log_filename = log_path / f"database_{today_str}.log"

    # -------------------------------------------------
    # Create / configure logger
    # -------------------------------------------------
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    # 🔥 CRITICAL: prevent logs going to root (terminal)
    logger.propagate = False

    # Remove any previous handlers
    if logger.hasHandlers():
        logger.handlers.clear()

    # -------------------------------------------------
    # Formatter
    # -------------------------------------------------
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # -------------------------------------------------
    # File handler ONLY
    # -------------------------------------------------
    file_handler = logging.FileHandler(log_filename, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.info(f"Logging initialized {log_filename.name}")
    return logger



# ────────────────────────────────────────────────────────────────
#                   USE IT LIKE THIS
# ────────────────────────────────────────────────────────────────

# At the very beginning of your script (after imports)
logger = setup_dated_logging(log_dir="tyre_yield_logs")
logger.error("Something bad happened", exc_info=True)

# ---- MongoDB Connections ----
# client = MongoClient("mongodb://localhost:27017/")
client = MongoClient("mongodb://admin:admin123@10.100.192.85:27017/")
# MONGO_URI = "mongodb://admin:admin123@10.100.192.123:27017/"

# Historical data
db = client["TUB_CIMCORP_merged_cleaned_data_historical_data"]
collection = db["data_mereged_tub_historical_data"]

# Active model reference
model_workcenter_db = client["model_workcenter_db"]
tbm_gtcodes = model_workcenter_db["tbm_gtcodes"]

current_time = datetime.now(timezone.utc)
################################ Yield Formula #####################
def truncate_2_decimals_regex(value):
    try:
        match = re.match(r'^-?\d+(?:\.\d{0,2})?', str(value))
        return float(match.group()) if match else 0.0
    except:
        return 0.0
    
def yield_percentage(numerator, denominator):
    return (numerator / denominator * 100) if denominator > 0 else 0

def total_rank_yield(batch_data):   # Add 05-07   
    try:
        if 'total_rank' not in batch_data.columns or 'barcode' not in batch_data.columns:
            return 0.0
        denominator_data = batch_data[(batch_data['total_rank'] > 0) & (batch_data['total_rank'] <= 5)]
        denominator = denominator_data['barcode'].nunique()
        if denominator == 0:
            return 0.0
        numerator_data = denominator_data[denominator_data['total_rank'].isin([1, 2])]
        numerator = numerator_data['barcode'].nunique()
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def rfv_yield(batch_data):   # Add 05-07
    try:
            # Filter the data based on the conditions for denominator
            denominator_data = batch_data[
                (batch_data['cwrfv_oa_rank'] > 0) &
                (batch_data['cwrfv_oa_rank'] <= 5) &
                (batch_data['ccwrfv_oa_rank'] > 0) &
                (batch_data['ccwrfv_oa_rank'] <= 5) &
                (batch_data['barcode'].notnull())
            ]
            
            # Calculate the denominator (unique barcodes)
            denominator = denominator_data['barcode'].nunique()
            
            # Filter the data based on conditions for numerator for cwrfv_oa_rank 1 and 2
            cwrfv_numerator_data = denominator_data[
                denominator_data['cwrfv_oa_rank'].isin([1, 2]) &
                denominator_data['barcode'].notnull()
            ]
            
            # Filter the data based on conditions for numerator for ccwrfv_oa_rank 1 and 2
            ccwrfv_numerator_data = denominator_data[
                denominator_data['ccwrfv_oa_rank'].isin([1, 2]) &
                denominator_data['barcode'].notnull()
            ]
            
            # Get counts of barcodes for cwrfv_oa_rank 1 & 2 and ccwrfv_oa_rank 1 & 2
            cwrfv_numerator = cwrfv_numerator_data['barcode'].nunique()
            ccwrfv_numerator = ccwrfv_numerator_data['barcode'].nunique()
            
            # Take the greater count as the numerator
            numerator = max(cwrfv_numerator, ccwrfv_numerator)
            
            # Return the yield percentage using the defined function
            return yield_percentage(numerator, denominator)
        
    except Exception as e:
            # Return 0.0 in case of an error
        return 0.0



def rfh1_yield(batch_data):   # Add 05-07
    try:
            # Filter the data based on the conditions for denominator
            denominator_data = batch_data[
                (batch_data['cwrfv_oa1h_rank'] > 0) &
                (batch_data['cwrfv_oa1h_rank'] <= 5) &
                (batch_data['ccwrfv_oa1h_rank'] > 0) &
                (batch_data['ccwrfv_oa1h_rank'] <= 5) &
                (batch_data['barcode'].notnull())
            ]
            
            # Calculate the denominator (unique barcodes)
            denominator = denominator_data['barcode'].nunique()
            
            # Filter the data based on conditions for numerator for cwrfv_oa_rank 1 and 2
            cwrfv_numerator_data = denominator_data[
                denominator_data['cwrfv_oa1h_rank'].isin([1, 2]) &
                denominator_data['barcode'].notnull()
            ]
            
            # Filter the data based on conditions for numerator for ccwrfv_oa_rank 1 and 2
            ccwrfv_numerator_data = denominator_data[
                denominator_data['ccwrfv_oa1h_rank'].isin([1, 2]) &
                denominator_data['barcode'].notnull()
            ]
            
            # Get counts of barcodes for cwrfv_oa_rank 1 & 2 and ccwrfv_oa_rank 1 & 2
            cwrfv_numerator = cwrfv_numerator_data['barcode'].nunique()
            ccwrfv_numerator = ccwrfv_numerator_data['barcode'].nunique()
            
            # Take the greater count as the numerator
            numerator = max(cwrfv_numerator, ccwrfv_numerator)
            
            # Return the yield percentage using the defined function
            return yield_percentage(numerator, denominator)
        
    except Exception as e:
            # Return 0.0 in case of an error
            return 0.0
    


def lfv_yield(batch_data):   # Add 05-07
    try:
            # Filter the data based on the conditions for denominator
            denominator_data = batch_data[
                (batch_data['lfv_oa_rank'] > 0) &
                (batch_data['lfv_oa_rank'] <= 5) &
                (batch_data['lfv_oa_1_rank'] > 0) &
                (batch_data['lfv_oa_1_rank'] <= 5) &
                (batch_data['barcode'].notnull())
            ]
            
            # Calculate the denominator (unique barcodes)
            denominator = denominator_data['barcode'].nunique()
            
            # Filter the data based on conditions for numerator for cwrfv_oa_rank 1 and 2
            cwrfv_numerator_data = denominator_data[
                denominator_data['lfv_oa_rank'].isin([1, 2]) &
                denominator_data['barcode'].notnull()
            ]
            
            # Filter the data based on conditions for numerator for ccwrfv_oa_rank 1 and 2
            ccwrfv_numerator_data = denominator_data[
                denominator_data['lfv_oa_1_rank'].isin([1, 2]) &
                denominator_data['barcode'].notnull()
            ]
            
            # Get counts of barcodes for cwrfv_oa_rank 1 & 2 and ccwrfv_oa_rank 1 & 2
            cwrfv_numerator = cwrfv_numerator_data['barcode'].nunique()
            ccwrfv_numerator = ccwrfv_numerator_data['barcode'].nunique()
            
            # Take the greater count as the numerator
            numerator = max(cwrfv_numerator, ccwrfv_numerator)
            
            # Return the yield percentage using the defined function
            return yield_percentage(numerator, denominator)
        
    except Exception as e:
            # Return 0.0 in case of an error
            return 0.0
    
def conicity_yield(batch_data):        # Add 05-07
    try:
        denominator_data = batch_data[(batch_data['con_rank'] > 0) & (batch_data['con_rank'] <= 5)]
        denominator = denominator_data['barcode'].nunique()
        numerator_data = denominator_data[
            (denominator_data['con_rank'].isin([1, 2])) &
            denominator_data['barcode'].notnull()
        ]
        numerator = numerator_data['barcode'].nunique()
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def ufm_yield(batch_data):         # Add 05-07
    try:
        denominator_data = batch_data[(batch_data['ufm_rank'] > 0) & (batch_data['ufm_rank'] <= 5)]
        denominator = denominator_data['barcode'].nunique()
        numerator_data = denominator_data[
            (denominator_data['ufm_rank'].isin([1, 2])) &
            denominator_data['barcode'].notnull()
        ]
        numerator = numerator_data['barcode'].nunique()
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def radial_runout_yield(batch_data):       # Add 05-07 geo
    try:
        denominator_data = batch_data[(batch_data['rc_oa_rank'] > 0) & (batch_data['rc_oa_rank'] <= 5)]
        denominator = denominator_data['barcode'].nunique()
        numerator_data = denominator_data[denominator_data['rc_oa_rank'].isin([1, 2])]
        numerator = numerator_data['barcode'].nunique()
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def radial_runout_first_harmonic_yield(batch_data):      # Add 05-07 geo
    try:
        denominator_data = batch_data[(batch_data['rc_oa1h_rank'] > 0) & (batch_data['rc_oa1h_rank'] <= 5)]
        denominator = denominator_data['barcode'].nunique()
        numerator_data = denominator_data[denominator_data['rc_oa1h_rank'].isin([1, 2])]
        numerator = numerator_data['barcode'].nunique()
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def rorank_yield(batch_data):        # Add 05-07 geo
    try:
        denominator_data = batch_data[(batch_data['rorank'] > 0) & (batch_data['rorank'] <= 5)]
        denominator = denominator_data['barcode'].nunique()
        numerator_data = denominator_data[
            (denominator_data['rorank'].isin([1, 2])) &
            denominator_data['barcode'].notnull()
        ]
        numerator = numerator_data['barcode'].nunique()
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def lateral_run_out_top_over(batch_data):        # Add 05-07 geo
    try:
        denominator_data = batch_data[(batch_data['lt_oa_rank'] > 0) & (batch_data['lt_oa_rank'] <= 5)]
        denominator = denominator_data['barcode'].nunique()
        numerator_data = denominator_data[denominator_data['lt_oa_rank'].isin([1, 2])]
        numerator = numerator_data['barcode'].nunique()
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def lateral_run_out_bottom_over(batch_data):      # Add 05-07 geo
    try:
        denominator_data = batch_data[(batch_data['lb_oa_rank'] > 0) & (batch_data['lb_oa_rank'] <= 5)]
        denominator = denominator_data['barcode'].nunique()
        numerator_data = denominator_data[denominator_data['lb_oa_rank'].isin([1, 2])]
        numerator = numerator_data['barcode'].nunique()
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def lateral_run_out_yield(batch_data):     # Add 05-07 geo
    try:
        denominator_data = batch_data[(batch_data['lateral_run_out_rank'] > 0) & (batch_data['lateral_run_out_rank'] <= 5)]
        denominator = denominator_data['barcode'].nunique()
        numerator_data = denominator_data[denominator_data['lateral_run_out_rank'].isin([1, 2])]
        numerator = numerator_data['barcode'].nunique()
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def lateral_run_out_bottom_first_harmonic(batch_data):  # Add 05-07 geo
    try:
        denominator_data = batch_data[(batch_data['lb_oa1h_rank'] > 0) & (batch_data['lb_oa1h_rank'] <= 5)]
        denominator = denominator_data['barcode'].nunique()
        numerator_data = denominator_data[denominator_data['lb_oa1h_rank'].isin([1, 2])]
        numerator = numerator_data['barcode'].nunique()
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def lateral_run_out_top_first_harmonic(batch_data):   # Add 05-07 geo
    try:
        denominator_data = batch_data[(batch_data['lt_oa1h_rank'] > 0) & (batch_data['lt_oa1h_rank'] <= 5)]
        denominator = denominator_data['barcode'].nunique()
        numerator_data = denominator_data[denominator_data['lt_oa1h_rank'].isin([1, 2])]
        numerator = numerator_data['barcode'].nunique()
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def lateral_run_out_harmonic_yield(batch_data):    # Add 05-07 geo
    try:
        denominator_data = batch_data[(batch_data['lateral_run_out_harmonic_rank'] > 0) & (batch_data['lateral_run_out_harmonic_rank'] <= 5)]
        denominator = denominator_data['barcode'].nunique()
        numerator_data = denominator_data[denominator_data['lateral_run_out_harmonic_rank'].isin([1, 2])]
        numerator = numerator_data['barcode'].nunique()
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def bulge_top_over(batch_data):      # Add 05-07 geo
    try:
        denominator_data = batch_data[(batch_data['ltbulg_rank'] > 0) & (batch_data['ltbulg_rank'] <= 5)]
        denominator = denominator_data['barcode'].nunique()
        numerator_data = denominator_data[denominator_data['ltbulg_rank'].isin([1, 2])]
        numerator = numerator_data['barcode'].nunique()
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def bulge_bottom_over(batch_data):        # Add 05-07 geo
    try:
        denominator_data = batch_data[(batch_data['lbbulg_rank'] > 0) & (batch_data['lbbulg_rank'] <= 5)]
        denominator = denominator_data['barcode'].nunique()
        numerator_data = denominator_data[denominator_data['lbbulg_rank'].isin([1, 2])]
        numerator = numerator_data['barcode'].nunique()
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def bulge_yield(batch_data):      # Add 05-07 geo
    try:
        denominator_data = batch_data[(batch_data['bulge_rank'] > 0) & (batch_data['bulge_rank'] <= 5)]
        denominator = denominator_data['barcode'].nunique()
        numerator_data = denominator_data[denominator_data['bulge_rank'].isin([1, 2])]
        numerator = numerator_data['barcode'].nunique()
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def dent_top_over(batch_data):     # Add 05-07 geo
    try:
        denominator_data = batch_data[(batch_data['dent_rank'] > 0) & (batch_data['dent_rank'] <= 5)]
        denominator = denominator_data['barcode'].nunique()
        numerator_data = denominator_data[denominator_data['dent_rank'].isin([1, 2])]
        numerator = numerator_data['barcode'].nunique()
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def dent_bottom_over(batch_data):        # Add 05-07 geo
    try:
        denominator_data = batch_data[(batch_data['dent1_rank'] > 0) & (batch_data['dent1_rank'] <= 5)]
        denominator = denominator_data['barcode'].nunique()
        numerator_data = denominator_data[denominator_data['dent1_rank'].isin([1, 2])]
        numerator = numerator_data['barcode'].nunique()
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def dent_yield(batch_data):   # Add 05-07 geo
    try:
        denominator_data = batch_data[(batch_data['dent_overall_rank'] > 0) & (batch_data['dent_overall_rank'] <= 5)]
        denominator = denominator_data['barcode'].nunique()
        numerator_data = denominator_data[denominator_data['dent_overall_rank'].isin([1, 2])]
        numerator = numerator_data['barcode'].nunique()
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def balancing_yield(batch_data):     # Add 05-07 bal
    try:
        denominator_data = batch_data[(batch_data['bal_rank'] > 0) & (batch_data['bal_rank'] <= 5)]
        denominator = denominator_data['barcode'].nunique()
        numerator_data = denominator_data[denominator_data['bal_rank'].isin([1, 2])]
        numerator = numerator_data['barcode'].nunique()
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def upper_yield(batch_data):    # Add 05-07 bal
    try:
        denominator_data = batch_data[(batch_data['upper_rank'] > 0) & (batch_data['upper_rank'] <= 5)]
        denominator = denominator_data['barcode'].nunique()
        numerator_data = denominator_data[denominator_data['upper_rank'].isin([1, 2])]
        numerator = numerator_data['barcode'].nunique()
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def lower_yield(batch_data):    # Add 05-07 bal
    try:
        denominator_data = batch_data[(batch_data['lower_rank'] > 0) & (batch_data['lower_rank'] <= 5)]
        denominator = denominator_data['barcode'].nunique()
        numerator_data = denominator_data[denominator_data['lower_rank'].isin([1, 2])]
        numerator = numerator_data['barcode'].nunique()
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def upper_lower_yield(batch_data):    # Add 05-07 bal
    try:
        denominator_data = batch_data[(batch_data['up_low_rank'] > 0) & (batch_data['up_low_rank'] <= 5)]
        denominator = denominator_data['barcode'].nunique()
        numerator_data = denominator_data[denominator_data['up_low_rank'].isin([1, 2])]
        numerator = numerator_data['barcode'].nunique()
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def static_yield(batch_data):    # Add 05-07 bal
    try:
        denominator_data = batch_data[(batch_data['static_rank'] > 0) & (batch_data['static_rank'] <= 5)]
        denominator = denominator_data['barcode'].nunique()
        numerator_data = denominator_data[denominator_data['static_rank'].isin([1, 2])]
        numerator = numerator_data['barcode'].nunique()
        return yield_percentage(numerator, denominator)
    except:
        return 0.0

def warehouse_yield(batch_data):    # Add 05-07
    try:
        # Initialize variables
        numerator = 0
        denominator = 0
 
        # Check for uftesting_result column first
        if 'uftesting_result' in batch_data.columns:
            try:
                oe_data = batch_data[batch_data['uftesting_result'].isin(['OE1', 'Repl'])]
                denominator = len(oe_data['barcode'])
            except Exception as e:
                raise ValueError(f"Error processing uftesting_result column: {e}")
        
        # Fallback to uftesting_result_tub if uftesting_result is not available
        elif 'uftesting_result_tub' in batch_data.columns:
            try:
                oe_data = batch_data[batch_data['uftesting_result_tub'].isin(['OE1', 'Repl'])]
                denominator = len(oe_data['barcode'])
            except Exception as e:
                raise ValueError(f"Error processing uftesting_result column: {e}")

        else:
            raise ValueError("Neither 'uftesting_result' nor 'uftesting_result_tub' columns are available in the data.")
 
        # Check for uftesting_result column first
        if 'uftesting_result' in batch_data.columns:
            try:
                oe_data = batch_data[batch_data['uftesting_result'] == 'OE1']
                # numerator = oe_data['barcode'].nunique()
                numerator = len(oe_data['barcode'])
            except Exception as e:
                raise ValueError(f"Error processing uftesting_result column: {e}")
        # Fallback to uftesting_result_tub if uftesting_result is not available
        elif 'uftesting_result_tub' in batch_data.columns:
            try:
                oe_data = batch_data[batch_data['uftesting_result_tub'] == 'OE1']
                # numerator = oe_data['barcode'].nunique()
                numerator = len(oe_data['barcode'])
            except Exception as e:
                raise ValueError(f"Error processing uftesting_result_tub column: {e}")
        else:
            raise ValueError("Neither 'uftesting_result' nor 'uftesting_result_tub' columns are available in the data.")
 
        # Call the yield_percentage function to get the yield percentage
        try:
            return yield_percentage(numerator, denominator)
        except Exception as e:
            raise ValueError(f"Error calculating yield percentage: {e}")
 
    except Exception as e:
        # Log the error and return None or a default value
        logging.info("Error in warehouse_yield: %s", e)
        return None
def visual_yield(batch_data):
    try:
        # Filter for total_rank between 1 and 5
        if 'operation' in batch_data.columns:
            denominator_data = batch_data[
                (batch_data['operation'].notna()) |
                (batch_data['operation'].isna()) |
                (~batch_data['operation'].isin(['Unknown', 'None']))
            ]

            denominator = len(denominator_data['barcode'])
        elif 'operation_tub' in batch_data.columns:
            denominator_data = batch_data[
                (batch_data['operation_tub'].notna()) |
                (batch_data['operation_tub'].isna()) |
                (~batch_data['operation_tub'].isin(['Unknown', 'None']))
            ]

            denominator = len(denominator_data['barcode'])

        # Check for available column: operation or operation_tub
        if 'operation' in batch_data.columns:
            numerator_data = denominator_data[
                (denominator_data['operation'].notna()) &
                (~denominator_data['operation'].isin(['Unknown', 'None']))
            ]
        elif 'operation_tub' in batch_data.columns:
            numerator_data = denominator_data[
                (denominator_data['operation_tub'].notna()) &
                (~denominator_data['operation_tub'].isin(['Unknown', 'None']))
            ]
        else:
            return 0.0  # Skip if neither column exists

        numerator = len(numerator_data['barcode'])

        # Apply yield formula
        return (1 - (numerator / denominator)) * 100 if denominator > 0 else 0.0

    except Exception as e:
        logging.info("Error in visual_yield: %s", e)
        return 0.0

# ----------------------------------------------------------------------
#  Helper – centralised “no data” handling
# ----------------------------------------------------------------------
def _empty_df_with_msg(msg: str, logger_obj=logger):
    """Log a message and return an empty DataFrame + None timestamps."""
    logger_obj.warning(msg)
    return pd.DataFrame(), None, None, None

# ---- Functions ----
def get_tbm_models():
    """
    Fetch only active modelno values from model_workcenter_db.tbm_gtcodes
    """
    try:
        active_models_cursor = tbm_gtcodes.find({"status": "active"}, {"modelno": 1, "_id": 0})
        active_models = {doc["modelno"] for doc in active_models_cursor if "modelno" in doc}
        return active_models
    except Exception as e:
        logger.error(f"Error fetching active tbm models: {str(e)}")
        return set()


def remove_duplicate_barcodes(df, timestamp_col="date_and_time"):
    """
    Remove duplicate barcodes for the same modelno, keeping the latest timestamp.
    """
    try:
        if df.empty or "barcode" not in df.columns or "modelno" not in df.columns:
            logger.warning(f"No valid data to process duplicates in  collection")
            return df

        # Sort by timestamp (latest first)
        df_sorted = df.sort_values(by=timestamp_col, ascending=True)

        # Drop duplicates based on barcode + modelno
        df_deduped = df_sorted.drop_duplicates(subset=["barcode", "modelno"], keep="first")
        return df_deduped

    except Exception as e:
        logger.error(f"Error removing duplicates in  collection: {str(e)}")
        return df

def load_and_process_daily_data(collection, tbm_gtcodes, current_date):
    """
    Load and process data for the daily window (07:00 → next day 06:59:59).
    Returns the processed DataFrame.
    """
    yesterday = current_date - timedelta(days=1)
    start_time = datetime(yesterday.year, yesterday.month, yesterday.day, 7, 0, 0)
    end_time = datetime(current_date.year, current_date.month, current_date.day, 6, 59, 59)

    query = {
        "date_and_time": {
            "$gte": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "$lte": end_time.strftime("%Y-%m-%d %H:%M:%S")
        }
    }

    cursor = collection.find(query)
    data = list(cursor)
    if not data:                                                 # <--- NEW
        return _empty_df_with_msg("No data found for the daily window (07:00 → 06:59)")
    df = pd.DataFrame(data)

    if df.empty:
        logger.warning("No records found for the given date range.")
    else:

        # Filter only active modelno
        active_models = get_tbm_models()
        df = df[df["modelno"].isin(active_models)]
   
        # Remove duplicate barcodes
        df = remove_duplicate_barcodes(df, timestamp_col="date_and_time")
    
    return df, start_time, end_time, yesterday

def load_and_process_daily_data_dash(collection_df, current_date):
    """
    Load and process data for the daily window (07:00 → next day 06:59:59).
    Returns the processed DataFrame.
    """
    yesterday = current_date - timedelta(days=1)
    start_time = datetime(yesterday.year, yesterday.month, yesterday.day, 7, 0, 0)
    end_time = datetime(current_date.year, current_date.month, current_date.day, 6, 59, 59)


    query = {
        "date_and_time": {
            "$gte": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "$lte": end_time.strftime("%Y-%m-%d %H:%M:%S")
        }
    }
    collection_df = client["TUB_CIMCORP_merged_cleaned_data_historical_data"]["data_mereged_dashboard_historical_data"]

    cursor = collection_df.find(query)
    data = list(cursor)
    if not data:                                                 # <--- NEW
        return _empty_df_with_msg("No dash data found for the daily window")
    df = pd.DataFrame(data)

    if df.empty:
        logger.warning("No records found for the given date range.")
    else:
   
        # Filter only active modelno
        active_models = get_tbm_models()
        df = df[df["modelno"].isin(active_models)]

        # Remove duplicate barcodes
        df = remove_duplicate_barcodes(df, timestamp_col="date_and_time")

    return df, start_time, end_time, yesterday


def load_and_process_7day_data(collection, tbm_gtcodes, from_date, yesterday):
    """
    Load and process data for the last 7 days.
    - Fetch full calendar days
    - Filter active models
    - Apply remove_duplicate_barcodes PER DAY (7AM → 7AM)
    """
    today = yesterday + timedelta(days=1)
    query_7days = {
        "date_and_time": {
            "$gte": datetime(
                from_date.year, from_date.month, from_date.day, 0, 0, 0
            ).strftime("%Y-%m-%d %H:%M:%S"),
            "$lte": datetime(
                today.year, today.month, today.day, 7, 0, 0
            ).strftime("%Y-%m-%d %H:%M:%S")
        }
    }
    cursor_7days = collection.find(query_7days)
    data_7days = list(cursor_7days)
    if not data_7days:
        return _empty_df_with_msg("No records found for last 7 days")
    df = pd.DataFrame(data_7days)
    if df.empty:
        logger.warning("No records found for last 7 days")
        return df
    # Convert datetime
    df["date_and_time"] = pd.to_datetime(df["date_and_time"], errors="coerce")
    # Filter active models
    active_models = set(map(str, get_tbm_models()))
    df["modelno"] = df["modelno"].astype(str)
    df = df[df["modelno"].isin(active_models)]
    # ---- APPLY DUPLICATE REMOVAL PER DAY (7AM → 7AM) ----
    cleaned_frames = []
    for i in range(7):
        day_start = from_date + timedelta(days=i)
        next_day = day_start + timedelta(days=1)
        start_dt = datetime(day_start.year, day_start.month, day_start.day, 7, 0, 0)
        end_dt = datetime(next_day.year, next_day.month, next_day.day, 7, 0, 0)
        day_df = df[
            (df["date_and_time"] >= start_dt) &
            (df["date_and_time"] < end_dt)
        ]
        if day_df.empty:
            continue
        day_df = remove_duplicate_barcodes(
            day_df,
            timestamp_col="date_and_time"
        )
        cleaned_frames.append(day_df)

    if not cleaned_frames:
        return pd.DataFrame()
    data_7days = pd.concat(cleaned_frames, ignore_index=True)
    return data_7days

def load_previous_previous_day_df(collection, tbm_gtcodes, current_date):
    """
    Load and process data for the previous-previous day's window (07:00 → next day 06:59:59).
    """
    prev_prev_day = current_date - timedelta(days=2)  # 2 days ago
    start_time = datetime(prev_prev_day.year, prev_prev_day.month, prev_prev_day.day, 7, 0, 0)
    end_time = datetime(prev_prev_day.year, prev_prev_day.month, prev_prev_day.day, 23, 59, 59) + timedelta(hours=7)
    # OR simpler:
    end_time = start_time + timedelta(hours=23, minutes=59, seconds=59)

    query = {
        "date_and_time": {
            "$gte": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "$lte": end_time.strftime("%Y-%m-%d %H:%M:%S")
        }
    }

    cursor = collection.find(query)
    data = list(cursor)
    if not data:                                                 # <--- NEW
        return _empty_df_with_msg("No records found for the previous-previous day")
    df = pd.DataFrame(data)

    if df.empty:
        logger.warning("No records found for the previous-previous day.")
    else:

        # Filter only active modelno
        active_models = get_tbm_models()
        df = df[df["modelno"].isin(active_models)]
     
        # Remove duplicate barcodes
        df = remove_duplicate_barcodes(df, timestamp_col="date_and_time")
    
    return df, start_time, end_time, prev_prev_day


def store_model_metrics(df, client, start_time, end_time, yesterday):
    """
    Calculate and store model metrics.
    """
    production_db = client['production_db']
    channel_collection = production_db["channel_no_curing"]
    curing_skus = {doc['modelno']: doc.get('sizecode', '') for doc in channel_collection.find({}, {"modelno": 1, "sizecode": 1, "_id": 0})}



    model_metrics = {}
    for modelno in df['modelno'].unique():
        model_df = df[df['modelno'] == modelno]
        total_tyres = model_df['barcode'].nunique() if not model_df.empty else 0
        oe_tyres = model_df[model_df['total_rank'].isin([1, 2])]['barcode'].nunique() if not model_df.empty else 0
        rejected_tyres = model_df[model_df['total_rank'].isin([4, 5])]['barcode'].nunique() if not model_df.empty else 0
        replacement_tyres = model_df[model_df['total_rank'] == 3]['barcode'].nunique() if not model_df.empty else 0
        sku = curing_skus.get(str(modelno), '')
        
        model_metrics[str(modelno)] = {
            "total_tyres": total_tyres,
            "oe_tyres": oe_tyres,
            "rejected_tyres": rejected_tyres,
            "replacement_tyres": replacement_tyres,
            "sku": sku
        }

    home_db = client["home_historical_data"]
    model_metrics_collection = home_db["model_metrics"]

    doc_to_store = {
        "date": yesterday.strftime("%Y-%m-%d"),
        "from_datetime": start_time.strftime("%Y-%m-%d %H:%M:%S"),
        "to_datetime": end_time.strftime("%Y-%m-%d %H:%M:%S"),
        "model_metrics": model_metrics
    }

    model_metrics_collection.insert_one(doc_to_store)


def store_oe_yield_metrics(df, client, start_time, end_time, yesterday):
    """
    Calculate and store OE yield metrics.
    """
    oe_yield_metrics = {}

    # ✅ Include all active modelnos (even if no data in df)
    active_models = get_tbm_models()

    for modelno in active_models:
        # Filter for this modelno
        model_df = df[df['modelno'] == modelno] if not df.empty else pd.DataFrame()

        if not model_df.empty:
            warehouse_yield_value = warehouse_yield(model_df)
            rfv_yield_value = rfv_yield(model_df)
            lfv_yield_value = lfv_yield(model_df)
            conicity_yield_value = conicity_yield(model_df)
            rorank_yield_value = rorank_yield(model_df)
            radial_runout_yield_value = radial_runout_yield(model_df)
            bulge_yield_value = bulge_yield(model_df)
            dent_yield_value = dent_yield(model_df)
            ufm_yield_value = ufm_yield(model_df)
            static_yield_value = static_yield(model_df)
            total_rank_yield_value = total_rank_yield(model_df)
            balancing_yield_value = balancing_yield(model_df)
            visual_yield_value = visual_yield(model_df)
            lro_bottom_v = lateral_run_out_bottom_first_harmonic(model_df)
            lro_top_v = lateral_run_out_top_first_harmonic(model_df)
            upper_yield_v = upper_yield(model_df)
            lower_yield_v = lower_yield(model_df)
            upper_lower_yield_v = upper_lower_yield(model_df)

            lro_yield_value = max(lro_bottom_v, lro_top_v)
            dynamic_balancing_yield_value = max(upper_yield_v, lower_yield_v, upper_lower_yield_v)
        else:
            # ✅ If no data found for active model, set all yields to 0.0
            warehouse_yield_value = 0.0
            rfv_yield_value = 0.0
            lfv_yield_value = 0.0
            conicity_yield_value = 0.0
            rorank_yield_value = 0.0
            radial_runout_yield_value = 0.0
            bulge_yield_value = 0.0
            dent_yield_value = 0.0
            ufm_yield_value = 0.0
            static_yield_value = 0.0
            total_rank_yield_value = 0.0
            balancing_yield_value = 0.0
            visual_yield_value = 0.0
            lro_yield_value = 0.0
            dynamic_balancing_yield_value = 0.0

        oe_yield_metrics[str(modelno)] = {
            "warehouse_yield": truncate_2_decimals_regex(warehouse_yield_value),
            "rfv_yield": truncate_2_decimals_regex(rfv_yield_value),
            "lfv_yield": truncate_2_decimals_regex(lfv_yield_value),
            "conicity_yield": truncate_2_decimals_regex(conicity_yield_value),
            "geometry_variation_yield": truncate_2_decimals_regex(rorank_yield_value),
            "rro_yield": truncate_2_decimals_regex(radial_runout_yield_value),
            "bulge_yield": truncate_2_decimals_regex(bulge_yield_value),
            "dent_yield": truncate_2_decimals_regex(dent_yield_value),
            "force_variation_yield": truncate_2_decimals_regex(ufm_yield_value),
            "static_yield": truncate_2_decimals_regex(static_yield_value),
            "total_rank_yield": truncate_2_decimals_regex(total_rank_yield_value),
            "mass_variation_yield": truncate_2_decimals_regex(balancing_yield_value),
            "lro_yield": truncate_2_decimals_regex(lro_yield_value),
            "dynamic_balancing_yield": truncate_2_decimals_regex(dynamic_balancing_yield_value),
            "visual_yield": truncate_2_decimals_regex(visual_yield_value)
        }

    # Save results in MongoDB
    home_db = client["home_historical_data"]
    oe_yield_collection = home_db["oe_yield_tree_diagram"]

    oe_yield_doc = {
        "date": yesterday.strftime("%Y-%m-%d"),
        "from_datetime": start_time.strftime("%Y-%m-%d %H:%M:%S"),
        "to_datetime": end_time.strftime("%Y-%m-%d %H:%M:%S"),
        "oe_yield_metrics": oe_yield_metrics
    }

    oe_yield_collection.insert_one(oe_yield_doc)

def compute_yield_with_counts(batch_data, parameter):
    try:
        denominator_data = batch_data[(batch_data['total_rank'] > 0) & (batch_data['total_rank'] < 6)]
        denominator = denominator_data['barcode'].nunique()
        # numerator = 0
        if parameter == "rfv_yield":
            # -----------------------------------
            # 1. Denominator (same as before)
            # -----------------------------------
            denominator_data = batch_data[
                ((batch_data['cwrfv_oa_rank'] > 0) & (batch_data['cwrfv_oa_rank'] <= 5)) &
                ((batch_data['ccwrfv_oa_rank'] > 0) & (batch_data['ccwrfv_oa_rank'] <= 5)) &
                batch_data['barcode'].notnull()
            ]
            denominator = denominator_data['barcode'].nunique()
       
            # -----------------------------------
            # 2. Numerator for cwrfv (rank 1 or 2)
            # -----------------------------------
            numerator_cwrfv = denominator_data[
                (denominator_data['cwrfv_oa_rank'].isin([1, 2])) &
                denominator_data['barcode'].notnull()
            ]['barcode'].nunique()
       
            # -----------------------------------
            # 3. Numerator for ccwrfv (rank 1 or 2)
            # -----------------------------------
            numerator_ccwrfv = denominator_data[
                (denominator_data['ccwrfv_oa_rank'].isin([1, 2])) &
                denominator_data['barcode'].notnull()
            ]['barcode'].nunique()
       
            # -----------------------------------
            # 4. Final Numerator = MAX of both counts
            # -----------------------------------
            numerator = max(numerator_cwrfv, numerator_ccwrfv)
        elif parameter == "lfv_yield":
            # -----------------------------------
            # 1. Denominator (same as before)
            # -----------------------------------
            denominator_data = batch_data[
                ((batch_data['lfv_oa_rank'] > 0) & (batch_data['lfv_oa_rank'] <= 5)) &
                ((batch_data['lfv_oa_1_rank'] > 0) & (batch_data['lfv_oa_1_rank'] <= 5)) &
                batch_data['barcode'].notnull()
            ]
            denominator = denominator_data['barcode'].nunique()
       
            # -----------------------------------
            # 2. Numerator for cwrfv (rank 1 or 2)
            # -----------------------------------
            numerator_cwrfv = denominator_data[
                (denominator_data['lfv_oa_rank'].isin([1, 2])) &
                denominator_data['barcode'].notnull()
            ]['barcode'].nunique()
       
            # -----------------------------------
            # 3. Numerator for ccwrfv (rank 1 or 2)
            # -----------------------------------
            numerator_ccwrfv = denominator_data[
                (denominator_data['lfv_oa_1_rank'].isin([1, 2])) &
                denominator_data['barcode'].notnull()
            ]['barcode'].nunique()
       
            # -----------------------------------
            # 4. Final Numerator = MAX of both counts
            # -----------------------------------
            numerator = max(numerator_cwrfv, numerator_ccwrfv)
       
        elif parameter == "conicity_yield":
            # --- Denominator (same as function conicity_yield) ---
            denominator_data = batch_data[
                ((batch_data['con_rank'] > 0) & (batch_data['con_rank'] <= 5))&
                batch_data['barcode'].notnull()
            ]
            denominator = denominator_data['barcode'].nunique()
            # --- Numerator (same as function conicity_yield) ---
            numerator_data = denominator_data[
                (denominator_data['con_rank'].isin([1, 2])) &
                denominator_data['barcode'].notnull()
            ]
            numerator = numerator_data['barcode'].nunique()
        elif parameter == "geometry_variation_yield":
            # --- Denominator (same as function geometry_variation_yield) ---
            denominator_data = batch_data[
                ((batch_data['rorank'] > 0) & (batch_data['rorank'] <= 5))&
                batch_data['barcode'].notnull()
            ]
            denominator = denominator_data['barcode'].nunique()
            # --- Numerator (same as function geometry_variation_yield) ---
            numerator_data = denominator_data[
                (denominator_data['rorank'].isin([1, 2])) &
                denominator_data['barcode'].notnull()
            ]
            numerator = numerator_data['barcode'].nunique()
   
        elif parameter == "radial_runout_first_harmonic_yield":
            # --- Denominator (same as function radial_runout_first_harmonic_yield) ---
            denominator_data = batch_data[
                ((batch_data['rc_oa1h_rank'] > 0) & (batch_data['rc_oa1h_rank'] <= 5))&
                batch_data['barcode'].notnull()
            ]
            denominator = denominator_data['barcode'].nunique()
            # --- Numerator (same as function radial_runout_first_harmonic_yield) ---
            numerator_data = denominator_data[
                (denominator_data['rc_oa1h_rank'].isin([1, 2])) &
                denominator_data['barcode'].notnull()
            ]
            numerator = numerator_data['barcode'].nunique()
        elif parameter == "lro_yield":
            denominator_data = batch_data[
                ((batch_data['lb_oa1h_rank'] > 0) & (batch_data['lb_oa1h_rank'] <= 5))&
                batch_data['barcode'].notnull()
            ]
            denominator = denominator_data['barcode'].nunique()
            # Bottom calculation
            bottom_data = denominator_data[denominator_data['lb_oa1h_rank'].isin([1, 2])]
            bottom_numerator = bottom_data['barcode'].nunique()
            # Top calculation
            top_data = denominator_data[denominator_data['lt_oa1h_rank'].isin([1, 2])]
            top_numerator = top_data['barcode'].nunique()
            # Pick the greater one
            numerator = max(bottom_numerator, top_numerator)
       
        elif parameter == "mass_variation_yield":
            # --- Denominator (same as function mass_variation_yield) ---
            denominator_data = batch_data[
                ((batch_data['bal_rank'] > 0) & (batch_data['bal_rank'] <= 5))&
                batch_data['barcode'].notnull()
            ]
            denominator = denominator_data['barcode'].nunique()
            # --- Numerator (same as function mass_variation_yield) ---
            numerator_data = denominator_data[
                (denominator_data['bal_rank'].isin([1, 2])) &
                denominator_data['barcode'].notnull()
            ]
            numerator = numerator_data['barcode'].nunique()
        elif parameter == "bulge_yield":
            # --- Denominator (same as function bulge_yield) ---
            denominator_data = batch_data[
                ((batch_data['bulge_rank'] > 0) & (batch_data['bulge_rank'] <= 5))&
                batch_data['barcode'].notnull()
            ]
            denominator = denominator_data['barcode'].nunique()
            # --- Numerator (same as function bulge_yield) ---
            numerator_data = denominator_data[
                (denominator_data['bulge_rank'].isin([1, 2])) &
                denominator_data['barcode'].notnull()
            ]
            numerator = numerator_data['barcode'].nunique()
        elif parameter == "dent_yield":
            # --- Denominator (same as function dent_yield) ---
            denominator_data = batch_data[
                ((batch_data['dent_overall_rank'] > 0) & (batch_data['dent_overall_rank'] <= 5))&
                batch_data['barcode'].notnull()
            ]
            denominator = denominator_data['barcode'].nunique()
            # --- Numerator (same as function dent_yield) ---
            numerator_data = denominator_data[
                (denominator_data['dent_overall_rank'].isin([1, 2])) &
                denominator_data['barcode'].notnull()
            ]
            numerator = numerator_data['barcode'].nunique()
  
        elif parameter == "force_variation_yield":
            # --- Denominator (same as function force_variation_yield) ---
            denominator_data = batch_data[
                ((batch_data['ufm_rank'] > 0) & (batch_data['ufm_rank'] <= 5))&
                batch_data['barcode'].notnull()
            ]
            denominator = denominator_data['barcode'].nunique()
            # --- Numerator (same as function force_variation_yield) ---
            numerator_data = denominator_data[
                (denominator_data['ufm_rank'].isin([1, 2])) &
                denominator_data['barcode'].notnull()
            ]
            numerator = numerator_data['barcode'].nunique()
        elif parameter == "rro_yield":
            # --- Denominator (same as function rro_yield) ---
            denominator_data = batch_data[
                ((batch_data['rc_oa_rank'] > 0) & (batch_data['rc_oa_rank'] <= 5))&
                batch_data['barcode'].notnull()
            ]
            denominator = denominator_data['barcode'].nunique()
            # --- Numerator (same as function rro_yield) ---
            numerator_data = denominator_data[
                (denominator_data['rc_oa_rank'].isin([1, 2])) &
                denominator_data['barcode'].notnull()
            ]
            numerator = numerator_data['barcode'].nunique()
        elif parameter == "static_yield":
            # --- Denominator (same as function static_yield) ---
            denominator_data = batch_data[
                ((batch_data['static_rank'] > 0) & (batch_data['static_rank'] <= 5))&
                batch_data['barcode'].notnull()
            ]
            denominator = denominator_data['barcode'].nunique()
            # --- Numerator (same as function static_yield) ---
            numerator_data = denominator_data[
                (denominator_data['static_rank'].isin([1, 2])) &
                denominator_data['barcode'].notnull()
            ]
            numerator = numerator_data['barcode'].nunique()
        elif parameter == "dynamic_balancing_yield":
            denominator_data = batch_data[
                ((batch_data['upper_rank'] > 0) & (batch_data['upper_rank'] <= 5))&
                batch_data['barcode'].notnull()
            ]
            denominator = denominator_data['barcode'].nunique()
           
            # Upper
            upper_data = denominator_data[denominator_data['upper_rank'].isin([1, 2])]
            upper_numerator = upper_data['barcode'].nunique()
            # Lower
            lower_data = denominator_data[denominator_data['lower_rank'].isin([1, 2])]
            lower_numerator = lower_data['barcode'].nunique()
            # Upper-Lower
            up_low_data = denominator_data[denominator_data['up_low_rank'].isin([1, 2])]
            up_low_numerator = up_low_data['barcode'].nunique()
            # Pick the maximum among them
            numerator = max(upper_numerator, lower_numerator, up_low_numerator)
        elif parameter == "warehouse_yield":
            # denominator_data = batch_data[(batch_data['total_rank'] > 0) & (batch_data['total_rank'] < 4)]
            # denominator = denominator_data['barcode'].nunique()
            if 'uftesting_result' in batch_data.columns:
                try:
                    oe_data = batch_data[batch_data['uftesting_result'].isin(['OE1', 'Repl'])]
                    denominator = len(oe_data['barcode'])
                except Exception as e:
                    raise ValueError(f"Error processing uftesting_result column: {e}")
            # Fallback to uftesting_result_tub if uftesting_result is not available
            elif 'uftesting_result_tub' in batch_data.columns:
                try:
                    oe_data = batch_data[batch_data['uftesting_result_tub'].isin(['OE1', 'Repl'])]
                    denominator = len(oe_data['barcode'])
                except Exception as e:
                    raise ValueError(f"Error processing uftesting_result column: {e}")
            else:
                raise ValueError("Neither 'uftesting_result' nor 'uftesting_result_tub' columns are available in the data.")
            if 'uftesting_result' in batch_data.columns:
                numerator_data = batch_data[batch_data['uftesting_result'] == 'OE1']
            elif 'uftesting_result_tub' in batch_data.columns:
                numerator_data = batch_data[batch_data['uftesting_result_tub'] == 'OE1']
            else:
                numerator_data = pd.DataFrame()
            numerator = numerator_data['barcode'].nunique()
        elif parameter == "visual_yield":
            if 'operation' in batch_data.columns:
                denominator_data = batch_data[
                    (batch_data['operation'].notna()) |
                    (batch_data['operation'].isna()) |
                    (~batch_data['operation'].isin(['Unknown', 'None']))
                ]

                denominator = len(denominator_data['barcode'])
            elif 'operation_tub' in batch_data.columns:
                denominator_data = batch_data[
                    (batch_data['operation_tub'].notna()) |
                    (batch_data['operation_tub'].isna()) |
                    (~batch_data['operation_tub'].isin(['Unknown', 'None']))
                ]

                denominator = len(denominator_data['barcode'])
            if 'operation' in denominator_data.columns:
                numerator_data = denominator_data[
                        (denominator_data['operation'].notna()) &
                        (~denominator_data['operation'].isin(['Unknown', 'None']))
                    ]
            elif 'operation_tub' in denominator_data.columns:
                    numerator_data = denominator_data[
                        (denominator_data['operation_tub'].notna()) &
                        (~denominator_data['operation_tub'].isin(['Unknown', 'None']))
                    ]
            else:
                return 0.0
            numerator = numerator_data['barcode'].nunique()
            # print("visualyield",numerator, denominator)
            yield_value = ((1 - (numerator / denominator)) * 100) if denominator > 0 else 0.0
            # print("visualyield",numerator, denominator, yield_value)
            return {"numerator": numerator, "denominator": denominator, "yield": yield_value}
        elif parameter == "total_rank_yield":
            numerator_data = denominator_data[denominator_data['total_rank'].isin([1, 2])]
            numerator = numerator_data['barcode'].nunique()
        yield_value = yield_percentage(numerator, denominator)
        return {"numerator": numerator, "denominator": denominator, "yield": yield_value}
    except Exception as e:
        logger.error(f"Error in compute_yield_with_counts({parameter}): {str(e)}")
        return {"numerator": 0, "denominator": 0, "yield": 0.0}
    
def store_flowchart_machinewise_data(df, client, start_time, end_time, yesterday):
    """
    Calculate and store flowchart machine-wise data.
    Uses compute_yield_with_counts(parameter) for consistent logic with the API.
    """
    # List of parameters we want to compute (should match API usage)
    parameters = [
        "warehouse_yield",
        "rfv_yield",
        "lfv_yield",
        "conicity_yield",
        "geometry_variation_yield",
        "force_variation_yield",
        "bulge_yield",
        "dent_yield",
        "static_yield",
        "total_rank_yield",
        "mass_variation_yield",
        "rro_yield",
        "lro_yield",
        "dynamic_balancing_yield",
        "visual_yield"
    ]

    # Defensive setup
    if df is None:
        df = pd.DataFrame()
    if "modelno" not in df.columns:
        df["modelno"] = pd.Series(dtype=str)

    df = df.copy()
    df["modelno"] = df["modelno"].astype(str).replace(["nan", "NaN", "None"], "")

    active_models = {str(m) for m in (get_tbm_models() or set())}

    flowchart_machinewise_data = {}

    for modelno in active_models:
        model_df = df[df["modelno"] == modelno]
        param_metrics = {}

        if model_df.empty:
            # Zero-fill for missing active models
            for param in parameters:
                param_metrics[param] = {
                    "tbm": {},
                    "ufm": {},
                    "curing": {},
                    "numerator_total_count": 0,
                    "denominator_total_count": 0
                }
            flowchart_machinewise_data[modelno] = param_metrics
            logger.debug(f"No data for active model {modelno} → stored zeros")
            continue

        for param_name in parameters:
            try:
                # Total level (whole model)
                total_result = compute_yield_with_counts(model_df, param_name)
                num_total   = total_result["numerator"]
                den_total   = total_result["denominator"]
                y_total     = total_result["yield"]

            except Exception as e:
                logger.exception(f"Error computing total {param_name} for model {modelno}: {e}")
                num_total = den_total = 0
                y_total = 0.0

            tbm_dict = {}
            ufm_dict = {}
            curing_dict = {}

            # ─── TBM groups ───────────────────────────────────────
            if "cimcorp_tbm_tub" in model_df.columns:
                for tbm_name, group in model_df.groupby("cimcorp_tbm_tub", dropna=False):
                    if group.empty:
                        continue
                    try:
                        res = compute_yield_with_counts(group, param_name)
                        tbm_dict[tbm_name] = {
                            "count": res["denominator"],
                            "count_numerator": res["numerator"],
                            "yields": round(res["yield"], 2)
                        }
                    except Exception as e:
                        logger.exception(f"{param_name} failed on TBM {tbm_name} (model {modelno})")
                        tbm_dict[tbm_name] = {"count": 0, "yields": 0.0}

            # ─── UFM groups ───────────────────────────────────────
            if "ubno" in model_df.columns:
                for ufm_name, group in model_df.groupby("ubno", dropna=False):
                    if group.empty:
                        continue
                    try:
                        res = compute_yield_with_counts(group, param_name)
                        ufm_dict[ufm_name] = {
                            "count": res["denominator"],
                            "count_numerator": res["numerator"],
                            "yields": round(res["yield"], 2)
                        }
                    except Exception as e:
                        logger.exception(f"{param_name} failed on UFM {ufm_name} (model {modelno})")
                        ufm_dict[ufm_name] = {"count": 0, "yields": 0.0}

            # ─── Curing groups ────────────────────────────────────
            if "curing_press_tub" in model_df.columns:
                for curing_name, group in model_df.groupby("curing_press_tub", dropna=False):
                    if group.empty:
                        continue
                    try:
                        res = compute_yield_with_counts(group, param_name)
                        curing_dict[curing_name] = {
                            "count": res["denominator"],
                            "count_numerator": res["numerator"],
                            "yields": round(res["yield"], 2)
                        }
                    except Exception as e:
                        logger.exception(f"{param_name} failed on curing {curing_name} (model {modelno})")
                        curing_dict[curing_name] = {"count": 0, "yields": 0.0}

            param_metrics[param_name] = {
                "tbm": tbm_dict,
                "ufm": ufm_dict,
                "curing": curing_dict,
                "numerator_total_count": int(num_total),
                "denominator_total_count": int(den_total),
                # optional: "yield_total_percent": round(y_total, 2)
            }

        flowchart_machinewise_data[modelno] = param_metrics

    # ─── Save to MongoDB ───────────────────────────────────────────────
    home_db = client["home_historical_data"]
    collection = home_db["flowchart_machinewise_data"]

    doc = {
        "date": yesterday.strftime("%Y-%m-%d"),
        "from_datetime": start_time.strftime("%Y-%m-%d %H:%M:%S"),
        "to_datetime": end_time.strftime("%Y-%m-%d %H:%M:%S"),
        "flowchart_machinewise_data": flowchart_machinewise_data
    }

    collection.insert_one(doc)

def store_yields_trend(df_7days, client, from_date, yesterday):
    """
    Calculate and store 7-day yield trends per parameter / model / date.
    Uses compute_yield_with_counts() for consistent logic across the system.
    Machine-level 'count' = real unique barcodes processed on that machine.
    """
    date_list = [(from_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]

    # Same parameters as in store_flowchart_machinewise_data
    parameters = [
        "warehouse_yield",
        "rfv_yield",
        "lfv_yield",
        "conicity_yield",
        "geometry_variation_yield",
        "force_variation_yield",
        "bulge_yield",
        "dent_yield",
        "static_yield",
        "total_rank_yield",
        "mass_variation_yield",
        "rro_yield",
        "lro_yield",
        "dynamic_balancing_yield",
        "visual_yield"
    ]

    # Defensive dataframe setup
    if df_7days is None:
        df_7days = pd.DataFrame()

    df = df_7days.copy()

    # Ensure date_and_time is datetime
    if "date_and_time" in df.columns:
        df["date_and_time"] = pd.to_datetime(df["date_and_time"], errors="coerce")
    else:
        df["date_and_time"] = pd.NaT

    # Normalize modelno
    if "modelno" not in df.columns:
        df["modelno"] = ""
    df["modelno"] = df["modelno"].astype(str).replace(["nan", "NaN", "None"], "")

    active_models = {str(m) for m in (get_tbm_models() or set())}

    final_trends = {}

    # Pre-group by model for efficiency
    model_to_df = {m: df[df["modelno"] == m] for m in active_models}

    for param_name in parameters:
        modelno_dict = {}

        for modelno in active_models:
            model_df_all = model_to_df.get(modelno, pd.DataFrame())
            trend_list = []

            for date_str in date_list:
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                next_day = date_obj + timedelta(days=1)

                start_dt = datetime(date_obj.year, date_obj.month, date_obj.day, 7, 0, 0)
                end_dt   = datetime(next_day.year,  next_day.month,  next_day.day,  7, 0, 0)

                mask = (model_df_all["date_and_time"] >= start_dt) & (model_df_all["date_and_time"] < end_dt)
                day_df = model_df_all[mask]

                if day_df.empty:
                    trend_entry = {
                        "date": date_str,
                        "tbm": {},
                        "ufm": {},
                        "curing_press": {},
                        "yield": 0.0,
                        "denominator_total_count": 0
                    }
                    trend_list.append(trend_entry)
                    continue

                # ─── Total for the day ───────────────────────────────────────
                try:
                    total_res = compute_yield_with_counts(day_df, param_name)
                    day_yield = round(total_res["yield"], 2)
                    day_denom = total_res["denominator"]
                except Exception as e:
                    logger.exception(f"{param_name} total failed - model {modelno} - {date_str}: {e}")
                    day_yield = 0.0
                    day_denom = 0

                # ─── Machine breakdowns ──────────────────────────────────────
                tbm_dict = {}
                if "cimcorp_tbm_tub" in day_df.columns:
                    for tbm_name, group in day_df.groupby("cimcorp_tbm_tub", dropna=False):
                        if group.empty:
                            continue
                        try:
                            res = compute_yield_with_counts(group, param_name)
                            tbm_dict[str(tbm_name)] = {
                                "count_numerator":   res["numerator"],
                                "count": res["denominator"],               # ← real unique barcodes
                                "yields": truncate_2_decimals_regex(res["yield"])
                            }
                        except Exception as e:
                            logger.exception(f"{param_name} TBM {tbm_name} failed - {modelno} - {date_str}")
                            tbm_dict[str(tbm_name)] = {"count": 0, "yields": 0.0}

                ufm_dict = {}
                if "ubno" in day_df.columns:
                    for ufm_name, group in day_df.groupby("ubno", dropna=False):
                        if group.empty:
                            continue
                        try:
                            res = compute_yield_with_counts(group, param_name)
                            ufm_dict[str(ufm_name)] = {
                                "count_numerator":   res["numerator"],
                                "count": res["denominator"],
                                "yields": truncate_2_decimals_regex(res["yield"])
                            }
                        except Exception as e:
                            logger.exception(f"{param_name} UFM {ufm_name} failed - {modelno} - {date_str}")
                            ufm_dict[str(ufm_name)] = {"count": 0, "yields": 0.0}

                curing_dict = {}
                if "curing_press_tub" in day_df.columns:
                    for curing_name, group in day_df.groupby("curing_press_tub", dropna=False):
                        if group.empty:
                            continue
                        try:
                            res = compute_yield_with_counts(group, param_name)
                            curing_dict[str(curing_name)] = {
                                "count_numerator":   res["numerator"],
                                "count": res["denominator"],
                                "yields": truncate_2_decimals_regex(res["yield"])
                            }
                        except Exception as e:
                            logger.exception(f"{param_name} curing {curing_name} failed - {modelno} - {date_str}")
                            curing_dict[str(curing_name)] = {"count": 0, "yields": 0.0}

                trend_entry = {
                    "date": date_str,
                    "tbm": tbm_dict,
                    "ufm": ufm_dict,
                    "curing_press": curing_dict,
                    "yield": day_yield,
                    "denominator_total_count": day_denom
                }
                trend_list.append(trend_entry)

            modelno_dict[modelno] = trend_list

        final_trends[param_name] = modelno_dict

    # ─── Save document ─────────────────────────────────────────────────────
    trend_doc = {
        "date": yesterday.strftime("%Y-%m-%d"),
        "trend": final_trends
    }

    home_db = client["home_historical_data"]
    yields_trend_collection = home_db["yields_trend"]

    yields_trend_collection.insert_one(trend_doc)

def get_prod_date(ts):
    """
    Returns production date string for 7AM–7AM window.
    - If time >= 7:00 → same date
    - If time < 7:00 → previous date
    """
    if ts.hour >= 7:
        return ts.strftime("%Y-%m-%d")
    else:
        return (ts - timedelta(days=1)).strftime("%Y-%m-%d")

def store_overall_yield(df_7days, client, from_date, yesterday):
    """
    Calculate and store overall yield for all models with 7-day trend.
    """
    production_db = client['production_db']
    channel_collection = production_db["channel_no_curing"]

    overall_yield_collection = client["home_historical_data"]["overall_yield"]
    now_ts = datetime.now(pytz.timezone("Asia/Kolkata"))

    df_7days["prod_date"] = df_7days["date_and_time"].apply(get_prod_date)

    date_list = [(from_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]

    # ✅ Get all active models
    active_models = get_tbm_models()

    all_models_data = []

    # ✅ Iterate over ALL active models (not just those in df_7days)
    for modelno in active_models:
        trend_dict = defaultdict(dict)

        # Filter records for this model
        model_df = df_7days[df_7days["modelno"] == modelno]

        for dt in date_list:
            group = model_df[model_df["prod_date"] == dt]
            if not group.empty:
                trend_dict["ufm"][dt] = float(truncate_2_decimals_regex(ufm_yield(group)))
                trend_dict["bal"][dt] = float(truncate_2_decimals_regex(balancing_yield(group)))
                trend_dict["oe"][dt] = float(truncate_2_decimals_regex(total_rank_yield(group)))
                trend_dict["ro"][dt] = float(truncate_2_decimals_regex(rorank_yield(group)))
            else:
                # ✅ If no data for that date or model, assign 0.0
                trend_dict["ufm"][dt] = 0.0
                trend_dict["bal"][dt] = 0.0
                trend_dict["oe"][dt] = 0.0
                trend_dict["ro"][dt] = 0.0

        # ✅ Fetch additional info from channel collection
        channel_rec = channel_collection.find_one({"modelno": modelno}) or {}
        gt_code_val = channel_rec.get("gt_code", [])
        if isinstance(gt_code_val, str):
            gt_code_val = [gt_code_val]

        model_doc = {
            "modelno": modelno,
            "trend": trend_dict,
            "spec_yield": float(channel_rec.get("spec_yield", 0)),
            "sku": str(channel_rec.get("sku", "")),
            "gt_code": gt_code_val,
            "sizecode": str(channel_rec.get("sizecode", ""))
        }

        all_models_data.append(model_doc)

    overall_doc = {
        "date": yesterday.strftime("%Y-%m-%d"),
        "models": all_models_data,
        "last_updated": now_ts
    }

    overall_yield_collection.insert_one(overall_doc)

def store_warehouse_yield_tree(df, client, start_time, end_time, yesterday):
    """
    Calculate and store warehouse_yield_tree metrics (same structure as API output).
    One document per day is inserted into the 'warehouse_yield_tree' collection.
    """
    try:

        if df.empty:
            logger.warning("No data found for warehouse_yield_tree computation.")
            metrics_doc = {
                "date": yesterday.strftime("%Y-%m-%d"),
                "from_datetime": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "to_datetime": end_time.strftime("%Y-%m-%d %H:%M:%S"),
                "tub_metrics": {
                    "total_tyres": 0,
                    "oe_tyres": 0,
                    "rejected_tyres": 0,
                    "replacement_tyres": 0,
                    "average_cycle_time": 0
                },
                "collection_averages": [],
                "overall_averages": {},
                "model_averages": [],
                "status": "success"
            }
            client["home_historical_data"]["warehouse_yield_tree"].insert_one(metrics_doc)
            return

        # ---------- Step 1: Basic metrics ----------
        active_models = df["modelno"].dropna().unique().tolist()
        ufm_models = get_tbm_models()

        valid_df = df[
            (df['barcode'].notnull()) &
            (df['total_rank'].isin([1, 2, 3, 4, 5])) &
            (df['modelno'].isin(ufm_models))
        ].copy()

        total_tyres = valid_df['barcode'].nunique()
        oe_tyres = valid_df[valid_df['total_rank'].isin([1, 2])]['barcode'].nunique()
        rejected_tyres = valid_df[valid_df['total_rank'].isin([4, 5])]['barcode'].nunique()
        replacement_tyres = valid_df[valid_df['total_rank'] == 3]['barcode'].nunique()
        avg_cycle = truncate_2_decimals_regex(valid_df['cycletime'].mean()) if 'cycletime' in valid_df.columns and not valid_df.empty else 0

        # ---------- Step 2: Collection-level averages ----------
        def calculate_yield_stats(df_slice, yield_type):
            if df_slice.empty:
                return {"average_total_rank_yield": 0.0, "record_count": 0, "type": yield_type}
            return {
                "average_total_rank_yield": truncate_2_decimals_regex(total_rank_yield(df_slice)),
                "record_count": len(df_slice),
                "type": yield_type
            }

        # Load dash_df and process same as df
        dash_df, _, _, _ = load_and_process_daily_data_dash(client, yesterday + timedelta(days=1))

        # Ensure 'barcode' exists
        if 'barcode' not in dash_df.columns:
            dash_df['barcode'] = None

        # Only keep active models & valid total_rank
        valid_dash_df = dash_df[
            dash_df['barcode'].notnull() &
            dash_df.get('total_rank', pd.Series([0]*len(dash_df))).isin([1,2,3,4,5]) &
            dash_df.get('modelno', pd.Series(['']*len(dash_df))).isin(ufm_models)
        ].copy()


        # Calculate collection averages using slices for tbm, ubno, curing
        collection_averages = [
            calculate_yield_stats(valid_dash_df, "tbm"),   # TBM slice
            calculate_yield_stats(df, "ubno"),             # UBNO slice
            calculate_yield_stats(valid_dash_df, "curing") # Curing slice
        ]


        # ---------- Step 3: Overall averages ----------
        def get_spec_yield_avg():
            try:
                all_yields = []
                for collection_name in ["channel_no_curing", "channel_no_tbm", "channel_no_ufm"]:
                    yields = list(client["production_db"][collection_name].find({}, {"spec_yield": 1, "_id": 0}))
                    all_yields.extend(y["spec_yield"] for y in yields if "spec_yield" in y)
                return truncate_2_decimals_regex(np.mean(all_yields)) if all_yields else 0.0
            except Exception as e:
                logger.error(f"Error computing spec_yield_avg: {e}")
                return 0.0

        def get_warehouse_yield_avg():
            try:
                model_df = df.copy()
                # denom = model_df[model_df.get('uftesting_result_tub', '').isin[('OE1','Repl']['barcode'].nunique()
                denom = model_df[model_df['uftesting_result_tub'].isin(['OE1', 'Repl'])]
                denom = len(denom['barcode'])
                # denom = model_df[(model_df['uftesting_result_tub'] > 0) & (model_df['total_rank'] <= 5)]['barcode'].nunique()
                if denom == 0:
                    return 0.0
                num = model_df[model_df.get('uftesting_result_tub', '') == 'OE1']['barcode'].nunique()
                return truncate_2_decimals_regex((num / denom) * 100)
            except Exception as e:
                logger.error(f"Error computing warehouse_yield_avg: {e}")
                return 0.0

        def get_visual_yield_avg():
            try:
                model_df = df.copy()
                # denom = model_df[(model_df['total_rank'] > 0) & (model_df['total_rank'] <= 5)]['barcode'].nunique()
                denominator_data = model_df[
                (model_df['operation_tub'].notna()) |
                (model_df['operation_tub'].isna()) |
                (~model_df['operation_tub'].isin(['Unknown', 'None']))
                ]
    
                denom = len(denominator_data['barcode'])
                if denom == 0:
                    return 0.0
                num = model_df[(model_df['operation_tub'].notna()) &
                               (~model_df['operation_tub'].isin(['Unknown', 'None']))]['barcode'].nunique()
                return truncate_2_decimals_regex((1 - (num / denom)) * 100)
            except Exception as e:
                logger.error(f"Error computing visual_yield_avg: {e}")
                return 0.0
          # ---------- Step 3b: Previous-previous day OE yield ----------
        try:
            prev_prev_df, _, _, _ = load_previous_previous_day_df(
                client["TUB_CIMCORP_merged_cleaned_data_historical_data"]["data_mereged_tub_historical_data"],
                tbm_gtcodes,
                current_date=current_date
            )

            # If dataframe is empty → yield = 0
            if prev_prev_df.empty:
                prev_day_oe_yield_avg = 0.0
            else:
                # Use your total_rank_yield function
                prev_day_oe_yield_avg = total_rank_yield(prev_prev_df)

        except Exception as e:
            logger.error(f"Error computing prev_day_oe_yield_avg: {e}")
            prev_day_oe_yield_avg = 0.0




        overall_averages = {
            "spec_yield_avg": get_spec_yield_avg(),
            "oe_yield_avg": total_rank_yield(df),
            "warehouse_yield_avg": get_warehouse_yield_avg(),
            "visual_yield_avg": get_visual_yield_avg(),
            "prev_day_oe_yield_avg": prev_day_oe_yield_avg
        }

        # ---------- Step 4: Model-level averages ----------
        model_meta = {
            m["modelno"]: {
                "sizecode": m.get("sizecode", ""),
                "sku": m.get("sku", ""),
                "spec_yield": m.get("spec_yield")
            }
            for m in client["production_db"]["channel_no_ufm"].find({}, {"modelno": 1, "sizecode": 1, "sku": 1, "spec_yield": 1, "_id": 0})
        }

        model_averages = []
        for modelno in ufm_models:
            model_df = df[df["modelno"] == modelno]
            record_count = model_df["barcode"].nunique()
            model_averages.append({
                "average_total_rank_yield": truncate_2_decimals_regex(total_rank_yield(model_df)) if record_count > 0 else 0.0,
                "modelno": str(modelno),
                "sizecode": model_meta.get(modelno, {}).get("sizecode", ""),
                "record_count": record_count,
                "sku": model_meta.get(modelno, {}).get("sku", ""),
                "spec_yield": model_meta.get(modelno, {}).get("spec_yield", None)
            })
        tub_date_start_time = {
            "from_date": start_time.strftime("%Y-%m-%d"),
            "time": f"({start_time.strftime('%I:%M %p')} - {end_time.strftime('%I:%M %p')})",
            "to_date": end_time.strftime("%Y-%m-%d")
        }


        # ---------- Step 5: Final document ----------
        doc = {
            "date": yesterday.strftime("%Y-%m-%d"),
            "from_datetime": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "to_datetime": end_time.strftime("%Y-%m-%d %H:%M:%S"),
            "tub_metrics": {
                "total_tyres": total_tyres,
                "oe_tyres": oe_tyres,
                "rejected_tyres": rejected_tyres,
                "replacement_tyres": replacement_tyres,
                "average_cycle_time": avg_cycle
            },
            "collection_averages": collection_averages,
            "overall_averages": overall_averages,
            "model_averages": model_averages,
            "tub_date_start_time": tub_date_start_time,
            "status": "success"
        }

        # ---------- Step 6: Store document ----------
        db = client["home_historical_data"]
        collection = db["warehouse_yield_tree"]

        # Insert new every day (no delete needed if inserting fresh; but kept delete for safety as per original)
        collection.delete_many({"date": yesterday.strftime("%Y-%m-%d")})
        collection.insert_one(doc)

    except Exception as e:
        logger.error(f"Error in store_warehouse_yield_tree: {str(e)}", exc_info=True)


DAILY_YIELD_HISTORY = "daily_yield_historical_data"
LATEST_YIELD = "latest_yield"
# Updated calculate_and_store_previous_day_yield function
def calculate_and_store_previous_day_yield(current_time):
    logger.info("calculate_and_store_previous_day_yield function is just started")
    output_messages = []
    try:
        # Use the passed current_time
        india_tz = pytz.timezone('Asia/Kolkata')
        current_date = current_time.date()
        
        # Define the previous day window: 7 AM previous day to 7 AM current day
        end_time = current_date
        start_time = current_date - timedelta(days=1)
        start_datetime = india_tz.localize(datetime.combine(start_time, datetime_time(7, 0, 0)))
        end_datetime = india_tz.localize(datetime.combine(end_time, datetime_time(7, 0, 0)))
        
        # Query the data_mereged_tub_new collection for records in the time window
        db = client['TUB_CIMCORP_merged_cleaned_data_historical_data']  # Updated database
        collection = db['data_mereged_tub_historical_data']  # Updated collection
        # query = {
        #     "date_and_time": {
        #         "$gte": start_datetime.isoformat(),
        #         "$lt": end_datetime.isoformat()
        #     }
        # }
        start_str = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end_datetime.strftime("%Y-%m-%d %H:%M:%S")

        query = {
            "date_and_time": {
                "$gte": start_str,
                "$lt": end_str
            }
        }

        records = list(collection.find(query, {"total_rank": 1, "barcode": 1, "date_and_time": 1, "modelno":1}))
        
        # Convert records to DataFrame for total_rank_yield
        batch_data = pd.DataFrame(records)

        # Remove duplicate barcodes → keep earliest date_and_time
        if not batch_data.empty:
            batch_data['date_and_time'] = pd.to_datetime(batch_data['date_and_time'])

            # Sort by timestamp (ascending → earliest first)
            batch_data = batch_data.sort_values(by="date_and_time", ascending=True)

            # Drop duplicates → keep FIRST timestamp for each barcode
            batch_data = batch_data.drop_duplicates(subset=["barcode"], keep="first")

                # Get active models
        active_models = get_tbm_models()

        # Filter batch_data to only include rows with active modelnos
        batch_data = batch_data[batch_data["modelno"].isin(active_models)]
 

        
        # Calculate yield using total_rank_yield
        if batch_data.empty:
            yield_percentage = 0.0
            output_messages.append("No records found for the previous day window.")
        else:
            yield_percentage = truncate_2_decimals_regex(total_rank_yield(batch_data))
            output_messages.append(f"Calculated yield: {yield_percentage:.2f}% for {start_datetime.strftime('%Y-%m-%d')} to {end_datetime.strftime('%Y-%m-%d')}")

        # Store in daily_yield_history collection
        daily_yield_collection = db[DAILY_YIELD_HISTORY]

        spec_doc = db["yield_spec"].find_one({}, {"spec": 1, "_id": 0})
        if not spec_doc or "spec" not in spec_doc:
            raise ValueError("Spec value not found in yield_spec collection")

        spec_value = spec_doc["spec"] # default 95 if missing

        yield_entry = {
            "yield_percentage": yield_percentage,
            "yield_flag": yield_percentage < spec_value,  # <-- Added flag
            "timestamp": current_time,
            "start_date": start_datetime.strftime('%Y-%m-%d'),
            "end_date": end_datetime.strftime('%Y-%m-%d')
        }
        daily_yield_collection.insert_one(yield_entry)
        output_messages.append(
            f"Stored yield {yield_percentage:.2f}% (flag: {yield_entry['yield_flag']}) for "
            f"{start_datetime.strftime('%Y-%m-%d')} to {end_datetime.strftime('%Y-%m-%d')} in {DAILY_YIELD_HISTORY}"
        )

        
        # Update latest_yield collection if yield is non-zero
        if yield_percentage > 0:
            latest_yield_collection = db[LATEST_YIELD]
            latest_yield_collection.delete_many({})  # Clear existing document
            latest_yield_entry = {
                "yield_percentage": yield_percentage,
                "date": start_datetime.strftime('%Y-%m-%d'),
                "timestamp": current_time
            }
            latest_yield_collection.insert_one(latest_yield_entry)
            output_messages.append(f"Updated {LATEST_YIELD} with yield {yield_percentage:.2f}% for date {start_datetime.strftime('%Y-%m-%d')}")
        else:
            output_messages.append(f"Yield is 0.0%, not updating {LATEST_YIELD}")
        
    except Exception as e:
        error_message = f"Error in calculate_and_store_previous_day_yield: {str(e)}"
        output_messages.append(error_message)
        logging.error(error_message)
    
    logger.info("calculate_and_store_previous_day_yield function is completed")
    return output_messages


def run_daily_modelwise_monthly_yield_job(client, current_date):
    """
    Runs DAILY.
    Uses current_date passed from main (already IST-aware).
    Stores cumulative modelwise yield from:
        1st of current month 07:00:00
        → today 07:00:00
    Month auto-resets.
    """

    try:
        now = current_date  # already Asia/Kolkata

        # --------------------------------------------------
        # STEP 1: Define 7AM window
        # --------------------------------------------------

        today_7am = now.replace(hour=7, minute=0, second=0, microsecond=0)

        # If job runs before 7AM, shift to previous production day
        if now < today_7am:
            today_7am -= timedelta(days=1)

        # If it is exactly 1st 7AM, we are closing previous month
        if today_7am.day == 1:
            previous_day = today_7am - timedelta(days=1)
            month_start_7am = previous_day.replace(day=1)
        else:
            month_start_7am = today_7am.replace(day=1)

        from_datetime = month_start_7am
        to_datetime   = today_7am

        logger.info(
            f"Daily Monthly Window: {from_datetime} → {to_datetime}"
        )

        # --------------------------------------------------
        # STEP 2: Fetch active models (UNCHANGED FUNCTION)
        # --------------------------------------------------

        active_models = get_tbm_models()
        if not active_models:
            logger.warning("No active TBM models found, skipping job")
            return

        # --------------------------------------------------
        # STEP 3: Mongo Query
        # --------------------------------------------------

        src_collection = client[
            "TUB_CIMCORP_merged_cleaned_data_historical_data"
        ]["data_mereged_tub_historical_data"]

        query = {
            "modelno": {"$in": list(active_models)},
            "date_and_time": {
                "$gte": from_datetime.strftime("%Y-%m-%d %H:%M:%S"),
                "$lt":  to_datetime.strftime("%Y-%m-%d %H:%M:%S")
            }
        }

        df_month = pd.DataFrame(list(src_collection.find(query)))

        if df_month.empty:
            logger.warning("No data found for monthly cumulative window")
            return

        # --------------------------------------------------
        # STEP 4: Normalize + Deduplicate (UNCHANGED FUNCTION)
        # --------------------------------------------------

        df_month["date_and_time"] = pd.to_datetime(
            df_month["date_and_time"], errors="coerce"
        )

        df_month = remove_duplicate_barcodes(
            df_month,
            timestamp_col="date_and_time"
        )

        # --------------------------------------------------
        # STEP 5: Modelwise Aggregation
        # --------------------------------------------------

        models_dict = {}

        for modelno, model_df in df_month.groupby("modelno"):
            if model_df.empty:
                continue

            models_dict[str(modelno)] = {
                "warehouse_yield": round(warehouse_yield(model_df) or 0, 2),
                "visual_yield": round(visual_yield(model_df), 2),
                "total_rank_yield": round(total_rank_yield(model_df), 2),
            }

        if not models_dict:
            logger.warning("No modelwise yield generated")
            return

        # --------------------------------------------------
        # STEP 6: Store (Upsert per Month)
        # --------------------------------------------------

        month_key = from_datetime.strftime("%Y-%m")

        payload = {
            "month": month_key,
            "from_datetime": from_datetime.strftime("%Y-%m-%d %H:%M:%S"),
            "to_datetime": to_datetime.strftime("%Y-%m-%d %H:%M:%S"),
            "models": models_dict,
            "last_updated": now.strftime("%Y-%m-%d %H:%M:%S")
        }

        tgt_collection = client[
            "home_historical_data"
        ]["monthly_modelwise_data"]

        tgt_collection.update_one(
            {"month": month_key},
            {"$set": payload},
            upsert=True
        )

        logger.info(
            f"Daily cumulative monthly modelwise yield stored for {month_key}"
        )

    except Exception:
        logger.exception("Daily monthly modelwise yield job failed")


# ---- Main Execution ----
if __name__ == "__main__":

        current_date = datetime.now(pytz.timezone('Asia/Kolkata'))
                        # ---- 1. Load the *daily* window -------------------------------------------------
        df_daily, start_time, end_time, yesterday = load_and_process_daily_data(
                collection, tbm_gtcodes, current_date
                        )

                        # If the daily window is empty we **stop everything**
        if df_daily.empty and start_time is None:
                    logger.warning("Daily data missing – skipping all downstream calculations.")
        else:
                            # ---- 2. Load the 7-day window ------------------------------------------------
                            # df_7days = load_and_process_7day_data(
                            #     collection, tbm_gtcodes, start_time, end_time
                            # )
                            # Compute 7-day range
                    from_date_for_trend = yesterday - timedelta(days=6)

                    df_7days = load_and_process_7day_data(
                        collection, tbm_gtcodes, from_date_for_trend, yesterday
                            )


                            # ---- 3. Store everything ----------------------------------------------------
                    store_model_metrics(df_daily, client, start_time, end_time, yesterday)
                    store_oe_yield_metrics(df_daily, client, start_time, end_time, yesterday)
                    store_flowchart_machinewise_data(df_daily, client, start_time, end_time, yesterday)

                    store_yields_trend(df_7days, client, from_date_for_trend, yesterday)
                    store_overall_yield(df_7days, client, from_date_for_trend, yesterday)

                    store_warehouse_yield_tree(df_daily, client, start_time, end_time, yesterday)

                            # ---- 4. Always run the previous-day yield ------------------------------------
                    logger.info("Running previous-day yield calculation…")
                    yield_messages = calculate_and_store_previous_day_yield(current_date)
                    logger.info("Yield calculation output:\n" + "\n".join(yield_messages))
                            # ✅ NEW DAILY MONTHLY-CUMULATIVE JOB
                    run_daily_modelwise_monthly_yield_job(client, current_date)


    
    #   current_dates = [

    # ]

    # for current_date in current_dates:

    #     print("\nRunning for date:", current_date.date())



    # OCT
#     [
#     datetime(2025, 10, 1, 7, 0, 0),
#     datetime(2025, 10, 2, 7, 0, 0),
#     datetime(2025, 10, 3, 7, 0, 0),
#     datetime(2025, 10, 4, 7, 0, 0),
#     datetime(2025, 10, 5, 7, 0, 0),
#     datetime(2025, 10, 6, 7, 0, 0),
#     datetime(2025, 10, 7, 7, 0, 0),
#     datetime(2025, 10, 8, 7, 0, 0),
#     datetime(2025, 10, 9, 7, 0, 0),
#     datetime(2025, 10, 10, 7, 0, 0),
#     datetime(2025, 10, 11, 7, 0, 0),
#     datetime(2025, 10, 12, 7, 0, 0),
#     datetime(2025, 10, 13, 7, 0, 0),
#     datetime(2025, 10, 14, 7, 0, 0),
#     datetime(2025, 10, 15, 7, 0, 0),
#     datetime(2025, 10, 16, 7, 0, 0),
#     datetime(2025, 10, 17, 7, 0, 0),
#     datetime(2025, 10, 18, 7, 0, 0),
#     datetime(2025, 10, 19, 7, 0, 0),
#     datetime(2025, 10, 20, 7, 0, 0),
#     datetime(2025, 10, 21, 7, 0, 0),
#     datetime(2025, 10, 22, 7, 0, 0),
#     datetime(2025, 10, 23, 7, 0, 0),
#     datetime(2025, 10, 24, 7, 0, 0),
#     datetime(2025, 10, 25, 7, 0, 0),
#     datetime(2025, 10, 26, 7, 0, 0),
#     datetime(2025, 10, 27, 7, 0, 0),
#     datetime(2025, 10, 28, 7, 0, 0),
#     datetime(2025, 10, 29, 7, 0, 0),
#     datetime(2025, 10, 30, 7, 0, 0),
#     datetime(2025, 10, 31, 7, 0, 0),
# ]

# NOV
# [
#     datetime(2025, 11, 1, 7, 0, 0),
#     datetime(2025, 11, 2, 7, 0, 0),
#     datetime(2025, 11, 3, 7, 0, 0),
#     datetime(2025, 11, 4, 7, 0, 0),
#     datetime(2025, 11, 5, 7, 0, 0),
#     datetime(2025, 11, 6, 7, 0, 0),
#     datetime(2025, 11, 7, 7, 0, 0),
#     datetime(2025, 11, 8, 7, 0, 0),
#     datetime(2025, 11, 9, 7, 0, 0),
#     datetime(2025, 11, 10, 7, 0, 0),
#     datetime(2025, 11, 11, 7, 0, 0),
#     datetime(2025, 11, 12, 7, 0, 0),
#     datetime(2025, 11, 13, 7, 0, 0),
#     datetime(2025, 11, 14, 7, 0, 0),
#     datetime(2025, 11, 15, 7, 0, 0),
#     datetime(2025, 11, 16, 7, 0, 0),
#     datetime(2025, 11, 17, 7, 0, 0),
#     datetime(2025, 11, 18, 7, 0, 0),
#     datetime(2025, 11, 19, 7, 0, 0),
#     datetime(2025, 11, 20, 7, 0, 0),
#     datetime(2025, 11, 21, 7, 0, 0),
#     datetime(2025, 11, 22, 7, 0, 0),
#     datetime(2025, 11, 23, 7, 0, 0),
#     datetime(2025, 11, 24, 7, 0, 0),
#     datetime(2025, 11, 25, 7, 0, 0),
#     datetime(2025, 11, 26, 7, 0, 0),
#     datetime(2025, 11, 27, 7, 0, 0),
#     datetime(2025, 11, 28, 7, 0, 0),
#     datetime(2025, 11, 29, 7, 0, 0),
#     datetime(2025, 11, 30, 7, 0, 0),
# ]

# DEC
# [
    # datetime(2025, 12, 1, 7, 0, 0),
    # datetime(2025, 12, 2, 7, 0, 0),
    # datetime(2025, 12, 3, 7, 0, 0),
    # datetime(2025, 12, 4, 7, 0, 0),
    # datetime(2025, 12, 5, 7, 0, 0),
    # datetime(2025, 12, 6, 7, 0, 0),
    # datetime(2025, 12, 7, 7, 0, 0),
    # datetime(2025, 12, 8, 7, 0, 0),
    # datetime(2025, 12, 9, 7, 0, 0),
    # datetime(2025, 12, 10, 7, 0, 0),
    # datetime(2025, 12, 11, 7, 0, 0),
    # datetime(2025, 12, 12, 7, 0, 0),
    # datetime(2025, 12, 13, 7, 0, 0),
    # datetime(2025, 12, 14, 7, 0, 0),
    # datetime(2025, 12, 15, 7, 0, 0),
    # datetime(2025, 12, 16, 7, 0, 0),
    # datetime(2025, 12, 17, 7, 0, 0),
    # datetime(2025, 12, 18, 7, 0, 0),
    # datetime(2025, 12, 19, 7, 0, 0),
    # datetime(2025, 12, 20, 7, 0, 0),
    # datetime(2025, 12, 21, 7, 0, 0),
    # datetime(2025, 12, 22, 7, 0, 0),
    # datetime(2025, 12, 23, 7, 0, 0),
    # datetime(2025, 12, 24, 7, 0, 0),
    # datetime(2025, 12, 25, 7, 0, 0),
    # datetime(2025, 12, 26, 7, 0, 0),
    # datetime(2025, 12, 27, 7, 0, 0),
    # datetime(2025, 12, 28, 7, 0, 0),
    # datetime(2025, 12, 29, 7, 0, 0),
    # datetime(2025, 12, 30, 7, 0, 0),
    # datetime(2025, 12, 31, 7, 0, 0),
# ]

# JAN
#     datetime(2026, 1, 1, 7, 0, 0),
# datetime(2026, 1, 2, 7, 0, 0),
# datetime(2026, 1, 3, 7, 0, 0),
# datetime(2026, 1, 4, 7, 0, 0),
# datetime(2026, 1, 5, 7, 0, 0),
# datetime(2026, 1, 6, 7, 0, 0),
# datetime(2026, 1, 7, 7, 0, 0),
# datetime(2026, 1, 8, 7, 0, 0),
# datetime(2026, 1, 9, 7, 0, 0),
# datetime(2026, 1, 10, 7, 0, 0),
# datetime(2026, 1, 11, 7, 0, 0),
# datetime(2026, 1, 12, 7, 0, 0),
# datetime(2026, 1, 13, 7, 0, 0),
# datetime(2026, 1, 14, 7, 0, 0),
# datetime(2026, 1, 15, 7, 0, 0),
# datetime(2026, 1, 16, 7, 0, 0),
# datetime(2026, 1, 17, 7, 0, 0),
# datetime(2026, 1, 18, 7, 0, 0),
# datetime(2026, 1, 19, 7, 0, 0),
# datetime(2026, 1, 20, 7, 0, 0),
# datetime(2026, 1, 21, 7, 0, 0)
