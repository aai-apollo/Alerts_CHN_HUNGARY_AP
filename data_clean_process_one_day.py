from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from pymongo import MongoClient
import pandas as pd
import re
from datetime import datetime, timezone, time as datetime_time
from datetime import datetime, date  # Ensure both datetime and date are imported                                                                                 
import inspect
from uuid import uuid4
import traceback
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
from datetime import datetime, timedelta


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

from config import (
    client,
    FILE_TRACKING_DB,
    FILE_TRACKING_COLLECTION,
    UNIFORMITY_DB_history,
    BALANCING_DB_history,
    GEOMETRY_DB_history,
    Data_Clean_UNIFORMITY_HISTORICAL_DATA,
    Data_Clean_BALANCING_HISTORICAL_DATA,
    Data_Clean_GEOMETRY_HISTORICAL_DATA,
    UNIFORMITY_CLEANED_PREFIX_history,
    BALANCING_CLEANED_PREFIX_history,
    GEOMETRY_CLEANED_PREFIX_history,
    COMBINED_Database_history,
    COMBINED_COLLECTION_NAME_history,
    ALL_COMBINED_UNIFORMITY_COLLECTION_history,
    ALL_COMBINED_BALANCING_COLLECTION_history,
    ALL_COMBINED_GEOMETRY_COLLECTION_history,
    INVALID_ROWS_DB,
    UNIFORMITY_BALANCING_GEOMETRY_DB_history,
    uniformity_collection_name_history,
    balancing_collection_name_history,
    geometry_collection_name_history,
    config_databse,
    channel_no_collect,
    alert_mail_db, # Add 05-07
    admin_table,   # Add 05-07
    alert_users

)

app = Flask(__name__)
app.secret_key = "supersecretkey"

def configure_logging():
    if not os.path.exists('logs'):
        os.makedirs('logs')
    current_date = datetime.now().strftime('%Y-%m-%d')
    log_filename = f'logs/log_historical_{current_date}.txt'
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.FileHandler(log_filename, mode='a', encoding='utf-8')]
    )
    warnings.simplefilter("always")
    logging.captureWarnings(True)

def yield_percentage(numerator, denominator):
    """Calculate yield percentage, handling division by zero."""
    try:
        if denominator == 0:
            return 0.0
        return (numerator / denominator) * 100
    except (TypeError, ValueError) as e:
        logging.warning(f"Error calculating yield percentage: {e}")
        return None

def oe_yield(batch_data):
    """
    Calculate OE yield percentage based on class values in batch_data.
    
    Args:
        batch_data: pandas DataFrame with 'class' and 'barcode' columns.
    
    Returns:
        float: Yield percentage, or None if an error occurs.
    """
    try:
        # Validate input
        if not isinstance(batch_data, pd.DataFrame):
            logging.warning("Input batch_data is not a pandas DataFrame")
            return None
        
        # Check for required columns
        if 'class' not in batch_data.columns or 'barcode' not in batch_data.columns:
            logging.warning("Missing required columns: 'class' or 'barcode'")
            return None
        
        # Check for missing values in 'class' column
        if batch_data['class'].isna().any():
            logging.warning("Missing values found in 'class' column")
            return None

        # Define valid classes for denominator
        valid_classes = ['TX', 'T1', 'T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'T8']
        denominator_data = batch_data[batch_data['class'].isin(valid_classes)]
        denominator = denominator_data['barcode'].nunique()
        
        if denominator == 0:
            logging.info("No valid barcodes found for denominator classes")
            return 0.0
        
        # Filter for numerator classes (T1, T2, T3, T4)
        numerator_data = denominator_data[denominator_data['class'].isin(['T1', 'T2', 'T3', 'T4'])]
        numerator = numerator_data['barcode'].nunique()
        
        result = yield_percentage(numerator, denominator)
        if result is None:
            logging.warning("Failed to calculate yield percentage")
            return None
            
        return float(result)
    
    except (KeyError, TypeError, ValueError) as e:
        logging.error(f"Error in oe_yield: {e}")
        return None

def balancing_oe_yield(batch_data):
    """
    Calculate OE yield percentage based on balancing_class values in batch_data.
    
    Args:
        batch_data: pandas DataFrame with 'balancing_class' and 'barcode' columns.
    
    Returns:
        float: Yield percentage, or None if an error occurs.
    """
    try:
        # Validate input
        if not isinstance(batch_data, pd.DataFrame):
            logging.warning("Input batch_data is not a pandas DataFrame")
            return None
        
        # Check for required columns
        if 'balancing_class' not in batch_data.columns or 'barcode' not in batch_data.columns:
            logging.warning("Missing required columns: 'balancing_class' or 'barcode'")
            return None
        
        # Check for missing values in 'balancing_class' column
        if batch_data['balancing_class'].isna().any():
            logging.warning("Missing values found in 'balancing_class' column")
            return None

        # Define valid classes for denominator
        valid_classes = ['TX', 'T1', 'T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'T8']
        denominator_data = batch_data[batch_data['balancing_class'].isin(valid_classes)]
        denominator = denominator_data['barcode'].nunique()
        
        if denominator == 0:
            logging.info("No valid barcodes found for denominator classes")
            return 0.0
        
        # Filter for numerator classes (T1, T2, T3, T4)
        numerator_data = denominator_data[denominator_data['balancing_class'].isin(['T1', 'T2' 'T3', 'T4'])]
        numerator = numerator_data['barcode'].nunique()
        
        result = yield_percentage(numerator, denominator)
        if result is None:
            logging.warning("Failed to calculate yield percentage")
            return None
            
        return float(result)
    
    except (KeyError, TypeError, ValueError) as e:
        logging.error(f"Error in balancing_oe_yield: {e}")
        return None

def geometry_oe_yield(batch_data):
    """
    Calculate OE yield percentage based on balancing_class values in batch_data.
    
    Args:
        batch_data: pandas DataFrame with 'balancing_class' and 'barcode' columns.
    
    Returns:
        float: Yield percentage, or None if an error occurs.
    """
    try:
        # Validate input
        if not isinstance(batch_data, pd.DataFrame):
            logging.warning("Input batch_data is not a pandas DataFrame")
            return None
        
        # Check for required columns
        if 'balancing_class' not in batch_data.columns or 'barcode' not in batch_data.columns:
            logging.warning("Missing required columns: 'balancing_class' or 'barcode'")
            return None
        
        # Check for missing values in 'balancing_class' column
        if batch_data['balancing_class'].isna().any():
            logging.warning("Missing values found in 'balancing_class' column")
            return None

        # Define valid classes for denominator
        valid_classes = ['TX', 'T1', 'T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'T8']
        denominator_data = batch_data[batch_data['balancing_class'].isin(valid_classes)]
        denominator = denominator_data['barcode'].nunique()
        
        if denominator == 0:
            logging.info("No valid barcodes found for denominator classes")
            return 0.0
        
        # Filter for numerator classes (T1, T2)
        numerator_data = denominator_data[denominator_data['balancing_class'].isin(['T1', 'T2', 'T3', 'T4'])]
        numerator = numerator_data['barcode'].nunique()
        
        result = yield_percentage(numerator, denominator)
        if result is None:
            logging.warning("Failed to calculate yield percentage")
            return None
            
        return float(result)
    
    except (KeyError, TypeError, ValueError) as e:
        logging.error(f"Error in geometry_oe_yield: {e}")
        return None
        
# Initialize MongoDB client
db_tracking = client[FILE_TRACKING_DB]
tracking_collection = db_tracking[FILE_TRACKING_COLLECTION]

def some_function():
    india_time = datetime.now(pytz.timezone('Asia/Kolkata'))
    return india_time

logging.info(some_function())

DEFAULT_DATETIME = datetime(1900, 1, 1, 12, 30, 0)
DEFAULT_DATE = datetime(1900, 1, 1)
date_formats = ["%d-%m-%Y", "%Y-%m-%d"]

# Access databases
file_tracking_db = client['file_tracking']
uniformity_db = client['UNIFORMITY_HISTORICAL']
balancing_db = client['BALANCING_HISTORICAL']
geometry_db = client['GEOMETRY_HISTORICAL']

# def insert_with_timestamp(df, collection_name_prefix, db_name):
#     timestamp = some_function().strftime("%Y%m%d_%H%M%S")
#     if db_name == 'BALANCING_HISTORICAL':
#         db = balancing_db
#     elif db_name == 'UNIFORMITY_HISTORICAL':
#         db = uniformity_db
#     elif db_name == 'GEOMETRY_HISTORICAL':
#         db = geometry_db
#     else:
#         raise ValueError("Invalid database name")
#     collection_name = f"{collection_name_prefix}_{timestamp}"
#     collection = db[collection_name]
#     collection.insert_many(df.to_dict('records'))
#     track_file_in_file_tracking(collection_name_prefix, timestamp, len(df), db_name)

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
    
    # Select database
    if db_name == 'BALANCING_HISTORICAL':
        db = balancing_db
    elif db_name == 'UNIFORMITY_HISTORICAL':
        db = uniformity_db
    elif db_name == 'GEOMETRY_HISTORICAL':
        db = geometry_db
    else:
        raise ValueError(f"Invalid database name: {db_name}")
    
    collection_name = f"{collection_name_prefix}_{timestamp}"
    collection = db[collection_name]
    
    # Insert records into MongoDB
    if records:
        try:
            collection.insert_many(records)
            print(f"Successfully inserted {len(records)} records into collection '{collection_name}'")
        except Exception as e:
            print(f"Error inserting into MongoDB: {e}")
            
            # Try one more time with simplified records
            print("Attempting simplified insertion...")
            simple_records = []
            for record in records:
                simple_record = {}
                for key, value in record.items():
                    # Convert everything to basic types
                    if value is None:
                        simple_record[key] = None
                    elif isinstance(value, (dict, list, tuple)):
                        simple_record[key] = str(value)
                    else:
                        simple_record[key] = value
                simple_records.append(simple_record)
            
            try:
                collection.insert_many(simple_records)
                print(f"Successfully inserted {len(simple_records)} records with simplified data")
            except Exception as e2:
                print(f"Failed to insert even with simplified data: {e2}")
                raise
    
    # Track the file insertion
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
        self.conn = redshift_connector.connect(
            host='atl-dl-prd-01.cuvxusm4qify.ap-south-1.redshift.amazonaws.com',
            database='db_prd_plant_ap',
            port=5439,
            user=os.environ.get('REDSHIFT_USER_ONE_DAY', ''),
            password=os.environ.get('REDSHIFT_PASSWORD_ONE_DAY', ''), sslmode='disable'
        )
    def execute_query(self, q):
        data = pd.read_sql(q, self.conn)
        return data
    
inst = RedshiftDataConnectorr()


############## Every one hour running code Start ###########################
local_tz = pytz.timezone('Asia/Kolkata')
now = datetime.now(pytz.utc).astimezone(local_tz)

def get_prev_hour_range(run_dt):
    # Get start of previous hour in IST
    prev_hour = (run_dt - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    next_hour = prev_hour + timedelta(hours=1)

    start_date= prev_hour.strftime("%Y-%m-%d")
    end_date = prev_hour.strftime("%Y-%m-%d")

    start_time = prev_hour.strftime("%H:%M:%S")
    end_time = next_hour.strftime("%H:%M:%S")

    return start_date, end_date, start_time, end_time

# Get actual query date/time range for the cron run
start_date, end_date, start_time, end_time = get_prev_hour_range(now)

# Combine query_date and times to form full TIMESTAMP strings
start_timestamp = f"{start_date} {start_time}"
end_timestamp = f"{end_date} {end_time}"
############## Every one hour running code Start ###########################


#################### Manual custom date start ######################
# def get_custom_range():
#     now = datetime.now()
    
#     start_date = "2026-02-23"
#     start_time = "07:00:00"
#     end_date = "2026-02-24"
#     end_time = "12:17:00"
    
#     return start_date, start_time, end_date, end_time

# # Get the time range
# start_date, start_time, end_date, end_time = get_custom_range()

# # Combine query_date and times to form full TIMESTAMP strings
# start_timestamp = f"{start_date} {start_time}"
# end_timestamp = f"{end_date} {end_time}"
#################### Manual custom date end ######################

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
       mvarval10, mvarval11, mvarval61, mvarval91, mvarval62, mvarval92,
       mvarval14, mvarval15, mvarval16, mvarval17, mvarval121, mvarval151,
       mvarval25, mvarval18, mvarval129, mvarval33, mvarval181, mvarval211,
       mvarval182, mvarval212, mvarval37, mvarval41, mvarval241, mvarval271,
       mvarval1, mvarval9, mvarval3, mvarval7, mvarval2, mvarval4, mvarval8,
       mvarval27, mvarval19, mvarval55, mvarval320, mvarval46, mvarval161,
       mvarval53, mvarval280, mvarval26, mvarval28, mvarval20, mvarval56,
       mvarval340, mvarval47, mvarval54, mvarval300, sku_name, attribute1,
       tbm_stage2, ptimebuild, cavity, mold, ptimecure, machinenum, putctime,
       class, shift, cycletime,ptime
        FROM db_prd_plant_ap.sc_ufs.t_pertiretu
        WHERE ptime >= '{start_timestamp}'
        AND ptime < '{end_timestamp}'
        AND attribute1 IN ({gt_code_sql});"""
df_uniformity = inst.execute_query(q1)
# df_uniformity.to_csv('df_uniformity_half.csv', escapechar='\\', quoting=csv.QUOTE_NONE, index=False)
# df_uniformity = pd.read_csv("df_uniformity_half.csv")

q2 = f"""SELECT barcode,mvarval25,mvarval27,mvarval17,mvarval19,mvarval55,
        mvarval320,mvarval46,mvarval161,mvarval53,mvarval280,mvarval26,
        mvarval28,mvarval18,mvarval20,mvarval56,mvarval340,mvarval47,mvarval181,
        mvarval54,mvarval300,sku_name,attribute1,tbm_stage2,ptimebuild,
        cavity,mold,ptimecure,machinenum,putctime,class,ptime 
        FROM db_prd_plant_ap.sc_ufs.t_pertiretg
        WHERE ptime >= '{start_timestamp}'
        AND ptime < '{end_timestamp}'
        AND attribute1 IN ({gt_code_sql});"""
df_geometry = inst.execute_query(q2)
# df_geometry.to_csv('df_geometry_half.csv', escapechar='\\', quoting=csv.QUOTE_NONE, index=False)
# df_geometry = pd.read_csv("df_geometry_half.csv")

q3 = f"""
SELECT
    tg.*
FROM db_prd_plant_ap.sc_ufs.t_pertiretg tg
JOIN db_prd_plant_ap.sc_ufs.t_pertiretu tu
    ON tg.barcode = tu.barcode
WHERE tg.ptime >= '{start_timestamp}'
  AND tg.ptime < '{end_timestamp}'
  AND tu.ptime >= '{start_timestamp}'
  AND tu.ptime < '{end_timestamp}'
  AND tg.attribute1 IN ({gt_code_sql});"""

df_balancing = inst.execute_query(q3)
# df_balancing.to_csv('df_balancing.csv', escapechar='\\', quoting=csv.QUOTE_NONE, index=False)
# df_balancing = pd.read_csv("df_balancing.csv")

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
        'mvarval10': 'rfpp_cw_kg',
        'mvarval11': 'rfpp_ccw_kg',
        'mvarval61': 'rfh1_cw_kg',
        'mvarval91': 'rfh1_ccw_kg',
        'mvarval62': 'rfh2_cw_kg',
        'mvarval92': 'rfh2_ccw_kg',
        'mvarval14': 'lfpp_cw_kg',
        'mvarval15': 'lfpp_ccw_kg',
        'mvarval16': 'lsft_cw_kg',
        'mvarval17': 'lsft_ccw_kg',
        'mvarval121': 'lfh1_cw_kg',
        'mvarval151': 'lfh1_ccw_kg',
        'mvarval25': 'plysteer_kg',
        'mvarval18': 'cony_kg',
        'mvarval129': 'rfmxa_cw_deg',
        'mvarval33': 'rfmxa_ccw_deg',
        'mvarval181': 'rfa1_cw_deg',
        'mvarval211': 'rfa1_ccw_deg',
        'mvarval182': 'rfa2_cw_deg',
        'mvarval212': 'rfa2_ccw_deg',
        'mvarval37': 'lfmxa_cw_deg',
        'mvarval41': 'lfmxa_ccw_deg',
        'mvarval241': 'lfa1_cw_deg',
        'mvarval271': 'lfa1_ccw_deg',
        'mvarval1': 'botplane_g',
        'mvarval9': 'topplane_g',
        'mvarval3': 'couple_g',
        'mvarval7': 'static_g',
        'mvarval2': 'botplane_deg',
        'mvarval4': 'couple_deg',
        'mvarval8': 'static_deg',
        'mvarval27': 'botdep_mm',
        'mvarval19': 'topdep_mm',
        'mvarval55': 'blro_mm',
        'mvarval320': 'blroh1_mm',
        'mvarval46': 'crro_mm',
        'mvarval161': 'crroh1_mm',
        'mvarval53': 'tlro_mm',
        'mvarval280': 'tlroh1_mm',
        'mvarval26': 'botbul_deg',
        'mvarval28': 'botdep_deg',
        'mvarval20': 'topdep_deg',
        'mvarval56': 'blro_deg',
        'mvarval340': 'blroh1_deg',
        'mvarval47': 'crro_deg',
        'mvarval54': 'tlro_deg',
        'mvarval300': 'tlroh1_deg',
        'sku_name': 'sku',
        'attribute1': 'gt_code',
        'tbm_stage2': 'tbm',
        'ptimebuild': 'tbm_buildtime',
        'cavity': 'curing_press',
        'mold': 'curing_mouldid',
        'ptimecure': 'curing_infeedtime',
        'machinenum': 'ubno',
        'putctime': 'date_of_utc',
        'class': 'class',
        'cycletime': 'cycletime',
        'ptime': 'date_of_ufm'
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
        'rfpp_ccw_kg': 'float64',
        'rfh1_cw_kg': 'float64',
        'rfh1_ccw_kg': 'float64',
        'rfh2_cw_kg': 'float64',
        'rfh2_ccw_kg': 'float64',
        'lfpp_cw_kg': 'float64',
        'lfpp_ccw_kg': 'float64',
        'lsft_cw_kg': 'float64',
        'lsft_ccw_kg': 'float64',
        'lfh1_cw_kg': 'float64',
        'lfh1_ccw_kg': 'float64',
        'plysteer_kg': 'float64',
        'cony_kg': 'float64',
        'rfmxa_cw_deg': 'float64',
        'rfmxa_ccw_deg': 'float64',
        'rfa1_cw_deg': 'float64',
        'rfa1_ccw_deg': 'float64',
        'rfa2_cw_deg': 'float64',
        'rfa2_ccw_deg': 'float64',
        'lfmxa_cw_deg': 'float64',
        'lfmxa_ccw_deg': 'float64',
        'lfa1_cw_deg': 'float64',
        'lfa1_ccw_deg': 'float64',
        'class': 'str',
        'cycletime': 'float64'
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
            'mvarval1':  'botplane_g',
            'mvarval9':  'topplane_g',
            'mvarval3':  'couple_g',
            'mvarval7':  'static_g',
            'mvarval2':  'botplane_deg',
            'mvarval10': 'topplane_deg',
            'mvarval4':  'couple_deg',
            'mvarval8':  'static_deg',
            'sku_name':  'sku',
            'attribute1': 'gt_code',
            'tbm_stage2': 'tbm',
            'ptimebuild': 'tbm_buildtime',
            'cavity':     'curing_press',
            'mold':       'curing_mouldid',
            'ptimecure':  'curing_infeedtime',
            'machinenum': 'ubno',
            'putctime':   'date_of_utc',
            'class':      'balancing_class',
            'ptime': 'date_of_ufm'
        }

        rename_dict = {k: v for k, v in rename_columns_mapping.items() if k in df.columns}
        if rename_dict:
            df.rename(columns=rename_dict, inplace=True)

        # 3. Convert ALL columns to appropriate types - FIXED
        # Define which columns should be strings vs numeric
        string_columns = [
            'sku', 'gt_code', 'tbm', 'tbm_buildtime', 'curing_press',
            'curing_mouldid', 'curing_infeedtime', 'ubno', 'date_of_utc',
            'balancing_class', 'barcode', 'date_of_ufm'
        ]
        
        numeric_columns = [
            'botplane_g', 'topplane_g', 'couple_g', 'static_g',
            'botplane_deg', 'topplane_deg', 'couple_deg', 'static_deg'
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
            df[col] = df[col].str.strip().replace('', None)

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
            'mvarval25': 'botbul_mm',
            'mvarval27': 'botdep_mm',
            'mvarval17': 'topbul_mm',
            'mvarval19': 'topdep_mm',
            'mvarval55': 'blro_mm',
            'mvarval320': 'blroh1_mm',
            'mvarval46': 'crro_mm',
            'mvarval161': 'crroh1_mm',
            'mvarval53': 'tlro_mm',
            'mvarval280': 'tlroh1_mm',
            'mvarval26': 'botbul_deg',
            'mvarval28': 'botdep_deg',
            'mvarval18': 'topbul_deg',
            'mvarval20': 'topdep_deg',
            'mvarval56': 'blro_deg',
            'mvarval340': 'blroh1_deg',
            'mvarval47': 'crro_deg',
            'mvarval181': 'crroh1_deg',
            'mvarval54': 'tlro_deg',
            'mvarval300': 'tlroh1_deg',
            'sku_name': 'sku',
            'attribute1': "gt_code",
            'tbm_stage2': 'tbm',
            'ptimebuild': 'tbm_buildtime',
            'cavity': 'curing_press',
            'mold': 'curing_mouldid',
            'ptimecure': 'curing_infeedtime',
            'machinenum': 'ubno',
            'putctime': 'date_of_utc',
            'class': 'geometry_class',
            'ptime': 'date_of_ufm'
        }
        
        # Apply renaming
        for old_name, new_name in rename_columns_mapping.items():
            if old_name in df.columns:
                df.rename(columns={old_name: new_name}, inplace=True)
        
        # 3. Define column types
        string_columns = [
            'sku', 'gt_code', 'tbm', 'tbm_buildtime', 'curing_press',
            'curing_mouldid', 'curing_infeedtime', 'ubno', 'date_of_utc',
            'geometry_class', 'barcode', 'date_of_ufm'
        ]
        
        measurement_columns = [
            'botbul_mm', 'botdep_mm', 'topbul_mm', 'topdep_mm',
            'blro_mm', 'blroh1_mm', 'crro_mm', 'crroh1_mm',
            'tlro_mm', 'tlroh1_mm', 'botbul_deg', 'botdep_deg',
            'topbul_deg', 'topdep_deg', 'blro_deg', 'blroh1_deg',
            'crro_deg', 'crroh1_deg', 'tlro_deg', 'tlroh1_deg'
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
        
        # 7. Strip whitespace from string columns
        string_cols = df.select_dtypes(include=['object']).columns
        for col in string_cols:
            df[col] = df[col].str.strip().replace('', None)
        
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
    try:
        if cleaned_df.empty:
            output_messages.append("No valid data to save after cleaning.")
            return output_messages
        timestamp = some_function().strftime("%Y%m%d_%H%M%S")
        if database_name == "UNIFORMITY_HISTORICAL":
            cleaned_collection_name = f"{UNIFORMITY_CLEANED_PREFIX_history}_{timestamp}"
            target_db_name = Data_Clean_UNIFORMITY_HISTORICAL_DATA
        elif database_name == "BALANCING_HISTORICAL":
            cleaned_collection_name = f"{BALANCING_CLEANED_PREFIX_history}_{timestamp}"
            target_db_name = Data_Clean_BALANCING_HISTORICAL_DATA
        elif database_name == "GEOMETRY_HISTORICAL":
            cleaned_collection_name = f"{GEOMETRY_CLEANED_PREFIX_history}_{timestamp}"
            target_db_name = Data_Clean_GEOMETRY_HISTORICAL_DATA
        else:
            error_message = f"Unknown database: {database_name}. Cannot insert cleaned data."
            output_messages.append(error_message)
            return output_messages
        data_to_insert = cleaned_df.to_dict(orient="records")
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
    logging.info(f"remove_duplicates_collection function started for {collection.name}")
   
    try:
        # Create compound index for better duplicate detection
        # Using the fields you mentioned: barcode, ubno, date, time
        index_name = "duplicate_detection_idx"
        existing_indexes = collection.index_information()
       
        if index_name not in existing_indexes:
            collection.create_index([
                ("barcode", 1),
                ("gt_code", 1),
                ("ubno", 1),
                ("date_of_ufm", 1),
                ("tbm", 1),
                ("curing_press", 1),
                ("curing_mouldid", 1)
            ], name=index_name, background=True)
            output_messages.append(f"Created compound index '{index_name}' on {collection.name}")
       
        # Use aggregation to find duplicates based on all relevant fields
        pipeline = [
            {"$group": {
                "_id": {
                    "barcode": "$barcode",
                    "gt_code": "$gt_code",
                    "ubno": "$ubno",
                    "date_of_ufm": "$date_of_ufm",
                    "tbm": "$tbm",
                    "curing_press": "$curing_press",
                    "curing_mouldid": "$curing_mouldid"
                },
                "count": {"$sum": 1},
                "ids": {"$push": "$_id"},
                "first_occurrence": {"$first": "$_id"}
            }},
            {"$match": {
                "count": {"$gt": 1}
            }}
        ]
       
        duplicates = list(collection.aggregate(pipeline))
        duplicate_count = sum(dup['count'] - 1 for dup in duplicates)
       
        if duplicate_count > 0:
            message = f"Found {duplicate_count} duplicate records in {collection.name}"
            output_messages.append(message)
            logging.info(message)
           
            # Remove duplicates - keep only the first occurrence
            removed_count = 0
            for dup in duplicates:
                # Keep the first document, remove others
                ids_to_remove = [id for id in dup['ids'] if id != dup['first_occurrence']]
                if ids_to_remove:
                    result = collection.delete_many({"_id": {"$in": ids_to_remove}})
                    removed_count += result.deleted_count
           
            success_message = f"Removed {removed_count} duplicates from {collection.name}"
            output_messages.append(success_message)
            logging.info(success_message)
        else:
            no_dupes_message = f"No duplicates found in {collection.name}"
            output_messages.append(no_dupes_message)
            logging.info(no_dupes_message)
           
    except Exception as e:
        error_message = f"Error removing duplicates from {collection.name}: {str(e)}"
        output_messages.append(error_message)
        logging.error(error_message)
        logging.exception("Detailed error traceback:")
   
    logging.info(f"remove_duplicates_collection function completed for {collection.name}")
    return output_messages

def process_data_for_api(db_client, source_data, database_name, filename):
    output_messages = []
    cleaned_collection_name = None
    invalid_collection_name = None
    try:
        df = pd.DataFrame(list(source_data))

        if database_name == "UNIFORMITY_HISTORICAL":
            cleaned_df, invalid_df, uniformity_messages = clean_uniformity_data(df.copy())
            output_messages.extend(uniformity_messages)

            cleaned_collection_name = UNIFORMITY_CLEANED_PREFIX_history + filename.replace(".csv", "").replace(".xlsx", "")
            invalid_collection_name = "invalid_data_UNIFORMITY_" + filename.replace(".csv", "").replace(".xlsx", "")

            if not invalid_df.empty:
                invalid_df = invalid_df.drop('_id', axis=1, errors='ignore')
                invalid_collect = db_client[INVALID_ROWS_DB][invalid_collection_name]
                output_messages.extend(remove_duplicates_collection(invalid_collect))
                db_client[INVALID_ROWS_DB][invalid_collection_name].insert_many(invalid_df.to_dict("records"))
                output_messages.append(f"Invalid UNIFORMITY_HISTORICAL data saved to invalid_data_UNIFORMITY.{invalid_collection_name}")
            else:
                output_messages.append("No invalid UNIFORMITY_HISTORICAL data to save.")

            output_messages.extend(insert_cleaned_data(db_client, cleaned_df, database_name, filename))

            if not cleaned_df.empty:
                cleaned_df = cleaned_df.drop('_id', axis=1, errors='ignore')
                # db_client[COMBINED_Database_history][ALL_COMBINED_UNIFORMITY_COLLECTION_history].insert_many(cleaned_df.to_dict("records"))
                # output_messages.append(f"Cleaned UNIFORMITY_HISTORICAL data pushed to {COMBINED_Database_history}.{ALL_COMBINED_UNIFORMITY_COLLECTION_history}")
                # CHANGE: Delete all existing data in combined uniformity collection before inserting new
                combined_uniformity_coll = db_client[COMBINED_Database_history][ALL_COMBINED_UNIFORMITY_COLLECTION_history]
                delete_result = combined_uniformity_coll.delete_many({})
                output_messages.append(f"Deleted {delete_result.deleted_count} existing records from {ALL_COMBINED_UNIFORMITY_COLLECTION_history}")

                # Now insert fresh cleaned data
                combined_uniformity_coll.insert_many(cleaned_df.to_dict("records"))
                output_messages.append(f"Cleaned UNIFORMITY_HISTORICAL data (replaced) pushed to {COMBINED_Database_history}.{ALL_COMBINED_UNIFORMITY_COLLECTION_history}")
            else:
                output_messages.append("No cleaned UNIFORMITY_HISTORICAL data to push to combined collection")

        elif database_name == "BALANCING_HISTORICAL":
            cleaned_df, invalid_barcode_rows, duplicates_df, balancing_messages = clean_balancing_data(df.copy())
            output_messages.extend(balancing_messages)

            cleaned_collection_name = BALANCING_CLEANED_PREFIX_history + filename.replace(".csv", "").replace(".xlsx", "")
            invalid_collection_name = "invalid_data_BALANCING_" + filename.replace(".csv", "").replace(".xlsx", "")

            if not invalid_barcode_rows.empty:
                invalid_barcode_rows = invalid_barcode_rows.drop('_id', axis=1, errors='ignore')
                invalid_collect = db_client[INVALID_ROWS_DB][invalid_collection_name]
                output_messages.extend(remove_duplicates_collection(invalid_collect))
                db_client[INVALID_ROWS_DB][invalid_collection_name].insert_many(invalid_barcode_rows.to_dict("records"))
                output_messages.append(f"Invalid BALANCING_HISTORICAL data saved to invalid_data_BALANCING.{invalid_collection_name}")
            else:
                output_messages.append("No invalid barcode BALANCING_HISTORICAL data to save.")

            if not duplicates_df.empty:
                duplicate_collection_name = f"duplicate_BALANCING_{filename.replace('.csv', '').replace('.xlsx', '')}"
                duplicates_df = duplicates_df.drop('_id', axis=1, errors='ignore')
                db_client[INVALID_ROWS_DB][duplicate_collection_name].insert_many(duplicates_df.to_dict("records"))
                output_messages.append(f"Duplicate BALANCING_HISTORICAL data saved to {INVALID_ROWS_DB}.{duplicate_collection_name}")
            else:
                output_messages.append("No duplicate BALANCING_HISTORICAL data to save.")

            output_messages.extend(insert_cleaned_data(db_client, cleaned_df, database_name, filename))

            if not cleaned_df.empty:
                cleaned_df = cleaned_df.drop('_id', axis=1, errors='ignore')
                # db_client[COMBINED_Database_history][ALL_COMBINED_BALANCING_COLLECTION_history].insert_many(cleaned_df.to_dict("records"))
                # output_messages.append(f"Cleaned BALANCING_HISTORICAL data pushed to {COMBINED_Database_history}.{ALL_COMBINED_BALANCING_COLLECTION_history}"
                combined_balancing_coll = db_client[COMBINED_Database_history][ALL_COMBINED_BALANCING_COLLECTION_history]
                delete_result = combined_balancing_coll.delete_many({})
                output_messages.append(f"Deleted {delete_result.deleted_count} existing records from {ALL_COMBINED_BALANCING_COLLECTION_history}")

                # Now insert fresh cleaned data
                combined_balancing_coll.insert_many(cleaned_df.to_dict("records"))
                output_messages.append(f"Cleaned BALANCING_HISTORICAL data (replaced) pushed to {COMBINED_Database_history}.{ALL_COMBINED_BALANCING_COLLECTION_history}")
            else:
                output_messages.append("No cleaned BALANCING_HISTORICAL data to push to combined collection")

        elif database_name == "GEOMETRY_HISTORICAL":
            cleaned_df, invalid_barcode_rows, duplicates_df, geometry_messages = clean_geometry_data(df.copy())
            output_messages.extend(geometry_messages)

            cleaned_collection_name = GEOMETRY_CLEANED_PREFIX_history + filename.replace(".csv", "").replace(".xlsx", "")
            invalid_collection_name = "invalid_data_GEOMETRY_" + filename.replace(".csv", "").replace(".xlsx", "")

            if not invalid_barcode_rows.empty:
                invalid_barcode_rows = invalid_barcode_rows.drop('_id', axis=1, errors='ignore')
                invalid_collect = db_client[INVALID_ROWS_DB][invalid_collection_name]
                output_messages.extend(remove_duplicates_collection(invalid_collect))
                db_client[INVALID_ROWS_DB][invalid_collection_name].insert_many(invalid_barcode_rows.to_dict("records"))
                output_messages.append(f"Invalid GEOMETRY_HISTORICAL data saved to invalid_data_GEOMETRY.{invalid_collection_name}")
            else:
                output_messages.append("No invalid barcode GEOMETRY_HISTORICAL data to save.")

            if not duplicates_df.empty:
                duplicate_collection_name = f"duplicate_GEOMETRY_{filename.replace('.csv', '').replace('.xlsx', '')}"
                duplicates_df = duplicates_df.drop('_id', axis=1, errors='ignore')
                db_client[INVALID_ROWS_DB][duplicate_collection_name].insert_many(duplicates_df.to_dict("records"))
                output_messages.append(f"Duplicate GEOMETRY_HISTORICAL data saved to {INVALID_ROWS_DB}.{duplicate_collection_name}")
            else:
                output_messages.append("No duplicate GEOMETRY_HISTORICAL data to save.")

            output_messages.extend(insert_cleaned_data(db_client, cleaned_df, database_name, filename))

            if not cleaned_df.empty:
                cleaned_df = cleaned_df.drop('_id', axis=1, errors='ignore')
                # db_client[COMBINED_Database_history][ALL_COMBINED_GEOMETRY_COLLECTION_history].insert_many(cleaned_df.to_dict("records"))
                # output_messages.append(f"Cleaned GEOMETRY_HISTORICAL data pushed to {COMBINED_Database_history}.{ALL_COMBINED_GEOMETRY_COLLECTION_history}")
                combined_geometry_coll = db_client[COMBINED_Database_history][ALL_COMBINED_GEOMETRY_COLLECTION_history]
                delete_result = combined_geometry_coll.delete_many({})
                output_messages.append(f"Deleted {delete_result.deleted_count} existing records from {ALL_COMBINED_GEOMETRY_COLLECTION_history}")

                # Now insert fresh cleaned data
                combined_geometry_coll.insert_many(cleaned_df.to_dict("records"))
                output_messages.append(f"Cleaned GEOMETRY_HISTORICAL data (replaced) pushed to {COMBINED_Database_history}.{ALL_COMBINED_GEOMETRY_COLLECTION_history}")
            else:
                output_messages.append("No cleaned GEOMETRY_HISTORICAL data to push to combined collection")

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

# def copy_to_single_collection_api():
#     output_messages = []

#     try:
#         source_db = client[COMBINED_Database_history]
#         target_db = client[UNIFORMITY_BALANCING_GEOMETRY_DB_history]

#         # Retrieve data from source collections
#         try:
#             uniformity_data = list(source_db[ALL_COMBINED_UNIFORMITY_COLLECTION_history].find())
#             output_messages.append(f"Retrieved {len(uniformity_data)} records from {ALL_COMBINED_UNIFORMITY_COLLECTION_history}")
#             uniformity_df = pd.DataFrame(uniformity_data)
#         except Exception as e:
#             output_messages.append(f"Error retrieving data from {ALL_COMBINED_UNIFORMITY_COLLECTION_history}: {str(e)}")
#             return output_messages

#         try:
#             balancing_data = list(source_db[ALL_COMBINED_BALANCING_COLLECTION_history].find())
#             output_messages.append(f"Retrieved {len(balancing_data)} records from {ALL_COMBINED_BALANCING_COLLECTION_history}")
#             balancing_df = pd.DataFrame(balancing_data)
#         except Exception as e:
#             output_messages.append(f"Error retrieving data from {ALL_COMBINED_BALANCING_COLLECTION_history}: {str(e)}")
#             balancing_df = pd.DataFrame()

#         try:
#             geometry_data = list(source_db[ALL_COMBINED_GEOMETRY_COLLECTION_history].find())
#             output_messages.append(f"Retrieved {len(geometry_data)} records from {ALL_COMBINED_GEOMETRY_COLLECTION_history}")
#             geometry_df = pd.DataFrame(geometry_data)
#         except Exception as e:
#             output_messages.append(f"Error retrieving data from {ALL_COMBINED_GEOMETRY_COLLECTION_history}: {str(e)}")
#             geometry_df = pd.DataFrame()

#         if uniformity_df.empty:
#             output_messages.append("UNIFORMITY_HISTORICAL data is empty. No data to process.")
#             return output_messages
        
#         db = client[COMBINED_Database_history]
#         all_combined_balancing_collection = db[ALL_COMBINED_BALANCING_COLLECTION_history]
#         all_combined_geometry_collection = db[ALL_COMBINED_GEOMETRY_COLLECTION_history]

#         try:
#             # Fetch balancing records
#             balancing_data = list(all_combined_balancing_collection.find(
#                 {}, {"barcode": 1, "topplane_deg": 1, "_id": 0}
#             ))

#             # Fetch geometry records
#             geometry_data = list(all_combined_geometry_collection.find(
#                 {}, {"barcode": 1, "topbul_mm": 1, "botbul_mm": 1, "topbul_deg": 1, "crroh1_deg": 1, "_id": 0}
#             ))

#             # Convert geometry data into dict for quick lookup by barcode
#             geometry_dict = {rec["barcode"]: rec for rec in geometry_data if "barcode" in rec}

#             # Merge balancing + geometry data by barcode
#             cimcorp_records = []
#             for bal in balancing_data:
#                 barcode = bal.get("barcode")
#                 merged = bal.copy()
#                 if barcode in geometry_dict:
#                     merged.update(geometry_dict[barcode])
#                 cimcorp_records.append(merged)

#             if not cimcorp_records:
#                 output_messages.append("Warning: No records found in both collections.")

#         except Exception as e:
#             output_messages.append(f"Error fetching data from MongoDB: {e}")
#             cimcorp_records = []

#         # Create lookup dictionaries from MongoDB records
#         topplane_deg_lookup = {}
#         topbul_mm_lookup = {}
#         botbul_mm_lookup = {}
#         topbul_deg_lookup = {}
#         crroh1_deg_lookup = {}
 
#         for rec in cimcorp_records:
#             barcode = rec.get('barcode')
#             if barcode is None:
#                 continue
 
#             # topplane_deg lookup
#             topplane_deg_lookup[barcode] = rec.get('topplane_deg')

#             # topbul_mm lookup
#             topbul_mm_lookup[barcode] = rec.get('topbul_mm')

#             # botbul_mm lookup
#             botbul_mm_lookup[barcode] = rec.get('botbul_mm')

#             # topbul_deg lookup
#             topbul_deg_lookup[barcode] = rec.get('topbul_deg')

#             # crroh1_deg lookup
#             crroh1_deg_lookup[barcode] = rec.get('crroh1_deg')

#         # Map values to uniformity_df
#         try:
#             uniformity_df['topplane_deg'] = uniformity_df['barcode'].map(topplane_deg_lookup)
#         except Exception as e:
#             output_messages.append(f"Error mapping topplane_deg to uniformity_df: {e}")
#             uniformity_df['topplane_deg'] = None

#         try:
#             uniformity_df['topbul_mm'] = uniformity_df['barcode'].map(topbul_mm_lookup)
#         except Exception as e:
#             output_messages.append(f"Error mapping topbul_mm to uniformity_df: {e}")
#             uniformity_df['topbul_mm'] = None
        
#         try:
#             uniformity_df['botbul_mm'] = uniformity_df['barcode'].map(botbul_mm_lookup)
#         except Exception as e:
#             output_messages.append(f"Error mapping botbul_mm to uniformity_df: {e}")
#             uniformity_df['botbul_mm'] = None

#         try:
#             uniformity_df['topbul_deg'] = uniformity_df['barcode'].map(topbul_deg_lookup)
#         except Exception as e:
#             output_messages.append(f"Error mapping topbul_deg to uniformity_df: {e}")
#             uniformity_df['topbul_deg'] = None

#         try:
#             uniformity_df['crroh1_deg'] = uniformity_df['barcode'].map(crroh1_deg_lookup)
#         except Exception as e:
#             output_messages.append(f"Error mapping topbul_deg to crroh1_deg: {e}")
#             uniformity_df['crroh1_deg'] = None
 
#         uniformity_df.replace('nan', pd.NA, inplace=True)
 
#         ##############added 05-july -2025##################################
#         balancing_df.replace('nan', pd.NA, inplace=True)
#         geometry_df.replace('nan', pd.NA, inplace=True)
 
#         uniformity_df['topplane_deg'] = uniformity_df['topplane_deg'].fillna('Unknown')
#         balancing_df['topplane_deg'] = balancing_df['topplane_deg'].fillna('Unknown')
#         uniformity_df['topbul_mm'] = uniformity_df['topbul_mm'].fillna('Unknown')
#         geometry_df['topbul_mm'] = geometry_df['topbul_mm'].fillna('Unknown')
#         uniformity_df['botbul_mm'] = uniformity_df['botbul_mm'].fillna('Unknown')
#         geometry_df['botbul_mm'] = geometry_df['botbul_mm'].fillna('Unknown')
#         uniformity_df['topbul_deg'] = uniformity_df['topbul_deg'].fillna('Unknown')
#         geometry_df['topbul_deg'] = geometry_df['topbul_deg'].fillna('Unknown')
#         uniformity_df['crroh1_deg'] = uniformity_df['crroh1_deg'].fillna('Unknown')
#         geometry_df['crroh1_deg'] = geometry_df['crroh1_deg'].fillna('Unknown')
#         uniformity_df['tbm_buildtime_shift'] = uniformity_df['tbm_buildtime'].apply(get_shift)

#         try:
#             # Fetch balancing records with specified columns
#             balancing_fields = {
#                 "barcode": 1, "topplane_deg": 1, "botplane_g": 1, "topplane_g": 1,
#                 "couple_g": 1, "static_g": 1, "botplane_deg": 1, "couple_deg": 1,
#                 "static_deg": 1, "_id": 0
#             }
#             balancing_data = list(all_combined_balancing_collection.find({}, balancing_fields))

#             # Fetch geometry records with specified columns
#             geometry_fields = {
#                 "barcode": 1, "topbul_mm": 1, "botbul_mm": 1, "topbul_deg": 1, "crroh1_deg": 1,
#                 "botdep_mm": 1, "topdep_mm": 1, "blro_mm": 1, "blroh1_mm": 1,
#                 "crro_mm": 1, "tlro_mm": 1, "tlroh1_mm": 1, "botbul_deg": 1,
#                 "botdep_deg": 1, "topdep_deg": 1, "blro_deg": 1, "blroh1_deg": 1,
#                 "crro_deg": 1, "tlro_deg": 1, "tlroh1_deg": 1, "_id": 0
#             }
#             geometry_data = list(all_combined_geometry_collection.find({}, geometry_fields))

#             # Convert geometry data into dict for quick lookup by barcode
#             geometry_dict = {rec["barcode"]: rec for rec in geometry_data if "barcode" in rec}

#             # Merge balancing + geometry data by barcode
#             cimcorp_records = []
#             for bal in balancing_data:
#                 barcode = bal.get("barcode")
#                 merged = bal.copy()
#                 if barcode in geometry_dict:
#                     merged.update(geometry_dict[barcode])
#                 cimcorp_records.append(merged)

#             if not cimcorp_records:
#                 output_messages.append("Warning: No records found in both collections.")

#         except Exception as e:
#             output_messages.append(f"Error fetching data from MongoDB: {e}")
#             cimcorp_records = []

#         # Create lookup dictionaries from MongoDB records
#         lookup_dicts = {
#             'topplane_deg': {}, 'botplane_g': {}, 'topplane_g': {}, 'couple_g': {},
#             'static_g': {}, 'botplane_deg': {}, 'couple_deg': {}, 'static_deg': {},
#             'topbul_mm': {}, 'botbul_mm': {}, 'botdep_mm': {}, 'topdep_mm': {},
#             'blro_mm': {}, 'blroh1_mm': {}, 'crro_mm': {}, 'crroh1_mm': {},
#             'tlro_mm': {}, 'tlroh1_mm': {}, 'botbul_deg': {}, 'botdep_deg': {},
#             'topbul_deg': {}, 'topdep_deg': {}, 'blro_deg': {}, 'blroh1_deg': {},
#             'crro_deg': {}, 'crroh1_deg': {}, 'tlro_deg': {}, 'tlroh1_deg': {}
#         }

#         for rec in cimcorp_records:
#             barcode = rec.get('barcode')
#             if barcode is None:
#                 continue

#             for field in lookup_dicts:
#                 lookup_dicts[field][barcode] = rec.get(field)

#         # Map values to uniformity_df
#         for field in lookup_dicts:
#             try:
#                 uniformity_df[field] = uniformity_df['barcode'].map(lookup_dicts[field])
#             except Exception as e:
#                 output_messages.append(f"Error mapping {field} to uniformity_df: {e}")
#                 uniformity_df[field] = None

#         uniformity_df.replace('nan', pd.NA, inplace=True)
#         balancing_df.replace('nan', pd.NA, inplace=True)
#         geometry_df.replace('nan', pd.NA, inplace=True)

#         # Fill NaN values with 'Unknown' for specified columns
#         for field in lookup_dicts:
#             if field in uniformity_df.columns:
#                 uniformity_df[field] = uniformity_df[field].fillna('Unknown')
#             if field in balancing_df.columns:
#                 balancing_df[field] = balancing_df[field].fillna('Unknown')
#             if field in geometry_df.columns:
#                 geometry_df[field] = geometry_df[field].fillna('Unknown')

#         # Fetch channel mapping data
#         try:
#             db = client[config_databse]
#             channel_no_collection = list(db[channel_no_collect].find())
#             channel_no_collection = pd.DataFrame(channel_no_collection)
#         except Exception as e:
#             output_messages.append(f"Error retrieving data from {channel_no_collect}: {str(e)}")
#             return output_messages

#         # Validation checks for channel_no_collection
#         if not isinstance(channel_no_collection, pd.DataFrame):
#             raise TypeError("channel_no_collection must be a pandas DataFrame.")
#         if 'sku' not in channel_no_collection.columns:
#             raise ValueError("The 'sku' column is missing in channel_no_collection dataframe.")
#         if 'gt_code' not in channel_no_collection.columns:
#             raise ValueError("The 'gt_code' column is missing in channel_no_collection dataframe.")

#         # Clean gt_code in channel_no_collection
#         def clean_gt_code(x):
#             try:
#                 if isinstance(x, list):
#                     return [str(item) for sublist in x for item in (sublist if isinstance(sublist, list) else [sublist])]
#                 return [str(item) for item in eval(x)] if isinstance(x, str) else [str(x)]
#             except (SyntaxError, NameError, TypeError):
#                 return [str(x)]

#         channel_no_collection['gt_code'] = channel_no_collection['gt_code'].apply(clean_gt_code)
#         valid_gt_codes = set()
#         for gt_codes in channel_no_collection['gt_code']:
#             valid_gt_codes.update(gt_codes)

#         # Clean and extract scalar gt_code for source DataFrames
#         def clean_source_gt_code(x):
#             try:
#                 if isinstance(x, list):
#                     return [str(item) for item in x]
#                 return [str(x)]
#             except (SyntaxError, NameError, TypeError):
#                 return [str(x)]

#         def get_scalar_gt_code(x):
#             return x[0] if x and isinstance(x, list) and len(x) > 0 else None

#         # Clean ubno values
#         def clean_ubno(x):
#             if isinstance(x, str) and x.endswith('.0'):
#                 return x[:-2]
#             return x

#         for df in [uniformity_df, balancing_df, geometry_df]:
#             if not df.empty and 'gt_code' in df.columns:
#                 df['gt_code_list'] = df['gt_code'].apply(clean_source_gt_code)
#                 df['gt_code'] = df['gt_code_list'].apply(get_scalar_gt_code)
#                 if 'ubno' in df.columns:
#                     df['ubno'] = df['ubno'].apply(clean_ubno)

#         # Filter DataFrames based on valid gt_codes
#         if not balancing_df.empty:
#             balancing_df = balancing_df[balancing_df['gt_code_list'].apply(lambda x: any(gt in valid_gt_codes for gt in x))]
#             output_messages.append(f"Filtered BALANCING_HISTORICAL to {len(balancing_df)} records based on channel_no_collect gt_code")
#         if not geometry_df.empty:
#             geometry_df = geometry_df[geometry_df['gt_code_list'].apply(lambda x: any(gt in valid_gt_codes for gt in x))]
#             output_messages.append(f"Filtered GEOMETRY_HISTORICAL to {len(geometry_df)} records based on channel_no_collect gt_code")

#         # Insert individual collections
#         def insert_with_check(collection_name, data_to_insert, existing_barcodes):
#             new_records = [doc for doc in data_to_insert if doc['barcode'] not in existing_barcodes]
#             if new_records:
#                 try:
#                     target_db[collection_name].insert_many(new_records)
#                     output_messages.append(f"Successfully appended {len(new_records)} new records into {target_db.name}.{collection_name}")
#                 except Exception as e:
#                     output_messages.append(f"Error inserting into {collection_name}: {str(e)}")
#             else:
#                 output_messages.append(f"No new records to append to {collection_name}")
#             return len(new_records)

#         # Modified: Exclude alert_id_ufm_ufm and ufm_machine_model_lot_id from uniformity_df before insertion
#         if not uniformity_df.empty:
#             # Remove specified columns from uniformity_df if they exist
#             columns_to_exclude_uniformity = ['alert_id_ufm_ufm', 'ufm_machine_model_lot_id']
#             uniformity_df = uniformity_df.drop(columns=[col for col in columns_to_exclude_uniformity if col in uniformity_df.columns])
#             data_to_insert_uniformity = uniformity_df.to_dict(orient="records")
#             existing_uniformity = {doc['barcode'] for doc in target_db[uniformity_collection_name_history].find({}, {'barcode': 1, '_id': 0})}
#             insert_with_check(uniformity_collection_name_history, data_to_insert_uniformity, existing_uniformity)
#             output_messages.append(f"Inserted all {len(data_to_insert_uniformity)} UNIFORMITY_HISTORICAL records from {ALL_COMBINED_UNIFORMITY_COLLECTION_history} into {uniformity_collection_name_history} without filtering")

#         if not balancing_df.empty:
#             data_to_insert_balancing = balancing_df.to_dict(orient="records")
#             existing_balancing = {doc['barcode'] for doc in target_db[balancing_collection_name_history].find({}, {'barcode': 1, '_id': 0})}
#             insert_with_check(balancing_collection_name_history, data_to_insert_balancing, existing_balancing)

#         if not geometry_df.empty:
#             data_to_insert_geometry = geometry_df.to_dict(orient="records")
#             existing_geometry = {doc['barcode'] for doc in target_db[geometry_collection_name_history].find({}, {'barcode': 1, '_id': 0})}
#             insert_with_check(geometry_collection_name_history, data_to_insert_geometry, existing_geometry)

#         # Get unique barcodes and gt_codes
#         uniformity_barcodes = set(uniformity_df['barcode'].astype(str)) if not uniformity_df.empty else set()
#         balancing_barcodes = set(balancing_df['barcode'].astype(str)) if not balancing_df.empty else set()
#         geometry_barcodes = set(geometry_df['barcode'].astype(str)) if not geometry_df.empty else set()

#         uniformity_gt_codes = set()
#         balancing_gt_codes = set()
#         geometry_gt_codes = set()

#         for df, gt_set in [(uniformity_df, uniformity_gt_codes), (balancing_df, balancing_gt_codes), (geometry_df, geometry_gt_codes)]:
#             if not df.empty and 'gt_code_list' in df.columns:
#                 for gt_codes in df['gt_code_list']:
#                     gt_set.update(gt_codes)

#         common_barcodes = uniformity_barcodes & balancing_barcodes & geometry_barcodes
#         common_gt_codes = uniformity_gt_codes & balancing_gt_codes & geometry_gt_codes

#         output_messages.append(f"Found {len(common_barcodes)} common barcodes and {len(common_gt_codes)} common gt_codes")

#         # Filter balancing and geometry DataFrames for combined data
#         if not balancing_df.empty:
#             balancing_df = balancing_df[
#                 (balancing_df['barcode'].astype(str).isin(common_barcodes)) &
#                 (balancing_df['gt_code_list'].apply(lambda x: any(gt in common_gt_codes for gt in x)))
#             ]
#             output_messages.append(f"Filtered BALANCING_HISTORICAL to {len(balancing_df)} records with common barcodes and gt_codes")

#         if not geometry_df.empty:
#             geometry_df = geometry_df[
#                 (geometry_df['barcode'].astype(str).isin(common_barcodes)) &
#                 (geometry_df['gt_code_list'].apply(lambda x: any(gt in valid_gt_codes for gt in x)))
#             ]
#             output_messages.append(f"Filtered GEOMETRY_HISTORICAL to {len(geometry_df)} records with common barcodes and gt_codes")

#         # Merge DataFrames with explicit column handling to avoid _x, _y suffixes
#         data_merged_combined = pd.DataFrame()
#         if not (uniformity_df.empty or balancing_df.empty or geometry_df.empty):
#             # Define columns to keep from each DataFrame
#             balancing_cols_to_keep = ['barcode', 'gt_code'] + [
#                 'botplane_g', 'topplane_g', 'couple_g', 'static_g',
#                 'botplane_deg', 'topplane_deg', 'couple_deg', 'static_deg'
#             ]
#             geometry_cols_to_keep = ['barcode', 'gt_code'] + [
#                 'botbul_mm', 'botdep_mm', 'topbul_mm', 'topdep_mm',
#                 'blro_mm', 'blroh1_mm', 'crro_mm', 'crroh1_mm',
#                 'tlro_mm', 'tlroh1_mm', 'botbul_deg', 'botdep_deg',
#                 'topbul_deg', 'topdep_deg', 'blro_deg', 'blroh1_deg',
#                 'crro_deg', 'crroh1_deg', 'tlro_deg', 'tlroh1_deg'
#             ]
#             uniformity_cols_to_keep = [col for col in uniformity_df.columns if col not in ['gt_code_list']]

#             # Filter DataFrames to keep only specified columns
#             temp_uniformity = uniformity_df[uniformity_cols_to_keep]
#             temp_balancing = balancing_df[balancing_cols_to_keep] if not balancing_df.empty else pd.DataFrame()
#             temp_geometry = geometry_df[geometry_cols_to_keep] if not geometry_df.empty else pd.DataFrame()

#             # Perform merges with explicit column selection
#             if not temp_balancing.empty:
#                 temp_merge = temp_uniformity.merge(temp_balancing, on=['barcode', 'gt_code'], how='inner', suffixes=('', '_bal'))
#                 # Drop any _bal suffixed columns
#                 temp_merge = temp_merge[[col for col in temp_merge.columns if not col.endswith('_bal')]]
#             else:
#                 temp_merge = temp_uniformity

#             if not temp_geometry.empty:
#                 data_merged_combined = temp_merge.merge(temp_geometry, on=['barcode', 'gt_code'], how='inner', suffixes=('', '_geo'))
#                 # Drop any _geo suffixed columns
#                 data_merged_combined = data_merged_combined[[col for col in data_merged_combined.columns if not col.endswith('_geo')]]
#             else:
#                 data_merged_combined = temp_merge

#             if data_merged_combined.empty:
#                 output_messages.append("No matching records found after merging UNIFORMITY_HISTORICAL, BALANCING_HISTORICAL, and GEOMETRY_HISTORICAL data.")
#             else:
#                 output_messages.append(f"Merged data resulted in {len(data_merged_combined)} records")
#                 if 'ubno' in data_merged_combined.columns:
#                     data_merged_combined['ubno'] = data_merged_combined['ubno'].apply(clean_ubno)

#         # Rename columns (excluding barcode and gt_code)
#         uniformity_cols = {col: f'{col}' for col in uniformity_df.columns if col not in ['barcode', 'gt_code', 'gt_code_list']}
#         balancing_cols = {col: f'{col}' for col in balancing_df.columns if col not in ['barcode', 'gt_code', 'gt_code_list']}
#         geometry_cols = {col: f'{col}' for col in geometry_df.columns if col not in ['barcode', 'gt_code', 'gt_code_list']}

#         uniformity_df.rename(columns=uniformity_cols, inplace=True)
#         if not balancing_df.empty:
#             balancing_df.rename(columns=balancing_cols, inplace=True)
#         if not geometry_df.empty:
#             geometry_df.rename(columns=geometry_cols, inplace=True)

#         # Modified: Remove specified alert and machine model columns from data_merged_combined before insertion
#         if not data_merged_combined.empty:
#             data_merged_combined.drop_duplicates(subset=['barcode', 'gt_code'], inplace=True)
#             # Define columns to exclude from COMBINED_COLLECTION_NAME_history
#             columns_to_exclude_combined = [
#                 'alert_id_ubno_balancing', 'alert_id_ubno_yield_drop', 'alert_id_curing_ufm',
#                 'alert_id_curing_balancing', 'alert_id_tbm_ufm', 'alert_id_tbm_balancing',
#                 'alert_id_tbm_yield_drop', 'alert_id_curing_yield_drop', 'alert_id_tbm_load_pressure',
#                 'alert_id_curing_load_pressure', 'tbm_machine_model_lot_id', 'tbm_previous_machine_model_lot_id', 'curing_machine_model_lot_id', 'curing_previous_machine_model_lot_id'
#             ]
#             # Remove specified columns if they exist
#             data_merged_combined = data_merged_combined.drop(columns=[col for col in columns_to_exclude_combined if col in data_merged_combined.columns])
#             # # Keep only existing columns for curing_machine_model_lot_id and curing_previous_machine_model_lot_id
#             # if 'curing_machine_model_lot_id' not in data_merged_combined.columns:
#             #     data_merged_combined['curing_machine_model_lot_id'] = None
#             # if 'curing_previous_machine_model_lot_id' not in data_merged_combined.columns:
#             #     data_merged_combined['curing_previous_machine_model_lot_id'] = None

#         data_to_insert_combined = data_merged_combined.to_dict(orient="records")

#         if data_merged_combined.empty:
#             output_messages.append("No matching records found in merged dataset to be copied.")

#         # Insert into COMBINED_COLLECTION_NAME_history
#         try:
#             existing = {doc['barcode'] for doc in target_db[COMBINED_COLLECTION_NAME_history].find({}, {'barcode': 1, '_id': 0})}
#             insert_with_check(COMBINED_COLLECTION_NAME_history, data_to_insert_combined, existing)
#             output_messages.extend(remove_duplicates_collection(target_db[COMBINED_COLLECTION_NAME_history]))
#         except Exception as e:
#             output_messages.append(f"Error accessing {COMBINED_COLLECTION_NAME_history}: {str(e)}")

#     except Exception as e:
#         error_message = f"An error occurred: {str(e)}"
#         output_messages.append(error_message)
#         output_messages.append(traceback.format_exc())

#     return output_messages

def copy_to_single_collection_api():
    output_messages = []

    try:
        source_db = client[COMBINED_Database_history]
        target_db = client[UNIFORMITY_BALANCING_GEOMETRY_DB_history]

        # Retrieve data from source collections
        try:
            uniformity_data = list(source_db[ALL_COMBINED_UNIFORMITY_COLLECTION_history].find())
            output_messages.append(f"Retrieved {len(uniformity_data)} records from {ALL_COMBINED_UNIFORMITY_COLLECTION_history}")
            uniformity_df = pd.DataFrame(uniformity_data)
        except Exception as e:
            output_messages.append(f"Error retrieving data from {ALL_COMBINED_UNIFORMITY_COLLECTION_history}: {str(e)}")
            return output_messages

        try:
            balancing_data = list(source_db[ALL_COMBINED_BALANCING_COLLECTION_history].find())
            output_messages.append(f"Retrieved {len(balancing_data)} records from {ALL_COMBINED_BALANCING_COLLECTION_history}")
            balancing_df = pd.DataFrame(balancing_data)
        except Exception as e:
            output_messages.append(f"Error retrieving data from {ALL_COMBINED_BALANCING_COLLECTION_history}: {str(e)}")
            balancing_df = pd.DataFrame()

        try:
            geometry_data = list(source_db[ALL_COMBINED_GEOMETRY_COLLECTION_history].find())
            output_messages.append(f"Retrieved {len(geometry_data)} records from {ALL_COMBINED_GEOMETRY_COLLECTION_history}")
            geometry_df = pd.DataFrame(geometry_data)
        except Exception as e:
            output_messages.append(f"Error retrieving data from {ALL_COMBINED_GEOMETRY_COLLECTION_history}: {str(e)}")
            geometry_df = pd.DataFrame()

        if uniformity_df.empty:
            output_messages.append("UNIFORMITY_HOME data is empty. No data to process.")
            return output_messages
        
        db = client[COMBINED_Database_history]
        all_combined_balancing_collection = db[ALL_COMBINED_BALANCING_COLLECTION_history]
        all_combined_geometry_collection = db[ALL_COMBINED_GEOMETRY_COLLECTION_history]

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
 
            # topplane_deg lookup
            topplane_deg_lookup[barcode] = rec.get('topplane_deg')

            # topbul_mm lookup
            topbul_mm_lookup[barcode] = rec.get('topbul_mm')

            # botbul_mm lookup
            botbul_mm_lookup[barcode] = rec.get('botbul_mm')

            # topbul_deg lookup
            topbul_deg_lookup[barcode] = rec.get('topbul_deg')

            # crroh1_deg lookup
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
            output_messages.append(f"Error mapping topbul_deg to crroh1_deg: {e}")
            uniformity_df['crroh1_deg'] = None
 
        uniformity_df.replace('nan', pd.NA, inplace=True)
 
        ##############added 05-july -2025##################################
        balancing_df.replace('nan', pd.NA, inplace=True)
        geometry_df.replace('nan', pd.NA, inplace=True)
 
        uniformity_df['topplane_deg'] = uniformity_df['topplane_deg'].fillna('Unknown')
        balancing_df['topplane_deg'] = balancing_df['topplane_deg'].fillna('Unknown')
        uniformity_df['topbul_mm'] = uniformity_df['topbul_mm'].fillna('Unknown')
        geometry_df['topbul_mm'] = geometry_df['topbul_mm'].fillna('Unknown')
        uniformity_df['botbul_mm'] = uniformity_df['botbul_mm'].fillna('Unknown')
        geometry_df['botbul_mm'] = geometry_df['botbul_mm'].fillna('Unknown')
        uniformity_df['topbul_deg'] = uniformity_df['topbul_deg'].fillna('Unknown')
        geometry_df['topbul_deg'] = geometry_df['topbul_deg'].fillna('Unknown')
        uniformity_df['crroh1_deg'] = uniformity_df['crroh1_deg'].fillna('Unknown')
        geometry_df['crroh1_deg'] = geometry_df['crroh1_deg'].fillna('Unknown')
        uniformity_df['tbm_buildtime_shift'] = uniformity_df['tbm_buildtime'].apply(get_shift)

        try:
            # Fetch balancing records with specified columns
            balancing_fields = {
                "barcode": 1, "topplane_deg": 1, "botplane_g": 1, "topplane_g": 1,
                "couple_g": 1, "static_g": 1, "botplane_deg": 1, "couple_deg": 1,
                "static_deg": 1, "balancing_class": 1, "_id": 0
            }
            balancing_data = list(all_combined_balancing_collection.find({}, balancing_fields))

            # Fetch geometry records with specified columns
            geometry_fields = {
                "barcode": 1, "topbul_mm": 1, "botbul_mm": 1, "topbul_deg": 1, "crroh1_deg": 1,
                "botdep_mm": 1, "topdep_mm": 1, "blro_mm": 1, "blroh1_mm": 1,
                "crro_mm": 1, "tlro_mm": 1, "tlroh1_mm": 1, "botbul_deg": 1,
                "botdep_deg": 1, "topdep_deg": 1, "blro_deg": 1, "blroh1_deg": 1,
                "crro_deg": 1, "tlro_deg": 1, "tlroh1_deg": 1, "geometry_class":1, "_id": 0
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
            'topbul_mm': {}, 'botbul_mm': {}, 'botdep_mm': {}, 'topdep_mm': {},
            'blro_mm': {}, 'blroh1_mm': {}, 'crro_mm': {}, 'crroh1_mm': {},
            'tlro_mm': {}, 'tlroh1_mm': {}, 'botbul_deg': {}, 'botdep_deg': {},
            'topbul_deg': {}, 'topdep_deg': {}, 'blro_deg': {}, 'blroh1_deg': {},
            'crro_deg': {}, 'crroh1_deg': {}, 'tlro_deg': {}, 'tlroh1_deg': {}, 'geometry_class':{}, 'balancing_class': {}
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
            output_messages.append(f"Filtered BALANCING_HOME to {len(balancing_df)} records based on channel_no_collect gt_code")
        if not geometry_df.empty:
            geometry_df = geometry_df[geometry_df['gt_code_list'].apply(lambda x: any(gt in valid_gt_codes for gt in x))]
            output_messages.append(f"Filtered GEOMETRY_HOME to {len(geometry_df)} records based on channel_no_collect gt_code")

        # Insert individual collections
        def insert_with_check(collection_name, data_to_insert, existing_barcodes):
            new_records = [doc for doc in data_to_insert if doc['barcode'] not in existing_barcodes]
            if new_records:
                try:
                    target_db[collection_name].insert_many(new_records)
                    output_messages.append(f"Successfully appended {len(new_records)} new records into {target_db.name}.{collection_name}")
                except Exception as e:
                    output_messages.append(f"Error inserting into {collection_name}: {str(e)}")
            else:
                output_messages.append(f"No new records to append to {collection_name}")
            return len(new_records)

        # Modified: Exclude alert_id_ufm_ufm and ufm_machine_model_lot_id from uniformity_df before insertion
        if not uniformity_df.empty:
            # Remove specified columns from uniformity_df if they exist
            columns_to_exclude_uniformity = ['alert_id_ufm_ufm', 'ufm_machine_model_lot_id']
            uniformity_df = uniformity_df.drop(columns=[col for col in columns_to_exclude_uniformity if col in uniformity_df.columns])
            data_to_insert_uniformity = uniformity_df.to_dict(orient="records")
            existing_uniformity = {doc['barcode'] for doc in target_db[uniformity_collection_name_history].find({}, {'barcode': 1, '_id': 0})}
            insert_with_check(uniformity_collection_name_history, data_to_insert_uniformity, existing_uniformity)
            output_messages.append(f"Inserted all {len(data_to_insert_uniformity)} UNIFORMITY_HOME records from {ALL_COMBINED_UNIFORMITY_COLLECTION_history} into {uniformity_collection_name_history} without filtering")

        if not balancing_df.empty:
            data_to_insert_balancing = balancing_df.to_dict(orient="records")
            existing_balancing = {doc['barcode'] for doc in target_db[balancing_collection_name_history].find({}, {'barcode': 1, '_id': 0})}
            insert_with_check(balancing_collection_name_history, data_to_insert_balancing, existing_balancing)

        if not geometry_df.empty:
            data_to_insert_geometry = geometry_df.to_dict(orient="records")
            existing_geometry = {doc['barcode'] for doc in target_db[geometry_collection_name_history].find({}, {'barcode': 1, '_id': 0})}
            insert_with_check(geometry_collection_name_history, data_to_insert_geometry, existing_geometry)

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
            output_messages.append(f"Filtered BALANCING_HOME to {len(balancing_df)} records with common barcodes and gt_codes")

        if not geometry_df.empty:
            geometry_df = geometry_df[
                (geometry_df['barcode'].astype(str).isin(common_barcodes)) &
                (geometry_df['gt_code_list'].apply(lambda x: any(gt in valid_gt_codes for gt in x)))
            ]
            output_messages.append(f"Filtered GEOMETRY_HOME to {len(geometry_df)} records with common barcodes and gt_codes")

        # Merge DataFrames with explicit column handling to avoid _x, _y suffixes
        data_merged_combined = pd.DataFrame()
        if not (uniformity_df.empty or balancing_df.empty or geometry_df.empty):
            # Define columns to keep from each DataFrame
            balancing_cols_to_keep = ['barcode', 'gt_code'] + [
                'botplane_g', 'topplane_g', 'couple_g', 'static_g',
                'botplane_deg', 'topplane_deg', 'couple_deg', 'static_deg', 'balancing_class'
            ]
            geometry_cols_to_keep = ['barcode', 'gt_code'] + [
                'botbul_mm', 'botdep_mm', 'topbul_mm', 'topdep_mm',
                'blro_mm', 'blroh1_mm', 'crro_mm', 'crroh1_mm',
                'tlro_mm', 'tlroh1_mm', 'botbul_deg', 'botdep_deg',
                'topbul_deg', 'topdep_deg', 'blro_deg', 'blroh1_deg',
                'crro_deg', 'crroh1_deg', 'tlro_deg', 'tlroh1_deg', 'geometry_class' 
            ]
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
                data_merged_combined = temp_merge.merge(temp_geometry, on=['barcode', 'gt_code'], how='inner', suffixes=('', '_geo'))
                # Drop any _geo suffixed columns
                data_merged_combined = data_merged_combined[[col for col in data_merged_combined.columns if not col.endswith('_geo')]]
            else:
                data_merged_combined = temp_merge

            if data_merged_combined.empty:
                output_messages.append("No matching records found after merging UNIFORMITY_HOME, BALANCING_HOME, and GEOMETRY_HOME data.")
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

        # Modified: Remove specified alert and machine model columns from data_merged_combined before insertion
        if not data_merged_combined.empty:
            data_merged_combined.drop_duplicates(subset=['barcode', 'gt_code'], inplace=True)
            # Define columns to exclude from COMBINED_COLLECTION_NAME_history
            columns_to_exclude_combined = [
                'alert_id_ubno_balancing', 'alert_id_ubno_yield_drop', 'alert_id_curing_ufm',
                'alert_id_curing_balancing', 'alert_id_tbm_ufm', 'alert_id_tbm_balancing',
                'alert_id_tbm_yield_drop', 'alert_id_curing_yield_drop', 'alert_id_tbm_load_pressure',
                'alert_id_curing_load_pressure', 'tbm_machine_model_lot_id', 'tbm_previous_machine_model_lot_id', 'curing_machine_model_lot_id', 'curing_previous_machine_model_lot_id'
            ]
            # Remove specified columns if they exist
            data_merged_combined = data_merged_combined.drop(columns=[col for col in columns_to_exclude_combined if col in data_merged_combined.columns])
            # # Keep only existing columns for curing_machine_model_lot_id and curing_previous_machine_model_lot_id
            # if 'curing_machine_model_lot_id' not in data_merged_combined.columns:
            #     data_merged_combined['curing_machine_model_lot_id'] = None
            # if 'curing_previous_machine_model_lot_id' not in data_merged_combined.columns:
            #     data_merged_combined['curing_previous_machine_model_lot_id'] = None

        data_to_insert_combined = data_merged_combined.to_dict(orient="records")

        if data_merged_combined.empty:
            output_messages.append("No matching records found in merged dataset to be copied.")

        # Insert into COMBINED_COLLECTION_NAME_history
        try:
            existing = {doc['barcode'] for doc in target_db[COMBINED_COLLECTION_NAME_history].find({}, {'barcode': 1, '_id': 0})}
            insert_with_check(COMBINED_COLLECTION_NAME_history, data_to_insert_combined, existing)
            output_messages.extend(remove_duplicates_collection(target_db[COMBINED_COLLECTION_NAME_history]))
        except Exception as e:
            output_messages.append(f"Error accessing {COMBINED_COLLECTION_NAME_history}: {str(e)}")

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
                "_id": "$barcode",
                "uniqueIds": {"$addToSet": "$_id"},
                "count": {"$sum": 1}
            }
        },
        {"$match": {"count": {"$gt": 1}}}
    ]
    
    duplicates = list(collection.aggregate(pipeline))
    
    for doc in duplicates:
        ids_to_remove = doc["uniqueIds"][1:]  # Keep first ID, remove rest
        collection.delete_many({"_id": {"$in": ids_to_remove}})
        messages.append(f"Removed {len(ids_to_remove)} duplicate records for barcode {doc['_id']}")
    
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
        files_to_process = get_unprocessed_files_api()
        if not files_to_process:
            output_messages.append("No unprocessed files found for data cleaning.")
            final_message = "No unprocessed files found for data cleaning."
            return {"status": "success", "message": final_message, "output_log": output_messages, "cleaned_collections": cleaned_collection_names, "invalid_collections": invalid_collection_names, "execution_time": 0}

        for file_data in files_to_process:
            database_name = file_data["database_name"]
            filename = file_data["filename"]
            file_id = file_data["_id"]

            output_messages.append(f"Processing file: {filename} from database: {database_name}")

            source_collection_name = sanitize_collection_name(filename)
            if database_name == "UNIFORMITY_HISTORICAL":
                source_db_name = UNIFORMITY_DB_history
            elif database_name == "BALANCING_HISTORICAL":
                source_db_name = BALANCING_DB_history
            elif database_name == "GEOMETRY_HISTORICAL":
                source_db_name = GEOMETRY_DB_history
            else:
                output_messages.append(f"Unknown database: {database_name}")
                continue

            source_data = client[source_db_name][source_collection_name].find()

            process_msgs, cleaned_collect_name, invalid_collect_name = process_data_for_api(client, source_data, database_name, filename)
            output_messages.extend(process_msgs)
            cleaned_collection_names.append(cleaned_collect_name)
            invalid_collection_names.append(invalid_collect_name)

            db = client[FILE_TRACKING_DB]
            db[FILE_TRACKING_COLLECTION].update_one(
                {"_id": file_id},
                {"$set": {"processed": True, "processed_timestamp": some_function()}}
            )
            output_messages.append(f"File {filename} processed and marked as processed in file tracking.")

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
        logging.info("Inserting UNIFORMITY_HISTORICAL sample data into MongoDB...")
        insert_with_timestamp(df_uniformity, UNIFORMITY_CLEANED_PREFIX_history, "UNIFORMITY_HISTORICAL")
        logging.info("UNIFORMITY_HISTORICAL sample data inserted and tracked.")
        logging.info("Inserting BALANCING_HISTORICAL sample data into MongoDB...")
        insert_with_timestamp(df_balancing, BALANCING_CLEANED_PREFIX_history, "BALANCING_HISTORICAL")
        logging.info("GEOMETRY_HISTORICAL sample data inserted and tracked.")
        insert_with_timestamp(df_geometry, GEOMETRY_CLEANED_PREFIX_history, "GEOMETRY_HISTORICAL")
        logging.info("GEOMETRY_HISTORICAL sample data inserted and tracked.")
        ensure_admin_collection_exists()
        logging.info("Admin collection check completed.")
        result = run_data_cleaning()
        logging.info(f"Data cleaning result: {result['message']}")
        logging.info(f"Execution time: {result['execution_time']:.2f} seconds")
        logging.info("Output log:\n" + "\n".join(result['output_log']))
        logging.info(f"Cleaned collections: {result['cleaned_collections']}")
        logging.info(f"Invalid collections: {result['invalid_collections']}")
        print(result["message"])
        print(f"Execution time: {result['execution_time']:.2f} seconds")
        print("Output log:", result["output_log"])
        print("Cleaned collections:", result["cleaned_collections"])
        print("Invalid collections:", result["invalid_collections"])

    except Exception as e:
        error_message = f"Error in main function: {str(e)}"
        logging.error(error_message)
        logging.error(traceback.format_exc())
        print(error_message)

if __name__ == "__main__":
    main()