import psycopg2
import json
import pymongo
import time
from psycopg2.extras import RealDictCursor, execute_values
from collections import defaultdict, deque
from datetime import datetime, timedelta, date

# Configuration
TOL_SECONDS = 300
MAX_GENEALOGY_DEPTH = 10

# TARGET_TBM_MACHINE_MODEL_LOT_ID is now fetched dynamically from MongoDB
# (alert_db_final_yield / alert_triggering_tbm_with_lot_size, machine_flag: false)
TARGET_TBM_MACHINE_MODEL_LOT_ID = None  # Will be populated at runtime

# PLANT_CODE_FILTER remains static
PLANT_CODE_FILTER = "P1007"

# Timeout settings (in seconds)
QUERY_TIMEOUT = 120  # 2 minutes timeout for long queries
CONNECTION_TIMEOUT = 300000

# MongoDB output database for machine parameter data
MACHINE_PARAMS_DB = "db_machine_parametrs_data"

# ============================================================================
# SSH & MONGODB SETTINGS
# ============================================================================

MONGO_URI = "mongodb://admin:admin123@10.100.192.85:27017/"


def get_mongo_client():
    """Return a pymongo MongoClient using direct connection."""
    client = pymongo.MongoClient(MONGO_URI)
    return client


def close_mongo_client(client):
    """Close a MongoClient."""
    if client is None:
        return
    try:
        client.close()
    except Exception:
        pass

# ============================================================================
# TABLE DEFINITIONS AND CATEGORIES
# ============================================================================

# Define all tables we are working with
ALL_TABLES = {
    # Core Tables
    "opcua_barcode": "OPCUA Barcode",
    "genealogy": "Genealogy",
    "charge": "Charge",
    "charge_history": "Charge History",
    "article": "Article",
    # Machine Parameter Tables
    "st1_machine_parameters": "ST1 Machine Parameters",
    "st2_machine_parameters": "ST2 Machine Parameters",
    "st1_recipe_change": "ST1 Recipe Change",
    "st2_recipe_change": "ST2 Recipe Change",
    # Cycle Time Tables
    "cycle_time": "Cycle Time Data",
    # Genealogy Hierarchy
    "genealogy_hierarchy_data": "Genealogy Hierarchy Data",
}

# Categorize tables for better organization in the UI
TABLE_CATEGORIES = {
    "genealogy": ["genealogy", "charge", "charge_history", "article", "opcua_barcode", 
                  "genealogy_hierarchy_data"],
    "machine_parameters": ["st1_machine_parameters", "st2_machine_parameters", 
                          "st1_recipe_change", "st2_recipe_change",
                          "st1_machine_tooling", "st2_machine_tooling"],
    "cycle_time": ["cycle_time"]
}

# Define important columns for each table (for preview purposes)
TABLE_COLUMN_GROUPS = {
    # Core tables
    "opcua_barcode": ["barcode", "machine_code", "charge_id", "created_at"],
    "genealogy": ["output_charge_id", "input_charge_id", "created_at", "operation_type"],
    "charge": ["id", "article_code", "size_code", "weight", "quantity", "status", "carrier_code", "plant_code"],
    "charge_history": ["charge_id", "article_code", "carrier_code", "transaction_type", "created_at"],
    "article": ["article_code", "description", "size_code", "category", "created_at"],
    
    # Machine parameter tables (sample columns)
    "st1_machine_parameters": ["machine_name", "plant_code", "created_at", "parameter_name", "parameter_value"],
    "st2_machine_parameters": ["machine_name", "plant_code", "created_at", "parameter_name", "parameter_value"],
    "st1_recipe_change": ["machine_name", "plant_code", "created_at", "recipe_id", "recipe_name"],
    "st2_recipe_change": ["machine_name", "plant_code", "created_at", "recipe_id", "recipe_name"],
    
    # Cycle time
    "cycle_time": ["s2_barcode_ct", "s2_machine_name", "s2_running_gtcode", "s2_shift", "s2_total_cycle", "s1_total_cycle"],
    
    # Genealogy hierarchy
    "genealogy_hierarchy_data": ["barcode", "level", "root_charge_id", "current_charge_id", "output_charge_id", 
                                 "input_charge_id", "article_code", "article_description", "hierarchy_path"]
}

# ============================================================================
# MONGODB FUNCTIONS (Combined)
# ============================================================================

def get_mongo_client():
    """Return a pymongo MongoClient using direct connection."""
    client = pymongo.MongoClient(MONGO_URI)
    return client


def close_mongo_client(client):
    """Close a MongoClient."""
    if client is None:
        return
    try:
        client.close()
    except Exception:
        pass


# ============================================================================
# SOURCE CONFIGURATIONS
# Each entry describes one MongoDB alert collection to process.
# ============================================================================

# TBM source
TBM_SOURCE = {
    "label":             "TBM",
    "collection":        "alert_triggering_tbm_with_lot_size",
    # Field inside machine_alerts[] that holds the computed lot id
    "machine_model_lot_id_field": "tbm_machine_model_lot_id",
    # Field inside machine_alerts[] that holds the TBM machine name
    # used to derive ST1 / ST2 filter  (machine + "_1" / machine + "_2")
    "machine_field":     "machine",
    # Path to shifts array inside each machine_alerts element
    "shifts_field":      "shifts_data",
}

# UFM source — barcodes come from shifts_data too;
# machine filter is derived from machine_alerts[].tbm_machine (the TBM machine for that UFM lot)
UFM_SOURCE = {
    "label":             "UFM",
    "collection":        "alert_triggering_ufm_with_lot_size",
    "machine_model_lot_id_field": "ufm_machine_model_lot_id",
    # UFM documents store the associated TBM machine in machine_alerts[].tbm_machine
    "machine_field":     "tbm_machine",
    "shifts_field":      "shifts_data",
}

# Curing source — same structure as UFM; tbm_machine used for ST1/ST2 filter
CURING_SOURCE = {
    "label":             "Curing",
    "collection":        "alert_triggering_curing_with_lot_size",
    "machine_model_lot_id_field": "curing_machine_model_lot_id",
    # Curing documents store the associated TBM machine in machine_alerts[].tbm_machine
    "machine_field":     "tbm_machine",
    "shifts_field":      "shifts_data",
}

ALL_SOURCES = [TBM_SOURCE, UFM_SOURCE, CURING_SOURCE]


def fetch_target_lot_ids_from_mongo(source, client=None):
    """
    Fetch lot_ids where machine_flag == False from the given source collection.
    Returns a list of unique top-level lot_id strings.
    """
    own_client = client is None
    lot_ids = []
    try:
        if own_client:
            client = get_mongo_client()
        col = client["alert_db_final_yield"][source["collection"]]

        # Fetch all distinct gt_codes present in the collection (no filter)
        gt_codes = col.distinct("machine_alerts.gt_code")
        print(f"  [{source['label']}] Found {len(gt_codes)} distinct gt_code(s): {gt_codes}")

        # Process each gt_code — skip if no records found
        for gt_code in gt_codes:
            pipeline = [
                {"$match": {
                    "machine_flag": False,
                    "machine_alerts.gt_code": gt_code,
                }},
                {"$group": {"_id": "$lot_id"}},
            ]

            result = list(col.aggregate(pipeline))
            gt_lot_ids = [doc["_id"] for doc in result if doc.get("_id")]

            if not gt_lot_ids:
                print(f"  [{source['label']}] gt_code={gt_code}: no records found — skipping.")
                continue

            print(f"  [{source['label']}] gt_code={gt_code}: {len(gt_lot_ids)} lot ID(s) found: {gt_lot_ids}")
            lot_ids.extend(gt_lot_ids)

        # Deduplicate while preserving order
        seen = set()
        lot_ids = [x for x in lot_ids if not (x in seen or seen.add(x))]
        print(f"  [{source['label']}] Total unique lot IDs: {len(lot_ids)}")

    except Exception as e:
        print(f"  [{source['label']}] Error fetching lot IDs: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if own_client:
            close_mongo_client(client)
    return lot_ids


def get_machine_filter_for_lot(lot_id, source, client=None):
    """
    For a given lot_id, read machine_alerts[].{machine_field} and derive:
      ST1 filter = machine + "_1"
      ST2 filter = machine + "_2"
    Returns (st1_filter, st2_filter) or (None, None) if not found.
    """
    own_client = client is None
    try:
        if own_client:
            client = get_mongo_client()
        col = client["alert_db_final_yield"][source["collection"]]

        pipeline = [
            {"$match": {"lot_id": lot_id, "machine_flag": False}},
            {"$unwind": "$machine_alerts"},
            {"$project": {"machine": f"$machine_alerts.{source['machine_field']}"}},
            {"$limit": 1},
        ]

        result = list(col.aggregate(pipeline))
        if result and result[0].get("machine"):
            machine = result[0]["machine"]
            st1_filter = f"{machine}_1"
            st2_filter = f"{machine}_2"
            print(f"  [{source['label']}] Lot {lot_id}: machine={machine}, "
                  f"ST1={st1_filter}, ST2={st2_filter}")
            return st1_filter, st2_filter
        else:
            print(f"  [{source['label']}] Warning: No machine found for lot {lot_id}")
            return None, None
    except Exception as e:
        print(f"  [{source['label']}] Error fetching machine for lot {lot_id}: {e}")
        return None, None
    finally:
        if own_client:
            close_mongo_client(client)


def get_lotwise_barcodes_from_mongo(target_lot_id, source, client=None):
    """
    Fetch barcodes for a specific lot_id from the given source collection.
    Returns:
      lot_map         : {lot_id: [barcode, ...]}
      model_lot_id_map: {lot_id: machine_model_lot_id}  (from machine_alerts[])
    """
    own_client = client is None
    lot_map = {}
    model_lot_id_map = {}
    model_lot_id_field = source["machine_model_lot_id_field"]
    shifts_field = source["shifts_field"]
    try:
        if own_client:
            client = get_mongo_client()
        col = client["alert_db_final_yield"][source["collection"]]

        pipeline = [
            {"$match": {"lot_id": target_lot_id, "machine_flag": False}},
            {"$unwind": "$machine_alerts"},
            {"$unwind": f"$machine_alerts.{shifts_field}"},
            {"$match": {
                f"machine_alerts.{shifts_field}.barcodes": {"$exists": True, "$ne": []},
            }},
            {"$project": {
                "barcodes":            f"$machine_alerts.{shifts_field}.barcodes",
                "lot_id":              "$lot_id",
                "model_lot_id":        f"$machine_alerts.{model_lot_id_field}",
            }},
            {"$unwind": "$barcodes"},
            {"$addFields": {
                "clean_barcode": {
                    "$ltrim": {"input": {"$toString": "$barcodes"}, "chars": "0"}
                }
            }},
            {"$group": {
                "_id":          "$lot_id",
                "barcodes":     {"$push": "$clean_barcode"},
                "model_lot_id": {"$first": "$model_lot_id"},
            }},
        ]

        for doc in col.aggregate(pipeline):
            lid      = doc["_id"]
            barcodes = doc["barcodes"]
            mid      = doc.get("model_lot_id")
            lot_map[lid]          = barcodes
            model_lot_id_map[lid] = mid
            print(f"  [{source['label']}] {len(barcodes)} barcodes for lot {lid} "
                  f"({model_lot_id_field}={mid})")

    except Exception as e:
        print(f"  [{source['label']}] Error in barcode aggregation for {target_lot_id}: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if own_client:
            close_mongo_client(client)

    return lot_map, model_lot_id_map


def mark_lot_processed(lot_id, source, client=None):
    """
    Flip machine_flag False -> True for all documents with the given lot_id.
    If a shared client is provided it is reused; otherwise a new one is opened.
    """
    own_client = client is None
    try:
        if own_client:
            client = get_mongo_client()
        col = client["alert_db_final_yield"][source["collection"]]
        result = col.update_many(
            {"lot_id": lot_id, "machine_flag": False},
            {"$set": {"machine_flag": True}},
        )
        print(f"  [{source['label']}] machine_flag set to True on "
              f"{result.modified_count} document(s) for lot {lot_id}")
    except Exception as e:
        print(f"  [{source['label']}] Error updating machine_flag for lot {lot_id}: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if own_client:
            close_mongo_client(client)


def save_data_to_mongo(lot_id, table_name, data_list, client=None):
    """
    Save data_list to MongoDB collection {table_name} in db_machine_parametrs_data.
    If a shared client is provided it is reused (no tunnel open/close per call).
    """
    if not data_list:
        print(f"    No data to save for {lot_id} / {table_name}")
        return

    own_client = client is None
    try:
        if own_client:
            client = get_mongo_client()
        db = client[MACHINE_PARAMS_DB]
        collection_name = str(table_name).replace(" ", "_")
        col = db[collection_name]

        docs = []
        for row in data_list:
            doc = {}
            for k, v in row.items():
                if isinstance(v, (datetime, date)):
                    doc[k] = v.isoformat()
                elif v is None:
                    doc[k] = None
                else:
                    doc[k] = v
            docs.append(doc)

        col.insert_many(docs)
        print(f"    Inserted {len(docs)} records into {MACHINE_PARAMS_DB}.{collection_name}")
    except Exception as e:
        print(f"    Error saving to MongoDB ({lot_id}/{table_name}): {e}")
        import traceback
        traceback.print_exc()
    finally:
        if own_client:
            close_mongo_client(client)


def save_all_and_mark(lot_id, source, section_data, tables_to_save, shared_client=None):
    """
    Insert all tables and mark machine_flag=True using a single MongoDB connection.
    If shared_client is provided it is reused (no extra tunnel); otherwise opens one.
    """
    own_client = shared_client is None
    client = shared_client
    try:
        if own_client:
            print(f"  [{source['label']}] Opening MongoDB connection for lot {lot_id}...")
            client = get_mongo_client()
        for table_name in tables_to_save:
            data_list = section_data.get(table_name, [])
            save_data_to_mongo(lot_id, table_name, data_list, client=client)
        mark_lot_processed(lot_id, source, client=client)
    except Exception as e:
        print(f"  [{source['label']}] MongoDB error for lot {lot_id}: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if own_client:
            close_mongo_client(client)

# ============================================================================
# DATABASE CONNECTION
# ============================================================================

def get_connection():
    """Get database connection with proper error handling"""
    try:
        conn = psycopg2.connect(
            host='atl-dl-prd-01.cuvxusm4qify.ap-south-1.redshift.amazonaws.com',
            database= 'db_prd_plant_che',
            port= 5439,
            user= 'usr_deepak_acharya',
            password= 'La794FN70HOY*q',
            connect_timeout=CONNECTION_TIMEOUT
        )
        
        # Set statement timeout
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = {QUERY_TIMEOUT * 100000};")  # Convert to milliseconds
            conn.commit()
        
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        raise

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def clean_barcode(barcode):
    """Clean barcode by removing leading zeros and '.0' suffix"""
    if barcode is None:
        return None
    
    barcode_str = str(barcode)
    barcode_str = barcode_str.lstrip('0')
    if barcode_str.endswith('.0'):
        barcode_str = barcode_str[:-2]
    
    return barcode_str

def normalize_datetime(dt):
    """Convert datetime to offset-naive if it's offset-aware"""
    if dt is None:
        return None
    if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
        return dt.replace(tzinfo=None)
    return dt

def flatten_record(record_data, prefix=''):
    """Flatten nested dictionary into flat key-value pairs"""
    flat_data = {}
    
    if not isinstance(record_data, dict):
        flat_data[prefix.rstrip('_') if prefix else 'value'] = record_data
        return flat_data
    
    for key, value in record_data.items():
        key_str = str(key)
        full_key = f"{prefix}{key_str}" if prefix else key_str
        
        if isinstance(value, dict):
            flat_data.update(flatten_record(value, f"{full_key}_"))
        elif isinstance(value, list):
            if value and isinstance(value[0], str):
                flat_data[full_key] = ', '.join(map(str, value))
            else:
                flat_data[full_key] = json.dumps(value, default=str)
        else:
            flat_data[full_key] = value
    
    return flat_data

# ============================================================================
# UPDATED GENEALOGY HIERARCHY DATA FUNCTION
# ============================================================================

# ============================================================================
# REQUIREMENT 4: IMPROVED MACHINE SNAPSHOTS FUNCTION
# ============================================================================

def fetch_machine_snapshots_improved(cur, table_name, tbm_times_map, machine_filter, plant_filter):
    """
    Requirement 4: Improved machine snapshots with before/actual/after logic
    
    For each barcode, get maximum 3 records (before, actual, after) based on tbm_buildtime
    If actual not found, get closest before and after
    If only one or two records exist, get whatever is available
    """
    results = defaultdict(list)
    
    if not tbm_times_map:
        return results
    
    print(f"    Fetching {table_name} with improved before/actual/after logic...")
    
    for barcode, tbm_time in tbm_times_map.items():
        if not tbm_time:
            continue
        
        tbm_time_normalized = normalize_datetime(tbm_time)
        before_time = tbm_time_normalized - timedelta(hours=2)
        after_time = tbm_time_normalized + timedelta(hours=2)
        
        sql = f"""
            SELECT 
                *,
                ABS(EXTRACT(EPOCH FROM (created_at - %s))) AS time_diff_seconds,
                CASE 
                    WHEN created_at < %s THEN 'before'
                    WHEN created_at = %s THEN 'actual'
                    ELSE 'after'
                END AS snapshot_type_raw
            FROM {table_name}
            WHERE created_at BETWEEN %s AND %s
              AND machine_name = %s
              AND plant_code = %s
            ORDER BY created_at
        """
        
        try:
            cur.execute(sql, [
                tbm_time_normalized, tbm_time_normalized, tbm_time_normalized,
                before_time, after_time, machine_filter, plant_filter
            ])
            all_records = cur.fetchall()
            
            if not all_records:
                continue
            
            # Categorize records
            before_records = []
            actual_records = []
            after_records = []
            
            for record in all_records:
                record_dict = dict(record)
                created_at = normalize_datetime(record_dict.get('created_at'))
                
                if created_at < tbm_time_normalized:
                    before_records.append(record_dict)
                elif created_at == tbm_time_normalized:
                    actual_records.append(record_dict)
                else:
                    after_records.append(record_dict)
            
            # Select best records (max 3 total)
            selected_records = []
            
            # 1. Always include actual if exists (take closest one if multiple)
            if actual_records:
                actual_records.sort(key=lambda x: x['time_diff_seconds'])
                best_actual = actual_records[0]
                best_actual['snapshot_type'] = 'actual'
                selected_records.append(best_actual)
            
            # 2. Include closest before if exists
            if before_records:
                before_records.sort(key=lambda x: x['time_diff_seconds'])
                best_before = before_records[0]
                best_before['snapshot_type'] = 'before'
                selected_records.append(best_before)
            
            # 3. Include closest after if exists
            if after_records:
                after_records.sort(key=lambda x: x['time_diff_seconds'])
                best_after = after_records[0]
                best_after['snapshot_type'] = 'after'
                selected_records.append(best_after)
            
            # 4. If no actual found, we might have only before and/or after
            # Sort selected records by time_diff_seconds and take max 3
            selected_records.sort(key=lambda x: x['time_diff_seconds'])
            
            # Ensure we don't have more than 3 records
            if len(selected_records) > 3:
                selected_records = selected_records[:3]
            
            # Add barcode and tbm_buildtime to each record
            for record in selected_records:
                record['barcode'] = barcode
                record['tbm_buildtime'] = tbm_time
                record['time_diff_minutes'] = record.get('time_diff_seconds', 0) / 60
                
                # Remove temporary fields
                if 'snapshot_type_raw' in record:
                    del record['snapshot_type_raw']
                if 'time_diff_seconds' in record:
                    del record['time_diff_seconds']
            
            results[barcode] = selected_records
            
        except Exception as e:
            print(f"      Error for barcode {barcode} in {table_name}: {e}")
            continue
    
    return results

# ============================================================================
# DATA FETCHING FUNCTIONS (COMBINED)
# ============================================================================

def fetch_data_batch(cur, table_name, id_field, ids):
    """Fetch data in batches for given IDs"""
    if not ids:
        return []
    
    batch_size = 1000
    all_results = []
    
    for i in range(0, len(ids), batch_size):
        batch_ids = ids[i:i + batch_size]
        placeholders = ','.join(['%s'] * len(batch_ids))
        sql = f"SELECT * FROM {table_name} WHERE {id_field} IN ({placeholders})"
        
        cur.execute(sql, batch_ids)
        all_results.extend(cur.fetchall())
    
    return all_results

def fetch_contexts_batch(cur, barcodes):
    """Fetch contexts for all barcodes in a single query - Redshift compatible with WP machine filter"""
    if not barcodes:
        return {}, []
    
    sql = """
        WITH ranked_data AS (
            SELECT
                LTRIM(ob.barcode,'0') AS barcode_norm,
                ob.barcode,
                ob.machine_code,
                tbm.gttypeid AS gt_code,
                tbm.deviceid AS tbm_machine,
                (tbm.timestamp + INTERVAL '5 hours 30 minutes') AS tbm_buildtime,
                cur.machinenumber AS curing_press,
                cur.mouldid,
                (cur.timestamp + INTERVAL '5 hours 30 minutes') AS curing_infeedtime,
                ub.ubno,
                ob.charge_id AS root_charge_id,
                ch.article_code,
                ROW_NUMBER() OVER (
                    PARTITION BY LTRIM(ob.barcode,'0') 
                    ORDER BY tbm.timestamp DESC
                ) as rn
            FROM sc_ames.opcua_barcode ob
            JOIN sc_tub_pcr.ubtable ub
                ON LTRIM(ub.barcode,'0') = LTRIM(ob.barcode,'0')
            JOIN sc_cimcorp_pcr.transportationunit tu
                ON LTRIM(tu.barcode,'0') = LTRIM(ob.barcode,'0')
            JOIN sc_cimcorp_pcr.tbm tbm
                ON tbm.transportationunitid = tu.transportationunitid
            LEFT JOIN sc_cimcorp_pcr.curing cur
                ON cur.transportationunitid = tu.transportationunitid
            LEFT JOIN sc_ames.charge ch
                ON ch.id = ob.charge_id
            WHERE LTRIM(ob.barcode,'0') IN %s
                AND ob.charge_id IS NOT NULL
                AND ob.machine_code LIKE '%%WP%%'
        )
        SELECT * FROM ranked_data WHERE rn = 1
    """
    
    batch_size = 500
    all_results = []
    missing_barcodes = set(barcodes)
    
    for i in range(0, len(barcodes), batch_size):
        batch = barcodes[i:i + batch_size]
        cur.execute(sql, (tuple(batch),))
        batch_results = cur.fetchall()
        all_results.extend(batch_results)
        
        found_in_batch = {row['barcode_norm'] for row in batch_results}
        missing_barcodes -= found_in_batch
    
    contexts = {}
    for row in all_results:
        barcode_norm = row['barcode_norm']
        row['tbm_machine'] = f"TBM{row['tbm_machine']}" if row['tbm_machine'] else None
        row['curing_press'] = f"M{row['curing_press']}" if row['curing_press'] else None
        contexts[barcode_norm] = dict(row)
    
    print(f"  Found {len(contexts)} barcodes with WP machine filter")
    return contexts, list(missing_barcodes)

def fetch_complete_genealogy_data(cur, charge_ids):
    """
    Fetch complete genealogy data for given charge_ids
    Includes all levels by recursively fetching for input_charge_ids
    """
    print("  Fetching complete genealogy data...")
    
    all_genealogy = []
    visited = set()
    to_process = set(charge_ids)
    level = 0
    
    while to_process and level < MAX_GENEALOGY_DEPTH:
        print(f"    Level {level}: Processing {len(to_process)} charge_ids")
        
        current_batch = list(to_process)
        to_process = set()
        
        # Fetch genealogy for current batch
        batch_genealogy = fetch_data_batch(cur, "sc_ames.genealogy", "output_charge_id", current_batch)
        
        for record in batch_genealogy:
            # Add to results
            all_genealogy.append(record)
            
            # Get input_charge_id for next level
            input_charge_id = record.get('input_charge_id')
            if input_charge_id and input_charge_id not in visited:
                to_process.add(input_charge_id)
        
        visited.update(current_batch)
        level += 1
    
    print(f"    Total genealogy records fetched: {len(all_genealogy)}")
    return all_genealogy

def fetch_charge_history_with_filter(cur, charge_ids):
    """Fetch charge_history data with filter for transport_order_complete and specific columns"""
    if not charge_ids:
        return defaultdict(list)
    
    print("  Fetching charge_history data...")
    batch_size = 1000
    charge_history_by_charge = defaultdict(list)
    
    for i in range(0, len(charge_ids), batch_size):
        batch_ids = charge_ids[i:i + batch_size]
        placeholders = ','.join(['%s'] * len(batch_ids))
        sql = f"""
            SELECT 
                charge_id,
                article_code,
                carrier_code,
                transaction_type,
                created_at
            FROM sc_ames.charge_history 
            WHERE charge_id IN ({placeholders})
            AND transaction_type = 'transport_order_complete'
        """
        
        cur.execute(sql, batch_ids)
        batch_results = cur.fetchall()
        
        for row in batch_results:
            charge_history_by_charge[row['charge_id']].append(dict(row))
    
    return charge_history_by_charge

def fetch_cycle_times_batch(cur, barcodes, tbm_times_map):
    """Fetch cycle time data for multiple barcodes with plant code filter"""
    if not barcodes:
        return {}
    
    print("  Fetching cycle time data...")
    cycle_start = time.time()
    
    results = {}
    
    # Process in batches
    batch_size = 50
    for i in range(0, len(barcodes), batch_size):
        batch_barcodes = barcodes[i:i + batch_size]
        
        # Create a temporary table with barcodes
        temp_table = f"temp_barcodes_cycle_{int(time.time())}_{i}"
        
        try:
            # Create temp table
            cur.execute(f"""
                CREATE TEMP TABLE {temp_table} (
                    barcode_norm VARCHAR(255)
                )
            """)
            
            # Insert barcodes
            execute_values(
                cur,
                f"INSERT INTO {temp_table} (barcode_norm) VALUES %s",
                [(b,) for b in batch_barcodes]
            )
            
            # Main cycle time query with plant code filter
            sql = f"""
                WITH params AS (
                    SELECT {TOL_SECONDS} AS tol_seconds
                ),
                s1 AS (
                    WITH base AS (
                        SELECT
                            local_datetime,
                            machine_name,
                            fs_ts_fw_ct,
                            fs_bead_blow_ct,
                            fs_bead_placing_ct,
                            fs_il_apply_ct,
                            fs_il_splice_check_ct,
                            fs_ply1_apply_ct,
                            fs_ply1_splice_check_ct,
                            fs_ply2_apply_ct,
                            fs_ply2_splice_check_ct,
                            fs_ply3_apply_ct,
                            fs_ply3_splice_check_ct,
                            fs_drum_rotation_ct,
                            fs_set_bead_ct,
                            fs_turn_up_ct,
                            fs_sw_apply_ct,
                            fs_sw_splice_check_ct,
                            fs_rrd_contour_disk_ct,
                            fs_idle_ct,
                            running_gtcode,
                            shift
                        FROM db_dev_iot_mfg.sc_tbm.t_pcr_st1_cycle_time
                        WHERE plant_code = %s
                    )
                    SELECT
                        local_datetime AS s1_local_datetime,
                        machine_name AS s1_machine_name,
                        fs_ts_fw_ct,
                        fs_bead_blow_ct,
                        fs_bead_placing_ct,
                        fs_il_apply_ct,
                        fs_il_splice_check_ct,
                        fs_ply1_apply_ct,
                        fs_ply1_splice_check_ct,
                        fs_ply2_apply_ct,
                        fs_ply2_splice_check_ct,
                        fs_ply3_apply_ct,
                        fs_ply3_splice_check_ct,
                        fs_drum_rotation_ct,
                        fs_set_bead_ct,
                        fs_turn_up_ct,
                        fs_sw_apply_ct,
                        fs_sw_splice_check_ct,
                        fs_rrd_contour_disk_ct,
                        fs_idle_ct,
                        running_gtcode AS s1_running_gtcode,
                        shift AS s1_shift,
                        (
                            COALESCE(fs_ts_fw_ct,0)
                            + COALESCE(fs_bead_blow_ct,0)
                            + COALESCE(fs_il_apply_ct,0)
                            + COALESCE(fs_ply1_apply_ct,0)
                            + COALESCE(fs_ply2_apply_ct,0)
                            + COALESCE(fs_set_bead_ct,0)
                            + COALESCE(fs_turn_up_ct,0)
                            + COALESCE(fs_sw_apply_ct,0)
                            + COALESCE(fs_rrd_contour_disk_ct,0)
                            + COALESCE(fs_drum_rotation_ct,0)
                        ) AS total_machine,
                        (
                            COALESCE(fs_bead_placing_ct,0)
                            + COALESCE(fs_il_splice_check_ct,0)
                            + COALESCE(fs_ply1_splice_check_ct,0)
                            + COALESCE(fs_ply2_splice_check_ct,0)
                            + COALESCE(fs_sw_splice_check_ct,0)
                        ) AS total_man,
                        (
                            COALESCE(fs_ts_fw_ct,0)
                            + COALESCE(fs_bead_blow_ct,0)
                            + COALESCE(fs_il_apply_ct,0)
                            + COALESCE(fs_ply1_apply_ct,0)
                            + COALESCE(fs_ply2_apply_ct,0)
                            + COALESCE(fs_set_bead_ct,0)
                            + COALESCE(fs_turn_up_ct,0)
                            + COALESCE(fs_sw_apply_ct,0)
                            + COALESCE(fs_rrd_contour_disk_ct,0)
                            + COALESCE(fs_drum_rotation_ct,0)
                            + COALESCE(fs_bead_placing_ct,0)
                            + COALESCE(fs_il_splice_check_ct,0)
                            + COALESCE(fs_ply1_splice_check_ct,0)
                            + COALESCE(fs_ply2_splice_check_ct,0)
                            + COALESCE(fs_sw_splice_check_ct,0)
                        ) AS s1_total_cycle,
                        (
                            COALESCE(fs_ts_fw_ct,0) + COALESCE(fs_bead_blow_ct,0) + COALESCE(fs_bead_placing_ct,0)
                            + COALESCE(fs_il_apply_ct,0) + COALESCE(fs_il_splice_check_ct,0)
                            + COALESCE(fs_ply1_apply_ct,0) + COALESCE(fs_ply1_splice_check_ct,0)
                            + COALESCE(fs_ply2_apply_ct,0) + COALESCE(fs_ply2_splice_check_ct,0)
                            + COALESCE(fs_ply3_apply_ct,0) + COALESCE(fs_ply3_splice_check_ct,0)
                            + COALESCE(fs_drum_rotation_ct,0) + COALESCE(fs_set_bead_ct,0)
                            + COALESCE(fs_turn_up_ct,0) + COALESCE(fs_sw_apply_ct,0)
                            + COALESCE(fs_sw_splice_check_ct,0) + COALESCE(fs_rrd_contour_disk_ct,0)
                            + COALESCE(fs_idle_ct,0)
                        ) AS s1_computed_total_cycle
                    FROM base
                ),
                s2 AS (
                    SELECT
                        machine_name AS s2_machine_name,
                        plc_time_ct AS s2_plc_time_ct,
                        plant_code AS s2_plant_code,
                        local_datetime AS s2_local_datetime,
                        running_gtcode AS s2_running_gtcode,
                        shift AS s2_shift,
                        barcode_ct AS s2_barcode_ct,
                        b1_tray_fwd_ct, b1_servicer_up_ct, b1_apply_ct, b1_servicer_down_ct, b1_splice_check_ct,
                        b2_tray_fwd_ct, b2_servicer_up_ct, b2_apply_ct, b2_servicer_down_ct, b2_splice_check_ct,
                        tt_rotating_ct, tread_app_fwd_ct, tread_apply_ct, tread_app_splice_check_ct,
                        tr_mov_from_mid_pos_ct, tr_seg_close_ct, tr_drum_collaps_ct, tr_mov_from_drum_pos_ct,
                        cs_fwd_ct, cs_apply_ct, cs_cutting_ct, cs_rev_ct,
                        cc_wating_for_ct, cc_pre_sh_barcode_tr_ct, cc_tr_mov_ct, cc_shapping_ct,
                        cc_bt_clamps_release_ct, cc_tr_rev_ct, cc_stitching_seq_ct, cc_stitcher_home_ct,
                        cc_tr_mov_mid_ct, cc_seg_close_ct, cc_shapping_collaps_ct, cc_rev_home_ct,
                        cc_clamps_release_ct, tr_cycle_complete_ct,
                        NULL::double precision AS total_sh,
                        NULL::double precision AS total_bt,
                        NULL::double precision AS s2_b_and_T_total_ct_orig,
                        NULL::double precision AS s2_cs_total_ct_orig,
                        NULL::double precision AS s2_shaper_side_total_ct_orig,
                        NULL::double precision AS s2_total_cycle_orig,
                        (
                            COALESCE(b1_tray_fwd_ct,0) + COALESCE(b1_servicer_up_ct,0) + COALESCE(b1_apply_ct,0) + COALESCE(b1_servicer_down_ct,0)
                            + COALESCE(b1_splice_check_ct,0) + COALESCE(b2_tray_fwd_ct,0) + COALESCE(b2_servicer_up_ct,0) + COALESCE(b2_apply_ct,0)
                            + COALESCE(b2_servicer_down_ct,0) + COALESCE(b2_splice_check_ct,0) + COALESCE(tt_rotating_ct,0)
                            + COALESCE(tread_app_fwd_ct,0) + COALESCE(tread_apply_ct,0) + COALESCE(tread_app_splice_check_ct,0)
                            + COALESCE(tr_mov_from_mid_pos_ct,0) + COALESCE(tr_seg_close_ct,0) + COALESCE(tr_drum_collaps_ct,0)
                            + COALESCE(tr_mov_from_drum_pos_ct,0)
                        ) AS s2_b_and_T_total_ct,
                        (
                            COALESCE(cs_fwd_ct,0) + COALESCE(cs_apply_ct,0) + COALESCE(cs_cutting_ct,0) + COALESCE(cs_rev_ct,0)
                        ) AS s2_cs_total_ct,
                        (
                            COALESCE(cc_pre_sh_barcode_tr_ct,0) + COALESCE(cc_tr_mov_ct,0) + COALESCE(cc_shapping_ct,0)
                            + COALESCE(cc_bt_clamps_release_ct,0) + COALESCE(cc_tr_rev_ct,0) + COALESCE(cc_stitching_seq_ct,0)
                            + COALESCE(cc_stitcher_home_ct,0) + COALESCE(cc_tr_mov_mid_ct,0) + COALESCE(cc_seg_close_ct,0)
                            + COALESCE(cc_shapping_collaps_ct,0) + COALESCE(cc_rev_home_ct,0) + COALESCE(cc_clamps_release_ct,0)
                            + COALESCE(tr_cycle_complete_ct,0)
                        ) AS s2_shaper_side_total_ct,
                        GREATEST(
                            COALESCE((
                                COALESCE(b1_tray_fwd_ct,0) + COALESCE(b1_servicer_up_ct,0) + COALESCE(b1_apply_ct,0) + COALESCE(b1_servicer_down_ct,0)
                                + COALESCE(b1_splice_check_ct,0) + COALESCE(b2_tray_fwd_ct,0) + COALESCE(b2_servicer_up_ct,0) + COALESCE(b2_apply_ct,0)
                                + COALESCE(b2_servicer_down_ct,0) + COALESCE(b2_splice_check_ct,0) + COALESCE(tt_rotating_ct,0)
                                + COALESCE(tread_app_fwd_ct,0) + COALESCE(tread_apply_ct,0) + COALESCE(tread_app_splice_check_ct,0)
                                + COALESCE(tr_mov_from_mid_pos_ct,0) + COALESCE(tr_seg_close_ct,0) + COALESCE(tr_drum_collaps_ct,0)
                                + COALESCE(tr_mov_from_drum_pos_ct,0)
                            ), 0),
                            COALESCE((
                                COALESCE(cs_fwd_ct,0) + COALESCE(cs_apply_ct,0) + COALESCE(cs_cutting_ct,0) + COALESCE(cs_rev_ct,0)
                            ), 0),
                            COALESCE((
                                COALESCE(cc_pre_sh_barcode_tr_ct,0) + COALESCE(cc_tr_mov_ct,0) + COALESCE(cc_shapping_ct,0)
                                + COALESCE(cc_bt_clamps_release_ct,0) + COALESCE(cc_tr_rev_ct,0) + COALESCE(cc_stitching_seq_ct,0)
                                + COALESCE(cc_stitcher_home_ct,0) + COALESCE(cc_tr_mov_mid_ct,0) + COALESCE(cc_seg_close_ct,0)
                                + COALESCE(cc_shapping_collaps_ct,0) + COALESCE(cc_rev_home_ct,0) + COALESCE(cc_clamps_release_ct,0)
                                + COALESCE(tr_cycle_complete_ct,0)
                            ), 0)
                        ) AS s2_computed_total_cycle
                    FROM db_dev_iot_mfg.sc_tbm.t_pcr_st2_cycle_time
                    WHERE plant_code = %s 
                      AND barcode_ct IS NOT NULL
                      AND LTRIM(barcode_ct, '0') IN (SELECT barcode_norm FROM {temp_table})
                ),
                ranked AS (
                    SELECT
                        s2.*,
                        s1.*,
                        ABS(EXTRACT(EPOCH FROM (s1.s1_local_datetime - s2.s2_local_datetime))) AS seconds_diff,
                        ROW_NUMBER() OVER (
                            PARTITION BY s2.s2_machine_name, s2.s2_running_gtcode, s2.s2_local_datetime
                            ORDER BY ABS(EXTRACT(EPOCH FROM (s1.s1_local_datetime - s2.s2_local_datetime)))
                        ) AS rn
                    FROM s2
                    LEFT JOIN s1
                        ON s1.s1_machine_name = s2.s2_machine_name
                       AND s1.s1_running_gtcode = s2.s2_running_gtcode
                       AND ABS(EXTRACT(EPOCH FROM (s1.s1_local_datetime - s2.s2_local_datetime))) <= (SELECT tol_seconds FROM params)
                ),
                matched AS (
                    SELECT * FROM ranked WHERE rn = 1
                )
                SELECT
                    -- stage2 cols
                    s2_machine_name, s2_plc_time_ct, s2_plant_code, s2_local_datetime,
                    s2_running_gtcode, 
                    s2_shift,
                    s2_barcode_ct,
                    b1_tray_fwd_ct, b1_servicer_up_ct, b1_apply_ct, b1_servicer_down_ct, b1_splice_check_ct,
                    b2_tray_fwd_ct, b2_servicer_up_ct, b2_apply_ct, b2_servicer_down_ct, b2_splice_check_ct,
                    tt_rotating_ct, tread_app_fwd_ct, tread_apply_ct, tread_app_splice_check_ct,
                    tr_mov_from_mid_pos_ct, tr_seg_close_ct, tr_drum_collaps_ct, tr_mov_from_drum_pos_ct,
                    cs_fwd_ct, cs_apply_ct, cs_cutting_ct, cs_rev_ct,
                    cc_wating_for_ct, cc_pre_sh_barcode_tr_ct, cc_tr_mov_ct, cc_shapping_ct,
                    cc_bt_clamps_release_ct, cc_tr_rev_ct, cc_stitching_seq_ct, cc_stitcher_home_ct,
                    cc_tr_mov_mid_ct, cc_seg_close_ct, cc_shapping_collaps_ct, cc_rev_home_ct,
                    cc_clamps_release_ct, tr_cycle_complete_ct,
                    total_sh, total_bt, s2_b_and_T_total_ct_orig, s2_cs_total_ct_orig, s2_shaper_side_total_ct_orig, s2_total_cycle_orig,
                    s2_b_and_T_total_ct, s2_cs_total_ct, s2_shaper_side_total_ct, s2_computed_total_cycle,
                    -- stage1 cols
                    s1_local_datetime, s1_machine_name,
                    fs_ts_fw_ct, fs_bead_blow_ct, fs_bead_placing_ct,
                    fs_il_apply_ct, fs_il_splice_check_ct,
                    fs_ply1_apply_ct, fs_ply1_splice_check_ct,
                    fs_ply2_apply_ct, fs_ply2_splice_check_ct,
                    fs_ply3_apply_ct, fs_ply3_splice_check_ct,
                    fs_drum_rotation_ct, fs_set_bead_ct, fs_turn_up_ct,
                    fs_sw_apply_ct, fs_sw_splice_check_ct,
                    fs_rrd_contour_disk_ct, fs_idle_ct,
                    s1_running_gtcode, s1_shift,
                    total_man, total_machine, s1_total_cycle,
                    s1_computed_total_cycle,
                    seconds_diff,
                    'matched' AS source_flag
                FROM matched
                WHERE LTRIM(s2_barcode_ct, '0') IN (SELECT barcode_norm FROM {temp_table})
            """
            
            cur.execute(sql, [PLANT_CODE_FILTER, PLANT_CODE_FILTER])
            batch_results = cur.fetchall()
            
            # Organize results by normalized barcode
            for row in batch_results:
                if row.get('s2_barcode_ct'):
                    barcode_norm = str(row['s2_barcode_ct']).lstrip('0')
                    if barcode_norm in batch_barcodes:
                        results.setdefault(barcode_norm, []).append(dict(row))
            
        except Exception as e:
            print(f"    Error fetching cycle times for batch {i}: {e}")
        finally:
            try:
                cur.execute(f"DROP TABLE IF EXISTS {temp_table}")
            except:
                pass
    
    print(f"    Cycle time fetch took: {time.time() - cycle_start:.2f} seconds")
    print(f"    Found cycle time data for {len(results)} barcodes")
    
    return results

# ============================================================================
# DATA ORGANIZATION AND SAVING (UPDATED)
# ============================================================================

def organize_all_data(contexts, genealogy_hierarchy, charge_by_id, charge_history_by_charge, 
                     article_by_code, machine_data, cycle_time_data, transportation_data,
                     genealogy_data, charge_data, charge_history_data, article_data, opcua_barcode_data,
                     tbm_lot_id_map=None):
    """Organize all data into sections including new requirements"""
    
    if tbm_lot_id_map is None:
        tbm_lot_id_map = {}

    section_data = {table: [] for table in ALL_TABLES.keys()}
    
    # Process each barcode that has context
    for lot_id, barcode_ctx in contexts.items():
        # Use machine_alerts.tbm_machine_model_lot_id if available, else fall back to lot_id
        machine_model_lot_id = tbm_lot_id_map.get(lot_id, lot_id)
        for barcode_norm, ctx in barcode_ctx.items():
            # Base data for all records - INCLUDING REQUIRED COLUMNS
            base_data = {
                "machine_model_lot_id": machine_model_lot_id,
                "barcode": barcode_norm,
                "tbm_machine": ctx.get("tbm_machine"),
                "tbm_buildtime": ctx.get("tbm_buildtime"),
                "curing_press": ctx.get("curing_press"),
                "curing_mouldid": ctx.get("mouldid"),
                "ubno": ctx.get("ubno"),
                "gt_code": ctx.get("gt_code"),
                # Additional context data
                "machine_code": ctx.get("machine_code"),
                "root_charge_id": ctx.get("root_charge_id"),
                "curing_infeedtime": ctx.get("curing_infeedtime"),
                "article_code": ctx.get("article_code")
            }
            
            # ========== ADD OPCUA_BARCODE DATA ==========
            opcua_record = base_data.copy()
            opcua_record["original_barcode"] = ctx.get("barcode")
            opcua_record["charge_id"] = ctx.get("root_charge_id")
            opcua_record["created_at"] = ctx.get("tbm_buildtime")
            section_data["opcua_barcode"].append(opcua_record)
            # =============================================
            
            charge_id = ctx.get("root_charge_id")
            
            # Add core data
            if charge_id:
                # INDIVIDUAL GENEALOGY DATA - Include all genealogy records
                # Filter genealogy records for this charge_id
                for genealogy_record in genealogy_data:
                    if genealogy_record.get('output_charge_id') == charge_id:
                        row_data = base_data.copy()
                        row_data.update(flatten_record(genealogy_record))
                        section_data["genealogy"].append(row_data)
                
                # Charge data (full record)
                if charge_id in charge_by_id:
                    row_data = base_data.copy()
                    row_data.update(flatten_record(charge_by_id[charge_id]))
                    section_data["charge"].append(row_data)
                
                # Charge history data (full record)
                if charge_id in charge_history_by_charge:
                    for history in charge_history_by_charge[charge_id]:
                        row_data = base_data.copy()
                        row_data.update(flatten_record(history))
                        section_data["charge_history"].append(row_data)
            
            # Article data (full record)
            article_code = ctx.get("article_code")
            if article_code and article_code in article_by_code:
                row_data = base_data.copy()
                row_data.update(flatten_record(article_by_code[article_code]))
                section_data["article"].append(row_data)
            
            # Cycle time data
            if barcode_norm in cycle_time_data:
                for cycle_record in cycle_time_data[barcode_norm]:
                    row_data = base_data.copy()
                    row_data.update(flatten_record(cycle_record))
                    section_data["cycle_time"].append(row_data)
            
            # Machine parameter data (already limited to max 3 records by fetch_machine_snapshots_improved)
            machine_tables = TABLE_CATEGORIES['machine_parameters']
            for table_name in machine_tables:
                if barcode_norm in machine_data.get(table_name, {}):
                    for record in machine_data[table_name][barcode_norm]:
                        row_data = base_data.copy()
                        row_data.update(flatten_record(record))
                        section_data[table_name].append(row_data)

    return section_data


# ============================================================================
# MAIN PROCESSING FUNCTION
# ============================================================================

def fetch_data_for_lot(lot_id, st1_machine_filter, st2_machine_filter, source, mongo_client=None):
    """
    Fetch all machine parameter data for a single lot and save to MongoDB.
    Works for both TBM and UFM lots via the source config dict.
    """
    start_time = time.time()
    label = source["label"]

    print("=" * 80)
    print(f"[{label}] Processing lot: {lot_id}")
    print(f"  ST1 machine filter: {st1_machine_filter}")
    print(f"  ST2 machine filter: {st2_machine_filter}")
    print("=" * 80)

    # Step 1: Get barcodes from MongoDB for this lot
    print(f"\n  [{label}] Fetching barcodes from MongoDB...")
    lotwise_barcodes, model_lot_id_map = get_lotwise_barcodes_from_mongo(lot_id, source, client=mongo_client)

    all_barcodes = []
    for _lot, barcodes in lotwise_barcodes.items():
        all_barcodes.extend(barcodes)

    if not all_barcodes:
        print(f"  [{label}] No barcodes found for lot {lot_id}. Skipping.")
        return

    if len(all_barcodes) > 100:
        print(f"  [{label}] Limiting from {len(all_barcodes)} to 100 barcodes")
        all_barcodes = all_barcodes[:100]

    print(f"  [{label}] Processing {len(all_barcodes)} barcodes...")

    # Step 2: Connect to Redshift and fetch data
    conn = None
    cur = None
    contexts = {}
    missing_barcodes = []
    genealogy_data = []
    charge_data = []
    charge_by_id = {}
    charge_history_by_charge = {}
    article_data = []
    article_by_code = {}
    cycle_time_data = {}
    machine_data = {}

    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Fetch contexts
        print(f"  [{label}] Fetching contexts...")
        contexts, missing_barcodes = fetch_contexts_batch(cur, all_barcodes)

        if not contexts:
            print(f"  [{label}] No contexts found for lot {lot_id}. Skipping.")
            return

        # Build batch query helpers
        all_charge_ids = []
        tbm_times_map = {}

        for barcode, ctx in contexts.items():
            if ctx.get('root_charge_id'):
                all_charge_ids.append(ctx['root_charge_id'])
            if ctx.get('tbm_buildtime'):
                tbm_times_map[barcode] = ctx['tbm_buildtime']

        all_charge_ids = list(set(all_charge_ids))

        # Fetch genealogy data
        genealogy_data = fetch_complete_genealogy_data(cur, all_charge_ids)

        # Fetch charge data (including genealogy inputs)
        charge_data = fetch_data_batch(cur, "sc_ames.charge", "id", all_charge_ids)
        all_genealogy_input_ids = list({
            r['input_charge_id'] for r in genealogy_data if r.get('input_charge_id')
        })
        if all_genealogy_input_ids:
            charge_data.extend(
                fetch_data_batch(cur, "sc_ames.charge", "id", all_genealogy_input_ids)
            )
        charge_by_id = {row['id']: row for row in charge_data}

        # Charge history
        charge_history_by_charge = fetch_charge_history_with_filter(cur, all_charge_ids)

        # Article data
        all_charge_article_codes = {
            c['article_code'] for c in charge_data if c.get('article_code')
        }
        article_data = []
        if all_charge_article_codes:
            article_data = fetch_data_batch(
                cur, "sc_ames.article", "article_code", list(all_charge_article_codes)
            )
        article_by_code = {row['article_code']: row for row in article_data}

        # Cycle time data
        barcodes_with_tbm = [b for b in contexts if b in tbm_times_map]
        cycle_time_data = fetch_cycle_times_batch(cur, barcodes_with_tbm, tbm_times_map)

        # Machine snapshots - dynamic filters derived from machine_alerts.machine
        # C1 (tbm machine base) = C1_1 for st1 tables, C1_2 for st2 tables
        machine_tables_config = {
            "st1_machine_parameters": {
                "table": "db_dev_iot_mfg.sc_tbm.t_pcr_st1_machine_parameters",
                "machine_filter": st1_machine_filter,
                "plant_filter": PLANT_CODE_FILTER,
            },
            "st2_machine_parameters": {
                "table": "db_dev_iot_mfg.sc_tbm.t_pcr_st2_machine_parameters",
                "machine_filter": st2_machine_filter,
                "plant_filter": PLANT_CODE_FILTER,
            },
            "st1_recipe_change": {
                "table": "db_dev_iot_mfg.sc_tbm.t_pcr_st1_recipe_change",
                "machine_filter": st1_machine_filter,
                "plant_filter": PLANT_CODE_FILTER,
            },
            "st2_recipe_change": {
                "table": "db_dev_iot_mfg.sc_tbm.t_pcr_st2_recipe_change",
                "machine_filter": st2_machine_filter,
                "plant_filter": PLANT_CODE_FILTER,
            },
        }

        valid_barcodes = {b: t for b, t in tbm_times_map.items() if t}
        if valid_barcodes and st1_machine_filter and st2_machine_filter:
            for table_name, cfg in machine_tables_config.items():
                print(f"    Fetching {table_name} (filter={cfg['machine_filter']})...")
                machine_data[table_name] = fetch_machine_snapshots_improved(
                    cur, cfg['table'], valid_barcodes,
                    cfg['machine_filter'], cfg['plant_filter']
                )

    except Exception as e:
        print(f"  [{label}] Error fetching Redshift data for lot {lot_id}: {e}")
        print(f"  [{label}] Skipping lot {lot_id} and continuing to next lot.")
        import traceback
        traceback.print_exc()
        return

    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    # Step 3: Organise data
    print(f"  [{label}] Organising data...")

    # Group contexts by lot
    contexts_by_lot = defaultdict(dict)
    for barcode, ctx in contexts.items():
        for _lot, barcodes_list in lotwise_barcodes.items():
            if barcode in barcodes_list:
                contexts_by_lot[_lot][barcode] = ctx
                break

    # Build opcua_barcode_data for hierarchy processing
    opcua_barcode_data = [
        {
            "barcode": barcode_norm,
            "charge_id": ctx.get("root_charge_id"),
            "machine_code": ctx.get("machine_code"),
            "created_at": ctx.get("tbm_buildtime"),
        }
        for _lot, barcode_ctx in contexts_by_lot.items()
        for barcode_norm, ctx in barcode_ctx.items()
    ]

    charge_history_data = [
        h for histories in charge_history_by_charge.values() for h in histories
    ]

    section_data = organize_all_data(
        contexts_by_lot, {}, charge_by_id,
        charge_history_by_charge, article_by_code, machine_data,
        cycle_time_data, {},
        genealogy_data, charge_data, charge_history_data, article_data, opcua_barcode_data,
        tbm_lot_id_map=model_lot_id_map,
    )

    # Step 4 + 5: Save all tables and mark lot processed — single tunnel
    tables_to_save = [
        "st1_machine_parameters",
        "st2_machine_parameters",
        "st1_recipe_change",
        "st2_recipe_change",
        "opcua_barcode",
        "genealogy",
        "charge",
        "charge_history",
        "article",
        "cycle_time",
    ]

    save_all_and_mark(lot_id, source, section_data, tables_to_save, shared_client=mongo_client)

    elapsed = time.time() - start_time
    minutes, secs = divmod(elapsed, 60)
    total_rows = sum(len(section_data.get(t, [])) for t in tables_to_save)
    print(f"  Lot {lot_id} done in {int(minutes)}m {secs:.1f}s, {total_rows} total records saved.")


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    print("TBM / UFM Machine Parameter Data Fetch")
    print("=" * 60)

    # Check database connectivity
    try:
        conn = get_connection()
        conn.close()
        print("Database connection verified")
    except Exception as e:
        print(f"Database connection failed: {e}")
        exit(1)

    for source in ALL_SOURCES:
        label = source["label"]
        print(f"\n[{label}] Opening MongoDB connection...")
        mongo_client = None
        try:
            mongo_client = get_mongo_client()

            print(f"[{label}] Fetching target lot IDs from MongoDB...")
            lot_ids = fetch_target_lot_ids_from_mongo(source, client=mongo_client)

            if not lot_ids:
                print(f"[{label}] No lot IDs found with machine_flag=False.")
                continue

            print(f"[{label}] Found {len(lot_ids)} lot(s): {lot_ids}")
            for lot_id in lot_ids:
                print(f"\n{'=' * 80}")
                print(f"[{label}] Processing lot: {lot_id}")
                st1_filter, st2_filter = get_machine_filter_for_lot(lot_id, source, client=mongo_client)
                if not st1_filter or not st2_filter:
                    print(f"  Skipping lot {lot_id}: could not determine machine filters.")
                    continue
                # Skip HF machines (HF1, HF2, etc.)
                machine_base = st1_filter.rsplit("_", 1)[0]
                if machine_base.upper().startswith("HF"):
                    print(f"  Skipping lot {lot_id}: machine {machine_base} is HF type (not supported).")
                    continue
                fetch_data_for_lot(lot_id, st1_filter, st2_filter, source, mongo_client=mongo_client)

        except Exception as e:
            print(f"[{label}] MongoDB connection error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            close_mongo_client(mongo_client)

    print("\nAll lots processed.")
