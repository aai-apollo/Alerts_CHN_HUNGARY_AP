############### working code of 93 server code2 #################
import psycopg2
from psycopg2.extras import RealDictCursor
from pymongo import MongoClient, UpdateOne, ReplaceOne
from pymongo.errors import BulkWriteError
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import traceback
import re
import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor

import config_hungary


# ============================================================
# CONFIGURATION
# ============================================================

REDSHIFT_HOST = "atl-dl-prd-01.cuvxusm4qify.ap-south-1.redshift.amazonaws.com"
REDSHIFT_DATABASE = "db_prd_plant_gyo"
REDSHIFT_PORT = 5439
REDSHIFT_USER = "usr_digital_twin_readonly"
REDSHIFT_PASSWORD = "UXY0%BSJa9dD#vP"

# ============================================================
# MONGODB SETTINGS
# ============================================================

# MongoDB connection string is read from config_hungary.py.
MONGO_URI = config_hungary.MONGO_URI

MONGO_DATABASE = "TUB_CIMCORP_VISUAL_merged_cleaned_data_historical_data"
MONGO_COLLECTION = "vi_data"

# -----------------------
# Direct PyMongo connection (no SSH tunnel)
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000000)


# ============================================================
# MONGODB CONNECTION HELPERS
# ============================================================

def setup_ssh_tunnel():
    """No SSH tunnel is used — MongoDB is connected directly via MONGO_URI.
    Kept for call-site compatibility; returns (None, None)."""
    return None, None


def get_mongo_client(_tunnel=None, _local_port=None):
    """Return a MongoClient connected directly to MONGO_URI."""
    c = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000000)
    try:
        c.admin.command('ping')
        print("MongoDB connection successful.")
        return c
    except Exception as e:
        print(f"MongoDB connection failed: {e}")
        raise


# ============================================================
# DATE RANGE  (auto-computed in IST, production day = 06:00 -> 06:00)
# ============================================================

# IST timezone (UTC+5:30). The script is meant to run daily at 10:50 AM IST
# via cron, so the window is derived from the current IST date — no manual
# editing of START_DATE / END_DATE needed.
IST = timezone(timedelta(hours=5, minutes=30))

# How many production days back to pull on each run. 1 = just "yesterday 06:00
# -> today 06:00". Kept at 3 to match the previous manual window (and to
# re-cover the last few days in case a daily run was missed).
LOOKBACK_DAYS = 3


def compute_date_range(lookback_days=LOOKBACK_DAYS):
    """Return (START_DATE, END_DATE) strings in IST.
    END_DATE = today 06:00 IST, START_DATE = (today - lookback_days) 06:00 IST.
    A production day runs 06:00 -> 06:00, so this covers whole production days.
    """
    now_ist = datetime.now(IST)
    end = now_ist.replace(hour=6, minute=0, second=0, microsecond=0)
    start = end - timedelta(days=lookback_days)
    fmt = "%Y-%m-%d %H:%M:%S"
    return start.strftime(fmt), end.strftime(fmt)


START_DATE, END_DATE = compute_date_range()


# ============================================================
# GT CODES
# ============================================================

GT_CODES = [
"GG206017ULAIWB", "GG225518ULAYB", "GG214517QPRHB", "GG206517ULAYB", "GG254520ULAWB", "GG244518QPRXY","GG235517QPRXY","GG236018QPRXW","GG244018QPRXY","GG185515WTRNH","GG195516WTRNH","GG196515WTRNT","GG205516WTRNH",
"GG206016WTRNH","GG215516WTRNH","GG216516WTRXHP","GG217016WTRNH","GG225516WTRNH","GG196515HTRNH","GG205516HTRNH","GG215516HTRNH","GG215517HTRNV","GG226517HTRNH","GG235518HTRNV","GG214518QPRXY","GG234518QPRXY","GG254519QPRXY",
"GG236018HTRNV","GG253519HYPXY","GG273020HYPXY","GG205516QT6NV","GG216516QT6NHP","GG185016QT6NH","GG216016QT6XV","GG196515QT6NV","GG186515QT6NH","GG235517HTRNV","GG176515HTRNH","GG185516HTRNH","GG234518HTRNV","GG217016QT6NH",
"GG236016QT6NH","GG194516QT6XV","GG195516QT6NH","GG196015QT6NV","GG205016QT6NV","GG206016QT6XV","GG214516QT6XV","GG176515QT6NH","GG196016QT6NH","GG215516QT6NH","GG185516QT6XV","GG216016WTRXH","GG195016QT6XV","GG186015QT6XH",
"GG195515QT6NH","GG226017QPRXV","GG226517QPRXV","GG166015QT6NH","GG166515QT6NT","GG175515QT6NT","GG206515QT6NH","GG195015QT6NV","GG206515WTRNT","GG195515WTRNH","GG195015WTRNH","GG196015WTRNT","GG186515WTRXT","GG186015WTRXT",
"GG176515WTRNT","GG254020QPRXY","GG215518QPEXV","GG224517QT6XV","GG235019QPEXW","GG235020QPEXV","GG254520QPEXV","GG254519QPEXW","GG225518QPEXV","GG204017QPPXW","GG204517QPPXY","GG205017QPPXY","GG214018QPPXY","GG214517QPPXY",
"GG214518QPPXY","GG215018QPPNW","GG215518QPPXV","GG216017QPPXV","GG216517QPPXV","GG225018QPPXW","GG225517QPPXW","GG226017QPPXV","GG226018QPPXW","GG226517QPPXV","GG234517QPPXY","GG234518QPPXY","GG234520QPPXW","GG235018QPPXV",
"GG235517QPPXY","GG235519QPPXW","GG236017QPPXV","GG236018QPPXW","GG236518QPPXV","GG244018QPPXY","GG244520QPPXW","GG254518QPPXY","GG254520QPPXW","GG255518QPPXW","GG256018QPPXV","GG274020QPPXY","GG235520QT6NV","GG214518HYPXY",
"GG254520WPPXV","GG204517WPPXV","GG205017WPPXV","GG224018WPPXW","GG225518WPPXV","GG226517WPPXH","GG235518WPPXH","GG235519WPPXV","GG205517WPPXV","GG215018WPPNV","GG215517WPPXV","GG215518WPPXV","GG216017WPPNH","GG216517WPPNV",
"GG224019WPPXY","GG224517WPPXV","GG224518WPPXW","GG225018WPPXV","GG225517WPPXV","GG225519WPPNV","GG226017WPPXH","GG226018WPPXV","GG233519WPPXY","GG234019WPPXW","GG234518WPPXV","GG234519WPPXV","GG234520WPPXV","GG235018WPPXV",
"GG235517WPPXV","GG236018WPPXV","GG236517WPPXV","GG236518WPPXH","GG243520WPPXY","GG244518WPPXV","GG244519WPPXW","GG244520WPPXV","GG253519WPPXY","GG254020WPPXY","GG254519WPPXV","GG255019WPPXV","GG255518WPPXV","GG256018WPPXV",
"GG224518QPRHB","GG244018UPRXY","GG235518WPRVA","GG186015ULAXH","GG205516ULANW","GG214516ULAXV","GG215517ULANW","GG204517ULAXY","GG214517ULAXY","GG206016ULANH","GG215518ULAXV","GG255019UPRXY","GG254520WPRHB","GG254520ULAWB","GG254021ULAWB",
"GG235518ULAVA","GG225517QPRXW","GG226018QPRXW","GG236517QPRXW","GG215517QT6XV","GG225518ULAYBEV","GG243519WPPXY","GG254019WPPXV","GG255519WPPXV","GG244018WPPXW","GG224519WPPXV","GG215517QP2XW","GG224018QP2XY","GG205517QP2XV","GG215017QP2XW",
"GG215518QP2XV","GG186515SP5VA","GG195017WPRHB","GG195018WPRHB","GG206017ULAIWB","GG206017ULAWB","GG206018WPRHB","GG206515QT6HV","GG206516ULAWB","GG206517ULAYB","GG206517WPRHB","GG214018ULAYB","GG214517QPRHB","GG214517ULAWA","GG216517WPRHA",
"GG225018ULAWB","GG225518ULAYB","GG236020ULAHB","GG254520QPRHB","GG254520ULAYB","GG244519QPRHB","GG244020QPRHB"
]


# ============================================================
# COMPONENT / CART MAPPING
# ============================================================

COMPONENTS = [
    ("breaker_1", "vst_vhmc_mes_1blo_cart_identifier"),
    ("breaker_2", "vst_vhmc_mes_2blo_cart_identifier"),
    ("tread", "vst_vhmc_mes_tdlo_cart_identifier"),
    ("innerliner", "vst_vhmc_mes_inlo_cart_identifier"),
    ("sidewall", "vst_vhmc_mes_swlo_cart_identifier"),
    ("body_ply_1", "vst_vhmc_mes_1plo_cart_identifier"),
    ("body_ply_2", "vst_vhmc_mes_2plo_cart_identifier"),
    ("bead", "vst_vhmc_mes_bdld_cart_identifier"),
    ("steel_chafer_left", "vst_vhmc_mes_sclo_left_cart_identifier"),
    ("steel_chafer_right", "vst_vhmc_mes_sclo_right_cart_identifier"),
    ("fabric_chafer_left", "vst_vhmc_mes_fclo_left_cart_identifier"),
    ("fabric_chafer_right", "vst_vhmc_mes_fclo_right_cart_identifier"),
    ("rubber_strip_left", "vst_vhmc_mes_rslo_left_cart_identifier"),
    ("rubber_strip_right", "vst_vhmc_mes_rslo_right_cart_identifier"),
]


PRODUCTION_TABLE = "sc_mhshistory.t_tbm_pcr_productiondata"


# ============================================================
# HELPERS
# ============================================================

def clean_value(value):
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if value == "":
            return None
        return value
    return value


def format_tbm_name(value):
    """Convert raw mv_tire tbm_name (e.g. 'TBTB01', 'TBTB05') into the display
    form stored in MongoDB ('TBM 1', 'TBM 5'). Extracts the trailing number,
    drops leading zeros, and returns 'TBM <n>'. If no number is found, returns
    the cleaned original value."""
    cleaned = clean_value(value)
    if not cleaned:
        return None
    match = re.search(r"(\d+)", str(cleaned))
    if match:
        return f"TBM {int(match.group(1))}"
    return cleaned


def safe_int(value):
    if value is None:
        return 0
    try:
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return 0


def safe_float(value):
    if value is None:
        return 0.0
    try:
        return float(value)
    except Exception:
        return 0.0


def format_timestamp(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def unique_preserve_order(values):
    result = []
    for value in values:
        if value is not None and value not in result:
            result.append(value)
    return result


# ============================================================
# MONGODB BSON VALUE CLEANER
# ============================================================

def convert_bson_value(value):
    """
    Convert Redshift/Python values into MongoDB BSON-safe values.
    Handles Decimal recursively inside dicts and lists.
    """
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    if isinstance(value, datetime):
        return value
    if isinstance(value, dict):
        return {
            key: convert_bson_value(val)
            for key, val in value.items()
        }
    if isinstance(value, list):
        return [
            convert_bson_value(item)
            for item in value
        ]
    if isinstance(value, tuple):
        return [
            convert_bson_value(item)
            for item in value
        ]
    return value


def convert_documents_for_mongodb(documents):
    return [
        convert_bson_value(document)
        for document in documents
    ]


# ============================================================
# GET PRODUCTION TABLE COLUMNS
# ============================================================

def get_production_columns(connection):
    print("Checking production table columns...")
    query = """
        SELECT
            column_name
        FROM information_schema.columns
        WHERE table_schema = 'sc_mhshistory'
          AND table_name = 't_tbm_pcr_productiondata'
        ORDER BY ordinal_position
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
    columns = [row[0] for row in rows]
    print(f"Production table columns found: {len(columns)}")
    return columns


# ============================================================
# GET MV_TIRE COLUMNS AND CHECK VI_TIME
# ============================================================

def get_mv_tire_columns(connection):
    """
    Get the actual column names from mv_tire view and find vi_time column
    """
    print("Checking mv_tire view columns...")
    
    query = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'sc_wms_apollo_pcr'
          AND table_name = 'mv_tire'
        ORDER BY ordinal_position
    """
    
    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [row[0] for row in rows]
    
    print(f"Columns in mv_tire: {columns}")
    
    # Try to find vi_time or similar columns
    vi_time_column = None
    for col in columns:
        if col.lower() == 'vi_time':
            vi_time_column = col
            break
        elif 'vi' in col.lower() and 'time' in col.lower():
            vi_time_column = col
            print(f"Found potential vi_time column: {col}")
            break
    
    if not vi_time_column:
        print("WARNING: No vi_time column found!")
        time_columns = [col for col in columns if 'time' in col.lower()]
        print(f"Available time columns: {time_columns}")
        
        # Try to find alternative columns
        possible_names = ['tire_vi_time', 'visual_inspection_time', 'inspection_time', 'vi_timestamp']
        for name in possible_names:
            if any(col.lower() == name.lower() for col in columns):
                vi_time_column = name
                print(f"Found alternative vi_time column: '{vi_time_column}'")
                break
        
        # If still not found, use the first time-related column as fallback
        if not vi_time_column and time_columns:
            vi_time_column = time_columns[0]
            print(f"Using fallback time column: '{vi_time_column}'")
        else:
            vi_time_column = 'vi_time'  # Use default and hope it works
            print("Using default column name 'vi_time'")
    else:
        print(f"✓ Using '{vi_time_column}' for vi_time")
    
    return vi_time_column, columns


def debug_vi_time_issue(connection, vi_time_column):
    """
    Debug why vi_time is NULL
    """
    print("=" * 50)
    print("DEBUG: Investigating vi_time issue")
    print("=" * 50)
    
    gt_filter = "', '".join(GT_CODES[:5])  # Use first 5 GT codes for debug
    
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        # Check if we can directly query vi_time
        query = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT({vi_time_column}) as has_vi_time,
                COUNT(CASE WHEN {vi_time_column} IS NOT NULL THEN 1 END) as non_null_vi_time,
                MIN({vi_time_column}) as earliest_vi_time,
                MAX({vi_time_column}) as latest_vi_time,
                COUNT(CASE WHEN {vi_time_column} IS NULL THEN 1 END) as null_count
            FROM sc_wms_apollo_pcr.mv_tire
            WHERE {vi_time_column} IS NOT NULL
              AND {vi_time_column} >= '{START_DATE}'
              AND {vi_time_column} <  '{END_DATE}'
              AND gt_code IN ('{gt_filter}')
        """
        cursor.execute(query)
        result = cursor.fetchone()
        print("vi_time statistics:")
        for key, value in result.items():
            print(f"  {key}: {value}")
        
        # Check if there are any rows at all for the date range
        query2 = f"""
            SELECT COUNT(*) as row_count
            FROM sc_wms_apollo_pcr.mv_tire
            WHERE {vi_time_column} IS NOT NULL
              AND {vi_time_column} >= '{START_DATE}'
              AND {vi_time_column} <  '{END_DATE}'
              AND gt_code IN ('{gt_filter}')
        """
        cursor.execute(query2)
        total_rows = cursor.fetchone()
        print(f"Total rows in date range: {total_rows['row_count']}")
        
        # Get a sample of data
        query3 = f"""
            SELECT 
                stt_id,
                tire_barcode,
                gt_code,
                last_change_time,
                {vi_time_column} as vi_time,
                ct_code,
                tbm_building_time,
                tbm_name,
                cp_curing_time
            FROM sc_wms_apollo_pcr.mv_tire
            WHERE {vi_time_column} IS NOT NULL
              AND {vi_time_column} >= '{START_DATE}'
              AND {vi_time_column} <  '{END_DATE}'
              AND gt_code IN ('{gt_filter}')
            LIMIT 5
        """
        cursor.execute(query3)
        samples = cursor.fetchall()
        print("\nSample rows:")
        for i, row in enumerate(samples):
            print(f"Row {i+1}:")
            for key, value in row.items():
                print(f"  {key}: {value}")
        
        # Check data type of vi_time
        if samples and 'vi_time' in samples[0]:
            sample_value = samples[0]['vi_time']
            print(f"\nvi_time data type: {type(sample_value)}")
            print(f"Sample vi_time value: {sample_value}")
    
    print("=" * 50)


# ============================================================
# NORMALIZE COLUMN NAME
# ============================================================

def normalize_column_name(name):
    if not name:
        return ""
    return (
        name
        .lower()
        .replace("_", "")
        .replace("-", "")
        .replace(" ", "")
    )


# ============================================================
# FIND ACTUAL CART COLUMN
# ============================================================

def find_actual_cart_column(
    requested_column,
    available_columns
):
    requested_normalized = normalize_column_name(requested_column)

    # 1. Exact match (case-insensitive)
    for column in available_columns:
        if column.lower() == requested_column.lower():
            return column

    # 2. Normalized match
    for column in available_columns:
        if normalize_column_name(column) == requested_normalized:
            return column

    # 3. Partial match
    important_parts = [
        part
        for part in requested_column.lower().split("_")
        if part not in ["vst", "vhmc", "mes"]
    ]

    candidates = []
    for column in available_columns:
        normalized = column.lower()
        matched = True
        for part in important_parts:
            if part not in normalized:
                matched = False
                break
        if matched:
            candidates.append(column)

    if len(candidates) == 1:
        return candidates[0]

    return None


# ============================================================
# BUILD CART COLUMN MAP
# ============================================================

def build_cart_column_map(available_columns):
    cart_column_map = {}
    print("------------------------------------------")
    print("Production CART column mapping")
    print("------------------------------------------")
    for component_name, requested_column in COMPONENTS:
        actual_column = find_actual_cart_column(
            requested_column,
            available_columns
        )
        cart_column_map[component_name] = actual_column
        if actual_column:
            print(f"{component_name:25} -> {actual_column}")
        else:
            print(f"{component_name:25} -> NOT FOUND")
    print("------------------------------------------")
    return cart_column_map


# ============================================================
# BUILD GT CODE SQL
# ============================================================

gt_code_sql = ", ".join(
    "'" + gt.replace("'", "''") + "'"
    for gt in GT_CODES
)


# ============================================================
# MAIN REDSHIFT QUERY BUILDER
# ============================================================

def build_query(cart_column_map, vi_time_column='vi_time'):
    # Only the CART identifier columns are needed from the production table.
    # Selecting p.* (all 276 columns) into `base` and then scanning `base`
    # 14 times in the CART UNION was a major slowdown, so pull ONLY the cart
    # columns actually referenced.
    _prod_cols = sorted({
        col for col in cart_column_map.values() if col
    })
    base_prod_select = ",\n        ".join(
        f'p."{col}" AS "{col}"' for col in _prod_cols
    ) if _prod_cols else "NULL AS _no_cart_cols"

    # CART UNION
    cart_union_parts = []
    for component_name, requested_column in COMPONENTS:
        actual_column = cart_column_map.get(component_name)
        if not actual_column:
            print(f"Skipping component because CART column was not found: {component_name}")
            continue

        part = f"""
        SELECT
            '{component_name}' AS component,
            p."{actual_column}" AS cart_identifier,
            p.barcode,
            p.sku,
            p.gt_code,
            p.last_change_time,
            p.rejection,
            p.scrap,
            p.upgraded,
            p.sur_repair_comment,
            p.sur_repair_time,
            p.sur_repair_code,
            p.curing_id,
            p.curing_utctimestamp,
            p.curing_localizedtimestamp,
            p.curing_sku_code,
            p.curing_barcode,
            p.curing_gt_code,
            p.bladder_code,
            p.mould_number,
            p.order_number,
            p.machineno,
            p.machine_number,
            p.current_recipe,
            p.tyre_size,
            p.curing_count,
            p.bladder_count,
            p.mould_count,
            p.bladder_target,
            p.mould_target,
            p.barcode_timestamp_rank,
            p.tbm_buildtime,
            p.tbm_name,
            p.production_day,
            p.curing_infeededtime,
            p.vi_time,
            p.createdon,
            p.viresult
        FROM base p
        WHERE p."{actual_column}" IS NOT NULL
          AND TRIM(CAST(p."{actual_column}" AS VARCHAR)) <> ''
        """
        cart_union_parts.append(part)

    if not cart_union_parts:
        raise RuntimeError(
            "None of the configured CART columns were found "
            "in sc_mhshistory.t_tbm_pcr_productiondata."
        )

    cart_union_sql = "\nUNION ALL\n".join(cart_union_parts)

    # COMPLETE QUERY
    query = f"""
WITH vi_result AS
(
    -- VI result source: sc_apollo_mom.t_vi_result. The inspection time and the
    -- reason text come from here (not mv_tire). Only passed inspections
    -- (viresult = 1) are considered, and the date window is on createdon.
    -- Take the LATEST vi_result per barcode (barcodeid), newest createdon.
    -- createdon is also exposed as a STRING (createdon_str).
    SELECT
        barcodeid,
        createdon,
        createdon_str,
        vireasonnok,
        viresult
    FROM
    (
        SELECT
            TRIM(barcodeid) AS barcodeid,
            createdon,
            TO_CHAR(createdon, 'YYYY-MM-DD HH24:MI:SS') AS createdon_str,
            vireasonnok,
            viresult,
            ROW_NUMBER() OVER
            (
                PARTITION BY TRIM(barcodeid)
                ORDER BY createdon DESC
            ) AS rn
        FROM sc_apollo_mom.t_vi_result
        WHERE viresult in (0,1)
          AND createdon >= '{START_DATE}'
          AND createdon <  '{END_DATE}'
    ) ranked
    WHERE rn = 1
),

vi AS
(
    -- Enrich the VI results with tyre attributes from mv_tire, joined on
    -- barcode = barcodeid. vi_time now comes from mv_tire ({vi_time_column});
    -- createdon still comes from t_vi_result. vi_reason = vireasonnok.
    -- createdon (raw) is carried for duplicate-barcode ordering.
    SELECT
        m.stt_id,
        TRIM(m.tire_barcode) AS barcode,
        m.gt_code,
        m.last_change_time,
        m.ct_code AS sku,
        m.tbm_building_time AS tbm_buildtime,
        m.tbm_name AS tbm_name,
        m.cp_curing_time AS curing_infeededtime,
        vr.createdon AS createdon,
        vr.createdon_str AS createdon_str,
        TO_CHAR(m.{vi_time_column}, 'YYYY-MM-DD HH24:MI:SS') AS vi_time,
        vr.vireasonnok  AS vi_reason,
        vr.viresult AS viresult,
        -- sur_repair_time (latest per stt_id) is pulled in here only so it can
        -- act as the final tie-breaker when de-duplicating barcodes.
        sur.sur_repair_time AS sur_repair_time_order,
        -- One production DAY runs 06:00 -> 06:00. Shifting the inspection time
        -- back by 6 hours makes a tyre inspected before 06:00 fall on the
        -- previous production day.
        TO_CHAR(vr.createdon - INTERVAL '6 hours', 'YYYY-MM-DD') AS production_day
    FROM sc_wms_apollo_pcr.mv_tire m
    INNER JOIN vi_result vr
        ON TRIM(m.tire_barcode) = vr.barcodeid
    LEFT JOIN
    (
        SELECT
            sur_stt_id,
            MAX(sur_repair_time) AS sur_repair_time
        FROM sc_wms_apollo_pcr.stock_unit_repair
        GROUP BY sur_stt_id
    ) sur
        ON sur.sur_stt_id = m.stt_id
    WHERE m.gt_code IN ({gt_code_sql})
),

vi_ranked AS
(
    SELECT
        stt_id,
        barcode,
        gt_code,
        last_change_time,
        sku,
        tbm_buildtime,
        tbm_name,
        curing_infeededtime,
        vi_time,
        createdon_str AS createdon,   -- string createdon surfaced to output
        vi_reason,
        viresult,
        production_day,
        -- Duplicate-barcode ordering: pick the winning record for each
        -- barcode by createdon FIRST; if more than one row shares the same
        -- createdon, break the tie by last_change_time, then by
        -- sur_repair_time (and finally stt_id for a stable result).
        ROW_NUMBER() OVER
        (
            PARTITION BY barcode
            ORDER BY
                createdon,
                last_change_time,
                sur_repair_time_order,
                stt_id
        ) AS barcode_timestamp_rank
    FROM vi
),

repair AS
(
    -- Reason/repair classification now comes from t_vi_result.vireasonnok
    -- (carried through vi_ranked). Applies to ANY reason code:
    --   upgraded : reason shows a downgrade-to-OE like '<code> --> 0'
    --   scrap    : reason is a plain reason code (digits only, no arrow)
    --   rejection = scrap + upgraded (any tyre that has a reason)
    SELECT
        barcode,
        MAX(
            CASE
                WHEN vi_reason ~ '[0-9]+[^0-9]*-+>[^0-9]*0([^0-9]|$)'
                THEN 1 ELSE 0
            END
        ) AS upgraded_count,
        MAX(
            CASE
                WHEN TRIM(vi_reason) ~ '^[0-9]+$'
                THEN 1 ELSE 0
            END
        ) AS scrap_count,
        MAX(
            CASE
                WHEN vi_reason IS NOT NULL
                 AND TRIM(vi_reason) <> ''
                THEN 1 ELSE 0
            END
        ) AS rejection_count
    FROM vi_ranked
    GROUP BY barcode
),

repair_su AS
(
    -- sur_repair_comment / sur_repair_time / sur_repair_code come from the
    -- stock_unit_repair table (keyed by sur_stt_id = mv_tire stt_id).
    -- One aggregated row per stt_id.
    SELECT
        sur_stt_id,
        MAX(sur_repair_comment) AS sur_repair_comment,
        MAX(sur_repair_time)    AS sur_repair_time,
        MAX(sur_repair_code)    AS sur_repair_code
    FROM sc_wms_apollo_pcr.stock_unit_repair
    WHERE sur_stt_id IN
    (
        SELECT stt_id FROM vi_ranked
    )
    GROUP BY sur_stt_id
),

production_ranked AS
(
    SELECT
        p.*,
        ROW_NUMBER() OVER
        (
            PARTITION BY TRIM(p.vst_vhmc_mes_gt_barcode)
            ORDER BY p.producedon DESC
        ) AS rn
    FROM {PRODUCTION_TABLE} p
    WHERE TRIM(p.vst_vhmc_mes_gt_barcode) IN
    (
        SELECT barcode
        FROM vi_ranked
    )
),

production AS
(
    SELECT *
    FROM production_ranked
    WHERE rn = 1
),

curing_ranked AS
(
    SELECT
        c.id,
        c.utctimestamp,
        c.localizedtimestamp,
        REGEXP_REPLACE(TRIM(c.sku_code), '^[.]+|[.]+$', '') AS sku_code,
        REGEXP_REPLACE(TRIM(c.barcode), '^[.]+|[.]+$', '') AS curing_barcode,
        REGEXP_REPLACE(TRIM(c.gt_code), '^[.]+|[.]+$', '') AS curing_gt_code,
        REGEXP_REPLACE(TRIM(c.bladder_code), '^[.]+|[.]+$', '') AS bladder_code,
        REGEXP_REPLACE(TRIM(c.mould_number), '^[.]+|[.]+$', '') AS mould_number,
        c.order_number,
        c.machineno,
        c.machine_number,
        c.current_recipe,
        c.tyre_size,
        c.curing_count,
        c.bladder_count,
        c.mould_count,
        c.bladder_target,
        c.mould_target,
        ROW_NUMBER() OVER
        (
            PARTITION BY REGEXP_REPLACE(TRIM(c.barcode), '^[.]+|[.]+$', '')
            ORDER BY
                c.localizedtimestamp DESC,
                c.utctimestamp DESC,
                c.id DESC
        ) AS rn
    FROM db_dev_iot_mfg.sc_pcr_curing.t_curing_batch c
    WHERE c.machine_number LIKE '%5500%'
      -- Restrict to only the barcodes in scope so we don't rank the ENTIRE
      -- curing table (this was a major slowdown).
      AND REGEXP_REPLACE(TRIM(c.barcode), '^[.]+|[.]+$', '') IN
      (
          SELECT barcode FROM vi_ranked
      )
),

curing AS
(
    SELECT *
    FROM curing_ranked
    WHERE rn = 1
),

base AS
(
    SELECT
        v.stt_id,
        v.barcode,
        v.sku,
        v.gt_code,
        v.last_change_time,
        v.barcode_timestamp_rank,
        v.tbm_buildtime,
        v.tbm_name,
        v.curing_infeededtime,
        v.vi_time,
        v.createdon,
        v.viresult,
        v.production_day,
        COALESCE(r.rejection_count, 0) AS rejection,
        COALESCE(r.scrap_count, 0) AS scrap,
        COALESCE(r.upgraded_count, 0) AS upgraded,
        su.sur_repair_comment,
        su.sur_repair_time,
        su.sur_repair_code,
        {base_prod_select},
        c.id AS curing_id,
        c.utctimestamp AS curing_utctimestamp,
        c.localizedtimestamp AS curing_localizedtimestamp,
        c.sku_code AS curing_sku_code,
        c.curing_barcode,
        c.curing_gt_code,
        c.bladder_code,
        c.mould_number,
        c.order_number,
        c.machineno,
        c.machine_number,
        c.current_recipe,
        c.tyre_size,
        c.curing_count,
        c.bladder_count,
        c.mould_count,
        c.bladder_target,
        c.mould_target
    FROM vi_ranked v
    LEFT JOIN production p
        ON TRIM(v.barcode) = TRIM(p.vst_vhmc_mes_gt_barcode)
    LEFT JOIN repair r
        ON v.barcode = r.barcode
    LEFT JOIN repair_su su
        ON v.stt_id = su.sur_stt_id
    LEFT JOIN curing c
        ON TRIM(v.barcode) = TRIM(c.curing_barcode)
),

cart_data AS
(
    {cart_union_sql}
)

SELECT *
FROM cart_data
ORDER BY
    gt_code,
    barcode,
    last_change_time,
    component,
    cart_identifier
"""

    return query


# ============================================================
# REDSHIFT CONNECTION
# ============================================================

def get_redshift_connection():
    print("Connecting to Redshift...")
    connection = psycopg2.connect(
        host=REDSHIFT_HOST,
        database=REDSHIFT_DATABASE,
        port=REDSHIFT_PORT,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )
    print("Redshift connection successful.")
    return connection


# ============================================================
# FETCH REDSHIFT DATA
# ============================================================

def fetch_redshift_data():
    connection = None
    try:
        connection = get_redshift_connection()
        
        # Get mv_tire columns and find vi_time column
        vi_time_column, mv_tire_columns = get_mv_tire_columns(connection)
        
        # Debug the vi_time issue
        debug_vi_time_issue(connection, vi_time_column)
        
        # Get actual production columns
        available_columns = get_production_columns(connection)
        
        if not available_columns:
            raise RuntimeError("No columns found for production table.")
        
        # Build dynamic CART mapping
        cart_column_map = build_cart_column_map(available_columns)
        
        # Check mandatory barcode column
        barcode_column = None
        for column in available_columns:
            if column.lower() == "vst_vhmc_mes_gt_barcode".lower():
                barcode_column = column
                break
        
        if not barcode_column:
            raise RuntimeError(
                "Mandatory production barcode column "
                "'vst_vhmc_mes_gt_barcode' was not found."
            )
        
        # Print the actual columns of t_vi_result so the correct reason column
        # name is known (the screenshot header may differ from the DB column).
        try:
            with connection.cursor() as _c:
                _c.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'sc_apollo_mom'
                      AND table_name = 't_vi_result'
                    ORDER BY ordinal_position
                """)
                _cols = [r[0] for r in _c.fetchall()]
            print(f"t_vi_result columns: {_cols}")
        except Exception as _e:
            print(f"[WARN] Could not read t_vi_result columns: {_e}")

        # Build query with correct vi_time column
        query = build_query(cart_column_map, vi_time_column)
        
        # Execute
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            print("Executing Redshift query...")
            cursor.execute(query)
            rows = cursor.fetchall()
            print(f"Redshift rows fetched: {len(rows)}")
            
            # Check if vi_time has data in the results
            if rows:
                sample = rows[0]
                print(f"Sample row keys: {list(sample.keys())}")
                print(f"Sample row vi_time: {sample.get('vi_time')}")
                
                # Count rows with vi_time not null
                vi_time_count = sum(1 for row in rows if row.get('vi_time') is not None)
                print(f"Rows with vi_time not null: {vi_time_count} out of {len(rows)}")
            
            return rows
            
    except Exception as e:
        print("Redshift query failed:")
        print(str(e))
        traceback.print_exc()
        raise
    finally:
        if connection:
            connection.close()
            print("Redshift connection closed.")


# ============================================================
# BUILD GT SUMMARY
# ============================================================

def build_gt_summary(rows):
    gt_data = {}
    barcode_records = {}

    for row in rows:
        gt_code = clean_value(row.get("gt_code"))
        barcode = clean_value(row.get("barcode"))
        if not gt_code or not barcode:
            continue

        key = (gt_code, barcode)
        if key not in barcode_records:
            barcode_records[key] = {
                "rejection": safe_int(row.get("rejection")),
                "scrap": safe_int(row.get("scrap")),
                "upgraded": safe_int(row.get("upgraded")),
            }
        else:
            barcode_records[key]["rejection"] = max(
                barcode_records[key]["rejection"],
                safe_int(row.get("rejection"))
            )
            barcode_records[key]["scrap"] = max(
                barcode_records[key]["scrap"],
                safe_int(row.get("scrap"))
            )
            barcode_records[key]["upgraded"] = max(
                barcode_records[key]["upgraded"],
                safe_int(row.get("upgraded"))
            )

    for (gt_code, barcode), data in barcode_records.items():
        if gt_code not in gt_data:
            gt_data[gt_code] = {
                "barcodes": set(),
                "rejection": 0,
                "scrap": 0,
                "upgraded": 0
            }
        gt_data[gt_code]["barcodes"].add(barcode)
        # rejection = scrap + upgraded (comment-based). Count per unique barcode.
        if data["rejection"] > 0:
            gt_data[gt_code]["rejection"] += 1
        if data["scrap"] > 0:
            gt_data[gt_code]["scrap"] += 1
        if data["upgraded"] > 0:
            gt_data[gt_code]["upgraded"] += 1

    final_summary = {}
    for gt_code, data in gt_data.items():
        production = len(data["barcodes"])
        rejection = data["rejection"]
        scrap = data["scrap"]
        rejection_pct = round((rejection * 100.0 / production) if production else 0, 2)
        scrap_pct = round((scrap * 100.0 / production) if production else 0, 2)
        final_summary[gt_code] = {
            "gt_production": production,
            "gt_rejection": rejection,
            "gt_scrap": scrap,
            "gt_rejection_pct": rejection_pct,
            "gt_final_scrap_pct": scrap_pct,
            "gt_upgraded": data["upgraded"]
        }

    return final_summary


# ============================================================
# BUILD CART SUMMARY
# ============================================================

def build_cart_summary(rows):
    cart_groups = {}

    for row in rows:
        component = clean_value(row.get("component"))
        cart_identifier = clean_value(row.get("cart_identifier"))
        gt_code = clean_value(row.get("gt_code"))
        barcode = clean_value(row.get("barcode"))

        if not component or not cart_identifier or not gt_code or not barcode:
            continue

        key = (component, cart_identifier, gt_code)
        if key not in cart_groups:
            cart_groups[key] = {
                "barcodes": set(),
                "rejection_barcodes": set(),
                "scrap_barcodes": set(),
                "upgraded_barcodes": set(),
            }

        group = cart_groups[key]
        group["barcodes"].add(barcode)

        if safe_int(row.get("rejection")) > 0:
            group["rejection_barcodes"].add(barcode)
        if safe_int(row.get("scrap")) > 0:
            group["scrap_barcodes"].add(barcode)
        if safe_int(row.get("upgraded")) > 0:
            group["upgraded_barcodes"].add(barcode)

    return cart_groups


# ============================================================
# BUILD CART BREAKDOWN DOCUMENTS  (one doc per cart)
# ============================================================

# ============================================================
# BUILD MONGODB DOCUMENTS
# ============================================================

def build_mongodb_documents(rows, gt_summary, cart_summary, vi_reason_by_gt=None):
    grouped = {}

    for row in rows:
        barcode = clean_value(row.get("barcode"))
        sku = clean_value(row.get("sku"))
        gt_code = clean_value(row.get("gt_code"))
        last_change_time = format_timestamp(row.get("last_change_time"))

        if not barcode:
            continue

        # Group by (barcode, sku, gt_code, vi_time). vi_time is part of the key
        # so the duplicate-barcode check is based on the inspection time — the
        # same barcode inspected at different vi_time values stays distinct.
        # last_change_time is NOT part of the key (it previously caused spurious
        # duplicate documents).
        vi_time_key = format_timestamp(row.get("vi_time"))
        group_key = (barcode, sku, gt_code, vi_time_key)

        if group_key not in grouped:
            grouped[group_key] = {
                "barcode": barcode,
                "sku": sku,
                "gt_code": gt_code,
                "last_change_time": last_change_time,
                "vi_occurrences": [],
                "tbm_buildtime": format_timestamp(row.get("tbm_buildtime")),
                "tbm_name": format_tbm_name(row.get("tbm_name")),
                "production_day": clean_value(row.get("production_day")),
                # per-barcode flags (this tyre's own scrap / upgrade / rejected)
                "barcode_scrap": 1 if safe_int(row.get("scrap")) > 0 else 0,
                "barcode_upgraded": 1 if safe_int(row.get("upgraded")) > 0 else 0,
                "barcode_rejection": 1 if safe_int(row.get("rejection")) > 0 else 0,
                "upgraded": safe_int(row.get("upgraded")),
                "curing_infeededtime": format_timestamp(row.get("curing_infeededtime")),
                "components": [],
                "sur_repair_comment": None,
                "sur_repair_time": None,
                "sur_repair_code": None,
                "curing": None,
                "vi_time": format_timestamp(row.get("vi_time")),
                "createdon": clean_value(row.get("createdon")),
                "viresult": safe_int(row.get("viresult")),
            }

        document = grouped[group_key]

        # Update vi_time if it's None and we have a value
        if document.get("vi_time") is None and row.get("vi_time") is not None:
            document["vi_time"] = format_timestamp(row.get("vi_time"))

        # Update createdon if it's None and we have a value
        if document.get("createdon") is None and row.get("createdon") is not None:
            document["createdon"] = clean_value(row.get("createdon"))

        # Keep the LATEST last_change_time for this barcode (the key no longer
        # includes it, but we still store the column).
        _lct = format_timestamp(row.get("last_change_time"))
        if _lct and (document.get("last_change_time") is None or _lct > document["last_change_time"]):
            document["last_change_time"] = _lct

        # Component
        component = clean_value(row.get("component"))
        cart_identifier = clean_value(row.get("cart_identifier"))
        if component and cart_identifier:
            pair = {"component": component, "cart_identifier": cart_identifier}
            if pair not in document["components"]:
                document["components"].append(pair)

        # Repair
        repair_comment = clean_value(row.get("sur_repair_comment"))
        if repair_comment:
            document["sur_repair_comment"] = repair_comment

        repair_time = format_timestamp(row.get("sur_repair_time"))
        if repair_time:
            document["sur_repair_time"] = repair_time

        repair_code = clean_value(row.get("sur_repair_code"))
        if repair_code:
            document["sur_repair_code"] = repair_code

        # TBM buildtime / Curing infeed time
        if document["tbm_buildtime"] is None:
            tbm_bt = format_timestamp(row.get("tbm_buildtime"))
            if tbm_bt:
                document["tbm_buildtime"] = tbm_bt

        # TBM name (TBTB01 -> "TBM 1")
        if document.get("tbm_name") is None:
            tbm_nm = format_tbm_name(row.get("tbm_name"))
            if tbm_nm:
                document["tbm_name"] = tbm_nm

        # production_day (06:00-based) — backfill if the first row lacked it
        if document.get("production_day") is None:
            pd_val = clean_value(row.get("production_day"))
            if pd_val:
                document["production_day"] = pd_val

        # Per-barcode flags (this tyre's own scrap/upgrade/rejected) —
        # keep the max (1 wins) across all of the barcode's rows.
        document["upgraded"] = max(
            safe_int(document.get("upgraded")),
            safe_int(row.get("upgraded"))
        )
        document["barcode_upgraded"] = max(
            safe_int(document.get("barcode_upgraded")),
            1 if safe_int(row.get("upgraded")) > 0 else 0
        )
        document["barcode_scrap"] = max(
            safe_int(document.get("barcode_scrap")),
            1 if safe_int(row.get("scrap")) > 0 else 0
        )
        document["barcode_rejection"] = max(
            safe_int(document.get("barcode_rejection")),
            1 if safe_int(row.get("rejection")) > 0 else 0
        )

        if document["curing_infeededtime"] is None:
            cur_it = format_timestamp(row.get("curing_infeededtime"))
            if cur_it:
                document["curing_infeededtime"] = cur_it

        # vi_time + last_change_time — combined array
        vi_t = format_timestamp(row.get("vi_time"))
        lct = format_timestamp(row.get("last_change_time")) or last_change_time

        if vi_t or lct:
            existing_vi_times = [e.get("vi_time") for e in document["vi_occurrences"]]
            if vi_t not in existing_vi_times:
                document["vi_occurrences"].append({
                    "vi_time": vi_t,
                    "last_change_time": lct
                })

        # Curing
        if row.get("curing_id") is not None:
            if document["curing"] is None:
                document["curing"] = {
                    "id": convert_bson_value(row.get("curing_id")),
                    "utctimestamp": format_timestamp(row.get("curing_utctimestamp")),
                    "localizedtimestamp": format_timestamp(row.get("curing_localizedtimestamp")),
                    "sku_code": clean_value(row.get("curing_sku_code")),
                    "curing_barcode": clean_value(row.get("curing_barcode")),
                    "gt_code": clean_value(row.get("curing_gt_code")),
                    "bladder_code": clean_value(row.get("bladder_code")),
                    "mould_number": clean_value(row.get("mould_number")),
                    "order_number": convert_bson_value(row.get("order_number")),
                    "machineno": convert_bson_value(row.get("machineno")),
                    "machine_number": convert_bson_value(row.get("machine_number")),
                    "current_recipe": clean_value(row.get("current_recipe")),
                    "tyre_size": clean_value(row.get("tyre_size")),
                    "curing_count": convert_bson_value(row.get("curing_count")),
                    "bladder_count": convert_bson_value(row.get("bladder_count")),
                    "mould_count": convert_bson_value(row.get("mould_count")),
                    "bladder_target": convert_bson_value(row.get("bladder_target")),
                    "mould_target": convert_bson_value(row.get("mould_target"))
                }

    # ------------------------------------------------------------------
    # PERFORMANCE: precompute EVERYTHING per cart ONCE. The previous code
    # rebuilt each cart's barcode breakdown for every document that shared the
    # cart AND scanned every cart of the whole GT per document -> O(documents *
    # carts_per_gt * barcodes_per_cart), which hangs before insertion.
    #
    #   cart_info[(comp, cart, gt)] -> fully-built cart summary dict (built once)
    #   comp_groups_by_gt_bc[(gt, barcode)] -> list of groups containing the barcode
    #   gt_all_barcodes[gt]         -> set of all barcodes for the gt (built once)
    # ------------------------------------------------------------------
    # cart_info: built ONCE per cart, COUNTS ONLY. The full per-barcode
    # breakdown is deliberately NOT embedded — duplicating a cart's whole
    # barcode list into every tyre document made each doc huge and made the
    # build + insert far exceed the 5-minute target for millions of records.
    # Each tyre keeps its OWN barcode's scrap/upgrade/rejected flags at the doc
    # root (barcode_scrap / barcode_upgraded / barcode_rejection), and each
    # component_counts entry carries the cart aggregate counts.
    cart_info = {}
    comp_groups_by_gt_bc = {}
    gt_all_barcodes = {}
    for (comp, cart, gt), group in cart_summary.items():
        cart_info[(comp, cart, gt)] = {
            "component": comp,
            "cart_identifier": cart,
            "cart_barcode_count": len(group["barcodes"]),
            "scrap": len(group.get("scrap_barcodes", set())),
            "upgraded": len(group.get("upgraded_barcodes", set())),
            "rejection": len(group.get("rejection_barcodes", set())),
        }
        gt_all_barcodes.setdefault(gt, set()).update(group["barcodes"])
        for bc in group["barcodes"]:
            comp_groups_by_gt_bc.setdefault((gt, bc), []).append(group)

    # Memo for cart-level metrics keyed by the set of carts a barcode belongs to
    # (so identical cart-combinations are computed once, not per document).
    _cart_metric_memo = {}

    # Final documents
    final_documents = []

    for document in grouped.values():
        gt_code = document["gt_code"]
        this_barcode = document.get("barcode")

        # Component list
        document["component"] = unique_preserve_order([
            x["component"] for x in document["components"]
        ])

        # Cart list
        document["cart_identifier"] = unique_preserve_order([
            x["cart_identifier"] for x in document["components"]
        ])

        # Component counts: for THIS document's own carts only, look up the
        # already-built cart summary directly (O(this doc's carts), no scan of
        # the whole GT and no per-document rebuild of barcode lists).
        _seen_keys = set()
        _counts = []
        for x in document["components"]:
            k = (x["component"], x["cart_identifier"], gt_code)
            if k in _seen_keys:
                continue
            _seen_keys.add(k)
            info = cart_info.get(k)
            if info is not None:
                _counts.append(info)

        document["component_counts"] = sorted(
            _counts,
            key=lambda x: (x["component"], x["cart_identifier"])
        )
        document["component_total_sum"] = sum(
            c["cart_barcode_count"] for c in document["component_counts"]
        )
        document["component_unique_barcodes"] = len(gt_all_barcodes.get(gt_code, ()))

        # Cart-level metrics for THIS specific barcode.
        # PERFORMANCE: the union of barcodes across the barcode's carts is the
        # same for every barcode that belongs to the SAME set of carts. The old
        # code recomputed these big set-unions for every document (52k docs ->
        # ~7 min). We now MEMOIZE the result by the tuple of cart keys, so the
        # expensive union runs once per distinct cart-combination.
        total_tyres = total_rejection = total_scrap = 0
        groups_for_bc = comp_groups_by_gt_bc.get((gt_code, this_barcode))
        if groups_for_bc:
            memo_key = tuple(sorted(id(g) for g in groups_for_bc))
            cached = _cart_metric_memo.get(memo_key)
            if cached is None:
                if len(groups_for_bc) == 1:
                    g = groups_for_bc[0]
                    cached = (len(g["barcodes"]),
                              len(g["rejection_barcodes"]),
                              len(g["scrap_barcodes"]))
                else:
                    ub = set()
                    rb = set()
                    sb = set()
                    for group in groups_for_bc:
                        ub |= group["barcodes"]
                        rb |= group["rejection_barcodes"]
                        sb |= group["scrap_barcodes"]
                    cached = (len(ub), len(rb), len(sb))
                _cart_metric_memo[memo_key] = cached
            total_tyres, total_rejection, total_scrap = cached

        document["tyres"] = total_tyres
        document["rejection"] = total_rejection
        document["scrap"] = total_scrap
        document["defect_pcr"] = round((total_rejection * 100.0 / total_tyres), 2) if total_tyres else 0.0
        document["final_scrap_pcr"] = round((total_scrap * 100.0 / total_tyres), 2) if total_tyres else 0.0

        # NOTE: the "vi_reason" array is intentionally NOT stored anymore
        # (removed per requirement). All other fields/columns are preserved.

        # Remove helper
        document.pop("components", None)

        # Metadata
        document["source"] = "Redshift"
        document["updated_at"] = datetime.now(timezone.utc)

        # Final BSON conversion
        document = convert_bson_value(document)
        final_documents.append(document)

    return final_documents


# ============================================================
# INSERT INTO MONGODB
# ============================================================

def _dedup_id(doc):
    """Deterministic _id from the dedup key (barcode, sku, gt_code,
    last_change_time). Same key -> same _id, so a rerun of the same record is a
    duplicate _id (skipped) while genuinely new records get new _ids.
    Dedup is enforced by _id uniqueness — no per-document query needed, which is
    what makes this fast enough over the SSH tunnel."""
    key = "|".join(str(doc.get(k) or "") for k in
                   ("barcode", "sku", "gt_code", "last_change_time"))
    return hashlib.md5(key.encode("utf-8")).hexdigest()


def insert_into_mongodb(documents, tunnel, local_port,
                        target_collection=None, index_specs=None):
    """APPEND + DEDUP into the target collection through the SSH tunnel.

    Design (fast AND tunnel-safe):
      - Dedup is done via a DETERMINISTIC _id built from
        (barcode, sku, gt_code, last_change_time). Reruns of the same record
        collide on _id and are skipped; new records insert. No per-doc query
        (much faster than ReplaceOne upserts over a high-latency tunnel).
      - Uses plain insert_many(ordered=False) and ignores duplicate-key errors.
      - ONE shared MongoClient with a bounded connection pool. Modest worker
        count so we DON'T exceed the SSH server's MaxSessions (the previous
        "one client per worker × 12" opened too many channels and the tunnel
        timed out with 'open new channel ssh error').
      - APPEND mode: existing data is never dropped.
    """
    if not documents:
        print("No documents to insert.")
        return

    target_collection = target_collection or MONGO_COLLECTION
    if index_specs is None:
        index_specs = [
            ([("gt_code", 1)], {"name": "gt_code_index"}),
            ([("barcode", 1), ("last_change_time", 1)], {"name": "barcode_lct_lookup"}),
        ]

    t0 = datetime.now()

    # Convert ALL documents to BSON-safe values once, and stamp the dedup _id.
    documents = convert_documents_for_mongodb(documents)
    for d in documents:
        d["_id"] = _dedup_id(d)
    total_docs = len(documents)
    print(f"[TIMING] BSON conversion done in {(datetime.now() - t0).total_seconds():.1f}s "
          f"({total_docs} docs) -> {target_collection}")

    batch_size = 50000           # docs per insert_many round trip

    batches = [documents[i:i + batch_size] for i in range(0, total_docs, batch_size)]
    total_batches = len(batches)

    inserted = {"n": 0}

    # SINGLE connection over the SSH tunnel. Threading gives NO real parallelism
    # over one tunnel (traffic serializes) and opening many channels caused the
    # 'open new channel ssh error' timeout. Explicit socket/connect timeouts
    # ensure the run fails fast instead of hanging silently for 30+ minutes.
    shared_client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=3000000,
        connectTimeoutMS=3000000,
        socketTimeoutMS=12000000,
        maxPoolSize=2,
    )
    shared_coll = shared_client[MONGO_DATABASE][target_collection]

    try:
        print("Connecting to MongoDB...")
        shared_client.admin.command("ping")

        before = shared_coll.estimated_document_count()
        print(f"[INFO] {target_collection} existing docs before this run: ~{before}")

        t1 = datetime.now()
        # Sequential batches — reliable over the tunnel.
        for idx, batch in enumerate(batches, start=1):
            newly = 0
            try:
                res = shared_coll.insert_many(batch, ordered=False)
                newly = len(res.inserted_ids)
            except BulkWriteError as bwe:
                # Duplicate-key (11000) errors are expected for records that
                # already exist (dedup via deterministic _id). Count only new.
                newly = bwe.details.get("nInserted", 0)
                non_dup = [e for e in bwe.details.get("writeErrors", [])
                           if e.get("code") != 11000]
                if non_dup:
                    print(f"[WARN] batch {idx}: {len(non_dup)} non-duplicate write errors "
                          f"(e.g. {non_dup[0].get('errmsg', '')[:120]})")
            inserted["n"] += newly
            print(f"MongoDB insert batch {idx}/{total_batches} "
                  f"(new so far: {inserted['n']}) "
                  f"[{(datetime.now() - t1).total_seconds():.1f}s]")
        print(f"[TIMING] insert phase done in {(datetime.now() - t1).total_seconds():.1f}s")

        # Lookup indexes (created after load; dedup is guaranteed by _id).
        def _ensure_index(key_spec, **kwargs):
            wanted = [(k, int(v)) for k, v in key_spec]
            try:
                for _name, _info in shared_coll.index_information().items():
                    if [(k, int(v)) for k, v in _info.get("key", [])] == wanted:
                        return
                shared_coll.create_index(key_spec, **kwargs)
            except Exception as _e:
                print(f"[WARN] index {key_spec} skipped: {_e}")

        for key_spec, kwargs in index_specs:
            _ensure_index(key_spec, **kwargs)

        after = shared_coll.estimated_document_count()
        print("------------------------------------------")
        print(f"MongoDB NEW documents inserted this run: {inserted['n']} -> {target_collection}")
        print(f"MongoDB {target_collection} total docs now: ~{after}")
        print(f"[TIMING] total insert_into_mongodb: {(datetime.now() - t0).total_seconds():.1f}s")
        print("MongoDB insert completed.")

    except Exception as e:
        print("MongoDB insertion failed:")
        print(str(e))
        traceback.print_exc()
        raise
    finally:
        shared_client.close()
        print("MongoDB connection closed.")


# ============================================================
# MAIN
# ============================================================

def main():
    start_time = datetime.now()
    print("==========================================")
    print("VI DATA PROCESS STARTED")
    print(f"Start Date : {START_DATE}")
    print(f"End Date   : {END_DATE}")
    print(f"GT Codes   : {GT_CODES}")
    print("==========================================")

    try:
        # 1. Fetch Redshift data
        rows = fetch_redshift_data()

        if not rows:
            print("No data found in Redshift.")
            return

        # 2. GT Summary
        print("Building GT summary...")
        gt_summary = build_gt_summary(rows)
        print(f"GT summaries created: {len(gt_summary)}")

        # 3. Cart Summary
        print("Building cart summary...")
        _t = datetime.now()
        cart_summary = build_cart_summary(rows)
        print(f"Cart summaries created: {len(cart_summary)} "
              f"[TIMING {(datetime.now() - _t).total_seconds():.1f}s]")

        # 4. MongoDB Documents
        # NOTE: build_vi_reason() is no longer needed — the vi_reason array is
        # not stored anymore, so we skip that pass entirely (saves time/memory).
        print("Building MongoDB documents...")
        _t = datetime.now()
        documents = build_mongodb_documents(rows, gt_summary, cart_summary, {})
        print(f"MongoDB documents prepared: {len(documents)} "
              f"[TIMING {(datetime.now() - _t).total_seconds():.1f}s]")

        # 6. Insert main tyre documents into MongoDB (only the vi_data collection)
        insert_into_mongodb(documents, None, None)

        # Complete
        end_time = datetime.now()
        duration = end_time - start_time
        print("==========================================")
        print("VI DATA PROCESS COMPLETED")
        print(f"Total time: {duration}")
        print("==========================================")

    except Exception as e:
        print("==========================================")
        print("PROCESS FAILED")
        print(str(e))
        print("==========================================")
        traceback.print_exc()


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    main()



