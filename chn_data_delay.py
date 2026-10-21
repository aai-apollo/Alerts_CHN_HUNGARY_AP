"""
CHN Data Delay Monitor
======================
Runs every 16 minutes. For each run it:
  1. Moves chn_mismatch_update_counts  →  chn_mismatch_counts  (archives previous run)
  2. Calculates a 16-minute window ending at now (IST)
  3. Fetches individual table row counts for the window
  4. Runs 6 cross-table mismatch queries for the window
  5. Saves fresh results into  chn_mismatch_update_counts  (staging)

Collection flow:
  chn_mismatch_update_counts  — latest run only  (staging / current snapshot)
  chn_mismatch_counts         — full historical archive of all past runs

Timestamp handling:
  - sc_tub_pcr.ubtable       : date + time columns, IST as-is (no conversion)
  - sc_cimcorp_pcr.tbm       : timestamp UTC → use UTC window
  - sc_cimcorp_pcr.curing    : timestamp UTC → use UTC window
  - sc_ames.opcua_barcode    : received_at UTC → use UTC window

Usage:
    python chn_data_delay.py            # single-shot (for cron)
    python chn_data_delay.py --loop     # run forever every 16 min
"""

import sys
import ast
import time
import logging
import datetime
import argparse
from zoneinfo import ZoneInfo

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
LOG_FILE = "chn_data_delay.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DEPENDENCIES
# ---------------------------------------------------------------------------
try:
    import redshift_connector
except ImportError:
    log.error("redshift_connector not installed.  pip install redshift-connector")
    sys.exit(1)

try:
    from pymongo import MongoClient
    from pymongo.errors import PyMongoError
except ImportError:
    log.error("pymongo not installed.  pip install pymongo")
    sys.exit(1)

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

REDSHIFT_CONFIG = {
    "host":     "atl-dl-prd-01.cuvxusm4qify.ap-south-1.redshift.amazonaws.com",
    "database": "db_prd_plant_che",
    "user":     "usr_thangadurai_a",
    "password": "QtHJO%1^m^dQEVFc",
    "port":     5439,
}

MONGO_CONFIG = {
    "uri":      "mongodb://admin:sdfda%40wdfsdfvsdg32FGSFD@10.100.192.96:27017/",
    # "uri":      "mongodb://localhost:27017/",
    "database": "redshift_monitor",
}

INTERVAL_MINUTES = 16

IST = ZoneInfo("Asia/Kolkata")
UTC = datetime.timezone.utc

# Collections
COL_UPDATES  = "chn_mismatch_update_counts"  # staging — latest run only
COL_MISMATCH = "chn_mismatch_counts"          # archive — all historical runs


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def get_redshift_conn():
    return redshift_connector.connect(**REDSHIFT_CONFIG)


def get_mongo_db():
    client = MongoClient(MONGO_CONFIG["uri"], serverSelectionTimeoutMS=5000)
    return client[MONGO_CONFIG["database"]]


def ensure_indexes(db):
    for col in (COL_UPDATES, COL_MISMATCH):
        db[col].create_index([("run_time_ist", -1)])
        db[col].create_index([("query_name", 1), ("run_time_ist", -1)])


def rotate_updates_to_archive(db):
    """
    Move all documents from chn_mismatch_update_counts → chn_mismatch_counts.
    Called at the START of every run so the previous run is archived before
    fresh data is written into staging.
    """
    docs = list(db[COL_UPDATES].find({}, {"_id": 0}))
    if not docs:
        log.info("[ROTATE] chn_mismatch_update_counts is empty — nothing to archive.")
        return 0
    try:
        db[COL_MISMATCH].insert_many(docs, ordered=False)
        db[COL_UPDATES].delete_many({})
        log.info(
            "[ROTATE] Moved %d docs  %s  →  %s.",
            len(docs), COL_UPDATES, COL_MISMATCH,
        )
        return len(docs)
    except PyMongoError as exc:
        log.error("[ROTATE] Failed: %s", exc)
        return 0


def run_count_query(conn, sql: str, label: str) -> int:
    """Execute a COUNT query. Returns count >= 0, or -1 on error."""
    try:
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        cur.close()
        count = int(row[0]) if row and row[0] is not None else 0
        log.info("  %-55s : %d", label, count)
        return count
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        log.error("  %-55s : ERROR — %s", label, _short_err(exc))
        return -1


def fetch_barcode_list(conn, barcode_sql: str, label: str) -> list:
    """
    Execute a SELECT barcode query and return a list of barcode strings.
    Only called when mis_match_count > 0.
    Returns empty list on error.
    """
    try:
        cur = conn.cursor()
        cur.execute(barcode_sql)
        rows = cur.fetchall()
        cur.close()
        barcodes = [str(row[0]) for row in rows if row[0] is not None]
        log.info("  %-55s : %d barcodes fetched", label + " [barcodes]", len(barcodes))
        return barcodes
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        log.error("  %-55s : ERROR fetching barcodes — %s", label, _short_err(exc))
        return []


def _short_err(exc) -> str:
    try:
        d = ast.literal_eval(str(exc)) if str(exc).strip().startswith("{") else {}
        if d:
            return f"[{d.get('C','?')}] {d.get('M', str(exc))}"
    except Exception:
        pass
    return str(exc)[:120]


# ---------------------------------------------------------------------------
# TIME WINDOW
# ---------------------------------------------------------------------------

def get_window(now_ist: datetime.datetime, minutes: int):
    """
    Returns (ws_ist, we_ist, ws_utc, we_utc) all timezone-aware.
    ws = window start, we = window end.
    """
    we_ist = now_ist
    ws_ist = now_ist - datetime.timedelta(minutes=minutes)
    ws_utc = ws_ist.astimezone(UTC)
    we_utc = we_ist.astimezone(UTC)
    return ws_ist, we_ist, ws_utc, we_utc


def fmt_ist(dt: datetime.datetime) -> str:
    """Naive IST string for embedding in Redshift SQL (IST-stored columns)."""
    # Strip tz for Redshift naive comparison
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def fmt_utc(dt: datetime.datetime) -> str:
    """Naive UTC string for embedding in Redshift SQL (UTC-stored columns)."""
    return dt.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# INDIVIDUAL TABLE COUNTS
# ---------------------------------------------------------------------------

def fetch_table_counts(conn, ws_ist, we_ist, ws_utc, we_utc) -> dict:
    """
    Fetch row count for each source table within the window.
    Returns a dict: { "TUB": n, "TBM": n, "CURING": n, "AMES": n }

    All comparisons are done in IST:
      TUB    (sc_tub_pcr.ubtable)    — date+time columns already IST, compare directly
      TBM    (sc_cimcorp_pcr.tbm)    — timestamp stored UTC, use CONVERT_TIMEZONE → IST
      CURING (sc_cimcorp_pcr.curing) — timestamp stored UTC, use CONVERT_TIMEZONE → IST
      AMES   (sc_ames.opcua_barcode) — received_at stored UTC, use CONVERT_TIMEZONE → IST
    """
    ubt_window = (
        f"(\"date\" > '{ws_ist.date()}' OR "
        f"(\"date\" = '{ws_ist.date()}' AND \"time\" >= '{ws_ist.strftime('%H:%M:%S')}')) "
        f"AND "
        f"(\"date\" < '{we_ist.date()}' OR "
        f"(\"date\" = '{we_ist.date()}' AND \"time\" < '{we_ist.strftime('%H:%M:%S')}'))"
    )

    ist_start = fmt_ist(ws_ist)
    ist_end   = fmt_ist(we_ist)

    table_queries = [
        (
            "TUB",
            f'SELECT COUNT(DISTINCT barcode) FROM sc_tub_pcr.ubtable WHERE {ubt_window}',
            "sc_tub_pcr.ubtable (IST)",
        ),
        (
            "TBM",
            f"""SELECT COUNT(DISTINCT t.barcode)
                FROM sc_cimcorp_pcr.tbm tbm
                LEFT JOIN sc_cimcorp_pcr.transportationunit t
                    ON tbm.transportationunitid = t.transportationunitid
                WHERE CONVERT_TIMEZONE('Asia/Kolkata', tbm.timestamp)
                      BETWEEN '{ist_start}' AND '{ist_end}'""",
            "sc_cimcorp_pcr.tbm (UTC→IST)",
        ),
        (
            "CURING",
            f"""SELECT COUNT(DISTINCT t.barcode)
                FROM sc_cimcorp_pcr.curing curing
                LEFT JOIN sc_cimcorp_pcr.transportationunit t
                    ON curing.transportationunitid = t.transportationunitid
                WHERE CONVERT_TIMEZONE('Asia/Kolkata', curing.timestamp)
                      BETWEEN '{ist_start}' AND '{ist_end}'""",
            "sc_cimcorp_pcr.curing (UTC→IST)",
        ),
        (
            "AMES",
            f"""SELECT COUNT(DISTINCT barcode)
                FROM sc_ames.opcua_barcode
                WHERE machine_code LIKE '%WP%'
                  AND CONVERT_TIMEZONE('Asia/Kolkata', received_at)
                      BETWEEN '{ist_start}' AND '{ist_end}'""",
            "sc_ames.opcua_barcode (UTC→IST)",
        ),
        (
            "TRANSPORTATION",
            f"""SELECT COUNT(DISTINCT barcode)
                FROM sc_cimcorp_pcr.transportationunit
                WHERE CONVERT_TIMEZONE('Asia/Kolkata', created_at)
                      BETWEEN '{ist_start}' AND '{ist_end}'""",
            "sc_cimcorp_pcr.transportationunit (UTC→IST)",
        ),
    ]

    counts = {}
    log.info("\n[ Individual Table Counts ]")
    for key, sql, label in table_queries:
        c = run_count_query(conn, sql, label)
        counts[key] = c if c >= 0 else 0

    return counts


# ---------------------------------------------------------------------------
# MISMATCH QUERIES — each query doc includes individual table counts
# ---------------------------------------------------------------------------

def fetch_mismatch_counts(conn, ws_ist, we_ist, ws_utc, we_utc,
                           table_counts: dict) -> list:
    """
    Runs 4 cross-table mismatch queries.

    Each result document shows:
      first_table        : name of source table
      second_table       : name of target table
      First_Table_Count  : total barcodes from source in window
      Second_Table_Count : source barcodes FOUND in target  (source - mismatch)
      mis_match_count    : source barcodes NOT in target

    Example — TUB=60, mismatch=0:
      first_table        : "TUB"
      second_table       : "AMES"
      First_Table_Count  : 60
      Second_Table_Count : 60
      mis_match_count    : 0

    Timestamp handling:
      ubtable : IST date/time columns (no conversion)
      tbm     : UTC stored → CONVERT_TIMEZONE to IST
      curing  : UTC stored → CONVERT_TIMEZONE to IST
      opcua   : UTC stored → CONVERT_TIMEZONE to IST
    """
    ist_start = fmt_ist(ws_ist)
    ist_end   = fmt_ist(we_ist)

    ubt_window = (
        f"(\"date\" > '{ws_ist.date()}' OR "
        f"(\"date\" = '{ws_ist.date()}' AND \"time\" >= '{ws_ist.strftime('%H:%M:%S')}')) "
        f"AND "
        f"(\"date\" < '{we_ist.date()}' OR "
        f"(\"date\" = '{we_ist.date()}' AND \"time\" < '{we_ist.strftime('%H:%M:%S')}'))"
    )

    # Each entry: (query_name, description, count_sql, barcode_sql, source_key, target_key)
    queries = [
        # ── 1. In TUB, not in TBM ──────────────────────────────────────────
        (
            "TUB_not_in_TBM (cimcorp)",
            "Present in TUB, not in TBM (cimcorp) (last 16 min)",
            f"""
            SELECT COUNT(DISTINCT barcode)
            FROM sc_tub_pcr.ubtable
            WHERE {ubt_window}
              AND barcode NOT IN (
                  SELECT t.barcode
                  FROM sc_cimcorp_pcr.tbm tbm
                  LEFT JOIN sc_cimcorp_pcr.transportationunit t
                      ON tbm.transportationunitid = t.transportationunitid
              )
            """,
            f"""
            SELECT DISTINCT barcode
            FROM sc_tub_pcr.ubtable
            WHERE {ubt_window}
              AND barcode NOT IN (
                  SELECT t.barcode
                  FROM sc_cimcorp_pcr.tbm tbm
                  LEFT JOIN sc_cimcorp_pcr.transportationunit t
                      ON tbm.transportationunitid = t.transportationunitid
              )
            """,
            "TUB", "TBM",
        ),
        # ── 2. In TUB, not in CURING ───────────────────────────────────────
        (
            "TUB_not_in_CURING(cimcorp)",
            "Present in TUB, not in CURING (cimcorp) (last 16 min)",
            f"""
            SELECT COUNT(DISTINCT barcode)
            FROM sc_tub_pcr.ubtable
            WHERE {ubt_window}
              AND barcode NOT IN (
                  SELECT t.barcode
                  FROM sc_cimcorp_pcr.curing curing
                  LEFT JOIN sc_cimcorp_pcr.transportationunit t
                      ON curing.transportationunitid = t.transportationunitid
              )
            """,
            f"""
            SELECT DISTINCT barcode
            FROM sc_tub_pcr.ubtable
            WHERE {ubt_window}
              AND barcode NOT IN (
                  SELECT t.barcode
                  FROM sc_cimcorp_pcr.curing curing
                  LEFT JOIN sc_cimcorp_pcr.transportationunit t
                      ON curing.transportationunitid = t.transportationunitid
              )
            """,
            "TUB", "CURING",
        ),
        # ── 3. In TBM (cimcorp), not in AMES ──────────────────────────────
        (
            "TBM(cimcorp)_not_in_AMES",
            "Present in TBM (cimcorp), not in AMES (last 16 min)",
            f"""
            SELECT COUNT(DISTINCT t.barcode)
            FROM sc_cimcorp_pcr.tbm tbm
            LEFT JOIN sc_cimcorp_pcr.transportationunit t
                ON tbm.transportationunitid = t.transportationunitid
            WHERE CONVERT_TIMEZONE('Asia/Kolkata', tbm.timestamp)
                  BETWEEN '{ist_start}' AND '{ist_end}'
              AND t.barcode NOT IN (
                  SELECT barcode
                  FROM sc_ames.opcua_barcode
                  WHERE machine_code LIKE '%WP%'
              )
            """,
            f"""
            SELECT DISTINCT t.barcode
            FROM sc_cimcorp_pcr.tbm tbm
            LEFT JOIN sc_cimcorp_pcr.transportationunit t
                ON tbm.transportationunitid = t.transportationunitid
            WHERE CONVERT_TIMEZONE('Asia/Kolkata', tbm.timestamp)
                  BETWEEN '{ist_start}' AND '{ist_end}'
              AND t.barcode NOT IN (
                  SELECT barcode
                  FROM sc_ames.opcua_barcode
                  WHERE machine_code LIKE '%WP%'
              )
            """,
            "TBM", "AMES",
        ),
        # ── 4. In TUB, not in AMES ────────────────────────────────────────
        (
            "TUB_not_in_AMES",
            "Present in TUB, not in AMES (last 16 min)",
            f"""
            SELECT COUNT(DISTINCT barcode)
            FROM sc_tub_pcr.ubtable
            WHERE {ubt_window}
              AND barcode NOT IN (
                  SELECT barcode
                  FROM sc_ames.opcua_barcode
                  WHERE machine_code LIKE '%WP%'
              )
            """,
            f"""
            SELECT DISTINCT barcode
            FROM sc_tub_pcr.ubtable
            WHERE {ubt_window}
              AND barcode NOT IN (
                  SELECT barcode
                  FROM sc_ames.opcua_barcode
                  WHERE machine_code LIKE '%WP%'
              )
            """,
            "TUB", "AMES",
        ),
        # ── 5. In CURING, not in TBM ──────────────────────────────────────
        (
            "CURING(cimcorp)_not_in_TBM(cimcorp)",
            "Present in CURING (cimcorp), not in TBM (cimcorp) (last 16 min)",
            f"""
            SELECT COUNT(DISTINCT t.barcode)
            FROM sc_cimcorp_pcr.curing curing
            LEFT JOIN sc_cimcorp_pcr.transportationunit t
                ON curing.transportationunitid = t.transportationunitid
            WHERE CONVERT_TIMEZONE('Asia/Kolkata', curing.timestamp)
                  BETWEEN '{ist_start}' AND '{ist_end}'
              AND t.barcode NOT IN (
                  SELECT t2.barcode
                  FROM sc_cimcorp_pcr.tbm tbm
                  LEFT JOIN sc_cimcorp_pcr.transportationunit t2
                      ON tbm.transportationunitid = t2.transportationunitid
              )
            """,
            f"""
            SELECT DISTINCT t.barcode
            FROM sc_cimcorp_pcr.curing curing
            LEFT JOIN sc_cimcorp_pcr.transportationunit t
                ON curing.transportationunitid = t.transportationunitid
            WHERE CONVERT_TIMEZONE('Asia/Kolkata', curing.timestamp)
                  BETWEEN '{ist_start}' AND '{ist_end}'
              AND t.barcode NOT IN (
                  SELECT t2.barcode
                  FROM sc_cimcorp_pcr.tbm tbm
                  LEFT JOIN sc_cimcorp_pcr.transportationunit t2
                      ON tbm.transportationunitid = t2.transportationunitid
              )
            """,
            "CURING", "TBM",
        ),
        # ── 6. In TBM, not in Transportation Unit ─────────────────────────
        (
            "TBM(cimcorp)_not_in_TRANSPORTATION",
            "Present in TBM (cimcorp), not in Transportation Unit (last 16 min)",
            f"""
            SELECT COUNT(*)
            FROM sc_cimcorp_pcr.tbm tbm
            WHERE CONVERT_TIMEZONE('Asia/Kolkata', tbm.timestamp)
                  BETWEEN '{ist_start}' AND '{ist_end}'
              AND tbm.transportationunitid NOT IN (
                  SELECT transportationunitid
                  FROM sc_cimcorp_pcr.transportationunit
              )
            """,
            f"""
            SELECT DISTINCT t.barcode
            FROM sc_cimcorp_pcr.tbm tbm
            LEFT JOIN sc_cimcorp_pcr.transportationunit t
                ON tbm.transportationunitid = t.transportationunitid
            WHERE CONVERT_TIMEZONE('Asia/Kolkata', tbm.timestamp)
                  BETWEEN '{ist_start}' AND '{ist_end}'
              AND tbm.transportationunitid NOT IN (
                  SELECT transportationunitid
                  FROM sc_cimcorp_pcr.transportationunit
              )
            """,
            "TBM", "TRANSPORTATION",
        ),
        # ── 7. In CURING, not in Transportation Unit ──────────────────────
        (
            "CURING(cimcorp)_not_in_TRANSPORTATION",
            "Present in CURING (cimcorp), not in Transportation Unit (last 16 min)",
            f"""
            SELECT COUNT(*)
            FROM sc_cimcorp_pcr.curing curing
            WHERE CONVERT_TIMEZONE('Asia/Kolkata', curing.timestamp)
                  BETWEEN '{ist_start}' AND '{ist_end}'
              AND curing.transportationunitid NOT IN (
                  SELECT transportationunitid
                  FROM sc_cimcorp_pcr.transportationunit
              )
            """,
            f"""
            SELECT DISTINCT t.barcode
            FROM sc_cimcorp_pcr.curing curing
            LEFT JOIN sc_cimcorp_pcr.transportationunit t
                ON curing.transportationunitid = t.transportationunitid
            WHERE CONVERT_TIMEZONE('Asia/Kolkata', curing.timestamp)
                  BETWEEN '{ist_start}' AND '{ist_end}'
              AND curing.transportationunitid NOT IN (
                  SELECT transportationunitid
                  FROM sc_cimcorp_pcr.transportationunit
              )
            """,
            "CURING", "TRANSPORTATION",
        ),
    ]

    results = []
    log.info("\n[ Mismatch Counts ]")
    for query_name, description, count_sql, barcode_sql, source_key, target_key in queries:
        not_matched = run_count_query(conn, count_sql, description)
        error       = not_matched < 0

        source_count  = table_counts.get(source_key, 0)
        nm_count      = not_matched if not error else 0
        matched_count = max(0, source_count - nm_count)

        # Fetch actual barcodes only when there is a mismatch
        barcodes = []
        if nm_count > 0:
            barcodes = fetch_barcode_list(conn, barcode_sql, description)

        log.info(
            "    first=%s(%d)  second=%s  matched=%d  mismatch=%d  barcodes=%d",
            source_key, source_count,
            target_key, matched_count,
            nm_count, len(barcodes),
        )

        results.append({
            "query_name":           query_name,
            "description":          description,
            "first_table":          source_key,
            "second_table":         target_key,
            "First_Table_Count":    source_count,
            "Second_Table_Count":   matched_count,
            "mis_match_count":      nm_count,
            "mismatched_barcodes":  barcodes,   # [] when no mismatch
            "data_flag":            False,
            "query_error":          error,
        })

    return results


# ---------------------------------------------------------------------------
# MAIN CYCLE
# ---------------------------------------------------------------------------

def fetch_invalid_gt_codes(conn, ws_ist, we_ist) -> dict:
    """
    Find TBM records where the gt_code and modelno combination is invalid
    (i.e. the barcode's modelno in ubtable does not match the expected modelno
    for that machine's gttypeid).

    Returns:
    {
        "total_invalid":  int,
        "by_gt_code": {
            "GT8027": 5,
            "GT7793": 2,
            ...
        }
    }

    Window filter on tbm.timestamp (UTC stored → CONVERT_TIMEZONE to IST).
    """
    ist_start = fmt_ist(ws_ist)
    ist_end   = fmt_ist(we_ist)

    # ── Total count ───────────────────────────────────────────────────────
    sql_total = f"""
        WITH gt_mapping AS (
            SELECT 'GT8743' AS expected_gt_code, 15  AS expected_modelno UNION ALL
            SELECT 'GT7774', 15  UNION ALL SELECT 'GT7794', 16  UNION ALL
            SELECT 'GT8744', 44  UNION ALL SELECT 'GT7775', 44  UNION ALL
            SELECT 'GT8038', 91  UNION ALL SELECT 'GT8047', 92  UNION ALL
            SELECT 'GT7799', 108 UNION ALL SELECT 'GT8044', 118 UNION ALL
            SELECT 'GT7793', 145 UNION ALL SELECT 'GT7785', 201 UNION ALL
            SELECT 'GT8048', 204 UNION ALL SELECT 'GT7790', 500 UNION ALL
            SELECT 'GT8018', 506 UNION ALL SELECT 'GT7668', 104 UNION ALL
            SELECT 'GT8148', 525 UNION ALL SELECT 'GT8027', 202 UNION ALL
            SELECT 'GT8028', 144 UNION ALL SELECT 'GT8764', 102 UNION ALL
            SELECT 'GT7784', 102 UNION ALL SELECT 'GT8020', 137 UNION ALL
            SELECT 'GT8765', 137
        )
        SELECT COUNT(*) AS total_invalid
        FROM sc_cimcorp_pcr.tbm t
        JOIN sc_cimcorp_pcr.transportationunit tu
            ON t.transportationunitid = tu.transportationunitid
        JOIN sc_tub_pcr.ubtable u
            ON tu.barcode = u.barcode
        LEFT JOIN gt_mapping m
            ON t.gttypeid = m.expected_gt_code
        WHERE tu.barcode IS NOT NULL
          AND tu.barcode <> ''
          AND u.modelno <> m.expected_modelno
          AND CONVERT_TIMEZONE('Asia/Kolkata', t."timestamp")
              BETWEEN '{ist_start}' AND '{ist_end}'
    """

    # ── Per gt_code barcodes (only when total > 0) ────────────────────────
    sql_barcodes = f"""
        WITH gt_mapping AS (
            SELECT 'GT8743' AS expected_gt_code, 15  AS expected_modelno UNION ALL
            SELECT 'GT7774', 15  UNION ALL SELECT 'GT7794', 16  UNION ALL
            SELECT 'GT8744', 44  UNION ALL SELECT 'GT7775', 44  UNION ALL
            SELECT 'GT8038', 91  UNION ALL SELECT 'GT8047', 92  UNION ALL
            SELECT 'GT7799', 108 UNION ALL SELECT 'GT8044', 118 UNION ALL
            SELECT 'GT7793', 145 UNION ALL SELECT 'GT7785', 201 UNION ALL
            SELECT 'GT8048', 204 UNION ALL SELECT 'GT7790', 500 UNION ALL
            SELECT 'GT8018', 506 UNION ALL SELECT 'GT7668', 104 UNION ALL
            SELECT 'GT8148', 525 UNION ALL SELECT 'GT8027', 202 UNION ALL
            SELECT 'GT8028', 144 UNION ALL SELECT 'GT8764', 102 UNION ALL
            SELECT 'GT7784', 102 UNION ALL SELECT 'GT8020', 137 UNION ALL
            SELECT 'GT8765', 137
        )
        SELECT DISTINCT tu.barcode
        FROM sc_cimcorp_pcr.tbm t
        JOIN sc_cimcorp_pcr.transportationunit tu
            ON t.transportationunitid = tu.transportationunitid
        JOIN sc_tub_pcr.ubtable u
            ON tu.barcode = u.barcode
        LEFT JOIN gt_mapping m
            ON t.gttypeid = m.expected_gt_code
        WHERE tu.barcode IS NOT NULL
          AND tu.barcode <> ''
          AND u.modelno <> m.expected_modelno
          AND CONVERT_TIMEZONE('Asia/Kolkata', t."timestamp")
              BETWEEN '{ist_start}' AND '{ist_end}'
    """

    sql_total = f"""
        WITH gt_mapping AS (
            SELECT 'GT8743' AS expected_gt_code, 15  AS expected_modelno UNION ALL
            SELECT 'GT7774', 15  UNION ALL SELECT 'GT7794', 16  UNION ALL
            SELECT 'GT8744', 44  UNION ALL SELECT 'GT7775', 44  UNION ALL
            SELECT 'GT8038', 91  UNION ALL SELECT 'GT8047', 92  UNION ALL
            SELECT 'GT7799', 108 UNION ALL SELECT 'GT8044', 118 UNION ALL
            SELECT 'GT7793', 145 UNION ALL SELECT 'GT7785', 201 UNION ALL
            SELECT 'GT8048', 204 UNION ALL SELECT 'GT7790', 500 UNION ALL
            SELECT 'GT8018', 506 UNION ALL SELECT 'GT7668', 104 UNION ALL
            SELECT 'GT8148', 525 UNION ALL SELECT 'GT8027', 202 UNION ALL
            SELECT 'GT8028', 144 UNION ALL SELECT 'GT8764', 102 UNION ALL
            SELECT 'GT7784', 102 UNION ALL SELECT 'GT8020', 137 UNION ALL
            SELECT 'GT8765', 137
        )
        SELECT COUNT(*) AS total_invalid
        FROM sc_cimcorp_pcr.tbm t
        JOIN sc_cimcorp_pcr.transportationunit tu
            ON t.transportationunitid = tu.transportationunitid
        JOIN sc_tub_pcr.ubtable u
            ON tu.barcode = u.barcode
        LEFT JOIN gt_mapping m
            ON t.gttypeid = m.expected_gt_code
        WHERE tu.barcode IS NOT NULL
          AND tu.barcode <> ''
          AND u.modelno <> m.expected_modelno
          AND CONVERT_TIMEZONE('Asia/Kolkata', t."timestamp")
              BETWEEN '{ist_start}' AND '{ist_end}'
    """

    sql_by_gt = f"""
        WITH gt_mapping AS (
            SELECT 'GT8743' AS expected_gt_code, 15  AS expected_modelno UNION ALL
            SELECT 'GT7774', 15  UNION ALL SELECT 'GT7794', 16  UNION ALL
            SELECT 'GT8744', 44  UNION ALL SELECT 'GT7775', 44  UNION ALL
            SELECT 'GT8038', 91  UNION ALL SELECT 'GT8047', 92  UNION ALL
            SELECT 'GT7799', 108 UNION ALL SELECT 'GT8044', 118 UNION ALL
            SELECT 'GT7793', 145 UNION ALL SELECT 'GT7785', 201 UNION ALL
            SELECT 'GT8048', 204 UNION ALL SELECT 'GT7790', 500 UNION ALL
            SELECT 'GT8018', 506 UNION ALL SELECT 'GT7668', 104 UNION ALL
            SELECT 'GT8148', 525 UNION ALL SELECT 'GT8027', 202 UNION ALL
            SELECT 'GT8028', 144 UNION ALL SELECT 'GT8764', 102 UNION ALL
            SELECT 'GT7784', 102 UNION ALL SELECT 'GT8020', 137 UNION ALL
            SELECT 'GT8765', 137
        )
        SELECT t.gttypeid AS gt_code, COUNT(*) AS invalid_count
        FROM sc_cimcorp_pcr.tbm t
        JOIN sc_cimcorp_pcr.transportationunit tu
            ON t.transportationunitid = tu.transportationunitid
        JOIN sc_tub_pcr.ubtable u
            ON tu.barcode = u.barcode
        LEFT JOIN gt_mapping m
            ON t.gttypeid = m.expected_gt_code
        WHERE tu.barcode IS NOT NULL
          AND tu.barcode <> ''
          AND u.modelno <> m.expected_modelno
          AND CONVERT_TIMEZONE('Asia/Kolkata', t."timestamp")
              BETWEEN '{ist_start}' AND '{ist_end}'
        GROUP BY t.gttypeid
        ORDER BY invalid_count DESC
    """

    total = 0
    by_gt = {}
    error = False
    invalid_barcodes = []

    try:
        cur = conn.cursor()
        cur.execute(sql_total)
        row = cur.fetchone()
        cur.close()
        total = int(row[0]) if row and row[0] is not None else 0
        log.info("  %-55s : %d", "TBM invalid gt_code/modelno (total)", total)
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        log.error("  TBM invalid gt_code total query failed: %s", _short_err(exc))
        error = True

    try:
        cur = conn.cursor()
        cur.execute(sql_by_gt)
        rows = cur.fetchall()
        cur.close()
        for gt_code, cnt in rows:
            by_gt[str(gt_code)] = int(cnt)
            log.info("    %-15s : %d invalid", gt_code, cnt)
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        log.error("  TBM invalid gt_code per-group query failed: %s", _short_err(exc))
        error = True

    # Fetch actual barcodes only when there are invalid records
    if total > 0:
        invalid_barcodes = fetch_barcode_list(
            conn, sql_barcodes, "TBM_invalid_gt_modelno"
        )

    return {
        "query_name":           "TBM_invalid_gt_modelno",
        "description":          "TBM machines with invalid gt_code/modelno combination (last 16 min)",
        "first_table":          "TBM",
        "second_table":         "GT_MAPPING",
        "First_Table_Count":    total,
        "Second_Table_Count":   0,
        "mis_match_count":      total,
        "mismatched_barcodes":  invalid_barcodes,   # [] when no invalid records
        "by_gt_code":           by_gt,
        "query_error":          error,
    }


# ---------------------------------------------------------------------------
# MAIN CYCLE
# ---------------------------------------------------------------------------

def run_cycle(db):
    """One complete fetch + save cycle."""
    now_utc = datetime.datetime.now(UTC)
    now_ist = now_utc.astimezone(IST)

    ws_ist, we_ist, ws_utc, we_utc = get_window(now_ist, INTERVAL_MINUTES)

    log.info("=" * 65)
    log.info("  CHN DATA DELAY CHECK")
    log.info("  Window IST : %s  →  %s", fmt_ist(ws_ist), fmt_ist(we_ist))
    log.info("  Window UTC : %s  →  %s", fmt_utc(ws_utc), fmt_utc(we_utc))
    log.info("=" * 65)

    # ── step 1: archive previous run staging → history ────────────────────
    log.info("Archiving previous run...")
    rotate_updates_to_archive(db)

    # ── step 2: connect Redshift ──────────────────────────────────────────
    try:
        conn = get_redshift_conn()
        log.info("Redshift connected.")
    except Exception as exc:
        log.error("Cannot connect to Redshift: %s", exc)
        return

    try:
        # ── step 3: individual table counts ──────────────────────────────
        table_counts = fetch_table_counts(conn, ws_ist, we_ist, ws_utc, we_utc)

        # ── step 4: mismatch queries (embed table counts into each doc) ───
        mismatches = fetch_mismatch_counts(
            conn, ws_ist, we_ist, ws_utc, we_utc, table_counts
        )

        # ── step 4b: invalid gt_code / modelno check ──────────────────────
        log.info("\n[ Invalid GT Code / ModelNo Check ]")
        invalid_gt = fetch_invalid_gt_codes(conn, ws_ist, we_ist)

    finally:
        try:
            conn.close()
        except Exception:
            pass

    log.info("\n[ Window Summary ]")
    for k, v in table_counts.items():
        log.info("  %-10s : %d rows", k, v)
    for r in mismatches:
        log.info(
            "  %-40s : first=%s(%d)  second=%s(%d)  mismatch=%d",
            r["query_name"],
            r["first_table"],        r["First_Table_Count"],
            r["second_table"],       r["Second_Table_Count"],
            r["mis_match_count"],
        )
    log.info(
        "  %-40s : total_invalid=%d  by_gt=%s",
        invalid_gt["query_name"],
        invalid_gt["mis_match_count"],
        invalid_gt["by_gt_code"],
    )

    # ── step 5: insert fresh data into staging ────────────────────────────
    run_meta = {
        "run_time_ist":     now_ist,
        "run_time_utc":     now_utc,
        "window_start_ist": ws_ist,
        "window_end_ist":   we_ist,
        "window_start_utc": ws_utc,
        "window_end_utc":   we_utc,
        "interval_minutes": INTERVAL_MINUTES,
    }

    # regular mismatch docs + invalid gt doc
    all_docs = [{**run_meta, **r} for r in mismatches]
    all_docs.append({**run_meta, **invalid_gt})

    try:
        db[COL_UPDATES].insert_many(all_docs, ordered=False)
        log.info(
            "Saved %d docs to '%s' (staging).", len(all_docs), COL_UPDATES
        )
    except PyMongoError as exc:
        log.error("MongoDB insert_many failed: %s", exc)

    log.info("Cycle complete.\n")


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="CHN Data Delay Monitor")
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Run continuously every 16 minutes (default: single-shot)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=INTERVAL_MINUTES,
        help=f"Interval in minutes for --loop mode (default: {INTERVAL_MINUTES})",
    )
    args = parser.parse_args()

    try:
        db = get_mongo_db()
        ensure_indexes(db)
        log.info("MongoDB connected: %s / %s", MONGO_CONFIG["uri"], MONGO_CONFIG["database"])
    except PyMongoError as exc:
        log.error("Cannot connect to MongoDB: %s", exc)
        sys.exit(1)

    if args.loop:
        interval_sec = args.interval * 60
        log.info("Loop mode: running every %d minutes. Ctrl+C to stop.", args.interval)
        cycle = 0
        while True:
            cycle += 1
            log.info("--- CYCLE %d ---", cycle)
            try:
                run_cycle(db)
            except Exception as exc:
                log.error("Cycle %d failed: %s", cycle, exc)
            log.info("Sleeping %d minutes...", args.interval)
            time.sleep(interval_sec)
    else:
        log.info("Single-shot mode.")
        run_cycle(db)
        log.info("Done.")


if __name__ == "__main__":
    main()


# =============================================================================
# USAGE
# =============================================================================
#
# Collection flow per run:
#   RUN N-1:  data written to  chn_mismatch_update_counts  (staging)
#
#   RUN N (current):
#     1. chn_mismatch_update_counts  →  chn_mismatch_counts   (archived)
#     2. Fresh data                  →  chn_mismatch_update_counts  (staged)
#
#   chn_mismatch_update_counts  = latest run only  (current snapshot)
#   chn_mismatch_counts         = full historical archive
#
# Single run:
#   python chn_data_delay.py
#
# Loop mode (every 16 min):
#   python chn_data_delay.py --loop
#
# Cron (MobaXterm, fires at :00, :16, :32, :48):
#   */16 * * * * /usr/bin/python3 /path/to/chn_data_delay.py >> /path/to/chn_data_delay.log 2>&1
#
# MongoDB — example document in chn_mismatch_update_counts / chn_mismatch_counts:
# {
#   "run_time_ist":     ISODate("2026-07-12T17:22:38Z"),
#   "window_start_ist": ISODate("2026-07-12T17:06:38Z"),
#   "window_end_ist":   ISODate("2026-07-12T17:22:38Z"),
#   "window_start_utc": ISODate("2026-07-12T11:36:38Z"),
#   "window_end_utc":   ISODate("2026-07-12T11:52:38Z"),
#   "interval_minutes": 16,
#   "query_name":       "tub_not_in_tbm",
#   "description":      "Present in TUB, not in TBM (last 16 min)",
#   "TUB":              30,          <- total TUB barcodes in window
#   "TBM_matched":      22,          <- TUB barcodes FOUND in TBM
#   "TBM_not_matched":   8,          <- TUB barcodes NOT in TBM
#   "mis_match_count":   8,          <- same as TBM_not_matched
#   "query_error":      false
# }
# =============================================================================
