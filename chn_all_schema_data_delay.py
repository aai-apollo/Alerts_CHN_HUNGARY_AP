"""
Redshift Table Last-Record Monitor
====================================
Designed to run as a cron job every 15 minutes (or manually).
Each execution does ONE check and saves results to MongoDB.

MongoDB Collections:
  - redshift_updates       : holds ONLY the most recent run's data (staging)
  - redshift_checks        : accumulates all historical run data
  - redshift_no_permission : tables the user cannot SELECT from

Flow per run:
  1. Move all documents from redshift_updates  →  redshift_checks  (previous run archived)
  2. Insert fresh data from this run           →  redshift_updates  (latest run staged)

Manual run:
    python chn_all_schema_data_delay.py

Cron setup (every 15 minutes):
    */15 * * * * /path/to/python /path/to/chn_all_schema_data_delay.py >> /path/to/chn_all_schema_data_delay.log 2>&1
"""

import sys
import ast
import csv
import time
import datetime
import logging
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Optional

try:
    pass  # openpyxl removed — Excel export disabled
except ImportError:
    pass

# ---------------------------------------------------------------------------
# LOGGING  (stdout + file, so cron output is captured)
# ---------------------------------------------------------------------------
LOG_FILE = Path(__file__).resolve().parent / "chn_all_schema_data_delay.log"
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
    from pymongo import MongoClient, UpdateOne
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
    "uri":      "mongodb://admin:sdfda%40wdfsdfvsdg32FGSFD@10.100.192.96:27017/",   # update credentials if wrong
    "database": "redshift_monitor",
}

CSV_FILE_PATH            = Path(__file__).resolve().parent / "columns_202607081806.csv"  # optional fallback only
CHECK_INTERVAL_MINUTES   = 15   # default interval for --loop mode
TARGET_FRESHNESS_MINUTES = 15

IST        = ZoneInfo("Asia/Kolkata")
UTC        = datetime.timezone.utc
IST_OFFSET = datetime.timedelta(hours=5, minutes=30)

PREFERRED_TS_COLS = ["updated_at", "created_at", "timestamp"]

# MongoDB collection names
COL_UPDATES   = "redshift_updates"         # latest run — staging (replaced every run)
COL_CHECKS    = "redshift_checks"          # all historical runs — archive
COL_NO_PERM   = "redshift_no_permission"   # permission-denied tables


# ---------------------------------------------------------------------------
# TABLE DISCOVERY  — reads directly from Redshift information_schema
# ---------------------------------------------------------------------------

def fetch_timestamp_tables_from_redshift(conn) -> dict:
    """
    Query information_schema.columns to find every table that has at least
    one timestamp column. Returns the same dict structure that the old CSV
    parser returned.

    Uses the Redshift database specified in REDSHIFT_CONFIG so it always
    reflects the live schema — no CSV file needed.
    """
    sql = """
        SELECT
            table_schema,
            table_name,
            column_name,
            data_type
        FROM information_schema.columns
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast',
                                   'pg_temp_1', 'pg_toast_temp_1')
          AND data_type ILIKE '%timestamp%'
        ORDER BY table_schema, table_name, column_name
    """
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
    except Exception as exc:
        log.error("Failed to query information_schema.columns: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return {}

    bucket = {}
    for (schema, table, col, _dtype) in rows:
        schema, table, col = schema.strip(), table.strip(), col.strip()
        bucket.setdefault((schema, table), []).append(col)

    tables = {}
    for (schema, table), ts_cols in bucket.items():
        best = next((p for p in PREFERRED_TS_COLS if p in ts_cols), ts_cols[0])
        tables[f"{schema}.{table}"] = dict(
            schema=schema, table=table,
            timestamp_column=best, all_ts_columns=ts_cols,
            timezone=None,
            last_raw_ts=None, last_utc_ts=None, last_ist_ts=None,
            last_delay_min=None,
            perm_denied=False, perm_error=None,
        )

    log.info("Discovered %d tables with timestamp columns from information_schema.", len(tables))
    return tables


def parse_csv_for_timestamp_tables(csv_path: str) -> dict:
    """
    FALLBACK: read from the local CSV if it is a valid columns-metadata file.
    Expected columns: table_schema, table_name, column_name, data_type
    Returns empty dict if the file is missing or has wrong format.
    """
    path = Path(csv_path)
    if not path.exists():
        return {}

    bucket = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            # Validate that this is actually a columns-metadata CSV
            if not reader.fieldnames or "table_schema" not in (reader.fieldnames or []):
                log.warning("CSV file does not look like a columns-metadata file — skipping.")
                return {}
            for row in reader:
                if "timestamp" in row.get("data_type", "").strip().lower():
                    schema = row["table_schema"].strip()
                    table  = row["table_name"].strip()
                    col    = row["column_name"].strip()
                    bucket.setdefault((schema, table), []).append(col)
    except Exception as exc:
        log.warning("Could not read CSV (%s): %s — will use Redshift live query.", csv_path, exc)
        return {}

    tables = {}
    for (schema, table), ts_cols in bucket.items():
        best = next((p for p in PREFERRED_TS_COLS if p in ts_cols), ts_cols[0])
        tables[f"{schema}.{table}"] = dict(
            schema=schema, table=table,
            timestamp_column=best, all_ts_columns=ts_cols,
            timezone=None,
            last_raw_ts=None, last_utc_ts=None, last_ist_ts=None,
            last_delay_min=None,
            perm_denied=False, perm_error=None,
        )
    return tables


# ---------------------------------------------------------------------------
# REDSHIFT HELPERS
# ---------------------------------------------------------------------------

def get_redshift_connection():
    return redshift_connector.connect(**REDSHIFT_CONFIG)


def is_connection_dead(err_str: str) -> bool:
    msg = err_str.lower()
    return any(x in msg for x in (
        "10054", "10053", "winerror", "broken pipe",
        "connection reset", "connection closed",
        "server closed the connection unexpectedly",
    ))


def get_redshift_utc_now(conn) -> datetime.datetime:
    """Ask Redshift for its own UTC time. Falls back to local UTC."""
    try:
        cur = conn.cursor()
        cur.execute("SELECT CONVERT_TIMEZONE('UTC', GETDATE())")
        row = cur.fetchone()
        cur.close()
        if row and row[0] is not None:
            ts = row[0]
            if hasattr(ts, "tzinfo") and ts.tzinfo is not None:
                return ts.astimezone(UTC).replace(tzinfo=None)
            return ts
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        log.warning("Cannot fetch Redshift server time (%s). Using local UTC.", exc)
    return datetime.datetime.now(UTC).replace(tzinfo=None)


def fetch_max_timestamp(conn, schema: str, table: str, ts_col: str):
    """Returns (raw_datetime | None, error_str | None)."""
    sql = f'SELECT MAX("{ts_col}") FROM "{schema}"."{table}"'
    try:
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        cur.close()
        return (row[0], None) if (row and row[0] is not None) else (None, None)
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        return None, str(exc)


def fetch_new_record_count(conn, schema: str, table: str, ts_col: str,
                           since_ist: datetime.datetime, tz_label: str) -> int:
    """
    COUNT(*) where ts_col >= since_time.

    since_ist : IST-aware datetime (the previous check run time)
    tz_label  : "IST" or "UTC" — how the column value is stored in Redshift

    The comparison value must match the column's stored timezone:
      - UTC column : compare against UTC naive  (since_ist → UTC → strip tz)
      - IST column : compare against IST naive  (since_ist           → strip tz)

    Returns count (>= 0) on success, -1 on any error.
    """
    # Derive the correct naive datetime for comparison
    if tz_label == "IST":
        since_naive = since_ist.replace(tzinfo=None) if since_ist.tzinfo else since_ist
    else:
        # Column stores UTC — convert the IST reference time to UTC naive
        since_aware = since_ist if since_ist.tzinfo else since_ist.replace(tzinfo=IST)
        since_naive = since_aware.astimezone(UTC).replace(tzinfo=None)

    sql = (
        f'SELECT COUNT(*) FROM "{schema}"."{table}" '
        f'WHERE "{ts_col}" >= \'{since_naive.strftime("%Y-%m-%d %H:%M:%S")}\''
    )
    try:
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        cur.close()
        return int(row[0]) if row and row[0] is not None else 0
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        log.warning("COUNT query failed for %s.%s: %s", schema, table, exc)
        return -1


def parse_redshift_error(err_str: str):
    """Returns (code, message) from Redshift's dict-style error string."""
    try:
        d = ast.literal_eval(err_str) if err_str.strip().startswith("{") else {}
        if d:
            return d.get("C", "?"), d.get("M", err_str)
    except Exception:
        pass
    for code in ("42501", "25P02", "42P01", "28000"):
        if code in err_str:
            return code, err_str
    return "?", err_str


# ---------------------------------------------------------------------------
# TIMEZONE DETECTION
# ---------------------------------------------------------------------------

def detect_timezone(raw_ts: datetime.datetime, rs_now_utc_naive: datetime.datetime) -> str:
    """
    Decide if a naive Redshift timestamp is UTC or IST by comparing against
    the Redshift server's own UTC time. Whichever interpretation gives a
    smaller non-negative delay wins.
    """
    if raw_ts.tzinfo is not None:
        return "UTC"

    SKEW = -5.0
    delay_utc = (rs_now_utc_naive - raw_ts).total_seconds() / 60.0
    delay_ist = (rs_now_utc_naive - (raw_ts - IST_OFFSET)).total_seconds() / 60.0

    utc_ok, ist_ok = delay_utc >= SKEW, delay_ist >= SKEW
    if utc_ok and ist_ok:
        return "UTC" if delay_utc <= delay_ist else "IST"
    if utc_ok:
        return "UTC"
    if ist_ok:
        return "IST"
    return "IST"   # default: ap-south-1 cluster


def normalise_to_utc(raw_ts: datetime.datetime, tz_label: str) -> datetime.datetime:
    if raw_ts.tzinfo is not None:
        return raw_ts.astimezone(UTC)
    if tz_label == "IST":
        return raw_ts.replace(tzinfo=IST).astimezone(UTC)
    return raw_ts.replace(tzinfo=UTC)


# ---------------------------------------------------------------------------
# MONGODB HELPERS
# ---------------------------------------------------------------------------

def get_mongo_db():
    client = MongoClient(MONGO_CONFIG["uri"], serverSelectionTimeoutMS=5000)
    return client[MONGO_CONFIG["database"]]


def ensure_indexes(db):
    """Create indexes once — safe to call repeatedly (no-op if already exist)."""
    # redshift_updates — staging collection (latest run only)
    db[COL_UPDATES].create_index(
        [("schema_name", 1), ("table_name", 1)]
    )
    db[COL_UPDATES].create_index([("check_time_ist", -1)])
    # redshift_checks — historical archive
    db[COL_CHECKS].create_index(
        [("schema_name", 1), ("table_name", 1), ("check_time_ist", -1)]
    )
    db[COL_CHECKS].create_index([("check_time_ist", -1)])
    # permission-denied
    db[COL_NO_PERM].create_index(
        [("schema_name", 1), ("table_name", 1)], unique=True
    )


def rotate_updates_to_checks(db):
    """
    Move all documents currently in redshift_updates → redshift_checks.
    This archives the previous run before the new one is written.
    Called at the START of every run cycle.
    """
    docs = list(db[COL_UPDATES].find({}, {"_id": 0}))   # fetch all, exclude _id
    if not docs:
        log.info("[ROTATE] redshift_updates is empty — nothing to archive.")
        return 0

    try:
        db[COL_CHECKS].insert_many(docs, ordered=False)
        db[COL_UPDATES].delete_many({})
        log.info(
            "[ROTATE] Moved %d docs from '%s' → '%s'.",
            len(docs), COL_UPDATES, COL_CHECKS,
        )
        return len(docs)
    except PyMongoError as exc:
        log.error("[ROTATE] Failed to rotate: %s", exc)
        return 0


def get_last_check_time(db) -> Optional[datetime.datetime]:
    """
    Retrieve the most recent check_time_ist.
    Checks redshift_updates first (most recent run), then redshift_checks.
    """
    for col_name in (COL_UPDATES, COL_CHECKS):
        doc = db[col_name].find_one(
            {"check_time_ist": {"$exists": True}},
            sort=[("check_time_ist", -1)],
        )
        if doc and doc.get("check_time_ist"):
            ts = doc["check_time_ist"]
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)
            return ts.astimezone(IST)
    return None


def save_check_to_mongo(db, now_ist: datetime.datetime, tables: dict,
                        prev_check_ist: Optional[datetime.datetime]):
    """
    Insert this run's data into redshift_updates (staging).
    redshift_updates was already cleared by rotate_updates_to_checks().
    Permission-denied tables are upserted into redshift_no_permission.
    """
    updates_docs = []
    perm_ops     = []

    for tname, info in tables.items():

        if info.get("perm_denied"):
            perm_ops.append(UpdateOne(
                {"schema_name": info["schema"], "table_name": info["table"]},
                {"$set": {
                    "schema_name":      info["schema"],
                    "table_name":       info["table"],
                    "timestamp_column": info["timestamp_column"],
                    "error_message":    info.get("perm_error", "permission denied"),
                    "first_seen_ist":   now_ist,
                    "last_seen_ist":    now_ist,
                }},
                upsert=True,
            ))
            continue

        if info["last_ist_ts"] is None:
            updates_docs.append({
                "schema_name":       info["schema"],
                "table_name":        info["table"],
                "table_key":         tname,
                "timestamp_column":  info["timestamp_column"],
                "detected_timezone": info.get("timezone") or "Unknown",
                "check_time_ist":    now_ist,
                "check_time_utc":    now_ist.astimezone(UTC),
                "last_record_ist":   None,
                "last_record_utc":   None,
                "delay_minutes":     None,
                "new_record_count":  0,
                "status":            "No Data",
                "data_flag":         False,
            })
            continue

        delay = info["last_delay_min"]
        if delay is None:
            status = "Unknown"
        elif delay <= TARGET_FRESHNESS_MINUTES:
            status = "OK"
        elif delay <= TARGET_FRESHNESS_MINUTES * 2:
            status = "Moderate"
        else:
            status = "STALE"

        updates_docs.append({
            "schema_name":       info["schema"],
            "table_name":        info["table"],
            "table_key":         tname,
            "timestamp_column":  info["timestamp_column"],
            "detected_timezone": info.get("timezone") or "Unknown",
            "check_time_ist":    now_ist,
            "check_time_utc":    now_ist.astimezone(UTC),
            "last_record_ist":   info["last_ist_ts"],
            "last_record_utc":   info["last_utc_ts"],
            "delay_minutes":     round(delay, 2) if delay is not None else None,
            "new_record_count":  info.get("_new_count_this_run", 0),
            "status":            status,
            "data_flag":         False,
        })

    inserted = 0
    if updates_docs:
        try:
            db[COL_UPDATES].insert_many(updates_docs, ordered=False)
            inserted = len(updates_docs)
            log.info(
                "Inserted %d docs into '%s' (staging).", inserted, COL_UPDATES
            )
        except PyMongoError as exc:
            log.error("insert_many into '%s' failed: %s", COL_UPDATES, exc)

    if perm_ops:
        try:
            db[COL_NO_PERM].bulk_write(perm_ops, ordered=False)
        except PyMongoError as exc:
            log.error("bulk_write (no-perm) failed: %s", exc)

    return inserted


# ---------------------------------------------------------------------------
# SINGLE CHECK  (one execution = one check)
# ---------------------------------------------------------------------------

def run_one_check(tables: dict, prev_check_ist: Optional[datetime.datetime]):
    """
    Connect to Redshift, fetch MAX(ts_col) + COUNT for every table.
    Stores timezone + last timestamps in-place on each table dict.
    Returns now_ist (IST-aware datetime of this run).
    """
    now_utc = datetime.datetime.now(UTC)
    now_ist = now_utc.astimezone(IST)

    log.info("=" * 65)
    log.info("  REDSHIFT CHECK  |  %s IST  |  %s UTC",
             now_ist.strftime("%Y-%m-%d %H:%M:%S"),
             now_utc.strftime("%H:%M:%S"))
    log.info("=" * 65)

    conn_box = [None]

    try:
        log.info("Connecting to Redshift...")
        conn_box[0] = get_redshift_connection()
        log.info("Connected.")

        rs_now = get_redshift_utc_now(conn_box[0])
        log.info("Redshift UTC now: %s", rs_now.strftime("%Y-%m-%d %H:%M:%S"))

        for tname, info in tables.items():

            if info.get("perm_denied"):
                continue

            # ── MAX(ts_col) ───────────────────────────────────────────────
            raw_ts, err = fetch_max_timestamp(
                conn_box[0], info["schema"], info["table"], info["timestamp_column"]
            )

            # reconnect on dead socket
            if err and is_connection_dead(err):
                log.warning("[RECONNECT] %s — reconnecting...", tname)
                try:
                    conn_box[0].close()
                except Exception:
                    pass
                try:
                    conn_box[0] = get_redshift_connection()
                    raw_ts, err = fetch_max_timestamp(
                        conn_box[0], info["schema"], info["table"], info["timestamp_column"]
                    )
                except Exception as re:
                    err = str(re)

            if err:
                code, msg = parse_redshift_error(err)
                if code == "42501" or "permission denied" in msg.lower():
                    info["perm_denied"] = True
                    info["perm_error"]  = msg
                    log.warning("[SKIP] %s: permission denied", tname)
                else:
                    log.error("[ERR ] %s: [%s] %s", tname, code, msg)
                continue

            if raw_ts is None:
                log.info("[NULL] %s: MAX() returned NULL — empty table", tname)
                continue

            info["last_raw_ts"] = raw_ts

            # ── timezone detection (once per table, reuse after) ──────────
            if info["timezone"] is None:
                info["timezone"] = detect_timezone(raw_ts, rs_now)
                log.info("[TZ  ] %s -> %s", tname, info["timezone"])

            tz = info["timezone"]

            ts_utc = normalise_to_utc(raw_ts, tz)
            ts_ist = ts_utc.astimezone(IST)

            info["last_utc_ts"] = ts_utc
            info["last_ist_ts"] = ts_ist

            # ── delay in IST ──────────────────────────────────────────────
            delay_min = (now_ist - ts_ist).total_seconds() / 60.0
            info["last_delay_min"] = round(delay_min, 2)

            # ── new record count since last cron run ──────────────────────
            if prev_check_ist is not None:
                count = fetch_new_record_count(
                    conn_box[0], info["schema"], info["table"],
                    info["timestamp_column"], prev_check_ist, tz
                )
                # -1 means the COUNT query itself errored — store 0
                info["_new_count_this_run"] = count if count >= 0 else 0
            else:
                # First ever run — no previous baseline, count as 0
                info["_new_count_this_run"] = 0

            h, m = int(delay_min // 60), int(abs(delay_min) % 60)
            cnt  = info["_new_count_this_run"]
            cnt_str = f"{cnt} new rows" if cnt is not None else "first run (no baseline yet)"
            log.info(
                "[OK  ] %-50s  tz=%-3s  last_ist=%s  delay=%dh%dm  %s",
                tname, tz,
                ts_ist.strftime("%Y-%m-%d %H:%M:%S"),
                h, m, cnt_str,
            )

    except Exception as exc:
        log.error("[FATAL] Redshift check failed: %s", exc)
    finally:
        if conn_box[0]:
            try:
                conn_box[0].close()
            except Exception:
                pass

    return now_ist


# ---------------------------------------------------------------------------
# MAIN  — single-shot execution (cron calls this every 15 min)
# ---------------------------------------------------------------------------


def run_single_cycle(db):
    """
    One complete check cycle:
      1. Archive previous run: move redshift_updates → redshift_checks
      2. Discover tables from Redshift information_schema
      3. Fetch MAX(ts) + COUNT for every table
      4. Insert fresh data into redshift_updates
    """
    # ── Step 1: archive previous run ──────────────────────────────────────
    log.info("Step 1: Archiving previous run data...")
    rotate_updates_to_checks(db)

    # ── Step 2: connect to Redshift for schema discovery ──────────────────
    try:
        discovery_conn = get_redshift_connection()
    except Exception as e:
        log.error("Cannot connect to Redshift: %s", e)
        return 0, []

    tables = fetch_timestamp_tables_from_redshift(discovery_conn)

    if not tables:
        log.warning("Live schema query returned 0 tables. Trying CSV fallback...")
        tables = parse_csv_for_timestamp_tables(str(CSV_FILE_PATH))

    try:
        discovery_conn.close()
    except Exception:
        pass

    if not tables:
        log.error("No timestamp tables found. Skipping this cycle.")
        return 0, []

    log.info("Tables to monitor: %d", len(tables))

    # ── Step 3: get previous check time for new-record counting ───────────
    # After rotation, redshift_updates is empty and previous run is in
    # redshift_checks — get_last_check_time reads from redshift_checks.
    prev_check_ist = get_last_check_time(db)
    if prev_check_ist:
        log.info("Previous check: %s IST", prev_check_ist.strftime("%Y-%m-%d %H:%M:%S"))
    else:
        log.info("First ever run — no previous check in MongoDB.")

    # ── Step 4: run the check ─────────────────────────────────────────────
    now_ist = run_one_check(tables, prev_check_ist)

    # ── Step 5: insert fresh data into redshift_updates ───────────────────
    inserted = save_check_to_mongo(db, now_ist, tables, prev_check_ist)
    denied   = [n for n, i in tables.items() if i.get("perm_denied")]

    if denied:
        log.info("%d permission-denied tables saved to '%s'.", len(denied), COL_NO_PERM)

    return inserted, denied


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Redshift Table Last-Record Monitor")
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Run continuously every 15 minutes (default: single-shot for cron)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=CHECK_INTERVAL_MINUTES,
        help=f"Interval in minutes between checks when using --loop (default: {CHECK_INTERVAL_MINUTES})",
    )
    args = parser.parse_args()

    # ── connect to MongoDB (once, shared across all cycles) ───────────────
    try:
        db = get_mongo_db()
        ensure_indexes(db)
        log.info("MongoDB connected: %s / %s", MONGO_CONFIG["uri"], MONGO_CONFIG["database"])
    except PyMongoError as e:
        log.error("Cannot connect to MongoDB: %s", e)
        sys.exit(1)

    if args.loop:
        # ── LOOP MODE: run forever, sleep between cycles ──────────────────
        interval_sec = args.interval * 60
        log.info("Loop mode started — running every %d minutes. Press Ctrl+C to stop.",
                 args.interval)
        cycle = 0
        while True:
            cycle += 1
            log.info("=" * 65)
            log.info("  CYCLE %d  (loop mode, interval=%d min)", cycle, args.interval)
            log.info("=" * 65)
            try:
                run_single_cycle(db)
            except Exception as exc:
                log.error("Cycle %d failed unexpectedly: %s", cycle, exc)

            log.info("Cycle %d complete. Sleeping %d minutes until next run...",
                     cycle, args.interval)
            log.info("-" * 65)
            time.sleep(interval_sec)

    else:
        # ── SINGLE-SHOT MODE: one check and exit (used by cron) ───────────
        log.info("Single-shot mode started.")
        run_single_cycle(db)
        log.info("Done — process exiting (cron will restart at next scheduled time).")


if __name__ == "__main__":
    main()


# =============================================================================
# HOW TO RUN
# =============================================================================
#
# ── Collection flow per run ───────────────────────────────────────────────────
#
#   RUN N-1:  data written to  redshift_updates
#
#   RUN N (current):
#     1. redshift_updates  →  redshift_checks   (previous run archived)
#     2. Fresh data        →  redshift_updates   (current run staged)
#
#   redshift_updates  = always the LATEST run only  (current snapshot)
#   redshift_checks   = full historical archive of all past runs
#
# ── Option A: Loop mode (keep process running, never exits) ──────────────────
#   python3 chn_all_schema_data_delay.py --loop
#   python3 chn_all_schema_data_delay.py --loop --interval 15
#
# ── Option B: Cron mode (single-shot, exits after each run) ──────────────────
#   python3 chn_all_schema_data_delay.py        # manual single run
#
#   Crontab (fires at :00, :15, :30, :45 every hour):
#     crontab -e
#     */15 * * * * /usr/bin/python3 /home/ec2-user/Alerts/v1/chn_all_schema_data_delay.py >> /home/ec2-user/Alerts/v1/chn_all_schema_data_delay.log 2>&1
#
#   Verify cron is running:
#     ps aux | grep cron
#     cron       # start if needed
#
#   Watch live logs:
#     tail -f /home/ec2-user/Alerts/v1/chn_all_schema_data_delay.log
#
# ── MongoDB queries ───────────────────────────────────────────────────────────
#
#   Current snapshot (latest run only):
#     db.redshift_updates.find().sort({check_time_ist:-1}).limit(20)
#
#   Full history for one table:
#     db.redshift_checks.find({table_key:"sc_cimcorp_pcr.curing"}).sort({check_time_ist:-1})
#
#   STALE tables in latest run:
#     db.redshift_updates.find({status:"STALE"})
#
#   STALE tables across all history:
#     db.redshift_checks.find({status:"STALE"}).sort({check_time_ist:-1})
#
#   Tables with no SELECT permission:
#     db.redshift_no_permission.find()
#
#   New records added in latest run:
#     db.redshift_updates.find({new_record_count:{$gt:0}}).sort({new_record_count:-1})
#
#   Count of docs archived in redshift_checks:
#     db.redshift_checks.countDocuments()
# =============================================================================
