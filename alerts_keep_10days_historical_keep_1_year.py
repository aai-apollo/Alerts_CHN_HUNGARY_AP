"""
alerts_keep_10days_historical_keep_1_year.py
============================================
Retention policy for merged tyre-data collections.

  LIVE collections  (TUB_CIMCORP_merged_cleaned_data)  → keep latest 10 days
  HISTORICAL collections (TUB_CIMCORP_merged_cleaned_data_historical_data) → keep latest 1 year

  The cut-off is based on the "date_and_time" field.
  Format expected:  "2026-10-01 07:04:28"  (string, parsed as datetime)

MongoDB connection comes from config.py (MONGO_URI / client).

Run:
    python alerts_keep_10days_historical_keep_1_year.py
"""

import logging
import os
import sys
from datetime import datetime, timedelta

import pytz
from pymongo import MongoClient

from config import (
    client,
    TUB_CIMCORP_DB,          # "TUB_CIMCORP_merged_cleaned_data"
    TUB_CIMCORP_DB_history,   # "TUB_CIMCORP_merged_cleaned_data_historical_data"
    inner_join_collection_name,          # "data_mereged_dashboard"
    tub_collection_name,                 # "data_mereged_tub"
    new_inner_join_collection_collection, # "new_data_mereged_dashboard"
    new_tub_collection_collection,        # "new_data_mereged_tub"
    inner_join_collection_name_history,  # "data_mereged_dashboard_historical_data"
    tub_collection_name_history,         # "data_mereged_tub_historical_data"
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def configure_logging():
    if not os.path.exists("logs"):
        os.makedirs("logs")
    current_date = datetime.now().strftime("%Y-%m-%d")
    log_filename = f"logs/log_retention_{current_date}.txt"
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_filename, mode="a", encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


# ---------------------------------------------------------------------------
# Retention configuration
# ---------------------------------------------------------------------------

# Cutoff field name and its string format stored in MongoDB
DATE_FIELD  = "date_and_time"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"      # matches "2026-10-01 07:04:28"

# How many days / years to keep
KEEP_LIVE_DAYS        = 10
KEEP_HISTORICAL_YEARS = 1

# Collections to clean
LIVE_COLLECTIONS = [
    inner_join_collection_name,            # "data_mereged_dashboard"
    tub_collection_name,                   # "data_mereged_tub"
    new_inner_join_collection_collection,  # "new_data_mereged_dashboard"
    new_tub_collection_collection,         # "new_data_mereged_tub"
]

HISTORICAL_COLLECTIONS = [
    inner_join_collection_name_history,    # "data_mereged_dashboard_historical_data"
    tub_collection_name_history,           # "data_mereged_tub_historical_data"
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_ist_now() -> datetime:
    """Return the current time in IST (naive)."""
    ist = pytz.timezone("Asia/Kolkata")
    return datetime.now(ist).replace(tzinfo=None)


def build_cutoff_filter(cutoff_dt: datetime) -> dict:
    """
    Build a MongoDB filter that matches documents whose date_and_time
    is OLDER than cutoff_dt.

    The field is stored as a string "YYYY-MM-DD HH:MM:SS".
    String comparison on ISO-format timestamps is lexicographically correct,
    so a simple $lt on the formatted string works reliably.
    """
    cutoff_str = cutoff_dt.strftime(DATE_FORMAT)
    return {DATE_FIELD: {"$lt": cutoff_str}}


def purge_collection(db, collection_name: str, cutoff_dt: datetime, label: str) -> int:
    """
    Delete all documents in db[collection_name] whose date_and_time < cutoff_dt.
    Returns the number of deleted documents.
    """
    coll   = db[collection_name]
    total  = coll.count_documents({})
    filt   = build_cutoff_filter(cutoff_dt)
    old    = coll.count_documents(filt)

    if old == 0:
        logging.info(
            "[%s] %s.%s — %d total docs, 0 to delete (all within retention window).",
            label, db.name, collection_name, total
        )
        return 0

    result = coll.delete_many(filt)
    deleted = result.deleted_count
    remaining = total - deleted
    logging.info(
        "[%s] %s.%s — deleted %d old doc(s) (cutoff: %s). Remaining: %d.",
        label, db.name, collection_name,
        deleted, cutoff_dt.strftime(DATE_FORMAT), remaining
    )
    return deleted


# ---------------------------------------------------------------------------
# Main retention jobs
# ---------------------------------------------------------------------------

def purge_live(now: datetime) -> None:
    """
    LIVE database: keep only records from the last 10 days.
    Deletes any record where date_and_time < (now - 10 days).
    """
    cutoff = now - timedelta(days=KEEP_LIVE_DAYS)
    logging.info(
        "=== LIVE retention: keep last %d days (cutoff: %s) ===",
        KEEP_LIVE_DAYS, cutoff.strftime(DATE_FORMAT)
    )
    db = client[TUB_CIMCORP_DB]
    total_deleted = 0
    for coll_name in LIVE_COLLECTIONS:
        total_deleted += purge_collection(db, coll_name, cutoff, "LIVE-10d")
    logging.info(
        "=== LIVE purge complete. Total deleted: %d ===", total_deleted
    )


def purge_historical(now: datetime) -> None:
    """
    HISTORICAL database: keep only records from the last 1 year.
    Deletes any record where date_and_time < (now - 365 days).
    """
    cutoff = now - timedelta(days=365 * KEEP_HISTORICAL_YEARS)
    logging.info(
        "=== HISTORICAL retention: keep last %d year(s) (cutoff: %s) ===",
        KEEP_HISTORICAL_YEARS, cutoff.strftime(DATE_FORMAT)
    )
    db = client[TUB_CIMCORP_DB_history]
    total_deleted = 0
    for coll_name in HISTORICAL_COLLECTIONS:
        total_deleted += purge_collection(db, coll_name, cutoff, "HIST-1yr")
    logging.info(
        "=== HISTORICAL purge complete. Total deleted: %d ===", total_deleted
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    configure_logging()
    logging.info("Retention job started.")

    now = get_ist_now()
    logging.info("Current IST time: %s", now.strftime(DATE_FORMAT))

    try:
        purge_live(now)
    except Exception as e:
        logging.error("LIVE purge failed: %s", e, exc_info=True)

    try:
        purge_historical(now)
    except Exception as e:
        logging.error("HISTORICAL purge failed: %s", e, exc_info=True)

    try:
        client.close()
    except Exception:
        pass

    logging.info("Retention job finished.")


if __name__ == "__main__":
    main()
