"""
logs_delete.py
--------------
Deletes log files from the logs folder that are older than 2 days.
The date is extracted directly from the filename (e.g. log_alerts_2026-10-20.txt).
Only files whose filename date is within the last 2 days are kept.

Usage:
    python logs_delete.py              # deletes logs older than 2 days
    python logs_delete.py --dry-run    # preview what would be deleted (no actual deletion)
"""

import os
import re
import sys
import logging
from datetime import datetime, timedelta
from typing import Optional

# ── Config ────────────────────────────────────────────────────────────────────
LOGS_FOLDER = r"C:\Users\ravik\Downloads\Hungary (2)\Hungary\logs"
KEEP_DAYS = 2  # keep files whose filename date is within the last 2 days

# Regex to extract a YYYY-MM-DD date from a log filename.
# Matches patterns like:
#   log_alerts_2026-10-20.txt
#   log_alerts_repl_2026-10-22.txt
#   log_historical_uat_2026-10-19.txt
DATE_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2})")

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def extract_date_from_filename(filename: str) -> Optional[datetime]:
    """
    Extract and return the date embedded in a log filename.
    Returns None if no YYYY-MM-DD date is found.
    """
    match = DATE_PATTERN.search(filename)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y-%m-%d")
    except ValueError:
        return None


def delete_old_logs(logs_folder: str, keep_days: int, dry_run: bool = False) -> None:
    """
    Delete all log files inside `logs_folder` whose filename date is older
    than `keep_days` days. Files with no parseable date in their name are
    skipped (kept) so they are never accidentally removed.

    Parameters
    ----------
    logs_folder : str
        Absolute path to the logs directory.
    keep_days : int
        Number of most-recent days to retain. Files dated within this
        window are kept; everything older is deleted.
    dry_run : bool
        When True, only print what would be deleted — nothing is removed.
    """
    if not os.path.isdir(logs_folder):
        logging.error(f"Logs folder not found: {logs_folder}")
        return

    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    cutoff_date = today - timedelta(days=keep_days)

    logging.info(f"Logs folder  : {logs_folder}")
    logging.info(f"Keep last    : {keep_days} days")
    logging.info(f"Today        : {today.strftime('%Y-%m-%d')}")
    logging.info(f"Cutoff date  : {cutoff_date.strftime('%Y-%m-%d')} (files dated before this are deleted)")
    logging.info(f"Dry run      : {dry_run}")
    logging.info("-" * 60)

    deleted_count = 0
    kept_count = 0
    skipped_count = 0
    error_count = 0

    for filename in sorted(os.listdir(logs_folder)):
        filepath = os.path.join(logs_folder, filename)

        # Skip subdirectories — only process files.
        if not os.path.isfile(filepath):
            continue

        file_date = extract_date_from_filename(filename)

        # No date found in filename — skip safely.
        if file_date is None:
            logging.warning(f"  Skipped : {filename}  (no date found in filename)")
            skipped_count += 1
            continue

        if file_date < cutoff_date:
            # File date is older than the cutoff — delete it.
            if dry_run:
                logging.info(f"  [DRY-RUN] Would delete : {filename}  (file date: {file_date.strftime('%Y-%m-%d')})")
            else:
                try:
                    os.remove(filepath)
                    logging.info(f"  Deleted : {filename}  (file date: {file_date.strftime('%Y-%m-%d')})")
                    deleted_count += 1
                except Exception as e:
                    logging.error(f"  ERROR deleting {filename}: {e}")
                    error_count += 1
        else:
            # File date is within the keep window — leave it.
            logging.info(f"  Kept    : {filename}  (file date: {file_date.strftime('%Y-%m-%d')})")
            kept_count += 1

    logging.info("-" * 60)
    if dry_run:
        logging.info(
            f"Dry-run complete. No files were deleted. "
            f"Would delete: {deleted_count + error_count} | "
            f"Would keep: {kept_count} | Skipped: {skipped_count}"
        )
    else:
        logging.info(
            f"Done. Deleted: {deleted_count} file(s) | "
            f"Kept: {kept_count} file(s) | "
            f"Skipped: {skipped_count} | "
            f"Errors: {error_count}"
        )


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    delete_old_logs(LOGS_FOLDER, KEEP_DAYS, dry_run=dry_run)
