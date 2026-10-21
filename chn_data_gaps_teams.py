"""
chn_data_gaps_teams.py
======================
Reads records from MongoDB  chn_mismatch_update_counts  where  data_flag = False,
sends them to Microsoft Teams via Power Automate webhook, then marks data_flag = True.

The payload sent to Power Automate is a simple JSON body with a "text" field
containing an HTML table — Power Automate passes this directly to Teams.

Run:
    python chn_data_gaps_teams.py           # single-shot
    python chn_data_gaps_teams.py --loop    # run every N minutes
"""

import sys
import time
import logging
import datetime
import argparse
import requests
from zoneinfo import ZoneInfo
from pymongo import MongoClient
from pymongo.errors import PyMongoError

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
LOG_FILE = "chn_data_gaps_teams.log"
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
# CONFIGURATION
# ---------------------------------------------------------------------------

MONGO_CONFIG = {
    "uri":      "mongodb://admin:sdfda%40wdfsdfvsdg32FGSFD@10.100.192.96:27017/",
    # "uri":      "mongodb://localhost:27017/",
    "database": "redshift_monitor",
}
COLLECTION = "chn_mismatch_update_counts"

# TEAMS_WEBHOOK_URL = (
#     "https://defaultf792002939f941b79c94ec73395d18.4d.environment.api.powerplatform.com:443"
#     "/powerautomate/automations/direct/workflows/67fc14590d7f465e95e10d0d4d516677"
#     "/triggers/manual/paths/invoke"
#     "?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0"
#     "&sig=RZrtfWzs6Dvy19O1W7uAJ1i-5JFRxzBrXX3PKH--6Jk"
# )

TEAMS_WEBHOOK_URL = ("https://defaultf792002939f941b79c94ec73395d18.4d.environment.api.powerplatform.com:443/powerautomate/automations/direct/cu/01/workflows/3e46cd5f7c2b494cbf7c5b5a965034c7/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=svpggRbVtqmYTYv6hhtOzku2LIWTZcOkYmBUvBbEbUE")

# TEAMS_WEBHOOK_URL = ("https://defaultf792002939f941b79c94ec73395d18.4d.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/67fc14590d7f465e95e10d0d4d516677/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=RZrtfWzs6Dvy19O1W7uAJ1i-5JFRxzBrXX3PKH--6Jk")

LOOP_INTERVAL_MINUTES = 18
IST = ZoneInfo("Asia/Kolkata")

# ---------------------------------------------------------------------------
# MONGODB
# ---------------------------------------------------------------------------

def get_db():
    client = MongoClient(MONGO_CONFIG["uri"], serverSelectionTimeoutMS=5000)
    return client[MONGO_CONFIG["database"]]


def fetch_pending(db) -> list:
    docs = list(
        db[COLLECTION].find(
            {"data_flag": False},
            sort=[("run_time_ist", -1)],
        )
    )
    log.info("Fetched %d pending record(s) (data_flag=False).", len(docs))
    return docs


def mark_sent(db, doc_ids: list):
    if not doc_ids:
        return
    result = db[COLLECTION].update_many(
        {"_id": {"$in": doc_ids}},
        {"$set": {"data_flag": True}},
    )
    log.info("Marked %d record(s) as data_flag=True.", result.modified_count)


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def fmt_dt(dt) -> str:
    if dt is None:
        return "—"
    if hasattr(dt, "tzinfo") and dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt.astimezone(IST).strftime("%d/%m/%Y %H:%M")


def gap_icon(gap: int) -> str:
    return "🔴" if gap > 0 else "🟢"


# ---------------------------------------------------------------------------
# BUILD TABLE MESSAGE
# ---------------------------------------------------------------------------

def build_message(docs: list) -> dict:
    """
    Build the payload for the Power Automate HTTP trigger.

    Table columns: Comparison | Source Table | Target Table | Data Gaps
    Title format:  CHN Data Gap Alert (start → end)
    Window column removed from table rows.
    """
    if not docs:
        return {}

    latest_ts  = docs[0].get("run_time_ist")

    # Use window start/end from first gap doc for the title
    gap_docs     = [d for d in docs if d.get("query_name") != "TBM_invalid_gt_modelno"]
    invalid_docs = [d for d in docs if d.get("query_name") == "TBM_invalid_gt_modelno"]

    # Title: use window_start → window_end (run_time)
    if gap_docs:
        ws_title = fmt_dt(gap_docs[0].get("window_start_ist"))
        we_title = fmt_dt(gap_docs[0].get("window_end_ist"))
    else:
        ws_title = fmt_dt(latest_ts)
        we_title = fmt_dt(latest_ts)

    title_str = f"CHN Data Gap Alert ({ws_title} \u2192 {we_title})"

    # ── HTML table for gap scenarios ──────────────────────────────────────
    rows = ""
    for d in gap_docs:
        first  = d.get("first_table",        "")
        second = d.get("second_table",       "")
        fc     = d.get("First_Table_Count",  0) or 0
        sc     = d.get("Second_Table_Count", 0) or 0
        gap    = d.get("mis_match_count",    0) or 0
        desc   = d.get("description", "")
        barcodes = d.get("mismatched_barcodes", []) or []
        icon   = gap_icon(gap)
        gap_color = "#D83B01" if gap > 0 else "#107C10"

        # Format barcodes cell — comma-separated list when present, empty otherwise
        if barcodes:
            barcode_str = ", ".join(str(b) for b in barcodes)
            barcode_cell = (
                f"<td style='padding:6px 10px;border-bottom:1px solid #eee;"
                f"font-size:11px;color:#555;max-width:300px;word-break:break-all;'>"
                f"{barcode_str}</td>"
            )
        else:
            barcode_cell = (
                "<td style='padding:6px 10px;border-bottom:1px solid #eee;"
                "text-align:center;color:#aaa;'>—</td>"
            )

        rows += (
            f"<tr>"
            f"<td style='padding:6px 10px;border-bottom:1px solid #eee;'>{icon} {desc}</td>"
            f"<td style='padding:6px 10px;border-bottom:1px solid #eee;"
            f"text-align:center;font-weight:bold;color:#0078D4;'>{first}: {fc}</td>"
            f"<td style='padding:6px 10px;border-bottom:1px solid #eee;"
            f"text-align:center;font-weight:bold;color:#107C10;'>{second}: {sc}</td>"
            f"<td style='padding:6px 10px;border-bottom:1px solid #eee;"
            f"text-align:center;font-weight:bold;color:{gap_color};'>{gap}</td>"
            f"{barcode_cell}"
            f"</tr>"
        )

    table_html = (
        "<table style='border-collapse:collapse;width:100%;"
        "font-family:Segoe UI,sans-serif;font-size:13px;'>"
        "<thead><tr style='background:#E5E5E5;'>"
        "<th style='padding:8px 10px;text-align:left;border-bottom:2px solid #2C3E50;"
        "color:#2C3E50;font-weight:bold;'>Comparison</th>"
        "<th style='padding:8px 10px;border-bottom:2px solid #2C3E50;"
        "color:#2C3E50;font-weight:bold;'>Source Table</th>"
        "<th style='padding:8px 10px;border-bottom:2px solid #2C3E50;"
        "color:#2C3E50;font-weight:bold;'>Target Table</th>"
        "<th style='padding:8px 10px;border-bottom:2px solid #2C3E50;"
        "color:#2C3E50;font-weight:bold;'>Data Gaps</th>"
        "<th style='padding:8px 10px;text-align:left;border-bottom:2px solid #2C3E50;"
        "color:#2C3E50;font-weight:bold;'>Mismatched Barcodes</th>"
        "</tr></thead>"
        f"<tbody>{rows}</tbody>"
        "</table>"
    )

    # ── Invalid GT section ────────────────────────────────────────────────
    invalid_html = ""
    for inv in invalid_docs:
        by_gt    = inv.get("by_gt_code", {}) or {}
        total    = inv.get("mis_match_count", 0) or 0
        barcodes = inv.get("mismatched_barcodes", []) or []
        ws       = fmt_dt(inv.get("window_start_ist"))
        we       = fmt_dt(inv.get("window_end_ist"))
        icon     = gap_icon(total)

        gt_rows = "".join(
            f"<tr>"
            f"<td style='padding:4px 10px;border-bottom:1px solid #eee;'>{gt}</td>"
            f"<td style='padding:4px 10px;border-bottom:1px solid #eee;"
            f"text-align:center;font-weight:bold;"
            f"color:{'#D83B01' if cnt > 0 else '#107C10'};'>{cnt}</td>"
            f"</tr>"
            for gt, cnt in sorted(by_gt.items(), key=lambda x: -x[1])
        )

        # Barcodes section for invalid GT
        if barcodes:
            barcode_str = ", ".join(str(b) for b in barcodes)
            barcode_section = (
                f"<br/><small style='color:#555;'>"
                f"<b>Mismatched Barcodes:</b> {barcode_str}</small>"
            )
        else:
            barcode_section = ""

        invalid_html = (
            f"<br/><b>{icon} TBM Invalid GT ModelNo</b> — "
            f"Total: <span style='color:#D83B01;font-weight:bold;'>{total}</span>"
            f" ({ws} \u2192 {we})<br/>"
            "<table style='border-collapse:collapse;font-size:13px;margin-top:6px;'>"
            "<thead><tr style='background:#E5E5E5;'>"
            "<th style='padding:6px 10px;border-bottom:2px solid #2C3E50;"
            "color:#2C3E50;font-weight:bold;'>GT Code</th>"
            "<th style='padding:6px 10px;border-bottom:2px solid #2C3E50;"
            "color:#2C3E50;font-weight:bold;'>Invalid Count</th>"
            "</tr></thead>"
            f"<tbody>{gt_rows}</tbody></table>"
            f"{barcode_section}"
        ) if by_gt else (
            f"<br/><b>{icon} TBM Invalid GT ModelNo</b> — Total: {total}"
            f" ({ws} \u2192 {we})<br/>"
            f"{barcode_section}"
        )

    full_html = (
        f"<b>\U0001f6a8 {title_str}</b><br/><br/>"
        f"{table_html}"
        f"{invalid_html}"
    )

    return {
        "title":   title_str,
        "text":    full_html,
        "summary": f"{title_str} — {len(gap_docs)} scenarios checked",
    }


# ---------------------------------------------------------------------------
# SEND
# ---------------------------------------------------------------------------

def send_to_teams(payload: dict) -> bool:
    log.info("Sending Teams alert (payload keys: %s)...", list(payload.keys()))
    try:
        resp = requests.post(
            TEAMS_WEBHOOK_URL,
            json=payload,
            timeout=20,
            headers={"Content-Type": "application/json"},
        )
        log.info("HTTP %d  response: %s", resp.status_code, resp.text[:200])
        if resp.status_code in (200, 202):
            log.info("Teams alert sent successfully.")
            return True
        log.error("Teams webhook returned HTTP %d.", resp.status_code)
        return False
    except requests.RequestException as exc:
        log.error("Request failed: %s", exc)
        return False


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def run_once():
    try:
        db = get_db()
        log.info("MongoDB connected: %s / %s",
                 MONGO_CONFIG["uri"], MONGO_CONFIG["database"])
    except PyMongoError as exc:
        log.error("Cannot connect to MongoDB: %s", exc)
        return

    docs = fetch_pending(db)
    if not docs:
        log.info("No pending records. Nothing to send.")
        return

    payload = build_message(docs)
    if not payload:
        log.warning("Empty payload — nothing to send.")
        return

    log.info("Title : %s", payload.get("title"))
    log.info("Summary: %s", payload.get("summary"))

    success = send_to_teams(payload)
    if success:
        mark_sent(db, [d["_id"] for d in docs])
    else:
        log.error("Send failed — records NOT marked (will retry next run).")


def main():
    parser = argparse.ArgumentParser(description="CHN Data Gaps — Teams Alerts")
    parser.add_argument("--loop", action="store_true",
                        help="Run every N minutes continuously")
    parser.add_argument("--interval", type=int, default=LOOP_INTERVAL_MINUTES)
    args = parser.parse_args()

    if args.loop:
        log.info("Loop mode: every %d minutes. Ctrl+C to stop.", args.interval)
        cycle = 0
        while True:
            cycle += 1
            log.info("--- CYCLE %d ---", cycle)
            try:
                run_once()
            except Exception as exc:
                log.error("Cycle %d failed: %s", cycle, exc)
            time.sleep(args.interval * 60)
    else:
        run_once()


if __name__ == "__main__":
    main()
