import sys, time, logging, datetime, argparse, requests
from zoneinfo import ZoneInfo
from pymongo import MongoClient
from pymongo.errors import PyMongoError

LOG_FILE = "chn_all_schema_delay.log"
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

MONGO_URI      = "mongodb://admin:sdfda%40wdfsdfvsdg32FGSFD@10.100.192.96:27017/"
MONGO_DB       = "redshift_monitor"
COLLECTION     = "redshift_updates"
# WEBHOOK_URL    = (
#     "https://defaultf792002939f941b79c94ec73395d18.4d.environment.api.powerplatform.com:443"
#     "/powerautomate/automations/direct/workflows/67fc14590d7f465e95e10d0d4d516677"
#     "/triggers/manual/paths/invoke"
#     "?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0"
#     "&sig=RZrtfWzs6Dvy19O1W7uAJ1i-5JFRxzBrXX3PKH--6Jk"
# )

WEBHOOK_URL    = ("https://defaultf792002939f941b79c94ec73395d18.4d.environment.api.powerplatform.com:443/powerautomate/automations/direct/cu/28/workflows/1042a476d5ac401db546b44c0ab52bcd/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=OhG-VBfeG4iFNkWYZjHuy5dZmdUqOJxbWJ05Aerl4YI")
LOOP_MINS  = 17
CHUNK_SIZE = 20
IST        = ZoneInfo("Asia/Kolkata")
UTC        = datetime.timezone.utc


# --------------- helpers ---------------------------------------------------

def get_db():
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)[MONGO_DB]


def fetch_pending(db):
    docs = list(db[COLLECTION].find({"data_flag": False}, sort=[("table_key", 1)]))
    log.info("Fetched %d pending records (data_flag=False).", len(docs))
    return docs


def mark_sent(db, ids):
    if ids:
        r = db[COLLECTION].update_many({"_id": {"$in": ids}}, {"$set": {"data_flag": True}})
        log.info("Marked %d records as data_flag=True.", r.modified_count)


def fmt_dt(dt):
    if dt is None:
        return "-"
    if hasattr(dt, "tzinfo") and dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(IST).strftime("%d/%m/%Y, %H:%M:%S")


def fmt_delay(mins):
    if mins is None or mins < 0:
        return "-"
    if mins < 1:
        return "< 1 min"
    h, m = int(mins // 60), int(mins % 60)
    return f"{h}h {m}m" if h > 0 else f"{m}m"


STATUS_ICON  = {"OK": "OK", "STALE": "Stale", "Moderate": "Moderate",
                "No Data": "No Data", "Unknown": "Unknown"}
STATUS_COLOR = {"OK": "#107C10", "STALE": "#D83B01", "Moderate": "#F7630C",
                "No Data": "#888888", "Unknown": "#888888"}


# --------------- build one chunk payload ----------------------------------

def build_payload(chunk, all_docs, chunk_num, total_chunks):
    if not chunk:
        return {}

    total      = len(all_docs)
    ok_n       = sum(1 for d in all_docs if d.get("status") == "OK")
    stale_n    = sum(1 for d in all_docs if d.get("status") == "STALE")
    mod_n      = sum(1 for d in all_docs if d.get("status") == "Moderate")
    nodata_n   = sum(1 for d in all_docs if d.get("status") in ("No Data", "Unknown"))
    check_str  = fmt_dt(all_docs[0].get("check_time_ist"))
    part       = f" (Part {chunk_num}/{total_chunks})" if total_chunks > 1 else ""
    title      = f"CHN Schema Delay Report{part} - {check_str}"

    # Summary bar on first chunk only
    summary_html = ""
    if chunk_num == 1:
        summary_html = (
            "<table style='border-collapse:collapse;width:100%;margin-bottom:12px;'><tr>"
            f"<td style='padding:8px 12px;background:#F0F0F0;text-align:center;border:1px solid #ddd;'>"
            f"<b style='font-size:18px;color:#2C3E50;'>{total}</b><br/>"
            f"<span style='font-size:11px;color:#555;'>Total Monitored</span></td>"
            f"<td style='padding:8px 12px;background:#EBF8EE;text-align:center;border:1px solid #ddd;'>"
            f"<b style='font-size:18px;color:#107C10;'>{ok_n}</b><br/>"
            f"<span style='font-size:11px;color:#107C10;'>Synced</span></td>"
            f"<td style='padding:8px 12px;background:#FEF0EC;text-align:center;border:1px solid #ddd;'>"
            f"<b style='font-size:18px;color:#D83B01;'>{stale_n}</b><br/>"
            f"<span style='font-size:11px;color:#D83B01;'>Stale</span></td>"
            f"<td style='padding:8px 12px;background:#FFF4EC;text-align:center;border:1px solid #ddd;'>"
            f"<b style='font-size:18px;color:#F7630C;'>{mod_n}</b><br/>"
            f"<span style='font-size:11px;color:#F7630C;'>Moderate</span></td>"
            f"<td style='padding:8px 12px;background:#F5F5F5;text-align:center;border:1px solid #ddd;'>"
            f"<b style='font-size:18px;color:#888;'>{nodata_n}</b><br/>"
            f"<span style='font-size:11px;color:#888;'>No Data</span></td>"
            "</tr></table>"
        )

    # Detail rows
    rows = ""
    for i, d in enumerate(chunk):
        tkey   = d.get("table_key", "")
        ts_col = d.get("timestamp_column", "-")
        tz     = d.get("detected_timezone", "Unknown")
        last   = fmt_dt(d.get("last_record_ist"))
        delay  = fmt_delay(d.get("delay_minutes"))
        newrec = d.get("new_record_count", 0) or 0
        chkt   = fmt_dt(d.get("check_time_ist"))
        status = d.get("status", "Unknown")
        sc     = STATUS_COLOR.get(status, "#888")
        tz_c   = "#0078D4" if tz == "UTC" else "#F7630C" if tz == "IST" else "#888"
        bg     = "#FAFAFA" if i % 2 == 0 else "#FFFFFF"

        rows += (
            f"<tr style='background:{bg};'>"
            f"<td style='padding:5px 8px;border-bottom:1px solid #eee;font-size:12px;color:#2C3E50;'>{tkey}</td>"
            f"<td style='padding:5px 8px;border-bottom:1px solid #eee;font-size:12px;text-align:center;'>{ts_col}</td>"
            f"<td style='padding:5px 8px;border-bottom:1px solid #eee;font-size:12px;text-align:center;font-weight:bold;color:{tz_c};'>{tz}</td>"
            f"<td style='padding:5px 8px;border-bottom:1px solid #eee;font-size:11px;text-align:center;'>{last}</td>"
            f"<td style='padding:5px 8px;border-bottom:1px solid #eee;font-size:12px;text-align:center;'>{delay}</td>"
            f"<td style='padding:5px 8px;border-bottom:1px solid #eee;font-size:12px;text-align:center;'>{newrec}</td>"
            f"<td style='padding:5px 8px;border-bottom:1px solid #eee;font-size:11px;text-align:center;'>{chkt}</td>"
            f"<td style='padding:5px 8px;border-bottom:1px solid #eee;font-size:12px;text-align:center;font-weight:bold;color:{sc};'>{status}</td>"
            f"</tr>"
        )

    detail = (
        "<table style='border-collapse:collapse;width:100%;font-family:Segoe UI,sans-serif;'>"
        "<thead><tr style='background:#E5E5E5;'>"
        "<th style='padding:7px 8px;text-align:left;border-bottom:2px solid #2C3E50;color:#2C3E50;font-size:12px;'>Table</th>"
        "<th style='padding:7px 8px;border-bottom:2px solid #2C3E50;color:#2C3E50;font-size:12px;'>TS Column</th>"
        "<th style='padding:7px 8px;border-bottom:2px solid #2C3E50;color:#2C3E50;font-size:12px;'>Timezone</th>"
        "<th style='padding:7px 8px;border-bottom:2px solid #2C3E50;color:#2C3E50;font-size:12px;'>Last Record (IST)</th>"
        "<th style='padding:7px 8px;border-bottom:2px solid #2C3E50;color:#2C3E50;font-size:12px;'>Delay</th>"
        "<th style='padding:7px 8px;border-bottom:2px solid #2C3E50;color:#2C3E50;font-size:12px;'>New Records</th>"
        "<th style='padding:7px 8px;border-bottom:2px solid #2C3E50;color:#2C3E50;font-size:12px;'>Check Time</th>"
        "<th style='padding:7px 8px;border-bottom:2px solid #2C3E50;color:#2C3E50;font-size:12px;'>Status</th>"
        "</tr></thead>"
        f"<tbody>{rows}</tbody></table>"
    )

    html = f"<b>{title}</b><br/><br/>{summary_html}{detail}"
    return {
        "title":   title,
        "text":    html,
        "summary": f"{title} | {ok_n} OK | {stale_n} STALE | {mod_n} Moderate | {nodata_n} No Data",
    }


# --------------- send ------------------------------------------------------

def send(payload):
    log.info("POST to webhook (text=%d chars)...", len(payload.get("text", "")))
    try:
        r = requests.post(WEBHOOK_URL, json=payload, timeout=30,
                          headers={"Content-Type": "application/json"})
        log.info("HTTP %d  %s", r.status_code, r.text[:200])
        return r.status_code in (200, 202)
    except requests.RequestException as e:
        log.error("Request failed: %s", e)
        return False


# --------------- main ------------------------------------------------------

def run_once():
    try:
        db = get_db()
        log.info("MongoDB: %s / %s", MONGO_URI, MONGO_DB)
    except PyMongoError as e:
        log.error("MongoDB error: %s", e)
        return

    docs = fetch_pending(db)
    if not docs:
        log.info("No pending records.")
        return

    chunks = [docs[i:i + CHUNK_SIZE] for i in range(0, len(docs), CHUNK_SIZE)]
    log.info("%d chunk(s) of up to %d rows each.", len(chunks), CHUNK_SIZE)

    all_ok = True
    for idx, chunk in enumerate(chunks, 1):
        payload = build_payload(chunk, docs, idx, len(chunks))
        ok = send(payload)
        if not ok:
            all_ok = False
            log.error("Chunk %d/%d failed — stopping.", idx, len(chunks))
            break
        if idx < len(chunks):
            time.sleep(3)

    if all_ok:
        mark_sent(db, [d["_id"] for d in docs])
    else:
        log.error("Not all chunks sent — records NOT marked (retry next run).")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--loop", action="store_true")
    ap.add_argument("--interval", type=int, default=LOOP_MINS)
    args = ap.parse_args()

    if args.loop:
        log.info("Loop mode: every %d min.", args.interval)
        c = 0
        while True:
            c += 1
            log.info("--- cycle %d ---", c)
            try:
                run_once()
            except Exception as e:
                log.error("Cycle %d error: %s", c, e)
            time.sleep(args.interval * 60)
    else:
        run_once()


if __name__ == "__main__":
    main()
