# import redshift_connector
# import pandas as pd
# import requests
# from datetime import datetime


# # ── Redshift Connection ───────────────────────────────────────────
# class RedshiftDataConnectorr:
#     def __init__(self):
#         self.conn = redshift_connector.connect(
#             host='atl-dl-prd-01.cuvxusm4qify.ap-south-1.redshift.amazonaws.com',
#             database='db_prd_plant_che',
#             port=5439,
            # user='usr_thangadurai_a',
            # password='QtHJO%1^m^dQEVFc',
#             sslmode='disable'
#         )

#     def execute_query(self, q):
#         cursor = self.conn.cursor()
#         cursor.execute(q)
#         rows = cursor.fetchall()
#         cols = [desc[0] for desc in cursor.description]
#         cursor.close()
#         return pd.DataFrame(rows, columns=cols)


# inst = RedshiftDataConnectorr()

# # ── Config ────────────────────────────────────────────────────────
# TEAMS_WEBHOOK_URL = 'https://automatonai1.webhook.office.com/webhookb2/4ed06a0e-d8cb-408f-99eb-e18cbf3d449f@92d2e83b-222e-41e1-a43c-045151212f01/IncomingWebhook/e4069f1464474339a49ac07fa193275d/3e952810-4b04-41e9-aa1e-99277bb56591/V2LJ2ELi2ZbC8IJT7WcONix02Z1pzkyNbN-dkt8zbaw-U1'

# QUERY = "SELECT COUNT(*) FROM db_prd_plant_che.sc_cimcorp_pcr.tbm"


# # ── Core job ─────────────────────────────────────────────────────
# def check_tbm_count():
#     now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#     try:
#         df = inst.execute_query(QUERY)
#         count = int(df.iloc[0, 0])
#         status = "✅ Data is available" if count > 0 else "⚠️ No data found (count = 0)"
#         message = (
#             f"**TBM Data Count Check**\n\n"
#             f"🕐 Time: {now}\n"
#             f"📊 Count: `{count}`\n"
#             f"Status: {status}"
#         )
#         print(f"[{now}] Count: {count} — {status}")
#     except Exception as e:
#         message = (
#             f"**TBM Data Count Check — ERROR**\n\n"
#             f"🕐 Time: {now}\n"
#             f"❌ Error: {e}"
#         )
#         print(f"[{now}] ERROR: {e}")

#     # Send to Teams
#     payload = {
#         "@type": "MessageCard",
#         "@context": "http://schema.org/extensions",
#         "summary": "TBM Data Count Check",
#         "themeColor": "0076D7",
#         "title": "TBM Data Count Check",
#         "text": message
#     }
#     try:
#         resp = requests.post(TEAMS_WEBHOOK_URL, json=payload, timeout=10)
#         if resp.status_code in (200, 202):
#             print(f"[{now}] Teams alert sent successfully.")
#         else:
#             print(f"[{now}] Teams alert failed: {resp.status_code} {resp.text}")
#     except Exception as e:
#         print(f"[{now}] Teams request error: {e}")



# if __name__ == "__main__":
#     check_tbm_count()



import redshift_connector
import pandas as pd
import logging
import os
from datetime import datetime


# ── Logging setup ─────────────────────────────────────────────────
LOG_DIR  = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"tbm_data_{datetime.now().strftime('%Y-%m-%d')}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),          # also print to console
    ]
)
log = logging.getLogger(__name__)


# ── Redshift Connection ───────────────────────────────────────────
class RedshiftDataConnectorr:
    def __init__(self):
        self.conn = redshift_connector.connect(
            host='atl-dl-prd-01.cuvxusm4qify.ap-south-1.redshift.amazonaws.com',
            database='db_prd_plant_che',
            port=5439,
            user='usr_thangadurai_a',
            password='QtHJO%1^m^dQEVFc',
            sslmode='disable'
        )

    def execute_query(self, q):
        cursor = self.conn.cursor()
        cursor.execute(q)
        rows = cursor.fetchall()
        cols = [desc[0] for desc in cursor.description]
        cursor.close()
        return pd.DataFrame(rows, columns=cols)


inst = RedshiftDataConnectorr()

# ── Queries ───────────────────────────────────────────────────────
QUERY = "SELECT COUNT(*) FROM db_prd_plant_che.sc_cimcorp_pcr.tbm"

JOIN_COUNT_QUERY = """
SELECT COUNT(*) AS total_count
FROM sc_tub_pcr.ubtable ub
LEFT JOIN sc_cimcorp_pcr.transportationunit t
    ON TRIM(ub.barcode) = TRIM(t.barcode)
LEFT JOIN sc_cimcorp_pcr.tbm tbm
    ON t.transportationunitid = tbm.transportationunitid
LEFT JOIN sc_cimcorp_pcr.curing curing
    ON t.transportationunitid = curing.transportationunitid
"""


# ── Core job ─────────────────────────────────────────────────────
def check_tbm_count():
    try:
        # TBM simple count
        df = inst.execute_query(QUERY)
        count = int(df.iloc[0, 0])
        status = "Data is available" if count > 0 else "No data found (count = 0)"
        log.info(f"TBM Count: {count} — {status}")

        # UB-TBM-Curing join count
        join_df = inst.execute_query(JOIN_COUNT_QUERY)
        join_count = int(join_df.iloc[0, 0])
        join_status = "Join data available" if join_count > 0 else "No join data found (count = 0)"
        log.info(f"UB-TBM-Curing Join Count: {join_count} — {join_status}")

    except Exception as e:
        log.error(f"TBM Data Count Check failed: {e}", exc_info=True)


if __name__ == "__main__":
    check_tbm_count()
