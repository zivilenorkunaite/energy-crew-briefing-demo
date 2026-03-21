# Databricks notebook source
# MAGIC %md
# MAGIC # Cache Warming Job
# MAGIC Calls the app's `/api/cache/warm` endpoint and monitors cache growth
# MAGIC by querying the Lakebase cache table directly.
# MAGIC Restarts warming if cache stops growing. Exits when 80% full or 15min elapsed.
# MAGIC Scheduled at 6am + 6pm AEST via DAB job.

# COMMAND ----------

import requests
import json
import time

host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
notebook_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

APP_URL = "https://ee-crew-briefing-1313663707993479.aws.databricksapps.com"
LAKEBASE_HOST = "ep-jolly-leaf-d20ipqcy.database.us-east-1.cloud.databricks.com"
ENDPOINT_NAME = "projects/ee-crew-briefing-as/branches/production/endpoints/primary"

# Expected cache entries
EXPECTED = {
    "get_swms": 7,
    "query_weather": 209,
    "search_local_notices": 19,
    "query_genie": 231,
}
TOTAL_EXPECTED = sum(EXPECTED.values())
TARGET_PCT = 0.80
TARGET_COUNT = int(TOTAL_EXPECTED * TARGET_PCT)
MAX_MINUTES = 15

print(f"Expected: {TOTAL_EXPECTED} entries, target: {TARGET_COUNT} ({TARGET_PCT:.0%}), max: {MAX_MINUTES} min")

# COMMAND ----------

# Get Lakebase credential for direct DB queries
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
db_cred = w.postgres.generate_database_credential(endpoint=ENDPOINT_NAME)
db_token = db_cred.token

import psycopg2
def get_cache_stats():
    """Query Lakebase cache table directly."""
    try:
        conn = psycopg2.connect(
            host=LAKEBASE_HOST, port=5432, dbname="databricks_postgres",
            user=spark.conf.get("spark.databricks.service.principal.id", "zivile.norkunaite@databricks.com"),
            password=db_token, sslmode="require",
        )
        cur = conn.cursor()
        cur.execute("SELECT tool_name, COUNT(*) FROM tool_cache GROUP BY tool_name")
        per_tool = {row[0]: row[1] for row in cur.fetchall()}
        total = sum(per_tool.values())
        cur.close()
        conn.close()
        return total, per_tool
    except Exception as e:
        print(f"  DB query error: {e}")
        return 0, {}

# Test connection
total, per_tool = get_cache_stats()
print(f"Current cache: {total} entries")
for t, c in per_tool.items():
    print(f"  {t}: {c}")

# COMMAND ----------

def start_warming():
    """Fire the warm endpoint. Uses notebook token — may not auth but the SSE stream starts."""
    try:
        # Try with notebook token first, then fall back
        for token in [notebook_token]:
            resp = requests.post(
                f"{APP_URL}/api/cache/warm",
                headers={"Authorization": f"Bearer {token}"},
                stream=True, timeout=10,
            )
            if resp.status_code == 200:
                for line in resp.iter_lines(decode_unicode=True):
                    if line and line.startswith("data: "):
                        d = json.loads(line[6:])
                        print(f"  Warming started: {d.get('phase', '?')}")
                        break
                resp.close()
                return True
            resp.close()
        print("  Could not start warming via API — trigger manually from Settings page")
        return False
    except Exception as e:
        print(f"  Error starting warm: {e}")
        return False

# COMMAND ----------

start_time = time.time()
prev_count = 0
stall_rounds = 0

# Try to start warming
print("Starting cache warm...")
started = start_warming()
if not started:
    print("WARNING: Could not trigger warming via API. Monitoring only.")
    print("Please start warming from the Settings page: /settings")

print(f"\nMonitoring cache growth...")

while True:
    elapsed_min = (time.time() - start_time) / 60

    if elapsed_min >= MAX_MINUTES:
        print(f"\n  Time limit reached ({MAX_MINUTES} min)")
        break

    total, per_tool = get_cache_stats()
    pct = total / TOTAL_EXPECTED * 100 if TOTAL_EXPECTED > 0 else 0
    status_parts = [f"{t}: {per_tool.get(t, 0)}/{e}" for t, e in EXPECTED.items()]
    print(f"  [{elapsed_min:4.1f}min] {total}/{TOTAL_EXPECTED} ({pct:.0f}%) — {', '.join(status_parts)}")

    if total >= TARGET_COUNT:
        print(f"\n  Target reached: {total}/{TARGET_COUNT} ({pct:.0f}%)")
        break

    if total > prev_count:
        stall_rounds = 0
        prev_count = total
        print(f"    Growing, sleeping 60s...")
        time.sleep(60)
    else:
        stall_rounds += 1
        if stall_rounds >= 3:
            print(f"    Stalled for {stall_rounds} rounds, giving up")
            break
        print(f"    Not growing (stall {stall_rounds}/3), restarting warm...")
        start_warming()
        prev_count = total
        time.sleep(60)

# COMMAND ----------

# Final summary
total, per_tool = get_cache_stats()
pct = total / TOTAL_EXPECTED * 100 if TOTAL_EXPECTED > 0 else 0
elapsed = (time.time() - start_time) / 60

print(f"\n{'='*60}")
print(f"Cache warming complete in {elapsed:.1f} min")
print(f"  Total: {total}/{TOTAL_EXPECTED} ({pct:.0f}%)")
for tool, exp in EXPECTED.items():
    actual = per_tool.get(tool, 0)
    status = "OK" if actual >= exp * TARGET_PCT else "LOW"
    print(f"  {tool:25s}: {actual:4d}/{exp} ({status})")
print(f"{'='*60}")

if total < TARGET_COUNT * 0.5:
    raise Exception(f"Cache critically low: {total}/{TARGET_COUNT} — check app health")
