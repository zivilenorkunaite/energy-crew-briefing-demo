# Databricks notebook source
# MAGIC %md
# MAGIC # Cache Warming Job
# MAGIC Calls the app's `/api/cache/warm` endpoint and monitors progress via `/api/cache/stats`.
# MAGIC Restarts warming if cache stops growing. Exits when 80% full or 15min elapsed.
# MAGIC Runs as the app's service principal for auth.
# MAGIC Scheduled at 6am + 6pm AEST via DAB job.

# COMMAND ----------

import requests
import json
import time

APP_URL = "https://ee-crew-briefing-1313663707993479.aws.databricksapps.com"

# Get OAuth token from the SP running this job
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
token = w.config.token
if not token:
    headers = w.config.authenticate()
    token = headers.get("Authorization", "").replace("Bearer ", "")
print(f"Auth token: {len(token)} chars")

HEADERS = {"Authorization": f"Bearer {token}"}

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

def get_cache_count():
    """Get current total cache entries from the app."""
    try:
        resp = requests.get(f"{APP_URL}/api/cache/stats", headers=HEADERS, timeout=10)
        stats = resp.json().get("stats", [])
        total = sum(s.get("count", 0) for s in stats)
        per_tool = {s["tool_name"]: s["count"] for s in stats}
        return total, per_tool
    except Exception as e:
        print(f"  Error checking stats: {e}")
        return 0, {}


def start_warming():
    """Fire the warm endpoint (non-blocking — start it and let it run)."""
    try:
        resp = requests.post(
            f"{APP_URL}/api/cache/warm",
            headers=HEADERS,
            stream=True,
            timeout=10,
        )
        for line in resp.iter_lines(decode_unicode=True):
            if line and line.startswith("data: "):
                d = json.loads(line[6:])
                print(f"  Warming started: {d.get('phase', '?')}")
                break
        resp.close()
        return True
    except Exception as e:
        print(f"  Error starting warm: {e}")
        return False

# COMMAND ----------

# Verify auth works
total, per_tool = get_cache_count()
print(f"Current cache: {total} entries")
for t, c in per_tool.items():
    print(f"  {t}: {c}")

if total == 0 and not per_tool:
    debug_resp = requests.get(f"{APP_URL}/api/cache/stats", headers=HEADERS, timeout=10)
    print(f"Stats response: {debug_resp.status_code} — {debug_resp.text[:500]}")
    if debug_resp.status_code != 200:
        raise Exception(f"Cannot reach cache stats API — status {debug_resp.status_code}: {debug_resp.text[:200]}")
    print("Cache is empty — warming will populate it")

# COMMAND ----------

start_time = time.time()
prev_count = total
stall_rounds = 0

# Start warming
print("\nStarting cache warm...")
start_warming()

print("Monitoring cache growth...")

while True:
    elapsed_min = (time.time() - start_time) / 60

    if elapsed_min >= MAX_MINUTES:
        print(f"\n  Time limit reached ({MAX_MINUTES} min)")
        break

    time.sleep(60)

    total, per_tool = get_cache_count()
    pct = total / TOTAL_EXPECTED * 100 if TOTAL_EXPECTED > 0 else 0
    status_parts = [f"{t}: {per_tool.get(t, 0)}/{e}" for t, e in EXPECTED.items()]
    print(f"  [{elapsed_min:4.1f}min] {total}/{TOTAL_EXPECTED} ({pct:.0f}%) — {', '.join(status_parts)}")

    if total >= TARGET_COUNT:
        print(f"\n  Target reached: {total}/{TARGET_COUNT} ({pct:.0f}%)")
        break

    if total > prev_count:
        stall_rounds = 0
        prev_count = total
        print(f"    Growing...")
    else:
        stall_rounds += 1
        if stall_rounds >= 3:
            print(f"    Stalled for {stall_rounds} rounds, giving up")
            break
        print(f"    Not growing (stall {stall_rounds}/3), restarting warm...")
        start_warming()
        prev_count = total

# COMMAND ----------

# Final summary
total, per_tool = get_cache_count()
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
