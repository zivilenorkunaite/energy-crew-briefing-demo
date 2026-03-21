# Databricks notebook source
# MAGIC %md
# MAGIC # Cache Warming Job
# MAGIC Calls the app's `/api/cache/warm` endpoint and monitors progress.
# MAGIC Restarts warming if cache stops growing. Exits when 80% full or 15min elapsed.
# MAGIC Scheduled at 6am + 6pm AEST via DAB job.

# COMMAND ----------

import requests
import json
import time

host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
notebook_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

APP_URL = "https://ee-crew-briefing-1313663707993479.aws.databricksapps.com"
HEADERS = {"Authorization": f"Bearer {notebook_token}"}

# Expected cache entries (from warm_cache.py)
EXPECTED = {
    "get_swms": 7,               # 7 SWMS documents
    "query_weather": 209,        # 19 locations × 11 days
    "search_local_notices": 19,  # 19 locations
    "query_genie": 231,          # ~21 crews × 11 days
}
TOTAL_EXPECTED = sum(EXPECTED.values())
TARGET_PCT = 0.80
TARGET_COUNT = int(TOTAL_EXPECTED * TARGET_PCT)
MAX_MINUTES = 15

print(f"Expected: {TOTAL_EXPECTED} entries, target: {TARGET_COUNT} ({TARGET_PCT:.0%})")
print(f"Max time: {MAX_MINUTES} min")

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
    """Fire the warm endpoint (non-blocking — we just start it and monitor via stats)."""
    try:
        # Start the SSE stream but don't block on it — read a few lines then let it run
        resp = requests.post(
            f"{APP_URL}/api/cache/warm",
            headers=HEADERS,
            stream=True,
            timeout=10,
        )
        # Read first line to confirm it started
        for line in resp.iter_lines(decode_unicode=True):
            if line and line.startswith("data: "):
                d = json.loads(line[6:])
                print(f"  Warming started: {d.get('phase', '?')}")
                break
        # Close the stream — warming continues server-side
        resp.close()
        return True
    except Exception as e:
        print(f"  Error starting warm: {e}")
        return False

# COMMAND ----------

start_time = time.time()
prev_count = 0
stall_rounds = 0
warming_active = False

print(f"\nStarting cache monitoring loop...")

while True:
    elapsed_min = (time.time() - start_time) / 60

    # Check time limit
    if elapsed_min >= MAX_MINUTES:
        print(f"\n  Time limit reached ({MAX_MINUTES} min)")
        break

    # Check current cache status
    total, per_tool = get_cache_count()
    pct = total / TOTAL_EXPECTED * 100 if TOTAL_EXPECTED > 0 else 0

    status_parts = [f"{tool}: {per_tool.get(tool, 0)}/{exp}" for tool, exp in EXPECTED.items()]
    print(f"  [{elapsed_min:4.1f}min] {total}/{TOTAL_EXPECTED} ({pct:.0f}%) — {', '.join(status_parts)}")

    # Check if target reached
    if total >= TARGET_COUNT:
        print(f"\n  Target reached: {total}/{TARGET_COUNT} ({pct:.0f}%)")
        break

    # Check if cache is growing
    if total > prev_count:
        # Growing — sleep and check again
        stall_rounds = 0
        prev_count = total
        print(f"    Cache growing, sleeping 60s...")
        time.sleep(60)
    else:
        # Stalled — restart warming
        stall_rounds += 1
        if stall_rounds >= 3:
            print(f"    Stalled for {stall_rounds} rounds, giving up")
            break
        print(f"    Cache not growing (stall {stall_rounds}/3), restarting warm...")
        start_warming()
        prev_count = total
        time.sleep(60)

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
