# Databricks notebook source
# MAGIC %md
# MAGIC # Cache Warming Job
# MAGIC Calls the app's `/api/cache/warm` endpoint to pre-populate the tool cache.
# MAGIC Scheduled every 12 hours via DAB job.

# COMMAND ----------

import requests
import json

# Get workspace host and token from notebook context
host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
notebook_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

APP_URL = "https://ee-crew-briefing-1313663707993479.aws.databricksapps.com"

# COMMAND ----------

# Call the warm cache endpoint (SSE stream)
print(f"Warming cache via {APP_URL}/api/cache/warm ...")

resp = requests.post(
    f"{APP_URL}/api/cache/warm",
    headers={"Authorization": f"Bearer {notebook_token}"},
    stream=True,
    timeout=600,  # 10 min max
)

last_progress = {}
for line in resp.iter_lines(decode_unicode=True):
    if not line or not line.startswith("data: "):
        continue
    try:
        d = json.loads(line[6:])
        last_progress = d
        phase = d.get("phase", "")
        done = d.get("done", 0)
        total = d.get("total", 0)
        skipped = d.get("skipped", 0)
        errors = d.get("errors", 0)
        pct = round(done / total * 100) if total > 0 else 0
        print(f"  [{pct:3d}%] {phase}: {done}/{total} ({skipped} cached, {errors} errors)")
    except Exception:
        pass

# COMMAND ----------

# Summary
done = last_progress.get("done", 0)
skipped = last_progress.get("skipped", 0)
errors = last_progress.get("errors", 0)
total = last_progress.get("total", 0)

print(f"\nCache warming complete:")
print(f"  Total: {done}/{total}")
print(f"  Already cached: {skipped}")
print(f"  Errors: {errors}")
print(f"  New entries: {done - skipped - errors}")

if errors > total * 0.2:
    raise Exception(f"Too many errors during cache warming: {errors}/{total}")
