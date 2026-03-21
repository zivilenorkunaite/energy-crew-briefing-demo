# Databricks notebook source
# MAGIC %md
# MAGIC # Gateway Token Refresh
# MAGIC Refreshes the PAT used by the crew-briefing-agent AI Gateway endpoint.
# MAGIC Scheduled every 30 minutes to prevent token expiry.

# COMMAND ----------

import requests
import json

ENDPOINT_NAME = "crew-briefing-agent"
SECRET_SCOPE = "ee-crew-briefing"
SECRET_KEY = "gateway-token"
PAT_COMMENT = "crew-briefing-agent-auto"

# Get workspace host and token from notebook context
host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
notebook_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

headers = {"Authorization": f"Bearer {notebook_token}", "Content-Type": "application/json"}

# COMMAND ----------

# Step 1: Revoke old tokens with this comment (cleanup)
old_tokens = requests.get(f"{host}/api/2.0/token/list", headers=headers).json().get("token_infos", [])
for t in old_tokens:
    if t.get("comment") == PAT_COMMENT:
        requests.post(f"{host}/api/2.0/token/delete", headers=headers, json={"token_id": t["token_id"]})
        print(f"Revoked old token {t['token_id']}")

# COMMAND ----------

# Step 2: Create a fresh PAT (24h lifetime — refreshed every 30min so always valid)
resp = requests.post(f"{host}/api/2.0/token/create", headers=headers, json={
    "comment": PAT_COMMENT,
    "lifetime_seconds": 86400,
}).json()
new_token = resp.get("token_value", "")
print(f"New PAT created: {len(new_token)} chars")

# COMMAND ----------

# Step 3: Update the secret
resp = requests.post(f"{host}/api/2.0/secrets/put", headers=headers, json={
    "scope": SECRET_SCOPE,
    "key": SECRET_KEY,
    "string_value": new_token,
})
print(f"Secret updated: {resp.status_code}")

# COMMAND ----------

# Step 4: Force gateway config refresh (re-reads the secret)
config = {
    "served_entities": [
        {"external_model": {"name": "databricks-claude-sonnet-4-6", "provider": "databricks-model-serving", "task": "llm/v1/chat",
         "databricks_model_serving_config": {"databricks_workspace_url": host, "databricks_api_token": f"{{{{secrets/{SECRET_SCOPE}/{SECRET_KEY}}}}}"}}, "name": "primary-claude"},
        {"external_model": {"name": "databricks-meta-llama-3-3-70b-instruct", "provider": "databricks-model-serving", "task": "llm/v1/chat",
         "databricks_model_serving_config": {"databricks_workspace_url": host, "databricks_api_token": f"{{{{secrets/{SECRET_SCOPE}/{SECRET_KEY}}}}}"}}, "name": "fallback-llama"}
    ],
    "traffic_config": {"routes": [
        {"served_model_name": "primary-claude", "traffic_percentage": 100},
        {"served_model_name": "fallback-llama", "traffic_percentage": 0}
    ]}
}
resp = requests.put(f"{host}/api/2.0/serving-endpoints/{ENDPOINT_NAME}/config", headers=headers, json=config)
print(f"Gateway config refreshed: {resp.status_code}")

# COMMAND ----------

# Step 5: Verify
resp = requests.post(f"{host}/serving-endpoints/{ENDPOINT_NAME}/invocations", headers=headers,
    json={"messages": [{"role": "user", "content": "Say OK"}], "max_tokens": 10})
data = resp.json()
if "choices" in data:
    print(f"Gateway verified OK: {data['choices'][0]['message']['content'][:50]}")
else:
    print(f"Gateway verification failed: {json.dumps(data)[:200]}")
