"""Add Lakebase postgres resource to the Databricks App.

DABs don't support postgres app resources yet. This script adds the
resource via REST API after bundle deploy creates the app.

Run with: python3 setup/13_postgres_resource.py
"""

import json
import urllib.request
import os

PROFILE = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
APP_NAME = "energy-crew-briefing"
LAKEBASE_BRANCH = "projects/energy-crew-briefing-as/branches/production"
LAKEBASE_DATABASE = "projects/energy-crew-briefing-as/branches/production/databases/db-i1ri-fqtfd0d6tm"


def main():
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient(profile=PROFILE)
    host = w.config.host
    token = w.config.token or w.config.authenticate().get("Authorization", "").replace("Bearer ", "")

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Get current app resources
    req = urllib.request.Request(f"{host}/api/2.0/apps/{APP_NAME}", headers=headers)
    app = json.loads(urllib.request.urlopen(req).read())
    current = {r["name"]: r for r in app.get("resources", [])}

    if "postgres" in current:
        print("  Postgres resource already present")
        return

    resources = list(app.get("resources", []))
    resources.append({
        "name": "postgres",
        "description": "Lakebase Autoscaling for session persistence",
        "postgres": {
            "branch": LAKEBASE_BRANCH,
            "database": LAKEBASE_DATABASE,
            "permission": "CAN_CONNECT_AND_CREATE",
        },
    })

    req = urllib.request.Request(
        f"{host}/api/2.0/apps/{APP_NAME}",
        data=json.dumps({"resources": resources}).encode(),
        method="PATCH",
        headers=headers,
    )
    try:
        resp = json.loads(urllib.request.urlopen(req).read())
        print(f"  Added postgres resource ({len(resp.get('resources', []))} resources total)")
    except Exception as e:
        error = e.read().decode() if hasattr(e, "read") else str(e)
        print(f"  Warning: {error[:200]}")


if __name__ == "__main__":
    print("Adding Lakebase postgres resource to app...")
    main()
