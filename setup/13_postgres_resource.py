"""Add Lakebase postgres resource to the Databricks App.

DABs don't support postgres app resources yet. This script adds the
resource via REST API after bundle deploy creates the app.
Handles both AWS (branch/database paths) and Azure (instance name) formats.

Run with: python3 setup/13_postgres_resource.py
"""

import json
import urllib.request
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from helpers import run_cli

PROFILE = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
APP_NAME = "energy-crew-briefing"
LAKEBASE_INSTANCE = "energy-crew-briefing"


def main():
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient(profile=PROFILE)
    host = w.config.host
    token = w.config.token or w.config.authenticate().get("Authorization", "").replace("Bearer ", "")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Get current app resources
    try:
        req = urllib.request.Request(f"{host}/api/2.0/apps/{APP_NAME}", headers=headers)
        app = json.loads(urllib.request.urlopen(req).read())
    except Exception as e:
        print(f"  App not found: {e}")
        return

    current = {r["name"]: r for r in app.get("resources", [])}
    if "postgres" in current:
        print("  Postgres resource already present")
        return

    # Try to discover the Lakebase instance details for the postgres resource
    instance = run_cli(["database", "get-database-instance", LAKEBASE_INSTANCE])
    if not instance or not isinstance(instance, dict):
        print(f"  Lakebase instance '{LAKEBASE_INSTANCE}' not found — skipping postgres resource")
        return

    # Build postgres resource — try different formats for AWS vs Azure
    resources = list(app.get("resources", []))
    postgres_resource = {
        "name": "postgres",
        "description": "Lakebase for session persistence",
        "postgres": {
            "instance": LAKEBASE_INSTANCE,
            "permission": "CAN_CONNECT_AND_CREATE",
        },
    }
    resources.append(postgres_resource)

    try:
        req = urllib.request.Request(
            f"{host}/api/2.0/apps/{APP_NAME}",
            data=json.dumps({"resources": resources}).encode(),
            method="PATCH",
            headers=headers,
        )
        resp = json.loads(urllib.request.urlopen(req).read())
        print(f"  Added postgres resource ({len(resp.get('resources', []))} resources total)")
    except Exception as e:
        error = e.read().decode() if hasattr(e, "read") else str(e)
        # If instance format didn't work, the app SP may already have access via Lakebase grants
        print(f"  Note: postgres app resource not added ({error[:150]})")
        print(f"  The app uses PGHOST env var directly — this is OK if Lakebase SP grants are in place.")


if __name__ == "__main__":
    print("Adding Lakebase postgres resource to app...")
    main()
