"""Add app resources not supported by DABs (Lakebase, Genie Room, SQL Warehouse).

Also grants UC permissions to the App Service Principal.
Run after bundle deploy creates the app.

Run with: python3 setup/13_app_resources.py
"""

import json
import urllib.request
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from helpers import run_cli, run_sql, get_warehouse_id, UC_FULL, UC_CATALOG

PROFILE = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
APP_NAME = "energy-crew-briefing"
LAKEBASE_INSTANCE = "energy-crew-briefing"
LAKEBASE_DATABASE = "crew_briefing"


def get_app_details():
    """Get app details including SP client ID."""
    result = run_cli(["api", "get", f"/api/2.0/apps/{APP_NAME}"])
    if not result or not isinstance(result, dict):
        print(f"  App '{APP_NAME}' not found")
        return None
    return result


def add_resources(app):
    """Add Lakebase, Genie Room, and SQL Warehouse resources to the app."""
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient(profile=PROFILE)
    host = w.config.host
    token = w.config.token or w.config.authenticate().get("Authorization", "").replace("Bearer ", "")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    current = {r["name"]: r for r in app.get("resources", [])}
    resources = list(app.get("resources", []))
    added = []

    # Discover Genie Room
    genie_id = None
    genie_result = run_cli(["api", "get", "/api/2.0/genie/spaces"])
    if genie_result and isinstance(genie_result, dict):
        for space in genie_result.get("spaces", []):
            if space.get("title") == "Field Operations":
                genie_id = space["space_id"]
                break

    # Discover warehouse
    warehouse_id = get_warehouse_id()

    # Add Genie Room
    if genie_id and "genie-field-ops" not in current:
        resources.append({
            "name": "genie-field-ops",
            "description": "Genie Room — Field Operations",
            "genie_space": {"space_id": genie_id, "permission": "CAN_RUN"},
        })
        added.append(f"genie-field-ops ({genie_id})")

    # Add SQL Warehouse
    if warehouse_id and "sql-warehouse" not in current:
        resources.append({
            "name": "sql-warehouse",
            "description": "SQL Warehouse for queries",
            "sql_warehouse": {"id": warehouse_id, "permission": "CAN_USE"},
        })
        added.append(f"sql-warehouse ({warehouse_id})")

    # Add Lakebase
    if "lakebase" not in current and "postgres" not in current:
        resources.append({
            "name": "lakebase",
            "description": "Lakebase for session persistence",
            "database": {
                "instance_name": LAKEBASE_INSTANCE,
                "database_name": LAKEBASE_DATABASE,
                "permission": "CAN_CONNECT_AND_CREATE",
            },
        })
        added.append(f"lakebase ({LAKEBASE_INSTANCE}/{LAKEBASE_DATABASE})")

    if not added:
        print("  All resources already present")
        return

    req = urllib.request.Request(
        f"{host}/api/2.0/apps/{APP_NAME}",
        data=json.dumps({"resources": resources}).encode(),
        method="PATCH",
        headers=headers,
    )
    try:
        resp = json.loads(urllib.request.urlopen(req).read())
        total = len(resp.get("resources", []))
        print(f"  Added: {', '.join(added)} ({total} total)")
    except Exception as e:
        error = e.read().decode() if hasattr(e, "read") else str(e)
        print(f"  Warning: {error[:200]}")


def grant_uc_permissions(app):
    """Grant UC table permissions to the App SP."""
    sp_client_id = app.get("service_principal_client_id", "")
    if not sp_client_id:
        print("  No SP client ID found — skipping UC grants")
        return

    grants = [
        f"GRANT USE_CATALOG ON CATALOG {UC_CATALOG} TO `{sp_client_id}`",
        f"GRANT USE_SCHEMA ON SCHEMA {UC_FULL} TO `{sp_client_id}`",
        f"GRANT SELECT ON SCHEMA {UC_FULL} TO `{sp_client_id}`",
        f"GRANT EXECUTE ON FUNCTION {UC_FULL}.get_weather TO `{sp_client_id}`",
    ]
    for sql in grants:
        result = run_sql(sql)
        status = "OK" if result else "FAIL"
        print(f"  {status}: {sql.split('TO')[0].strip()}")


if __name__ == "__main__":
    print("=" * 60)
    print("App Resources + UC Grants")
    print("=" * 60)

    app = get_app_details()
    if not app:
        print("App not found — run bundle deploy first")
        sys.exit(1)

    print(f"\nApp: {app.get('name')}")
    print(f"SP: {app.get('service_principal_name')} ({app.get('service_principal_client_id')})")

    print("\n--- Adding app resources ---")
    add_resources(app)

    print("\n--- Granting UC permissions ---")
    grant_uc_permissions(app)

    print(f"\n{'=' * 60}")
    print("Done")
    print(f"{'=' * 60}")
