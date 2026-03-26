"""Update databricks.yml and app.yaml with discovered resource IDs after setup.

Reads workspace state (Lakebase endpoint, Genie Room ID, SQL Warehouse ID,
MLflow experiment ID) and patches the config files.

Run with: python3 setup/99_update_config.py
"""

import json
import os
import sys
import re

sys.path.insert(0, os.path.dirname(__file__))
from helpers import run_cli, get_warehouse_id, get_host, UC_FULL

PROJECT_DIR = os.path.join(os.path.dirname(__file__), "..")
LAKEBASE_INSTANCE = "energy-crew-briefing"
APP_NAME = "energy-crew-briefing"


def discover_lakebase():
    """Find Lakebase PGHOST and ENDPOINT_NAME."""
    result = run_cli(["database", "get-database-instance", LAKEBASE_INSTANCE])
    if not result or not isinstance(result, dict):
        print("  Lakebase instance not found — skipping")
        return None, None

    # Extract endpoint host from the instance
    endpoint = result.get("endpoint", "")
    pghost = ""
    endpoint_name = ""

    # The endpoint info is in the instance response
    instance_id = result.get("instance_id", "")
    if not instance_id:
        # Try name field
        instance_id = result.get("name", "")

    # Get the endpoint details
    result2 = run_cli(["api", "get", f"/api/2.0/database/instances/{LAKEBASE_INSTANCE}"])
    if result2 and isinstance(result2, dict):
        for branch in result2.get("branches", []):
            for ep in branch.get("endpoints", []):
                pghost = ep.get("host", "")
                endpoint_name = ep.get("name", "")
                if pghost:
                    break
            if pghost:
                break

    if not pghost:
        # Try alternative API
        result3 = run_cli(["database", "list-database-endpoints", LAKEBASE_INSTANCE])
        if result3 and isinstance(result3, dict):
            endpoints = result3.get("endpoints", [])
            if endpoints:
                pghost = endpoints[0].get("host", "")
                endpoint_name = endpoints[0].get("name", "")

    return pghost, endpoint_name


def discover_genie_room():
    """Find the Field Operations Genie Room ID."""
    result = run_cli(["api", "get", "/api/2.0/genie/spaces"])
    if result and isinstance(result, dict):
        for space in result.get("spaces", []):
            if space.get("title") == "Field Operations":
                return space["space_id"]
    return None


def discover_experiment():
    """Find the MLflow experiment ID."""
    path = "/Shared/energy-crew-briefing-traces-uc"
    result = run_cli([
        "api", "post", "/api/2.0/mlflow/experiments/get-by-name",
        "--json", json.dumps({"experiment_name": path}),
    ])
    if result and isinstance(result, dict) and result.get("experiment"):
        return result["experiment"]["experiment_id"]
    return None


def update_yaml(filepath, updates):
    """Simple key-value updates in a YAML file (preserves formatting)."""
    with open(filepath, "r") as f:
        content = f.read()

    for key, value in updates.items():
        # Match patterns like: key: old_value or key: "old_value" or key: 'old_value'
        pattern = rf"({key}:\s*)['\"]?[^'\"\n]*['\"]?"
        replacement = rf"\g<1>{value}"
        content = re.sub(pattern, replacement, content)

    with open(filepath, "w") as f:
        f.write(content)


def update_databricks_yml(pghost, endpoint_name, genie_id, experiment_id, warehouse_id):
    """Update databricks.yml with discovered values."""
    filepath = os.path.join(PROJECT_DIR, "databricks.yml")

    with open(filepath, "r") as f:
        content = f.read()

    # Add PGHOST, ENDPOINT_NAME env vars if not present
    env_vars_to_add = []
    if pghost and "PGHOST" not in content:
        env_vars_to_add.append(f"          - name: PGHOST\n            value: \"{pghost}\"")
    if endpoint_name and "ENDPOINT_NAME" not in content:
        env_vars_to_add.append(f"          - name: ENDPOINT_NAME\n            value: \"{endpoint_name}\"")

    if env_vars_to_add:
        # Insert after PGSSLMODE line
        insert_point = content.find("PGSSLMODE")
        if insert_point >= 0:
            line_end = content.find("\n", insert_point)
            next_line_end = content.find("\n", line_end + 1)
            content = content[:next_line_end + 1] + "\n".join(env_vars_to_add) + "\n" + content[next_line_end + 1:]

    # Add genie resource if not present
    if genie_id and "genie-field-ops" not in content:
        genie_block = f"""
        - name: genie-field-ops
          description: "Genie Room — Field Operations"
          genie_space:
            name: "Field Operations"
            space_id: "{genie_id}"
            permission: CAN_RUN
"""
        # Insert before the Lakebase comment
        lakebase_comment = content.find("# Lakebase postgres")
        if lakebase_comment >= 0:
            content = content[:lakebase_comment] + genie_block + "\n        " + content[lakebase_comment:]

    # Add experiment resource if not present
    if experiment_id and "mlflow-traces" not in content:
        exp_block = f"""
        - name: mlflow-traces
          description: "MLflow experiment for agent tracing"
          experiment:
            experiment_id: "{experiment_id}"
            permission: CAN_MANAGE
"""
        lakebase_comment = content.find("# Lakebase postgres")
        if lakebase_comment >= 0:
            content = content[:lakebase_comment] + exp_block + "\n        " + content[lakebase_comment:]

    # Add warehouse resource if not present
    if warehouse_id and "sql-warehouse" not in content:
        wh_block = f"""
        - name: sql-warehouse
          description: "SQL Warehouse for data queries"
          sql_warehouse:
            id: {warehouse_id}
            permission: CAN_USE
"""
        lakebase_comment = content.find("# Lakebase postgres")
        if lakebase_comment >= 0:
            content = content[:lakebase_comment] + wh_block + "\n        " + content[lakebase_comment:]

    with open(filepath, "w") as f:
        f.write(content)

    print(f"  Updated {filepath}")


def update_app_yaml(pghost, endpoint_name):
    """Update app.yaml with Lakebase details."""
    filepath = os.path.join(PROJECT_DIR, "app.yaml")

    with open(filepath, "r") as f:
        content = f.read()

    lines_to_add = []
    if pghost and "PGHOST" not in content:
        lines_to_add.append(f"  - name: PGHOST\n    value: '{pghost}'")
    if endpoint_name and "ENDPOINT_NAME" not in content:
        lines_to_add.append(f"  - name: ENDPOINT_NAME\n    value: '{endpoint_name}'")

    if lines_to_add:
        content = content.rstrip() + "\n" + "\n".join(lines_to_add) + "\n"
        with open(filepath, "w") as f:
            f.write(content)
        print(f"  Updated {filepath}")
    else:
        print(f"  {filepath} already up to date")


if __name__ == "__main__":
    print("=" * 60)
    print("Updating config files with discovered resource IDs")
    print("=" * 60)

    print("\n--- Discovering Lakebase ---")
    pghost, endpoint_name = discover_lakebase()
    print(f"  PGHOST: {pghost or '(not found)'}")
    print(f"  ENDPOINT_NAME: {endpoint_name or '(not found)'}")

    print("\n--- Discovering Genie Room ---")
    genie_id = discover_genie_room()
    print(f"  Genie Room ID: {genie_id or '(not found)'}")

    print("\n--- Discovering MLflow Experiment ---")
    experiment_id = discover_experiment()
    print(f"  Experiment ID: {experiment_id or '(not found)'}")

    print("\n--- Discovering SQL Warehouse ---")
    warehouse_id = get_warehouse_id()
    print(f"  Warehouse ID: {warehouse_id}")

    print("\n--- Updating config files ---")
    update_databricks_yml(pghost, endpoint_name, genie_id, experiment_id, warehouse_id)
    update_app_yaml(pghost, endpoint_name)

    print(f"\n{'=' * 60}")
    print("Config files updated. Run 'databricks bundle deploy' next.")
    print(f"{'=' * 60}")
