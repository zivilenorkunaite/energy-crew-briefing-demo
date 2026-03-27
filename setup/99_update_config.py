"""Update databricks.yml and app.yaml with discovered resource IDs after setup.

Reads workspace state (Lakebase endpoint, Genie Room ID, SQL Warehouse ID)
and patches the config files.

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


def discover_lakebase():
    """Find Lakebase PGHOST."""
    result = run_cli(["database", "get-database-instance", LAKEBASE_INSTANCE])
    if not result or not isinstance(result, dict):
        return None
    return result.get("read_write_dns", "")


def discover_genie_room():
    """Find the Field Operations Genie Room ID."""
    result = run_cli(["api", "get", "/api/2.0/genie/spaces"])
    if result and isinstance(result, dict):
        for space in result.get("spaces", []):
            if space.get("title") == "Field Operations":
                return space["space_id"]
    return None


def inject_env_var(content, var_name, value):
    """Add or update an env var in databricks.yml content."""
    pattern = rf"(- name: {var_name}\n\s+value: ).*"
    if re.search(pattern, content):
        return re.sub(pattern, rf"\g<1>{value}", content)
    # Insert before AGENT_VERSION (last env var)
    insert_before = "          - name: AGENT_VERSION"
    if insert_before in content:
        new_var = f"          - name: {var_name}\n            value: {value}\n"
        return content.replace(insert_before, new_var + insert_before)
    return content


def update_configs(pghost, genie_id, warehouse_id):
    """Update databricks.yml and app.yaml."""
    # databricks.yml
    yml_path = os.path.join(PROJECT_DIR, "databricks.yml")
    with open(yml_path, "r") as f:
        content = f.read()

    changed = False
    if pghost and "PGHOST" not in content:
        content = inject_env_var(content, "PGHOST", pghost)
        changed = True
    if pghost and "ENDPOINT_NAME" not in content:
        content = inject_env_var(content, "ENDPOINT_NAME", LAKEBASE_INSTANCE)
        changed = True
    if genie_id:
        # Update existing GENIE_SPACE_ID value
        content = re.sub(
            r"(- name: GENIE_SPACE_ID\n\s+value: ).*",
            rf'\g<1>"{genie_id}"',
            content,
        )
        changed = True

    if changed:
        with open(yml_path, "w") as f:
            f.write(content)
        print(f"  Updated {yml_path}")
    else:
        print(f"  {yml_path} already up to date")

    # app.yaml
    yaml_path = os.path.join(PROJECT_DIR, "app.yaml")
    with open(yaml_path, "r") as f:
        content = f.read()

    lines_to_add = []
    if pghost and "PGHOST" not in content:
        lines_to_add.append(f"  - name: PGHOST\n    value: '{pghost}'")
    if "ENDPOINT_NAME" not in content:
        lines_to_add.append(f"  - name: ENDPOINT_NAME\n    value: '{LAKEBASE_INSTANCE}'")

    if lines_to_add:
        content = content.rstrip() + "\n" + "\n".join(lines_to_add) + "\n"
        with open(yaml_path, "w") as f:
            f.write(content)
        print(f"  Updated {yaml_path}")
    else:
        print(f"  {yaml_path} already up to date")


if __name__ == "__main__":
    print("=" * 60)
    print("Updating config files with discovered resource IDs")
    print("=" * 60)

    print("\n--- Discovering Lakebase ---")
    pghost = discover_lakebase()
    print(f"  PGHOST: {pghost or '(not found)'}")

    print("\n--- Discovering Genie Room ---")
    genie_id = discover_genie_room()
    print(f"  Genie Room ID: {genie_id or '(not found)'}")

    print("\n--- Discovering SQL Warehouse ---")
    warehouse_id = get_warehouse_id()
    print(f"  Warehouse ID: {warehouse_id}")

    print("\n--- Updating config files ---")
    update_configs(pghost, genie_id, warehouse_id)

    print(f"\n{'=' * 60}")
    print("Config updated. Run 'databricks bundle deploy' next.")
    print(f"{'=' * 60}")
