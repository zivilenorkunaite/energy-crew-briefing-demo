"""Shared helpers for setup scripts — profile, auth, SQL, resource discovery."""

import subprocess
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from server.branding import UC_FULL, UC_CATALOG, UC_SCHEMA

PROFILE = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")

# Lazy-initialized — populated by get_workspace_config()
_config = {}


def get_workspace_config() -> dict:
    """Discover workspace host, user, and token from the CLI profile."""
    global _config
    if _config:
        return _config

    result = subprocess.run(
        ["databricks", "auth", "describe", "--profile", PROFILE, "--output", "json"],
        capture_output=True, text=True,
    )
    if result.returncode == 0 and result.stdout.strip():
        try:
            data = json.loads(result.stdout)
            # username is at top level
            _config["user"] = data.get("username", "")
            # host may be at top level or nested in details
            host = data.get("host", "")
            if not host:
                host = (data.get("details", {}).get("configuration", {})
                        .get("host", {}).get("value", ""))
            _config["host"] = host.rstrip("/")
        except json.JSONDecodeError:
            pass

    if not _config.get("host"):
        # Fallback: read from SDK
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient(profile=PROFILE)
            _config["host"] = w.config.host.rstrip("/")
            _config["user"] = ""
        except Exception:
            pass

    if not _config.get("host"):
        raise RuntimeError(f"Cannot determine workspace host from profile '{PROFILE}'. Check 'databricks auth describe --profile {PROFILE}'")

    return _config


def get_host() -> str:
    return get_workspace_config()["host"]


def get_user() -> str:
    return get_workspace_config().get("user", "")


def run_cli(args: list[str], parse_json=True):
    """Run a databricks CLI command and return parsed output."""
    cmd = ["databricks"] + args + ["--profile", PROFILE]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  ERROR: {result.stderr.strip()[:300]}")
        return None
    if parse_json and result.stdout.strip():
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            return result.stdout.strip()
    return result.stdout.strip()


def run_sql(statement: str, warehouse_id: str = None):
    """Execute SQL via the SQL Statements API."""
    wh = warehouse_id or get_warehouse_id()
    payload = {
        "statement": statement,
        "warehouse_id": wh,
        "wait_timeout": "50s",
    }
    result = run_cli([
        "api", "post", "/api/2.0/sql/statements/",
        "--json", json.dumps(payload),
    ])
    if result and isinstance(result, dict):
        status = result.get("status", {}).get("state", "")
        if status == "FAILED":
            err = result.get("status", {}).get("error", {}).get("message", "Unknown")
            print(f"  SQL FAILED: {err[:300]}")
            return None
    return result


# ── Resource discovery (cached) ──

_warehouse_id = None


def get_warehouse_id() -> str:
    """Get or create a SQL warehouse for setup operations."""
    global _warehouse_id
    if _warehouse_id:
        return _warehouse_id

    # Check env var first
    from_env = os.environ.get("WAREHOUSE_ID")
    if from_env:
        _warehouse_id = from_env
        return _warehouse_id

    # Try to find an existing serverless warehouse
    result = run_cli(["api", "get", "/api/2.0/sql/warehouses/"])
    if result and isinstance(result, dict):
        for wh in result.get("warehouses", []):
            if wh.get("state") in ("RUNNING", "STOPPED") and wh.get("enable_serverless_compute"):
                _warehouse_id = wh["id"]
                print(f"  Using existing warehouse: {wh.get('name', '')} ({_warehouse_id})")
                return _warehouse_id

    # Create a new serverless warehouse
    print("  Creating serverless SQL warehouse...")
    payload = {
        "name": "energy-crew-briefing-setup",
        "cluster_size": "2X-Small",
        "auto_stop_mins": 10,
        "warehouse_type": "PRO",
        "enable_serverless_compute": True,
        "tags": {"custom_tags": [{"key": "demo", "value": "energy_crew_briefing"}]},
    }
    result = run_cli([
        "api", "post", "/api/2.0/sql/warehouses/",
        "--json", json.dumps(payload),
    ])
    if result and isinstance(result, dict) and result.get("id"):
        _warehouse_id = result["id"]
        print(f"  Created warehouse: {_warehouse_id}")

        # Wait for it to be running
        import time
        for _ in range(30):
            status = run_cli(["api", "get", f"/api/2.0/sql/warehouses/{_warehouse_id}"])
            if status and status.get("state") == "RUNNING":
                break
            time.sleep(10)

        return _warehouse_id

    raise RuntimeError("Could not find or create a SQL warehouse")


_app_sp_id = None


def get_app_sp_id(app_name: str = "energy-crew-briefing") -> str:
    """Get the Service Principal ID for the app (created by DAB deploy)."""
    global _app_sp_id
    if _app_sp_id:
        return _app_sp_id

    from_env = os.environ.get("APP_SP_ID")
    if from_env:
        _app_sp_id = from_env
        return _app_sp_id

    # Look up the app to find its SP
    result = run_cli(["api", "get", f"/api/2.0/apps/{app_name}"])
    if result and isinstance(result, dict):
        sp = result.get("service_principal_id")
        if sp:
            _app_sp_id = str(sp)
            print(f"  App SP ID: {_app_sp_id}")
            return _app_sp_id
        # Try service_principal_name
        spn = result.get("service_principal_name")
        if spn:
            _app_sp_id = spn
            print(f"  App SP name: {_app_sp_id}")
            return _app_sp_id

    print(f"  Warning: Could not find SP for app '{app_name}'. Grants may fail.")
    print(f"  Set APP_SP_ID env var manually if needed.")
    _app_sp_id = ""
    return _app_sp_id
