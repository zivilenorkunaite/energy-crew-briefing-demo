"""Phase 1: Create Vector Search endpoint + Delta Sync index for SWMS documents.

Run once from local machine with Databricks CLI configured (DEFAULT profile).
Prerequisites: Table `zivile.essential_energy_wacs.swms_documents` must exist with columns:
  work_type, section_title, content, document_name

Steps:
  1. Add chunk_id identity column + enable Change Data Feed
  2. Create Vector Search endpoint (ee-crew-briefing-vs)
  3. Create Delta Sync index on content column
  4. Grant App SP query access
"""

import subprocess
import json
import time
import sys

PROFILE = "DEFAULT"
CATALOG = "zivile"
SCHEMA = "essential_energy_wacs"
TABLE = f"{CATALOG}.{SCHEMA}.swms_documents"
VS_ENDPOINT = "ee-crew-briefing-vs"
VS_INDEX = f"{CATALOG}.{SCHEMA}.swms_documents_vs_index"
EMBEDDING_MODEL = "databricks-gte-large-en"
APP_SP_ID = "84fba77d-2b5d-40ef-94e4-a0c81b5af427"
WAREHOUSE_ID = "c2abb17a6c9e6bc0"


def run_cli(args: list[str], parse_json=True):
    """Run a databricks CLI command and return parsed output."""
    cmd = ["databricks"] + args + ["--profile", PROFILE]
    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  ERROR: {result.stderr.strip()}")
        return None
    if parse_json and result.stdout.strip():
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            return result.stdout.strip()
    return result.stdout.strip()


def run_sql(statement: str):
    """Execute SQL via the SQL Statements API."""
    payload = {
        "statement": statement,
        "warehouse_id": WAREHOUSE_ID,
        "wait_timeout": "30s",
    }
    result = run_cli([
        "api", "post", "/api/2.0/sql/statements/",
        "--json", json.dumps(payload),
    ])
    if result and isinstance(result, dict):
        status = result.get("status", {}).get("state", "")
        if status == "FAILED":
            err = result.get("status", {}).get("error", {}).get("message", "Unknown")
            print(f"  SQL FAILED: {err}")
            return None
    return result


def step1_alter_table():
    """Enable Change Data Feed (table already has an 'id' column for PK)."""
    print("\n=== Step 1: Enable Change Data Feed ===")
    run_sql(f"ALTER TABLE {TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    print("  Done.")


def step2_create_endpoint():
    """Create Vector Search endpoint."""
    print("\n=== Step 2: Create Vector Search endpoint ===")

    # Check if endpoint already exists
    result = run_cli(["api", "get", f"/api/2.0/vector-search/endpoints/{VS_ENDPOINT}"])
    if result and isinstance(result, dict) and result.get("name") == VS_ENDPOINT:
        status = result.get("endpoint_status", {}).get("state", "")
        print(f"  Endpoint '{VS_ENDPOINT}' already exists (status: {status})")
        if status == "ONLINE":
            return
        print("  Waiting for endpoint to come online...")
    else:
        print(f"  Creating endpoint '{VS_ENDPOINT}'...")
        run_cli([
            "api", "post", "/api/2.0/vector-search/endpoints",
            "--json", json.dumps({
                "name": VS_ENDPOINT,
                "endpoint_type": "STANDARD",
            }),
        ])

    # Poll until online (up to 10 minutes)
    for i in range(60):
        time.sleep(10)
        result = run_cli(["api", "get", f"/api/2.0/vector-search/endpoints/{VS_ENDPOINT}"])
        if result and isinstance(result, dict):
            status = result.get("endpoint_status", {}).get("state", "")
            print(f"  [{i*10}s] Endpoint status: {status}")
            if status == "ONLINE":
                return
    print("  WARNING: Endpoint did not come online within 10 minutes. Check workspace UI.")


def step3_create_index():
    """Create Delta Sync index on content column."""
    print("\n=== Step 3: Create Delta Sync index ===")

    # Check if index exists
    result = run_cli(["api", "get", f"/api/2.0/vector-search/indexes/{VS_INDEX}"])
    if result and isinstance(result, dict) and result.get("name") == VS_INDEX:
        status = result.get("status", {}).get("ready")
        print(f"  Index '{VS_INDEX}' already exists (ready: {status})")
        return

    print(f"  Creating index '{VS_INDEX}'...")
    payload = {
        "name": VS_INDEX,
        "endpoint_name": VS_ENDPOINT,
        "primary_key": "id",
        "index_type": "DELTA_SYNC",
        "delta_sync_index_spec": {
            "source_table": TABLE,
            "pipeline_type": "TRIGGERED",
            "embedding_source_columns": [
                {
                    "name": "content",
                    "embedding_model_endpoint_name": EMBEDDING_MODEL,
                }
            ],
        },
    }
    result = run_cli([
        "api", "post", "/api/2.0/vector-search/indexes",
        "--json", json.dumps(payload),
    ])
    if result:
        print(f"  Index creation initiated: {result}")

    # Poll for readiness (up to 5 minutes)
    for i in range(30):
        time.sleep(10)
        result = run_cli(["api", "get", f"/api/2.0/vector-search/indexes/{VS_INDEX}"])
        if result and isinstance(result, dict):
            status = result.get("status", {})
            ready = status.get("ready")
            print(f"  [{i*10}s] Index ready: {ready}")
            if ready:
                return
    print("  Index may still be syncing. Check workspace UI.")


def step4_grant_sp():
    """Grant App SP query access to the VS endpoint."""
    print("\n=== Step 4: Grant SP access ===")

    # Grant CAN_QUERY on the serving endpoint used for embeddings
    print(f"  Granting CAN_QUERY on VS endpoint to SP {APP_SP_ID}...")
    payload = {
        "access_control_list": [
            {
                "service_principal_name": APP_SP_ID,
                "all_permissions": [{"permission_level": "CAN_MANAGE"}],
            }
        ]
    }
    run_cli([
        "api", "put", f"/api/2.0/permissions/vector-search-endpoints/{VS_ENDPOINT}",
        "--json", json.dumps(payload),
    ])
    print("  Done.")


if __name__ == "__main__":
    print("=" * 60)
    print("Phase 1: SWMS Vector Search Setup")
    print("=" * 60)
    step1_alter_table()
    step2_create_endpoint()
    step3_create_index()
    step4_grant_sp()
    print("\n=== Phase 1 complete ===")
