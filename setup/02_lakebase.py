"""Phase 2: Create Lakebase instance + database + tables for conversation memory.

Run once from local machine with Databricks CLI configured (DEFAULT profile).

Steps:
  1. Create Lakebase instance 'ee-crew-briefing' + database 'crew_briefing'
  2. Create conversations + messages tables (via psycopg2)
  3. Grant App SP connect + table access
"""

import subprocess
import json
import time
import sys

PROFILE = "DEFAULT"
LAKEBASE_INSTANCE = "ee-crew-briefing"
DATABASE = "crew_briefing"
APP_SP_ID = "84fba77d-2b5d-40ef-94e4-a0c81b5af427"


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


def step1_create_instance():
    """Create Lakebase instance and database."""
    print("\n=== Step 1: Create Lakebase instance ===")

    # Check if instance exists
    result = run_cli(["database", "get-database-instance", LAKEBASE_INSTANCE])
    if result and isinstance(result, dict) and result.get("name") == LAKEBASE_INSTANCE:
        state = result.get("state", "")
        print(f"  Instance '{LAKEBASE_INSTANCE}' already exists (state: {state})")
        host = result.get("read_write_dns", "")
        if host:
            print(f"  PGHOST: {host}")
        return result

    print(f"  Creating instance '{LAKEBASE_INSTANCE}'...")
    result = run_cli([
        "database", "create-database-instance",
        LAKEBASE_INSTANCE,
        "--capacity", "CU_1",
    ])
    if not result:
        print("  Failed to create instance.")
        sys.exit(1)

    # Poll until ready
    for i in range(60):
        time.sleep(10)
        result = run_cli(["database", "get-database-instance", LAKEBASE_INSTANCE])
        if result and isinstance(result, dict):
            state = result.get("state", "")
            print(f"  [{i*10}s] Instance state: {state}")
            if state == "RUNNING":
                host = result.get("read_write_dns", "")
                print(f"  PGHOST: {host}")
                return result
    print("  WARNING: Instance not ready in 10 minutes.")
    return result


def step2_create_database_and_tables():
    """Create database and tables using psycopg2."""
    print("\n=== Step 2: Create database + tables ===")

    # Get instance details
    result = run_cli(["database", "get-database-instance", "--name", LAKEBASE_INSTANCE])
    if not result or not isinstance(result, dict):
        print("  Cannot get instance details.")
        sys.exit(1)

    host = result.get("read_write_dns", "")
    if not host:
        print("  No read_write_dns found.")
        sys.exit(1)

    # Get token
    token_result = run_cli(["database", "generate-database-credential"])
    if not token_result or not isinstance(token_result, dict):
        print("  Cannot generate database credential.")
        sys.exit(1)
    token = token_result.get("token", "")

    import psycopg2

    # Create database
    print(f"  Connecting to {host} to create database '{DATABASE}'...")
    conn = psycopg2.connect(
        host=host,
        port=5432,
        database="databricks_postgres",
        user="zivile.norkunaite@databricks.com",
        password=token,
        sslmode="require",
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("SELECT datname FROM pg_database WHERE datname = %s", (DATABASE,))
    if cur.fetchone():
        print(f"  Database '{DATABASE}' already exists.")
    else:
        cur.execute(f'CREATE DATABASE "{DATABASE}"')
        print(f"  Database '{DATABASE}' created.")
    cur.close()
    conn.close()

    # Create tables in the new database
    print(f"  Creating tables in '{DATABASE}'...")
    conn = psycopg2.connect(
        host=host,
        port=5432,
        database=DATABASE,
        user="zivile.norkunaite@databricks.com",
        password=token,
        sslmode="require",
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS conversations (
            id SERIAL PRIMARY KEY,
            session_id VARCHAR(64) UNIQUE NOT NULL,
            title TEXT,
            user_email VARCHAR(255),
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id SERIAL PRIMARY KEY,
            session_id VARCHAR(64) REFERENCES conversations(session_id) ON DELETE CASCADE,
            role VARCHAR(16) NOT NULL,
            content TEXT,
            sources TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id)
    """)

    print("  Tables created.")
    cur.close()
    conn.close()


def step3_grant_sp():
    """Grant App SP connect + table access."""
    print("\n=== Step 3: Grant SP access ===")
    print(f"  Granting CONNECT on instance '{LAKEBASE_INSTANCE}' to SP {APP_SP_ID}...")

    # Grant via the database permissions API
    run_cli([
        "database", "grant-database-access",
        LAKEBASE_INSTANCE,
        "--principal-type", "SERVICE_PRINCIPAL",
        "--principal-id", APP_SP_ID,
    ])

    print("  Done.")


if __name__ == "__main__":
    print("=" * 60)
    print("Phase 2: Lakebase Conversation Memory Setup")
    print("=" * 60)
    step1_create_instance()
    step2_create_database_and_tables()
    step3_grant_sp()
    print("\n=== Phase 2 complete ===")
