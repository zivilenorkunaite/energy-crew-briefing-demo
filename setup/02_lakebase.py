"""Phase 2: Create Lakebase instance + database + tables for conversation memory.

Run once from local machine with Databricks CLI configured (DEFAULT profile).

Steps:
  1. Create Lakebase instance 'energy-crew-briefing' + database 'crew_briefing'
  2. Create conversations + messages tables (via psycopg2)
  3. Grant App SP connect + table access
"""

import json
import time
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from helpers import run_cli, get_app_sp_id, get_user, PROFILE

LAKEBASE_INSTANCE = "energy-crew-briefing"
DATABASE = "crew_briefing"


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
            if state in ("RUNNING", "AVAILABLE"):
                host = result.get("read_write_dns", "")
                print(f"  PGHOST: {host}")
                return result
    print("  WARNING: Instance not ready in 10 minutes.")
    return result


def step2_create_database_and_tables():
    """Create database and tables using psycopg2."""
    print("\n=== Step 2: Create database + tables ===")

    # Get instance details
    result = run_cli(["database", "get-database-instance", LAKEBASE_INSTANCE])
    if not result or not isinstance(result, dict):
        print("  Cannot get instance details.")
        sys.exit(1)

    host = result.get("read_write_dns", "")
    if not host:
        print("  No read_write_dns found.")
        sys.exit(1)

    # Get token
    token_result = run_cli([
        "database", "generate-database-credential",
        "--json", json.dumps({"instance_names": [LAKEBASE_INSTANCE]}),
    ])
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
        user=get_user(),
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
        user=get_user(),
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

    cur.execute("""
        CREATE TABLE IF NOT EXISTS tool_cache (
            cache_key VARCHAR(64) PRIMARY KEY,
            tool_name VARCHAR(64) NOT NULL,
            args_json TEXT,
            result TEXT,
            ttl_seconds INTEGER DEFAULT 86400,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS app_settings (
            key VARCHAR(128) PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    print("  Tables created (conversations, messages, tool_cache, app_settings).")
    cur.close()
    conn.close()


def step3_grant_sp():
    """Grant App SP connect + table access."""
    print("\n=== Step 3: Grant SP access ===")
    sp_id = get_app_sp_id()
    print(f"  Granting CONNECT on instance '{LAKEBASE_INSTANCE}' to SP {sp_id}...")

    # Grant via the database permissions API
    run_cli([
        "database", "grant-database-access",
        LAKEBASE_INSTANCE,
        "--principal-type", "SERVICE_PRINCIPAL",
        "--principal-id", sp_id,
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
