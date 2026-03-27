"""Create Lakebase tables for conversation memory and caching.

The Lakebase instance + database are created by DAB (databricks.yml).
This script creates the application tables + grants SP access.

Run with: python3 setup/02_lakebase.py
"""

import json
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from helpers import run_cli, get_app_sp_id, get_user, PROFILE

LAKEBASE_INSTANCE = "energy-crew-briefing"
DATABASE = "crew_briefing"


def get_lakebase_host():
    """Get the Lakebase read/write DNS."""
    result = run_cli(["database", "get-database-instance", LAKEBASE_INSTANCE])
    if result and isinstance(result, dict):
        host = result.get("read_write_dns", "")
        state = result.get("state", "")
        print(f"  Instance: {LAKEBASE_INSTANCE} (state: {state})")
        print(f"  PGHOST: {host}")
        return host
    print(f"  Instance '{LAKEBASE_INSTANCE}' not found — run 'databricks bundle deploy' first")
    sys.exit(1)


def create_tables():
    """Create application tables in the Lakebase database."""
    print("\n=== Creating tables ===")

    host = get_lakebase_host()

    # Generate credential
    cred = run_cli([
        "database", "generate-database-credential",
        "--json", json.dumps({"instance_names": [LAKEBASE_INSTANCE]}),
    ])
    if not cred or not cred.get("token"):
        print("  Cannot generate database credential")
        sys.exit(1)

    import psycopg2
    conn = psycopg2.connect(
        host=host, port=5432, database=DATABASE,
        user=get_user(), password=cred["token"], sslmode="require",
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

    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id)")

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

    print("  Tables created (conversations, messages, tool_cache, app_settings)")

    # Grant SP access to tables
    sp_id = get_app_sp_id()
    if sp_id:
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (sp_id,))
        if cur.fetchone():
            cur.execute(f'GRANT ALL ON ALL TABLES IN SCHEMA public TO "{sp_id}"')
            cur.execute(f'GRANT USAGE, CREATE ON SCHEMA public TO "{sp_id}"')
            cur.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO "{sp_id}"')
            cur.execute(f'GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO "{sp_id}"')
            cur.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE ON SEQUENCES TO "{sp_id}"')
            print(f"  SP table grants applied ({sp_id[:20]}...)")
        else:
            print(f"  SP role not yet created — will be created by DAB on next deploy")

    cur.close()
    conn.close()


if __name__ == "__main__":
    print("=" * 60)
    print("Lakebase Tables Setup")
    print("=" * 60)
    create_tables()
    print("\n=== Done ===")
