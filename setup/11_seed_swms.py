"""Seed SWMS documents Delta table from swms_content.py.

Creates the table if it doesn't exist, then inserts/replaces all SWMS content.
Run with: python3 setup/11_seed_swms.py
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from helpers import run_cli, run_sql, get_app_sp_id, UC_FULL, UC_CATALOG

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from server.swms_content import SWMS_CONTENT

TABLE = f"{UC_FULL}.swms_documents"


def step1_create_table():
    print("\n=== Step 1: Create SWMS documents table ===")
    run_sql(f"CREATE SCHEMA IF NOT EXISTS {UC_FULL}")
    sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        document_name STRING NOT NULL,
        section_title STRING NOT NULL,
        content STRING
    )
    COMMENT 'Safe Work Method Statement content for energy distribution field work'
    """
    run_sql(sql)
    print("  Table ready.")


def step2_seed_data():
    print("\n=== Step 2: Seed SWMS content ===")

    # Clear existing data and re-insert (idempotent)
    run_sql(f"TRUNCATE TABLE {TABLE}")

    row_count = 0
    for doc_name, sections in SWMS_CONTENT.items():
        for section_title, content in sections.items():
            safe_doc = doc_name.replace("'", "''")
            safe_title = section_title.replace("'", "''")
            safe_content = content.replace("'", "''").replace("\\", "\\\\")
            run_sql(f"""
                INSERT INTO {TABLE} (document_name, section_title, content)
                VALUES ('{safe_doc}', '{safe_title}', '{safe_content}')
            """)
            row_count += 1
        print(f"  {doc_name}: {len(sections)} sections")

    print(f"  Total: {row_count} rows inserted")


def step3_grant_access():
    print("\n=== Step 3: Grant App SP access ===")
    sp_id = get_app_sp_id()
    run_sql(f"GRANT USE_CATALOG ON CATALOG {UC_CATALOG} TO `{sp_id}`")
    run_sql(f"GRANT USE_SCHEMA ON SCHEMA {UC_FULL} TO `{sp_id}`")
    run_sql(f"GRANT SELECT ON TABLE {TABLE} TO `{sp_id}`")
    print("  Grants applied.")


if __name__ == "__main__":
    print("=" * 60)
    print("SWMS Documents — Seed Delta Table")
    print("=" * 60)
    step1_create_table()
    step2_seed_data()
    step3_grant_access()
    print(f"\n{'=' * 60}")
    print(f"Done. {len(SWMS_CONTENT)} documents seeded into {TABLE}")
    print(f"{'=' * 60}")
