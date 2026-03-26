"""Create Genie Room for field operations data.

Creates a Genie Room pointing to the work_orders and work_tasks tables.
Idempotent — skips if a room with the same title already exists.

Run with: python3 setup/12_genie_room.py
"""

import json
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from helpers import run_cli, get_warehouse_id, get_app_sp_id, UC_FULL
ROOM_TITLE = "Field Operations"
ROOM_DESCRIPTION = (
    "Field operations data for energy distribution crews. "
    "Ask about work orders, crew assignments, task schedules, and project status."
)
SAMPLE_QUESTIONS = [
    "What work orders are scheduled for Grafton Lines A tomorrow?",
    "Show overdue work orders by crew",
    "Which crews have the most work orders this week?",
    "What tasks are assigned to Tamworth Lines today?",
    "Show work order status breakdown by type",
]


def step1_create_room():
    """Create the Genie Room via API."""
    print("\n=== Step 1: Create Genie Room ===")

    # Check if room already exists by listing spaces
    result = run_cli([
        "api", "get", "/api/2.0/genie/spaces",
    ])
    if result and isinstance(result, dict):
        for space in result.get("spaces", []):
            if space.get("title") == ROOM_TITLE:
                space_id = space["space_id"]
                print(f"  Room already exists: {ROOM_TITLE} (ID: {space_id})")
                return space_id

    # Create the room
    payload = {
        "title": ROOM_TITLE,
        "description": ROOM_DESCRIPTION,
        "warehouse_id": get_warehouse_id(),
        "table_identifiers": [
            f"{UC_FULL}.work_orders",
            f"{UC_FULL}.work_tasks",
            f"{UC_FULL}.assets",
            f"{UC_FULL}.asset_types",
        ],
        "sample_questions": SAMPLE_QUESTIONS,
    }

    result = run_cli([
        "api", "post", "/api/2.0/genie/spaces",
        "--json", json.dumps(payload),
    ])

    if result and isinstance(result, dict):
        space_id = result.get("space_id")
        if space_id:
            print(f"  Created room: {ROOM_TITLE} (ID: {space_id})")
            return space_id

    print("  Failed to create Genie Room.")
    return None


def step2_grant_access(space_id: str):
    """Grant App SP CAN_RUN on the Genie Room."""
    print(f"\n=== Step 2: Grant CAN_RUN to App SP ===")

    sp_id = get_app_sp_id()
    payload = {
        "access_control_list": [
            {
                "service_principal_name": sp_id,
                "permission_level": "CAN_RUN",
            }
        ]
    }

    result = run_cli([
        "api", "patch", f"/api/2.0/permissions/genie/{space_id}",
        "--json", json.dumps(payload),
    ])

    if result:
        print("  CAN_RUN granted.")
    else:
        print("  Warning: grant may have failed. Check manually.")


if __name__ == "__main__":
    print("=" * 60)
    print("Genie Room — Field Operations Setup")
    print("=" * 60)

    space_id = step1_create_room()
    if space_id:
        step2_grant_access(space_id)
        print(f"\n{'=' * 60}")
        print(f"Genie Room ID: {space_id}")
        print(f"")
        print(f"If this is a new room, update these files:")
        print(f"  databricks.yml → genie-field-ops → space_id: \"{space_id}\"")
        print(f"{'=' * 60}")
    else:
        print("\nFailed — check errors above.")
