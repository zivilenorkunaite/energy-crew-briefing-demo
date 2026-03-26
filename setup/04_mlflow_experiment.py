"""Phase 4: Create MLflow experiment for agent tracing.

Run once from local machine with Databricks CLI configured (DEFAULT profile).
"""

import json
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from helpers import run_cli, PROFILE

EXPERIMENT_PATH = "/Shared/energy-crew-briefing-traces-uc"


if __name__ == "__main__":
    print("=" * 60)
    print("Phase 4: MLflow Experiment Setup")
    print("=" * 60)

    # Check if experiment exists
    result = run_cli([
        "api", "post", "/api/2.0/mlflow/experiments/get-by-name",
        "--json", json.dumps({"experiment_name": EXPERIMENT_PATH}),
    ])
    if result and isinstance(result, dict) and result.get("experiment"):
        exp_id = result["experiment"]["experiment_id"]
        print(f"  Experiment already exists: {EXPERIMENT_PATH} (ID: {exp_id})")
    else:
        print(f"  Creating experiment: {EXPERIMENT_PATH}")
        result = run_cli([
            "api", "post", "/api/2.0/mlflow/experiments/create",
            "--json", json.dumps({"name": EXPERIMENT_PATH}),
        ])
        if result and isinstance(result, dict):
            exp_id = result.get("experiment_id", "?")
            print(f"  Created experiment ID: {exp_id}")
        else:
            print("  Failed to create experiment.")

    print("\n=== Phase 4 setup complete ===")
