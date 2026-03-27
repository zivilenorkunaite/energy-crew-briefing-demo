#!/bin/bash
# Deploy Energy Crew Briefing via Databricks Asset Bundles
#
# Usage:
#   DATABRICKS_PROFILE=azure-aus ./deploy.sh --setup  — first-time setup + deploy
#   DATABRICKS_PROFILE=azure-aus ./deploy.sh           — code deploy only
#
# Teardown: DATABRICKS_PROFILE=azure-aus ./teardown.sh --confirm
set -euo pipefail

PROFILE="${DATABRICKS_PROFILE:-DEFAULT}"
APP_NAME="energy-crew-briefing"
SECRET_SCOPE="energy-crew-briefing"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== Energy Crew Briefing — Deploy (profile: $PROFILE) ==="

# ── Handle flags ──────────────────────────────────────────────────────
RUN_SETUP=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --setup) RUN_SETUP=true; shift ;;
        *) shift ;;
    esac
done

# Export profile so all Python setup scripts pick it up
export DATABRICKS_PROFILE="$PROFILE"

# ── First-time setup ─────────────────────────────────────────────────
if $RUN_SETUP; then
    echo ""
    echo "=== First-time setup (10 phases) ==="

    echo ""
    echo "--- Phase 1: Prerequisites (secret scope + UC catalog) ---"
    databricks secrets create-scope "$SECRET_SCOPE" --profile "$PROFILE" 2>/dev/null \
        && echo "  Created scope '$SECRET_SCOPE'" \
        || echo "  Scope '$SECRET_SCOPE' already exists"

    # Verify UC catalog exists (create if possible, warn if not)
    UC_CATALOG=$(python3 -c "from server.branding import UC_CATALOG; print(UC_CATALOG)")
    if databricks api get "/api/2.1/unity-catalog/catalogs/$UC_CATALOG" --profile "$PROFILE" >/dev/null 2>&1; then
        echo "  Catalog '$UC_CATALOG' exists"
    else
        databricks api post /api/2.1/unity-catalog/catalogs \
            --json "{\"name\": \"$UC_CATALOG\", \"comment\": \"Energy Crew Briefing demo\"}" \
            --profile "$PROFILE" 2>/dev/null \
            && echo "  Created catalog '$UC_CATALOG'" \
            || echo "  WARNING: Cannot create catalog '$UC_CATALOG'. Ask a metastore admin to create it."
    fi

    echo ""
    echo "--- Phase 2: Lakebase (instance + database + tables) ---"
    python3 "$SCRIPT_DIR/setup/02_lakebase.py"

    echo ""
    echo "--- Phase 3: SWMS Documents (seed Delta table) ---"
    python3 "$SCRIPT_DIR/setup/11_seed_swms.py"

    echo ""
    echo "--- Phase 4: Vector Search (endpoint + index) ---"
    python3 "$SCRIPT_DIR/setup/01_vector_search.py"

    echo ""
    echo "--- Phase 5: BOM Weather (table + UC function + data) ---"
    python3 "$SCRIPT_DIR/setup/03_bom_weather.py"

    echo ""
    echo "--- Phase 6: MLflow Experiment ---"
    python3 "$SCRIPT_DIR/setup/04_mlflow_experiment.py"

    echo ""
    echo "--- Phase 7: Demo Data (assets + work orders + tasks) ---"
    python3 "$SCRIPT_DIR/setup/05_realistic_data.py"

    echo ""
    echo "--- Phase 8: Genie Room ---"
    python3 "$SCRIPT_DIR/setup/12_genie_room.py"

    echo ""
    echo "--- Phase 9: SWMS Serving Endpoint ---"
    python3 "$SCRIPT_DIR/setup/06_swms_agent.py" 2>&1 || echo "  Phase 9 failed — deploy SWMS endpoint manually: python3 setup/06_swms_agent.py"

    echo ""
    echo "--- Phase 10: Prompt Registry ---"
    python3 "$SCRIPT_DIR/setup/08_prompt_registry.py"

    echo ""
    echo "--- Updating config with discovered resource IDs ---"
    python3 "$SCRIPT_DIR/setup/99_update_config.py"

    echo ""
    echo "=== Setup complete ==="
    echo ""
    echo "MANUAL STEP REMAINING:"
    echo "  Put Tavily API key in secret scope:"
    echo "  databricks secrets put-secret $SECRET_SCOPE tavily-api-key --profile $PROFILE"
    echo ""
fi

# ── Bundle deploy ─────────────────────────────────────────────────────
echo "Running bundle deploy..."
cd "$SCRIPT_DIR"
databricks bundle deploy --profile "$PROFILE"
echo "Bundle deployed"

# ── Postgres resource (not supported in DABs) ─────────────────────────
echo ""
echo "Adding Lakebase postgres resource..."
python3 "$SCRIPT_DIR/setup/13_postgres_resource.py"

# ── App deploy (restart with new code) ────────────────────────────────
echo ""
# Discover the bundle path from the current user
USER_EMAIL=$(databricks auth describe --profile "$PROFILE" 2>/dev/null | grep "User:" | awk '{print $2}')
if [ -z "$USER_EMAIL" ]; then
    USER_EMAIL=$(python3 -c "from databricks.sdk import WorkspaceClient; w=WorkspaceClient(profile='$PROFILE'); print(w.current_user.me().user_name)")
fi
BUNDLE_PATH="/Workspace/Users/${USER_EMAIL}/.bundle/energy-crew-briefing/default/files"

echo "Deploying app from: $BUNDLE_PATH"
databricks apps deploy "$APP_NAME" \
    --source-code-path "$BUNDLE_PATH" \
    --profile "$PROFILE"

echo ""
echo "=== Deploy complete ==="
# Get workspace host for app URL
HOST=$(databricks auth describe --profile "$PROFILE" 2>/dev/null | grep "Host:" | awk '{print $2}' | sed 's|https://||')
echo "Workspace: $HOST"
echo "App: https://${APP_NAME}-*.databricksapps.com (check Databricks UI for exact URL)"
