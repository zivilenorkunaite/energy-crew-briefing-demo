#!/bin/bash
# Deploy EE Crew Briefing app with all infrastructure components
# Usage: ./deploy.sh [--set-tavily-key YOUR_KEY] [--setup]
set -euo pipefail

PROFILE="${DATABRICKS_PROFILE:-DEFAULT}"
APP_NAME="ee-crew-briefing"
WS_PATH="/Workspace/Users/zivile.norkunaite@databricks.com/ee-crew-briefing"
SECRET_NAME="tavily-api-key"

echo "=== EE Crew Briefing — Deploy ==="

# ── Handle flags ──────────────────────────────────────────────────────
RUN_SETUP=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --set-tavily-key)
            echo "Setting Tavily API key as app secret..."
            databricks apps set-secret "$APP_NAME" "$SECRET_NAME" --string-value "$2" --profile "$PROFILE"
            echo "Secret '$SECRET_NAME' set."
            shift 2
            ;;
        --setup)
            RUN_SETUP=true
            shift
            ;;
        *)
            shift
            ;;
    esac
done

# ── Run setup scripts ────────────────────────────────────────────────
if $RUN_SETUP; then
    echo ""
    echo "Running setup scripts..."
    echo "--- Phase 1: Vector Search ---"
    python3 setup/01_vector_search.py
    echo ""
    echo "--- Phase 2: Lakebase ---"
    python3 setup/02_lakebase.py
    echo ""
    echo "--- Phase 3: BOM Weather ---"
    python3 setup/03_bom_weather.py
    echo ""
    echo "--- Phase 4: MLflow Experiment ---"
    python3 setup/04_mlflow_experiment.py
    echo ""
fi

# ── Upload source code ────────────────────────────────────────────────
echo "Uploading source code to workspace..."
databricks workspace import-dir . "$WS_PATH" \
    --overwrite \
    --exclude-hidden-files \
    --profile "$PROFILE"
echo "Source uploaded to $WS_PATH"

# ── Deploy the app ────────────────────────────────────────────────────
echo "Deploying app..."
databricks apps deploy "$APP_NAME" \
    --source-code-path "$WS_PATH" \
    --profile "$PROFILE"

echo ""
echo "=== Deploy complete ==="
echo "App URL: https://${APP_NAME}-1313663707993479.aws.databricksapps.com"
echo ""
echo "First-time setup: ./deploy.sh --setup"
echo "Set Tavily key:   ./deploy.sh --set-tavily-key tvly-YOUR_KEY_HERE"
