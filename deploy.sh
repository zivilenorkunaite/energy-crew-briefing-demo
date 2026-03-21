#!/bin/bash
# Deploy EE Crew Briefing via Databricks Asset Bundles
# Bundle handles: app, env vars, serving endpoint + secret + warehouse permissions, jobs
# This script handles: Lakebase + Genie resources (not supported in bundles), setup scripts
#
# Usage: ./deploy.sh [--setup]
set -euo pipefail

PROFILE="${DATABRICKS_PROFILE:-DEFAULT}"
APP_NAME="ee-crew-briefing"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== EE Crew Briefing — Deploy ==="

# ── Handle flags ──────────────────────────────────────────────────────
RUN_SETUP=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --setup)
            RUN_SETUP=true
            shift
            ;;
        *)
            shift
            ;;
    esac
done

# ── Run setup scripts (first-time only) ──────────────────────────────
if $RUN_SETUP; then
    echo ""
    echo "Running setup scripts..."
    echo "--- Phase 1: Vector Search ---"
    python3 "$SCRIPT_DIR/setup/01_vector_search.py"
    echo ""
    echo "--- Phase 2: Lakebase ---"
    python3 "$SCRIPT_DIR/setup/02_lakebase.py"
    echo ""
    echo "--- Phase 3: BOM Weather ---"
    python3 "$SCRIPT_DIR/setup/03_bom_weather.py"
    echo ""
    echo "--- Phase 4: MLflow Experiment ---"
    python3 "$SCRIPT_DIR/setup/04_mlflow_experiment.py"
    echo ""
fi

# ── Bundle deploy (app + jobs + permissions) ──────────────────────────
echo "Running bundle deploy..."
cd "$SCRIPT_DIR"
databricks bundle deploy --profile "$PROFILE"
echo "Bundle deployed"

# ── Add resources not supported by bundles (Lakebase + Genie) ─────────
echo "Adding Lakebase + Genie resources via API..."
python3 -c "
from databricks.sdk import WorkspaceClient
import json, urllib.request

w = WorkspaceClient(profile='$PROFILE')
host = w.config.host
token = w.config.token or w.config.authenticate().get('Authorization','').replace('Bearer ','')

# Get current resources
req = urllib.request.Request(f'{host}/api/2.0/apps/$APP_NAME',
    headers={'Authorization': f'Bearer {token}'})
app = json.loads(urllib.request.urlopen(req).read())
current = {r['name']: r for r in app.get('resources', [])}

# Only add postgres/genie if not already present
need_update = False
resources = list(app.get('resources', []))

if 'postgres' not in current:
    resources.append({
        'name': 'postgres',
        'description': 'Lakebase Autoscaling for session persistence',
        'postgres': {
            'branch': 'projects/ee-crew-briefing-as/branches/production',
            'database': 'projects/ee-crew-briefing-as/branches/production/databases/db-i1ri-fqtfd0d6tm',
            'permission': 'CAN_CONNECT_AND_CREATE'
        }
    })
    need_update = True
    print('  Adding postgres resource')

if 'genie-room' not in current and 'genie-room-field-ops' not in current:
    resources.append({
        'name': 'genie-room',
        'description': 'Genie Room — Field Operations',
        'genie': {
            'space_id': '01f111b05416164989106b097e2f7d21',
            'permission': 'CAN_RUN'
        }
    })
    need_update = True
    print('  Adding genie-room resource')

if need_update:
    req = urllib.request.Request(f'{host}/api/2.0/apps/$APP_NAME',
        data=json.dumps({'resources': resources}).encode(),
        method='PATCH',
        headers={'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'})
    try:
        resp = json.loads(urllib.request.urlopen(req).read())
        print(f'  {len(resp.get(\"resources\",[]))} resources total')
    except Exception as e:
        error = e.read().decode() if hasattr(e,'read') else str(e)
        print(f'  Warning: resource update failed: {error[:200]}')
        print('  (postgres/genie may not be supported via API — SP permissions may already exist)')
else:
    print('  Lakebase + Genie resources already present')
"
echo ""

# ── Deploy the app (triggers restart with new code) ───────────────────
BUNDLE_PATH="/Workspace/Users/zivile.norkunaite@databricks.com/.bundle/ee-crew-briefing/default/files"
echo "Deploying app from bundle path..."
databricks apps deploy "$APP_NAME" \
    --source-code-path "$BUNDLE_PATH" \
    --profile "$PROFILE"

echo ""
echo "=== Deploy complete ==="
echo "App URL: https://${APP_NAME}-1313663707993479.aws.databricksapps.com"
echo ""
echo "First-time setup: ./deploy.sh --setup"
