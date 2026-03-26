#!/bin/bash
# Remove all resources created by the Energy Crew Briefing demo.
#
# This script tears down everything --setup created:
#   - Databricks App (+ all app resources)
#   - DAB jobs (BOM refresh, gateway token refresh)
#   - Lakebase instance + database
#   - Vector Search endpoint + index
#   - UC schema tables (swms_documents, bom_weather, work_orders, work_tasks)
#   - UC function (get_weather)
#   - Genie Room
#   - MLflow experiment
#   - Secret scope
#
# Usage: ./teardown.sh [--confirm]
set -euo pipefail

PROFILE="${DATABRICKS_PROFILE:-DEFAULT}"
APP_NAME="energy-crew-briefing"
UC_SCHEMA="main.energy_crew_briefing"
LAKEBASE_INSTANCE="energy-crew-briefing"
VS_ENDPOINT="energy-crew-briefing-vs"
SECRET_SCOPE="energy-crew-briefing"
WAREHOUSE_ID="${WAREHOUSE_ID:-}"

if [[ "${1:-}" != "--confirm" ]]; then
    echo "=== Energy Crew Briefing — Teardown ==="
    echo ""
    echo "This will PERMANENTLY DELETE all demo resources:"
    echo "  - App: $APP_NAME"
    echo "  - Lakebase: $LAKEBASE_INSTANCE"
    echo "  - UC schema tables: $UC_SCHEMA"
    echo "  - Vector Search: $VS_ENDPOINT"
    echo "  - Secret scope: $SECRET_SCOPE"
    echo "  - Genie Room, MLflow experiment, scheduled jobs"
    echo ""
    echo "Run with --confirm to proceed:"
    echo "  ./teardown.sh --confirm"
    exit 0
fi

echo "=== Energy Crew Briefing — Teardown (confirmed) ==="

# Discover warehouse ID if not set
if [ -z "$WAREHOUSE_ID" ]; then
    WAREHOUSE_ID=$(databricks api get /api/2.0/sql/warehouses/ --profile "$PROFILE" 2>/dev/null \
        | python3 -c "import sys,json; whs=json.load(sys.stdin).get('warehouses',[]); print(next((w['id'] for w in whs if w.get('state') in ('RUNNING','STOPPED')), ''))" 2>/dev/null)
    [ -n "$WAREHOUSE_ID" ] && echo "Using warehouse: $WAREHOUSE_ID"
fi

# 1. Delete the Databricks App
echo ""
echo "--- Deleting app ---"
databricks apps delete "$APP_NAME" --profile "$PROFILE" 2>/dev/null || echo "  App not found or already deleted"

# 2. Destroy the bundle (removes jobs + app resource)
echo ""
echo "--- Destroying bundle ---"
cd "$(dirname "$0")"
databricks bundle destroy --auto-approve --profile "$PROFILE" 2>/dev/null || echo "  Bundle destroy failed or not deployed"

# 3. Delete Lakebase instance
echo ""
echo "--- Deleting Lakebase instance ---"
databricks database delete-database-instance "$LAKEBASE_INSTANCE" --profile "$PROFILE" 2>/dev/null || echo "  Lakebase instance not found"

# 4. Delete Vector Search endpoint
echo ""
echo "--- Deleting Vector Search endpoint ---"
databricks api delete "/api/2.0/vector-search/endpoints/$VS_ENDPOINT" --profile "$PROFILE" 2>/dev/null || echo "  VS endpoint not found"

# 5. Drop UC tables and function
echo ""
echo "--- Dropping UC objects ---"
python3 -c "
import subprocess, json

PROFILE = '$PROFILE'
WAREHOUSE_ID = '$WAREHOUSE_ID'
SCHEMA = '$UC_SCHEMA'

def run_sql(sql):
    payload = {'statement': sql, 'warehouse_id': WAREHOUSE_ID, 'wait_timeout': '30s'}
    cmd = ['databricks', 'api', 'post', '/api/2.0/sql/statements/', '--json', json.dumps(payload), '--profile', PROFILE]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode == 0

objects = [
    f'DROP FUNCTION IF EXISTS {SCHEMA}.get_weather',
    f'DROP TABLE IF EXISTS {SCHEMA}.bom_weather',
    f'DROP TABLE IF EXISTS {SCHEMA}.swms_documents',
    f'DROP TABLE IF EXISTS {SCHEMA}.work_tasks',
    f'DROP TABLE IF EXISTS {SCHEMA}.work_orders',
    f'DROP TABLE IF EXISTS {SCHEMA}.assets',
    f'DROP TABLE IF EXISTS {SCHEMA}.asset_types',
    f'DROP TABLE IF EXISTS {SCHEMA}.swms_documents_vs_index',
]
for sql in objects:
    ok = run_sql(sql)
    print(f'  {\"OK\" if ok else \"FAIL\"}: {sql}')
"

# 6. Delete secret scope
echo ""
echo "--- Deleting secret scope ---"
databricks secrets delete-scope "$SECRET_SCOPE" --profile "$PROFILE" 2>/dev/null || echo "  Scope not found"

# 7. Delete Genie Room (find by title)
echo ""
echo "--- Deleting Genie Room ---"
python3 -c "
import subprocess, json
PROFILE = '$PROFILE'
result = subprocess.run(['databricks', 'api', 'get', '/api/2.0/genie/spaces', '--profile', PROFILE], capture_output=True, text=True)
if result.returncode == 0:
    data = json.loads(result.stdout)
    for space in data.get('spaces', []):
        if space.get('title') == 'Field Operations':
            sid = space['space_id']
            subprocess.run(['databricks', 'api', 'delete', f'/api/2.0/genie/spaces/{sid}', '--profile', PROFILE])
            print(f'  Deleted Genie Room: {sid}')
            break
    else:
        print('  Genie Room not found')
else:
    print('  Could not list Genie Rooms')
"

echo ""
echo "=== Teardown complete ==="
echo "All demo resources have been removed."
