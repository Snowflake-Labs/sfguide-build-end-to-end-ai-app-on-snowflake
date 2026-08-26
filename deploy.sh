#!/bin/bash
set -euo pipefail

CONNECTION=${1:-dash-builder}
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "============================================================"
echo "  HOL Full Deploy — Connection: $CONNECTION"
echo "============================================================"

echo ""
echo "[1/5] Running cleanup.sql..."
snow sql -f "$SCRIPT_DIR/cleanup.sql" -c "$CONNECTION"

echo ""
echo "[2/5] Running setup.sql (50M data load + all objects)..."
echo "       This takes ~5 min (data load + DT refresh + Interactive Tables)"
snow sql -f "$SCRIPT_DIR/setup.sql" -c "$CONNECTION"

echo ""
echo "[3/5] Creating agent..."
snow sql -f "$SCRIPT_DIR/snowflake-cowork/create_agent.sql" -c "$CONNECTION"

echo ""
echo "[4/5] Deploying Streamlit dashboard..."
cd "$SCRIPT_DIR/streamlit-dashboard"
snow streamlit deploy --replace -c "$CONNECTION"
cd "$SCRIPT_DIR"

echo ""
echo "[5/5] Running Interactive Tables load test..."
SNOWFLAKE_CONNECTION_NAME="$CONNECTION" python3 "$SCRIPT_DIR/interactive/load_test.py"

echo ""
echo "============================================================"
echo "  Deploy complete!"
echo "============================================================"
echo ""
echo "  Next steps:"
echo "    - Test agent: snow sql -f test.sql -c $CONNECTION"
echo "    - Run evaluation: Snowsight → AI & ML → Agents → Evaluations"
echo "    - View dashboard: Snowsight → Streamlit → THE_DASHBOARD"
echo "============================================================"
