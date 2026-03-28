#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# FreeLaw Ingestion Engine — EC2 startup script
# Run once to install deps, then re-run to start/restart the API server.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

VENV_DIR="$SCRIPT_DIR/.venv"

# ── 1. Create venv if missing ─────────────────────────────────────────────────
if [ ! -d "$VENV_DIR" ]; then
    echo "[setup] Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
fi

source "$VENV_DIR/bin/activate"

# ── 2. Install / upgrade dependencies ────────────────────────────────────────
echo "[setup] Installing dependencies..."
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt

# ── 3. Verify .env is present ─────────────────────────────────────────────────
if [ ! -f "$SCRIPT_DIR/.env" ]; then
    echo "[error] .env file not found. Copy .env.example and fill in your keys."
    exit 1
fi

# ── 4. Start the API server ───────────────────────────────────────────────────
echo "[server] Starting FreeLaw API on port 8000..."
uvicorn main:app \
    --host 0.0.0.0 \
    --port 8000 \
    --workers 1 \
    --log-level info
