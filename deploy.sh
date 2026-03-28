#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# FreeLaw — deploy backend to EC2
# Run this from your Mac:  bash deploy.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

KEY="AWS/g8rcase.pem"
HOST="ubuntu@ec2-54-242-194-234.compute-1.amazonaws.com"
REMOTE_DIR="/home/ubuntu/freelaw/backend"

echo "==> Copying backend files to EC2..."
# Create remote directory
ssh -i "$KEY" "$HOST" "mkdir -p $REMOTE_DIR"

# Copy all backend files (excludes venv and progress file)
scp -i "$KEY" \
    backend/main.py \
    backend/ingestion.py \
    backend/chunker.py \
    backend/milvus_client.py \
    backend/progress.py \
    backend/requirements.txt \
    backend/.env \
    backend/start.sh \
    backend/freelaw.service \
    "$HOST:$REMOTE_DIR/"

echo "==> Running setup on EC2..."
ssh -i "$KEY" "$HOST" bash << 'REMOTE'
set -euo pipefail
cd /home/ubuntu/freelaw/backend

# Ensure python3-venv is installed (required on Ubuntu 22+)
sudo apt-get update -qq
sudo apt-get install -y python3-venv python3-pip

# Create venv (remove broken one if pip is missing)
if [ ! -f .venv/bin/pip ]; then
    rm -rf .venv
    python3 -m venv .venv
    echo "  venv created"
fi

# Install dependencies
echo "  Installing Python packages (this takes a few minutes first time)..."
.venv/bin/pip install --quiet --upgrade pip
.venv/bin/pip install --quiet -r requirements.txt
echo "  Packages installed."

# Make start.sh executable
chmod +x start.sh

# Install and enable the systemd service
sudo cp freelaw.service /etc/systemd/system/freelaw.service
sudo systemctl daemon-reload
sudo systemctl enable freelaw
sudo systemctl restart freelaw

echo "  Service started."
sudo systemctl status freelaw --no-pager
REMOTE

echo ""
echo "==> Deploy complete!"
echo "    API is live at: http://ec2-54-242-194-234.compute-1.amazonaws.com:8000"
echo "    Health check:   curl http://ec2-54-242-194-234.compute-1.amazonaws.com:8000/health"
echo ""
echo "    To watch live logs on EC2:"
echo "    ssh -i AWS/g8rcase.pem ubuntu@ec2-54-242-194-234.compute-1.amazonaws.com"
echo "    sudo journalctl -u freelaw -f"
