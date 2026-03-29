#!/usr/bin/env python3
"""
laws_app.py — Standalone SC Code of Laws ingestion manager web app.

Run:
    uvicorn laws_app:app --host 0.0.0.0 --port 8001

Then open http://<host>:8001 in a browser.

Designed to be merged into main.py later — all routes are prefixed /laws/.
"""

import json
import threading
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

import laws_ingest as _laws
from laws_ingest import PROGRESS_FILE, _load_progress, _save_progress

app = FastAPI(title="SC Laws Ingest Manager")

_thread: threading.Thread | None = None
_lock   = threading.Lock()


# ── Background worker ─────────────────────────────────────────────────────────

def _worker():
    try:
        _laws._SHUTDOWN = False
        _ingest_run()
    except Exception as exc:
        print(f"[LawsApp] Worker error: {exc}")


# ── API routes ────────────────────────────────────────────────────────────────

@app.post("/laws/start")
def start_ingest(reset: bool = False):
    global _thread
    with _lock:
        if _thread and _thread.is_alive():
            return JSONResponse({"status": "already_running", "message": "Ingestion already in progress."})
        _laws._SHUTDOWN = False
        _thread = threading.Thread(target=_worker, daemon=True)
        _thread.start()
    return {"status": "started", "message": "Ingestion started." + (" (reset)" if reset else "")}


@app.post("/laws/stop")
def stop_ingest():
    _laws._SHUTDOWN = True
    return {"status": "stopping", "message": "Stop signal sent — finishing current batch."}


@app.get("/laws/status")
def get_status():
    progress = _load_progress()
    running  = bool(_thread and _thread.is_alive())

    total = progress.get("chunks_total", 0)
    done  = progress.get("chunks_done", 0)
    files = progress.get("files", {})

    return {
        "is_running":    running,
        "chunks_total":  total,
        "chunks_done":   done,
        "pct":           round(done / total * 100, 1) if total else 0,
        "started_at":    progress.get("started_at"),
        "updated_at":    progress.get("updated_at"),
        "files": [
            {
                "name":        name,
                "status":      info["status"],
                "chunks_done": info["chunks_done"],
            }
            for name, info in sorted(files.items())
        ],
    }


@app.post("/laws/reset")
def reset_ingest():
    from datetime import datetime, timezone
    with _lock:
        if _thread and _thread.is_alive():
            return JSONResponse(
                status_code=409,
                content={"status": "error", "message": "Stop the ingest before resetting."},
            )
        blank = {
            "started_at":   datetime.now(timezone.utc).isoformat(),
            "updated_at":   datetime.now(timezone.utc).isoformat(),
            "chunks_total": 0,
            "chunks_done":  0,
            "files":        {},
        }
        _save_progress(blank)
    return {"status": "reset", "message": "Progress cleared."}


# ── UI ────────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
def ui():
    return HTMLResponse(content=_HTML)


_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SC Laws Ingest Manager</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: 'Segoe UI', system-ui, sans-serif;
    background: #0f1117;
    color: #e2e8f0;
    min-height: 100vh;
    padding: 2rem;
  }
  h1 { font-size: 1.5rem; font-weight: 600; margin-bottom: 0.25rem; }
  .subtitle { color: #94a3b8; font-size: 0.85rem; margin-bottom: 2rem; }

  .card {
    background: #1e2330;
    border: 1px solid #2d3548;
    border-radius: 10px;
    padding: 1.25rem 1.5rem;
    margin-bottom: 1.25rem;
  }
  .card-title { font-size: 0.7rem; font-weight: 600; letter-spacing: .08em;
                text-transform: uppercase; color: #64748b; margin-bottom: 1rem; }

  .stats { display: flex; gap: 2rem; flex-wrap: wrap; }
  .stat-val { font-size: 1.75rem; font-weight: 700; line-height: 1; }
  .stat-lbl { font-size: 0.75rem; color: #64748b; margin-top: 0.25rem; }

  .progress-bar-bg {
    background: #2d3548; border-radius: 99px; height: 12px;
    margin: 1rem 0 0.4rem;
  }
  .progress-bar-fill {
    background: linear-gradient(90deg, #3b82f6, #6366f1);
    height: 100%; border-radius: 99px;
    transition: width 0.6s ease;
  }
  .pct-label { font-size: 0.8rem; color: #94a3b8; text-align: right; }

  .badge {
    display: inline-block; padding: 0.2rem 0.65rem; border-radius: 99px;
    font-size: 0.72rem; font-weight: 600; letter-spacing: .04em;
  }
  .badge-running  { background: #1d4ed8; color: #bfdbfe; }
  .badge-idle     { background: #1e293b; color: #64748b; border: 1px solid #334155; }
  .badge-complete { background: #14532d; color: #86efac; }
  .badge-stopping { background: #7c2d12; color: #fed7aa; }

  .btn {
    padding: 0.5rem 1.25rem; border-radius: 7px; border: none;
    font-size: 0.85rem; font-weight: 600; cursor: pointer; transition: opacity .15s;
  }
  .btn:disabled { opacity: .35; cursor: not-allowed; }
  .btn-start  { background: #2563eb; color: #fff; }
  .btn-stop   { background: #dc2626; color: #fff; }
  .btn-reset  { background: #374151; color: #d1d5db; }
  .btn + .btn { margin-left: 0.6rem; }

  table { width: 100%; border-collapse: collapse; font-size: 0.82rem; }
  th { text-align: left; color: #64748b; font-weight: 600;
       font-size: 0.7rem; text-transform: uppercase; letter-spacing: .06em;
       padding: 0 0 0.6rem; border-bottom: 1px solid #2d3548; }
  td { padding: 0.45rem 0; border-bottom: 1px solid #1a2035; vertical-align: middle; }
  tr:last-child td { border-bottom: none; }

  .file-status {
    display: inline-block; width: 8px; height: 8px; border-radius: 50%;
    margin-right: 6px; vertical-align: middle;
  }
  .s-complete    { background: #22c55e; }
  .s-in_progress { background: #3b82f6; animation: pulse 1.2s infinite; }
  .s-pending     { background: #334155; }

  @keyframes pulse {
    0%,100% { opacity: 1; } 50% { opacity: .4; }
  }

  .updated { font-size: 0.72rem; color: #475569; margin-top: 0.5rem; }
</style>
</head>
<body>

<h1>SC Laws Ingest Manager</h1>
<p class="subtitle">South Carolina Code of Laws → ModernBERT → Zilliz</p>

<div class="card">
  <div class="card-title">Controls</div>
  <button class="btn btn-start"  id="btnStart" onclick="startIngest()">Start Ingest</button>
  <button class="btn btn-stop"   id="btnStop"  onclick="stopIngest()" disabled>Stop</button>
  <button class="btn btn-reset"  id="btnReset" onclick="resetIngest()">Reset Progress</button>
  <span id="actionMsg" style="margin-left:1rem;font-size:.82rem;color:#94a3b8;"></span>
</div>

<div class="card">
  <div class="card-title">Progress</div>
  <div class="stats">
    <div>
      <div class="stat-val" id="statDone">—</div>
      <div class="stat-lbl">Chunks upserted</div>
    </div>
    <div>
      <div class="stat-val" id="statTotal">—</div>
      <div class="stat-lbl">Total chunks</div>
    </div>
    <div>
      <div class="stat-val" id="statFiles">—</div>
      <div class="stat-lbl">Files complete</div>
    </div>
    <div style="margin-left:auto;align-self:center;">
      <span class="badge badge-idle" id="statusBadge">Idle</span>
    </div>
  </div>
  <div class="progress-bar-bg">
    <div class="progress-bar-fill" id="progressBar" style="width:0%"></div>
  </div>
  <div class="pct-label" id="pctLabel">0%</div>
  <div class="updated" id="updatedAt"></div>
</div>

<div class="card">
  <div class="card-title">Files</div>
  <table>
    <thead>
      <tr>
        <th>File</th>
        <th style="text-align:right">Chunks</th>
        <th style="text-align:right">Status</th>
      </tr>
    </thead>
    <tbody id="fileTable"><tr><td colspan="3" style="color:#475569">Loading…</td></tr></tbody>
  </table>
</div>

<script>
let _running = false;

async function fetchStatus() {
  try {
    const r = await fetch('/laws/status');
    const d = await r.json();
    _running = d.is_running;

    document.getElementById('statDone').textContent  = d.chunks_done.toLocaleString();
    document.getElementById('statTotal').textContent = d.chunks_total.toLocaleString();

    const complete = d.files.filter(f => f.status === 'complete').length;
    document.getElementById('statFiles').textContent = `${complete} / ${d.files.length}`;

    document.getElementById('progressBar').style.width = d.pct + '%';
    document.getElementById('pctLabel').textContent    = d.pct.toFixed(1) + '%';

    const badge = document.getElementById('statusBadge');
    if (d.is_running) {
      badge.textContent  = 'Running';
      badge.className    = 'badge badge-running';
    } else if (d.pct >= 100 && d.chunks_done > 0) {
      badge.textContent  = 'Complete';
      badge.className    = 'badge badge-complete';
    } else {
      badge.textContent  = 'Idle';
      badge.className    = 'badge badge-idle';
    }

    document.getElementById('btnStart').disabled = d.is_running;
    document.getElementById('btnStop').disabled  = !d.is_running;
    document.getElementById('btnReset').disabled = d.is_running;

    if (d.updated_at) {
      const dt = new Date(d.updated_at);
      document.getElementById('updatedAt').textContent =
        'Last update: ' + dt.toLocaleTimeString();
    }

    // File table
    const tbody = document.getElementById('fileTable');
    if (d.files.length === 0) {
      tbody.innerHTML = '<tr><td colspan="3" style="color:#475569">No files found. Is DATA_DIR set correctly on EC2?</td></tr>';
    } else {
      tbody.innerHTML = d.files.map(f => {
        const icon = `<span class="file-status s-${f.status}"></span>`;
        const name = f.name.replace(/^sc_title_\d+_/, '').replace(/_/g, ' ').replace('.jsonl','');
        return `<tr>
          <td>${icon}${f.name}</td>
          <td style="text-align:right;color:#94a3b8">${f.chunks_done.toLocaleString()}</td>
          <td style="text-align:right">${f.status}</td>
        </tr>`;
      }).join('');
    }
  } catch(e) {
    document.getElementById('statusBadge').textContent = 'Offline';
  }
}

async function startIngest() {
  document.getElementById('actionMsg').textContent = 'Starting…';
  const r = await fetch('/laws/start', { method: 'POST' });
  const d = await r.json();
  document.getElementById('actionMsg').textContent = d.message;
  fetchStatus();
}

async function stopIngest() {
  document.getElementById('actionMsg').textContent = 'Stopping…';
  const r = await fetch('/laws/stop', { method: 'POST' });
  const d = await r.json();
  document.getElementById('actionMsg').textContent = d.message;
}

async function resetIngest() {
  if (!confirm('Clear all progress and start over?')) return;
  const r = await fetch('/laws/reset', { method: 'POST' });
  const d = await r.json();
  document.getElementById('actionMsg').textContent = d.message;
  fetchStatus();
}

// Poll every 3s while running, every 10s while idle
function schedulePoll() {
  setTimeout(() => { fetchStatus().then(schedulePoll); }, _running ? 3000 : 10000);
}

fetchStatus().then(schedulePoll);
</script>
</body>
</html>
"""
