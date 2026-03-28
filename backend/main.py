import os
from typing import List, Optional

from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

load_dotenv()

from bulk_ingest import bulk_ingest_worker
from court_stats import load_stats, scan_status, scan_worker, stats_exist
from ingestion import ingest_worker, status_tracker
from milvus_client import connect_milvus, get_or_create_collection
from progress import reset_progress

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="FreeLaw Ingestion Engine", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # Lock down to your Angular origin in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Startup: connect Milvus and prepare collection ───────────────────────────
connect_milvus()
collection = get_or_create_collection()


# ── Request / response models ─────────────────────────────────────────────────
class IngestRequest(BaseModel):
    years: List[int]


class BulkIngestRequest(BaseModel):
    start_year: Optional[int] = None
    end_year:   Optional[int] = None


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/ingest")
def start_ingestion(req: IngestRequest, background_tasks: BackgroundTasks):
    if status_tracker["is_running"]:
        return {
            "status": "already_running",
            "message": "Ingestion is already in progress.",
            "current_year": status_tracker["current_year"],
        }
    background_tasks.add_task(ingest_worker, sorted(req.years), collection)
    return {"status": "started", "years": sorted(req.years)}


@app.post("/stop-ingestion")
def stop_ingestion():
    if not status_tracker["is_running"]:
        return {"status": "not_running", "message": "Nothing is currently running."}
    status_tracker["is_running"] = False
    return {"status": "stopping", "message": "Will stop after the current page finishes."}


@app.get("/status")
def get_status():
    return status_tracker


@app.post("/bulk-ingest")
def start_bulk_ingestion(req: BulkIngestRequest, background_tasks: BackgroundTasks):
    """Trigger ingestion from CourtListener S3 bulk CSV files (no API key needed)."""
    if status_tracker["is_running"]:
        return {
            "status": "already_running",
            "message": "Ingestion is already in progress.",
        }
    year_label = (
        f"{req.start_year}–{req.end_year}" if req.start_year and req.end_year
        else f"from {req.start_year}" if req.start_year
        else f"up to {req.end_year}"  if req.end_year
        else "all years"
    )
    background_tasks.add_task(bulk_ingest_worker, collection, req.start_year, req.end_year)
    return {"status": "started", "message": f"Bulk S3 ingest started ({year_label})."}


@app.get("/court-stats")
def get_court_stats():
    """Return cached per-court storage estimates, plus current scan status."""
    return {
        "available":   stats_exist(),
        "scan_status": scan_status,
        **(load_stats() if stats_exist() else {}),
    }


@app.post("/start-court-scan")
def start_court_scan(background_tasks: BackgroundTasks):
    """Trigger a one-time background scan of the S3 dockets CSV to build storage estimates."""
    if scan_status["is_scanning"]:
        return {"status": "already_scanning", "message": "Scan already running."}
    if status_tracker["is_running"]:
        return {"status": "busy", "message": "Cannot scan while ingestion is running."}
    background_tasks.add_task(scan_worker)
    return {"status": "started", "message": "Court stats scan started (~5–10 min)."}


@app.post("/reset-progress")
def reset(
    year:  Optional[int] = Query(default=None, description="Specific year to reset (omit for full reset)"),
    court: Optional[str] = Query(default=None, description="Specific court: 'sc' or 'scctapp'"),
):
    if status_tracker["is_running"]:
        return {"status": "error", "message": "Cannot reset while ingestion is running."}
    reset_progress(year, court)
    label = f"year={year}" if year else "all progress"
    label += f" court={court}" if court else ""
    return {"status": "reset", "cleared": label}
