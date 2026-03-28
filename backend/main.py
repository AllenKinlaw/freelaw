import os
from typing import List, Optional

from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

load_dotenv()

from bulk_ingest import bulk_ingest_worker
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
def start_bulk_ingestion(background_tasks: BackgroundTasks):
    """Trigger ingestion from CourtListener S3 bulk CSV files (no API key needed)."""
    if status_tracker["is_running"]:
        return {
            "status": "already_running",
            "message": "Ingestion is already in progress.",
        }
    background_tasks.add_task(bulk_ingest_worker, collection)
    return {"status": "started", "message": "Bulk S3 ingest started (SC + SCCtApp)."}


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
