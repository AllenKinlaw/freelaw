#!/usr/bin/env python3
"""
laws_ingest.py — Standalone SC Code of Laws ingestion manager.

Reads pre-chunked JSONL files from DATA_DIR, enriches each chunk with a
metadata header, embeds with ModernBERT, and upserts to the sc_laws_rag
Milvus collection.

Progress is written to PROGRESS_FILE after every batch so the process can
be killed and restarted at any time without re-processing completed work.

Usage:
    python laws_ingest.py                     # ingest all pending chunks
    python laws_ingest.py --reset             # clear progress and start over
    python laws_ingest.py --status            # print progress summary and exit
    python laws_ingest.py --data-dir /path    # override data directory
    python laws_ingest.py --batch-size 32     # override embed batch size
"""

import argparse
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer

import laws_milvus

# ── Configuration ─────────────────────────────────────────────────────────────

load_dotenv()

DATA_DIR      = Path(os.getenv("LAWS_DATA_DIR", "/home/ubuntu/freelaw/sc_code"))
PROGRESS_FILE = Path(os.getenv("LAWS_PROGRESS_FILE", "laws_ingest_progress.json"))
EMBED_BATCH   = int(os.getenv("LAWS_EMBED_BATCH", "32"))

_SHUTDOWN = False  # set to True by SIGINT/SIGTERM for graceful exit


# ── Signal handling ────────────────────────────────────────────────────────────

def _handle_signal(sig, frame):
    global _SHUTDOWN
    print("\n[Laws] Shutdown signal received — finishing current batch then stopping…")
    _SHUTDOWN = True

signal.signal(signal.SIGINT,  _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ── Progress tracking ─────────────────────────────────────────────────────────

def _load_progress() -> dict:
    """
    Progress file schema:
    {
        "started_at":   "ISO-8601",
        "updated_at":   "ISO-8601",
        "chunks_total": int,       # sum across all files (from manifest, updated at start)
        "chunks_done":  int,       # running total of upserted chunks
        "files": {
            "sc_title_01_administration.jsonl": {
                "status":      "pending" | "in_progress" | "complete",
                "chunks_done": int,      # chunks upserted from this file
                "last_doc_id": str|null  # last doc_id successfully upserted
            },
            ...
        }
    }
    """
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {
        "started_at":   datetime.now(timezone.utc).isoformat(),
        "updated_at":   datetime.now(timezone.utc).isoformat(),
        "chunks_total": 0,
        "chunks_done":  0,
        "files":        {},
    }


def _save_progress(progress: dict) -> None:
    progress["updated_at"] = datetime.now(timezone.utc).isoformat()
    tmp = PROGRESS_FILE.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(progress, f, indent=2)
    tmp.replace(PROGRESS_FILE)  # atomic rename


# ── Chunk enrichment ──────────────────────────────────────────────────────────

def _enrich(record: dict) -> str:
    """
    Prepend a structured metadata header to the chunk body so the LLM always
    knows the statutory source during retrieval.

    Example:
        [SC Code § 56-1-5 | Title 56: Motor Vehicles | Chapter 1 | DMV Established]

        (A) The Department of Motor Vehicles is hereby established…
    """
    section_id  = record.get("section_id", "")
    title_num   = record.get("title_num", "")
    title_name  = (record.get("title_name") or "").strip()
    chapter_num = record.get("chapter_num", "")
    short_title = (record.get("short_title") or "").strip()
    body        = (record.get("body_text") or record.get("text") or "").strip()

    parts = [f"SC Code § {section_id}"]
    if title_num and title_name:
        parts.append(f"Title {title_num}: {title_name}")
    elif title_name:
        parts.append(title_name)
    if chapter_num:
        parts.append(f"Chapter {chapter_num}")
    if short_title:
        parts.append(short_title)

    header = f"[{' | '.join(parts)}]\n\n"
    return header + body


# ── File discovery ─────────────────────────────────────────────────────────────

def _title_files() -> list[Path]:
    """
    Return canonical per-title JSONL files (exclude train/val/test splits).
    Sorted by title number so progress is predictable.
    """
    files = sorted(
        p for p in DATA_DIR.glob("sc_title_*.jsonl")
        if not any(p.stem.endswith(s) for s in ("_train", "_val", "_test"))
    )
    if not files:
        raise FileNotFoundError(f"No sc_title_*.jsonl files found in {DATA_DIR}")
    return files


# ── Progress summary ──────────────────────────────────────────────────────────

def _print_status(progress: dict) -> None:
    total    = progress.get("chunks_total", 0)
    done     = progress.get("chunks_done", 0)
    pct      = f"{done/total*100:.1f}%" if total else "—"
    updated  = progress.get("updated_at", "")[:19].replace("T", " ")
    started  = progress.get("started_at", "")[:19].replace("T", " ")

    files    = progress.get("files", {})
    complete = sum(1 for v in files.values() if v["status"] == "complete")
    pending  = sum(1 for v in files.values() if v["status"] == "pending")
    in_prog  = sum(1 for v in files.values() if v["status"] == "in_progress")

    print()
    print("═" * 60)
    print("  SC Laws Ingest — Progress Summary")
    print("═" * 60)
    print(f"  Started:      {started} UTC")
    print(f"  Last update:  {updated} UTC")
    print(f"  Chunks:       {done:,} / {total:,}  ({pct})")
    print(f"  Files:        {complete} complete  |  {in_prog} in-progress  |  {pending} pending")
    print("═" * 60)

    for fname, info in sorted(files.items()):
        icon = {"complete": "✓", "in_progress": "→", "pending": "·"}.get(info["status"], " ")
        print(f"  {icon}  {fname:<55}  {info['chunks_done']:>6} chunks")
    print()


# ── Main ingest loop ──────────────────────────────────────────────────────────

def _ingest_file(
    path:       Path,
    file_state: dict,
    collection,
    model:      SentenceTransformer,
    progress:   dict,
) -> None:
    """
    Ingest a single JSONL file.  Resumes from file_state["last_doc_id"] if set.
    Updates file_state and progress in-place; caller is responsible for saving.
    """
    resume_after = file_state.get("last_doc_id")
    skipping     = resume_after is not None
    batch_records: list[dict] = []
    batch_texts:   list[str]  = []

    def _flush_batch():
        nonlocal batch_records, batch_texts
        if not batch_texts:
            return

        embeddings = model.encode(batch_texts, batch_size=EMBED_BATCH, show_progress_bar=False)

        collection.insert([
            [r["section_id"][:64]                         for r in batch_records],
            [int(r.get("title_num") or 0)                 for r in batch_records],
            [(r.get("title_name") or "")[:256]            for r in batch_records],
            [int(r.get("chapter_num") or 0)               for r in batch_records],
            [(r.get("short_title") or "")[:512]           for r in batch_records],
            [int(r.get("chunk_index") or 0)               for r in batch_records],
            [int(r.get("chunk_total") or 1)               for r in batch_records],
            [(r.get("url") or "")[:512]                   for r in batch_records],
            [t[:65535]                                     for t in batch_texts],
            embeddings.tolist(),
        ])

        n = len(batch_records)
        file_state["chunks_done"]  += n
        file_state["last_doc_id"]   = batch_records[-1]["doc_id"]
        progress["chunks_done"]    += n
        _save_progress(progress)

        batch_records.clear()
        batch_texts.clear()

    with open(path, encoding="utf-8") as f:
        for line in f:
            if _SHUTDOWN:
                return

            line = line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            doc_id = record.get("doc_id", "")

            # Resume: skip lines until we pass the last successfully upserted doc_id
            if skipping:
                if doc_id == resume_after:
                    skipping = False
                continue

            # Skip inactive sections
            if record.get("status", "active") != "active":
                continue

            body = (record.get("body_text") or record.get("text") or "").strip()
            if len(body) < 50:
                continue

            batch_records.append(record)
            batch_texts.append(_enrich(record))

            if len(batch_texts) >= EMBED_BATCH:
                _flush_batch()

                elapsed = time.time() - _start_time
                pct     = (progress["chunks_done"] / progress["chunks_total"] * 100
                           if progress["chunks_total"] else 0)
                print(
                    f"\r  [{pct:5.1f}%]  {progress['chunks_done']:,}/{progress['chunks_total']:,} chunks"
                    f"  |  {path.name}  ({file_state['chunks_done']} done)"
                    f"  |  {elapsed/60:.1f}m elapsed",
                    end="", flush=True,
                )

    # Flush remaining
    _flush_batch()


_start_time = time.time()


def run(reset: bool = False, status_only: bool = False, data_dir: Path | None = None, batch_size: int | None = None) -> None:
    global DATA_DIR, EMBED_BATCH, _start_time

    if data_dir:
        DATA_DIR = data_dir
    if batch_size:
        EMBED_BATCH = batch_size

    progress = _load_progress()

    if reset:
        print("[Laws] Resetting progress — all files will be re-ingested.")
        progress = {
            "started_at":   datetime.now(timezone.utc).isoformat(),
            "updated_at":   datetime.now(timezone.utc).isoformat(),
            "chunks_total": 0,
            "chunks_done":  0,
            "files":        {},
        }
        _save_progress(progress)

    files = _title_files()

    # Register any new files in progress
    for path in files:
        if path.name not in progress["files"]:
            progress["files"][path.name] = {
                "status":      "pending",
                "chunks_done": 0,
                "last_doc_id": None,
            }

    # Count total chunks from files (quick line count of non-split JSONL)
    if progress["chunks_total"] == 0:
        print("[Laws] Counting chunks…", end="", flush=True)
        total = 0
        for path in files:
            with open(path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        total += 1
        progress["chunks_total"] = total
        _save_progress(progress)
        print(f" {total:,} total chunks across {len(files)} title files.")

    if status_only:
        _print_status(progress)
        return

    _print_status(progress)

    # Connect Milvus and load model
    laws_milvus.connect_milvus()
    collection = laws_milvus.get_or_create_collection()

    print("[Laws] Loading ModernBERT model…")
    model = SentenceTransformer("answerdotai/ModernBERT-base")
    print("[Laws] Model ready.\n")

    _start_time = time.time()

    for path in files:
        if _SHUTDOWN:
            break

        file_state = progress["files"][path.name]

        if file_state["status"] == "complete":
            continue

        print(f"\n[Laws] Processing: {path.name}")
        file_state["status"] = "in_progress"
        _save_progress(progress)

        _ingest_file(path, file_state, collection, model, progress)

        if _SHUTDOWN:
            _save_progress(progress)
            print(f"\n[Laws] Stopped. Progress saved — resume by re-running without --reset.")
            break

        file_state["status"] = "complete"
        _save_progress(progress)
        print(f"\n[Laws] ✓ {path.name}  ({file_state['chunks_done']:,} chunks)")

    if not _SHUTDOWN:
        elapsed = (time.time() - _start_time) / 60
        print(f"\n[Laws] Ingest complete — {progress['chunks_done']:,} chunks in {elapsed:.1f}m")
        _print_status(progress)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SC Code of Laws ingestion manager")
    parser.add_argument("--reset",      action="store_true", help="Clear progress and start over")
    parser.add_argument("--status",     action="store_true", help="Print progress summary and exit")
    parser.add_argument("--data-dir",   type=Path,           help=f"Path to JSONL files (default: {DATA_DIR})")
    parser.add_argument("--batch-size", type=int,            help=f"Embed batch size (default: {EMBED_BATCH})")
    args = parser.parse_args()

    run(
        reset       = args.reset,
        status_only = args.status,
        data_dir    = args.data_dir,
        batch_size  = args.batch_size,
    )
