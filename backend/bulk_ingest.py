"""
bulk_ingest.py – Stream SC court opinions from CourtListener S3 bulk data.

Pipeline (no large downloads — everything streams from S3):
  Phase 1   – dockets CSV          → collect SC docket IDs  (court_id in {'sc','scctapp'})
  Phase 2   – opinion_clusters CSV → collect SC cluster metadata  (case_name, year, court)
  Phase 2.5 – citations CSV        → attach citation strings to SC clusters
  Phase 3   – opinions CSV         → chunk → embed → upsert to Milvus

Caching & graceful restart
──────────────────────────
Phase 1/2/2.5 results are saved to CACHE_DIR after completion, keyed by the
S3 bulk-data date and year-range filter.  On the next run those phases are
skipped entirely — only Phase 3 is re-run.

Phase 3 saves a progress checkpoint (last successfully upserted opinion_id)
after every CHECKPOINT_EVERY opinions.  If the process is killed, the next
run streams through the opinions CSV, skips everything up to that id, and
continues from where it left off.

Cache is invalidated automatically when the S3 bulk-data date changes
(CourtListener publishes new snapshots periodically).

Public bucket access (no AWS credentials required):
  s3://com-courtlistener-storage/bulk-data/
"""

import bz2
import csv
import io
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from pymilvus import Collection
from sentence_transformers import SentenceTransformer

from chunker import enrich_chunk, legal_chunker
from ingestion import status_tracker  # shared live-status dict

# ── Constants ────────────────────────────────────────────────────────────────
S3_BUCKET        = "com-courtlistener-storage"
S3_PREFIX        = "bulk-data"
SC_COURTS        = {"sc", "scctapp"}
EMBED_BATCH      = 64
CHECKPOINT_EVERY = 100          # save Phase 3 progress every N opinions
CACHE_DIR        = Path("bulk_ingest_cache")

_model: SentenceTransformer | None = None


# ── Model (lazy-loaded so import is fast) ────────────────────────────────────

def _get_model() -> SentenceTransformer:
    global _model
    if _model is None:
        print("[BulkIngest] Loading ModernBERT model…")
        _model = SentenceTransformer("answerdotai/ModernBERT-base")
        print("[BulkIngest] Model ready.")
    return _model


# ── S3 helpers ───────────────────────────────────────────────────────────────

def _s3_client():
    """Anonymous boto3 client for the public CourtListener bucket."""
    return boto3.client(
        "s3",
        region_name="us-east-1",
        config=Config(signature_version=UNSIGNED),
    )


def _find_latest_key(s3, file_prefix: str) -> str:
    """Return the S3 key of the most recent bz2 CSV matching *file_prefix*."""
    resp = s3.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=f"{S3_PREFIX}/{file_prefix}",
    )
    keys = [
        o["Key"]
        for o in resp.get("Contents", [])
        if o["Key"].endswith(".csv.bz2")
    ]
    if not keys:
        raise FileNotFoundError(
            f"No bulk data files found in s3://{S3_BUCKET}/{S3_PREFIX}/ "
            f"matching prefix '{file_prefix}'"
        )
    return sorted(keys)[-1]


def _date_from_key(key: str) -> str:
    """Extract YYYY-MM-DD from an S3 key like 'bulk-data/dockets-2025-12-31.csv.bz2'."""
    m = re.search(r"(\d{4}-\d{2}-\d{2})", key)
    return m.group(1) if m else "unknown"


# ── Streaming bz2 CSV reader ─────────────────────────────────────────────────

class _S3Bz2Stream(io.RawIOBase):
    """
    A readable io.RawIOBase that decompresses a bz2-encoded S3 streaming body
    on the fly.  Feed it into io.BufferedReader → io.TextIOWrapper → csv.DictReader
    for fully streaming, zero-disk-use CSV parsing that correctly handles quoted
    multi-line fields.
    """

    def __init__(self, body, chunk_size: int = 2 * 1024 * 1024):
        self._body = body
        self._dec  = bz2.BZ2Decompressor()
        self._buf  = b""
        self._eof  = False
        self._chunk_size = chunk_size

    def readable(self) -> bool:
        return True

    def readinto(self, b: bytearray) -> int:
        while not self._eof and len(self._buf) < len(b):
            raw = self._body.read(self._chunk_size)
            if not raw:
                self._eof = True
                break
            self._buf += self._dec.decompress(raw)

        n = min(len(b), len(self._buf))
        if n == 0:
            return 0
        b[:n] = self._buf[:n]
        self._buf = self._buf[n:]
        return n


def _stream_csv(s3, bucket: str, key: str):
    """Yield csv.DictReader rows from a bz2-compressed S3 CSV (streaming, no disk)."""
    csv.field_size_limit(10 * 1024 * 1024)  # some opinion fields exceed the 128 KB default
    print(f"[BulkIngest] Streaming  s3://{bucket}/{key}")
    obj         = s3.get_object(Bucket=bucket, Key=key)
    raw_stream  = _S3Bz2Stream(obj["Body"])
    buf_stream  = io.BufferedReader(raw_stream, buffer_size=4 * 1024 * 1024)
    text_stream = io.TextIOWrapper(buf_stream, encoding="utf-8", errors="replace")
    yield from csv.DictReader(text_stream)


# ── Utility ───────────────────────────────────────────────────────────────────

def _strip_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _atomic_write(path: Path, data: dict) -> None:
    """Write JSON atomically via a temp file to avoid corrupt files on crash."""
    CACHE_DIR.mkdir(exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(data, f)
    tmp.replace(path)


# ── Cache helpers ─────────────────────────────────────────────────────────────

def _year_tag(start_year: int | None, end_year: int | None) -> str:
    return f"{start_year or 'all'}_{end_year or 'all'}"


def _dockets_cache_path(date: str) -> Path:
    return CACHE_DIR / f"dockets_{date}.json"


def _clusters_cache_path(date: str, start_year, end_year) -> Path:
    return CACHE_DIR / f"clusters_{date}_{_year_tag(start_year, end_year)}.json"


def _opinions_progress_path(date: str, start_year, end_year) -> Path:
    return CACHE_DIR / f"opinions_progress_{date}_{_year_tag(start_year, end_year)}.json"


def _load_dockets_cache(date: str) -> dict[str, str] | None:
    path = _dockets_cache_path(date)
    if path.exists():
        print(f"[BulkIngest] Phase 1 cache hit — loading {path.name}")
        with open(path) as f:
            return json.load(f)
    return None


def _save_dockets_cache(date: str, data: dict[str, str]) -> None:
    _atomic_write(_dockets_cache_path(date), data)
    print(f"[BulkIngest] Phase 1 cached → {_dockets_cache_path(date).name}")


def _load_clusters_cache(date: str, start_year, end_year) -> dict[str, dict] | None:
    path = _clusters_cache_path(date, start_year, end_year)
    if path.exists():
        print(f"[BulkIngest] Phase 2/2.5 cache hit — loading {path.name}")
        with open(path) as f:
            return json.load(f)
    return None


def _save_clusters_cache(date: str, start_year, end_year, data: dict[str, dict]) -> None:
    path = _clusters_cache_path(date, start_year, end_year)
    _atomic_write(path, data)
    print(f"[BulkIngest] Phase 2/2.5 cached → {path.name}")


def _load_opinions_progress(date: str, start_year, end_year) -> dict | None:
    path = _opinions_progress_path(date, start_year, end_year)
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return None


def _save_opinions_progress(date: str, start_year, end_year, last_opinion_id: str) -> None:
    _atomic_write(
        _opinions_progress_path(date, start_year, end_year),
        {
            "last_opinion_id":    last_opinion_id,
            "opinions_processed": status_tracker["opinions_processed"],
            "chunks_upserted":    status_tracker["chunks_upserted"],
            "saved_at":           datetime.now(timezone.utc).isoformat(),
        },
    )


def _clear_opinions_progress(date: str, start_year, end_year) -> None:
    path = _opinions_progress_path(date, start_year, end_year)
    path.unlink(missing_ok=True)


# ── Phase 1 ───────────────────────────────────────────────────────────────────

def _collect_sc_dockets(s3) -> tuple[dict[str, str], str]:
    """Return ({docket_id: court_id}, s3_date).  Uses cache if available."""
    key  = _find_latest_key(s3, "dockets")
    date = _date_from_key(key)

    cached = _load_dockets_cache(date)
    if cached is not None:
        status_tracker["message"] = f"Phase 1/3: Loaded {len(cached):,} SC dockets from cache."
        return cached, date

    status_tracker["message"] = f"Phase 1/3: Scanning dockets … (key: {key.split('/')[-1]})"
    sc_dockets: dict[str, str] = {}
    total = 0

    for row in _stream_csv(s3, S3_BUCKET, key):
        if not status_tracker["is_running"]:
            break
        total += 1
        if total % 500_000 == 0:
            status_tracker["message"] = (
                f"Phase 1/3: Scanned {total:,} dockets — {len(sc_dockets):,} SC found…"
            )
        if row.get("court_id") in SC_COURTS:
            sc_dockets[row["id"]] = row["court_id"]

    print(f"[BulkIngest] Phase 1 done: {len(sc_dockets):,} SC dockets / {total:,} total")

    if status_tracker["is_running"]:
        _save_dockets_cache(date, sc_dockets)

    return sc_dockets, date


# ── Phase 2 ───────────────────────────────────────────────────────────────────

def _collect_sc_clusters(
    s3,
    sc_dockets: dict[str, str],
    start_year: int | None = None,
    end_year:   int | None = None,
) -> dict[str, dict]:
    """Return {cluster_id: {case_name, court, year}} for SC opinion clusters."""
    key        = _find_latest_key(s3, "opinion-clusters")
    year_label = (
        f"{start_year}–{end_year}" if start_year and end_year
        else f"from {start_year}" if start_year
        else f"up to {end_year}"  if end_year
        else "all years"
    )
    status_tracker["message"] = (
        f"Phase 2/3: Scanning clusters ({year_label}) … (key: {key.split('/')[-1]})"
    )

    sc_clusters: dict[str, dict] = {}
    total = 0

    for row in _stream_csv(s3, S3_BUCKET, key):
        if not status_tracker["is_running"]:
            break
        total += 1
        if total % 200_000 == 0:
            status_tracker["message"] = (
                f"Phase 2/3: Scanned {total:,} clusters — {len(sc_clusters):,} SC found…"
            )
        docket_id = row.get("docket_id", "")
        if docket_id not in sc_dockets:
            continue

        date_filed = row.get("date_filed", "") or ""
        try:
            year = int(date_filed[:4]) if len(date_filed) >= 4 else 0
        except ValueError:
            year = 0

        if start_year and year and year < start_year:
            continue
        if end_year and year and year > end_year:
            continue

        case_name = (
            row.get("case_name")
            or row.get("case_name_short")
            or row.get("case_name_full")
            or "Unknown"
        ).strip() or "Unknown"

        sc_clusters[row["id"]] = {
            "case_name": case_name,
            "court":     sc_dockets[docket_id],
            "year":      year,
        }

    print(f"[BulkIngest] Phase 2 done: {len(sc_clusters):,} SC clusters / {total:,} total")
    return sc_clusters


# ── Phase 2.5 ─────────────────────────────────────────────────────────────────

def _collect_sc_citations(s3, sc_cluster_ids: set) -> dict[str, str]:
    """Return {cluster_id: "vol reporter page, …"} for SC clusters."""
    key = _find_latest_key(s3, "citations")
    status_tracker["message"] = f"Phase 2.5/3: Collecting citations … (key: {key.split('/')[-1]})"

    sc_citations: dict[str, str] = {}
    total = 0

    for row in _stream_csv(s3, S3_BUCKET, key):
        if not status_tracker["is_running"]:
            break
        total += 1
        if total % 500_000 == 0:
            status_tracker["message"] = (
                f"Phase 2.5/3: Scanned {total:,} citations — {len(sc_citations):,} SC found…"
            )

        cluster_id = row.get("cluster_id", "")
        if cluster_id not in sc_cluster_ids:
            continue

        vol = (row.get("volume") or "").strip()
        rep = (row.get("reporter") or "").strip()
        pg  = (row.get("page") or "").strip()
        if not (vol and rep and pg):
            continue

        cite     = f"{vol} {rep} {pg}"
        existing = sc_citations.get(cluster_id, "")
        sc_citations[cluster_id] = f"{existing}, {cite}" if existing else cite

    print(f"[BulkIngest] Phase 2.5 done: {len(sc_citations):,} SC citations / {total:,} total")
    return sc_citations


# ── Phase 3 ───────────────────────────────────────────────────────────────────

def _process_opinions(
    s3,
    sc_clusters:      dict[str, dict],
    collection:       Collection,
    date:             str,
    start_year:       int | None,
    end_year:         int | None,
    resume_opinion_id: str | None = None,
) -> None:
    """Stream opinions, filter for SC, chunk → embed → upsert to Milvus.

    If resume_opinion_id is set, rows are skipped until that id is passed,
    then processing continues normally.  Progress is checkpointed every
    CHECKPOINT_EVERY opinions so restarts lose at most that many opinions.
    """
    key = _find_latest_key(s3, "opinions")
    if resume_opinion_id:
        status_tracker["message"] = (
            f"Phase 3/3: Resuming after opinion {resume_opinion_id} … (key: {key.split('/')[-1]})"
        )
    else:
        status_tracker["message"] = f"Phase 3/3: Processing opinions … (key: {key.split('/')[-1]})"

    model    = _get_model()
    total    = skipped = 0
    skipping = resume_opinion_id is not None
    since_checkpoint = 0

    for row in _stream_csv(s3, S3_BUCKET, key):
        if not status_tracker["is_running"]:
            break

        total += 1
        raw_id     = row.get("id", "")
        opinion_id = raw_id

        # Resume: fast-forward until we pass the last checkpointed opinion
        if skipping:
            if opinion_id == resume_opinion_id:
                skipping = False
            skipped += 1
            if total % 500_000 == 0:
                status_tracker["message"] = (
                    f"Phase 3/3: Fast-forwarding to resume point … {total:,} rows scanned"
                )
            continue

        if total % 100_000 == 0:
            status_tracker["message"] = (
                f"Phase 3/3: Scanned {total:,} opinions | "
                f"{status_tracker['opinions_processed']:,} SC processed | "
                f"{status_tracker['chunks_upserted']:,} chunks upserted"
            )

        cluster_id = row.get("cluster_id", "")
        if cluster_id not in sc_clusters:
            skipped += 1
            continue

        text = (row.get("plain_text") or "").strip()
        if not text:
            html = (row.get("html_with_citations") or "").strip()
            if html:
                text = _strip_html(html)
        if len(text) < 150:
            skipped += 1
            continue

        meta         = sc_clusters[cluster_id]
        opinion_id_i = int(raw_id) if raw_id.isdigit() else 0
        opinion_type = str(row.get("type", "Lead"))[:50]
        citation     = meta.get("citation", "")

        chunks = legal_chunker(text)
        if not chunks:
            skipped += 1
            continue

        enriched   = [
            enrich_chunk(c, meta["case_name"], meta["court"], meta["year"], opinion_type, citation)
            for c in chunks
        ]
        embeddings = model.encode(enriched, batch_size=EMBED_BATCH, show_progress_bar=False)

        n              = len(enriched)
        cluster_id_int = int(cluster_id) if cluster_id.isdigit() else 0

        collection.insert([
            [opinion_id_i]            * n,  # opinion_id
            [cluster_id_int]          * n,  # cluster_id
            [meta["case_name"][:512]] * n,  # case_name
            [meta["court"]]           * n,  # court
            [meta["year"]]            * n,  # year
            [opinion_type]            * n,  # opinion_type
            list(range(n)),                  # chunk_index
            [c[:65535] for c in enriched],  # text
            [citation[:512]]          * n,  # citation
            embeddings.tolist(),             # vector
        ])

        status_tracker["opinions_processed"] += 1
        status_tracker["chunks_upserted"]    += n
        status_tracker["current_year"]        = meta["year"]
        status_tracker["current_court"]       = meta["court"]

        since_checkpoint += 1
        if since_checkpoint >= CHECKPOINT_EVERY:
            _save_opinions_progress(date, start_year, end_year, opinion_id)
            since_checkpoint = 0

    # Save final checkpoint position (or clear it if complete)
    if not status_tracker["is_running"]:
        # Killed mid-run — save where we stopped
        _save_opinions_progress(date, start_year, end_year, opinion_id)
    else:
        # Completed normally — remove progress file so next run starts clean
        _clear_opinions_progress(date, start_year, end_year)

    print(
        f"[BulkIngest] Phase 3 done: "
        f"scanned {total:,} | "
        f"processed {status_tracker['opinions_processed']:,} | "
        f"skipped {skipped:,}"
    )


# ── Main worker ───────────────────────────────────────────────────────────────

def bulk_ingest_worker(
    collection:  Collection,
    start_year:  int | None = None,
    end_year:    int | None = None,
) -> None:
    """
    Full bulk S3 ingestion pipeline with caching and graceful restart.

    Phases 1/2/2.5 are cached by S3 date + year range and skipped on
    subsequent runs.  Phase 3 checkpoints every CHECKPOINT_EVERY opinions
    so restarts resume from the last saved position.
    """
    year_label = (
        f"{start_year}–{end_year}" if start_year and end_year
        else f"from {start_year}" if start_year
        else f"up to {end_year}"  if end_year
        else "all years"
    )
    status_tracker.update({
        "is_running":         True,
        "current_year":       None,
        "current_court":      None,
        "current_page":       0,
        "opinions_processed": 0,
        "chunks_upserted":    0,
        "total_expected":     0,
        "phase":              "dockets",
        "started_at":         datetime.now(timezone.utc).isoformat(),
        "message":            f"Starting bulk S3 ingest ({year_label})…",
    })

    try:
        s3 = _s3_client()

        # ── Phase 1: dockets (cached) ────────────────────────────────────────
        sc_dockets, date = _collect_sc_dockets(s3)
        if not status_tracker["is_running"]:
            status_tracker["message"] = "Stopped during Phase 1 (dockets)."
            return
        if not sc_dockets:
            status_tracker["message"] = "No SC dockets found — aborting."
            return

        # ── Phase 2 + 2.5: clusters + citations (cached together) ────────────
        status_tracker["phase"] = "clusters"
        sc_clusters = _load_clusters_cache(date, start_year, end_year)

        if sc_clusters is None:
            # Cache miss — run both phases and save combined result
            sc_clusters = _collect_sc_clusters(s3, sc_dockets, start_year, end_year)
            del sc_dockets
            if not status_tracker["is_running"]:
                status_tracker["message"] = "Stopped during Phase 2 (clusters)."
                return
            if not sc_clusters:
                status_tracker["message"] = "No SC clusters found — aborting."
                return

            status_tracker["phase"] = "citations"
            sc_citations = _collect_sc_citations(s3, set(sc_clusters.keys()))
            if not status_tracker["is_running"]:
                status_tracker["message"] = "Stopped during Phase 2.5 (citations)."
                return
            for cid, cite_str in sc_citations.items():
                if cid in sc_clusters:
                    sc_clusters[cid]["citation"] = cite_str
            del sc_citations

            _save_clusters_cache(date, start_year, end_year, sc_clusters)
        else:
            del sc_dockets  # not needed when clusters loaded from cache
            status_tracker["message"] = (
                f"Phases 1/2/2.5 loaded from cache ({len(sc_clusters):,} SC clusters)."
            )

        if not sc_clusters:
            status_tracker["message"] = "No SC clusters found — aborting."
            return

        # ── Phase 3: opinions (with resume support) ──────────────────────────
        status_tracker["phase"]          = "opinions"
        status_tracker["total_expected"] = len(sc_clusters)

        progress      = _load_opinions_progress(date, start_year, end_year)
        resume_id     = None
        if progress:
            resume_id = progress.get("last_opinion_id")
            status_tracker["opinions_processed"] = progress.get("opinions_processed", 0)
            status_tracker["chunks_upserted"]    = progress.get("chunks_upserted", 0)
            print(
                f"[BulkIngest] Resuming Phase 3 after opinion {resume_id} "
                f"({status_tracker['opinions_processed']:,} already done)"
            )

        _process_opinions(s3, sc_clusters, collection, date, start_year, end_year, resume_id)

        if status_tracker["is_running"]:
            status_tracker["phase"]   = "done"
            status_tracker["message"] = (
                f"Bulk ingest complete — "
                f"{status_tracker['opinions_processed']:,} opinions / "
                f"{status_tracker['chunks_upserted']:,} chunks upserted"
            )

    except Exception as exc:
        status_tracker["message"] = f"Bulk ingest error: {exc}"
        print(f"[BulkIngest] !! Worker crashed: {exc}")
        raise

    finally:
        status_tracker["is_running"] = False
        print(
            f"[BulkIngest] Finished. "
            f"Opinions={status_tracker['opinions_processed']:,}  "
            f"Chunks={status_tracker['chunks_upserted']:,}"
        )
