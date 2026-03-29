"""
bulk_ingest.py – Stream appellate court opinions from CourtListener S3 bulk data.

Pipeline (no large downloads — everything streams from S3):
  Phase 1    – dockets CSV          → collect INGEST_COURTS docket IDs
  Phase 2    – opinion_clusters CSV → collect INGEST_COURTS cluster metadata
  Phase 2.5  – citations CSV        → attach citation strings to clusters
  Phase 3a-1 – dockets CSV          → collect DB_COURTS docket IDs (wider set)
  Phase 3a-2 – opinion_clusters CSV → collect DB_COURTS cluster metadata (all years)
  Phase 3a   – opinions CSV         → extract text-bearing rows for DB_COURTS → SQLite
  Phase 3b   – SQLite query         → chunk → embed → upsert to Milvus

Courts
──────
DB_COURTS    – all state supreme courts + federal circuits + SCOTUS + state appellate
               courts (~106 courts).  All text-bearing opinions stored in local SQLite.
INGEST_COURTS – subset currently being embedded into Milvus (SC only for now).

Caching & graceful restart
──────────────────────────
Phase 1/2/2.5 results cached by S3 date + year range; skipped on subsequent runs.
Phase 3a-1/3a-2 DB_COURTS dockets + clusters cached by S3 date only.
Phase 3a SQLite DB cached by S3 date — covers all DB_COURTS courts and years so any
  court/year combination queries it in milliseconds without another S3 scan.
Phase 3b checkpoints every CHECKPOINT_EVERY opinions for graceful resume.

Public bucket access (no AWS credentials required):
  s3://com-courtlistener-storage/bulk-data/
"""

import bz2
import csv
import io
import json
import os
import re
import sqlite3
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
EMBED_BATCH      = 64
CHECKPOINT_EVERY = 100          # save Phase 3b progress every N opinions
MIN_TEXT_LEN     = 150          # skip opinions with less useful text than this
CACHE_DIR        = Path("bulk_ingest_cache")

# Courts whose opinions get stored in the local SQLite DB (Phase 3a).
# Superset of INGEST_COURTS — Phase 3b queries are filtered to INGEST_COURTS.
DB_COURTS: frozenset[str] = frozenset({
    # US Supreme Court
    "scotus",
    # Federal circuits
    "ca1", "ca2", "ca3", "ca4", "ca5", "ca6", "ca7", "ca8", "ca9",
    "ca10", "ca11", "cadc", "cafc",
    # State supreme courts (50 states + DC)
    "ala", "alaska", "ariz", "ark", "cal", "colo", "conn", "del", "dc",
    "fla", "ga", "haw", "idaho", "ill", "ind", "iowa", "kan", "ky", "la",
    "me", "md", "mass", "mich", "minn", "miss", "mo", "mont", "neb", "nev",
    "nh", "nj", "nm", "ny", "nc", "nd", "ohio", "okla", "or", "pa", "ri",
    "sc", "sd", "tenn", "tex", "utah", "vt", "va", "wash", "wva", "wis", "wyo",
    # State courts of appeals
    "alacivapp", "alacrimapp", "arizctapp", "arkctapp", "calctapp", "coloapp",
    "connappct", "flaapp", "gaapp", "idahoctapp", "illappct", "indctapp",
    "iowactapp", "kanctapp", "kyctapp", "laapp", "mdapp", "massappct",
    "michctapp", "minnctapp", "missctapp", "moapp", "nebctapp",
    "njsuperctappdiv", "nmctapp", "ncctapp", "ohioctapp", "oklaapp",
    "oklacrimapp", "orctapp", "pacommwct", "pasuperct", "scctapp",
    "tennctapp", "tenncrimapp", "texapp", "texcrimapp", "utahctapp",
    "vaapp", "washctapp", "wisctapp",
})

# Courts currently being embedded into Milvus (SC only for now).
INGEST_COURTS: frozenset[str] = frozenset({"sc", "scctapp"})

# Backward-compat alias used in Phase 1/2 helper names
SC_COURTS = INGEST_COURTS

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


def _query_opinions_db(
    db_path:    Path,
    start_year: int | None,
    end_year:   int | None,
    courts:     list[str] | None = None,
):
    """Yield opinion rows from the local SQLite cache.

    Filters by an optional court list and year range so Phase 3b only sees
    the INGEST_COURTS subset for the requested year window.
    """
    print(f"[BulkIngest] Querying local SQLite cache  {db_path.name}")
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    clauses: list[str] = []
    params:  list      = []

    if courts:
        placeholders = ",".join("?" * len(courts))
        clauses.append(f"court IN ({placeholders})")
        params.extend(courts)
    if start_year:
        clauses.append("year >= ?")
        params.append(start_year)
    if end_year:
        clauses.append("year <= ?")
        params.append(end_year)

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    cur.execute(f"SELECT * FROM opinions {where} ORDER BY id", params)

    for row in cur:
        yield dict(row)
    con.close()


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
    """INGEST_COURTS dockets cache (Phase 1)."""
    return CACHE_DIR / f"dockets_{date}.json"


def _clusters_cache_path(date: str, start_year, end_year) -> Path:
    """INGEST_COURTS clusters cache (Phase 2/2.5) — keyed by year range."""
    return CACHE_DIR / f"clusters_{date}_{_year_tag(start_year, end_year)}.json"


def _db_dockets_cache_path(date: str) -> Path:
    """DB_COURTS dockets cache (Phase 3a-1) — all courts, all years."""
    return CACHE_DIR / f"db_dockets_{date}.json"


def _db_clusters_cache_path(date: str) -> Path:
    """DB_COURTS clusters cache (Phase 3a-2) — all courts, all years."""
    return CACHE_DIR / f"db_clusters_{date}.json"


def _opinions_db_path(date: str) -> Path:
    """SQLite DB of text-bearing opinions for DB_COURTS, keyed by S3 date only.
    Indexed by (court, year) for instant Phase 3b queries.
    v2 schema adds court + year columns and text filter vs the original SC-only DB.
    """
    return CACHE_DIR / f"opinions_v2_{date}.db"


def _opinions_meta_path(date: str) -> Path:
    """JSON storing the total row count of the opinions CSV for this date."""
    return CACHE_DIR / f"opinions_meta_{date}.json"


def _load_opinions_total(date: str) -> int | None:
    path = _opinions_meta_path(date)
    if path.exists():
        with open(path) as f:
            return json.load(f).get("total_rows")
    return None


def _save_opinions_total(date: str, total_rows: int) -> None:
    _atomic_write(_opinions_meta_path(date), {"total_rows": total_rows})


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


# ── Phase 1 — INGEST_COURTS dockets ──────────────────────────────────────────

def _collect_sc_dockets(s3) -> tuple[dict[str, str], str]:
    """Return ({docket_id: court_id}, s3_date) for INGEST_COURTS.  Uses cache if available."""
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
        if row.get("court_id") in INGEST_COURTS:
            sc_dockets[row["id"]] = row["court_id"]

    print(f"[BulkIngest] Phase 1 done: {len(sc_dockets):,} SC dockets / {total:,} total")

    if status_tracker["is_running"]:
        _save_dockets_cache(date, sc_dockets)

    return sc_dockets, date


# ── Phase 2 — INGEST_COURTS clusters ─────────────────────────────────────────

def _collect_sc_clusters(
    s3,
    sc_dockets: dict[str, str],
    start_year: int | None = None,
    end_year:   int | None = None,
) -> dict[str, dict]:
    """Return {cluster_id: {case_name, court, year}} for INGEST_COURTS opinion clusters."""
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


# ── Phase 2.5 — Citations ─────────────────────────────────────────────────────

def _collect_sc_citations(s3, sc_cluster_ids: set) -> dict[str, str]:
    """Return {cluster_id: "vol reporter page, …"} for INGEST_COURTS clusters."""
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


# ── Phase 3a-1 — DB_COURTS dockets ───────────────────────────────────────────

def _collect_db_dockets(s3) -> tuple[dict[str, str], str]:
    """Return ({docket_id: court_id}, s3_date) for all DB_COURTS.  Uses cache."""
    key  = _find_latest_key(s3, "dockets")
    date = _date_from_key(key)

    path = _db_dockets_cache_path(date)
    if path.exists():
        print(f"[BulkIngest] DB dockets cache hit — {path.name}")
        status_tracker["message"] = f"Phase 3a-1: Loaded DB dockets from cache."
        with open(path) as f:
            return json.load(f), date

    status_tracker["message"] = (
        f"Phase 3a-1: Scanning dockets for {len(DB_COURTS)} courts… ({key.split('/')[-1]})"
    )
    db_dockets: dict[str, str] = {}
    total = 0

    for row in _stream_csv(s3, S3_BUCKET, key):
        if not status_tracker["is_running"]:
            break
        total += 1
        if total % 500_000 == 0:
            status_tracker["message"] = (
                f"Phase 3a-1: Scanned {total:,} dockets — {len(db_dockets):,} found…"
            )
        if row.get("court_id") in DB_COURTS:
            db_dockets[row["id"]] = row["court_id"]

    print(f"[BulkIngest] DB dockets: {len(db_dockets):,} / {total:,} total rows")

    if status_tracker["is_running"]:
        _atomic_write(path, db_dockets)
        print(f"[BulkIngest] DB dockets cached → {path.name}")

    return db_dockets, date


# ── Phase 3a-2 — DB_COURTS clusters ──────────────────────────────────────────

def _collect_db_clusters(s3, db_dockets: dict[str, str], date: str) -> dict[str, dict]:
    """Return {cluster_id: {court, year}} for all DB_COURTS, all years.  Uses cache."""
    path = _db_clusters_cache_path(date)
    if path.exists():
        print(f"[BulkIngest] DB clusters cache hit — {path.name}")
        status_tracker["message"] = f"Phase 3a-2: Loaded DB clusters from cache."
        with open(path) as f:
            return json.load(f)

    key = _find_latest_key(s3, "opinion-clusters")
    status_tracker["message"] = (
        f"Phase 3a-2: Scanning clusters for {len(DB_COURTS)} courts… ({key.split('/')[-1]})"
    )
    db_clusters: dict[str, dict] = {}
    total = 0

    for row in _stream_csv(s3, S3_BUCKET, key):
        if not status_tracker["is_running"]:
            break
        total += 1
        if total % 200_000 == 0:
            status_tracker["message"] = (
                f"Phase 3a-2: Scanned {total:,} clusters — {len(db_clusters):,} found…"
            )
        docket_id = row.get("docket_id", "")
        if docket_id not in db_dockets:
            continue

        date_filed = row.get("date_filed", "") or ""
        try:
            year = int(date_filed[:4]) if len(date_filed) >= 4 else 0
        except ValueError:
            year = 0

        db_clusters[row["id"]] = {
            "court": db_dockets[docket_id],
            "year":  year,
        }

    print(f"[BulkIngest] DB clusters: {len(db_clusters):,} / {total:,} total rows")

    if status_tracker["is_running"]:
        _atomic_write(path, db_clusters)
        print(f"[BulkIngest] DB clusters cached → {path.name}")

    return db_clusters


# ── Phase 3a — Build opinions SQLite DB ──────────────────────────────────────

def _build_opinions_db(s3, db_clusters: dict[str, dict], date: str) -> Path:
    """Stream the full S3 opinions CSV ONCE; write text-bearing rows for DB_COURTS to SQLite.

    • Covers ALL DB_COURTS and ALL years — one DB per S3 snapshot date.
    • Text-bearing filter: rows with < MIN_TEXT_LEN chars are skipped (eliminates
      the ~1.4B PACER stub rows that have no opinion text).
    • Adds court and year columns (from db_clusters lookup) so Phase 3b can query
      instantly: SELECT * FROM opinions WHERE court IN (…) AND year BETWEEN ? AND ?

    Returns the path to the completed SQLite DB.
    """
    db_path = _opinions_db_path(date)
    key     = _find_latest_key(s3, "opinions")
    status_tracker["message"] = (
        f"Phase 3a: Building opinions DB ({len(DB_COURTS)} courts) … ({key.split('/')[-1]})"
    )

    CACHE_DIR.mkdir(exist_ok=True)

    # Temp path so a killed run never leaves a partial DB that looks valid
    tmp_path = db_path.with_suffix(".tmp.db")
    tmp_path.unlink(missing_ok=True)

    con = sqlite3.connect(tmp_path)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("""
        CREATE TABLE opinions (
            id                   TEXT PRIMARY KEY,
            cluster_id           TEXT,
            court                TEXT,
            year                 INTEGER,
            type                 TEXT,
            plain_text           TEXT,
            html_with_citations  TEXT
        )
    """)
    con.execute("CREATE INDEX idx_cluster  ON opinions(cluster_id)")
    con.execute("CREATE INDEX idx_court_yr ON opinions(court, year)")
    con.commit()

    db_cluster_ids = set(db_clusters.keys())
    total = db_found = 0
    BATCH = 500
    rows_batch: list = []

    def _flush():
        con.executemany(
            "INSERT OR IGNORE INTO opinions VALUES (?,?,?,?,?,?,?)",
            rows_batch,
        )
        con.commit()
        rows_batch.clear()

    status_tracker["db_rows_scanned"] = 0
    status_tracker["db_rows_written"] = 0

    for row in _stream_csv(s3, S3_BUCKET, key):
        if not status_tracker["is_running"]:
            break
        total += 1
        if total % 100_000 == 0:
            status_tracker["db_rows_scanned"] = total
            status_tracker["db_rows_written"]  = db_found
            status_tracker["message"] = (
                f"Phase 3a: Scanned {total:,} opinions — {db_found:,} stored…"
            )

        cluster_id = row.get("cluster_id", "")
        if cluster_id not in db_cluster_ids:
            continue

        # Text-bearing filter — skip stubs with no useful content
        text = (row.get("plain_text") or "").strip()
        if not text:
            html = (row.get("html_with_citations") or "").strip()
            if html:
                text = _strip_html(html)
        if len(text) < MIN_TEXT_LEN:
            continue

        meta = db_clusters[cluster_id]
        rows_batch.append((
            row.get("id", ""),
            cluster_id,
            meta["court"],
            meta["year"],
            row.get("type", ""),
            row.get("plain_text", ""),
            row.get("html_with_citations", ""),
        ))
        db_found += 1

        if len(rows_batch) >= BATCH:
            _flush()
            status_tracker["db_rows_written"] = db_found

    if rows_batch:
        _flush()

    con.close()

    if status_tracker["is_running"]:
        # Atomic promote temp → final
        tmp_path.replace(db_path)
        mb = db_path.stat().st_size / 1024 / 1024
        status_tracker["db_rows_scanned"] = total
        status_tracker["db_rows_written"]  = db_found
        status_tracker["db_total_mb"]      = round(mb, 1)
        _save_opinions_total(date, total)   # persist for accurate % on future runs
        print(
            f"[BulkIngest] Phase 3a done: {db_found:,} opinions stored / {total:,} scanned "
            f"→ {db_path.name} ({mb:.1f} MB)"
        )
    else:
        tmp_path.unlink(missing_ok=True)
        print("[BulkIngest] Phase 3a interrupted — temp DB discarded.")

    return db_path


# ── Phase 3b — Embed & upsert ─────────────────────────────────────────────────

def _process_opinions(
    s3,
    sc_clusters:       dict[str, dict],
    collection:        Collection,
    date:              str,
    start_year:        int | None,
    end_year:          int | None,
    db_path:           Path | None = None,
    resume_opinion_id: str | None  = None,
) -> None:
    """Chunk → embed → upsert INGEST_COURTS opinions to Milvus.

    Reads from the local SQLite DB (Phase 3a output) filtered by INGEST_COURTS
    and year range.  Falls back to streaming from S3 if the DB doesn't exist.
    Progress is checkpointed every CHECKPOINT_EVERY opinions for graceful resume.
    """
    if db_path and db_path.exists():
        source_label = db_path.name
        rows = _query_opinions_db(
            db_path, start_year, end_year,
            courts=list(INGEST_COURTS),
        )
    else:
        key  = _find_latest_key(s3, "opinions")
        source_label = key.split("/")[-1]
        rows = _stream_csv(s3, S3_BUCKET, key)

    if resume_opinion_id:
        status_tracker["message"] = (
            f"Phase 3b: Resuming after opinion {resume_opinion_id} … ({source_label})"
        )
    else:
        status_tracker["message"] = f"Phase 3b: Processing opinions … ({source_label})"

    model    = _get_model()
    total    = skipped = 0
    skipping = resume_opinion_id is not None
    since_checkpoint = 0
    opinion_id = ""

    for row in rows:
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
        if len(text) < MIN_TEXT_LEN:
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

    # Save final checkpoint (or clear if complete)
    if not status_tracker["is_running"] and opinion_id:
        _save_opinions_progress(date, start_year, end_year, opinion_id)
    else:
        _clear_opinions_progress(date, start_year, end_year)

    print(
        f"[BulkIngest] Phase 3b done: "
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

    Phases 1/2/2.5 (INGEST_COURTS) and Phase 3a-1/3a-2 (DB_COURTS) are each
    cached by S3 date and skipped on subsequent runs.  Phase 3b checkpoints
    every CHECKPOINT_EVERY opinions so restarts resume from the last saved
    position.
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
        "db_rows_scanned":    0,
        "db_rows_written":    0,
        "db_rows_total":      0,
        "db_total_mb":        0.0,
        "db_courts_total":    len(DB_COURTS),
    })

    try:
        s3 = _s3_client()

        # ── Phase 1: INGEST_COURTS dockets (cached) ──────────────────────────
        sc_dockets, date = _collect_sc_dockets(s3)
        if not status_tracker["is_running"]:
            status_tracker["message"] = "Stopped during Phase 1 (dockets)."
            return
        if not sc_dockets:
            status_tracker["message"] = "No SC dockets found — aborting."
            return

        # ── Phase 2 + 2.5: INGEST_COURTS clusters + citations (cached) ───────
        status_tracker["phase"] = "clusters"
        sc_clusters = _load_clusters_cache(date, start_year, end_year)

        if sc_clusters is None:
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
            del sc_dockets
            status_tracker["message"] = (
                f"Phases 1/2/2.5 loaded from cache ({len(sc_clusters):,} SC clusters)."
            )

        if not sc_clusters:
            status_tracker["message"] = "No SC clusters found — aborting."
            return

        # ── Phase 3a: build broad opinions SQLite DB ──────────────────────────
        status_tracker["phase"]        = "prefilter"
        status_tracker["db_courts_total"] = len(DB_COURTS)
        db_path = _opinions_db_path(date)

        # Load known row total for accurate progress % (set after first full scan)
        known_total = _load_opinions_total(date)
        if known_total:
            status_tracker["db_rows_total"] = known_total

        if db_path.exists():
            mb = db_path.stat().st_size / 1024 / 1024
            print(f"[BulkIngest] Phase 3a cache hit — {db_path.name} ({mb:.1f} MB)")
            status_tracker["message"] = (
                f"Phase 3a: Opinions DB ready ({mb:.1f} MB, {len(DB_COURTS)} courts) — "
                f"skipping S3 scan."
            )
        else:
            # Need to build the DB — first collect DB_COURTS dockets + clusters
            db_clusters_cache = _db_clusters_cache_path(date)
            if db_clusters_cache.exists():
                with open(db_clusters_cache) as f:
                    db_clusters_for_build = json.load(f)
                print(
                    f"[BulkIngest] DB clusters cache hit — "
                    f"{len(db_clusters_for_build):,} entries"
                )
                status_tracker["message"] = (
                    f"Phase 3a: Loaded {len(db_clusters_for_build):,} clusters from cache."
                )
            else:
                # Phase 3a-1: collect DB_COURTS dockets
                db_dockets_for_build, _ = _collect_db_dockets(s3)
                if not status_tracker["is_running"]:
                    status_tracker["message"] = "Stopped during Phase 3a-1 (DB dockets)."
                    return
                if not db_dockets_for_build:
                    status_tracker["message"] = "No DB_COURTS dockets found — aborting."
                    return

                # Phase 3a-2: collect DB_COURTS clusters (all years)
                db_clusters_for_build = _collect_db_clusters(
                    s3, db_dockets_for_build, date
                )
                del db_dockets_for_build
                if not status_tracker["is_running"]:
                    status_tracker["message"] = "Stopped during Phase 3a-2 (DB clusters)."
                    return

            _build_opinions_db(s3, db_clusters_for_build, date)
            del db_clusters_for_build
            if not status_tracker["is_running"]:
                status_tracker["message"] = "Stopped during Phase 3a (building opinions DB)."
                return

        # ── Phase 3b: embed & upsert (with resume support) ───────────────────
        status_tracker["phase"]          = "opinions"
        status_tracker["total_expected"] = len(sc_clusters)

        progress  = _load_opinions_progress(date, start_year, end_year)
        resume_id = None
        if progress:
            resume_id = progress.get("last_opinion_id")
            status_tracker["opinions_processed"] = progress.get("opinions_processed", 0)
            status_tracker["chunks_upserted"]    = progress.get("chunks_upserted", 0)
            print(
                f"[BulkIngest] Resuming Phase 3b after opinion {resume_id} "
                f"({status_tracker['opinions_processed']:,} already done)"
            )

        _process_opinions(
            s3, sc_clusters, collection, date, start_year, end_year,
            db_path=db_path, resume_opinion_id=resume_id,
        )

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
