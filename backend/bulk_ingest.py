"""
bulk_ingest.py – Stream SC court opinions from CourtListener S3 bulk data.

Pipeline (no large downloads — everything streams from S3):
  Phase 1   – dockets CSV          → collect SC docket IDs  (court_id in {'sc','scctapp'})
  Phase 2   – opinion_clusters CSV → collect SC cluster metadata  (case_name, year, court)
  Phase 2.5 – citations CSV        → attach citation strings to SC clusters
  Phase 3   – opinions CSV         → chunk → embed → upsert to Milvus

Public bucket access (no AWS credentials required):
  s3://com-courtlistener-storage/bulk-data/
"""

import bz2
import csv
import io
import os
import re
from datetime import datetime, timezone

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from pymilvus import Collection
from sentence_transformers import SentenceTransformer

from chunker import enrich_chunk, legal_chunker
from ingestion import status_tracker  # shared live-status dict

# ── Constants ────────────────────────────────────────────────────────────────
S3_BUCKET   = "com-courtlistener-storage"
S3_PREFIX   = "bulk-data"
SC_COURTS   = {"sc", "scctapp"}
EMBED_BATCH = 16

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
    """
    List the bucket and return the key of the most recent bz2 CSV whose name
    starts with *file_prefix* (e.g. 'dockets', 'opinions').
    Date stamps are YYYY-MM-DD so lexicographic sort == chronological sort.
    """
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
        # Refill internal buffer from S3 + bz2 until we have enough bytes
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


# ── Phase 1 ───────────────────────────────────────────────────────────────────

def _collect_sc_dockets(s3) -> dict[str, str]:
    """Return {docket_id: court_id} for all SC / SCCtApp dockets."""
    key = _find_latest_key(s3, "dockets")
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
    return sc_dockets


# ── Phase 2 ───────────────────────────────────────────────────────────────────

def _collect_sc_clusters(
    s3,
    sc_dockets: dict[str, str],
    start_year: int | None = None,
    end_year:   int | None = None,
) -> dict[str, dict]:
    """Return {cluster_id: {case_name, court, year}} for SC opinion clusters.

    If start_year / end_year are provided, only clusters whose date_filed falls
    within [start_year, end_year] (inclusive) are kept.
    """
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

        # Apply year range filter
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
    """Return {cluster_id: "vol reporter page[, vol reporter page, ...]"} for SC clusters.

    Streams the citations CSV (volume, reporter, page, cluster_id) and builds a
    comma-separated citation string per cluster, e.g. "272 S.C. 120, 251 S.E.2d 890".
    Only clusters already in sc_cluster_ids are kept.
    """
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

        cite = f"{vol} {rep} {pg}"
        existing = sc_citations.get(cluster_id, "")
        sc_citations[cluster_id] = f"{existing}, {cite}" if existing else cite

    print(f"[BulkIngest] Phase 2.5 done: {len(sc_citations):,} SC citations / {total:,} total")
    return sc_citations


# ── Phase 3 ───────────────────────────────────────────────────────────────────

def _process_opinions(s3, sc_clusters: dict[str, dict], collection: Collection) -> None:
    """Stream opinions, filter for SC, chunk → embed → upsert to Milvus."""
    key = _find_latest_key(s3, "opinions")
    status_tracker["message"] = f"Phase 3/3: Processing opinions … (key: {key.split('/')[-1]})"

    model = _get_model()
    total = skipped = 0

    for row in _stream_csv(s3, S3_BUCKET, key):
        if not status_tracker["is_running"]:
            break

        total += 1
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

        # Prefer plain_text; fall back to html_with_citations
        text = (row.get("plain_text") or "").strip()
        if not text:
            html = (row.get("html_with_citations") or "").strip()
            if html:
                text = _strip_html(html)
        if len(text) < 150:
            skipped += 1
            continue

        meta         = sc_clusters[cluster_id]
        raw_id       = row.get("id", "")
        opinion_id   = int(raw_id) if raw_id.isdigit() else 0
        opinion_type = str(row.get("type", "Lead"))[:50]

        chunks = legal_chunker(text)
        if not chunks:
            skipped += 1
            continue

        citation = meta.get("citation", "")

        enriched   = [
            enrich_chunk(c, meta["case_name"], meta["court"], meta["year"], opinion_type, citation)
            for c in chunks
        ]
        embeddings = model.encode(enriched, batch_size=EMBED_BATCH, show_progress_bar=False)

        n = len(enriched)
        cluster_id_int = int(cluster_id) if cluster_id.isdigit() else 0

        collection.insert([
            [opinion_id]              * n,  # opinion_id
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
    Full bulk S3 ingestion pipeline.
    Runs as a FastAPI BackgroundTask — updates status_tracker throughout.

    start_year / end_year: optional inclusive year bounds (None = no limit).
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

        # ── Phase 1: dockets ────────────────────────────────────────────────
        sc_dockets = _collect_sc_dockets(s3)
        if not status_tracker["is_running"]:
            status_tracker["message"] = "Stopped during Phase 1 (dockets)."
            return
        if not sc_dockets:
            status_tracker["message"] = "No SC dockets found — aborting."
            return

        # ── Phase 2: opinion clusters ────────────────────────────────────────
        status_tracker["phase"] = "clusters"
        sc_clusters = _collect_sc_clusters(s3, sc_dockets, start_year, end_year)
        del sc_dockets  # free memory — no longer needed
        if not status_tracker["is_running"]:
            status_tracker["message"] = "Stopped during Phase 2 (clusters)."
            return
        if not sc_clusters:
            status_tracker["message"] = "No SC clusters found — aborting."
            return

        # ── Phase 2.5: citations ─────────────────────────────────────────────
        status_tracker["phase"] = "citations"
        sc_citations = _collect_sc_citations(s3, set(sc_clusters.keys()))
        if not status_tracker["is_running"]:
            status_tracker["message"] = "Stopped during Phase 2.5 (citations)."
            return
        # Attach citation strings to cluster metadata
        for cid, cite_str in sc_citations.items():
            if cid in sc_clusters:
                sc_clusters[cid]["citation"] = cite_str
        del sc_citations  # free memory

        # ── Phase 3: opinions ────────────────────────────────────────────────
        status_tracker["phase"]          = "opinions"
        status_tracker["total_expected"] = len(sc_clusters)
        _process_opinions(s3, sc_clusters, collection)

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
