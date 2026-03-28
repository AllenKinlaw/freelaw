"""
court_stats.py — One-time scan of the CourtListener S3 dockets CSV.

Counts dockets per court_id and derives estimated Milvus storage for each court.
Results are cached in court_stats.json and served by the /court-stats endpoint.

Storage model
─────────────
  dockets  ×  AVG_OPINIONS_PER_DOCKET  ×  AVG_CHUNKS_PER_OPINION  ×  BYTES_PER_CHUNK
              2.2                          3.0                         5 000

  5 000 bytes/chunk = 3 072 (768-dim float32 vector)
                    + ~1 928 (metadata fields + HNSW graph overhead)
"""

import json
import os
from collections import defaultdict
from datetime import datetime, timezone

S3_BUCKET  = "com-courtlistener-storage"
STATS_FILE = "court_stats.json"

AVG_OPINIONS_PER_DOCKET = 2.2
AVG_CHUNKS_PER_OPINION  = 3.0
BYTES_PER_CHUNK         = 5_000

# ── Shared scan status (polled via /court-stats) ──────────────────────────────
scan_status: dict = {
    "is_scanning": False,
    "message":     "Not yet scanned — click Scan to generate estimates.",
    "dockets_scanned": 0,
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def stats_exist() -> bool:
    return os.path.exists(STATS_FILE)


def load_stats() -> dict:
    if stats_exist():
        with open(STATS_FILE) as f:
            return json.load(f)
    return {}


def _fmt_bytes(n: int) -> str:
    if n < 1_000_000:
        return f"{n // 1_000} KB"
    elif n < 1_000_000_000:
        return f"{n / 1_000_000:.1f} MB"
    else:
        return f"{n / 1_000_000_000:.2f} GB"


# ── Background worker ─────────────────────────────────────────────────────────

def scan_worker() -> None:
    """
    Streams the most recent dockets CSV from S3, counts dockets per court_id,
    derives storage estimates, and saves to court_stats.json.
    Runs once as a FastAPI BackgroundTask — typically takes 5–10 minutes.
    """
    from bulk_ingest import _find_latest_key, _s3_client, _stream_csv  # avoid circular at import

    scan_status.update({
        "is_scanning":    True,
        "message":        "Starting dockets scan…",
        "dockets_scanned": 0,
    })

    try:
        s3  = _s3_client()
        key = _find_latest_key(s3, "dockets")
        scan_status["message"] = f"Scanning {key.split('/')[-1]} …"

        counts: dict[str, int] = defaultdict(int)
        total = 0

        for row in _stream_csv(s3, S3_BUCKET, key):
            total += 1
            if total % 500_000 == 0:
                scan_status["message"]        = f"Scanned {total:,} dockets…"
                scan_status["dockets_scanned"] = total

            court_id = row.get("court_id", "").strip()
            if court_id:
                counts[court_id] += 1

        # Build per-court storage estimates
        courts_out: dict[str, dict] = {}
        for court_id, docket_count in sorted(counts.items()):
            est_opinions = int(docket_count * AVG_OPINIONS_PER_DOCKET)
            est_chunks   = int(est_opinions  * AVG_CHUNKS_PER_OPINION)
            est_bytes    = est_chunks * BYTES_PER_CHUNK
            courts_out[court_id] = {
                "dockets":      docket_count,
                "est_opinions": est_opinions,
                "est_chunks":   est_chunks,
                "est_bytes":    est_bytes,
                "est_storage":  _fmt_bytes(est_bytes),
            }

        payload = {
            "scanned_at":    datetime.now(timezone.utc).isoformat(),
            "total_dockets": total,
            "total_courts":  len(courts_out),
            "courts":        courts_out,
        }
        with open(STATS_FILE, "w") as f:
            json.dump(payload, f)

        scan_status.update({
            "is_scanning":     False,
            "message":         f"Scan complete — {len(courts_out):,} courts, {total:,} dockets.",
            "dockets_scanned": total,
        })
        print(f"[CourtStats] Done: {len(courts_out)} courts, {total:,} dockets")

    except Exception as exc:
        scan_status.update({"is_scanning": False, "message": f"Scan error: {exc}"})
        print(f"[CourtStats] Error: {exc}")
        raise
