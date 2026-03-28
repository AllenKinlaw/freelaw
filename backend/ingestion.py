import os
import re
import time

import requests
from pymilvus import Collection
from sentence_transformers import SentenceTransformer

from chunker import enrich_chunk, legal_chunker
from progress import get_saved_page, save_progress

# ── Constants ────────────────────────────────────────────────────────────────
COURTS = ["sc", "scctapp"]
BASE_URL = "https://www.courtlistener.com/api/v3"
EMBED_BATCH_SIZE = 16   # opinions per embedding batch
REQUEST_DELAY = 0.5     # seconds between paginated API calls
RATE_LIMIT_BACKOFF = 60 # seconds to wait on 429

# ── Model (loaded once at import time) ───────────────────────────────────────
print("[Ingestion] Loading ModernBERT model — this may take a minute on first run...")
model = SentenceTransformer("answerdotai/ModernBERT-base")
print("[Ingestion] Model ready.")

# ── Shared status (read by /status endpoint) ─────────────────────────────────
status_tracker: dict = {
    "is_running":         False,
    "current_year":       None,
    "current_court":      None,
    "current_page":       0,
    "opinions_processed": 0,
    "chunks_upserted":    0,
    "message":            "Idle",
    # bulk-ingest extras
    "phase":              "idle",   # idle | dockets | clusters | opinions | done
    "total_expected":     0,        # SC opinion clusters found in Phase 2
    "started_at":         None,     # ISO-8601 UTC timestamp when worker started
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def _headers() -> dict:
    key = os.environ.get("COURTLISTENER_API_KEY", "")
    return {"Authorization": f"Token {key}"}


def _strip_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _fetch_cluster_meta(cluster_url: str) -> dict:
    """
    Fetch case_name and cluster_id from the opinion cluster endpoint.
    Returns safe defaults on failure so ingestion never hard-stops on a missing cluster.
    """
    if not cluster_url:
        return {"case_name": "Unknown", "cluster_id": 0}
    try:
        resp = requests.get(cluster_url, headers=_headers(), timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            raw_id = cluster_url.rstrip("/").split("/")[-1]
            return {
                "case_name": data.get("case_name", "Unknown"),
                "cluster_id": int(raw_id) if raw_id.isdigit() else 0,
            }
    except Exception:
        pass
    return {"case_name": "Unknown", "cluster_id": 0}


# ── Core processing ───────────────────────────────────────────────────────────

def _process_page(results: list, court: str, year: int, collection: Collection) -> None:
    """Embed one page of opinions and upsert into Milvus."""
    for op in results:
        if not status_tracker["is_running"]:
            return

        # Prefer plain_text; fall back to HTML (stripped)
        text = op.get("plain_text") or op.get("html_with_citations", "")
        if "<" in text:
            text = _strip_html(text)
        if len(text) < 150:
            continue

        opinion_id   = int(op.get("id", 0))
        opinion_type = str(op.get("type", "Lead"))[:50]
        cluster_url  = op.get("cluster", "")

        meta   = _fetch_cluster_meta(cluster_url)
        chunks = legal_chunker(text)

        if not chunks:
            continue

        enriched   = [enrich_chunk(c, meta["case_name"], court, year, opinion_type) for c in chunks]
        embeddings = model.encode(enriched, batch_size=EMBED_BATCH_SIZE, show_progress_bar=False)

        n = len(enriched)
        collection.insert([
            [opinion_id]                    * n,   # opinion_id
            [meta["cluster_id"]]            * n,   # cluster_id
            [meta["case_name"][:512]]       * n,   # case_name
            [court]                         * n,   # court
            [year]                          * n,   # year
            [opinion_type]                  * n,   # opinion_type
            list(range(n)),                        # chunk_index
            [c[:65535] for c in enriched],         # text
            embeddings.tolist(),                   # vector
        ])

        status_tracker["opinions_processed"] += 1
        status_tracker["chunks_upserted"]    += n


# ── Main worker ───────────────────────────────────────────────────────────────

def ingest_worker(years: list, collection: Collection) -> None:
    """
    Background task.  Iterates years → courts → pages.
    Checks status_tracker["is_running"] at every page so /stop-ingestion
    takes effect within one page (usually < 10 seconds).
    """
    global status_tracker
    status_tracker.update({
        "is_running": True,
        "opinions_processed": 0,
        "chunks_upserted": 0,
    })

    try:
        for year in years:
            if not status_tracker["is_running"]:
                status_tracker["message"] = f"Stopped before year {year}"
                break

            status_tracker["current_year"] = year
            status_tracker["message"]      = f"Starting year {year}"
            print(f"\n>>> Year {year}")

            for court in COURTS:
                if not status_tracker["is_running"]:
                    break

                page = get_saved_page(year, court)
                status_tracker["current_court"] = court
                print(f"  > {court}  (resuming at page {page})")

                while status_tracker["is_running"]:
                    url = (
                        f"{BASE_URL}/opinions/"
                        f"?court={court}&date_filed__year={year}&page={page}"
                    )
                    try:
                        resp = requests.get(url, headers=_headers(), timeout=30)
                    except requests.RequestException as exc:
                        print(f"  !! Network error: {exc} — retrying in 10 s")
                        time.sleep(10)
                        continue

                    if resp.status_code == 404:
                        break
                    if resp.status_code == 429:
                        print(f"  !! Rate limited — waiting {RATE_LIMIT_BACKOFF} s")
                        time.sleep(RATE_LIMIT_BACKOFF)
                        continue
                    if resp.status_code != 200:
                        print(f"  !! HTTP {resp.status_code} — skipping court {court} / year {year}")
                        break

                    results = resp.json().get("results", [])
                    if not results:
                        break

                    status_tracker["current_page"] = page
                    status_tracker["message"] = (
                        f"Year {year} | {court} | page {page} | {len(results)} opinions"
                    )
                    print(f"    page {page}: {len(results)} opinions")

                    _process_page(results, court, year, collection)
                    save_progress(year, court, page)
                    page += 1
                    time.sleep(REQUEST_DELAY)

                print(f"  < done {court}")

            status_tracker["message"] = f"Finished year {year}"
            print(f"<<< Year {year} complete")

        if status_tracker["is_running"]:
            status_tracker["message"] = (
                f"All years completed — "
                f"{status_tracker['opinions_processed']} opinions / "
                f"{status_tracker['chunks_upserted']} chunks"
            )

    except Exception as exc:
        status_tracker["message"] = f"Worker error: {exc}"
        print(f"!! Worker crashed: {exc}")

    finally:
        status_tracker["is_running"] = False
        print(
            f"\n[Worker] Done.  "
            f"Opinions: {status_tracker['opinions_processed']}  "
            f"Chunks: {status_tracker['chunks_upserted']}"
        )
