"""
embed_script.py — SageMaker Processing entry point for FreeLaw opinion embedding.

Runs on ml.g4dn.xlarge (T4 GPU, 16 GB GPU RAM, ~$0.74/hr).
Reads opinion Parquet files written by Glue Job 3, chunks + enriches + embeds
each opinion, and upserts vectors directly to Zilliz Cloud (Milvus).

Environment variables (passed by run_job.py):
  MILVUS_URI      — Zilliz Cloud URI
  MILVUS_TOKEN    — Zilliz Cloud API token
  INGEST_COURTS   — comma-separated court IDs  (default: sc,scctapp)
  START_YEAR      — optional lower year bound   (default: none)
  END_YEAR        — optional upper year bound   (default: none)

SageMaker input channels (mounted under /opt/ml/processing/input/):
  opinions/court=sc/        — Parquet for each INGEST_COURT
  opinions/court=scctapp/
  checkpoint/               — previous checkpoint JSON (may be empty on first run)

SageMaker output channel:
  /opt/ml/processing/output/ — updated checkpoint JSON written here, then synced
                               back to s3://<bucket>/freelaw/checkpoints/
"""

import json
import os
import re
import sys
from pathlib import Path

import pandas as pd
from pymilvus import (
    Collection,
    CollectionSchema,
    DataType,
    FieldSchema,
    connections,
    utility,
)
from sentence_transformers import SentenceTransformer

# ── Paths ─────────────────────────────────────────────────────────────────────

INPUT_DIR  = Path("/opt/ml/processing/input")
OUTPUT_DIR = Path("/opt/ml/processing/output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CHECKPOINT_OUT  = OUTPUT_DIR / "embed_checkpoint.json"
CHECKPOINT_IN   = INPUT_DIR  / "checkpoint" / "embed_checkpoint.json"

# ── Config from environment ───────────────────────────────────────────────────

MILVUS_URI    = os.environ["MILVUS_URI"]
MILVUS_TOKEN  = os.environ["MILVUS_TOKEN"]
INGEST_COURTS = set(os.environ.get("INGEST_COURTS", "sc,scctapp").split(","))
START_YEAR    = int(os.environ["START_YEAR"]) if os.environ.get("START_YEAR") else None
END_YEAR      = int(os.environ["END_YEAR"])   if os.environ.get("END_YEAR")   else None

# Larger batch on GPU vs EC2 CPU — T4 can handle 256 comfortably at 768-dim
EMBED_BATCH       = 256
CHECKPOINT_EVERY  = 500   # upsert checkpoint every N opinions
MIN_TEXT_LEN      = 150

# ── Milvus schema (mirrors backend/milvus_client.py exactly) ─────────────────

COLLECTION_NAME = "sc_legal_rag"
VECTOR_DIM      = 768
REQUIRED_FIELDS = {
    "id", "opinion_id", "cluster_id", "case_name", "court",
    "year", "opinion_type", "chunk_index", "text", "citation", "vector",
}


def connect_milvus() -> None:
    connections.connect(alias="default", uri=MILVUS_URI, token=MILVUS_TOKEN)
    print(f"[Embed] Connected to Milvus: {MILVUS_URI}")


def get_or_create_collection() -> Collection:
    if utility.has_collection(COLLECTION_NAME):
        col      = Collection(COLLECTION_NAME)
        existing = {f.name for f in col.schema.fields}
        if REQUIRED_FIELDS.issubset(existing):
            print(f"[Embed] Loading existing collection '{COLLECTION_NAME}'")
            col.load()
            return col
        print(f"[Embed] Schema outdated — dropping '{COLLECTION_NAME}'")
        utility.drop_collection(COLLECTION_NAME)

    print(f"[Embed] Creating collection '{COLLECTION_NAME}'")
    fields = [
        FieldSchema(name="id",           dtype=DataType.INT64,        is_primary=True, auto_id=True),
        FieldSchema(name="opinion_id",   dtype=DataType.INT64),
        FieldSchema(name="cluster_id",   dtype=DataType.INT64),
        FieldSchema(name="case_name",    dtype=DataType.VARCHAR,      max_length=512),
        FieldSchema(name="court",        dtype=DataType.VARCHAR,      max_length=20),
        FieldSchema(name="year",         dtype=DataType.INT32),
        FieldSchema(name="opinion_type", dtype=DataType.VARCHAR,      max_length=50),
        FieldSchema(name="chunk_index",  dtype=DataType.INT32),
        FieldSchema(name="text",         dtype=DataType.VARCHAR,      max_length=65535),
        FieldSchema(name="citation",     dtype=DataType.VARCHAR,      max_length=512),
        FieldSchema(name="vector",       dtype=DataType.FLOAT_VECTOR, dim=VECTOR_DIM),
    ]
    schema = CollectionSchema(fields, description="Legal Opinions RAG")
    col    = Collection(COLLECTION_NAME, schema)
    col.create_index("vector", {
        "metric_type": "COSINE",
        "index_type":  "HNSW",
        "params":      {"M": 16, "efConstruction": 200},
    })
    col.load()
    print(f"[Embed] Collection created and indexed.")
    return col


# ── Chunking (mirrors backend/chunker.py exactly) ─────────────────────────────

def legal_chunker(text: str, chunk_size: int = 3000, overlap: int = 500) -> list[str]:
    text = re.sub(r"\r\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = text.strip()
    if not text:
        return []

    paragraphs = re.split(r"\n\n+", text)
    chunks: list[str] = []
    current_chunk = ""

    for para in paragraphs:
        para = para.strip()
        if not para:
            continue
        if len(para) > chunk_size:
            sentences = re.split(r"(?<=[.!?])\s+(?=[A-Z])", para)
            for sentence in sentences:
                if len(current_chunk) + len(sentence) + 1 <= chunk_size:
                    current_chunk += sentence + " "
                else:
                    if current_chunk.strip():
                        chunks.append(current_chunk.strip())
                    tail = current_chunk[-overlap:] if len(current_chunk) > overlap else current_chunk
                    current_chunk = tail + sentence + " "
        else:
            if len(current_chunk) + len(para) + 2 <= chunk_size:
                current_chunk += para + "\n\n"
            else:
                if current_chunk.strip():
                    chunks.append(current_chunk.strip())
                tail = current_chunk[-overlap:] if len(current_chunk) > overlap else current_chunk
                current_chunk = tail + para + "\n\n"

    if current_chunk.strip():
        chunks.append(current_chunk.strip())

    return chunks


def enrich_chunk(
    chunk: str,
    case_name: str,
    court: str,
    year: int,
    opinion_type: str,
    citation: str = "",
) -> str:
    court_label = {
        "sc":       "SC Supreme Court",
        "scctapp":  "SC Court of Appeals",
        "scotus":   "US Supreme Court",
        "ca4":      "4th Circuit Court of Appeals",
    }.get(court, court)

    parts = [
        f"Source: {case_name}",
        f"Court: {court_label}",
        f"Year: {year}",
        f"Opinion Type: {opinion_type}",
    ]
    if citation:
        parts.append(f"Citation: {citation}")

    return f"[{' | '.join(parts)}]\n\n{chunk}"


def strip_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


# ── Checkpoint helpers ────────────────────────────────────────────────────────

def load_checkpoint() -> str | None:
    """Return last processed opinion_id from a previous run, or None."""
    if CHECKPOINT_IN.exists():
        with open(CHECKPOINT_IN) as f:
            data = json.load(f)
        last_id = data.get("last_opinion_id")
        opinions_done = data.get("opinions_processed", 0)
        chunks_done   = data.get("chunks_upserted", 0)
        print(
            f"[Embed] Resuming from checkpoint: opinion {last_id} "
            f"({opinions_done:,} opinions / {chunks_done:,} chunks already done)"
        )
        return last_id, opinions_done, chunks_done
    return None, 0, 0


def save_checkpoint(last_opinion_id: str, opinions_processed: int, chunks_upserted: int) -> None:
    with open(CHECKPOINT_OUT, "w") as f:
        json.dump({
            "last_opinion_id":    last_opinion_id,
            "opinions_processed": opinions_processed,
            "chunks_upserted":    chunks_upserted,
        }, f)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print(f"[Embed] Courts to embed: {sorted(INGEST_COURTS)}")
    print(f"[Embed] Year range: {START_YEAR or 'all'} – {END_YEAR or 'all'}")

    # ── Load Parquet ──────────────────────────────────────────────────────────
    dfs = []
    for court in sorted(INGEST_COURTS):
        court_path = INPUT_DIR / "opinions" / f"court={court}"
        if not court_path.exists():
            print(f"[Embed] WARNING: no Parquet found for court={court} — skipping")
            continue
        df_court = pd.read_parquet(court_path)
        df_court["court"] = court   # ensure column is set (handles Spark partition col behavior)
        dfs.append(df_court)
        print(f"[Embed] Loaded {len(df_court):,} opinions for court={court}")

    if not dfs:
        print("[Embed] No input data found — exiting.")
        sys.exit(1)

    df = pd.concat(dfs, ignore_index=True)

    # Apply year filter
    if START_YEAR:
        df = df[df["year"] >= START_YEAR]
    if END_YEAR:
        df = df[df["year"] <= END_YEAR]

    # Sort by opinion_id for deterministic ordering and resume support
    df = df.sort_values("opinion_id").reset_index(drop=True)
    print(f"[Embed] {len(df):,} total opinions after year filter")

    # ── Resume from checkpoint ────────────────────────────────────────────────
    last_opinion_id, opinions_processed, chunks_upserted = load_checkpoint()
    if last_opinion_id is not None:
        df = df[df["opinion_id"].astype(str) > str(last_opinion_id)]
        print(f"[Embed] {len(df):,} opinions remaining after resume skip")

    if df.empty:
        print("[Embed] All opinions already processed — nothing to do.")
        save_checkpoint(last_opinion_id or "", opinions_processed, chunks_upserted)
        return

    # ── Setup model + Milvus ──────────────────────────────────────────────────
    print("[Embed] Loading ModernBERT model on GPU…")
    model = SentenceTransformer("answerdotai/ModernBERT-base", device="cuda")
    print("[Embed] Model ready.")

    connect_milvus()
    collection = get_or_create_collection()

    # ── Embed loop ────────────────────────────────────────────────────────────
    since_checkpoint = 0
    current_opinion_id = last_opinion_id or ""

    for _, row in df.iterrows():
        # Extract text
        text = (row.get("plain_text") or "").strip()
        if not text:
            html = (row.get("html_with_citations") or "").strip()
            if html:
                text = strip_html(html)
        if len(text) < MIN_TEXT_LEN:
            continue

        chunks = legal_chunker(text)
        if not chunks:
            continue

        case_name    = str(row.get("case_name", "Unknown"))[:512]
        court        = str(row.get("court", ""))
        year         = int(row.get("year", 0))
        opinion_type = str(row.get("opinion_type", "Lead"))[:50]
        citation     = str(row.get("citation", ""))[:512]
        opinion_id_i = int(row["opinion_id"]) if str(row["opinion_id"]).isdigit() else 0
        cluster_id_i = int(row["cluster_id"]) if str(row["cluster_id"]).isdigit() else 0

        enriched   = [
            enrich_chunk(c, case_name, court, year, opinion_type, citation)
            for c in chunks
        ]
        embeddings = model.encode(enriched, batch_size=EMBED_BATCH, show_progress_bar=False)

        n = len(enriched)
        collection.insert([
            [opinion_id_i]                   * n,   # opinion_id
            [cluster_id_i]                   * n,   # cluster_id
            [case_name]                      * n,   # case_name
            [court]                          * n,   # court
            [year]                           * n,   # year
            [opinion_type]                   * n,   # opinion_type
            list(range(n)),                          # chunk_index
            [c[:65535] for c in enriched],           # text
            [citation]                       * n,   # citation
            embeddings.tolist(),                     # vector
        ])

        current_opinion_id  = str(row["opinion_id"])
        opinions_processed += 1
        chunks_upserted    += n
        since_checkpoint   += 1

        if opinions_processed % 100 == 0:
            print(
                f"[Embed] {opinions_processed:,} opinions / "
                f"{chunks_upserted:,} chunks — court={court} year={year}"
            )

        if since_checkpoint >= CHECKPOINT_EVERY:
            save_checkpoint(current_opinion_id, opinions_processed, chunks_upserted)
            since_checkpoint = 0
            print(f"[Embed] Checkpoint saved at opinion {current_opinion_id}")

    # Final checkpoint
    save_checkpoint(current_opinion_id, opinions_processed, chunks_upserted)
    print(
        f"[Embed] Complete — {opinions_processed:,} opinions / "
        f"{chunks_upserted:,} chunks upserted to Milvus"
    )


if __name__ == "__main__":
    main()
