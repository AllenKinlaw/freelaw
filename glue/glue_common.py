"""
glue_common.py — Shared constants and helpers for FreeLaw Glue ETL jobs.

Uploaded to S3 and passed to each job via --extra-py-files so all three
jobs share the same DB_COURTS definition and utility functions.
"""

import re

# ── Court sets ────────────────────────────────────────────────────────────────

# All courts whose opinions are stored in the Parquet lake.
DB_COURTS = {
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
}

# Courts to embed into Milvus/vector store (SC only for now).
# Expand this set when you're ready to add more courts without re-running ETL.
INGEST_COURTS = {"sc", "scctapp"}

# Minimum character length to consider an opinion text-bearing.
# Filters out ~1.4B PACER stub rows that have no opinion text.
MIN_TEXT_LEN = 150

# Public CourtListener S3 bucket
CL_BUCKET = "com-courtlistener-storage"
CL_PREFIX = "bulk-data"


# ── S3 helpers ────────────────────────────────────────────────────────────────

def find_latest_cl_key(file_prefix: str) -> str:
    """Return the full S3 path (s3a://) of the latest bz2 CSV for file_prefix."""
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config

    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    resp = s3.list_objects_v2(Bucket=CL_BUCKET, Prefix=f"{CL_PREFIX}/{file_prefix}")
    keys = [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".csv.bz2")]
    if not keys:
        raise FileNotFoundError(f"No bz2 files found matching '{file_prefix}'")
    latest = sorted(keys)[-1]
    return f"s3a://{CL_BUCKET}/{latest}"


def extract_date(s3_path: str) -> str:
    """Extract YYYY-MM-DD from an S3 path."""
    m = re.search(r"(\d{4}-\d{2}-\d{2})", s3_path)
    return m.group(1) if m else "unknown"


# ── Text helpers ──────────────────────────────────────────────────────────────

def strip_html(text: str) -> str:
    """Strip HTML tags and collapse whitespace."""
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()
