"""
job3_opinions.py — Glue ETL Job 3: Build denormalized opinions Parquet.

Reads:
  s3://com-courtlistener-storage/bulk-data/opinions-<date>.csv.bz2  (public, ~50 GB)
  s3://<output_bucket>/freelaw/clusters/clusters.parquet             (Job 2 output)

Writes: s3://<output_bucket>/freelaw/opinions/court={court}/*.parquet

Output schema (denormalized — everything SageMaker needs to embed a chunk):
  opinion_id            STRING  — CourtListener opinion ID
  cluster_id            STRING
  case_name             STRING
  court                 STRING
  year                  INT
  citation              STRING  — "272 S.C. 120" etc.
  opinion_type          STRING  — Lead / Concurrence / Dissent etc.
  plain_text            STRING  — raw plain text (may be empty if HTML-only)
  html_with_citations   STRING  — raw HTML (used when plain_text is empty)

Text filter: rows where effective text length < 150 chars are dropped.
Partitioned by court so the embedding job reads only the courts it needs
(e.g. court=sc/ + court=scctapp/) without scanning everything.

NOTE: The opinions bz2 file (~50 GB compressed, ~200 GB decompressed) is a
single file — Spark reads it with one task initially (bz2 is not splittable).
After read, data is repartitioned across workers for the join + filter + write.
This is the dominant runtime cost of the pipeline.

Runtime: ~3–4 hrs   Cost: ~$4.00 (8x G.1X workers)
"""

import sys
import re

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from glue_common import find_latest_cl_key, MIN_TEXT_LEN

# ── Init ──────────────────────────────────────────────────────────────────────

args = getResolvedOptions(sys.argv, ["JOB_NAME", "output_bucket"])

sc = SparkContext()
sc._jsc.hadoopConfiguration().set(
    "fs.s3a.bucket.com-courtlistener-storage.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
)

# Increase max result size to handle the large CSV fields
sc._conf.set("spark.driver.maxResultSize", "4g")

glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

output_bucket = args["output_bucket"]

# ── HTML strip UDF ────────────────────────────────────────────────────────────

_TAG_RE   = re.compile(r"<[^>]+>")
_SPACE_RE = re.compile(r"\s+")

def _strip_html(text):
    if not text:
        return ""
    text = _TAG_RE.sub(" ", text)
    return _SPACE_RE.sub(" ", text).strip()

strip_html_udf = F.udf(_strip_html, StringType())

# ── Load clusters (Job 2 output) ──────────────────────────────────────────────

clusters_path = f"s3://{output_bucket}/freelaw/clusters/"
print(f"[Job3] Loading clusters from {clusters_path}")

# clusters.parquet is small (~50–200 MB) — safe to broadcast for the join
clusters_df = F.broadcast(spark.read.parquet(clusters_path))
cluster_count = clusters_df.count()
print(f"[Job3] {cluster_count:,} clusters loaded (broadcast join)")

# ── Read opinions ─────────────────────────────────────────────────────────────

opinions_source = find_latest_cl_key("opinions")
print(f"[Job3] Reading opinions: {opinions_source}")
print(f"[Job3] NOTE: single bz2 file — initial read is single-threaded, then parallelises")

opinions_raw = (
    spark.read
    .option("header",          "true")
    .option("multiLine",       "true")
    .option("escape",          '"')
    .option("mode",            "DROPMALFORMED")
    .option("maxCharsPerColumn", str(10 * 1024 * 1024))  # 10 MB field limit
    .csv(opinions_source)
)

# Repartition early to distribute across workers for join + filter
opinions_raw = opinions_raw.repartition(400)

# ── Join with clusters ────────────────────────────────────────────────────────

# Inner join drops opinions whose cluster_id is not in DB_COURTS
opinions_joined = (
    opinions_raw
    .join(clusters_df, opinions_raw["cluster_id"] == clusters_df["cluster_id"], "inner")
    .select(
        opinions_raw["id"].alias("opinion_id"),
        opinions_raw["cluster_id"],
        clusters_df["case_name"],
        clusters_df["court"],
        clusters_df["year"],
        clusters_df["citation"],
        F.coalesce(F.trim(opinions_raw["type"]), F.lit("Lead")).alias("opinion_type"),
        F.coalesce(opinions_raw["plain_text"],            F.lit("")).alias("plain_text"),
        F.coalesce(opinions_raw["html_with_citations"],   F.lit("")).alias("html_with_citations"),
    )
)

# ── Text-bearing filter ───────────────────────────────────────────────────────

# Compute effective text length: prefer plain_text, fall back to stripped HTML
opinions_with_text = opinions_joined.withColumn(
    "_effective_text",
    F.when(
        F.length(F.trim(F.col("plain_text"))) >= MIN_TEXT_LEN,
        F.trim(F.col("plain_text")),
    ).otherwise(
        strip_html_udf(F.col("html_with_citations"))
    )
)

opinions_filtered = (
    opinions_with_text
    .filter(F.length(F.col("_effective_text")) >= MIN_TEXT_LEN)
    .drop("_effective_text")
)

# ── Write partitioned by court ────────────────────────────────────────────────

output_path = f"s3://{output_bucket}/freelaw/opinions/"

(
    opinions_filtered
    .repartition(F.col("court"))   # one output partition group per court
    .write
    .partitionBy("court")
    .mode("overwrite")
    .parquet(output_path)
)

print(f"[Job3] Done — opinions written to {output_path}")
print(f"[Job3] Partitioned by court. Embedding job reads court=sc/ + court=scctapp/ only.")

job.commit()
