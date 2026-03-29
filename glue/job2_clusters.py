"""
job2_clusters.py — Glue ETL Job 2: Build enriched clusters Parquet.

Reads:
  s3://com-courtlistener-storage/bulk-data/opinion-clusters-<date>.csv.bz2  (public)
  s3://com-courtlistener-storage/bulk-data/citations-<date>.csv.bz2         (public)
  s3://<output_bucket>/freelaw/dockets/dockets.parquet                       (Job 1 output)

Writes: s3://<output_bucket>/freelaw/clusters/clusters.parquet

Output schema:
  cluster_id  STRING  — CourtListener opinion cluster ID
  case_name   STRING  — human-readable case name
  court       STRING  — court identifier
  year        INT     — year filed
  citation    STRING  — "272 S.C. 120, 56 S.E.2d 4" etc. (may be empty)

This replaces Phases 1, 2, and 2.5 from the old EC2 pipeline.

Runtime: ~30 min   Cost: ~$1.00 (4x G.1X workers)
"""

import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

from glue_common import find_latest_cl_key, extract_date

# ── Init ──────────────────────────────────────────────────────────────────────

args = getResolvedOptions(sys.argv, ["JOB_NAME", "output_bucket"])

sc = SparkContext()
sc._jsc.hadoopConfiguration().set(
    "fs.s3a.bucket.com-courtlistener-storage.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
)

glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

output_bucket = args["output_bucket"]

# ── Load dockets (Job 1 output) ───────────────────────────────────────────────

dockets_path = f"s3://{output_bucket}/freelaw/dockets/"
print(f"[Job2] Loading dockets from {dockets_path}")

dockets_df = spark.read.parquet(dockets_path)
# dockets_df: docket_id, court_id — small enough to broadcast
dockets_bc = F.broadcast(dockets_df)

# ── Read & join opinion clusters ──────────────────────────────────────────────

clusters_source = find_latest_cl_key("opinion-clusters")
print(f"[Job2] Reading clusters: {clusters_source}")

clusters_raw = (
    spark.read
    .option("header",    "true")
    .option("multiLine", "true")
    .option("escape",    '"')
    .option("mode",      "DROPMALFORMED")
    .csv(clusters_source)
    .repartition(200)
)

# Extract year from date_filed (YYYY-MM-DD or YYYY-...)
clusters_raw = clusters_raw.withColumn(
    "year",
    F.when(
        F.col("date_filed").isNotNull() & (F.length(F.col("date_filed")) >= 4),
        F.col("date_filed").substr(1, 4).cast(IntegerType()),
    ).otherwise(F.lit(0))
)

# Pick best available case name field
clusters_raw = clusters_raw.withColumn(
    "case_name",
    F.coalesce(
        F.when(F.trim(F.col("case_name"))       != "", F.trim(F.col("case_name"))),
        F.when(F.trim(F.col("case_name_short"))  != "", F.trim(F.col("case_name_short"))),
        F.when(F.trim(F.col("case_name_full"))   != "", F.trim(F.col("case_name_full"))),
        F.lit("Unknown"),
    )
)

# Join with dockets to get court_id; inner join drops non-DB_COURTS clusters
clusters_joined = (
    clusters_raw
    .join(dockets_bc, clusters_raw["docket_id"] == dockets_df["docket_id"], "inner")
    .select(
        clusters_raw["id"].alias("cluster_id"),
        F.col("case_name"),
        F.col("court_id").alias("court"),
        F.col("year"),
    )
)

print(f"[Job2] Clusters after dockets join: {clusters_joined.count():,}")

# ── Read & aggregate citations ────────────────────────────────────────────────

citations_source = find_latest_cl_key("citations")
print(f"[Job2] Reading citations: {citations_source}")

citations_raw = (
    spark.read
    .option("header",    "true")
    .option("multiLine", "true")
    .option("escape",    '"')
    .option("mode",      "DROPMALFORMED")
    .csv(citations_source)
    .repartition(100)
)

# Build "vol reporter page" string per row, then aggregate into comma-separated list per cluster
citations_agg = (
    citations_raw
    .filter(
        F.col("volume").isNotNull()   & (F.trim(F.col("volume"))   != "") &
        F.col("reporter").isNotNull() & (F.trim(F.col("reporter")) != "") &
        F.col("page").isNotNull()     & (F.trim(F.col("page"))     != "")
    )
    .withColumn(
        "cite_str",
        F.concat_ws(" ", F.trim(F.col("volume")), F.trim(F.col("reporter")), F.trim(F.col("page")))
    )
    .groupBy("cluster_id")
    .agg(F.concat_ws(", ", F.collect_list("cite_str")).alias("citation"))
)

# ── Join clusters + citations ─────────────────────────────────────────────────

enriched = (
    clusters_joined
    .join(F.broadcast(citations_agg), "cluster_id", "left")
    .withColumn("citation", F.coalesce(F.col("citation"), F.lit("")))
    .select("cluster_id", "case_name", "court", "year", "citation")
)

# ── Write ─────────────────────────────────────────────────────────────────────

output_path = f"s3://{output_bucket}/freelaw/clusters/"

(
    enriched
    .coalesce(8)
    .write
    .mode("overwrite")
    .parquet(output_path)
)

count = enriched.count()
print(f"[Job2] Done: {count:,} clusters written → {output_path}")

job.commit()
