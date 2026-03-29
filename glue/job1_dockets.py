"""
job1_dockets.py — Glue ETL Job 1: Extract DB_COURTS dockets from CourtListener S3.

Reads:  s3://com-courtlistener-storage/bulk-data/dockets-<date>.csv.bz2  (public)
Writes: s3://<output_bucket>/freelaw/dockets/dockets.parquet

Output schema:
  docket_id  STRING  — CourtListener docket ID
  court_id   STRING  — court identifier (e.g. "sc", "scotus", "ca4")

Runtime: ~15 min   Cost: ~$0.50 (2x G.1X workers)
"""

import sys
import boto3
from botocore import UNSIGNED
from botocore.config import Config

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
import pyspark.sql.functions as F

from glue_common import DB_COURTS, find_latest_cl_key, extract_date

# ── Init ──────────────────────────────────────────────────────────────────────

args = getResolvedOptions(sys.argv, ["JOB_NAME", "output_bucket"])

sc = SparkContext()

# Allow anonymous (unsigned) reads from the public CourtListener bucket
# while keeping normal IAM-signed writes to our own bucket.
sc._jsc.hadoopConfiguration().set(
    "fs.s3a.bucket.com-courtlistener-storage.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
)

glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

output_bucket = args["output_bucket"]

# ── Find latest dockets file ──────────────────────────────────────────────────

source_path = find_latest_cl_key("dockets")
snapshot_date = extract_date(source_path)
print(f"[Job1] Source: {source_path}  (snapshot: {snapshot_date})")
print(f"[Job1] Filtering for {len(DB_COURTS)} courts")

# ── Read & filter ─────────────────────────────────────────────────────────────

# bz2 is not splittable — entire file read by one task initially.
# Repartition after read to parallelize downstream processing.
df = (
    spark.read
    .option("header",    "true")
    .option("multiLine", "true")   # handle quoted newlines in CSV fields
    .option("escape",    '"')
    .option("mode",      "DROPMALFORMED")
    .csv(source_path)
)

df = df.repartition(200)

df_filtered = (
    df
    .filter(F.col("court_id").isin(list(DB_COURTS)))
    .select(
        F.col("id").alias("docket_id"),
        F.col("court_id"),
    )
    .dropna(subset=["docket_id", "court_id"])
)

# ── Write ─────────────────────────────────────────────────────────────────────

output_path = f"s3://{output_bucket}/freelaw/dockets/"

(
    df_filtered
    .coalesce(4)   # small output — few files is fine
    .write
    .mode("overwrite")
    .parquet(output_path)
)

count = df_filtered.count()
print(f"[Job1] Done: {count:,} dockets written → {output_path}")
print(f"[Job1] Snapshot date: {snapshot_date}")

# Store snapshot date in a marker file so Job2/3 can confirm they use the same snapshot
sc._jsc.hadoopConfiguration()   # no-op, but boto3 write below does the marker
marker = f"s3://{output_bucket}/freelaw/_snapshot_date.txt"
(
    spark.createDataFrame([(snapshot_date,)], ["snapshot_date"])
    .coalesce(1)
    .write
    .mode("overwrite")
    .text(marker.replace("/freelaw/_snapshot_date.txt", "/freelaw/"))
)

# Use boto3 to write a simple text marker (cleaner than Spark for a single value)
boto3.client("s3").put_object(
    Bucket=output_bucket,
    Key="freelaw/_snapshot_date.txt",
    Body=snapshot_date.encode(),
)

job.commit()
