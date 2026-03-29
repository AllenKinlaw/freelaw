"""
run_job.py — Submit the FreeLaw SageMaker Processing embedding job.

Run from your local machine (or EC2) after the Glue ETL pipeline has completed
and opinions Parquet is available in S3.

Prerequisites:
  pip install sagemaker boto3

Required environment variables:
  MILVUS_URI            — Zilliz Cloud URI
  MILVUS_TOKEN          — Zilliz Cloud API token

Optional environment variables:
  INGEST_COURTS         — comma-separated court IDs  (default: sc,scctapp)
  START_YEAR            — lower year bound            (default: none = all years)
  END_YEAR              — upper year bound            (default: none = all years)
  AWS_DEFAULT_REGION    — AWS region                  (default: us-east-1)
  SAGEMAKER_ROLE_ARN    — override auto-detected role

Usage:
  # Source your Milvus credentials first
  export MILVUS_URI="https://..."
  export MILVUS_TOKEN="..."

  # Run SC courts, all years
  python sagemaker/run_job.py

  # Run SC courts, 2020–2025 only
  START_YEAR=2020 END_YEAR=2025 python sagemaker/run_job.py

  # Check status of a running job
  python sagemaker/run_job.py --status <job-name>

  # Create the IAM role (one-time setup)
  python sagemaker/run_job.py --setup
"""

import argparse
import json
import os
import sys

import boto3
import sagemaker
from sagemaker.huggingface import HuggingFaceProcessor
from sagemaker.processing import ProcessingInput, ProcessingOutput

# ── Config ────────────────────────────────────────────────────────────────────

REGION        = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
ACCOUNT_ID    = boto3.client("sts", region_name=REGION).get_caller_identity()["Account"]
BUCKET        = f"freelaw-data-{ACCOUNT_ID}"
ROLE_NAME     = "freelaw-sagemaker-role"
INGEST_COURTS = os.environ.get("INGEST_COURTS", "sc,scctapp")
START_YEAR    = os.environ.get("START_YEAR", "")
END_YEAR      = os.environ.get("END_YEAR", "")

# HuggingFace PyTorch GPU base image (pre-installed: torch, transformers, sentence-transformers)
HF_PYTORCH_VERSION      = "2.1.0"
HF_TRANSFORMERS_VERSION = "4.37.0"
HF_PY_VERSION           = "py310"

# ml.g4dn.xlarge: 4 vCPU, 16 GB RAM, 1x T4 GPU (16 GB VRAM) — $0.736/hr
INSTANCE_TYPE = "ml.g4dn.xlarge"


# ── IAM role setup (one-time) ─────────────────────────────────────────────────

def create_sagemaker_role() -> str:
    """Create the freelaw-sagemaker-role if it doesn't exist. Returns role ARN."""
    iam = boto3.client("iam", region_name=REGION)

    trust = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "sagemaker.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }],
    })

    try:
        role = iam.get_role(RoleName=ROLE_NAME)
        arn  = role["Role"]["Arn"]
        print(f"[Setup] Role already exists: {arn}")
    except iam.exceptions.NoSuchEntityException:
        role = iam.create_role(
            RoleName=ROLE_NAME,
            AssumeRolePolicyDocument=trust,
        )
        arn = role["Role"]["Arn"]

        # Managed policies
        for policy in [
            "arn:aws:iam::aws:policy/AmazonSageMakerFullAccess",
            "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess",
        ]:
            iam.attach_role_policy(RoleName=ROLE_NAME, PolicyArn=policy)

        print(f"[Setup] Created role: {arn}")

    # Inline S3 policy for our bucket
    s3_policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": [
                "s3:GetObject", "s3:PutObject",
                "s3:DeleteObject", "s3:ListBucket",
            ],
            "Resource": [
                f"arn:aws:s3:::{BUCKET}",
                f"arn:aws:s3:::{BUCKET}/*",
            ],
        }],
    })
    iam.put_role_policy(
        RoleName=ROLE_NAME,
        PolicyName="freelaw-s3-access",
        PolicyDocument=s3_policy,
    )
    print(f"[Setup] S3 inline policy attached to {ROLE_NAME}")
    return arn


def get_role_arn() -> str:
    if os.environ.get("SAGEMAKER_ROLE_ARN"):
        return os.environ["SAGEMAKER_ROLE_ARN"]
    iam = boto3.client("iam", region_name=REGION)
    try:
        return iam.get_role(RoleName=ROLE_NAME)["Role"]["Arn"]
    except iam.exceptions.NoSuchEntityException:
        print(f"[Error] Role '{ROLE_NAME}' not found. Run: python run_job.py --setup")
        sys.exit(1)


# ── Submit job ────────────────────────────────────────────────────────────────

def submit_job() -> None:
    # Validate required env vars
    milvus_uri   = os.environ.get("MILVUS_URI")
    milvus_token = os.environ.get("MILVUS_TOKEN")
    if not milvus_uri or not milvus_token:
        print("[Error] MILVUS_URI and MILVUS_TOKEN environment variables are required.")
        sys.exit(1)

    role_arn = get_role_arn()
    session  = sagemaker.Session(boto_session=boto3.Session(region_name=REGION))

    print(f"[Job] Account  : {ACCOUNT_ID}")
    print(f"[Job] Bucket   : {BUCKET}")
    print(f"[Job] Courts   : {INGEST_COURTS}")
    print(f"[Job] Years    : {START_YEAR or 'all'} – {END_YEAR or 'all'}")
    print(f"[Job] Instance : {INSTANCE_TYPE}")
    print(f"[Job] Role     : {role_arn}")

    # ── S3 paths ──────────────────────────────────────────────────────────────
    opinions_base  = f"s3://{BUCKET}/freelaw/opinions"
    checkpoint_s3  = f"s3://{BUCKET}/freelaw/checkpoints"

    # One ProcessingInput per court — downloads only the courts we're embedding
    courts = [c.strip() for c in INGEST_COURTS.split(",") if c.strip()]
    inputs = [
        ProcessingInput(
            source=f"{opinions_base}/court={court}/",
            destination=f"/opt/ml/processing/input/opinions/court={court}/",
            input_name=f"opinions_{court}",
            s3_data_type="S3Prefix",
            s3_input_mode="File",
        )
        for court in courts
    ]

    # Checkpoint input — previous run's output (empty prefix OK on first run)
    inputs.append(ProcessingInput(
        source=f"{checkpoint_s3}/",
        destination="/opt/ml/processing/input/checkpoint/",
        input_name="checkpoint",
        s3_data_type="S3Prefix",
        s3_input_mode="File",
    ))

    # Checkpoint output — written back to S3 for next run to resume from
    outputs = [
        ProcessingOutput(
            source="/opt/ml/processing/output/",
            destination=f"{checkpoint_s3}/",
            output_name="checkpoint",
        )
    ]

    # ── Environment variables passed into the container ───────────────────────
    environment = {
        "MILVUS_URI":     milvus_uri,
        "MILVUS_TOKEN":   milvus_token,
        "INGEST_COURTS":  INGEST_COURTS,
    }
    if START_YEAR:
        environment["START_YEAR"] = START_YEAR
    if END_YEAR:
        environment["END_YEAR"] = END_YEAR

    # ── Processor ─────────────────────────────────────────────────────────────
    processor = HuggingFaceProcessor(
        role=role_arn,
        instance_type=INSTANCE_TYPE,
        instance_count=1,
        transformers_version=HF_TRANSFORMERS_VERSION,
        pytorch_version=HF_PYTORCH_VERSION,
        py_version=HF_PY_VERSION,
        base_job_name="freelaw-embed",
        sagemaker_session=session,
        env=environment,
    )

    # ── Submit ────────────────────────────────────────────────────────────────
    processor.run(
        code="embed_script.py",
        source_dir=str(os.path.dirname(os.path.abspath(__file__))),
        inputs=inputs,
        outputs=outputs,
        wait=False,   # return immediately; monitor via --status or AWS console
        logs=False,
    )

    job_name = processor.latest_job_name
    print(f"\n[Job] Submitted: {job_name}")
    print(f"[Job] Monitor  : python sagemaker/run_job.py --status {job_name}")
    print(f"[Job] Logs     : CloudWatch → /aws/sagemaker/ProcessingJobs → {job_name}")


# ── Status check ──────────────────────────────────────────────────────────────

def check_status(job_name: str) -> None:
    sm = boto3.client("sagemaker", region_name=REGION)
    resp = sm.describe_processing_job(ProcessingJobName=job_name)

    status      = resp["ProcessingJobStatus"]
    start       = resp.get("ProcessingStartTime", "—")
    end         = resp.get("ProcessingEndTime", "—")
    failure_msg = resp.get("FailureReason", "")

    print(f"Job    : {job_name}")
    print(f"Status : {status}")
    print(f"Start  : {start}")
    print(f"End    : {end}")
    if failure_msg:
        print(f"Error  : {failure_msg}")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FreeLaw SageMaker embedding job")
    parser.add_argument("--setup",  action="store_true", help="Create IAM role (one-time)")
    parser.add_argument("--status", metavar="JOB_NAME",  help="Check status of a job")
    args = parser.parse_args()

    if args.setup:
        create_sagemaker_role()
    elif args.status:
        check_status(args.status)
    else:
        submit_job()
