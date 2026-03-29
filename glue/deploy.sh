#!/usr/bin/env bash
# deploy.sh — Create AWS infrastructure for the FreeLaw Glue ETL pipeline.
#
# What this creates:
#   S3 bucket            freelaw-data-<account-id>   (Parquet lake + Glue scripts)
#   IAM role             freelaw-glue-role
#   Glue jobs            freelaw-etl-dockets/clusters/opinions
#   Step Functions SM    freelaw-etl-pipeline
#   EventBridge rule     freelaw-etl-monthly  (1st of each month, 02:00 UTC)
#
# Prerequisites:
#   aws CLI configured with a profile that has Admin or sufficient permissions
#   All glue/*.py files present in the same directory as this script
#
# Usage:
#   chmod +x deploy.sh
#   ./deploy.sh
#   ./deploy.sh --destroy    # tear everything down

set -euo pipefail

# ── Config — edit these ───────────────────────────────────────────────────────

REGION="${AWS_DEFAULT_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
BUCKET="freelaw-data-${ACCOUNT_ID}"
GLUE_ROLE="freelaw-glue-role"
SF_ROLE="freelaw-sf-role"
SF_NAME="freelaw-etl-pipeline"
EB_RULE="freelaw-etl-monthly"
GLUE_SCRIPTS_PREFIX="glue-scripts"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== FreeLaw Glue ETL Deploy ==="
echo "Account : $ACCOUNT_ID"
echo "Region  : $REGION"
echo "Bucket  : $BUCKET"

# ── Destroy mode ──────────────────────────────────────────────────────────────

if [[ "${1:-}" == "--destroy" ]]; then
  echo ""
  echo "=== DESTROYING all FreeLaw ETL infrastructure ==="
  read -p "Type 'yes' to confirm: " CONFIRM
  [[ "$CONFIRM" == "yes" ]] || { echo "Aborted."; exit 1; }

  aws events remove-targets --rule "$EB_RULE" --ids "1" --region "$REGION" 2>/dev/null || true
  aws events delete-rule --name "$EB_RULE" --region "$REGION" 2>/dev/null || true
  aws stepfunctions delete-state-machine \
    --state-machine-arn "arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:${SF_NAME}" 2>/dev/null || true
  for job in freelaw-etl-dockets freelaw-etl-clusters freelaw-etl-opinions; do
    aws glue delete-job --job-name "$job" --region "$REGION" 2>/dev/null || true
  done
  aws iam detach-role-policy --role-name "$GLUE_ROLE" \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole 2>/dev/null || true
  aws iam delete-role-policy --role-name "$GLUE_ROLE" --policy-name freelaw-s3-access 2>/dev/null || true
  aws iam delete-role --role-name "$GLUE_ROLE" 2>/dev/null || true
  aws iam detach-role-policy --role-name "$SF_ROLE" \
    --policy-arn arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess 2>/dev/null || true
  aws iam delete-role --role-name "$SF_ROLE" 2>/dev/null || true
  echo "Done. S3 bucket '$BUCKET' NOT deleted (contains your Parquet data)."
  echo "Delete manually if needed: aws s3 rb s3://$BUCKET --force"
  exit 0
fi

# ── 1. S3 bucket ──────────────────────────────────────────────────────────────

echo ""
echo "--- 1/6  S3 bucket"

if aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  echo "  Bucket $BUCKET already exists."
else
  if [[ "$REGION" == "us-east-1" ]]; then
    aws s3api create-bucket --bucket "$BUCKET" --region "$REGION"
  else
    aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
      --create-bucket-configuration LocationConstraint="$REGION"
  fi
  # Block all public access
  aws s3api put-public-access-block --bucket "$BUCKET" \
    --public-access-block-configuration \
      "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
  # Enable versioning for safety
  aws s3api put-bucket-versioning --bucket "$BUCKET" \
    --versioning-configuration Status=Enabled
  echo "  Created s3://$BUCKET"
fi

# ── 2. Upload Glue scripts ────────────────────────────────────────────────────

echo ""
echo "--- 2/6  Uploading Glue scripts to s3://$BUCKET/$GLUE_SCRIPTS_PREFIX/"

for f in glue_common.py job1_dockets.py job2_clusters.py job3_opinions.py; do
  aws s3 cp "$SCRIPT_DIR/$f" "s3://$BUCKET/$GLUE_SCRIPTS_PREFIX/$f"
  echo "  Uploaded $f"
done

# ── 3. IAM role for Glue ──────────────────────────────────────────────────────

echo ""
echo "--- 3/6  IAM role $GLUE_ROLE"

TRUST_GLUE='{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "glue.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}'

if aws iam get-role --role-name "$GLUE_ROLE" 2>/dev/null; then
  echo "  Role already exists."
else
  aws iam create-role \
    --role-name "$GLUE_ROLE" \
    --assume-role-policy-document "$TRUST_GLUE"
  aws iam attach-role-policy \
    --role-name "$GLUE_ROLE" \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
  echo "  Created $GLUE_ROLE with AWSGlueServiceRole"
fi

# Inline policy: read/write our S3 bucket + read public CourtListener bucket
S3_POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::${BUCKET}",
        "arn:aws:s3:::${BUCKET}/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject","s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::com-courtlistener-storage",
        "arn:aws:s3:::com-courtlistener-storage/*"
      ]
    }
  ]
}
EOF
)

aws iam put-role-policy \
  --role-name "$GLUE_ROLE" \
  --policy-name "freelaw-s3-access" \
  --policy-document "$S3_POLICY"
echo "  S3 inline policy attached."

GLUE_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${GLUE_ROLE}"

# ── 4. Glue jobs ──────────────────────────────────────────────────────────────

echo ""
echo "--- 4/6  Glue jobs"

COMMON_ARGS=(
  --role               "$GLUE_ROLE_ARN"
  --glue-version       "4.0"
  --language           "python"
  --default-arguments  "{\"--extra-py-files\":\"s3://${BUCKET}/${GLUE_SCRIPTS_PREFIX}/glue_common.py\",\"--output_bucket\":\"${BUCKET}\"}"
  --region             "$REGION"
)

create_or_update_job() {
  local NAME=$1; local SCRIPT=$2; local WORKERS=$3; local TYPE=$4
  local SCRIPT_PATH="s3://${BUCKET}/${GLUE_SCRIPTS_PREFIX}/${SCRIPT}"

  if aws glue get-job --job-name "$NAME" --region "$REGION" 2>/dev/null; then
    aws glue update-job --job-name "$NAME" --region "$REGION" \
      --job-update "Role=${GLUE_ROLE_ARN},Command={Name=glueetl,ScriptLocation=${SCRIPT_PATH},PythonVersion=3},WorkerType=${TYPE},NumberOfWorkers=${WORKERS},GlueVersion=4.0,DefaultArguments={\"--extra-py-files\":\"s3://${BUCKET}/${GLUE_SCRIPTS_PREFIX}/glue_common.py\",\"--output_bucket\":\"${BUCKET}\"}" \
      > /dev/null
    echo "  Updated $NAME"
  else
    aws glue create-job --name "$NAME" --region "$REGION" \
      --role "$GLUE_ROLE_ARN" \
      --command "Name=glueetl,ScriptLocation=${SCRIPT_PATH},PythonVersion=3" \
      --glue-version "4.0" \
      --worker-type "$TYPE" \
      --number-of-workers "$WORKERS" \
      --default-arguments "{\"--extra-py-files\":\"s3://${BUCKET}/${GLUE_SCRIPTS_PREFIX}/glue_common.py\",\"--output_bucket\":\"${BUCKET}\"}" \
      > /dev/null
    echo "  Created $NAME ($WORKERS x $TYPE workers)"
  fi
}

create_or_update_job "freelaw-etl-dockets"  "job1_dockets.py"  2 "G.1X"
create_or_update_job "freelaw-etl-clusters" "job2_clusters.py" 4 "G.1X"
create_or_update_job "freelaw-etl-opinions" "job3_opinions.py" 8 "G.1X"

# ── 5. Step Functions ─────────────────────────────────────────────────────────

echo ""
echo "--- 5/6  Step Functions state machine"

# IAM role for Step Functions to invoke Glue
TRUST_SF='{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "states.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}'

if ! aws iam get-role --role-name "$SF_ROLE" 2>/dev/null; then
  aws iam create-role --role-name "$SF_ROLE" \
    --assume-role-policy-document "$TRUST_SF"
  aws iam attach-role-policy --role-name "$SF_ROLE" \
    --policy-arn arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess
  echo "  Created $SF_ROLE"
fi

SF_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${SF_ROLE}"
SF_ARN="arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:${SF_NAME}"

SF_DEF=$(cat "$SCRIPT_DIR/stepfunctions.json")

if aws stepfunctions describe-state-machine --state-machine-arn "$SF_ARN" 2>/dev/null; then
  aws stepfunctions update-state-machine \
    --state-machine-arn "$SF_ARN" \
    --definition "$SF_DEF" \
    --role-arn   "$SF_ROLE_ARN" > /dev/null
  echo "  Updated $SF_NAME"
else
  aws stepfunctions create-state-machine \
    --name       "$SF_NAME" \
    --definition "$SF_DEF" \
    --role-arn   "$SF_ROLE_ARN" \
    --type       STANDARD > /dev/null
  echo "  Created $SF_NAME"
fi

# ── 6. EventBridge monthly trigger ───────────────────────────────────────────

echo ""
echo "--- 6/6  EventBridge monthly trigger (1st of month, 02:00 UTC)"

# IAM policy for EventBridge to start Step Functions
EB_POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": "states:StartExecution",
    "Resource": "${SF_ARN}"
  }]
}
EOF
)

# Re-use SF role or create a dedicated EB role — here we extend the SF role
aws iam put-role-policy \
  --role-name "$SF_ROLE" \
  --policy-name "freelaw-eb-sfn" \
  --policy-document "$EB_POLICY"

# EventBridge rule: cron(min hr dom mon dow yr) — 2:00 UTC on 1st of every month
aws events put-rule \
  --name           "$EB_RULE" \
  --schedule-expression "cron(0 2 1 * ? *)" \
  --state          ENABLED \
  --region         "$REGION" > /dev/null

# Target: start the Step Functions execution with our bucket as input
aws events put-targets \
  --rule  "$EB_RULE" \
  --region "$REGION" \
  --targets "[{
    \"Id\": \"1\",
    \"Arn\": \"${SF_ARN}\",
    \"RoleArn\": \"${SF_ROLE_ARN}\",
    \"Input\": \"{\\\"output_bucket\\\":\\\"${BUCKET}\\\"}\"
  }]" > /dev/null

echo "  EventBridge rule $EB_RULE created."

# ── Summary ───────────────────────────────────────────────────────────────────

echo ""
echo "=== Deploy complete ==="
echo ""
echo "Resources created:"
echo "  S3 bucket   : s3://$BUCKET"
echo "  Glue scripts: s3://$BUCKET/$GLUE_SCRIPTS_PREFIX/"
echo "  Glue jobs   : freelaw-etl-dockets, freelaw-etl-clusters, freelaw-etl-opinions"
echo "  Step Fn     : $SF_ARN"
echo "  EventBridge : $EB_RULE (monthly, 1st @ 02:00 UTC)"
echo ""
echo "To run the pipeline manually:"
echo "  aws stepfunctions start-execution \\"
echo "    --state-machine-arn $SF_ARN \\"
echo "    --input '{\"output_bucket\":\"$BUCKET\"}'"
echo ""
echo "Parquet output will appear at:"
echo "  s3://$BUCKET/freelaw/dockets/"
echo "  s3://$BUCKET/freelaw/clusters/"
echo "  s3://$BUCKET/freelaw/opinions/court=sc/"
echo "  s3://$BUCKET/freelaw/opinions/court=scotus/"
echo "  ... (one prefix per court)"
