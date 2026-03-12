#!/usr/bin/env bash
# =============================================================================
# scripts/deploy_cloud_run_job.sh — F6 + F7: Cloud Run Job + Cloud Scheduler
# =============================================================================
# Creates/updates the Cloud Run Job and wires up a Cloud Scheduler trigger.
# Run this AFTER: setup_iam.sh, create_secrets.sh, build_and_push.sh
#
# Usage:
#   bash scripts/deploy_cloud_run_job.sh
#
# Prerequisites:
#   gcloud auth login && gcloud config set project skillpulse-india
# =============================================================================
set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:-skillpulse-india}"
REGION="asia-south1"
REPO="skillpulse-repo"
JOB_NAME="skillpulse-pipeline-job"
SCHEDULER_JOB_NAME="skillpulse-daily-trigger"
SA_EMAIL="skillpulse-runner@${PROJECT_ID}.iam.gserviceaccount.com"
FULL_IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/pipeline:latest"

# ── Non-secret env vars (safe to inline) ─────────────────────────────────────
ENV_VARS="STORAGE_MODE=gcs,\
BQ_DATASET_BRONZE=skillpulse_bronze,\
BQ_DATASET_SILVER=skillpulse_silver,\
BQ_DATASET_GOLD=skillpulse_gold,\
BQ_LOCATION=asia-south1,\
GCS_BRONZE_PREFIX=bronze,\
GCS_SILVER_PREFIX=silver,\
GCS_GOLD_PREFIX=gold,\
GEMINI_MODEL=gemini-2.5-flash-lite,\
GEMINI_BATCH_SIZE=20,\
GEMINI_MAX_RETRIES=3,\
GEMINI_RETRY_DELAY_SECONDS=5,\
JSEARCH_RESULTS_PER_PAGE=10,\
JSEARCH_TARGET_COUNTRY=IN,\
JSEARCH_API_HOST=jsearch.p.rapidapi.com,\
JSEARCH_BASE_URL=https://jsearch.p.rapidapi.com/search,\
LOG_LEVEL=INFO,\
LOG_FORMAT=json,\
DRY_RUN=false,\
INGESTION_BATCH_SIZE=100"

# ── Secrets: mounted as env vars from Secret Manager ─────────────────────────
# Format: ENV_VAR_NAME=SECRET_NAME:latest
SET_SECRETS="\
GEMINI_API_KEY=GEMINI_API_KEY:latest,\
RAPIDAPI_KEY_PRIMARY=RAPIDAPI_KEY_PRIMARY:latest,\
RAPIDAPI_KEY_MEGA=RAPIDAPI_KEY_MEGA:latest,\
BQ_PROJECT_ID=BQ_PROJECT_ID:latest,\
GCS_BUCKET_NAME=GCS_BUCKET_NAME:latest,\
GCP_PROJECT_ID=BQ_PROJECT_ID:latest"

echo "=============================================="
echo " SkillPulse — Deploy Cloud Run Job"
echo " Project  : $PROJECT_ID"
echo " Region   : $REGION"
echo " Job      : $JOB_NAME"
echo " Image    : $FULL_IMAGE"
echo "=============================================="

# ── F6: Create or Update Cloud Run Job ───────────────────────────────────────
echo ""
echo "Deploying Cloud Run Job: $JOB_NAME..."

if gcloud run jobs describe "$JOB_NAME" \
    --project="$PROJECT_ID" \
    --region="$REGION" &>/dev/null; then
  echo "ℹ️  Job exists — updating..."
  DEPLOY_CMD="update"
else
  echo "➕ Job does not exist — creating..."
  DEPLOY_CMD="create"
fi

gcloud run jobs "$DEPLOY_CMD" "$JOB_NAME" \
  --image="$FULL_IMAGE" \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --service-account="$SA_EMAIL" \
  --memory="2Gi" \
  --cpu="1" \
  --task-timeout="7200s" \
  --max-retries=1 \
  --set-env-vars="$ENV_VARS" \
  --set-secrets="$SET_SECRETS"

echo "✅ Cloud Run Job deployed: $JOB_NAME"

# ── F7: Cloud Scheduler Trigger ───────────────────────────────────────────────
echo ""
echo "Setting up Cloud Scheduler trigger..."

# Build the Cloud Run execute API URI
EXECUTE_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run"

if gcloud scheduler jobs describe "$SCHEDULER_JOB_NAME" \
    --project="$PROJECT_ID" \
    --location="$REGION" &>/dev/null; then
  echo "ℹ️  Scheduler job exists — updating..."
  SCHEDULER_CMD="update http"
else
  echo "➕ Creating scheduler job..."
  SCHEDULER_CMD="create http"
fi

gcloud scheduler jobs "$SCHEDULER_CMD" "$SCHEDULER_JOB_NAME" \
  --project="$PROJECT_ID" \
  --location="$REGION" \
  --schedule="30 0 * * *" \
  --time-zone="UTC" \
  --uri="$EXECUTE_URI" \
  --http-method="POST" \
  --oauth-service-account-email="$SA_EMAIL" \
  --description="Daily 6:00 AM IST trigger for SkillPulse pipeline"

echo "✅ Cloud Scheduler job configured: $SCHEDULER_JOB_NAME"
echo "   Fires daily at 6:00 AM IST (00:30 UTC)"

# ── Summary ──────────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║       Deployment Complete — Quick Reference             ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║  Manual trigger (live):                                 ║"
echo "║    gcloud run jobs execute $JOB_NAME \\"
echo "║      --region=$REGION                            ║"
echo "║                                                         ║"
echo "║  Manual trigger (DRY RUN):                              ║"
echo "║    gcloud run jobs execute $JOB_NAME \\"
echo "║      --region=$REGION \\"
echo "║      --update-env-vars DRY_RUN=true                     ║"
echo "║                                                         ║"
echo "║  View logs:                                             ║"
echo "║    gcloud run jobs executions list \\"
echo "║      --job=$JOB_NAME --region=$REGION          ║"
echo "╚══════════════════════════════════════════════════════════╝"
