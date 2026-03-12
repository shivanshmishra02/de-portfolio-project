#!/usr/bin/env bash
# =============================================================================
# scripts/build_and_push.sh — F3: Artifact Registry + Image Build
# =============================================================================
# Creates the Artifact Registry Docker repository (if needed) and builds +
# pushes the pipeline image using Cloud Build.
#
# Usage:
#   bash scripts/build_and_push.sh
#
# Prerequisites:
#   gcloud auth login && gcloud config set project skillpulse-india
#   Cloud Build API enabled: gcloud services enable cloudbuild.googleapis.com
# =============================================================================
set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:-skillpulse-india}"
REGION="asia-south1"
REPO="skillpulse-repo"
IMAGE_NAME="pipeline"
IMAGE_TAG="latest"
FULL_IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${IMAGE_NAME}:${IMAGE_TAG}"

# Resolve project root (one level up from scripts/)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "=============================================="
echo " SkillPulse — Build & Push Pipeline Image"
echo " Project : $PROJECT_ID"
echo " Region  : $REGION"
echo " Image   : $FULL_IMAGE"
echo "=============================================="

# ── 1. Enable required APIs ──────────────────────────────────────────────────
echo ""
echo "Enabling required GCP APIs..."
gcloud services enable \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  run.googleapis.com \
  cloudscheduler.googleapis.com \
  secretmanager.googleapis.com \
  --project="$PROJECT_ID" \
  --quiet
echo "✅ APIs enabled."

# ── 2. Create Artifact Registry repo (idempotent) ────────────────────────────
echo ""
if gcloud artifacts repositories describe "$REPO" \
    --project="$PROJECT_ID" \
    --location="$REGION" &>/dev/null; then
  echo "ℹ️  Artifact Registry repo '$REPO' already exists."
else
  echo "➕ Creating Artifact Registry repo: $REPO..."
  gcloud artifacts repositories create "$REPO" \
    --repository-format=docker \
    --location="$REGION" \
    --description="SkillPulse pipeline Docker images" \
    --project="$PROJECT_ID"
  echo "✅ Repository created."
fi

# ── 3. Submit Cloud Build ────────────────────────────────────────────────────
echo ""
echo "Submitting build to Cloud Build..."
echo "  Source: $PROJECT_ROOT"
echo "  Tag   : $FULL_IMAGE"
gcloud builds submit "$PROJECT_ROOT" \
  --tag="$FULL_IMAGE" \
  --project="$PROJECT_ID"

echo ""
echo "✅ Image pushed: $FULL_IMAGE"
echo ""
echo "Next step — run deploy_cloud_run_job.sh to create/update the Cloud Run Job."
