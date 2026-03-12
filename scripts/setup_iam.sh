#!/usr/bin/env bash
# =============================================================================
# scripts/setup_iam.sh — F5: Service Account + Role Bindings
# =============================================================================
# Creates the `skillpulse-runner` service account and assigns all required
# GCP roles for the Cloud Run Pipeline Job.
#
# Usage:
#   bash scripts/setup_iam.sh
#
# Safe to re-run: gcloud add-iam-policy-binding is idempotent.
# Prerequisites:
#   gcloud auth login && gcloud config set project skillpulse-india
# =============================================================================
set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:-skillpulse-india}"
SA_NAME="skillpulse-runner"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

echo "=============================================="
echo " SkillPulse — IAM Service Account Setup"
echo " Project : $PROJECT_ID"
echo " SA      : $SA_EMAIL"
echo "=============================================="

# ── 1. Create service account (idempotent) ───────────────────────────────────
if gcloud iam service-accounts describe "$SA_EMAIL" --project="$PROJECT_ID" &>/dev/null; then
  echo "ℹ️  Service account already exists: $SA_EMAIL"
else
  echo "➕ Creating service account..."
  gcloud iam service-accounts create "$SA_NAME" \
    --project="$PROJECT_ID" \
    --display-name="SkillPulse Pipeline Runner" \
    --description="Service account for Cloud Run pipeline job execution"
  echo "✅ Service account created."
fi

# ── 2. Assign roles ──────────────────────────────────────────────────────────
ROLES=(
  "roles/bigquery.dataEditor"      # Read/write BQ tables
  "roles/bigquery.jobUser"         # Run BQ load jobs + queries
  "roles/storage.objectAdmin"      # Read/write GCS bronze/silver/gold
  "roles/secretmanager.secretAccessor"  # Access Secret Manager secrets
  "roles/run.invoker"              # Cloud Scheduler → Cloud Run trigger
)

echo ""
echo "Granting roles to $SA_EMAIL..."
for ROLE in "${ROLES[@]}"; do
  echo "  → $ROLE"
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="$ROLE" \
    --quiet
  echo "     ✅ Granted."
done

echo ""
echo "✅ IAM setup complete."
echo ""
echo "Verify with:"
echo "  gcloud projects get-iam-policy $PROJECT_ID --flatten='bindings[].members' \\"
echo "    --format='table(bindings.role,bindings.members)' \\"
echo "    --filter='bindings.members:$SA_EMAIL'"
