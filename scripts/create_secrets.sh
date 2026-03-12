#!/usr/bin/env bash
# =============================================================================
# scripts/create_secrets.sh — F4: Secret Manager Setup
# =============================================================================
# Reads secret values from your local .env and pushes them to GCP Secret Manager.
# Safe to re-run: if a secret already exists it adds a new version instead.
#
# Usage:
#   bash scripts/create_secrets.sh
#   bash scripts/create_secrets.sh --dry-run   (prints what would happen)
#
# Prerequisites:
#   gcloud auth login && gcloud config set project skillpulse-india
# =============================================================================
set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:-skillpulse-india}"
DRY_RUN_FLAG="${1:-}"

# Map: Secret Manager name → .env variable name
declare -A SECRET_MAP=(
  ["GEMINI_API_KEY"]="GEMINI_API_KEY"
  ["RAPIDAPI_KEY_PRIMARY"]="RAPIDAPI_KEY_PRIMARY"
  ["RAPIDAPI_KEY_MEGA"]="RAPIDAPI_KEY_MEGA"
  ["BQ_PROJECT_ID"]="BQ_PROJECT_ID"
  ["GCS_BUCKET_NAME"]="GCS_BUCKET_NAME"
  ["PIXELFORGE_API_KEY"]="PIXELFORGE_API_KEY"
)

# Load .env values into shell (strip comments and blank lines)
ENV_FILE="$(dirname "$0")/../.env"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "❌ .env file not found at $ENV_FILE"
  exit 1
fi

load_env() {
  local key="$1"
  # Extract value: strip inline comments and surrounding whitespace
  grep -E "^${key}\s*=" "$ENV_FILE" \
    | head -1 \
    | sed 's/^[^=]*=\s*//' \
    | sed 's/\s*#.*//' \
    | tr -d '\r'
}

echo "=============================================="
echo " SkillPulse — Secret Manager Sync"
echo " Project: $PROJECT_ID"
[[ "$DRY_RUN_FLAG" == "--dry-run" ]] && echo " MODE: DRY RUN (no changes made)"
echo "=============================================="

for SECRET_NAME in "${!SECRET_MAP[@]}"; do
  ENV_VAR="${SECRET_MAP[$SECRET_NAME]}"
  VALUE="$(load_env "$ENV_VAR")"

  if [[ -z "$VALUE" ]]; then
    echo "⚠️  $SECRET_NAME — value not found in .env (skipping)"
    continue
  fi

  if [[ "$DRY_RUN_FLAG" == "--dry-run" ]]; then
    echo "   [DRY RUN] Would push: $SECRET_NAME (${#VALUE} chars)"
    continue
  fi

  # Check if secret already exists
  if gcloud secrets describe "$SECRET_NAME" --project="$PROJECT_ID" &>/dev/null; then
    echo "🔄  $SECRET_NAME — exists, adding new version..."
    echo -n "$VALUE" | gcloud secrets versions add "$SECRET_NAME" \
      --project="$PROJECT_ID" \
      --data-file=-
  else
    echo "➕  $SECRET_NAME — creating..."
    echo -n "$VALUE" | gcloud secrets create "$SECRET_NAME" \
      --project="$PROJECT_ID" \
      --replication-policy="automatic" \
      --data-file=-
  fi
  echo "   ✅ $SECRET_NAME synced."
done

echo ""
echo "✅ Secret Manager sync complete."
echo ""
echo "Verify with:"
echo "  gcloud secrets list --project=$PROJECT_ID"
