# SkillPulse India

**AI-Powered Job Market Intelligence Pipeline**

An end-to-end data engineering pipeline that ingests raw job postings from Indian job boards via API daily, uses Gemini AI to extract structured entities (skills, salary, role type, experience) from unstructured job descriptions, applies Medallion Architecture (Bronze → Silver → Gold), stores analytical data in BigQuery using a star schema, and serves insights via a 5-page Streamlit dashboard.

## Architecture

```
Cloud Scheduler (6 AM IST daily)
        │
        ▼
Cloud Run Job (skillpulse-pipeline-job)
        │
        ├── Step 1: JSearch API → GCS Bronze (raw JSON)
        ├── Step 2: Gemini AI  → BigQuery Silver (enriched, Pydantic-validated)
        ├── Step 3: BQ Load    → stg_job_postings
        └── Step 4: dbt build  → Gold Marts (fact + dims)
                │
                ▼
        BigQuery Gold Layer
                │
                ▼
        Streamlit Dashboard (5 pages)
```

**Medallion Layers**

| Layer | Storage | Tool |
|---|---|---|
| Bronze | GCS (`skillpulse-india-datalake/bronze`) | JSearch API |
| Silver | BigQuery `skillpulse_silver` | Gemini AI + Pydantic |
| Gold | BigQuery `skillpulse_gold` | dbt |

## How to Run

### Manual Pipeline Trigger (Cloud Run)

```bash
# Live run
gcloud run jobs execute skillpulse-pipeline-job --region=asia-south1 --wait

# Dry run (no live API calls)
gcloud run jobs execute skillpulse-pipeline-job \
  --region=asia-south1 \
  --update-env-vars DRY_RUN=true \
  --wait
```

### View Execution Logs

```bash
# List executions
gcloud run jobs executions list --job=skillpulse-pipeline-job --region=asia-south1

# Stream logs from latest execution
gcloud logging read 'resource.type="cloud_run_job" AND resource.labels.job_name="skillpulse-pipeline-job"' \
  --project=skillpulse-india \
  --limit=100 \
  --format="value(textPayload)"
```

### Streamlit Dashboard (Local)

1. Set up Google Application Default Credentials:
   ```bash
   gcloud auth application-default login
   ```
2. Install dashboard dependencies:
   ```bash
   pip install streamlit pandas plotly google-cloud-bigquery
   ```
3. Launch:
   ```bash
   streamlit run src/dashboard/app.py
   ```

## Infrastructure Setup (First Time)

Run these scripts in order from the project root (requires `gcloud` CLI + project owner permissions):

```bash
# 1. Create IAM service account + grant roles
bash scripts/setup_iam.sh

# 2. Push secrets from local .env → GCP Secret Manager
bash scripts/create_secrets.sh

# 3. Build Docker image + push to Artifact Registry
bash scripts/build_and_push.sh

# 4. Deploy Cloud Run Job + Cloud Scheduler trigger
bash scripts/deploy_cloud_run_job.sh
```

## Project Structure

```
skillpulse-india/
├── run_pipeline.py              # Pipeline entrypoint (replaces Airflow DAG)
├── Dockerfile                   # Cloud Run container definition
├── requirements-pipeline.txt    # Pipeline-only Python dependencies
├── requirements.txt             # Dashboard Python dependencies
├── scripts/
│   ├── setup_iam.sh             # Service account + IAM roles
│   ├── create_secrets.sh        # Secret Manager push from .env
│   ├── build_and_push.sh        # Artifact Registry build + push
│   └── deploy_cloud_run_job.sh  # Cloud Run Job + Scheduler setup
├── src/
│   ├── ingestion/               # JSearch API → GCS Bronze
│   ├── enrichment/              # Gemini AI → Silver enrichment
│   ├── warehouse/               # Silver → BigQuery load
│   └── dashboard/               # 5-page Streamlit app
├── dbt_project/                 # Gold layer transformations (star schema)
└── airflow/                     # Legacy (archived, not active)
```

## Data Architecture & Robustness

### 1. Slowly Changing Dimensions (SCD)
- **Type 1 (Overwrite):** `dim_company`, `dim_role`, and `dim_skill` automatically update with the most recent information while preserving their distinct grain.
- **Type 2 (History):** `dim_job` maintains an `is_active` flag. Jobs expire dynamically based on API signals or a strict 60-day Time-To-Live (TTL).

### 2. Deep Validation & Dead-Letter Queue
- External AI model extraction (Google Gemini) uses strict **Pydantic** typing for robust schema mapping.
- Unstructured responses that fail validation or trigger safety limits are cleanly diverted to a **Google Cloud Storage Dead-Letter Queue** (`/dead_letter/`) without breaking the daily pipeline run.
- Regex fallbacks run pre- and post-LLM extraction to guarantee high-confidence parsing of null salary and geo-cities.

### 3. Pipeline Observability
Every Cloud Run execution logs its run state to `skillpulse_gold.pipeline_runs` in BigQuery — tracking status, duration, API efficiency, duplication rates, and enrichment confidence.

```sql
-- Check recent pipeline runs
SELECT run_id, status, run_date, run_duration_seconds
FROM `skillpulse-india.skillpulse_gold.pipeline_runs`
ORDER BY dag_trigger_time DESC
LIMIT 5;
```

### 4. Setup

- GCP Project: `skillpulse-india`
- Region: `asia-south1` (Mumbai)
- Service Account: `skillpulse-runner@skillpulse-india.iam.gserviceaccount.com`
- Cloud Run Job: `skillpulse-pipeline-job`
- Scheduler: Daily at 6:00 AM IST (00:30 UTC)
