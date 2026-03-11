import os
import sys
import json
import uuid
from datetime import datetime, timezone
from dotenv import load_dotenv

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add src to the path to allow imports when running standalone
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.ingestion.jsearch_client import JSearchClient
from src.utils.storage_client import StorageClient

def normalize_payload(raw: dict, source_api: str, ingestion_date: str, query: str, pipeline_run_id: str) -> dict:
    """Map raw JSearch payload to the canonical Bronze schema."""
    import json
    
    return {
        "source_job_id": raw.get("job_id"),
        "company_name": raw.get("employer_name"),
        "company_sector": raw.get("employer_company_type"),
        "job_title": raw.get("job_title"),
        "job_description_raw": raw.get("job_description"),
        "city": raw.get("job_city"),
        "state": raw.get("job_state"),
        "country": raw.get("job_country"),
        "is_remote_flag": raw.get("job_is_remote"),
        "employment_type": raw.get("job_employment_type"),
        "posted_at": raw.get("job_posted_at_datetime_utc"),
        "expires_at": raw.get("job_offer_expiration_datetime_utc"),
        "salary_min_raw": raw.get("job_min_salary"),
        "salary_max_raw": raw.get("job_max_salary"),
        "salary_currency": raw.get("job_salary_currency"),
        "salary_period": raw.get("job_salary_period"),
        "experience_obj": json.dumps(raw.get("job_required_experience", {})) if raw.get("job_required_experience") else None,
        "education_obj": json.dumps(raw.get("job_required_education", {})) if raw.get("job_required_education") else None,
        
        # Additional fields
        "source_api": source_api,
        
        # Medallion Audit
        "medallion_audit_created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "pipeline_run_id": pipeline_run_id,
        "source_system": "jsearch_api",
        "ingestion_date": ingestion_date,
        "search_query": query
    }

def main():
    # Load env variables from .env
    load_dotenv()
    
    # Configuration
    bronze_path = os.getenv("BRONZE_PATH", "./data/bronze")
    run_date = os.getenv("PIPELINE_RUN_DATE", datetime.now().strftime("%Y-%m-%d"))
    pipeline_run_id = str(uuid.uuid4())
    source_system = "jsearch_api"
    
    # Guardrails
    is_dry_run = os.getenv("DRY_RUN", "false").lower() == "true"
    queries_per_run = 2 if is_dry_run else int(os.getenv("JSEARCH_QUERIES_PER_RUN", "6"))
    pages_per_query = 1 if is_dry_run else int(os.getenv("JSEARCH_PAGES_PER_QUERY", "1"))
    
    logger.info(f"Starting Bronze Ingestion (DRY_RUN={is_dry_run}). Limit: {queries_per_run} queries, {pages_per_query} page(s).")
    
    # Ensure bronze directory exists
    os.makedirs(bronze_path, exist_ok=True)
    
    # Initialize component clients
    client = JSearchClient()
    storage_client = StorageClient()
    
    # Define query expansion list
    full_queries = [
        "data engineer",
        "data engineering",
        "Azure data engineer",
        "PySpark developer",
        "ETL developer",
        "Databricks engineer",
        "dbt developer",
        "big data engineer"
    ]
    
    # For now, simplistic slice. To do round-robin properly across days, we'd need to store state.
    # We will slice the top queries_per_run.
    queries = full_queries[:queries_per_run]
    
    # ---------------------------------------------------------
    # Deduplication Logic: Load existing source_job_ids from Bronze
    # ---------------------------------------------------------
    existing_job_ids = set()
    
    bronze_files = storage_client.list_files(bronze_path, suffix=".json")
    for filepath in bronze_files:
        try:
            data = storage_client.read_json(filepath)
            for job in data:
                job_id = job.get("source_job_id") or job.get("job_id")
                if job_id:
                    existing_job_ids.add(job_id)
        except Exception as e:
            logger.warning(f"Failed to read existing bronze file {filepath} for deduplication: {e}")
                    
    logger.info(f"Found {len(existing_job_ids)} existing job_ids in Bronze layer for deduplication.")
    
    all_jobs = []
    total_duplicates_skipped = 0
    jobs_per_api = {"JSEARCH_PRIMARY": 0, "JSEARCH_MEGA": 0}
    api_hosts = ["JSEARCH_PRIMARY"] if is_dry_run else ["JSEARCH_PRIMARY", "JSEARCH_MEGA"]
    
    for query in queries:
        logger.info(f"Processing query: '{query}'")
        
        for api_source in api_hosts:
            response_data = client.fetch_jobs(query, api_source=api_source, page=1, num_pages=pages_per_query)
            
            if response_data and 'data' in response_data:
                jobs = response_data['data']
                skipped_this_query = 0
                
                for raw_job in jobs:
                    source_job_id = raw_job.get("job_id")
                    
                    if not source_job_id or source_job_id in existing_job_ids:
                        skipped_this_query += 1
                        total_duplicates_skipped += 1
                        continue
                    
                    # Normalize Payload
                    canonical_job = normalize_payload(
                        raw=raw_job,
                        source_api=api_source,
                        ingestion_date=run_date,
                        query=query,
                        pipeline_run_id=pipeline_run_id
                    )
                    
                    existing_job_ids.add(source_job_id)
                    all_jobs.append(canonical_job)
                    jobs_per_api[api_source] += 1
                    
                logger.info(f"Retrieved {len(jobs)} jobs for '{query}' from {api_source}. Skipped {skipped_this_query} duplicates.")
            else:
                logger.warning(f"No valid data returned for query '{query}' from {api_source}")
            
    if all_jobs:
        # Save to bronze layer as raw JSON
        # Format filename: source_system_YYYYMMDD_runID.json
        output_filename = f"{source_system}_{run_date.replace('-', '')}_{pipeline_run_id[:8]}.json"
        
        # Unify path handling
        output_filepath = f"{bronze_path}/{output_filename}" if storage_client.mode == "gcs" else os.path.join(bronze_path, output_filename)
        
        storage_client.write_json(all_jobs, output_filepath)
            
        logger.info(f"Successfully saved {len(all_jobs)} new jobs to {output_filepath}.")
        logger.info(f"Total initially fetched across all: {len(all_jobs) + total_duplicates_skipped}")
        logger.info(f"Total after dedup: {len(all_jobs)}. Skipped: {total_duplicates_skipped}")
        logger.info(f"Jobs per API source: {jobs_per_api}")
    else:
        logger.warning(f"No new jobs fetched (Duplicates skipped: {total_duplicates_skipped}). Nothing to save.")

    # Record Pipeline Run State
    run_metadata = {
        "run_id": pipeline_run_id,
        "run_date": run_date,
        "dag_trigger_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "dry_run_mode": is_dry_run,
        "jobs_fetched_total": len(all_jobs) + total_duplicates_skipped,
        "jobs_after_dedup": len(all_jobs),
        "api_requests_used": sum(jobs_per_api.values())
    }
    storage_client.write_json(run_metadata, f"runs/{run_date}_run_state.json")

if __name__ == "__main__":
    main()
