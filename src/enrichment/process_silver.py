import os
import sys
import time
import json
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.enrichment.gemini_client import GeminiEnrichmentClient
from src.utils.storage_client import StorageClient

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def process_bronze_to_silver():
    load_dotenv()
    
    bronze_path = os.getenv("BRONZE_PATH", "./data/bronze")
    silver_path = os.getenv("SILVER_PATH", "./data/silver")
    run_date = os.getenv("PIPELINE_RUN_DATE", datetime.now().strftime("%Y-%m-%d"))
    batch_size = int(os.getenv("GEMINI_BATCH_SIZE", "10"))
    model_used = os.getenv("GEMINI_MODEL", "gemini-2.5-pro")
    
    # 1. Setup Silver partition folder structure (e.g. ./data/silver/2026-03-09/)
    silver_partition_path = os.path.join(silver_path, run_date)
    os.makedirs(silver_partition_path, exist_ok=True)
    
    # Initialize Storage Client
    storage_client = StorageClient()
    
    # 2. Gather Bronze jobs
    logger.info(f"Scanning {bronze_path} for raw Bronze files...")
        
    all_bronze_jobs = []
    bronze_files = storage_client.list_files(bronze_path, suffix=".json")
    for filepath in bronze_files:
        try:
            data = storage_client.read_json(filepath)
            if isinstance(data, list):
                all_bronze_jobs.extend(data)
        except Exception as e:
            logger.error(f"Failed reading {filepath}: {e}")
                
    if not all_bronze_jobs:
        logger.warning("No Bronze jobs found to process. Exiting.")
        return
        
    logger.info(f"Found {len(all_bronze_jobs)} total jobs to try enriching.")

    # 3. Deduplicate inputs against existing Silver runs based on job_id (only successfully enriched ones)
    existing_silver_job_ids = set()
    silver_files = storage_client.list_files(silver_path, suffix=".json")
    for filepath in silver_files:
        if "silver_enriched_" in filepath:
            try:
                data = storage_client.read_json(filepath)
                for job in data:
                    if "job_id" in job:
                        existing_silver_job_ids.add(job["job_id"])
            except Exception as e:
                logger.warning(f"Error reading existing silver file {filepath}: {e}")
                    
    jobs_to_process = [
        job for job in all_bronze_jobs 
        if job.get("job_id") not in existing_silver_job_ids
    ]
    
    skipped_count = len(all_bronze_jobs) - len(jobs_to_process)
    logger.info(f"Skipped {skipped_count} jobs already present in Silver layer.")
    
    if not jobs_to_process:
        logger.info("All Bronze jobs have already been enriched. Exiting.")
        return

    # 4. Process through Gemini in batches
    logger.info(f"Starting Gemini processing for {len(jobs_to_process)} jobs (Batch size: {batch_size})...")
    client = GeminiEnrichmentClient()
    successful_jobs = []
    failed_jobs = []
    failed_jobs_count = 0
    
    # Generate the single run ID for this silver batch
    # We grab the ID from one of the bronze records if possible, 
    # but the requirement states we create a run ID
    import uuid
    pipeline_run_id = str(uuid.uuid4())
    
    # Process in chunks as requested
    job_batches = list(chunks(jobs_to_process, batch_size))
    for batch_idx, batch in enumerate(job_batches, 1):
        logger.info(f"Processing Batch {batch_idx}/{len(job_batches)} ({len(batch)} jobs)...")
        
        for job in batch:
            job_description = job.get('job_description')
            if not job_description:
                failed_jobs_count += 1
                job['silver_medallion_audit'] = {
                    'failure_reason': 'missing_job_description',
                    'pipeline_run_id': pipeline_run_id,
                    'failed_at': datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                }
                failed_jobs.append(job)
                logger.warning(f"Job missing description (ID: {job.get('job_id', 'unknown')}). Skipping.")
                continue
                
            # Call Gemini AI
            structured_data = client.extract_job_entities(job_description)
            
            if structured_data:
                # Merge AI Output
                job['ai_enriched_data'] = structured_data
                
                # Append Silver Audit details
                job['silver_medallion_audit'] = {
                    'enriched_at': datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    'pipeline_run_id': pipeline_run_id,
                    'gemini_model_used': model_used,
                    'enrichment_confidence': structured_data.get('enrichment_confidence', 'unknown')
                }
                
                successful_jobs.append(job)
            else:
                failed_jobs_count += 1
                job['silver_medallion_audit'] = {
                    'failure_reason': 'gemini_enrichment_failed',
                    'pipeline_run_id': pipeline_run_id,
                    'failed_at': datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                }
                failed_jobs.append(job)
                logger.warning(f"Failed to enrich job (ID: {job.get('job_id', 'unknown')}). It will not be saved to Silver.")
            
            # Simple manual rate limiting protection 
            # if we are doing many fast requests in a batch loop
            time.sleep(1) 
            
    # 5. Save Output
    if successful_jobs:
        output_filename = f"silver_enriched_{run_date.replace('-', '')}_{pipeline_run_id[:8]}.json"
        output_filepath = f"{silver_partition_path}/{output_filename}" if storage_client.mode == "gcs" else os.path.join(silver_partition_path, output_filename)
        
        storage_client.write_json(successful_jobs, output_filepath)
            
        logger.info(f"Successfully enriched {len(successful_jobs)} jobs.")
        logger.info(f"Saved to {output_filepath}")
        
    if failed_jobs:
        failed_filename = f"silver_failed_enrichment_{run_date.replace('-', '')}_{pipeline_run_id[:8]}.json"
        failed_filepath = f"{silver_partition_path}/{failed_filename}" if storage_client.mode == "gcs" else os.path.join(silver_partition_path, failed_filename)
        
        storage_client.write_json(failed_jobs, failed_filepath)
            
        logger.warning(f"{len(failed_jobs)} jobs failed enrichment and were saved to {failed_filepath} for later reprocessing.")
        
    if not successful_jobs and not failed_jobs:
        logger.error("No jobs were processed. Nothing saved to Silver layer.")

if __name__ == "__main__":
    process_bronze_to_silver()
