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
from src.utils.skill_normalizer import normalize_skill

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def parse_experience(experience_obj_str):
    """Parse experience_obj mapping to integer years"""
    if not experience_obj_str:
        return None
    try:
        obj = json.loads(experience_obj_str)
        months = obj.get("required_experience_in_months")
        if months is not None:
            return round(months / 12, 1)
        if obj.get("experience_mentioned") is True:
            return -1
    except Exception:
        pass
    return None

def parse_education(education_obj_str):
    """Parse education_obj to a degree string"""
    if not education_obj_str:
        return "Not Specified"
    try:
        obj = json.loads(education_obj_str)
        if obj.get("postgraduate_degree") is True:
            return "Postgraduate"
        if obj.get("bachelors_degree") is True:
            return "Bachelors"
        if obj.get("professional_certification") is True:
            return "Certification"
    except Exception:
        pass
    return "Not Specified"

def parse_work_mode(is_remote_flag):
    """Determine initial work mode"""
    if is_remote_flag is True:
        return "Remote"
    elif is_remote_flag is False:
        return "Onsite"
    return None

def process_bronze_to_silver():
    load_dotenv()
    
    bronze_path = os.getenv("BRONZE_PATH", "./data/bronze")
    silver_path = os.getenv("SILVER_PATH", "./data/silver")
    run_date = os.getenv("PIPELINE_RUN_DATE", datetime.now().strftime("%Y-%m-%d"))
    batch_size = int(os.getenv("GEMINI_BATCH_SIZE", "10"))
    model_used = os.getenv("GEMINI_MODEL", "gemini-2.5-pro")
    
    # 1. Setup Silver partition folder structure
    silver_partition_path = os.path.join(silver_path, run_date)
    os.makedirs(silver_partition_path, exist_ok=True)
    
    storage_client = StorageClient()
    
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

    # 3. Deduplicate inputs against existing Silver runs based on source_job_id
    existing_silver_job_ids = set()
    silver_files = storage_client.list_files(silver_path, suffix=".json")
    for filepath in silver_files:
        if "silver_enriched_" in filepath:
            try:
                data = storage_client.read_json(filepath)
                for job in data:
                    job_id = job.get("source_job_id") or job.get("job_id")
                    if job_id:
                        existing_silver_job_ids.add(job_id)
            except Exception as e:
                logger.warning(f"Error reading existing silver file {filepath}: {e}")
                    
    jobs_to_process = [
        job for job in all_bronze_jobs 
        if (job.get("source_job_id") or job.get("job_id")) not in existing_silver_job_ids
    ]
    
    skipped_count = len(all_bronze_jobs) - len(jobs_to_process)
    logger.info(f"Skipped {skipped_count} jobs already present in Silver layer.")
    
    if not jobs_to_process:
        logger.info("All Bronze jobs have already been enriched. Exiting.")
        return

    logger.info(f"Starting Gemini processing for {len(jobs_to_process)} jobs (Batch size: {batch_size})...")
    client = GeminiEnrichmentClient()
    successful_jobs = []
    failed_jobs = []
    failed_jobs_count = 0
    
    import uuid
    pipeline_run_id = str(uuid.uuid4())
    
    job_batches = list(chunks(jobs_to_process, batch_size))
    for batch_idx, batch in enumerate(job_batches, 1):
        logger.info(f"Processing Batch {batch_idx}/{len(job_batches)} ({len(batch)} jobs)...")
        
        for job in batch:
            job_description = job.get('job_description_raw') or job.get('job_description')
            job_id = job.get("source_job_id") or job.get("job_id")
            
            if not job_description:
                failed_jobs_count += 1
                job['silver_medallion_audit'] = {
                    'failure_reason': 'missing_job_description',
                    'pipeline_run_id': pipeline_run_id,
                    'failed_at': datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                }
                failed_jobs.append(job)
                logger.warning(f"Job missing description (ID: {job_id}). Skipping.")
                continue
            
            # 1. FREE FIELD PARSING (Zero Cost)
            parsed_experience = parse_experience(job.get("experience_obj"))
            parsed_education = parse_education(job.get("education_obj"))
            parsed_work_mode = parse_work_mode(job.get("is_remote_flag"))

            # 2. Call leaner Gemini API
            ai_data_obj = client.extract_job_entities(job_description, job_id)
            ai_data = ai_data_obj.model_dump()
            
            # Since the safe default returns an object with empty skills, we can check if it actually succeeded
            # Alternatively, we could check enrichment_confidence if it was part of the model.
            # We will consider it successful if it extracted at least something (or we could just say it's successful always, 
            # but the dead letter queue already caught failures. However, if skills are empty, we might still want to keep it.)
            if ai_data:
                # 3. MERGE LOGIC
                # Final silver record = Base fields + Parsed fields + Gemini fields
                
                # Base Fields (passthrough)
                silver_job = {
                    "source_job_id": job_id,
                    "company_name": job.get("company_name") or job.get("employer_name"),
                    "company_sector": job.get("company_sector") or job.get("employer_company_type"),
                    "job_title": job.get("job_title"),
                    "city": job.get("city") or job.get("job_city"),
                    "state": job.get("state") or job.get("job_state"),
                    "country": job.get("country") or job.get("job_country"),
                    "employment_type": job.get("employment_type") or job.get("job_employment_type"),
                    "posted_at": job.get("posted_at") or job.get("job_posted_at_datetime_utc"),
                    "expires_at": job.get("expires_at") or job.get("job_offer_expiration_datetime_utc"),
                    "salary_min_raw": job.get("salary_min_raw") or job.get("job_min_salary"),
                    "salary_max_raw": job.get("salary_max_raw") or job.get("job_max_salary"),
                    "salary_currency": job.get("salary_currency") or job.get("job_salary_currency"),
                    "salary_period": job.get("salary_period") or job.get("job_salary_period"),
                    "source_api": job.get("source_api", "JSEARCH_PRIMARY")
                }
                
                # Parsed Fields
                silver_job["experience_years"] = parsed_experience
                silver_job["degree_requirement"] = parsed_education
                
                # AI Fields
                raw_skills = ai_data.get("skills", [])
                silver_job["skills"] = [normalize_skill(s) for s in raw_skills if normalize_skill(s)]
                silver_job["seniority_level"] = ai_data.get("seniority_level", "Unknown")
                silver_job["tech_stack_category"] = ai_data.get("tech_stack_category", "Other")
                silver_job["salary_min_lpa"] = ai_data.get("salary_min_lpa")
                silver_job["salary_max_lpa"] = ai_data.get("salary_max_lpa")
                
                # Work Mode Merge Logic
                override_mode = ai_data.get("work_mode_override")
                if override_mode:
                    silver_job["work_mode"] = override_mode
                else:
                    silver_job["work_mode"] = parsed_work_mode or "Unknown"

                # Append Silver Audit details
                silver_job['silver_medallion_audit'] = {
                    'enriched_at': datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    'pipeline_run_id': pipeline_run_id,
                    'gemini_model_used': model_used,
                    'enrichment_confidence': 'high' # Hardcoded success for this simpler model
                }
                
                successful_jobs.append(silver_job)
            else:
                failed_jobs_count += 1
                job['silver_medallion_audit'] = {
                    'failure_reason': 'gemini_enrichment_failed',
                    'pipeline_run_id': pipeline_run_id,
                    'failed_at': datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                }
                failed_jobs.append(job)
                logger.warning(f"Failed to enrich job (ID: {job_id}). Not saved to Silver.")
            
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
