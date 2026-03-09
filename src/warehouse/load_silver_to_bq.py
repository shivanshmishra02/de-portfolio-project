import os
import sys
import json
import logging
from google.cloud import bigquery
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_silver_to_bq():
    """
    Reads enriched JSON files from the Silver layer and loads them via
    a Batch Load Job into the BigQuery Sandbox. Uses Application Default Credentials
    for seamless, keyless local authentication.
    """
    load_dotenv()
    
    # Configuration
    project_id = os.getenv("BQ_PROJECT_ID")
    dataset_silver = os.getenv("BQ_DATASET_SILVER", "skillpulse_silver")
    silver_path = os.getenv("SILVER_PATH", "./data/silver")
    
    if not project_id:
        logger.error("BQ_PROJECT_ID missing from .env. Please configure it.")
        return

    # Initialize BigQuery Client using local ADC
    client = bigquery.Client(project=project_id)
    
    # Target table format: project.dataset.table
    target_table_id = f"{project_id}.{dataset_silver}.stg_job_postings"
    
    logger.info(f"Targeting BigQuery Table: {target_table_id}")
    logger.info(f"Scanning Silver Layer: {silver_path}")
    
    if not os.path.exists(silver_path):
        logger.error(f"Silver path does not exist: {silver_path}")
        return

    # Configuration for the BigQuery batch Load Job
    # Using SOURCE_FORMAT=NEWLINE_DELIMITED_JSON requires jsonl format,
    # or we can pass the Python dictionaries directly by loading from disk.
    job_config = bigquery.LoadJobConfig(
        # We'll let BigQuery auto-detect the schema for the initial staging table.
        # dbt will handle the strict typing downstream.
        autodetect=True,
        # Write append ensures we keep a historical log in staging
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        # We pass python dicts directly, so format isn't strictly needed for the API method used below,
        # but good practice to define.
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    )

    all_valid_jobs = []
    failed_enrichment_ignored = 0
    total_files_scanned = 0

    # Crawl Silver for enriched data
    for root, _, files in os.walk(silver_path):
        for file in files:
            # We strictly want the successfully enriched payload files
            if file.startswith("silver_enriched_") and file.endswith(".json"):
                total_files_scanned += 1
                filepath = os.path.join(root, file)
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        
                    for job in data:
                        # Extract the nested output from Gemini
                        ai_data = job.get('ai_enriched_data')
                        
                        # Rule: Failed enrichment records (enrichment_confidence = null) 
                        # should be flagged, not loaded to Gold
                        if not ai_data or not ai_data.get('enrichment_confidence'):
                            logger.debug(f"Skipping job {job.get('job_id')} due to missing enrichment confidence.")
                            failed_enrichment_ignored += 1
                            continue
                            
                        # To flatten slightly for BigQuery schema auto-detection purposes 
                        # without making the structure too complex:
                        # We merge 'ai_enriched_data' and 'silver_medallion_audit' up to the root level.
                        # (Note: dbt could handle the JSON extraction, but BigQuery handles flat structures better
                        # for standard tables).
                        
                        flat_job = dict(job)
                        
                        # Flatten AI Output
                        if 'ai_enriched_data' in flat_job:
                            ai_dict = flat_job.pop('ai_enriched_data')
                            for k, v in ai_dict.items():
                                flat_job[f"ai_{k}"] = v
                                
                        # Flatten Medallion Audit
                        if 'silver_medallion_audit' in flat_job:
                            audit_dict = flat_job.pop('silver_medallion_audit')
                            for k, v in audit_dict.items():
                                flat_job[f"audit_{k}"] = v
                                
                        # Clean empty complex structures (lists/dicts) that crash BQ auto-detect
                        keys_to_delete = []
                        for k, v in flat_job.items():
                            if isinstance(v, (list, dict)) and not v:
                                keys_to_delete.append(k)
                        for k in keys_to_delete:
                            del flat_job[k]
                                
                        all_valid_jobs.append(flat_job)
                        
                except Exception as e:
                    logger.error(f"Error reading file {filepath}: {e}")

    logger.info(f"Scanned {total_files_scanned} files.")
    logger.info(f"Skipped {failed_enrichment_ignored} jobs lacking enrichment confidence.")
    logger.info(f"Prepared {len(all_valid_jobs)} total mapped jobs for BigQuery Load.")

    if not all_valid_jobs:
        logger.warning("No valid jobs to load.")
        return

    # Trigger BigQuery Batch Load (No Streaming Inserts)
    logger.info("Initiating BQ Batch Load Job...")
    try:
        # load_table_from_json handles a list of dictionaries as a batch API payload!
        load_job = client.load_table_from_json(
            all_valid_jobs, 
            target_table_id, 
            job_config=job_config
        )
        # Wait for the batch load job to complete
        load_job.result()  
        
        # Verify
        destination_table = client.get_table(target_table_id)
        logger.info(f"✅ Successfully loaded {load_job.output_rows} rows into {target_table_id}.")
        logger.info(f"Table now contains {destination_table.num_rows} total rows.")
        
    except Exception as e:
        logger.error(f"BigQuery Load Job failed: {e}")
        if hasattr(load_job, 'errors') and load_job.errors:
             logger.error(f"Detailed BQ Errors: {load_job.errors}")

if __name__ == "__main__":
    load_silver_to_bq()
