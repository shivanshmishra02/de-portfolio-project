import os
import sys
import json
import logging
from datetime import datetime, timezone
from google.cloud import bigquery
from dotenv import load_dotenv
from src.utils.storage_client import StorageClient

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
    run_date = os.getenv("PIPELINE_RUN_DATE", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    
    if not project_id:
        logger.error("BQ_PROJECT_ID missing from .env. Please configure it.")
        return

    # Initialize BigQuery Client using local ADC
    client = bigquery.Client(project=project_id)
    
    # Target table format: project.dataset.table
    target_table_id = f"{project_id}.{dataset_silver}.stg_job_postings"
    
    logger.info(f"Targeting BigQuery Table: {target_table_id}")
    logger.info(f"Scanning Silver Layer: {silver_path}")
    
    # Initialize Storage Client
    storage_client = StorageClient()

    # Configuration for the BigQuery batch Load Job
    # Using SOURCE_FORMAT=NEWLINE_DELIMITED_JSON requires jsonl format,
    # or we can pass the Python dictionaries directly by loading from disk.
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
    )

    all_valid_jobs = []
    failed_enrichment_ignored = 0
    total_files_scanned = 0

    # Crawl Silver for enriched data
    silver_files = storage_client.list_files(silver_path, suffix=".json")
    for filepath in silver_files:
        # We strictly want the successfully enriched payload files
        if "silver_enriched_" in filepath:
            total_files_scanned += 1
            try:
                data = storage_client.read_json(filepath)
                    
                for job in data:
                        audit_data = job.get('silver_medallion_audit', {})
                        
                        # Rule: Failed enrichment records are flagged by their failure_reason or lack of confidence
                        if audit_data.get('failure_reason') or not audit_data.get('enrichment_confidence'):
                            logger.debug(f"Skipping job {job.get('source_job_id') or job.get('job_id')} due to missing confidence/failure.")
                            failed_enrichment_ignored += 1
                            continue
                            
                        # To flatten slightly for BigQuery schema auto-detection purposes 
                        flat_job = dict(job)
                                
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

    # -------- Pipeline Run Logging --------
    state_file = f"runs/{run_date}_run_state.json"
    try:
        run_metadata = storage_client.read_json(state_file)
    except Exception:
        run_metadata = {"run_id": "unknown_run", "run_date": run_date}
        
    run_metadata["jobs_written_to_bq"] = len(all_valid_jobs)
    
    start_time_str = run_metadata.get("dag_trigger_time")
    if start_time_str:
        start_time = datetime.fromisoformat(start_time_str.replace("Z", "+00:00"))
        run_duration = (datetime.now(timezone.utc) - start_time).total_seconds()
    else:
        run_duration = 0.0
        
    run_metadata["run_duration_seconds"] = run_duration
    run_metadata["status"] = "SUCCESS" if run_metadata.get("jobs_enriched_failed", 0) == 0 else "PARTIAL"
    
    # Save final JSON state
    storage_client.write_json(run_metadata, state_file)
    
    # Append to BQ
    try:
        # Avoid nested structures because we didn't specify RECORD in BQ schema
        clean_row = {
            k: v for k, v in run_metadata.items()
            if v is not None
        }
        bq_run_table_id = f"{project_id}.{os.getenv('BQ_DATASET_GOLD', 'skillpulse_gold')}.pipeline_runs"
        errors = client.insert_rows_json(bq_run_table_id, [clean_row])
        if not errors:
            logger.info("✅ Successfully logged run to pipeline_runs table.")
        else:
            logger.error(f"Failed to log run: {errors}")
    except Exception as e:
        logger.error(f"Error inserting pipeline_runs to BQ: {e}")


if __name__ == "__main__":
    load_silver_to_bq()
