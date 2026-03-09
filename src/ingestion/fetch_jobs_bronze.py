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

def main():
    # Load env variables from .env
    load_dotenv()
    
    # Configuration
    bronze_path = os.getenv("BRONZE_PATH", "./data/bronze")
    run_date = os.getenv("PIPELINE_RUN_DATE", datetime.now().strftime("%Y-%m-%d"))
    pipeline_run_id = str(uuid.uuid4())
    source_system = "jsearch_api"
    
    # Ensure bronze directory exists
    os.makedirs(bronze_path, exist_ok=True)
    
    # Initialize client
    client = JSearchClient()
    
    # Define queries to search for
    queries = [
        "Data Engineer in India",
        "Data Analyst in India",
        # "Analytics Engineer in India"
    ]
    
    all_jobs = []
    
    for query in queries:
        logger.info(f"Processing query: '{query}'")
        
        # In a real scenario we'd respect JSEARCH_RESULTS_PER_PAGE and 
        # potentially paginate, but for now we fetch the first page
        response_data = client.fetch_jobs(query, page=1, num_pages=1)
        
        if response_data and 'data' in response_data:
            jobs = response_data['data']
            logger.info(f"Retrieved {len(jobs)} jobs for query '{query}'")
            
            # Enrich each job posting with Medallion audit columns
            for job in jobs:
                job['medallion_audit'] = {
                    'created_at': datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    'pipeline_run_id': pipeline_run_id,
                    'source_system': source_system,
                    'ingestion_date': run_date,
                    'search_query': query
                }
            
            all_jobs.extend(jobs)
        else:
            logger.warning(f"No valid data returned for query '{query}'")
            
    if all_jobs:
        # Save to bronze layer as raw JSON
        # Format filename: source_system_YYYYMMDD_runID.json
        output_filename = f"{source_system}_{run_date.replace('-', '')}_{pipeline_run_id[:8]}.json"
        output_filepath = os.path.join(bronze_path, output_filename)
        
        with open(output_filepath, 'w', encoding='utf-8') as f:
            json.dump(all_jobs, f, ensure_ascii=False, indent=2)
            
        logger.info(f"Successfully saved {len(all_jobs)} total jobs to {output_filepath}")
    else:
        logger.warning("No jobs were fetched across all queries. Nothing to save to bronze.")

if __name__ == "__main__":
    main()
