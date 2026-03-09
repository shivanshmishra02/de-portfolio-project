import os
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()
client = bigquery.Client(project=os.getenv('BQ_PROJECT_ID'))
dataset_id = f"{os.getenv('BQ_PROJECT_ID')}.{os.getenv('BQ_DATASET_GOLD', 'skillpulse_gold')}"
try:
    client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
    print(f"Deleted incorrect dataset {dataset_id}")
except Exception as e:
    print(f"Skipping deletion: {e}")
