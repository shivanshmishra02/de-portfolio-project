import os
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()
client = bigquery.Client(project=os.getenv('BQ_PROJECT_ID'))
dataset_id = f"{os.getenv('BQ_PROJECT_ID')}.{os.getenv('BQ_DATASET_GOLD', 'skillpulse_gold')}"
dataset = bigquery.Dataset(dataset_id)
dataset.location = 'US'
client.create_dataset(dataset, exists_ok=True)
print(f"Verified dataset {dataset_id} exists in location US.")
