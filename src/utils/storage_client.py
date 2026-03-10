import os
import json
import logging
from google.cloud import storage

logger = logging.getLogger(__name__)

class StorageClient:
    """
    A unified storage client that abstracts away local vs Google Cloud Storage (GCS).
    Behavior is toggled via the STORAGE_MODE environment variable.
    """
    def __init__(self):
        self.mode = os.getenv("STORAGE_MODE", "local").lower()
        self.bucket_name = os.getenv("GCS_BUCKET_NAME")
        
        if self.mode == "gcs":
            # Uses Application Default Credentials
            gcp_project = os.getenv("GCP_PROJECT_ID", "skillpulse-india")
            self.gcs_client = storage.Client(project=gcp_project)
            self.bucket = self.gcs_client.bucket(self.bucket_name)

    def write_json(self, data, path: str):
        """Writes JSON dict to the specified path (either local or GCS)"""
        if self.mode == "local":
            # Ensure local directory exists
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info(f"Successfully wrote local file: {path}")
        elif self.mode == "gcs":
            # For GCS, 'path' is treated as the blob prefix/name
            # E.g., data/bronze/file.json
            path = path.replace("\\", "/").lstrip("./").lstrip("/")
            blob = self.bucket.blob(path)
            blob.upload_from_string(
                data=json.dumps(data, indent=2, ensure_ascii=False),
                content_type="application/json"
            )
            logger.info(f"Successfully wrote GCS file: gs://{self.bucket_name}/{path}")
        else:
            raise ValueError(f"Unsupported STORAGE_MODE: {self.mode}")

    def read_json(self, path: str):
        """Reads JSON dict from the specified path"""
        if self.mode == "local":
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        elif self.mode == "gcs":
            path = path.replace("\\", "/").lstrip("./").lstrip("/")
            blob = self.bucket.blob(path)
            content = blob.download_as_string()
            return json.loads(content)
        else:
            raise ValueError(f"Unsupported STORAGE_MODE: {self.mode}")

    def list_files(self, directory_path: str, suffix=".json"):
        """Lists files with the given suffix in the directory"""
        files = []
        if self.mode == "local":
            if not os.path.exists(directory_path):
                return []
            for root, _, filenames in os.walk(directory_path):
                for filename in filenames:
                    if filename.endswith(suffix):
                        # Keep the absolute/relative structure coherent
                        files.append(os.path.join(root, filename).replace("\\", "/"))
        elif self.mode == "gcs":
            prefix = directory_path.replace("\\", "/").lstrip("./").lstrip("/")
            if not prefix.endswith("/"):
                prefix += "/"
            blobs = self.bucket.list_blobs(prefix=prefix)
            for blob in blobs:
                if blob.name.endswith(suffix):
                    files.append(blob.name)
        return files

    def file_exists(self, path: str) -> bool:
        """Checks if a specific file exists"""
        if self.mode == "local":
            return os.path.exists(path)
        elif self.mode == "gcs":
            path = path.replace("\\", "/").lstrip("./").lstrip("/")
            blob = self.bucket.blob(path)
            return blob.exists()
        return False
