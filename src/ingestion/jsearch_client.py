import os
import time
import logging
import requests
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class JSearchClient:
    """
    Client for interacting with the JSearch API (RapidAPI) to fetch job postings.
    Includes built-in retry logic and error handling.
    """
    
    def __init__(self):
        self.api_key = os.getenv("JSEARCH_API_KEY")
        self.api_host = os.getenv("JSEARCH_API_HOST", "jsearch.p.rapidapi.com")
        self.base_url = os.getenv("JSEARCH_BASE_URL", "https://jsearch.p.rapidapi.com/search")
        
        if not self.api_key:
            raise ValueError("JSEARCH_API_KEY environment variable is required but not set.")
            
        self.headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": self.api_host
        }

    def fetch_jobs(self, query: str, page: int = 1, num_pages: int = 1, max_retries: int = 3) -> Optional[Dict[str, Any]]:
        """
        Fetches job postings from JSearch API for a given query.
        
        Args:
            query (str): Job search query (e.g., 'Data Engineer in India')
            page (int): Page number to start fetching
            num_pages (int): Number of pages to fetch per query
            max_retries (int): Maximum number of retry attempts on failure
            
        Returns:
            Optional[Dict[str, Any]]: API response JSON or None if request fails
        """
        params = {
            "query": query,
            "page": str(page),
            "num_pages": str(num_pages),
            "country": os.getenv("JSEARCH_TARGET_COUNTRY", "IN"),
            "date_posted": "today" # To get daily batch updates
        }
        
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Fetching jobs from JSearch (Attempt {attempt}/{max_retries}). Query: '{query}'")
                response = requests.get(
                    self.base_url, 
                    headers=self.headers, 
                    params=params,
                    timeout=30
                )
                
                # Raise HTTPError for bad responses (4xx or 5xx)
                response.raise_for_status()
                
                data = response.json()
                logger.info(f"Successfully fetched {len(data.get('data', []))} jobs.")
                return data
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed: {str(e)}")
                if attempt < max_retries:
                    backoff = 2 ** attempt  # Exponential backoff (2s, 4s, 8s)
                    logger.info(f"Retrying in {backoff} seconds...")
                    time.sleep(backoff)
                else:
                    logger.error("Max retries reached. API call failed.")
                    
        return None
