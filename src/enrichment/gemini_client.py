import os
import time
import json
import logging
from typing import Dict, Any, Optional
from google import genai
from google.genai.errors import APIError

logger = logging.getLogger(__name__)

SKILL_EXTRACTION_PROMPT = """
Analyze this job description and return ONLY valid JSON with these fields:

{{
  "skills": ["skill1", "skill2"],
  "seniority_level": "Junior|Mid|Senior|Lead|Principal",
  "tech_stack_category": "Data Platform|MLOps|BI & Analytics|Data Science|Cloud Infrastructure|Other",
  "work_mode_override": "Remote|Hybrid|Onsite|null",
  "salary_min_lpa": <number or null>,
  "salary_max_lpa": <number or null>
}}

Rules:
- work_mode_override: only fill if you can clearly determine from JD text, else null
- salary: extract LPA figures from JD text if mentioned, else null
- Return ONLY the JSON object, no markdown, no explanation

JOB POSTING TO ANALYZE:
---
{job_description}
---
"""

class GeminiEnrichmentClient:
    """
    Client for interacting with Gemini 2.5 Pro API using the google-genai library
    Extracts structured entities from job descriptions.
    """
    
    def __init__(self):
        self.api_key = os.getenv("GEMINI_API_KEY")
        self.model_name = os.getenv("GEMINI_MODEL", "gemini-2.5-pro")
        self.max_retries = int(os.getenv("GEMINI_MAX_RETRIES", "3"))
        
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY environment variable is required.")
            
        # Initialize the new google-genai client
        self.client = genai.Client(api_key=self.api_key)
        
    def extract_job_entities(self, job_description: str) -> Optional[Dict[str, Any]]:
        """
        Sends a single job description to Gemini and attempts to parse the returned JSON.
        Implements exponential backoff for rate limits.
        """
        if not job_description or not str(job_description).strip():
            logger.warning("Empty job description provided. Skipping enrichment.")
            return None
            
        formatted_prompt = SKILL_EXTRACTION_PROMPT.format(job_description=job_description)
        
        for attempt in range(1, self.max_retries + 1):
            response_text = ""
            try:
                # Use generate_content from google-genai
                response = self.client.models.generate_content(
                    model=self.model_name,
                    contents=formatted_prompt,
                    # We can use generation_config to attempt to enforce JSON, though prompt instruction is primary
                    config=genai.types.GenerateContentConfig(
                        temperature=0.1 # Low temperature for consistent JSON structured data extraction
                    )
                )
                
                response_text = response.text
                if not response_text:
                    logger.warning("Gemini returned an empty response.")
                    return None
                    
                # Clean up potential markdown formatting that Gemini might sporadically include
                # despite prompt rules
                cleaned_text = response_text.strip()
                if cleaned_text.startswith("```json"):
                    cleaned_text = cleaned_text[7:]
                if cleaned_text.startswith("```"):
                    cleaned_text = cleaned_text[3:]
                if cleaned_text.endswith("```"):
                    cleaned_text = cleaned_text[:-3]
                    
                cleaned_text = cleaned_text.strip()
                
                # Parse to JSON
                return json.loads(cleaned_text)
                
            except APIError as e:
                error_msg = str(e)
                # Handle Rate Limit / Quota Errors specifically
                if "429" in error_msg or "quota" in error_msg.lower():
                    logger.warning(f"Rate limit exceeded (Attempt {attempt}/{self.max_retries}): {error_msg}")
                    if attempt < self.max_retries:
                        backoff = (2 ** attempt) * int(os.getenv("GEMINI_RETRY_DELAY_SECONDS", "5"))
                        logger.info(f"Retrying in {backoff} seconds...")
                        time.sleep(backoff)
                    else:
                        logger.error("Max retries reached handling Gemini API rate limits.")
                        return None
                else:
                    logger.error(f"Gemini API Error: {error_msg}")
                    return None
                    
            except json.JSONDecodeError as e:
                # Requirement: Log failure and continue if invalid JSON returned
                logger.error(f"Failed to parse Gemini response as JSON: {e}")
                logger.debug(f"Raw Gemini response was: {response_text}")
                return None
                
            except Exception as e:
                logger.error(f"Unexpected error during enrichment: {str(e)}")
                return None
                
        return None
