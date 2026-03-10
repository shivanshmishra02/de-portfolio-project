import os
import logging
from google.cloud import bigquery
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_bq_views():
    load_dotenv()
    
    project_id = os.getenv("BQ_PROJECT_ID")
    dataset_gold = os.getenv("BQ_DATASET_GOLD", "skillpulse_gold")
    
    if not project_id:
        logger.error("BQ_PROJECT_ID is missing from .env")
        return
        
    client = bigquery.Client(project=project_id)
    
    # Define the 3 Views for Looker Studio
    views = {
        "vw_skill_demand": f"""
            SELECT 
                s.skill_name, 
                COUNT(s.job_skill_key) AS demand_count,
                MIN(s.dbt_valid_from) AS first_seen,
                MAX(s.dbt_valid_from) AS last_seen
            FROM `{project_id}.{dataset_gold}.dim_skills` s
            WHERE s.dbt_valid_to IS NULL
            GROUP BY s.skill_name
            ORDER BY demand_count DESC
            LIMIT 50
        """,
        "vw_salary_by_role": f"""
            SELECT 
                r.role_category,
                AVG(f.salary_min_lpa) AS avg_min_salary_lpa,
                AVG(f.salary_max_lpa) AS avg_max_salary_lpa,
                COUNT(f.job_id) AS total_jobs
            FROM `{project_id}.{dataset_gold}.fact_job_postings` f
            JOIN `{project_id}.{dataset_gold}.dim_roles` r ON f.role_key = r.role_key
            GROUP BY r.role_category
            ORDER BY total_jobs DESC
        """,
        "vw_jobs_by_city": f"""
            WITH CitySkills AS (
                SELECT 
                    l.location_key,
                    l.city,
                    l.state,
                    s.skill_name,
                    COUNT(s.job_skill_key) AS skill_count
                FROM `{project_id}.{dataset_gold}.fact_job_postings` f
                JOIN `{project_id}.{dataset_gold}.dim_locations` l ON f.location_key = l.location_key
                JOIN `{project_id}.{dataset_gold}.dim_skills` s ON f.job_id = s.job_id
                WHERE s.dbt_valid_to IS NULL
                GROUP BY 1, 2, 3, 4
            ),
            RankedSkills AS (
                SELECT 
                    location_key,
                    city,
                    state,
                    skill_name,
                    ROW_NUMBER() OVER(PARTITION BY location_key ORDER BY skill_count DESC) as rnk
                FROM CitySkills
            )
            SELECT 
                l.city,
                l.state,
                COUNT(DISTINCT f.job_id) AS job_count,
                MAX(rs.skill_name) AS top_skill
            FROM `{project_id}.{dataset_gold}.fact_job_postings` f
            JOIN `{project_id}.{dataset_gold}.dim_locations` l ON f.location_key = l.location_key
            LEFT JOIN RankedSkills rs ON l.location_key = rs.location_key AND rs.rnk = 1
            GROUP BY l.city, l.state
            ORDER BY job_count DESC
        """
    }
    
    for view_name, query in views.items():
        view_id = f"{project_id}.{dataset_gold}.{view_name}"
        view = bigquery.Table(view_id)
        view.view_query = query
        
        try:
            # Delete if exists to recreate
            client.delete_table(view_id, not_found_ok=True)
            view = client.create_table(view)
            logger.info(f"Successfully created view: {view_id}")
        except Exception as e:
            logger.error(f"Failed to create view {view_id}: {e}")

if __name__ == "__main__":
    create_bq_views()
