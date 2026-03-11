def normalize_skill(skill: str) -> str:
    """
    Normalizes a skill name to its canonical form using a predefined map.
    Returns the canonical form if found, otherwise falls back to stripped title case.
    """
    if not skill or not isinstance(skill, str):
        return ""
        
    canonical_map = {
        "sql": "SQL",
        "etl": "ETL",
        "etl pipeline": "ETL",
        "pyspark": "PySpark",
        "py spark": "PySpark",
        "power bi": "Power BI",
        "powerbi": "Power BI",
        "ms power bi": "Power BI",
        "adf": "Azure Data Factory",
        "azure data factory": "Azure Data Factory",
        "azure databricks": "Azure Databricks",
        "databricks": "Databricks",
        "ci/cd": "CI/CD",
        "cicd": "CI/CD",
        "ml": "Machine Learning",
        "nlp": "NLP",
        "natural language processing": "NLP",
        "aws": "AWS",
        "gcp": "GCP",
        "google cloud": "GCP",
        "ms sql": "SQL Server",
        "sql server": "SQL Server",
        "mssql": "SQL Server",
        "snowflake": "Snowflake",
        "dbt": "dbt",
        "airflow": "Airflow",
        "apache airflow": "Airflow",
        "apache spark": "Spark",
        "spark": "Spark",
        "scala": "Scala",
        "kafka": "Kafka",
        "apache kafka": "Kafka",
        "delta lake": "Delta Lake",
        "data warehousing": "Data Warehousing",
        "data modeling": "Data Modeling",
        "data modelling": "Data Modeling"
    }
    
    clean_skill = skill.strip().lower()
    
    if clean_skill in canonical_map:
        return canonical_map[clean_skill]
        
    return skill.strip().title()
