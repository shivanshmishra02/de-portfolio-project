# SkillPulse India

**AI-Powered Job Market Intelligence Pipeline**

An end-to-end data engineering pipeline that ingests raw job postings from Indian job boards via API daily, uses Gemini AI to extract structured entities (skills, salary, role type, experience) from unstructured job descriptions, applies Medallion Architecture (Bronze → Silver → Gold), stores analytical data in BigQuery using a star schema, and serves insights via a Looker Studio dashboard.

## Setup Instructions

### 1. Prerequisites
- Python 3.9+
- Google Cloud SDK (gcloud CLI)
- A RapidAPI account for JSearch
- A Google Gemini API Key
- BigQuery Sandbox project initialized

### 2. Environment Variables
Create a `.env` file in the root directory following the parameters described in `test_keys.py` / `.env.example`.

### 3. Google Cloud Authentication
**SECURITY NOTICE: Do NOT create or use Service Account JSON Key files.**

This project uses Google Application Default Credentials (ADC) for secure local authentication to BigQuery.

To set this up, install the [Google Cloud CLI](https://cloud.google.com/sdk/docs/install) and run the following command in your terminal:
```bash
gcloud auth application-default login
```
This will open a browser window to authenticate with your Google account and locally cache the necessary credentials for the `google-cloud-bigquery` and `dbt-bigquery` adapters to securely connect.

## Streamlit Dashboard setup
1. Set up your Google Application Default Credentials as described above.
2. Install the necessary dashboard requirements: `pip install streamlit pandas plotly google-cloud-bigquery`
3. Launch the dashboard locally:
```bash
streamlit run src/dashboard/app.py
```

### Dashboard Features
The SkillPulse Streamlit application serves intelligence across 5 key dimensions:
- **Skill Demand:** Top 20 skills and market trends
- **Salary by Role:** Compensation spread and expectations
- **Geographic Intelligence:** City-wise job volume and top skills
- **Company Intelligence:** Leading hiring organizations and remote work adoption
- **Role Intelligence:** Demand distribution across seniority levels and tech stacks

## Looker Dashboard Setup (Deprecated)
For instructions on the legacy Looker Studio integration, see `DASHBOARD_SETUP.md`.
