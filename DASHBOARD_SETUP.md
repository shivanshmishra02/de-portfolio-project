# Looker Studio Dashboard Setup (Part C)

This guide walks you through connecting your SkillPulse `skillpulse_gold` BigQuery dataset to Google Looker Studio to visualize job market intelligence.

## Prerequisites
1. Ensure `python -m src.utils.bq_views` has been run successfully. This generates 3 aggregated views:
   - `skillpulse_gold.vw_skill_demand`
   - `skillpulse_gold.vw_salary_by_role`
   - `skillpulse_gold.vw_jobs_by_city`
2. Go to [Looker Studio](https://lookerstudio.google.com/) and log in with the Google account tied to your `skillpulse-india` GCP Project.

## View 1: Skill Pulse (Demand & Trends)
1. **Create** a new blank report in Looker Studio.
2. **Add Data** -> Select **BigQuery**.
3. Authorize Looker Studio to access your GCP project.
4. Navigate to `skillpulse-india` -> `skillpulse_gold` -> `vw_skill_demand` and click **Add**.

**Charts to Build:**
- **Top 20 Skills (Bar Chart):**
  - **Dimension:** `skill_name`
  - **Metric:** `demand_count`
  - *Sort by `demand_count` Descending.*
  - *Limit rows to 20 under the Style tab.*
- **Rising vs Declining (Table with Heatmap):**
  - **Dimension:** `skill_name`
  - **Metrics:** `demand_count`, `first_seen`, `last_seen`.
  - *This leverages the SCD Type 2 `valid_from` timestamps to show when skills first appeared in the dataset and when they were last actively required.*

## View 2: Role Deep Dive (Salaries & Work Modes)
1. Click **Add Data** from the top menu.
2. Navigate to BigQuery -> `skillpulse-india` -> `skillpulse_gold` -> `vw_salary_by_role` -> **Add**.

**Charts to Build:**
- **Salary Range by Role Category (Column Chart):**
  - **Dimension:** `role_category`
  - **Metrics:** `avg_min_salary_lpa` and `avg_max_salary_lpa` (Side-by-side columns).
  - *Add an optional breakdown dimension for `seniority_level` to stack seniority bands.*
- **Remote vs Hybrid Split (Pie or Donut Chart):**
  - **Dimension:** `work_mode`
  - **Metric:** `total_jobs`

## View 3: City Intelligence
1. Click **Add Data** from the top menu.
2. Navigate to BigQuery -> `skillpulse-india` -> `skillpulse_gold` -> `vw_jobs_by_city` -> **Add**.

**Charts to Build:**
- **Job Volume by City (Geo Map / Bubble Map):**
  - **Location Dimension:** `city`
  - **Tooltip/Size Metric:** `job_count`
  - *Make sure Looker Studio recognizes `city` as a Geographic text field.*
- **Top Skill per City (Table):**
  - **Dimension:** `city`, `state`, `top_skill`
  - **Metric:** `job_count`
  - *Sort by `job_count` Descending to see what the hottest skill is in the largest tech hubs.*

## Finishing Touches
- Add a **Date Range Control** to the top of the dashboard. Both BigQuery models currently aggregate everything, but as you hook it up to scheduled Airflow runs, you can filter `vw_skill_demand` by the ingestion dates!
- Share the report link and paste it into the `README.md`!
