{{ config(
    materialized='table'
) }}

with stg_jobs as (
    select * from {{ ref('stg_job_postings') }}
)

select
    -- Core fact grain
    job_id,
    
    -- Surrogate Keys mapping to dimensions
    {{ dbt_utils.generate_surrogate_key(['employer_name']) }} as company_key,
    {{ dbt_utils.generate_surrogate_key(['job_city', 'job_state', 'job_country']) }} as location_key,
    {{ dbt_utils.generate_surrogate_key(['job_title', 'seniority_level', 'role_category']) }} as role_key,
    cast(format_date('%Y%m%d', date(enriched_at)) as int64) as date_key,
    
    -- Facts / Continuous Metrics
    salary_min_lpa,
    salary_max_lpa,
    experience_years_min,
    experience_years_max,
    
    -- Medallion Audit fields
    enriched_at as created_at,
    pipeline_run_id,
    source_system,
    enrichment_confidence

from stg_jobs
where job_id is not null
