{{ config(
    materialized='table'
) }}

with stg_jobs as (
    select * from {{ ref('stg_job_postings') }}
)

select
    -- Core fact grain
    coalesce(source_job_id, job_id) as job_id,
    
    -- Surrogate Keys mapping to dimensions
    {{ dbt_utils.generate_surrogate_key(['company_name']) }} as company_key,
    {{ dbt_utils.generate_surrogate_key(['city', 'state', 'country']) }} as location_key,
    {{ dbt_utils.generate_surrogate_key(['job_title', 'seniority_level', 'tech_stack_category']) }} as role_key,
    cast(format_date('%Y%m%d', date(enriched_at)) as int64) as date_key,
    
    -- Denormalized for query speed
    seniority_level,
    work_mode,
    experience_years,
    
    -- Facts / Continuous Metrics
    salary_min_lpa,
    salary_max_lpa,
    
    -- Extracted Complex Arrays (Pass through)
    skills as skills_array,
    
    -- Medallion Audit fields
    enriched_at as created_at,
    pipeline_run_id,
    source_api,
    enrichment_confidence

from stg_jobs
where (source_job_id is not null or job_id is not null)
