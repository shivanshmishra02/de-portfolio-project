{{ config(
    materialized='view'
) }}

with source as (
    select *
    from {{ source('skillpulse_silver', 'stg_job_postings') }}
)

select
    -- Primary Identifiers
    cast(job_id as string) as job_id,
    
    -- Dimensions mappings payload
    cast(employer_name as string) as employer_name,
    cast(job_title as string) as job_title,
    cast(job_city as string) as job_city,
    cast(job_state as string) as job_state,
    cast(job_country as string) as job_country,
    cast(job_is_remote as boolean) as job_is_remote,
    
    -- AI Enriched Fields
    cast(ai_role_category as string) as role_category,
    cast(ai_seniority_level as string) as seniority_level,
    cast(ai_work_mode as string) as work_mode,
    cast(ai_salary_min_lpa as float64) as salary_min_lpa,
    cast(ai_salary_max_lpa as float64) as salary_max_lpa,
    cast(ai_experience_years_min as int64) as experience_years_min,
    cast(ai_experience_years_max as int64) as experience_years_max,
    cast(ai_education_required as string) as education_required,
    
    -- Extracted Complex Arrays (We parse JSON string array directly in BigQuery if bigquery stored it as array of strings, but since it auto-detected, we just select it to be unnested in dimension step)
    ai_skills_required,
    ai_skills_preferred,
    
    -- Medallion Audit Columns
    cast(audit_enriched_at as timestamp) as enriched_at,
    cast(audit_pipeline_run_id as string) as pipeline_run_id,
    cast(medallion_audit.source_system as string) as source_system,
    cast(audit_enrichment_confidence as string) as enrichment_confidence

from source
where
    job_id is not null
    and audit_enrichment_confidence is not null
