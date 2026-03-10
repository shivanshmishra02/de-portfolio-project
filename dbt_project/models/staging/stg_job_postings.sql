{{ config(
    materialized='view'
) }}

with source as (
    select *
    from {{ source('skillpulse_silver', 'stg_job_postings') }}
)

select
    -- Primary Identifiers
    cast(job_id as string) as job_id, -- Legacy
    cast(source_job_id as string) as source_job_id,
    
    -- Dimensions mappings payload
    cast(company_name as string) as company_name,
    cast(company_sector as string) as company_sector,
    cast(job_title as string) as job_title,
    cast(city as string) as city,
    cast(state as string) as state,
    cast(country as string) as country,
    cast(employment_type as string) as employment_type,
    cast(posted_at as string) as posted_at,
    cast(expires_at as string) as expires_at,
    
    -- Unified parsing overrides
    cast(work_mode as string) as work_mode,
    cast(degree_requirement as string) as degree_requirement,
    cast(experience_years as float64) as experience_years,
    
    -- AI Enriched Fields
    cast(tech_stack_category as string) as tech_stack_category,
    cast(seniority_level as string) as seniority_level,
    cast(salary_min_lpa as float64) as salary_min_lpa,
    cast(salary_max_lpa as float64) as salary_max_lpa,
    
    -- Extracted Complex Arrays (Auto-detected schema passes these as string representation for now)
    skills,
    
    -- Medallion Audit Columns
    cast(audit_enriched_at as timestamp) as enriched_at,
    cast(audit_pipeline_run_id as string) as pipeline_run_id,
    cast(source_api as string) as source_api,
    cast(audit_enrichment_confidence as string) as enrichment_confidence

from source
where
    (source_job_id is not null or job_id is not null)

