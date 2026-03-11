{{ config(
    materialized='table'
) }}

with stg_jobs as (
    select * from {{ ref('stg_job_postings') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['job_title', 'seniority_level', 'tech_stack_category']) }} as role_id,
    job_title as role_name,
    seniority_level,
    tech_stack_category,
    min(enriched_at) as first_seen_at
from stg_jobs
where job_title is not null
group by job_title, seniority_level, tech_stack_category
