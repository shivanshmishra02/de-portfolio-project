{{ config(
    materialized='table'
) }}

with stg_jobs as (
    select * from {{ ref('stg_job_postings') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['source_job_id', 'job_id']) }} as job_key,
    coalesce(source_job_id, job_id) as source_job_id,
    job_title,
    employment_type,
    experience_years,
    degree_requirement,
    work_mode,
    posted_at,
    expires_at,
    -- is_active logic: expires_at > current_date OR expires_at IS NULL
    -- using Safe_Cast as Date here since expires_at is iso date str
    case 
        when expires_at is not null and safe_cast(substring(expires_at, 1, 10) as date) < current_date() then false
        when safe_cast(substring(posted_at, 1, 10) as date) < date_sub(current_date(), interval 60 day) then false
        else true
    end as is_active,
    min(enriched_at) as created_at
from stg_jobs
where (stg_jobs.source_job_id is not null or stg_jobs.job_id is not null)
group by
    stg_jobs.source_job_id,
    stg_jobs.job_id,
    job_title,
    employment_type,
    experience_years,
    degree_requirement,
    work_mode,
    posted_at,
    expires_at
