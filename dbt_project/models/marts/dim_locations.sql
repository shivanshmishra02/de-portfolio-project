{{ config(
    materialized='table'
) }}

with stg_jobs as (
    select * from {{ ref('stg_job_postings') }}
)

select distinct
    {{ dbt_utils.generate_surrogate_key(['job_city', 'job_state', 'job_country']) }} as location_key,
    job_city as city,
    job_state as state,
    job_country as country,
    min(enriched_at) as first_seen_at
from stg_jobs
group by job_city, job_state, job_country
