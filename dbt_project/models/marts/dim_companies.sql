{{ config(
    materialized='table'
) }}

with stg_jobs as (
    select * from {{ ref('stg_job_postings') }}
)

select distinct
    {{ dbt_utils.generate_surrogate_key(['employer_name']) }} as company_key,
    employer_name as company_name,
    min(enriched_at) as first_seen_at
from stg_jobs
where employer_name is not null
group by employer_name
