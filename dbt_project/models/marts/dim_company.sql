{{ config(
    materialized='table'
) }}

with stg_jobs as (
    select * from {{ ref('stg_job_postings') }}
)

select
    -- Surrogate key on company_name to handle changing sectors over time but keep name grain
    {{ dbt_utils.generate_surrogate_key(['company_name']) }} as company_key,
    company_name,
    max(company_sector) as company_sector, -- SCD Type 1 behavior (latest/most frequent depending on logic, max gets latest alphabetical or non-null in some engines; window function safer but for now this acts as simple latest distinct pick if we assume it doesn't change often)
    min(enriched_at) as first_seen_date,
    max(enriched_at) as last_seen_date,
    count(distinct coalesce(source_job_id, job_id)) as total_jobs_posted
from stg_jobs
where company_name is not null
group by company_name
