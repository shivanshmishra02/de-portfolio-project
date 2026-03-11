{{ config(
    materialized='table'
) }}

with stg as (
    select * from {{ ref('stg_job_postings') }}
),
exploded_skills as (
    select 
        coalesce(source_job_id, job_id) as job_id,
        trim(replace(cast(skill as string), '"', '')) as skill_name, 
        enriched_at
    from stg,
    unnest(skills) as skill
    where skill is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['skill_name']) }} as skill_id,
    skill_name,
    min(enriched_at) as first_seen_date,
    count(distinct job_id) as total_demand_count
from exploded_skills
where skill_name != ''
group by skill_name
