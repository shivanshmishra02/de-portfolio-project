{{ config(
    materialized='table'
) }}

with stg as (
    select * from {{ ref('stg_job_postings') }}
),
dim_job as (
    select * from {{ ref('dim_job') }}
),
exploded_skills as (
    select 
        coalesce(source_job_id, job_id) as job_id,
        trim(replace(cast(skill as string), '"', '')) as skill_name
    from stg,
    unnest(skills) as skill
    where skill is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['e.job_id', 'e.skill_name']) }} as job_skill_key,
    e.job_id,
    {{ dbt_utils.generate_surrogate_key(['e.skill_name']) }} as skill_id,
    j.is_active
from exploded_skills e
join dim_job j on e.job_id = j.source_job_id
where e.skill_name != ''
