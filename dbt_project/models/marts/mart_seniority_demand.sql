{{ config(
    materialized='table'
) }}

with facts as (
    select * from {{ ref('fact_job_postings') }}
),
locations as (
    select * from {{ ref('dim_locations') }}
),
roles as (
    select * from {{ ref('dim_roles') }}
),
dim_job as (
    select * from {{ ref('dim_job') }}
)

select
    r.seniority_level,
    l.city,
    r.tech_stack_category,
    count(distinct f.job_id) as job_count
from facts f
left join locations l on f.location_key = l.location_key
left join roles r on f.role_key = r.role_key
left join dim_job dj on f.job_id = dj.source_job_id
where l.city is not null 
  and r.seniority_level is not null
  and r.tech_stack_category is not null
  and dj.is_active = true
group by
    r.seniority_level,
    l.city,
    r.tech_stack_category
order by job_count desc
