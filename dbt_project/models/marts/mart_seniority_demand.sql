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
)

select
    r.seniority_level,
    l.city,
    r.tech_stack_category,
    count(distinct f.job_id) as job_count
from facts f
left join locations l on f.location_key = l.location_key
left join roles r on f.role_key = r.role_key
where l.city is not null 
  and r.seniority_level is not null
  and r.tech_stack_category is not null
group by
    r.seniority_level,
    l.city,
    r.tech_stack_category
order by job_count desc
