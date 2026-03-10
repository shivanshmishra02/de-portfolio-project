{{ config(
    materialized='table'
) }}

with facts as (
    select * from {{ ref('fact_job_postings') }}
),
roles as (
    select * from {{ ref('dim_roles') }}
),
base_counts as (
    select
        f.work_mode,
        r.tech_stack_category,
        count(distinct f.job_id) as job_count
    from facts f
    left join roles r on f.role_key = r.role_key
    where f.work_mode is not null
      and r.tech_stack_category is not null
    group by
        f.work_mode,
        r.tech_stack_category
),
category_totals as (
    select
        tech_stack_category,
        sum(job_count) as total_category_jobs
    from base_counts
    group by tech_stack_category
)

select
    b.work_mode,
    b.tech_stack_category,
    b.job_count,
    cast(b.job_count as float64) / nullif(t.total_category_jobs, 0) as pct_of_total
from base_counts b
join category_totals t on b.tech_stack_category = t.tech_stack_category
order by 
    b.tech_stack_category, 
    b.job_count desc
