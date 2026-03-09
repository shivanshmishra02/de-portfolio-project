{{ config(
    materialized='table'
) }}

with stg_jobs as (
    select * from {{ ref('stg_job_postings') }}
),
distinct_dates as (
    select distinct date(enriched_at) as date_day
    from stg_jobs
    where enriched_at is not null
)

select
    cast(format_date('%Y%m%d', date_day) as int64) as date_key,
    date_day,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(day from date_day) as day,
    extract(dayofweek from date_day) as day_of_week
from distinct_dates
