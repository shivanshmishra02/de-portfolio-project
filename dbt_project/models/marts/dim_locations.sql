{{ config(
    materialized='table'
) }}

with stg as (
    select * from {{ ref('stg_job_postings') }}
)

select distinct
    {{ dbt_utils.generate_surrogate_key(['city', 'state', 'country']) }} as location_key,
    city,
    state,
    country
from stg
where city is not null
