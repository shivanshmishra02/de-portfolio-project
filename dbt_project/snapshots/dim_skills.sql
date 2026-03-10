{% snapshot dim_skills %}

{{
    config(
      target_schema=env_var('BQ_DATASET_GOLD', 'skillpulse_gold'),
      unique_key='job_skill_key',
      strategy='timestamp',
      updated_at='created_at'
    )
}}

with stg as (
    select * from {{ ref('stg_job_postings') }}
),
-- Unnest the JSON array of skills
exploded_skills as (
    select 
        job_id, 
        trim(replace(replace(cast(skill as string), '"', ''), "'", "")) as skill_name, 
        'required' as skill_type,
        enriched_at,
        pipeline_run_id,
        source_api
    from stg,
    unnest(skills) as skill
)

select distinct
    {{ dbt_utils.generate_surrogate_key(['job_id', 'skill_name', 'skill_type']) }} as job_skill_key,
    job_id,
    skill_name,
    skill_type,
    enriched_at as created_at,
    pipeline_run_id,
    source_api
from exploded_skills
where skill_name is not null and skill_name != ''

{% endsnapshot %}
