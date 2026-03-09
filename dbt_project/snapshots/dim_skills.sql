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
-- BigQuery treats our loaded python lists as REPEATED fields so we UNNEST
exploded_required as (
    select 
        job_id, 
        cast(skill as string) as skill_name, 
        'required' as skill_type,
        enriched_at,
        pipeline_run_id,
        source_system
    from stg,
    unnest(ai_skills_required) as skill
),
exploded_preferred as (
    select 
        job_id, 
        cast(skill as string) as skill_name, 
        'preferred' as skill_type,
        enriched_at,
        pipeline_run_id,
        source_system
    from stg,
    unnest(ai_skills_preferred) as skill
),
combined as (
    select * from exploded_required
    union all
    select * from exploded_preferred
)

select distinct
    {{ dbt_utils.generate_surrogate_key(['job_id', 'skill_name', 'skill_type']) }} as job_skill_key,
    job_id,
    skill_name,
    skill_type,
    enriched_at as created_at,
    pipeline_run_id,
    source_system
from combined
where skill_name is not null

{% endsnapshot %}
