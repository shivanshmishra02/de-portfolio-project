{{ config(
    materialized='table'
) }}

with facts as (
    select job_id, skills_array from {{ ref('fact_job_postings') }}
),
unnested_skills as (
    select 
        f.job_id,
        trim(replace(replace(cast(skill_json as string), '"', ''), '''', '')) as skill
    from facts f,
    unnest(json_query_array(f.skills_array)) as skill_json
),
clean_skills as (
    select distinct job_id, skill
    from unnested_skills
    where skill is not null and skill != ''
),
top_skills as (
    -- Limit to top 20 skills globally to keep the matrix manageable
    select skill, count(*) as freq
    from clean_skills
    group by skill
    order by freq desc
    limit 20
),
filtered_skills as (
    select c.job_id, c.skill
    from clean_skills c
    inner join top_skills t on c.skill = t.skill
)

select
    s1.skill as skill_a,
    s2.skill as skill_b,
    count(distinct s1.job_id) as co_occurrence_count
from filtered_skills s1
join filtered_skills s2 
  on s1.job_id = s2.job_id 
  and s1.skill < s2.skill -- Ensure combination uniqueness and prevent identical self-joins
group by
    s1.skill,
    s2.skill
order by 
    co_occurrence_count desc
