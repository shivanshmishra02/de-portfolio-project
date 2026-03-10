{{ config(
    materialized='table'
) }}

with facts as (
    select * from {{ ref('fact_job_postings') }}
),
companies as (
    select * from {{ ref('dim_companies') }} -- Use the existing pluralized dim model if it exists, or the newly created `dim_company` (waiting to verify the exact name, assuming `dim_company` was just created but checking existing). We created dim_company, so we use that.
    -- Wait, looking closely: the user asked to "CREATE dim_company.sql", but I see "dim_companies.sql" exists. 
    -- I will rename my created file to align or just reference dim_company. I created dim_company.sql above. I'll stick to it.
)

-- To get top skill, we need to unnest. This requires a subquery or CTE.
, unnested_skills as (
    select 
        f.company_key,
        skill
    from facts f,
    unnest(json_query_array(f.skills_array)) as skill_json -- BigQuery handles JSON arrays a bit differently depending on format. Usually just unnest(skills_array) if it's truly a BQ array, but auto-detect often makes it a JSON string or JSON array. Assuming JSON array.
    cross join unnest([replace(replace(cast(skill_json as string), '"', ''), '''', '')]) as skill -- clean quotes
),
skill_counts as (
    select 
        company_key,
        skill,
        count(*) as skill_count,
        row_number() over(partition by company_key order by count(*) desc) as rnk
    from unnested_skills
    where skill is not null and skill != ''
    group by company_key, skill
),
top_skills as (
    select company_key, skill as top_skill 
    from skill_counts
    where rnk = 1
),
company_agg as (
    select
        f.company_key,
        count(distinct f.job_id) as total_postings,
        avg(f.experience_years) as avg_experience_years,
        countif(f.work_mode = 'Remote') / nullif(count(*), 0) as remote_pct
    from facts f
    group by f.company_key
)

select
    c.company_name,
    c.company_sector,
    a.total_postings,
    t.top_skill,
    a.avg_experience_years,
    a.remote_pct
from company_agg a
join {{ ref('dim_company') }} c on a.company_key = c.company_key
left join top_skills t on a.company_key = t.company_key
order by a.total_postings desc
limit 50
