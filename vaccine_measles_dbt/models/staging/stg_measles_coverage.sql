{{
    config(
        materialized = 'view'
    )
}}
with baseload as (
    select
        upper(code) as country_code,
        upper(entity) as country_name,
        year,
        antigen,
        coverage_pct
    from RAW.coverage_raw
    where year > 1999
    and code is not null
),
pivoted as (
    SELECT
        country_code,
        country_name,
        year,
        max(case when antigen = 'MCV1' then coverage_pct end) as measles_coverage_mcv1_pct,
        max(case when antigen = 'MCV2' then coverage_pct end) as measles_coverage_mcv2_pct
    FROM baseload
    group by country_code, country_name, year
)

select
    country_code,
    country_name,
    year,
    measles_coverage_mcv1_pct,
    measles_coverage_mcv2_pct,
    measles_coverage_mcv1_pct as measles_coverage_pct
from pivoted
where year is not null
and year > 1999
and country_code is not null