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
        measles_cases
    from RAW.MEASLES_CASES_RAW
),
pivoted as (
    SELECT
        country_code,
        country_name,
        year,
        max(measles_cases) as measles_cases
    FROM baseload
    group by country_code, country_name, year
)

select
    country_code,
    country_name,
    year,
    coalesce(measles_cases, 0) as measles_cases
from pivoted
where year > 1999
  and year is not null
  and country_code is not null