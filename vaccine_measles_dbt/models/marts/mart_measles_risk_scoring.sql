{{config (
    materialized = 'table'
)}}
with base as (
     select
        c.country_code,
        c.country_name,
        fc.year,
        fc.measles_cases,
        cov.measles_coverage_mcv1_pct,
        cov.measles_coverage_mcv2_pct,

        lag(fc.measles_cases) over (
            partition by c.country_code
            order by fc.year
        ) as total_cases_prev_year,

        lag(cov.measles_coverage_mcv1_pct) over (
            partition by c.country_code
            order by fc.year
        )as mcv1_prev_year,

        lag(cov.measles_coverage_mcv2_pct) over (
            partition by c.country_code
            order by fc.year
        )as mcv2_prev_year

    from {{ref ('fact_measles_cases')}} as fc
    join {{ref ('dim_country')}} as c
    on c.country_code = fc.country_code
    left join {{ref ('fact_measles_coverage')}} as cov
    on cov.country_code = fc.country_code
    and cov.year = fc.year
),
latest_per_country as (
    select b.*
    from base b
    join (
        select country_code,
            max(year) as max_year
        from base
        group by country_code
    )m
    on m.country_code = b.country_code
    and m.max_year = b.year
)

select 
    country_code,
    country_name,
    year,
    COALESCE(measles_cases, 0) as measles_cases,
    COALESCE(total_cases_prev_year, 0) as total_cases_prev_year,
    measles_coverage_mcv1_pct,
    measles_coverage_mcv2_pct,
    mcv1_prev_year,
    mcv2_prev_year,
    (measles_coverage_mcv1_pct - mcv1_prev_year) as delta_mcv1,
    (measles_coverage_mcv2_pct - mcv2_prev_year) as delta_mcv2
from latest_per_country