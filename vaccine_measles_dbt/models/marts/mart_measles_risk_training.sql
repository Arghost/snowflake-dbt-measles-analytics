{{config(materialized = 'table')}}
-- creating columns for previous and next year using lag and lead.
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
        )as mcv2_prev_year,

        lead(fc.measles_cases) over (
            partition by c.country_code
            order by fc.year
        ) as total_cases_next_year
    from {{ref ('fact_measles_cases')}} as fc
    join {{ref ('dim_country')}} as c
    on c.country_code = fc.country_code
    left join {{ref ('fact_measles_coverage')}} as cov
    on cov.country_code = fc.country_code
    and cov.year = fc.year
),
features as (
    select 
        country_code,
        country_name,
        year,
        COALESCE (measles_cases, 0) as measles_cases,
        COALESCE (total_cases_prev_year, 0) as total_cases_prev_year,
        mcv1_prev_year,
        mcv2_prev_year,
        measles_coverage_mcv1_pct,
        measles_coverage_mcv2_pct,
        (measles_coverage_mcv1_pct - mcv1_prev_year) as delta_mcv1,
        (measles_coverage_mcv2_pct - mcv2_prev_year) as delta_mcv2,

        case
            when total_cases_next_year > 500 then 1
            else 0
        end as high_risk_next_year
    from base
    where total_cases_next_year is not null -- los ultimos años no se usan
)
select *
from features
where high_risk_next_year is not null