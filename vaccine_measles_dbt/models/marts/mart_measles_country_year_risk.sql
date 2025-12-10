{{config(
    materialized = 'table'
)}}
with base as (
    select 
        cov.country_code,
        cov.country_name,
        cov.region,
        cov.year,
        coalesce(cov.measles_coverage_pct, 0) as measles_coverage_pct,
        coalesce(cases.measles_cases, 0) as measles_cases
    from {{ ref('fact_measles_coverage') }} cov
    left join {{ ref('fact_measles_cases') }} cases
      on cov.country_code = cases.country_code
     and cov.year = cases.year
),
with_gaps as (
    select
        country_code,
        country_name,
        region,
        year,
        measles_coverage_pct,
        measles_cases,
        -- Coverage gap vs 95% target (herd immunity)
        coalesce(greatest(0, 95 - measles_coverage_pct), 0) as coverage_gap_vs_95
    from base
),
with_norm as (
    select
        country_code,
        country_name,
        region,
        year,
        measles_coverage_pct,
        measles_cases,
        coverage_gap_vs_95,
        -- Normalise coverage gap to [0,1]
        case
            when max(coverage_gap_vs_95) over () = min(coverage_gap_vs_95) over () then 0
            else (coverage_gap_vs_95 - min(coverage_gap_vs_95) over ())
                 / nullif(max(coverage_gap_vs_95) over () - min(coverage_gap_vs_95) over (), 0)
        end as norm_coverage_gap,
        -- Normalise cases to [0,1]
        case
            when max(measles_cases) over () = min(measles_cases) over () then 0
            else (measles_cases - min(measles_cases) over ())
                 / nullif(max(measles_cases) over () - min(measles_cases) over (), 0)
        end as norm_cases
    from with_gaps
),
scored as (
    select
        country_code,
        country_name,
        region,
        year,
        measles_coverage_pct,
        measles_cases,
        coverage_gap_vs_95,
        norm_coverage_gap,
        norm_cases,

        -- Simple risk score combining incidence and immunity gap
        0.6 * norm_cases + 0.4 * norm_coverage_gap as risk_score
    from with_norm
)

select
    country_code,
    country_name,
    region,
    year,
    measles_coverage_pct,
    measles_cases,
    coverage_gap_vs_95,
    norm_coverage_gap,
    norm_cases,
    risk_score,
    case
        when risk_score < 0.25 then 'LOW'
        when risk_score < 0.5  then 'MEDIUM'
        when risk_score < 0.75 then 'HIGH'
        else 'CRITICAL'
    end as risk_band
from scored