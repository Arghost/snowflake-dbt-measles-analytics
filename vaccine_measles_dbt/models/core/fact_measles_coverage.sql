{{config (
    materialized = 'table'
)}}
select  co.country_code,
        co.country_name,
        co.region,
        mc.year,
        mc.measles_coverage_mcv1_pct,
        mc.measles_coverage_mcv2_pct,
        mc.measles_coverage_pct,
from {{ref ('stg_measles_coverage')}} mc
join {{ref ('dim_country')}} co
on mc.country_code = co.country_code
and mc.country_name = co.country_name