{{config (
    materialized = 'table'
)}}
select  co.country_code,
        co.country_name,
        co.region,
        mc.year,
        mc.measles_cases,
from {{ref ('stg_measles_cases')}} mc
join {{ref ('dim_country')}} co
on mc.country_code = co.country_code
and mc.country_name = co.country_name