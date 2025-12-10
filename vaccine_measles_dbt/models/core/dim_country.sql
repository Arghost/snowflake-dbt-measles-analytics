{{ config(materialized = 'view') }}

with from_coverage as (
    select distinct
        country_code,
        country_name
    from {{ ref('stg_measles_coverage') }}
),
from_cases as (
    select distinct
        country_code,
        country_name
    from {{ ref('stg_measles_cases') }}
),
combined as (
    select * from from_coverage
    union
    select * from from_cases
)

select
    country_code,
    country_name,
    null::string as region   -- placeholder, se llenará luego con un seed
from combined
where country_code is not null
  and country_name is not null