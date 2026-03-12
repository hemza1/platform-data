{{
  config(
    materialized='table',
    schema='silver'
  )
}}

select
  date_meteo::date as date_meteo,
    coalesce(weather_code::int, 0) as weather_code
from {{ source('bronze', 'meteo_quotidien') }}
where date_meteo is not null