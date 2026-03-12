{{
  config(
    materialized='table',
    schema='gold'
  )
}}

select
    date_meteo,
    count(*) as nb_observations,
    avg(weather_code)::numeric(10,2) as weather_code_moyen,
    min(weather_code) as weather_code_min,
    max(weather_code) as weather_code_max
from {{ ref('meteo_quotidien_gold') }}
group by date_meteo