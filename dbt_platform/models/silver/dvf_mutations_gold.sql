{{
  config(
    materialized='table',
    schema='silver'
  )
}}

with cleaned as (
  select distinct
    to_date(date_mutation, 'DD/MM/YYYY') as date_mutation,
    replace(valeur_fonciere, ',', '.')::numeric(12,2) as valeur_fonciere,
    case
      when surface_reelle_bati is null or surface_reelle_bati = '' then null
      else replace(surface_reelle_bati, ',', '.')::numeric(10,2)
    end as surface_reelle_bati,
    nullif(trim(commune), '') as commune,
    nullif(trim(type_local), '') as type_local,
    nullif(trim(nature_mutation), '') as nature_mutation
  from {{ source('bronze', 'dvf_2025_s1') }}
  where nullif(date_mutation, '') is not null
    and nullif(valeur_fonciere, '') is not null
    and replace(valeur_fonciere, ',', '.')::numeric > 0
),
final as (
  select
    md5(
      concat_ws(
        '|',
        to_char(date_mutation, 'YYYY-MM-DD'),
        coalesce(valeur_fonciere::text, ''),
        coalesce(surface_reelle_bati::text, ''),
        coalesce(commune, ''),
        coalesce(type_local, ''),
        coalesce(nature_mutation, '')
      )
    ) as id_mutation,
    date_mutation,
    valeur_fonciere,
    surface_reelle_bati,
    commune,
    type_local,
    nature_mutation
  from cleaned
)

select *
from final