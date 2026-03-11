{{
  config(
    materialized='table',
    schema='silver'
  )
}}

select
    to_date(date_mutation, 'DD/MM/YYYY') as date_mutation,

    case
        when valeur_fonciere is null or valeur_fonciere = '' then null
        else replace(valeur_fonciere, ',', '.')::numeric(12,2)
    end as valeur_fonciere,

    case
        when surface_reelle_bati is null or surface_reelle_bati = '' then null
        else replace(surface_reelle_bati, ',', '.')::numeric(10,2)
    end as surface_reelle_bati,

    commune,
    type_local,
    nullif(nature_mutation, '') as nature_mutation
from {{ source('bronze', 'dvf_2025_s1') }}
where valeur_fonciere is not null
  and valeur_fonciere <> ''
  and replace(valeur_fonciere, ',', '.')::numeric > 0