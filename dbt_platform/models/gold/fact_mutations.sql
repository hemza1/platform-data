{{
  config(
    materialized='table',
    schema='gold'
  )
}}

select
    date_mutation,
    valeur_fonciere,
    surface_reelle_bati,
    type_local,
    nature_mutation
from {{ ref('dvf_mutations_gold') }}