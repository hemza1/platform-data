{{
  config(
    materialized='table',
    schema='gold'
  )
}}

select distinct
    to_char(date_mutation, 'YYYYMMDD')::int as date_id,
    date_mutation,
    extract(year from date_mutation)::int as annee,
    extract(month from date_mutation)::int as mois,
    extract(day from date_mutation)::int as jour,
    extract(dow from date_mutation)::int as jour_semaine
from {{ ref('dvf_mutations_gold') }}
where date_mutation is not null
