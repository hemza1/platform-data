{{
  config(
    materialized='table',
    schema='gold'
  )
}}

with s as (
  select *
  from {{ ref('dvf_mutations_gold') }}
),
dc as (
  select *
  from {{ ref('dim_commune') }}
),
dl as (
  select *
  from {{ ref('dim_local') }}
),
dn as (
  select *
  from {{ ref('dim_nature_mutation') }}
),
dt as (
  select *
  from {{ ref('dim_temps') }}
)

select
  s.id_mutation,
  dt.date_id,
  dc.commune_id,
  dl.type_local_id,
  dn.nature_mutation_id,
  s.date_mutation,
    valeur_fonciere,
    surface_reelle_bati,
  s.type_local,
  s.nature_mutation
from s
left join dc
  on s.commune = dc.commune
left join dl
  on s.type_local = dl.type_local
left join dn
  on s.nature_mutation = dn.nature_mutation
left join dt
  on s.date_mutation = dt.date_mutation