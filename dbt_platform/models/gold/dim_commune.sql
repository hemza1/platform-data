{{
  config(
    materialized='table',
    schema='gold'
  )
}}

select distinct
    commune as commune_id,
    commune
from {{ ref('dvf_mutations_gold') }}
where commune is not null
  and commune <> ''