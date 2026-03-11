{{
  config(
    materialized='table',
    schema='gold'
  )
}}

select distinct
    type_local as type_local_id,
    type_local as libelle
from {{ ref('dvf_mutations_gold') }}
where type_local is not null