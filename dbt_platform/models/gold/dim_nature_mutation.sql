{{
  config(
    materialized='table',
    schema='gold'
  )
}}

select distinct
    nature_mutation as nature_mutation_id,
    nature_mutation
from {{ ref('dvf_mutations_gold') }}
where nature_mutation is not null
