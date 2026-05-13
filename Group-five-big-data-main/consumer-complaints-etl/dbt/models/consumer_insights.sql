{{ config(materialized = 'table') }}

select
    state_abbrv,
    state_name,
    complaints_count,
    pop_2014,
    latitude,
    longitude,
    complaints_count * 1.0 / pop_2014 as complaints_per_capita
from {{ source('consumer', 'consumer_insights') }}
