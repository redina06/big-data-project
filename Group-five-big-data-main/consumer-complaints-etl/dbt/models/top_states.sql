{{ config(materialized='table') }}

SELECT
    state_abbrv,
    state_name,
    complaints_count,
    pop_2014,
    complaints_per_capita
FROM {{ ref('consumer_insights') }}
WHERE complaints_per_capita IS NOT NULL
ORDER BY complaints_per_capita DESC
LIMIT 10