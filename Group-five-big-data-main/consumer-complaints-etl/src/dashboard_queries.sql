-- Dashboard queries for DuckDB or CSV-based BI reporting

-- 1. Top states by complaint count
SELECT state_abbrv,
       state_name,
       complaints_count,
       pop_2014,
       complaints_count * 1.0 / pop_2014 AS complaints_per_capita
FROM consumer_insights
ORDER BY complaints_count DESC
LIMIT 15;

-- 2. Complaint intensity by population
SELECT state_abbrv,
       state_name,
       pop_2014,
       complaints_count,
       complaints_count * 1.0 / pop_2014 AS complaints_per_capita
FROM consumer_insights
ORDER BY complaints_per_capita DESC
LIMIT 15;

-- 3. Geographic dataset for BI mapping tools
SELECT state_abbrv,
       state_name,
       complaints_count,
       latitude,
       longitude
FROM consumer_insights
WHERE latitude IS NOT NULL AND longitude IS NOT NULL;
