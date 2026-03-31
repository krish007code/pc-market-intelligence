
  
  create view "pc_market_intelligence"."main"."stg_cpus__dbt_tmp" as (
    

WITH raw_cpus AS (
    -- Reference the 'cpus' source from your YML
    SELECT * FROM "pc_market_intelligence"."main"."cpus"
)

SELECT
    name AS cpu_full_name,
    brand,
    -- Extracting number of cores (e.g., '6 Cores' -> 6)
    COALESCE(regexp_extract(cores, '(\d+)', 1)::INT, 0) AS core_count,
    -- Extracting speed (e.g., '4.2GHz' -> 4.2)
    regexp_extract(speed, '(\d+\.?\d*)', 1)::FLOAT AS clock_speed_ghz,
    
    socket,
    tdp AS tdp_watts,
    source AS website_source
FROM raw_cpus
WHERE name IS NOT NULL AND (category = 'Processors' OR name LIKE '%Processor%')
  );
