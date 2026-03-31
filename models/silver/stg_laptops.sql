{{ config(materialized='view') }}

WITH raw_data AS (
    SELECT * FROM {{ source('pc_market_sources', 'laptops') }}
)

SELECT
    -- Identity
    COALESCE(name, model, model_name) AS laptop_name,
    brand,
    source AS website_source,

    -- CPU Cleaning (Stealing from name if the cpu column is empty)
    lower(COALESCE(cpu, LEFT(name, 50))) AS cpu_description,
    
    -- RAM Cleaning: Turning "16GB" or "8GB" into a Number
    -- We use memory_type since I saw that in your column list
    COALESCE(regexp_extract(memory_type, '(\d+)', 1)::INT, 8) AS ram_gb,

    -- Graphics
    lower(COALESCE(graphics_card, gpu, integrated_graphics)) AS gpu_description,

    -- Metadata
    current_timestamp AS dbt_updated_at

FROM raw_data
WHERE name IS NOT NULL