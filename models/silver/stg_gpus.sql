{{ config(materialized='view') }}

WITH raw_gpus AS (
    SELECT * FROM {{ source('pc_market_sources', 'gpus') }}
)

SELECT
    name AS gpu_full_name,
    brand,
    -- Extracting VRAM (e.g., '8GB GDDR6' -> 8)
    COALESCE(regexp_extract(memory_size, '(\d+)', 1)::INT, 0) AS vram_gb,
    memory_type, -- GDDR6, etc.
    
    -- Identifying if it's a high-end RTX card
    CASE WHEN lower(name) LIKE '%rtx%' THEN true ELSE false END AS is_rtx,
    
    recommended_psu,
    source AS website_source
FROM raw_gpus
WHERE name IS NOT NULL AND (category = 'Graphics Card' OR name LIKE '%Graphics Card%')