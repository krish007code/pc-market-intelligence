

WITH raw_data AS (
    SELECT * FROM "pc_market_intelligence"."main"."laptops"
)

SELECT
    -- Use 'name' if 'model' is null
    COALESCE(name, 'Unknown Model') AS laptop_name,
    brand,
    
    -- Bulletproofing the CPU: if the 'cpu' column is null, 
    -- we take the first 50 characters of the 'name' instead
    lower(COALESCE(cpu, LEFT(name, 50), 'unknown cpu')) AS cpu_description,

    -- Standardizing RAM (Safe extraction)
    -- If memory_type is null, we default to 0 to avoid crashes
    COALESCE(regexp_extract(memory_type, '(\d+)', 1)::INT, 0) AS ram_gb,

    -- Portability
    weight,
    source AS website_source

FROM raw_data
-- Only include rows that have at least a name
WHERE name IS NOT NULL