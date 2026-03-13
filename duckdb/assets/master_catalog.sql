-- @bruin.name: master_catalog
-- @bruin.type: duckdb.sql
-- @bruin.materialization: table

/* 🛡️ DATA GUARDRAILS */
-- @bruin.check.not_null: [name, price_inr, category]
-- @bruin.check.positive: [price_inr]
-- @bruin.check.unique: [url]

WITH unified_data AS (
    -- Laptops
    SELECT 
        COALESCE(name, 'Unknown Laptop') as name,
        CAST(price_inr AS DOUBLE) as price_inr,
        url,
        'laptop' as category,
        (COALESCE(processor, 'N/A') || ' | ' || COALESCE(ram_size, 'N/A')) as specs
    FROM laptops
    
    UNION ALL

    -- CPUs
    SELECT 
        COALESCE(name, 'Unknown CPU') as name,
        CAST(price_inr AS DOUBLE) as price_inr,
        url,
        'cpu' as category,
        (COALESCE(socket, 'N/A') || ' | ' || COALESCE(cores, '0') || ' Cores') as specs
    FROM cpus

    UNION ALL

    -- GPUs
    SELECT 
        COALESCE(name, 'Unknown GPU') as name,
        CAST(price_inr AS DOUBLE) as price_inr,
        url,
        'gpu' as category,
        (COALESCE(chipset, 'N/A') || ' | ' || COALESCE(memory_size, 'N/A')) as specs
    FROM gpus

    UNION ALL

    -- RAM
    SELECT 
        COALESCE(name, 'Unknown RAM') as name,
        CAST(price_inr AS DOUBLE) as price_inr,
        url,
        'ram' as category,
        (COALESCE(memory_type, 'N/A') || ' | ' || COALESCE(capacity, 'N/A')) as specs
    FROM ram
)

SELECT 
    *,
    -- Logic for the AI to understand market positioning
    CASE 
        WHEN price_inr < 25000 THEN 'Entry Level'
        WHEN price_inr BETWEEN 25000 AND 75000 THEN 'Mid-Range'
        ELSE 'High-End/Pro'
    END AS market_tier
FROM unified_data
WHERE price_inr > 500; -- Extra safety to remove placeholder listings