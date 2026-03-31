

WITH base_data AS (
    SELECT * FROM "pc_market_intelligence"."main"."stg_laptops"
),

ranked_data AS (
    SELECT
        laptop_name,
        brand,
        cpu_description,
        ram_gb,
        gpu_description,
        
        -- 2026 Gaming Logic: High VRAM and RTX/RX Architecture
        CASE 
            WHEN gpu_description LIKE '%rtx 50%' OR gpu_description LIKE '%rtx 4090%' THEN 100
            WHEN gpu_description LIKE '%rtx 40%' OR gpu_description LIKE '%rx 7900%' THEN 85
            WHEN gpu_description LIKE '%rtx 30%' OR gpu_description LIKE '%rx 6000%' THEN 60
            ELSE 20 
        END AS gaming_tier_score,

        -- 2026 Coding/DevOps Logic: High Core Count & RAM
        CASE 
            WHEN ram_gb >= 32 THEN 50
            WHEN ram_gb >= 16 THEN 30
            ELSE 10 
        END + 
        CASE 
            WHEN cpu_description LIKE '%i9%' OR cpu_description LIKE '%ryzen 9%' THEN 50
            WHEN cpu_description LIKE '%i7%' OR cpu_description LIKE '%ryzen 7%' THEN 35
            ELSE 15 
        END AS dev_score

    FROM base_data
)

SELECT
    *,
    -- Final Classifications
    CASE 
        WHEN gaming_tier_score >= 85 AND dev_score >= 80 THEN 'Extreme Workstation'
        WHEN gaming_tier_score >= 80 THEN 'High-End Gaming'
        WHEN dev_score >= 60 THEN 'DevOps/Data Science Ready'
        ELSE 'Entry Level / Student'
    END AS build_category,
    
    -- Rank them globally
    RANK() OVER (ORDER BY (gaming_tier_score + dev_score) DESC) AS global_rank
FROM ranked_data