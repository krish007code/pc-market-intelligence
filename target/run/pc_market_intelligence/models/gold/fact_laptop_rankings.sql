
  
    
    

    create  table
      "pc_market_intelligence"."main"."fact_laptop_rankings__dbt_tmp"
  
    as (
      

WITH silver_laptops AS (
    SELECT * FROM "pc_market_intelligence"."main"."stg_laptops"
    -- Filter out components/monitors that aren't laptops
    WHERE laptop_name NOT LIKE '%Monitor%' 
      AND laptop_name NOT LIKE '%Graphics Card%'
      AND laptop_name NOT LIKE '%Processor%'
)

SELECT
    laptop_name,
    brand,
    cpu_description,
    ram_gb,
    gpu_description,
    website_source,

    -- 1. Coding Score (RAM is King)
    -- We give 10 points for 16GB+, 5 for 8GB, 2 for less
    (CASE WHEN ram_gb >= 16 THEN 10 WHEN ram_gb >= 8 THEN 5 ELSE 2 END) +
    (CASE WHEN cpu_description LIKE '%i7%' OR cpu_description LIKE '%ryzen 7%' THEN 10 
          WHEN cpu_description LIKE '%i5%' OR cpu_description LIKE '%ryzen 5%' THEN 7 
          ELSE 3 END) AS coding_score,

    -- 2. Gaming Score (GPU is King)
    CASE WHEN gpu_description LIKE '%rtx 40%' THEN 15
         WHEN gpu_description LIKE '%rtx 30%' THEN 10
         WHEN gpu_description LIKE '%rx%' THEN 8
         WHEN gpu_description LIKE '%integrated%' OR gpu_description = 'no' THEN 1
         ELSE 3 END AS gaming_score,

    -- Final Ranks
    RANK() OVER (ORDER BY (CASE WHEN ram_gb >= 16 THEN 10 WHEN ram_gb >= 8 THEN 5 ELSE 2 END) DESC) AS coding_rank,
    RANK() OVER (ORDER BY (CASE WHEN gpu_description LIKE '%rtx%' THEN 10 ELSE 1 END) DESC) AS gaming_rank

FROM silver_laptops
    );
  
  