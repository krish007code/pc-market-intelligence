WITH core_tier_prices AS (
    SELECT
        cores,
        ROUND(AVG(price_inr), 2) AS avg_price_inr,
        COUNT(*) AS processor_count
    FROM {{ ref('stg_processor') }}
    WHERE cores IS NOT NULL 
    GROUP BY cores
)

SELECT
    cores,
    avg_price_inr,
    avg_price_inr - LAG(avg_price_inr) OVER (ORDER BY cores ASC) AS price_jump_inr,
    ROUND(
        (avg_price_inr - LAG(avg_price_inr) OVER (ORDER BY cores ASC)) * 100.0 / 
        LAG(avg_price_inr) OVER (ORDER BY cores ASC), 
        2
    ) AS percentage_jump
FROM core_tier_prices
ORDER BY cores ASC