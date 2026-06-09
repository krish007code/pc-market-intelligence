WITH chipset_pricing AS (
    SELECT
        platform,
        chipset,
        MIN(price_inr) AS min_price_inr,
        ROUND(AVG(price_inr), 2) AS avg_price_inr,
        COUNT(*) AS board_count
    FROM {{ ref('stg_motherboard') }}
    GROUP BY 1, 2
)

SELECT 
    platform,
    chipset,
    min_price_inr,
    avg_price_inr,
    board_count
FROM chipset_pricing
ORDER BY 
    platform ASC, 
    avg_price_inr ASC