WITH tiered_motherboards AS (
    SELECT
        wireless_networking, 
        CASE
            WHEN price_inr < 10000 THEN 'Under ₹10,000'
            WHEN price_inr BETWEEN 10000 AND 15000 THEN '₹10,000 to ₹15,000'
            WHEN price_inr > 15000 THEN 'Above ₹15,000'
        END AS price_tier
    FROM {{ ref('stg_motherboard') }}
)

SELECT DISTINCT
    price_tier,
    COUNT(*) OVER (PARTITION BY price_tier) AS total_boards_in_tier,
    ROUND(
        SUM(CASE WHEN wireless_networking IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY price_tier) * 100.0 / 
        COUNT(*) OVER (PARTITION BY price_tier), 
        2
    ) AS wifi_adoption_percentage
FROM tiered_motherboards
WHERE price_tier IS NOT NULL
ORDER BY 
    CASE price_tier
        WHEN 'Under ₹10,000' THEN 1
        WHEN '₹10,000 to ₹15,000' THEN 2
        WHEN 'Above ₹15,000' THEN 3
    END ASC