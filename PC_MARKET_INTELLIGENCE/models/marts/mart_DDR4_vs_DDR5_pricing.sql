WITH ram_pricing AS (
    SELECT
        capacity_in_gb,
        AVG(CASE WHEN UPPER(memory_type) = 'DDR4' THEN price_inr END) AS avg_ddr4_price,
        AVG(CASE WHEN UPPER(memory_type) = 'DDR5' THEN price_inr END) AS avg_ddr5_price,
        MIN(CASE WHEN UPPER(memory_type) = 'DDR5' THEN price_inr END) AS min_ddr5_price,
        COUNT(*) AS total_kits
    FROM {{ ref('stg_ram') }}
    GROUP BY capacity_in_gb
)

SELECT
    capacity_in_gb,
    ROUND(avg_ddr4_price, 2) AS avg_ddr4_price_inr,
    ROUND(avg_ddr5_price, 2) AS avg_ddr5_price_inr,
    ROUND(avg_ddr5_price - avg_ddr4_price, 2) AS ddr5_premium_inr,
    ROUND(
        (avg_ddr5_price - avg_ddr4_price) * 100.0 / NULLIF(avg_ddr4_price, 0), 
        2
    ) AS premium_percentage,
    ROUND(min_ddr5_price, 2) AS entry_ddr5_price_inr
FROM ram_pricing
ORDER BY capacity_in_gb ASC