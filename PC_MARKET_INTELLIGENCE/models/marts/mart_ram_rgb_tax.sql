WITH product_baselines AS (
    SELECT
        capacity_in_gb,
        speed_in_mhz,
        lighting,
        price_inr,
        AVG(CASE WHEN UPPER(lighting) <> 'RBG' THEN price_inr END) OVER(PARTITION BY capacity_in_gb, speed_in_mhz) AS avg_non_rgb_price,
        AVG(CASE WHEN UPPER(lighting) = 'RGB' THEN price_inr END) OVER(PARTITION BY capacity_in_gb, speed_in_mhz) AS avg_rgb_price
    FROM {{ ref('stg_ram') }}
)
SELECT DISTINCT
    capacity_in_gb,
    speed_in_mhz,
    ROUND(avg_non_rgb_price, 2) AS avg_non_rgb_price_inr,
    ROUND(avg_rgb_price, 2) AS avg_rgb_price_inr,
    ROUND(avg_rgb_price - avg_non_rgb_price, 2) AS rgb_premium_inr,
    ROUND( (avg_rgb_price - avg_non_rgb_price) * 100.0 / NULLIF(avg_non_rgb_price, 0), 2) AS rgb_tax_percentage
FROM product_baselines
WHERE (avg_rgb_price IS NOT NULL) AND (avg_non_rgb_price IS NOT NULL)
ORDER BY capacity_in_gb ASC, speed_in_mhz ASC