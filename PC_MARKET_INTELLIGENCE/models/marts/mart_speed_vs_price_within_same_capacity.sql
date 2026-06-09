WITH ram_speed_tiers AS (
    SELECT
        speed_in_mhz,
        MAX(kit_type) AS sample_kit_type, 
        MIN(price_inr) AS min_price_inr,
        AVG(price_inr) AS avg_price_inr,
        COUNT(*) AS available_kits
    FROM {{ ref('stg_ram') }}
    WHERE capacity_in_gb = 16
    GROUP BY speed_in_mhz
)

SELECT
    speed_in_mhz,
    sample_kit_type,
    available_kits,
    ROUND(min_price_inr, 2) AS entry_price_inr,
    ROUND(avg_price_inr, 2) AS avg_price_inr,
    ROUND(
        avg_price_inr - LAG(avg_price_inr) OVER (ORDER BY speed_in_mhz ASC), 
        2
    ) AS price_increase_from_prev_tier,
    ROUND(
        (avg_price_inr - LAG(avg_price_inr) OVER (ORDER BY speed_in_mhz ASC)) * 100.0 / 
        NULLIF(LAG(avg_price_inr) OVER (ORDER BY speed_in_mhz ASC), 0),
        2
    ) AS percentage_jump
FROM ram_speed_tiers
ORDER BY speed_in_mhz ASC