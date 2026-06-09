WITH series_pricing AS (
    SELECT
        series,
        AVG(CASE WHEN has_unlocked = TRUE THEN price_inr END) AS avg_unlocked_price,
        AVG(CASE WHEN has_unlocked = FALSE THEN price_inr END) AS avg_locked_price,
        COUNT(*) AS total_processors
    FROM {{ ref('stg_processor') }}
    WHERE series IS NOT NULL
    GROUP BY series
)

SELECT
    series,
    ROUND(avg_locked_price, 2) AS avg_locked_price_inr,
    ROUND(avg_unlocked_price, 2) AS avg_unlocked_price_inr,
    ROUND(avg_unlocked_price - avg_locked_price, 2) AS price_premium_inr,
    ROUND(
        (avg_unlocked_price - avg_locked_price) * 100.0 / NULLIF(avg_locked_price, 0), 
        2
    ) AS premium_percentage
FROM series_pricing
WHERE avg_unlocked_price IS NOT NULL 
  AND avg_locked_price IS NOT NULL
ORDER BY premium_percentage DESC