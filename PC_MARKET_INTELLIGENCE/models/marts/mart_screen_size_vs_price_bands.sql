SELECT
    screen_size_in_inch,
    panel_type,
    refresh_rate_in_hz,
    MIN(price_inr) AS min_price_inr,
    ROUND(AVG(price_inr), 2) AS avg_price_inr,
    MAX(price_inr) AS max_price_inr,
    COUNT(*) AS monitor_count
FROM {{ ref('stg_monitor') }}
GROUP BY 1, 2, 3
ORDER BY 
    screen_size_in_inch ASC, 
    avg_price_inr DESC