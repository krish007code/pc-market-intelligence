WITH panel_refresh_pricing AS (
    SELECT
        panel_type,
        refresh_rate_in_hz,
        AVG(price_inr) AS avg_price_inr,
        COUNT(*) AS monitor_count
    FROM {{ ref('stg_monitor') }}
    GROUP BY 1, 2
)

SELECT 
    panel_type,
    refresh_rate_in_hz,
    ROUND(avg_price_inr, 2) AS price_inr
FROM panel_refresh_pricing
ORDER BY 
    panel_type ASC, 
    refresh_rate_in_hz ASC