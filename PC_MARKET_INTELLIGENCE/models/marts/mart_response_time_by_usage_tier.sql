SELECT
    name,
    url,
    response_time_in_ms,
    price_inr,
    DENSE_RANK() OVER (ORDER BY response_time_in_ms ASC) AS response_time_rank
FROM {{ ref('stg_monitor') }}
WHERE price_inr < 15000
  AND response_time_in_ms < 5
ORDER BY response_time_in_ms ASC, price_inr ASC