SELECT
    cooling_type,
    AVG(price_inr) AS avg_price
FROM {{ref("stg_cpu_cooler")}}
group by cooling_type