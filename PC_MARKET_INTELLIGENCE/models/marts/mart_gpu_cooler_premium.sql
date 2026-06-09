SELECT
    COUNT(*)
    chipset,
    cooler,
    AVG(price_inr) as avg_price_per_cooler_per_chipset
FROM {{ ref('stg_gpu')}}
GROUP BY chipset, cooler
ORDER BY avg_price_per_cooler_per_chipset DESC