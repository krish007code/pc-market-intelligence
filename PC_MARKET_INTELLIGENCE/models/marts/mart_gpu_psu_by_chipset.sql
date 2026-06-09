SELECT 
    chipset,
    AVG(recommended_psu) AS avg_recommended_psu
FROM {{ ref('stg_gpu')}}
GROUP BY chipset
ORDER BY avg_recommended_psu DESC