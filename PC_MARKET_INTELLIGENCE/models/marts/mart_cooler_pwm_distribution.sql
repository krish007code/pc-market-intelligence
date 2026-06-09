WITH CTE AS(
SELECT
    CASE 
        WHEN price_inr < 2000 THEN 'budget'
        WHEN price_inr < 5000 THEN 'mid'
        ELSE 'premium'
    END AS price_tier,
    pwm_controller
FROM {{ref('stg_cpu_cooler')}}
)
SELECT price_tier, pwm_controller, COUNT(*) AS count
FROM CTE
GROUP BY price_tier, pwm_controller