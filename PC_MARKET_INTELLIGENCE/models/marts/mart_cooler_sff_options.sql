SELECT
    name,
    url
FROM {{ref('stg_cpu_cooler')}}
WHERE (cooling_type = 'AIR COOLER' AND fan_size_in_mm <= 120)
   OR (cooling_type = 'LIQUID AIO COOLER' AND radiator_size_in_mm = 240)
