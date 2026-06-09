{{ config(materialized='view') }}

SELECT
    "name",
    "url",
    "cooling_type",
    "socket_support",
    "supported_cooler_cpu_wise",
    "pwm_controller",
    "lighting",
    "price_inr",
    "warranty_in_years",
    "radiator_size_in_mm",
    "fan_size_in_mm"
FROM {{ source('bronze', 'cpu_cooler_cleaned') }}