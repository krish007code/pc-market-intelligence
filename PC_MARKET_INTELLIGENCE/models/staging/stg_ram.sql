{{ config(materialized='view') }}

SELECT
    "name",
    "url",
    "model",
    "product_series",
    "memory_type",
    "lighting",
    "kit_type",
    "tested_latency",
    "dimm_type",
    "profile_type",
    "price_inr",
    "capacity_in_GB",
    "speed_in_MHz",
    "tested_voltage_in_V"
FROM {{ source("bronze", "ram_cleaned") }}
