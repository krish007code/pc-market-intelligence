{{ config(materialized='view') }}
SELECT
    "name",
    "url",
    "aspect_ratio",
    "panel_type",
    "resolution",
    TRY_CAST(REGEXP_EXTRACT(response_time, '(\d+\.?\d*)') AS FLOAT) AS response_time_in_ms,
    TRY_CAST(REGEXP_EXTRACT(warranty, '(\d+)', 1) AS INTEGER) AS warranty_in_years,
    TRY_CAST(REGEXP_EXTRACT(screen_size, '(\d+)') AS INTEGER) AS screen_size_in_INCH,
    "display",
    TRY_CAST(REGEXP_EXTRACT(refresh_rate, '(\d+)') AS INTEGER) AS refresh_rate_in_Hz,
    "contrast_ratio",
    "screen_surface",
    "brightness",
    "viewing_angle",
    "color_gamut",
    "connectivity",
    "price_inr"
FROM {{ source("bronze", "monitor_cleaned") }}