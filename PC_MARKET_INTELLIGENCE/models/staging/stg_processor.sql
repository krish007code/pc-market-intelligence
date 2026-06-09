{{ config(materialized='view') }}
select
    "name",
    "url",
    "cpu",
    TRY_CAST(REGEXP_EXTRACT(cores, '(\d+)') AS INT) as cores,
    "series",
    "memory_type",
    TRY_CAST(REGEXP_EXTRACT(threads, '(\d+)') AS INT) as threads,
    "socket",
    TRY_CAST(REGEXP_EXTRACT(speed, '(\d+)') AS INT) as speed_in_GHz,
    TRY_CAST(REGEXP_EXTRACT('speed_(turbo)', '(\d+)') AS INT) as speed_turbo_in_GHz,
    TRY_CAST(REGEXP_EXTRACT(cache, '(\d+)') AS INT) as cache_in_MB,
    TRY_CAST(REGEXP_EXTRACT(max_memory_support, '(\d+)') AS INT) as max_memory_support_in_GB,
    "integrated_graphics",
    TRY_CAST(REGEXP_EXTRACT(tdp, '(\d+)') AS INT) as tdp_W,
    CASE WHEN included_cpu_cooler = 'Yes' THEN 1 ELSE 0 END AS has_cpu_cooler,
    CASE WHEN unlocked = 'Yes' THEN 1 ELSE 0 END AS has_unlocked,
    "instruction_set",
    "price_inr",
    "warranty_in_years"
from {{source ("bronze", "processor_cleaned")}}







