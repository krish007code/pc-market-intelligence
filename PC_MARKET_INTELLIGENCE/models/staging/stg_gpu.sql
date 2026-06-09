{{ config(materialized='view') }}
SELECT
    "name",
    "url",
    "model",
    "chipset",
    "gpu",
    "pci_express",
    "memory_clock",
    "memory_size",
    "memory_interface",
    "memory_type",
    "opengl",
    "ports", 
    "directx", 
    "resolution",
    "cooler",
    "max_display_support",
    "recommended_psu",
    "power_connectors",
    "price_inr",
    "gpu_core",
    "warranty_in_year"
FROM {{ source('bronze', 'gpu_sync_cleaned') }}