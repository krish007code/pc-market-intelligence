{{ config(materialized='view') }}

SELECT
    "name",
    "url",
    "platform",
    "socket",
    "cpu_type",
    "chipset",
    "supported_memory_type",
    "channel_supported",
    "memory_feature",
    "graphics_port",
    "expansion_slots",
    "back_panel_i/o_ports",
    "internal_i/o_connector",
    "form_factor",
    "cpu",
    "memory",
    "audio",
    "lan",
    "storage",
    "usb",
    "wireless_networking",
    "price_inr",
    "warranty_in_years"
FROM
    {{ source("bronze", "motherboard_cleaned") }}