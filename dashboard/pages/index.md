# PC Market Intelligence
> Real-time PC component pricing from Indian retailers 

---

## Cooling Solutions

### Average Price by Cooler Type
```sql mart_cooler_price_by_type
select * from pc_parts.cooler_price_by_type
```
<BarChart
    data={mart_cooler_price_by_type}
    x="cooling_type"
    y="avg_price"
    yAxisTitle="Avg Price (₹)"
    xAxisTitle="Cooler Type"
/>

### PWM Controller Distribution by Price Tier
```sql mart_cooler_pwm_distribution
select * from pc_parts.cooler_pwm_distribution
```
<DataTable
    data={mart_cooler_pwm_distribution}
    rows=20
/>

### Small Form Factor (SFF) Compatible Coolers
```sql mart_cooler_sff_options
select * from pc_parts.cooler_sff_options
```
<DataTable
    data={mart_cooler_sff_options}
    columns={['name', 'price_inr']}
/>

---

## Processors (CPU)

### Price Jump by Core Count
```sql mart_core_count_vs_price
select * from pc_parts.core_count_vs_price
```
<LineChart
    data={mart_core_count_vs_price}
    x="cores"
    y="avg_price_inr"
    yAxisTitle="Avg Price (₹)"
    xAxisTitle="Core Count"
/>
<DataTable
    data={mart_core_count_vs_price}
    columns={['cores', 'avg_price_inr', 'price_jump_inr', 'percentage_jump']}
/>

### Unlocked vs Locked Pricing Premium by Series
```sql mart_unlocked_vs_locked_pricing
select * from pc_parts.unlocked_vs_locked_pricing
```
<BarChart
    data={mart_unlocked_vs_locked_pricing}
    x="series"
    y="premium_percentage"
    yAxisTitle="Price Premium (%)"
    xAxisTitle="Processor Series"
    swapXY=true
/>

---

## Memory (RAM)

### DDR4 vs DDR5 Price Premium by Capacity
```sql mart_DDR4_vs_DDR5_pricing
select * from pc_parts.DDR4_vs_DDR5_pricing
```

<DataTable
    data={mart_DDR4_vs_DDR5_pricing}
    columns={['capacity_in_gb', 'avg_ddr4_price_inr', 'avg_ddr5_price_inr', 'ddr5_premium_inr', 'premium_percentage']}
/>

### The RGB Tax — How Much Extra Does Lighting Cost?
```sql mart_ram_rgb_tax
select * from pc_parts.ram_rgb_tax
```
<DataTable
    data={mart_ram_rgb_tax}
    columns={['capacity_in_gb', 'speed_in_mhz', 'avg_non_rgb_price_inr', 'avg_rgb_price_inr', 'rgb_premium_inr', 'rgb_tax_percentage']}
/>

### Speed vs Price — 16GB Kits
```sql mart_speed_vs_price
select * from pc_parts.speed_vs_price_within_same_capacity
```

<DataTable
    data={mart_speed_vs_price}
    columns={['speed_in_mhz', 'entry_price_inr', 'avg_price_inr', 'price_increase_from_prev_tier', 'percentage_jump']}
/>

---

## Graphics Cards (GPU)

### Average Price by Cooler Type per Chipset
```sql mart_gpu_cooler_premium
select * from pc_parts.gpu_cooler_premium
```
<BarChart
    data={mart_gpu_cooler_premium}
    x="cooler"
    y="avg_price_per_cooler_per_chipset"
    yAxisTitle="Avg Price (₹)"
    xAxisTitle="Cooler Type"
    swapXY=true
/>

### Recommended PSU Wattage by Chipset
```sql mart_gpu_psu_by_chipset
select * from pc_parts.gpu_psu
```
<BarChart
    data={mart_gpu_psu_by_chipset}
    x="chipset"
    y="avg_recommended_psu"
    yAxisTitle="Avg PSU (W)"
    xAxisTitle="Chipset"
    swapXY=true
/>

### Legacy API Cards Still on Market
```sql mart_gpu_legacy_api
select * from pc_parts.gpu_legacy_api
```
<DataTable
    data={mart_gpu_legacy_api}
    columns={['name', 'url']}
/>

---

## Displays & Monitors

### Panel Type vs Refresh Rate Pricing
```sql mart_panel_type_vs_price
select * from pc_parts.panel_type_vs_price
```
<BarChart
    data={mart_panel_type_vs_price}
    x="panel_type"
    y="price_inr"
    yAxisTitle="Avg Price (₹)"
    xAxisTitle="Panel Type"
/>

### Screen Size vs Price Distribution
```sql mart_screen_size_vs_price_bands
select * from pc_parts.screen_size_vs_price_bands
```
<DataTable
    data={mart_screen_size_vs_price_bands}
    columns={['screen_size_in_inch', 'panel_type', 'refresh_rate_in_hz', 'min_price_inr', 'avg_price_inr', 'max_price_inr', 'monitor_count']}
/>

### Best Budget Gaming Monitors (Under ₹15,000, Under 5ms)
```sql mart_response_time_budget
select * from pc_parts.response_time_by_usage_tier
```
<DataTable
    data={mart_response_time_budget}
    columns={['name', 'response_time_in_ms', 'price_inr', 'response_time_rank']}
/>

---

## Motherboards

### Platform & Chipset Pricing
```sql mart_platform_chipset_pricing
select * from pc_parts.platform__and_chipset_pricing
```
<BarChart
    data={mart_platform_chipset_pricing}
    x="chipset"
    y="avg_price_inr"
    yAxisTitle="Avg Price (₹)"
    xAxisTitle="Chipset"
    swapXY=true
/>
<DataTable
    data={mart_platform_chipset_pricing}
    columns={['platform', 'chipset', 'min_price_inr', 'avg_price_inr', 'board_count']}
/>

### WiFi Adoption by Price Tier
```sql mart_wifi_adoption
select * from pc_parts.wireless_adoption_by_price_tier
```
<BarChart
    data={mart_wifi_adoption}
    x="price_tier"
    y="wifi_adoption_percentage"
    yAxisTitle="WiFi Adoption (%)"
    xAxisTitle="Price Tier"
/>