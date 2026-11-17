# Data Dictionary: Indian Logistics Pipeline Dataset

**Dataset Source:** `Delivery_Logistics.csv` (Extracted from `Delivery_Logistics.zip`)  
**Row Count:** 25,000 records  
**Column Count:** 15 columns  
**Target Domain:** Indian 3PL / E-commerce Logistics Operations  

---

## 1. Field Specifications

| Column Name | Raw Data Type | Target PySpark Type | Null Count (%) | Example Values | Description & Transformation Logic |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `delivery_id` | String / Float | `StringType` | 0 (0.00%) | `'250.99'`, `'251.0'`, `'24750.01'` | Raw identifier. Contains anomaly where rows 0-249 have `250.99` and rows 24750-24999 have `24750.01`. In Silver, a canonical `shipment_pk` is generated. |
| `delivery_partner` | String | `StringType` | 0 (0.00%) | `'delhivery'`, `'xpressbees'`, `'shadowfax'`, `'blue dart'`, `'ekart'`, `'fedex'`, `'dhl'`, `'ecom express'`, `'amazon logistics'` | Logistics 3PL service provider operating across India. Standardized to title case / uppercase in Silver. |
| `package_type` | String | `StringType` | 0 (0.00%) | `'automobile parts'`, `'cosmetics'`, `'groceries'`, `'fragile items'`, `'pharmacy'`, `'documents'`, `'electronics'`, `'clothing'`, `'furniture'` | Category of goods shipped. |
| `vehicle_type` | String | `StringType` | 0 (0.00%) | `'bike'`, `'ev van'`, `'truck'`, `'ev bike'`, `'van'`, `'scooter'` | Delivery vehicle utilized for last-mile or line-haul movement. |
| `delivery_mode` | String | `StringType` | 0 (0.00%) | `'same day'`, `'express'`, `'two day'`, `'standard'` | Service level agreement (SLA) mode requested by customer. |
| `region` | String | `StringType` | 0 (0.00%) | `'west'`, `'central'`, `'east'`, `'north'`, `'south'` | Geographic operational zone in India. |
| `weather_condition` | String | `StringType` | 0 (0.00%) | `'clear'`, `'cold'`, `'rainy'`, `'foggy'`, `'stormy'`, `'hot'` | Environmental/weather conditions during delivery window. |
| `distance_km` | String / Float | `DoubleType` | 0 (0.00%) | `297.0`, `89.6`, `273.5` | Delivery distance in kilometers. Range: 3.6 km to 297.1 km (mean: 150.39 km). |
| `package_weight_kg` | String / Float | `DoubleType` | 0 (0.00%) | `46.96`, `47.39`, `26.89` | Weight of shipment in kilograms. Range: 0.67 kg to 49.52 kg (mean: 25.15 kg). |
| `delivery_time_hours` | String | `IntegerType` (parsed) | 0 (0.00%) | `'1970-01-01 00:00:00.000000008'` | Raw elapsed duration formatted as nanosecond timestamp. Parsed in Silver to `INT` hours (`delivery_duration_hours`). Range: 0 to 19 hours. |
| `expected_time_hours` | String | `IntegerType` (parsed) | 0 (0.00%) | `'1970-01-01 00:00:00.000000008'` | Raw SLA delivery duration formatted as nanosecond timestamp. Parsed in Silver to `INT` hours (`expected_duration_hours`). Range: 2 to 24 hours. |
| `delayed` | String | `BooleanType` / `StringType` | 0 (0.00%) | `'no'`, `'yes'` | Binary indicator whether shipment exceeded SLA expected time. Standardized to boolean (`is_delayed`). |
| `delivery_status` | String | `StringType` | 0 (0.00%) | `'delivered'`, `'delayed'`, `'failed'` | Final delivery outcome. `delivered` (18,331), `delayed` (5,341), `failed` (1,328). |
| `delivery_rating` | String / Int | `IntegerType` | 0 (0.00%) | `3`, `5`, `4`, `2`, `1` | Customer feedback score on a scale from 1 (poor) to 5 (excellent). Mean: 3.67. |
| `delivery_cost` | String / Float | `DoubleType` | 0 (0.00%) | `1632.72`, `640.17`, `1448.17` | Total shipping cost incurred in Indian Rupees (INR). Range: ₹95.67 to ₹1632.72 (mean: ₹864.94). |

---

## 2. Engineered / Derived Metrics in Silver & Gold

1. **`shipment_pk`**: Unique surrogate key generated in Silver layer (`SHP_<ROW_ID>`) to resolve duplicate raw `delivery_id` values.
2. **`delivery_duration_hours`**: Extracted integer hours from `delivery_time_hours`.
3. **`expected_duration_hours`**: Extracted integer hours from `expected_time_hours`.
4. **`delay_variance_hours`**: `delivery_duration_hours - expected_duration_hours` (positive values indicate delay magnitude).
5. **`cost_per_km`**: `delivery_cost / distance_km` (efficiency benchmark in INR/km).
6. **`cost_per_kg`**: `delivery_cost / package_weight_kg` (cost per weight capacity).
7. **`on_time_flag`**: `CASE WHEN delivery_status = 'delivered' AND is_delayed = false THEN 1 ELSE 0 END`.
8. **`failure_flag`**: `CASE WHEN delivery_status = 'failed' THEN 1 ELSE 0 END`.
9. **`delivery_speed_kmh`**: `distance_km / delivery_duration_hours` (handling zero division edge cases).

---

## 3. Scope Limitations & Clarifications

1. **Temporal Calendar Scope:** The raw dataset does not contain calendar timestamps (e.g. `2024-03-15 14:00:00`), only relative durations (`delivery_time_hours` / `expected_time_hours`). To maintain data integrity without inventing fictional historical dates:
   - The batch pipeline uses pipeline execution metadata (`ingestion_timestamp`, `batch_date`) for partition keys and lineage.
   - For real-time streaming demonstration, a synthetic Kafka producer generates realistic timestamped status events (`ORDER_CREATED`, `PICKED_UP`, `IN_TRANSIT`, `OUT_FOR_DELIVERY`, `DELIVERED`, `DELAYED`, `FAILED`) adhering to the primary dataset's valid partners, modes, and categories.
2. **Geographical Granularity:** Data is provided at the macro zonal level (`north`, `south`, `east`, `west`, `central`) without specific pincodes or GPS lat/long coordinates.
