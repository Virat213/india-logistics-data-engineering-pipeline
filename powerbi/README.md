# Power BI Analytics Dashboard & Data Model Guide

**Project:** Indian Logistics Data Engineering Pipeline  
**Target Environment:** Power BI Desktop  
**Data Export Path:** `powerbi/data/` (or Parquet from `data/gold/`)  

---

## 1. Data Model Architecture (Star Schema)

The analytics data model follows a strict Kimball dimensional modeling pattern designed for fast aggregation and clean cross-filtering in Power BI.

```
                  +-------------------------+
                  |  dim_delivery_partner   |
                  |-------------------------|
                  | partner_id (PK)         |
                  | delivery_partner        |
                  | partner_display_name    |
                  | headquarters_hub        |
                  | partner_tier            |
                  +------------+------------+
                               | 1
                               |
                               | *
+--------------------+         |         +--------------------+
|     dim_region     |         |         |    dim_vehicle     |
|--------------------|         |         |--------------------|
| region_id (PK)     | 1       |       1 | vehicle_id (PK)    |
| region             |---------+---------| vehicle_type       |
| region_name        |         |         | is_electric        |
| primary_hub        |         |         | vehicle_category   |
| zone_geography     |         |         +--------------------+
+--------------------+         |
                               |
                               | *
                  +------------+------------+
                  |      fact_shipments     |
                  |-------------------------|
                  | shipment_pk (PK)        |
                  | partner_id (FK)         |
                  | region_id (FK)          |
                  | vehicle_id (FK)         |
                  | package_type_id (FK)    |
                  | distance_km             |
                  | package_weight_kg       |
                  | delivery_duration_hours |
                  | expected_duration_hours |
                  | delay_variance_hours    |
                  | is_delayed              |
                  | on_time_flag            |
                  | failure_flag            |
                  | delivery_rating         |
                  | delivery_cost           |
                  | cost_per_km             |
                  | delivery_speed_kmh      |
                  | sla_compliance          |
                  +------------+------------+
                               | *
                               |
                               | 1
                  +------------+------------+
                  |    dim_package_type     |
                  |-------------------------|
                  | package_type_id (PK)    |
                  | package_type            |
                  | requires_temp_care      |
                  +-------------------------+
```

### Table Relationships in Power BI Model View:
1. `fact_shipments[partner_id]` (`*`) to `dim_delivery_partner[partner_id]` (`1`) — Single direction.
2. `fact_shipments[region_id]` (`*`) to `dim_region[region_id]` (`1`) — Single direction.
3. `fact_shipments[vehicle_id]` (`*`) to `dim_vehicle[vehicle_id]` (`1`) — Single direction.
4. `fact_shipments[package_type_id]` (`*`) to `dim_package_type[package_type_id]` (`1`) — Single direction.

---

## 2. DAX Measures Specification

Create a dedicated Measures Table in Power BI named `_Measures` and add the following DAX calculations:

### Volume & Delivery Performance Measures:
```dax
Total Shipments = COUNTROWS(fact_shipments)
```
```dax
On-Time Deliveries = CALCULATE(COUNTROWS(fact_shipments), fact_shipments[on_time_flag] = 1)
```
```dax
Delayed Deliveries = CALCULATE(COUNTROWS(fact_shipments), fact_shipments[is_delayed] = TRUE())
```
```dax
Failed Deliveries = CALCULATE(COUNTROWS(fact_shipments), fact_shipments[failure_flag] = 1)
```
```dax
On-Time Delivery Rate (%) = 
DIVIDE([On-Time Deliveries], [Total Shipments], 0) * 100
```
```dax
Delay Rate (%) = 
DIVIDE([Delayed Deliveries], [Total Shipments], 0) * 100
```
```dax
Failure Rate (%) = 
DIVIDE([Failed Deliveries], [Total Shipments], 0) * 100
```

### Operational Speed & Duration Measures:
```dax
Avg Delivery Duration (Hrs) = AVERAGE(fact_shipments[delivery_duration_hours])
```
```dax
Avg Expected SLA (Hrs) = AVERAGE(fact_shipments[expected_duration_hours])
```
```dax
Avg Delay Variance (Hrs) = AVERAGE(fact_shipments[delay_variance_hours])
```
```dax
Avg Transit Speed (KM/H) = AVERAGE(fact_shipments[delivery_speed_kmh])
```

### Financial & Customer Satisfaction Measures:
```dax
Total Logistics Spend (INR) = SUM(fact_shipments[delivery_cost])
```
```dax
Avg Delivery Cost (INR) = AVERAGE(fact_shipments[delivery_cost])
```
```dax
Avg Cost per KM (INR) = AVERAGE(fact_shipments[cost_per_km])
```
```dax
Avg Customer Rating = AVERAGE(fact_shipments[delivery_rating])
```
```dax
Carrier Efficiency Score = 
DIVIDE([On-Time Delivery Rate (%)] * [Avg Customer Rating], [Avg Cost per KM (INR)], 0)
```

---

## 3. Recommended Dashboard Pages & Visual Wireframes

### Page 1: Executive Logistics Overview
- **Header:** Date Slicer, Regional Filter, Delivery Mode Slicer.
- **Top KPI Cards (Row 1):**
  - Card 1: `Total Shipments` (25,000)
  - Card 2: `On-Time Delivery Rate (%)` (73.3%)
  - Card 3: `Average Delivery Time` (6.25 Hrs)
  - Card 4: `Total Logistics Spend` (₹2.16 Cr)
  - Card 5: `Average Rating` (3.67 ★)
- **Visual 1 (Donut Chart):** Delivery Outcome Split (`delivered` 73.3%, `delayed` 21.4%, `failed` 5.3%).
- **Visual 2 (Clustered Column Chart):** Shipment Volume & On-time Rate by Region (`dim_region[region_name]`).
- **Visual 3 (Bar Chart):** Delivery Volume by SLA Mode (`delivery_mode`: Same Day, Express, Two Day, Standard).
- **Visual 4 (Table Matrix):** Carrier Overview (Volume, On-time %, Total Cost, Avg Rating).

### Page 2: 3PL Carrier SLA & Reliability Benchmarking
- **Visual 1 (Scatter Plot):** Carrier Trade-off Analysis.
  - X-Axis: `Avg Cost per KM (INR)`
  - Y-Axis: `On-Time Delivery Rate (%)`
  - Bubble Size: `Total Shipments`
  - Legend: `dim_delivery_partner[partner_display_name]`
- **Visual 2 (Horizontal Bar Chart):** Carriers Ranked by `Carrier Efficiency Score`.
- **Visual 3 (Clustered Bar Chart):** Average Delay Hours vs Average Delivery Rating by Carrier.
- **Visual 4 (Decomposition Tree):** Root Cause of Breached SLAs (`sla_compliance` -> `weather_condition` -> `vehicle_type`).

### Page 3: Route, Weather & Regional Bottlenecks
- **Visual 1 (Heatmap Matrix):** Delay Rate (%) across `region` (rows) vs `weather_condition` (columns). Highlights weather vulnerabilities in eastern/northern corridors.
- **Visual 2 (Line Chart):** Delivery Speed (km/h) across Weather Conditions (Foggy, Stormy, Rainy, Cold, Clear, Hot).
- **Visual 3 (Clustered Column):** Package Category Risk (`dim_package_type[package_type]`) vs Failure Rate (%) and Weight (kg).

### Page 4: Fleet Electrification & Cost Efficiency (EV vs ICE)
- **Visual 1 (Clustered Column Chart):** `Avg Cost per KM (INR)` by `dim_vehicle[vehicle_type]` (EV Bike, EV Van vs Bike, Van, Truck).
- **Visual 2 (Donut Chart):** Share of Deliveries fulfilled by Green Fleet (`is_electric`).
- **Visual 3 (Bar Chart):** Average Transit Speed by Vehicle Class.
- **Visual 4 (Card Matrix):** Estimated Fuel Cost Savings achieved by EV fleet.

---

## 4. Step-by-Step Connection Instructions for Power BI Desktop

1. **Launch Power BI Desktop** on your computer.
2. Click **Get Data** > **Text/CSV** (or **Folder**).
3. Browse to the project data directory:
   `C:\Users\Virat\.gemini\antigravity\scratch\india-logistics-pipeline\powerbi\data\`
4. Load the following 5 core Star Schema CSV files:
   - `fact_shipments.csv`
   - `dim_delivery_partner.csv`
   - `dim_region.csv`
   - `dim_vehicle.csv`
   - `dim_package_type.csv`
5. Switch to **Model View** (left sidebar icon with table boxes).
6. Verify and set relationships:
   - Drag `dim_delivery_partner[partner_id]` to `fact_shipments[partner_id]`.
   - Drag `dim_region[region_id]` to `fact_shipments[region_id]`.
   - Drag `dim_vehicle[vehicle_id]` to `fact_shipments[vehicle_id]`.
   - Drag `dim_package_type[package_type_id]` to `fact_shipments[package_type_id]`.
7. Go to **Report View**, click **New Measure**, and paste the DAX measures from Section 2.
8. Arrange the visuals on your canvas according to the wireframes in Section 3.
9. Save your completed dashboard as `india_logistics_analytics.pbix`.
