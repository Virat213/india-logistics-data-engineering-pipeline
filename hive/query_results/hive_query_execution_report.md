# HiveQL Analytical Query Execution Report
**Database:** `india_logistics_dw`  
**Total Queries Executed:** 7  
**Execution Timestamp:** 2026-11-17 19:07:39

---

## Query 1: Query 1: Overall Logistics Performance Overview

- **Execution Time:** 2.473 seconds
- **Record Count:** 1

```sql
SELECT 
    COUNT(*) AS total_shipments_processed,
    SUM(on_time_flag) AS total_on_time_deliveries,
    ROUND(SUM(on_time_flag) * 100.0 / COUNT(*), 2) AS overall_on_time_rate_pct,
    SUM(failure_flag) AS total_failed_deliveries,
    ROUND(SUM(failure_flag) * 100.0 / COUNT(*), 2) AS overall_failure_rate_pct,
    ROUND(AVG(delivery_duration_hours), 2) AS avg_delivery_duration_hrs,
    ROUND(AVG(delay_variance_hours), 2) AS avg_delay_variance_hrs,
    ROUND(AVG(delivery_cost), 2) AS avg_delivery_cost_inr,
    ROUND(AVG(delivery_rating), 2) AS avg_customer_satisfaction_rating
FROM fact_shipments
```

**Query Output:**

```
 total_shipments_processed  total_on_time_deliveries overall_on_time_rate_pct  total_failed_deliveries overall_failure_rate_pct  avg_delivery_duration_hrs  avg_delay_variance_hrs  avg_delivery_cost_inr  avg_customer_satisfaction_rating
                     25000                     18331                    73.32                     1328                     5.31                       6.25                   -6.86                 864.94                              3.67
```

---

## Query 2: Query 2: Partner Reliability & SLA Compliance Leaderboard

- **Execution Time:** 1.477 seconds
- **Record Count:** 9

```sql
SELECT 
    p.partner_display_name,
    p.headquarters_hub,
    p.partner_tier,
    COUNT(f.shipment_pk) AS total_orders,
    SUM(f.on_time_flag) AS on_time_orders,
    ROUND(SUM(f.on_time_flag) * 100.0 / COUNT(f.shipment_pk), 2) AS on_time_percentage,
    ROUND(AVG(f.delay_variance_hours), 2) AS avg_delay_variance_hours,
    ROUND(AVG(f.delivery_rating), 2) AS avg_rating,
    ROUND(AVG(f.cost_per_km), 2) AS avg_cost_per_km_inr,
    DENSE_RANK() OVER (ORDER BY SUM(f.on_time_flag) * 100.0 / COUNT(f.shipment_pk) DESC) AS reliability_rank
FROM fact_shipments f
JOIN dim_delivery_partner p ON f.partner_id = p.partner_id
GROUP BY p.partner_display_name, p.headquarters_hub, p.partner_tier
ORDER BY reliability_rank
```

**Query Output:**

```
 partner_display_name     headquarters_hub          partner_tier  total_orders  on_time_orders on_time_percentage  avg_delay_variance_hours  avg_rating  avg_cost_per_km_inr  reliability_rank
            Delhivery    Gurugram, Haryana  National 3PL Carrier          2786            2095              75.20                     -7.24        3.69                 7.02                 1
          FedEx India  Mumbai, Maharashtra        Global Express          2818            2109              74.84                     -6.94        3.70                 7.12                 2
    DHL Express India  Mumbai, Maharashtra        Global Express          2802            2064              73.66                     -6.95        3.69                 7.00                 3
         Ecom Express    Gurugram, Haryana  National 3PL Carrier          2722            1997              73.37                     -6.84        3.67                 7.04                 4
    Blue Dart Express  Mumbai, Maharashtra  National 3PL Carrier          2798            2044              73.05                     -6.71        3.68                 7.04                 5
Amazon Transportation Bengaluru, Karnataka Enterprise E-Commerce          2711            1973              72.78                     -6.84        3.64                 7.13                 6
            Shadowfax Bengaluru, Karnataka  National 3PL Carrier          2736            1989              72.70                     -6.78        3.66                 7.02                 7
      Ekart Logistics Bengaluru, Karnataka Enterprise E-Commerce          2801            2033              72.58                     -6.72        3.66                 7.05                 8
           Xpressbees    Pune, Maharashtra  National 3PL Carrier          2826            2027              71.73                     -6.72        3.61                 6.95                 9
```

---

## Query 3: Query 3: Regional Bottleneck Analysis & Delay Concentration

- **Execution Time:** 0.667 seconds
- **Record Count:** 5

```sql
SELECT 
    r.region_name,
    r.primary_hub,
    r.zone_geography,
    COUNT(f.shipment_pk) AS shipment_volume,
    SUM(CASE WHEN f.is_delayed = true THEN 1 ELSE 0 END) AS delayed_count,
    ROUND(SUM(CASE WHEN f.is_delayed = true THEN 1 ELSE 0 END) * 100.0 / COUNT(f.shipment_pk), 2) AS delay_percentage,
    ROUND(AVG(f.distance_km), 2) AS avg_route_distance_km,
    ROUND(AVG(f.delivery_duration_hours), 2) AS avg_duration_hours,
    ROUND(SUM(f.delivery_cost), 2) AS total_regional_spend_inr
FROM fact_shipments f
JOIN dim_region r ON f.region_id = r.region_id
GROUP BY r.region_name, r.primary_hub, r.zone_geography
ORDER BY delay_percentage DESC
```

**Query Output:**

```
   region_name   primary_hub                     zone_geography  shipment_volume  delayed_count delay_percentage  avg_route_distance_km  avg_duration_hours  total_regional_spend_inr
 Central India    Bhopal Hub            Central Transit Plateau             5060           1379            27.25                 151.04                6.28                4401952.01
 Western India    Mumbai Hub      Coastal & Industrial Corridor             5095           1373            26.95                 152.25                6.30                4454106.23
Southern India Bengaluru Hub           Peninsular Tech Corridor             4977           1333            26.78                 149.17                6.20                4271740.80
Northern India Delhi NCR Hub Gangetic Plains & Highway Corridor             4949           1315            26.57                 149.56                6.25                4258313.72
 Eastern India   Kolkata Hub      Eastern Gateway & River Basin             4919           1269            25.80                 149.86                6.21                4237501.72
```

---

## Query 4: Query 4: Environmental & Weather Impact on Delivery Speed and Failures

- **Execution Time:** 0.328 seconds
- **Record Count:** 6

```sql
SELECT 
    weather_condition,
    COUNT(*) AS total_shipments,
    ROUND(AVG(delivery_speed_kmh), 2) AS avg_speed_kmh,
    ROUND(AVG(delivery_duration_hours), 2) AS avg_delivery_time_hours,
    SUM(CASE WHEN is_delayed = true THEN 1 ELSE 0 END) AS delayed_deliveries,
    ROUND(SUM(CASE WHEN is_delayed = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS delay_rate_pct,
    SUM(failure_flag) AS failed_deliveries,
    ROUND(AVG(delivery_rating), 2) AS customer_rating
FROM fact_shipments
GROUP BY weather_condition
ORDER BY delay_rate_pct DESC
```

**Query Output:**

```
weather_condition  total_shipments  avg_speed_kmh  avg_delivery_time_hours  delayed_deliveries delay_rate_pct  failed_deliveries  customer_rating
           stormy             4198          16.10                     8.81                1740          41.45                374             3.35
            rainy             4171          18.59                     7.80                1558          37.35                308             3.47
            foggy             4219          22.26                     6.49                1279          30.32                250             3.60
            clear             4124          31.35                     4.78                 719          17.43                131             3.84
              hot             4130          31.56                     4.78                 707          17.12                134             3.86
             cold             4158          31.53                     4.77                 666          16.02                131             3.88
```

---

## Query 5: Query 5: Vehicle Fleet Electrification & Cost Efficiency (EV vs ICE)

- **Execution Time:** 0.873 seconds
- **Record Count:** 6

```sql
SELECT 
    v.vehicle_category,
    v.vehicle_type,
    v.is_electric,
    COUNT(f.shipment_pk) AS total_deliveries,
    ROUND(AVG(f.cost_per_km), 2) AS avg_cost_per_km_inr,
    ROUND(AVG(f.distance_km), 2) AS avg_distance_km,
    ROUND(AVG(f.delivery_speed_kmh), 2) AS avg_transit_speed_kmh,
    ROUND(SUM(f.on_time_flag) * 100.0 / COUNT(f.shipment_pk), 2) AS on_time_rate_pct,
    ROUND(AVG(f.delivery_rating), 2) AS avg_rating
FROM fact_shipments f
JOIN dim_vehicle v ON f.vehicle_id = v.vehicle_id
GROUP BY v.vehicle_category, v.vehicle_type, v.is_electric
ORDER BY avg_cost_per_km_inr ASC
```

**Query Output:**

```
        vehicle_category vehicle_type  is_electric  total_deliveries  avg_cost_per_km_inr  avg_distance_km  avg_transit_speed_kmh on_time_rate_pct  avg_rating
             Two-Wheeler      scooter        False              4174                 6.98           150.16                  24.90            73.86        3.68
             Two-Wheeler         bike        False              4160                 7.01           151.42                  25.26            72.96        3.65
     Heavy Freight Truck        truck        False              4145                 7.05           150.27                  25.41            72.96        3.67
Light Commercial Vehicle          van        False              4187                 7.05           148.77                  24.96            73.23        3.66
Light Commercial Vehicle       ev van         True              4116                 7.07           150.68                  25.32            73.37        3.68
             Two-Wheeler      ev bike         True              4218                 7.09           151.04                  25.30            73.57        3.67
```

---

## Query 6: Query 6: Regional Partner Ranking using Window Functions

- **Execution Time:** 0.692 seconds
- **Record Count:** 45

```sql
WITH PartnerRegionalVolume AS (
    SELECT 
        r.region_name,
        p.partner_display_name,
        COUNT(f.shipment_pk) AS orders_handled,
        ROUND(SUM(f.on_time_flag) * 100.0 / COUNT(f.shipment_pk), 2) AS on_time_pct,
        ROUND(AVG(f.cost_per_km), 2) AS avg_cost_km
    FROM fact_shipments f
    JOIN dim_region r ON f.region_id = r.region_id
    JOIN dim_delivery_partner p ON f.partner_id = p.partner_id
    GROUP BY r.region_name, p.partner_display_name
)
SELECT 
    region_name,
    partner_display_name,
    orders_handled,
    on_time_pct,
    avg_cost_km,
    ROW_NUMBER() OVER (PARTITION BY region_name ORDER BY on_time_pct DESC, orders_handled DESC) AS regional_rank
FROM PartnerRegionalVolume
ORDER BY region_name, regional_rank
```

**Query Output:**

```
   region_name  partner_display_name  orders_handled on_time_pct  avg_cost_km  regional_rank
 Central India     Blue Dart Express             565       73.98         6.85              1
 Central India             Shadowfax             561       73.80         7.14              2
 Central India           FedEx India             572       73.60         7.25              3
 Central India             Delhivery             590       73.22         7.00              4
 Central India          Ecom Express             528       73.11         6.97              5
 Central India     DHL Express India             596       72.48         7.16              6
 Central India       Ekart Logistics             530       72.08         7.20              7
 Central India Amazon Transportation             549       71.77         6.82              8
 Central India            Xpressbees             569       70.65         7.10              9
 Eastern India             Delhivery             546       77.11         7.23              1
 Eastern India           FedEx India             578       76.47         7.09              2
 Eastern India Amazon Transportation             513       76.02         7.17              3
 Eastern India       Ekart Logistics             591       74.11         6.95              4
 Eastern India     Blue Dart Express             533       74.11         6.99              5
 Eastern India     DHL Express India             583       73.41         6.99              6
 Eastern India             Shadowfax             511       73.39         6.91              7
 Eastern India          Ecom Express             524       72.33         7.33              8
 Eastern India            Xpressbees             540       70.74         6.81              9
Northern India             Delhivery             528       76.14         6.90              1
Northern India            Xpressbees             587       74.79         7.24              2
```

---

## Query 7: Query 7: Package Category Risk Analysis (Fragile & Pharmacy SLA Compliance)

- **Execution Time:** 0.717 seconds
- **Record Count:** 9

```sql
SELECT 
    pkg.package_type,
    pkg.requires_temperature_or_fragile_care,
    COUNT(f.shipment_pk) AS total_shipments,
    SUM(f.failure_flag) AS failed_shipments,
    ROUND(SUM(f.failure_flag) * 100.0 / COUNT(f.shipment_pk), 2) AS failure_rate_pct,
    ROUND(AVG(f.package_weight_kg), 2) AS avg_weight_kg,
    ROUND(AVG(f.cost_per_kg), 2) AS avg_cost_per_kg_inr,
    ROUND(AVG(f.delivery_rating), 2) AS avg_rating
FROM fact_shipments f
JOIN dim_package_type pkg ON f.package_type_id = pkg.package_type_id
GROUP BY pkg.package_type, pkg.requires_temperature_or_fragile_care
ORDER BY failure_rate_pct DESC
```

**Query Output:**

```
    package_type  requires_temperature_or_fragile_care  total_shipments  failed_shipments failure_rate_pct  avg_weight_kg  avg_cost_per_kg_inr  avg_rating
automobile parts                                 False             2795               161             5.76          24.93                84.05        3.69
        clothing                                 False             2767               152             5.49          25.26                84.05        3.69
       cosmetics                                 False             2744               148             5.39          25.14                83.36        3.64
       documents                                 False             2805               151             5.38          24.91                80.19        3.67
       furniture                                 False             2746               144             5.24          25.26                80.65        3.69
       groceries                                 False             2693               141             5.24          25.00                79.84        3.66
   fragile items                                  True             2848               149             5.23          25.32                82.67        3.66
        pharmacy                                  True             2810               142             5.05          25.23                85.65        3.65
     electronics                                  True             2792               140             5.01          25.26                75.15        3.65
```

---

