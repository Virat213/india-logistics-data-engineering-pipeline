-- ============================================================================
-- Indian Logistics Data Warehouse Analytical Queries (Apache Hive / Spark SQL)
-- Database: india_logistics_dw
-- ============================================================================

USE india_logistics_dw;

-- Query 1: Overall Logistics Performance Overview
-- High-level executive KPI scorecard
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
FROM fact_shipments;

-- Query 2: Partner Reliability & SLA Compliance Leaderboard
-- Measures 3PL carrier SLA compliance and customer feedback
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
ORDER BY reliability_rank;

-- Query 3: Regional Bottleneck Analysis & Delay Concentration
-- Evaluates which Indian geographic zones experience the highest delay intensity
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
ORDER BY delay_percentage DESC;

-- Query 4: Environmental & Weather Impact on Delivery Speed and Failures
-- Analyzes how monsoon/fog/storm conditions affect delivery reliability
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
ORDER BY delay_rate_pct DESC;

-- Query 5: Vehicle Fleet Electrification & Cost Efficiency (EV vs ICE)
-- Compares Electric Vehicles (EV Bike, EV Van) against conventional fossil fuel fleet
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
ORDER BY avg_cost_per_km_inr ASC;

-- Query 6: Regional Partner Ranking using Window Functions
-- Ranks each carrier within each regional hub to identify regional champions
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
ORDER BY region_name, regional_rank;

-- Query 7: Package Category Risk Analysis (Fragile & Pharmacy SLA Compliance)
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
ORDER BY failure_rate_pct DESC;
