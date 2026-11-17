"""
Gold Layer Dimensional Modeling & Analytics Module.
Constructs a Kimball-style Star Schema (Fact and Conformed Dimensions)
and pre-aggregated business analytical tables optimized for Power BI and Hive.
"""

import sys
from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, round, when, lit,
    monotonically_increasing_id, concat, lpad
)

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.config import SILVER_DATA_PATH, GOLD_DATA_PATH, POWERBI_DATA_PATH
from src.utils.logger import get_logger
from src.utils.spark_session import get_spark_session

logger = get_logger("GoldAnalytics")

def build_dimension_tables(df_silver: DataFrame):
    """
    Constructs conformed dimension tables using pure Catalyst expressions.
    """
    logger.info("Building conformed dimension tables...")
    
    # 1. Dimension: Delivery Partner
    dim_partner = df_silver.select("delivery_partner").distinct() \
        .withColumn("partner_id", concat(lit("DPT_"), lpad(monotonically_increasing_id() + 1, 3, "0"))) \
        .withColumn(
            "partner_display_name",
            when(col("delivery_partner") == "delhivery", "Delhivery")
            .when(col("delivery_partner") == "xpressbees", "Xpressbees")
            .when(col("delivery_partner") == "shadowfax", "Shadowfax")
            .when(col("delivery_partner") == "blue dart", "Blue Dart Express")
            .when(col("delivery_partner") == "ekart", "Ekart Logistics")
            .when(col("delivery_partner") == "fedex", "FedEx India")
            .when(col("delivery_partner") == "dhl", "DHL Express India")
            .when(col("delivery_partner") == "ecom express", "Ecom Express")
            .otherwise("Amazon Transportation")
        ) \
        .withColumn(
            "headquarters_hub",
            when(col("delivery_partner").isin("delhivery", "ecom express"), "Gurugram, Haryana")
            .when(col("delivery_partner").isin("shadowfax", "ekart", "amazon logistics"), "Bengaluru, Karnataka")
            .when(col("delivery_partner").isin("blue dart", "fedex", "dhl"), "Mumbai, Maharashtra")
            .otherwise("Pune, Maharashtra")
        ) \
        .withColumn(
            "partner_tier",
            when(col("delivery_partner").isin("fedex", "dhl"), "Global Express")
            .when(col("delivery_partner").isin("amazon logistics", "ekart"), "Enterprise E-Commerce")
            .otherwise("National 3PL Carrier")
        )
        
    # 2. Dimension: Region
    dim_region = df_silver.select("region").distinct() \
        .withColumn("region_id", concat(lit("RGN_"), lpad(monotonically_increasing_id() + 1, 3, "0"))) \
        .withColumn(
            "region_name",
            when(col("region") == "west", "Western India")
            .when(col("region") == "central", "Central India")
            .when(col("region") == "south", "Southern India")
            .when(col("region") == "north", "Northern India")
            .otherwise("Eastern India")
        ) \
        .withColumn(
            "primary_hub",
            when(col("region") == "west", "Mumbai Hub")
            .when(col("region") == "central", "Bhopal Hub")
            .when(col("region") == "south", "Bengaluru Hub")
            .when(col("region") == "north", "Delhi NCR Hub")
            .otherwise("Kolkata Hub")
        ) \
        .withColumn(
            "zone_geography",
            when(col("region") == "west", "Coastal & Industrial Corridor")
            .when(col("region") == "central", "Central Transit Plateau")
            .when(col("region") == "south", "Peninsular Tech Corridor")
            .when(col("region") == "north", "Gangetic Plains & Highway Corridor")
            .otherwise("Eastern Gateway & River Basin")
        )

    # 3. Dimension: Vehicle
    dim_vehicle = df_silver.select("vehicle_type").distinct() \
        .withColumn("vehicle_id", concat(lit("VEH_"), lpad(monotonically_increasing_id() + 1, 3, "0"))) \
        .withColumn("is_electric", when(col("vehicle_type").isin("ev bike", "ev van"), True).otherwise(False)) \
        .withColumn(
            "vehicle_category",
            when(col("vehicle_type").isin("bike", "scooter", "ev bike"), "Two-Wheeler")
            .when(col("vehicle_type").isin("van", "ev van"), "Light Commercial Vehicle")
            .otherwise("Heavy Freight Truck")
        )

    # 4. Dimension: Package Type
    dim_package = df_silver.select("package_type").distinct() \
        .withColumn("package_type_id", concat(lit("PKG_"), lpad(monotonically_increasing_id() + 1, 3, "0"))) \
        .withColumn(
            "requires_temperature_or_fragile_care",
            when(col("package_type").isin("fragile items", "pharmacy", "electronics"), True).otherwise(False)
        )
        
    return dim_partner, dim_region, dim_vehicle, dim_package

def build_fact_and_aggregations(spark, silver_path: Path = SILVER_DATA_PATH):
    """
    Constructs the Star Schema fact table and Gold analytics marts.
    """
    logger.info(f"Loading Silver dataset from: {silver_path}")
    df_silver = spark.read.parquet(str(silver_path))
    
    # Build Dimensions
    dim_partner, dim_region, dim_vehicle, dim_package = build_dimension_tables(df_silver)
    
    # Build Fact Table: fact_shipments
    logger.info("Constructing Star Schema fact table: fact_shipments...")
    fact_shipments = df_silver \
        .join(dim_partner.select("delivery_partner", "partner_id"), on="delivery_partner", how="left") \
        .join(dim_region.select("region", "region_id"), on="region", how="left") \
        .join(dim_vehicle.select("vehicle_type", "vehicle_id"), on="vehicle_type", how="left") \
        .join(dim_package.select("package_type", "package_type_id"), on="package_type", how="left") \
        .select(
            "shipment_pk",
            "partner_id",
            "region_id",
            "vehicle_id",
            "package_type_id",
            "raw_delivery_id",
            "delivery_mode",
            "weather_condition",
            "distance_km",
            "package_weight_kg",
            "delivery_duration_hours",
            "expected_duration_hours",
            "delay_variance_hours",
            "delivery_status",
            "is_delayed",
            "on_time_flag",
            "failure_flag",
            "delivery_rating",
            "delivery_cost",
            "cost_per_km",
            "cost_per_kg",
            "delivery_speed_kmh",
            "sla_compliance",
            "processed_timestamp"
        )
        
    # --- Gold Aggregation Table 1: Partner KPI Summary ---
    logger.info("Generating Gold Aggregation Mart: gold_partner_kpi...")
    gold_partner_kpi = df_silver.groupBy("delivery_partner").agg(
        count("shipment_pk").alias("total_shipments"),
        spark_sum("on_time_flag").alias("on_time_shipments"),
        spark_sum(when(col("is_delayed"), 1).otherwise(0)).alias("delayed_shipments"),
        spark_sum("failure_flag").alias("failed_shipments"),
        round(spark_sum("on_time_flag") * 100.0 / count("shipment_pk"), 2).alias("on_time_delivery_rate_pct"),
        round(spark_sum("failure_flag") * 100.0 / count("shipment_pk"), 2).alias("failure_rate_pct"),
        round(avg("delivery_duration_hours"), 2).alias("avg_delivery_duration_hours"),
        round(avg("delay_variance_hours"), 2).alias("avg_delay_variance_hours"),
        round(avg("delivery_rating"), 2).alias("avg_customer_rating"),
        round(spark_sum("delivery_cost"), 2).alias("total_logistics_cost_inr"),
        round(avg("cost_per_km"), 2).alias("avg_cost_per_km"),
        round(avg("delivery_speed_kmh"), 2).alias("avg_delivery_speed_kmh")
    ).withColumn(
        "carrier_efficiency_score",
        round((col("on_time_delivery_rate_pct") * col("avg_customer_rating")) / col("avg_cost_per_km"), 2)
    ).orderBy(col("on_time_delivery_rate_pct").desc())

    # --- Gold Aggregation Table 2: Regional Performance Summary ---
    logger.info("Generating Gold Aggregation Mart: gold_regional_performance...")
    gold_regional_performance = df_silver.groupBy("region").agg(
        count("shipment_pk").alias("total_shipments"),
        spark_sum("on_time_flag").alias("on_time_shipments"),
        spark_sum(when(col("is_delayed"), 1).otherwise(0)).alias("delayed_shipments"),
        spark_sum("failure_flag").alias("failed_shipments"),
        round(spark_sum("on_time_flag") * 100.0 / count("shipment_pk"), 2).alias("on_time_rate_pct"),
        round(avg("distance_km"), 2).alias("avg_distance_km"),
        round(avg("delivery_duration_hours"), 2).alias("avg_delivery_duration_hours"),
        round(spark_sum("delivery_cost"), 2).alias("total_regional_cost_inr"),
        round(avg("cost_per_km"), 2).alias("avg_cost_per_km")
    ).orderBy(col("total_shipments").desc())

    # --- Gold Aggregation Table 3: Weather Impact Analysis ---
    logger.info("Generating Gold Aggregation Mart: gold_weather_impact...")
    gold_weather_impact = df_silver.groupBy("weather_condition").agg(
        count("shipment_pk").alias("total_shipments"),
        spark_sum(when(col("is_delayed"), 1).otherwise(0)).alias("delayed_shipments"),
        round(spark_sum(when(col("is_delayed"), 1).otherwise(0)) * 100.0 / count("shipment_pk"), 2).alias("delay_rate_pct"),
        round(avg("delivery_speed_kmh"), 2).alias("avg_speed_kmh"),
        round(avg("delivery_duration_hours"), 2).alias("avg_duration_hours"),
        round(avg("delivery_rating"), 2).alias("avg_rating"),
        spark_sum("failure_flag").alias("failed_shipments")
    ).orderBy(col("delay_rate_pct").desc())

    # --- Gold Aggregation Table 4: Vehicle & EV Efficiency ---
    logger.info("Generating Gold Aggregation Mart: gold_vehicle_efficiency...")
    gold_vehicle_efficiency = df_silver.groupBy("vehicle_type").agg(
        count("shipment_pk").alias("total_deliveries"),
        round(avg("cost_per_km"), 2).alias("avg_cost_per_km"),
        round(avg("distance_km"), 2).alias("avg_distance_km"),
        round(avg("delivery_speed_kmh"), 2).alias("avg_speed_kmh"),
        round(spark_sum("on_time_flag") * 100.0 / count("shipment_pk"), 2).alias("on_time_pct"),
        round(avg("delivery_rating"), 2).alias("avg_rating")
    ).withColumn("is_ev", when(col("vehicle_type").isin("ev bike", "ev van"), True).otherwise(False)) \
     .orderBy(col("avg_cost_per_km").asc())

    # Export dictionary of tables
    gold_tables = {
        "dim_delivery_partner": dim_partner,
        "dim_region": dim_region,
        "dim_vehicle": dim_vehicle,
        "dim_package_type": dim_package,
        "fact_shipments": fact_shipments,
        "gold_partner_kpi": gold_partner_kpi,
        "gold_regional_performance": gold_regional_performance,
        "gold_weather_impact": gold_weather_impact,
        "gold_vehicle_efficiency": gold_vehicle_efficiency
    }

    # Save outputs as Parquet in data/gold and CSV in powerbi/data
    Path(GOLD_DATA_PATH).mkdir(parents=True, exist_ok=True)
    Path(POWERBI_DATA_PATH).mkdir(parents=True, exist_ok=True)

    for name, df in gold_tables.items():
        parquet_out = Path(GOLD_DATA_PATH) / f"{name}.parquet"
        csv_out = Path(POWERBI_DATA_PATH) / f"{name}.csv"
        
        logger.info(f"Writing Parquet: {parquet_out}")
        df.write.mode("overwrite").parquet(str(parquet_out))
        
        logger.info(f"Writing Power BI CSV export: {csv_out}")
        # Convert to Pandas for clean single CSV file export
        pdf = df.toPandas()
        pdf.to_csv(str(csv_out), index=False, encoding="utf-8")

    logger.info("All Gold tables and Power BI CSV exports generated successfully!")
    return gold_tables

if __name__ == "__main__":
    spark = get_spark_session("Gold-Analytics-Job", enable_hive=False)
    try:
        gold_marts = build_fact_and_aggregations(spark)
        logger.info("\n--- Partner KPI Leaderboard ---")
        gold_marts["gold_partner_kpi"].show(10, truncate=False)
        logger.info("\n--- Regional Performance Summary ---")
        gold_marts["gold_regional_performance"].show(10, truncate=False)
    finally:
        spark.stop()
