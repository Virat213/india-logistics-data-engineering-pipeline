
"""
Silver Layer Transformation & Data Quality Module.
Cleans raw bronze logistics data, resolves duplicate delivery IDs with canonical surrogate keys,
parses encoded hour durations, derives operational KPIs, and partitions by region.
"""

import sys
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, trim, lower, when, lit, concat, lpad,
    regexp_extract, round, current_timestamp, row_number
)
from pyspark.sql.window import Window

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.config import BRONZE_DATA_PATH, SILVER_DATA_PATH
from src.utils.logger import get_logger
from src.utils.spark_session import get_spark_session

logger = get_logger("SilverTransformation")


def validate_silver_data_quality(df: DataFrame) -> bool:

    logger.info("Executing Data Quality Validation Suite on Silver Data...")

    total_count = df.count()

    if total_count == 0:
        raise ValueError("Data Quality Failure: Silver dataset is empty!")

    distinct_pk_count = df.select("shipment_pk").distinct().count()

    if distinct_pk_count != total_count:
        raise ValueError(
            f"Data Quality Failure: shipment_pk uniqueness violation! "
            f"Total: {total_count}, Distinct PKs: {distinct_pk_count}"
        )

    logger.info(
        f"Check 1 PASS: shipment_pk is 100% unique ({distinct_pk_count} distinct keys)."
    )

    required_columns = [
        "distance_km",
        "package_weight_kg",
        "delivery_cost",
        "delivery_rating",
        "delivery_duration_hours",
        "expected_duration_hours"
    ]

    null_conditions = [
        col(column).isNull() for column in required_columns
    ]

    invalid_nulls = df.filter(
        null_conditions[0] |
        null_conditions[1] |
        null_conditions[2] |
        null_conditions[3] |
        null_conditions[4] |
        null_conditions[5]
    ).count()

    if invalid_nulls > 0:
        raise ValueError(
            f"Data Quality Failure: {invalid_nulls} records contain NULL values "
            f"in required numeric columns."
        )

    logger.info("Check 2 PASS: Required numeric columns contain no NULL values.")

    invalid_metrics = df.filter(
        (col("distance_km") <= 0) |
        (col("package_weight_kg") <= 0) |
        (col("delivery_cost") <= 0)
    ).count()

    if invalid_metrics > 0:
        raise ValueError(
            f"Data Quality Failure: Found {invalid_metrics} records "
            f"with invalid non-positive metrics!"
        )

    logger.info("Check 3 PASS: All distance, weight, and cost values are positive.")

    invalid_ratings = df.filter(
        (col("delivery_rating") < 1) |
        (col("delivery_rating") > 5)
    ).count()

    if invalid_ratings > 0:
        raise ValueError(
            f"Data Quality Failure: Found {invalid_ratings} records "
            f"with delivery_rating outside [1, 5]!"
        )

    logger.info("Check 4 PASS: All delivery ratings are within [1, 5].")

    invalid_durations = df.filter(
        (col("delivery_duration_hours") < 0) |
        (col("expected_duration_hours") <= 0)
    ).count()

    if invalid_durations > 0:
        raise ValueError(
            f"Data Quality Failure: Found {invalid_durations} "
            f"records with invalid durations!"
        )

    logger.info("Check 5 PASS: All delivery and expected duration hours are valid.")

    logger.info("All Silver Data Quality checks PASSED successfully!")

    return True


def transform_bronze_to_silver(
    spark,
    bronze_path: Path = BRONZE_DATA_PATH,
    silver_path: Path = SILVER_DATA_PATH
) -> DataFrame:

    logger.info(f"Loading Bronze dataset from: {bronze_path}")

    df_bronze = spark.read.parquet(str(bronze_path))

    numeric_columns = {
        "distance_km": "double",
        "package_weight_kg": "double",
        "delivery_rating": "int",
        "delivery_cost": "double"
    }

    for column_name, data_type in numeric_columns.items():
        df_bronze = df_bronze.withColumn(
            column_name,
            col(column_name).cast(data_type)
        )

    window_spec = Window.orderBy(
        col("ingestion_timestamp"),
        col("delivery_id")
    )

    df_with_pk = df_bronze \
        .withColumn("row_num", row_number().over(window_spec)) \
        .withColumn(
            "shipment_pk",
            concat(lit("SHP_"), lpad(col("row_num"), 6, "0"))
        ) \
        .withColumnRenamed("delivery_id", "raw_delivery_id") \
        .drop("row_num")

    categorical_columns = [
        "delivery_partner",
        "package_type",
        "vehicle_type",
        "delivery_mode",
        "region",
        "weather_condition",
        "delivery_status"
    ]

    df_standardized = df_with_pk

    for column_name in categorical_columns:
        df_standardized = df_standardized.withColumn(
            column_name,
            lower(trim(col(column_name)))
        )

    df_parsed_time = df_standardized \
        .withColumn(
            "delivery_duration_hours",
            regexp_extract(
                col("delivery_time_hours"),
                r"\.(\d+)$",
                1
            ).cast("int")
        ) \
        .withColumn(
            "expected_duration_hours",
            regexp_extract(
                col("expected_time_hours"),
                r"\.(\d+)$",
                1
            ).cast("int")
        ) \
        .drop("delivery_time_hours", "expected_time_hours")

    df_enriched = df_parsed_time \
        .withColumn(
            "delay_variance_hours",
            col("delivery_duration_hours") - col("expected_duration_hours")
        ) \
        .withColumn(
            "is_delayed",
            when(lower(trim(col("delayed"))) == "yes", True).otherwise(False)
        ) \
        .withColumn(
            "on_time_flag",
            when(
                (col("delivery_status") == "delivered") &
                (~col("is_delayed")),
                1
            ).otherwise(0)
        ) \
        .withColumn(
            "failure_flag",
            when(col("delivery_status") == "failed", 1).otherwise(0)
        ) \
        .withColumn(
            "cost_per_km",
            round(col("delivery_cost") / col("distance_km"), 2)
        ) \
        .withColumn(
            "cost_per_kg",
            round(col("delivery_cost") / col("package_weight_kg"), 2)
        ) \
        .withColumn(
            "delivery_speed_kmh",
            round(
                col("distance_km") /
                when(
                    col("delivery_duration_hours") == 0,
                    lit(1.0)
                ).otherwise(col("delivery_duration_hours").cast("double")),
                2
            )
        ) \
        .withColumn(
            "sla_compliance",
            when(col("delivery_status") == "failed", "FAILED")
            .when(col("delay_variance_hours") <= 0, "MET")
            .otherwise("BREACHED")
        ) \
        .withColumn("processed_timestamp", current_timestamp())

    validate_silver_data_quality(df_enriched)

    logger.info(f"Writing partitioned Silver dataset to: {silver_path}")

    df_enriched.write \
        .mode("overwrite") \
        .partitionBy("region") \
        .parquet(str(silver_path))

    logger.info("Silver transformation and partition write completed successfully!")

    return df_enriched


if __name__ == "__main__":

    spark = get_spark_session(
        "Silver-Transformation-Job",
        enable_hive=False
    )

    try:
        silver_df = transform_bronze_to_silver(spark)

        silver_df.printSchema()

        logger.info("Sample Silver records:")

        silver_df.select(
            "shipment_pk",
            "delivery_partner",
            "region",
            "delivery_duration_hours",
            "expected_duration_hours",
            "delay_variance_hours",
            "delivery_speed_kmh",
            "sla_compliance",
            "cost_per_km"
        ).show(5, truncate=False)

    finally:
        spark.stop()
