
"""
Spark Session factory for the Indian Logistics Data Pipeline.
Supports local Spark execution, optional Hive integration and Kafka streaming.
"""

import os
import sys

from pyspark.sql import SparkSession

from config.config import (
    SPARK_MASTER as DEFAULT_SPARK_MASTER,
    SPARK_APP_NAME_BATCH,
    HIVE_WAREHOUSE_DIR
)

from src.utils.logger import get_logger


logger = get_logger("SparkSessionFactory")


def get_spark_session(
    app_name: str = SPARK_APP_NAME_BATCH,
    enable_hive: bool = True
) -> SparkSession:

    spark_master = os.environ.get(
        "SPARK_MASTER",
        DEFAULT_SPARK_MASTER
    )

    logger.info(
        f"Initializing SparkSession: '{app_name}' "
        f"(Master: {spark_master})"
    )

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(spark_master)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.9"
        )
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.warehouse.dir", HIVE_WAREHOUSE_DIR)
        .config("spark.sql.adaptive.enabled", "true")
        .config(
            "spark.sql.adaptive.coalescePartitions.enabled",
            "true"
        )
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )

    if enable_hive:
        logger.info(
            f"Enabling Hive support. Warehouse: {HIVE_WAREHOUSE_DIR}"
        )
        builder = builder.enableHiveSupport()

    spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    logger.info(
        f"SparkSession created successfully. "
        f"Version: {spark.version}, "
        f"Master: {spark.sparkContext.master}"
    )

    return spark
