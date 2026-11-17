"""
Configuration module for the Indian Logistics Data Engineering Pipeline.
Centralizes all paths, database names, Kafka configurations, and Spark settings.
"""

import os
from pathlib import Path

# Base Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_PATH = DATA_DIR / "raw" / "Delivery_Logistics.csv"
BRONZE_DATA_PATH = DATA_DIR / "bronze" / "shipments_bronze.parquet"
SILVER_DATA_PATH = DATA_DIR / "silver" / "shipments_silver.parquet"
GOLD_DATA_PATH = DATA_DIR / "gold"
POWERBI_DATA_PATH = PROJECT_ROOT / "powerbi" / "data"

# Hive Data Warehouse Configurations
HIVE_WAREHOUSE_DIR = str(PROJECT_ROOT / "hive" / "warehouse")
HIVE_METASTORE_DB = str(PROJECT_ROOT / "hive" / "metastore_db")
HIVE_DATABASE_NAME = "india_logistics_dw"

# Kafka Streaming Configurations
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_SHIPMENTS = "india.logistics.shipment_events"
STREAMING_CHECKPOINT_DIR = str(PROJECT_ROOT / "data" / "streaming" / "checkpoints")
STREAMING_OUTPUT_DIR = str(PROJECT_ROOT / "data" / "streaming" / "events_parquet")

# Spark Application Settings
SPARK_APP_NAME_BATCH = "IndiaLogistics-BatchPipeline"
SPARK_APP_NAME_STREAMING = "IndiaLogistics-RealtimeStreaming"
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[2]")
