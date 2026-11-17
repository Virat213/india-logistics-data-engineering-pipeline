"""
Hive Data Warehouse Initialization & Schema Definition Module.
Creates the `india_logistics_dw` database and registers conformed Star Schema
and analytical tables pointing to the Parquet storage layer.
"""

import sys
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.config import GOLD_DATA_PATH, HIVE_DATABASE_NAME
from src.utils.logger import get_logger
from src.utils.spark_session import get_spark_session

logger = get_logger("HiveWarehouseInit")

def init_hive_schema(spark):
    """
    Creates the Hive database and registers external Parquet tables.
    """
    logger.info(f"Initializing Apache Hive Data Warehouse: '{HIVE_DATABASE_NAME}'")
    
    # 1. Create Hive Database
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DATABASE_NAME}")
    spark.sql(f"USE {HIVE_DATABASE_NAME}")
    logger.info(f"Active Hive Database: {HIVE_DATABASE_NAME}")
    
    # 2. Register External Tables over Parquet Gold Storage
    tables = [
        "dim_delivery_partner",
        "dim_region",
        "dim_vehicle",
        "dim_package_type",
        "fact_shipments",
        "gold_partner_kpi",
        "gold_regional_performance",
        "gold_weather_impact",
        "gold_vehicle_efficiency"
    ]
    
    for tbl in tables:
        parquet_path = Path(GOLD_DATA_PATH) / f"{tbl}.parquet"
        # Convert path to forward slashes for HiveQL compatibility
        parquet_uri = str(parquet_path).replace("\\", "/")
        
        logger.info(f"Registering Hive External Table: `{HIVE_DATABASE_NAME}.{tbl}` from {parquet_uri}")
        
        # Drop existing table definition if present to ensure clean schema sync
        spark.sql(f"DROP TABLE IF EXISTS {tbl}")
        
        # Create external table over Parquet storage
        # Spark SQL infers schema directly from the Parquet metadata
        spark.sql(f"""
            CREATE TABLE {tbl}
            USING PARQUET
            LOCATION '{parquet_uri}'
        """)
        
    logger.info("Verifying registered Hive tables:")
    tables_df = spark.sql("SHOW TABLES")
    tables_df.show(truncate=False)
    
    logger.info("Hive Data Warehouse initialized successfully!")
    return tables_df

if __name__ == "__main__":
    spark = get_spark_session("Hive-Init-Job", enable_hive=True)
    try:
        init_hive_schema(spark)
    finally:
        spark.stop()
