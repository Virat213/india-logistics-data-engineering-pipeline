
"""
Bronze Layer Ingestion Module.
Ingests raw Indian logistics CSV data into the Bronze Parquet data lake layer,
attaching data governance, lineage, and audit metadata.
"""

import sys
import uuid
from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.config import RAW_DATA_PATH, BRONZE_DATA_PATH
from src.utils.logger import get_logger
from src.utils.spark_session import get_spark_session

logger = get_logger("BronzeIngestion")


def ingest_raw_to_bronze(
    spark,
    raw_path: Path = RAW_DATA_PATH,
    bronze_path: Path = BRONZE_DATA_PATH
) -> DataFrame:

    logger.info(f"Starting Bronze Ingestion from: {raw_path}")

    if not Path(raw_path).exists():
        raise FileNotFoundError(f"Raw dataset not found at: {raw_path}")

    batch_id = str(uuid.uuid4())
    logger.info(f"Generated Pipeline Batch ID: {batch_id}")

    df_raw = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .csv(str(raw_path))

    raw_count = df_raw.count()

    logger.info(
        f"Raw CSV loaded. Row count: {raw_count}, "
        f"Column count: {len(df_raw.columns)}"
    )

    logger.info(f"Source columns: {df_raw.columns}")

    df_bronze = df_raw \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_file", lit(Path(raw_path).name)) \
        .withColumn("batch_id", lit(batch_id)) \
        .withColumn("record_status", lit("RAW_INGESTED"))

    logger.info(f"Writing Bronze dataset to Parquet: {bronze_path}")

    df_bronze.write \
        .mode("overwrite") \
        .parquet(str(bronze_path))

    logger.info(
        f"Bronze Ingestion completed successfully! "
        f"Total records preserved: {raw_count}"
    )

    return df_bronze


if __name__ == "__main__":
    spark = get_spark_session("Bronze-Ingestion-Job", enable_hive=False)

    try:
        bronze_df = ingest_raw_to_bronze(spark)
        bronze_df.printSchema()
        logger.info("Sample Bronze records:")
        bronze_df.show(3, truncate=False)
    finally:
        spark.stop()

