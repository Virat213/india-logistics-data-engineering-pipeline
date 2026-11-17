
import sys
import time
import socket
from pathlib import Path

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    BooleanType
)

from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    count,
    avg,
    sum as spark_sum,
    round as spark_round,
    when,
    current_timestamp
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_SHIPMENTS,
    STREAMING_CHECKPOINT_DIR,
    STREAMING_OUTPUT_DIR
)

from src.utils.logger import get_logger
from src.utils.spark_session import get_spark_session

logger = get_logger("SparkStreamingConsumer")


def is_kafka_broker_active(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    timeout=0.5
):
    try:
        host, port = bootstrap_servers.split(",")[0].split(":")
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except Exception:
        return False


def get_shipment_event_schema():
    return StructType([
        StructField("event_id", StringType(), True),
        StructField("shipment_id", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("delivery_partner", StringType(), True),
        StructField("region", StringType(), True),
        StructField("current_hub", StringType(), True),
        StructField("vehicle_type", StringType(), True),
        StructField("package_type", StringType(), True),
        StructField("weather_condition", StringType(), True),
        StructField("distance_km", DoubleType(), True),
        StructField("package_weight_kg", DoubleType(), True),
        StructField("simulated_ambient_temp_c", IntegerType(), True),
        StructField("is_delayed", BooleanType(), True),
        StructField("is_simulated", BooleanType(), True),
        StructField("data_source", StringType(), True)
    ])


def process_batch(batch_df, batch_id, output_dir):
    batch_df = batch_df.persist()

    try:
        record_count = batch_df.count()

        if record_count == 0:
            logger.info(f"Batch {batch_id}: No new events.")
            return

        logger.info(
            f"Batch {batch_id}: Processing {record_count} shipment events."
        )

        batch_df.write.mode("append").parquet(str(output_dir))

        logger.info(
            f"Batch {batch_id}: Events successfully written to Parquet."
        )

        kpis = (
            batch_df
            .groupBy("delivery_partner", "event_type")
            .agg(
                count("event_id").alias("live_events_count"),
                spark_round(avg("distance_km"), 2).alias("avg_distance_km"),
                spark_sum(
                    when(col("is_delayed"), 1).otherwise(0)
                ).alias("delayed_events_count")
            )
            .orderBy(col("live_events_count").desc())
        )

        logger.info(
            f"Batch {batch_id}: Live shipment KPI snapshot"
        )

        kpis.show(20, truncate=False)

    finally:
        batch_df.unpersist()


def start_streaming_pipeline(
    spark,
    processing_time="5 seconds",
    await_seconds=180
):
    logger.info("Initializing Spark Structured Streaming Consumer...")

    if not is_kafka_broker_active():
        raise ConnectionError(
            f"Kafka broker is unavailable at {KAFKA_BOOTSTRAP_SERVERS}"
        )

    schema = get_shipment_event_schema()

    checkpoint_dir = Path(STREAMING_CHECKPOINT_DIR) / "parquet_events"
    output_dir = Path(STREAMING_OUTPUT_DIR)

    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(
        f"Connecting to Kafka topic: {KAFKA_TOPIC_SHIPMENTS}"
    )

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_SHIPMENTS)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 1000)
        .load()
    )

    parsed_stream = (
        raw_stream
        .selectExpr("CAST(value AS STRING) AS json_payload")
        .select(
            from_json(col("json_payload"), schema).alias("data")
        )
        .select("data.*")
        .filter(col("event_id").isNotNull())
        .withColumn(
            "event_timestamp",
            to_timestamp(col("event_timestamp"))
        )
        .withColumn("streaming_ingested_at", current_timestamp())
    )

    query = (
        parsed_stream.writeStream
        .outputMode("append")
        .foreachBatch(
            lambda batch_df, batch_id: process_batch(
                batch_df, batch_id, output_dir
            )
        )
        .option("checkpointLocation", str(checkpoint_dir))
        .trigger(processingTime=processing_time)
        .start()
    )

    logger.info(
        f"Streaming query active. Listening for events for {await_seconds} seconds."
    )

    try:
        query.awaitTermination(await_seconds)
    finally:
        if query.isActive:
            query.stop()

        logger.info("Streaming query stopped.")

        if query.exception() is not None:
            raise RuntimeError(
                f"Streaming query failed: {query.exception()}"
            )


if __name__ == "__main__":
    spark = get_spark_session(
        "Spark-Streaming-Consumer",
        enable_hive=False
    )

    try:
        start_streaming_pipeline(
            spark,
            processing_time="5 seconds",
            await_seconds=180
        )
    finally:
        spark.stop()
