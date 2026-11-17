"""
Comprehensive Real-Time Streaming Demonstration Runner.
Simulates live Kafka ingestion, executes Spark Structured Streaming with sliding windows,
and verifies output storage and checkpoint persistence.
"""

import sys
import time
from pathlib import Path

# Add project root and local directory to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
KAFKA_DIR = Path(__file__).resolve().parent

for p in [str(PROJECT_ROOT), str(KAFKA_DIR)]:
    if p not in sys.path:
        sys.path.insert(0, p)

from src.utils.logger import get_logger
from src.utils.spark_session import get_spark_session
from shipment_event_producer import produce_events
from src.streaming.shipment_event_consumer import start_streaming_pipeline

logger = get_logger("StreamingDemoRunner")

def run_streaming_demo():
    """
    Orchestrates the real-time producer and streaming consumer demonstration.
    """
    logger.info("=================================================================")
    logger.info("  STARTING REAL-TIME LOGISTICS EVENT STREAMING DEMONSTRATION     ")
    logger.info("=================================================================")
    
    # Step 1: Pre-populate simulated events in streaming inbox / Kafka
    logger.info("\n>>> STEP 1: PRODUCING SIMULATED SHIPMENT EVENTS <<<")
    events = produce_events(num_events=40, delay_seconds=0.02, write_to_inbox=True)
    logger.info(f"Generated {len(events)} real-time telemetry events.")
    
    # Step 2: Launch Spark Structured Streaming Consumer
    logger.info("\n>>> STEP 2: LAUNCHING SPARK STRUCTURED STREAMING ENGINE <<<")
    spark = get_spark_session("Streaming-Demo-Session", enable_hive=False)
    
    try:
        start_streaming_pipeline(spark, stream_mode="auto", processing_time="2 seconds", await_seconds=12)
        
        # Step 3: Verify Streaming Parquet outputs on disk
        output_parquet_dir = PROJECT_ROOT / "data" / "streaming" / "events_parquet"
        parquet_files = list(output_parquet_dir.glob("*.parquet"))
        checkpoint_dir = PROJECT_ROOT / "data" / "streaming" / "checkpoints" / "parquet_events"
        
        logger.info("\n>>> STEP 3: STREAMING VERIFICATION & PERSISTENCE CHECK <<<")
        logger.info(f"Parquet Part Files Written: {len(parquet_files)}")
        logger.info(f"Checkpoint Metadata Exists: {checkpoint_dir.exists()}")
        
        if len(parquet_files) > 0:
            df_streamed = spark.read.parquet(str(output_parquet_dir))
            logger.info(f"Total Streamed Records Persisted: {df_streamed.count()}")
            logger.info("Sample Persisted Streaming Records:")
            df_streamed.select(
                "event_id", "shipment_id", "delivery_partner", "event_type",
                "current_hub", "weather_condition", "streaming_ingested_at"
            ).show(5, truncate=False)
            
        logger.info("=================================================================")
        logger.info("  REAL-TIME STREAMING DEMONSTRATION COMPLETED SUCCESSFULLY!      ")
        logger.info("=================================================================")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    run_streaming_demo()
