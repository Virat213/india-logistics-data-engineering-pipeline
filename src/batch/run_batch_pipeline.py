"""
End-to-End Batch Pipeline Orchestrator.
Sequentially executes Bronze Ingestion, Silver Transformation & Data Quality Validation,
and Gold Dimensional Mart generation.
Can be executed as a standalone CLI job or triggered via Apache Airflow.
"""

import sys
import time
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.logger import get_logger
from src.utils.spark_session import get_spark_session
from src.ingestion.ingest_bronze import ingest_raw_to_bronze
from src.transformations.clean_silver import transform_bronze_to_silver
from src.batch.build_gold import build_fact_and_aggregations

logger = get_logger("BatchPipelineOrchestrator")

def run_pipeline():
    """
    Executes the entire batch data engineering pipeline.
    """
    start_time = time.time()
    logger.info("=================================================================")
    logger.info("  STARTING INDIAN LOGISTICS DATA PIPELINE (APACHE PYSPARK BATCH) ")
    logger.info("=================================================================")
    
    spark = get_spark_session("IndiaLogistics-EndToEndBatch", enable_hive=True)
    
    try:
        # Step 1: Bronze Ingestion
        logger.info("\n>>> STAGE 1: RAW TO BRONZE INGESTION <<<")
        bronze_df = ingest_raw_to_bronze(spark)
        bronze_count = bronze_df.count()
        logger.info(f"Stage 1 Complete: {bronze_count} records ingested into Bronze lake.")
        
        # Step 2: Silver Cleaning & Validation
        logger.info("\n>>> STAGE 2: BRONZE TO SILVER CLEANING & QUALITY CHECKS <<<")
        silver_df = transform_bronze_to_silver(spark)
        silver_count = silver_df.count()
        logger.info(f"Stage 2 Complete: {silver_count} records cleaned and validated in Silver lake.")
        
        # Step 3: Gold Dimensional Marts & Power BI Exports
        logger.info("\n>>> STAGE 3: SILVER TO GOLD DIMENSIONAL MARTS & POWER BI EXPORTS <<<")
        gold_tables = build_fact_and_aggregations(spark)
        logger.info(f"Stage 3 Complete: {len(gold_tables)} Gold tables and analytical marts generated.")
        
        duration = round(time.time() - start_time, 2)
        logger.info("=================================================================")
        logger.info(f"  BATCH PIPELINE EXECUTED SUCCESSFULLY IN {duration} SECONDS!   ")
        logger.info("=================================================================")
        
        return {
            "status": "SUCCESS",
            "duration_seconds": duration,
            "bronze_records": bronze_count,
            "silver_records": silver_count,
            "gold_tables_count": len(gold_tables)
        }
        
    except Exception as e:
        logger.error(f"Batch Pipeline Failed with error: {str(e)}", exc_info=True)
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    run_pipeline()
