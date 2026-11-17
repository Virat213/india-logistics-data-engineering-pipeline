
"""
Apache Airflow DAG: Indian Logistics Batch Pipeline Orchestrator.

Orchestrates:
1. Raw data validation
2. Bronze ingestion
3. Silver transformation
4. Silver data quality validation
5. Gold analytics and Power BI exports
6. Hive warehouse synchronization
7. Hive analytical audit
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


PROJECT_ROOT = Path(
    os.environ.get(
        "PROJECT_ROOT",
        Path(__file__).resolve().parents[2]
    )
)

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "email": ["alerts@indialogistics.internal"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30)
}


def verify_raw_data_landing():
    """Verify that the raw logistics CSV exists and is not empty."""

    raw_path = PROJECT_ROOT / "data" / "raw" / "Delivery_Logistics.csv"

    if not raw_path.exists():
        raise FileNotFoundError(
            f"Raw dataset missing: {raw_path}"
        )

    size_bytes = raw_path.stat().st_size

    if size_bytes < 1000:
        raise ValueError(
            f"Raw dataset is too small: {size_bytes} bytes"
        )

    print(
        f"Raw dataset verified. File size: {size_bytes} bytes."
    )

    return True


def run_bronze_ingestion():
    """Execute Bronze layer ingestion."""

    from src.utils.spark_session import get_spark_session
    from src.ingestion.ingest_bronze import ingest_raw_to_bronze

    spark = get_spark_session(
        "Airflow-Bronze-Task",
        enable_hive=False
    )

    try:
        df = ingest_raw_to_bronze(spark)
        record_count = df.count()

        print(
            f"Bronze ingestion completed: {record_count} records."
        )

    finally:
        spark.stop()


def run_silver_transformation():
    """Execute Silver layer cleaning and transformation."""

    from src.utils.spark_session import get_spark_session
    from src.transformations.clean_silver import transform_bronze_to_silver

    spark = get_spark_session(
        "Airflow-Silver-Task",
        enable_hive=False
    )

    try:
        df = transform_bronze_to_silver(spark)
        record_count = df.count()

        print(
            f"Silver transformation completed: {record_count} records."
        )

    finally:
        spark.stop()


def run_silver_data_quality_gate():
    """Validate the Silver Parquet dataset."""

    import pyarrow.parquet as pq

    silver_path = (
        PROJECT_ROOT
        / "data"
        / "silver"
        / "shipments_silver.parquet"
    )

    if not silver_path.exists():
        raise FileNotFoundError(
            f"Silver dataset not found: {silver_path}"
        )

    dataset = pq.ParquetDataset(str(silver_path))
    table = dataset.read()

    row_count = table.num_rows

    if row_count != 25000:
        raise AssertionError(
            f"Silver quality gate failed. "
            f"Expected 25000 records, found {row_count}."
        )

    print(
        f"Silver quality gate passed: {row_count} records verified."
    )

    return True


def run_gold_analytics():
    """Build Gold analytical marts and Power BI exports."""

    from src.utils.spark_session import get_spark_session
    from src.batch.build_gold import build_fact_and_aggregations

    spark = get_spark_session(
        "Airflow-Gold-Task",
        enable_hive=False
    )

    try:
        marts = build_fact_and_aggregations(spark)

        print(
            f"Gold analytics completed: {len(marts)} marts generated."
        )

    finally:
        spark.stop()


def run_hive_sync():
    """Synchronize analytical tables with the Hive warehouse."""

    from src.utils.spark_session import get_spark_session
    from hive.init_hive_warehouse import init_hive_schema

    spark = get_spark_session(
        "Airflow-Hive-Sync-Task",
        enable_hive=True
    )

    try:
        init_hive_schema(spark)

        print(
            "Hive warehouse synchronization completed."
        )

    finally:
        spark.stop()


def run_hive_analytical_audit():
    """Execute Hive analytical queries."""

    from src.utils.spark_session import get_spark_session
    from hive.run_hive_queries import run_analytical_queries

    spark = get_spark_session(
        "Airflow-Hive-Audit-Task",
        enable_hive=True
    )

    try:
        run_analytical_queries(spark)

        print(
            "Hive analytical audit completed."
        )

    finally:
        spark.stop()


with DAG(
    dag_id="india_logistics_batch_etl",
    default_args=default_args,
    description=(
        "Daily Indian Logistics Batch ETL Pipeline "
        "using PySpark, Parquet, Hive and Power BI"
    ),
    schedule="0 2 * * *",
    start_date=pendulum.datetime(
        2026, 1, 1, tz="Asia/Kolkata"
    ),
    catchup=False,
    tags=[
        "logistics",
        "pyspark",
        "india",
        "hive",
        "powerbi"
    ]
) as dag:

    start_pipeline = EmptyOperator(
        task_id="start_pipeline"
    )

    check_raw_data = PythonOperator(
        task_id="verify_raw_data_landing",
        python_callable=verify_raw_data_landing
    )

    stage_bronze = PythonOperator(
        task_id="ingest_raw_to_bronze_lake",
        python_callable=run_bronze_ingestion
    )

    stage_silver = PythonOperator(
        task_id="transform_bronze_to_silver_lake",
        python_callable=run_silver_transformation
    )

    quality_gate_silver = PythonOperator(
        task_id="quality_gate_silver_validation",
        python_callable=run_silver_data_quality_gate
    )

    stage_gold = PythonOperator(
        task_id="build_gold_dimensional_marts",
        python_callable=run_gold_analytics
    )

    sync_hive = PythonOperator(
        task_id="sync_hive_data_warehouse",
        python_callable=run_hive_sync
    )

    audit_hive = PythonOperator(
        task_id="execute_hive_analytical_audit",
        python_callable=run_hive_analytical_audit
    )

    end_pipeline = EmptyOperator(
        task_id="end_pipeline"
    )

    (
        start_pipeline
        >> check_raw_data
        >> stage_bronze
        >> stage_silver
        >> quality_gate_silver
        >> stage_gold
        >> sync_hive
        >> audit_hive
        >> end_pipeline
    )
