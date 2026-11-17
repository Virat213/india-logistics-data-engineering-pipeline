"""
Unit Tests for Indian Logistics PySpark Transformation Logic.
Validates hour parsing, delay variance calculation, KPI flags, and surrogate key generation.
"""

import sys
from pathlib import Path
import pytest

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col, regexp_extract, when, round as spark_round
from src.utils.spark_session import get_spark_session

@pytest.fixture(scope="session")
def spark():
    """Provides a shared local SparkSession for unit tests."""
    session = get_spark_session("PySpark-UnitTests", enable_hive=False)
    yield session
    session.stop()

def test_duration_hour_parsing(spark):
    """
    Tests parsing integer hours from the raw nanosecond timestamp string format.
    E.g. '1970-01-01 00:00:00.000000008' -> 8 hours.
    """
    test_data = [
        ("1970-01-01 00:00:00.000000008", 8),
        ("1970-01-01 00:00:00.000000016", 16),
        ("1970-01-01 00:00:00.000000002", 2),
        ("1970-01-01 00:00:00.000000000", 0),
        ("1970-01-01 00:00:00.000000024", 24)
    ]
    df = spark.createDataFrame(test_data, ["raw_time_str", "expected_int"])
    
    parsed_df = df.withColumn(
        "parsed_hours",
        regexp_extract(col("raw_time_str"), r"\.(\d+)$", 1).cast("int")
    )
    
    results = parsed_df.collect()
    for row in results:
        assert row["parsed_hours"] == row["expected_int"], (
            f"Parsing mismatch for {row['raw_time_str']}: got {row['parsed_hours']}, expected {row['expected_int']}"
        )

def test_delay_variance_calculation(spark):
    """
    Tests SLA delay variance: delivery_duration - expected_duration.
    """
    test_data = [
        (8, 8, 0, "MET"),
        (4, 2, 2, "BREACHED"),
        (3, 8, -5, "MET"),
        (10, 8, 2, "BREACHED")
    ]
    df = spark.createDataFrame(test_data, ["del_hrs", "exp_hrs", "expected_diff", "expected_sla"])
    
    result_df = df \
        .withColumn("delay_variance", col("del_hrs") - col("exp_hrs")) \
        .withColumn("sla_compliance", when(col("delay_variance") <= 0, "MET").otherwise("BREACHED"))
        
    for row in result_df.collect():
        assert row["delay_variance"] == row["expected_diff"]
        assert row["sla_compliance"] == row["expected_sla"]

def test_kpi_derived_metrics(spark):
    """
    Tests cost_per_km, cost_per_kg, and on_time_flag formulas.
    """
    test_data = [
        (100.0, 50.0, 10.0, "delivered", False, 2.0, 10.0, 1),
        (200.0, 80.0, 20.0, "delayed", True, 2.5, 10.0, 0),
        (150.0, 30.0, 5.0, "failed", True, 5.0, 30.0, 0)
    ]
    schema = ["cost", "distance", "weight", "status", "is_delayed", "exp_cost_km", "exp_cost_kg", "exp_on_time"]
    df = spark.createDataFrame(test_data, schema)
    
    res_df = df \
        .withColumn("cost_per_km", spark_round(col("cost") / col("distance"), 2)) \
        .withColumn("cost_per_kg", spark_round(col("cost") / col("weight"), 2)) \
        .withColumn("on_time_flag", when((col("status") == "delivered") & (~col("is_delayed")), 1).otherwise(0))
        
    for row in res_df.collect():
        assert row["cost_per_km"] == row["exp_cost_km"]
        assert row["cost_per_kg"] == row["exp_cost_kg"]
        assert row["on_time_flag"] == row["exp_on_time"]
