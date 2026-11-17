"""
Data Quality & Storage Layer Verification Suite.
Validates the physical Parquet data artifacts in Bronze, Silver, and Gold layers.
"""

import sys
from pathlib import Path
import pytest
import pyarrow.parquet as pq

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.config import BRONZE_DATA_PATH, SILVER_DATA_PATH, GOLD_DATA_PATH, POWERBI_DATA_PATH

def test_bronze_layer_integrity():
    """Validates that Bronze Parquet exists and preserves all 25,000 raw rows with audit columns."""
    assert Path(BRONZE_DATA_PATH).exists(), "Bronze dataset missing!"
    table = pq.read_table(str(BRONZE_DATA_PATH))
    assert table.num_rows == 25000, f"Expected 25,000 Bronze rows, found {table.num_rows}"
    
    # Verify audit columns
    cols = table.column_names
    assert "ingestion_timestamp" in cols
    assert "source_file" in cols
    assert "batch_id" in cols
    assert "record_status" in cols

def test_silver_layer_integrity():
    """Validates that Silver Parquet dataset has surrogate keys, parsed hours, and valid metrics."""
    assert Path(SILVER_DATA_PATH).exists(), "Silver dataset missing!"
    dataset = pq.ParquetDataset(str(SILVER_DATA_PATH))
    table = dataset.read()
    assert table.num_rows == 25000, f"Expected 25,000 Silver rows, found {table.num_rows}"
    
    cols = table.column_names
    assert "shipment_pk" in cols
    assert "delivery_duration_hours" in cols
    assert "expected_duration_hours" in cols
    assert "delay_variance_hours" in cols
    assert "sla_compliance" in cols
    assert "cost_per_km" in cols

def test_gold_layer_integrity():
    """Validates that all Star Schema dimensions, fact table, and KPI marts exist."""
    required_tables = [
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
    
    for tbl in required_tables:
        parquet_file = Path(GOLD_DATA_PATH) / f"{tbl}.parquet"
        assert parquet_file.exists(), f"Gold table Parquet missing: {parquet_file}"
        
        csv_file = Path(POWERBI_DATA_PATH) / f"{tbl}.csv"
        assert csv_file.exists(), f"Power BI CSV export missing: {csv_file}"

def test_powerbi_csv_exports():
    """Validates that the Power BI CSV files are non-empty and readable."""
    for csv_path in Path(POWERBI_DATA_PATH).glob("*.csv"):
        assert csv_path.stat().st_size > 50, f"CSV file {csv_path.name} is too small or empty!"
