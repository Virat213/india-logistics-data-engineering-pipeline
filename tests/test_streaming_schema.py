"""
Unit Tests for Streaming Telemetry Schema and Event Generation.
"""

import sys
from pathlib import Path
import pytest

# Add project root and kafka dir to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
KAFKA_DIR = PROJECT_ROOT / "kafka"
for p in [str(PROJECT_ROOT), str(KAFKA_DIR)]:
    if p not in sys.path:
        sys.path.insert(0, p)

from shipment_event_producer import generate_shipment_event, PARTNERS, REGIONS, VEHICLES, EVENT_TYPES
from src.streaming.shipment_event_consumer import get_shipment_event_schema

def test_streaming_event_structure():
    """Validates that simulated events conform to expected data contracts."""
    event = generate_shipment_event()
    
    assert event["is_simulated"] is True
    assert event["data_source"] == "KAFKA_STREAMING_SIMULATOR"
    assert event["delivery_partner"] in PARTNERS
    assert event["region"] in REGIONS
    assert event["vehicle_type"] in VEHICLES
    assert event["event_type"] in EVENT_TYPES
    assert event["distance_km"] > 0
    assert event["package_weight_kg"] > 0
    assert "event_timestamp" in event

def test_streaming_schema_field_completeness():
    """Validates that the StructType schema covers all produced event fields."""
    schema = get_shipment_event_schema()
    schema_fields = set(schema.names)
    
    sample_event = generate_shipment_event()
    for field_name in sample_event.keys():
        assert field_name in schema_fields, f"Field {field_name} missing from Spark streaming schema!"
