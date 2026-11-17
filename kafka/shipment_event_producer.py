"""
Simulated Indian Logistics Shipment Event Producer for Apache Kafka.
Generates realistic real-time telemetry and shipment status lifecycle events
using valid carrier names, regions, vehicles, and weather conditions from the dataset.
"""

import os
import sys
import json
import time
import uuid
import random
import socket
from datetime import datetime, timezone
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_SHIPMENTS
)
from src.utils.logger import get_logger

logger = get_logger("KafkaShipmentProducer")

# Grounded domain choices based on primary dataset
PARTNERS = [
    "delhivery", "xpressbees", "shadowfax", "blue dart",
    "ekart", "fedex", "dhl", "ecom express", "amazon logistics"
]
REGIONS = ["north", "south", "east", "west", "central"]
VEHICLES = ["bike", "scooter", "ev bike", "van", "ev van", "truck"]
PACKAGE_TYPES = [
    "electronics", "clothing", "groceries", "fragile items",
    "pharmacy", "documents", "automobile parts", "cosmetics", "furniture"
]
WEATHER_CONDITIONS = ["clear", "cold", "rainy", "foggy", "stormy", "hot"]
EVENT_TYPES = ["ORDER_CREATED", "PICKED_UP", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED", "DELAYED", "FAILED"]

REGIONAL_HUBS = {
    "north": ["Delhi NCR Sorting Hub", "Gurugram Gateway", "Chandigarh Facility"],
    "south": ["Bengaluru Tech Corridor Hub", "Chennai Port Facility", "Hyderabad Hub"],
    "west": ["Mumbai Bhiwandi Mega-Hub", "Pune Logistics Park", "Ahmedabad Facility"],
    "east": ["Kolkata Dankuni Gateway", "Guwahati Transit Point", "Bhubaneswar Hub"],
    "central": ["Bhopal Central Hub", "Nagpur Multimodal Logistics Park", "Indore Facility"]
}

def is_kafka_broker_active(bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS, timeout: float = 0.5) -> bool:
    """
    Instantly checks whether a Kafka broker is reachable without blocking.
    """
    try:
        host, port = bootstrap_servers.split(",")[0].split(":")
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except Exception:
        return False

def generate_shipment_event(shipment_id: str = None, event_type: str = None) -> dict:
    """
    Generates a single synthetic shipment event record.
    """
    region = random.choice(REGIONS)
    partner = random.choice(PARTNERS)
    ev_type = event_type if event_type else random.choice(EVENT_TYPES)
    shp_id = shipment_id if shipment_id else f"SIM_SHP_{random.randint(100000, 999999)}"
    
    event = {
        "event_id": str(uuid.uuid4()),
        "shipment_id": shp_id,
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": ev_type,
        "delivery_partner": partner,
        "region": region,
        "current_hub": random.choice(REGIONAL_HUBS[region]),
        "vehicle_type": random.choice(VEHICLES),
        "package_type": random.choice(PACKAGE_TYPES),
        "weather_condition": random.choice(WEATHER_CONDITIONS),
        "distance_km": round(random.uniform(5.0, 290.0), 1),
        "package_weight_kg": round(random.uniform(0.5, 48.0), 2),
        "simulated_ambient_temp_c": random.randint(15, 42),
        "is_delayed": True if ev_type in ["DELAYED", "FAILED"] else (random.random() < 0.15),
        "is_simulated": True,
        "data_source": "KAFKA_STREAMING_SIMULATOR"
    }
    return event

def produce_events(
    num_events: int = 40,
    delay_seconds: float = 0.01,
    write_to_inbox: bool = True
):
    """
    Produces simulated shipment events to Kafka and/or local streaming inbox for Spark.
    """
    logger.info(f"Initializing Shipment Event Producer...")
    logger.info(f"Target Kafka Topic: {KAFKA_TOPIC_SHIPMENTS} (Brokers: {KAFKA_BOOTSTRAP_SERVERS})")
    
    inbox_dir = PROJECT_ROOT / "data" / "streaming" / "events_inbox"
    if write_to_inbox:
        inbox_dir.mkdir(parents=True, exist_ok=True)
        
    producer = None
    if is_kafka_broker_active():
        try:
            from kafka import KafkaProducer
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            logger.info("Connected to live Kafka cluster successfully!")
        except Exception as e:
            logger.warning(f"Kafka client initialization error: {e}")
    else:
        logger.info("Kafka broker offline on localhost:9092. Streaming micro-batches to directory inbox for Spark Structured Streaming.")

    generated_events = []
    logger.info(f"Generating {num_events} simulated shipment lifecycle events...")
    
    for i in range(num_events):
        event = generate_shipment_event()
        generated_events.append(event)
        
        # 1. Send to Kafka if connected
        if producer:
            try:
                producer.send(KAFKA_TOPIC_SHIPMENTS, event)
            except Exception as ex:
                logger.warning(f"Error publishing to Kafka: {ex}")
                
        # 2. Write micro-batch files for Spark Structured Streaming file stream
        if write_to_inbox and (i + 1) % 5 == 0:
            batch_file = inbox_dir / f"batch_{int(time.time() * 1000)}_{i}.json"
            batch_slice = generated_events[i-4:i+1]
            with open(batch_file, "w", encoding="utf-8") as f:
                for ev in batch_slice:
                    f.write(json.dumps(ev) + "\n")
                    
        time.sleep(delay_seconds)
        
    if producer:
        producer.flush()
        producer.close()
        
    logger.info(f"Production finished! Successfully generated {len(generated_events)} events.")
    return generated_events

if __name__ == "__main__":
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    produce_events(num_events=count, delay_seconds=0.01)
