#!/usr/bin/env python3
"""
Press MQTT Publisher - Acme Schema Examples

This script publishes realistic press MQTT payloads to demonstrate all schema types.
Configure your broker details in the .env file and run.

Usage:
    python press_mqtt_publisher.py
Requirements:
    pip install paho-mqtt python-dotenv
"""

import json
import time
import random
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
import paho.mqtt.client as mqtt
import logging
from jsonschema import validate, ValidationError

# =============================================================================
# ENVIRONMENT CONFIGURATION
# =============================================================================

# Load environment variables from .env file
load_dotenv()

# MQTT Broker Configuration
BROKER_ADDRESS = os.getenv("MQTT_BROKER_HOST")
BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT"))
USERNAME = os.getenv("MQTT_BROKER_USERNAME")
PASSWORD = os.getenv("MQTT_BROKER_PASSWORD")
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID")
MQTT_KEEPALIVE = int(os.getenv("MQTT_KEEPALIVE"))
MQTT_QOS = int(os.getenv("MQTT_QOS"))

# MQTT Topic Configuration
MQTT_BASE_TOPIC = os.getenv("MQTT_BASE_TOPIC")
MQTT_TOPIC_PREFIX = os.getenv("MQTT_TOPIC_PREFIX")

# Asset Configuration
PUMP_ID = int(os.getenv("ASSET_ID"))
PUMP_NAME = os.getenv("ASSET_NAME")
PUMP_DESCRIPTION = os.getenv("ASSET_DESCRIPTION")
PARENT_ASSET_ID = 22
PARENT_ASSET_NAME = "Hood Press"

# Publisher Configuration
PUBLISH_INTERVAL = int(os.getenv("PUBLISH_INTERVAL"))
ENABLE_RANDOM_VARIATION = os.getenv("SIMULATION_MODE")
LOG_LEVEL = os.getenv("LOG_LEVEL")

# Optional: TLS/SSL Configuration
MQTT_USE_TLS = os.getenv("MQTT_USE_TLS", "false").lower() == "true"
MQTT_CA_CERT_PATH = os.getenv("MQTT_CA_CERT_PATH", "")
MQTT_CLIENT_CERT_PATH = os.getenv("MQTT_CLIENT_CERT_PATH", "")
MQTT_CLIENT_KEY_PATH = os.getenv("MQTT_CLIENT_KEY_PATH", "")

# Optional: Authentication
MQTT_USE_AUTH = os.getenv("MQTT_USE_AUTH", "false").lower() == "true"

# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# =============================================================================
# SCHEMA VALIDATION
# =============================================================================

def load_schemas():
    """Load all JSON schemas for validation"""
    schemas = {}
    schema_dir = os.path.join(os.path.dirname(__file__), '..', 'schemas')
    
    schema_files = {
        'MachineIdentification': 'MachineIdentification/MachineIdentification.json',
        'Equipment': 'Equipment/Equipment.json',
        'MonitoringProcess': 'MonitoringProcess/MonitoringProcess.json',
        'MachineryItemState': 'MonitoringStatus/MonitoringStatusState.json',
        'OperationMode': 'MonitoringStatus/MonitoringStatusMode.json',
        'EnergyManagementConsumption': 'MonitoringConsumption/MonitoringConsumption.json',
        'sensor': 'sensor/sensor.json'
    }
    
    for schema_name, schema_path in schema_files.items():
        try:
            full_path = os.path.join(schema_dir, schema_path)
            with open(full_path, 'r') as f:
                schemas[schema_name] = json.load(f)
            logger.debug(f"Loaded schema: {schema_name}")
        except Exception as e:
            logger.warning(f"Could not load schema {schema_name}: {e}")
    
    return schemas

def validate_payload(payload, schema_name, schemas):
    """Validate a payload against its schema"""
    if schema_name not in schemas:
        logger.warning(f"No schema found for {schema_name}, skipping validation")
        print(f"⚠️  No schema found for {schema_name}, skipping validation")
        return True
    
    try:
        validate(instance=payload, schema=schemas[schema_name])
        return True
    except ValidationError as e:
        logger.error(f"Schema validation failed for {schema_name}: {e}")
        print(f"❌ Schema validation failed for {schema_name}:")
        print(f"   Error: {e.message}")
        print(f"   Path: {' -> '.join(str(p) for p in e.absolute_path) if e.absolute_path else 'root'}")
        if e.validator_value:
            print(f"   Expected: {e.validator_value}")
        return False

# Load schemas at startup
SCHEMAS = load_schemas()

# =============================================================================
# MQTT CLIENT SETUP
# =============================================================================

def on_connect(client, userdata, flags, rc):
    """Called when connected to MQTT broker"""
    if rc == 0:
        logger.info(f"Connected to MQTT broker at {BROKER_ADDRESS}:{BROKER_PORT}")
        print(f"✅ Connected to MQTT broker at {BROKER_ADDRESS}:{BROKER_PORT}")
    else:
        logger.error(f"Failed to connect to MQTT broker. Return code: {rc}")
        print(f"❌ Failed to connect to MQTT broker. Return code: {rc}")

def on_publish(client, userdata, mid):
    """Called when message is published"""
    logger.debug(f"Published message ID: {mid}")
    print(f"📤 Published message ID: {mid}")

def on_disconnect(client, userdata, rc):
    """Called when disconnected from MQTT broker"""
    logger.info(f"Disconnected from MQTT broker. Return code: {rc}")
    print(f"🔌 Disconnected from MQTT broker. Return code: {rc}")

# Create MQTT client
client = mqtt.Client(client_id=MQTT_CLIENT_ID)
client.on_connect = on_connect
client.on_publish = on_publish
client.on_disconnect = on_disconnect

# Set authentication if provided
if MQTT_USE_AUTH and USERNAME and PASSWORD:
    client.username_pw_set(USERNAME, PASSWORD)

# Enable TLS/SSL if configured
if MQTT_USE_TLS:
    if MQTT_CA_CERT_PATH and MQTT_CLIENT_CERT_PATH and MQTT_CLIENT_KEY_PATH:
        # Use certificate-based TLS
        client.tls_set(
            ca_certs=MQTT_CA_CERT_PATH,
            certfile=MQTT_CLIENT_CERT_PATH,
            keyfile=MQTT_CLIENT_KEY_PATH
        )
    else:
        # Use basic TLS without certificates
        client.tls_set()
        client.tls_insecure_set(True)  # Allow self-signed certificates

# =============================================================================
# PRESS DATA GENERATION
# =============================================================================

def get_timestamp():
    """Get current ISO 8601 timestamp"""
    return datetime.now(timezone.utc).isoformat()

def add_variation(base_value, variation_percent=3):
    """Add realistic variation to values - reduced from 5% to 3% for more stable readings"""
    if not ENABLE_RANDOM_VARIATION:
        return base_value
    variation = base_value * (variation_percent / 100)
    return base_value + random.uniform(-variation, variation)

# =============================================================================
# SCHEMA PAYLOADS
# =============================================================================

def create_machine_identification_payload():
    return {
        "assetId": "BT-12345",
        "componentName": "Main Press",
        "deviceClass": "Hydraulic Press",
        "hardwareRevision": "v4.0",
        "initialOperationDate": "1999-06-01T07:00:00Z",
        "location": "172.16.0.11",
        "manufacturer": "Presses R Us",
        "manufacturerUri": "http://www.pressesrus.com",
        "model": "PRU-3000",
        "monthOfConstruction": 6,
        "productCode": "PRU-3000-HYD",
        "productInstanceUri": "urn:PressesRUs:SN8675309",
        "serialNumber": "SN8675309",
        "softwareRevision": "v2.3.1",
        "yearOfConstruction": 1998
    }

def create_equipment_payload():
    return {
        "assetId": "SD-67890",
        "componentName": "Stamping Die Cans 67890",
        "description": "Die used for stamping metal parts in the press",
        "deviceClass": "Stamping Die",
        "location": "Storage Room 3",
        "machineryEquipmentTypeId": "SDM-Cans",
        "manufacturerUri": "http://www.pressesrus.com",
        "model": "SDM-1000",
        "serialNumber": "SN444567"
    }

def create_monitoring_process_payload():
    good_parts_count = random.randint(1,2)
    scrap_parts_count = random.randint(0,1)
    total_die_strokes = random.randint(3, 10)

    return {
        "goodPartsCount":  good_parts_count,
        "scrapPartsCount": scrap_parts_count,
        "productionTarget": 500,
        "cycleTime": random.uniform(25, 32),
        "strokesPerMinute": random.uniform(4, 6),
        "strokesPerPart": 2,
        "dieTemperature": random.uniform(100, 125),
        "totalDieStrokes": total_die_strokes,
        "operator": "Ollie"
    }

def create_machinery_item_state_payload():

    return {
        "currentState":  "Executing" 
    }

def create_operation_mode_payload():

    return {
        "currentState":  "Processing" 
    }

def create_energy_management_consumption_payload():
    good_parts_count = random.randint(1,2)
    scrap_parts_count = random.randint(0,1)
    total_die_strokes = random.randint(3, 10)

    return {
        "applicationTag": "PM001",
        "resource": 1,
        "accuracyClass": 2,
        "measurementValue": random.uniform(15.0, 25.0),
        "unit": "kWh"
    }

def create_sensor_payload():
    return {    		
		  "00:13:A2:00:41:F9:1E:3B": {
		    "firmware_version": 6,
		    "transmission_count": 1,
		    "reserve_byte": 0,
		    "battery_level":  random.uniform(28.0, 28.5),
		    "type": 22,
		    "node_id": 1,
		    "rssi": 28,
		    "Temperature":  random.uniform(97.6, 99.5)
		  }
}


# =============================================================================
# PAYLOAD DEFINITIONS
# =============================================================================

# Define all schema payloads (functions to call "when needed)
SCHEMA_PAYLOADS = {
    "MachineIdentification": ("Machine Identification", create_machine_identification_payload),
    "MachineryEquipment": ("Machinery Equipment", create_equipment_payload),
    "MonitoringProcess": ("Monitoring Process data", create_monitoring_process_payload),
    "MachineryItemState": ("Monitoring Status State", create_machinery_item_state_payload),
    "OperationMode": ("Monitoring Status Mode", create_operation_mode_payload),
    "EnergyManagementConsumption": ("Energy Management Consumption", create_energy_management_consumption_payload),  # Reusing monitoring process for energy consumption example
    "sensor": ("Sensor data", create_sensor_payload)
}

# =============================================================================
# MAIN PUBLISHING LOOP
# =============================================================================

def publish_pump_data():
    """Main function to publish pump data using all schema types"""
    # Create MQTT client
    client = mqtt.Client()
    
    # Set up callbacks
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_publish = on_publish
    
    # Set up authentication if provided
    if USERNAME and PASSWORD:
        client.username_pw_set(USERNAME, PASSWORD)
    
    # Set up TLS if enabled
    if MQTT_USE_TLS:
        client.tls_set()
    
    # Create base topic
    base_topic = MQTT_BASE_TOPIC.replace("pump-101", PUMP_NAME.lower())
    
    try:
        # Connect to broker
        client.connect(BROKER_ADDRESS, BROKER_PORT, MQTT_KEEPALIVE)
        
        # Start the loop
        client.loop_start()
        
        # Wait for connection
        time.sleep(2)
        
        cycle = 0
        while True:
            cycle += 1
            print(f"\n🔄 Cycle {cycle} - {datetime.now().strftime('%H:%M:%S')}")
            
            # Publish data for each schema type
            for schema_type, (description, payload_func) in SCHEMA_PAYLOADS.items():
                print(f"📤 Publishing {schema_type.upper()} payload...")
                
                try:
                    if schema_type == "value":
                        print(f"  ❌ {description:20} → Validation failed")
                    else:
                        # Single payload for other schemas
                        payload = payload_func()
                        # Create topic
                        topic = f"{base_topic}/{schema_type}"
                        
                        # Validate and publish payload
                        if publish_payload(client, topic, payload):
                            print(f"  ✅ {description:20} → {topic}")
                        else:
                            print(f"  ❌ {description:20} → Validation failed")
                        
                except Exception as e:
                    logger.error(f"Error publishing {schema_type}: {e}")
                    print(f"  ❌ {description:20} → Error: {e}")
            
            print(f"⏳ Waiting {PUBLISH_INTERVAL} seconds until next cycle...")
            time.sleep(PUBLISH_INTERVAL)
        
    except KeyboardInterrupt:
        print(f"\n🛑 Stopping pump MQTT publisher...")
    except Exception as e:
        logger.error(f"Connection error: {e}")
        print(f"❌ Connection failed: {e}")
    
    finally:
        # Stop the loop and disconnect
        client.loop_stop()
        client.disconnect()
        print("👋 Disconnected from MQTT broker")

def publish_payload(client, topic, payload):
    """Publish a payload to MQTT with schema validation"""
    # Determine schema type from topic path first, then payload structure
    schema_name = None
    
    # Topic-based detection (more reliable)
    if '/edge/' in topic:
        schema_name = 'reading'  # Edge payloads use reading schema
    elif '/reading/' in topic:
        schema_name = 'reading'
    elif '/measurement/' in topic:
        schema_name = 'measurement'
    elif '/count/' in topic:
        schema_name = 'count'
    elif '/kpi/' in topic:
        schema_name = 'kpi'
    elif '/asset' in topic:
        schema_name = 'asset'
    elif '/alert' in topic:
        schema_name = 'alert'
    elif '/state' in topic:
        schema_name = 'state'
    elif '/product' in topic and '/production' not in topic:
        schema_name = 'product'
    elif '/production' in topic:
        schema_name = 'production'
    elif '/value' in topic:
        schema_name = 'value'
    
    # Fallback to payload structure detection if topic-based fails
    if not schema_name:
        if 'MachineryEquipmentTypeId' in payload:
            schema_name = 'Equipment'
        elif 'manufacturer' in payload:
            schema_name = 'MachineIdentification'
        elif 'goodPartsCount' in payload and 'scrapPartsCount' in payload:
            schema_name = 'MonitoringProcess'
        elif 'currentState' in payload and len(payload) == 1:
            # Could be MachineryItemState or OperationMode, but we'll check topic first
            schema_name = 'MachineryItemState'  # Default to state if only currentState is present
        elif 'applicationTag' in payload and 'resource' in payload:
            schema_name = 'EnergyManagementConsumption'
        elif 'assetId' in payload:
            schema_name = 'asset'
        elif 'alertId' in payload:
            schema_name = 'alert'
        elif 'stateId' in payload:
            schema_name = 'state'
        elif 'measurementId' in payload:
            schema_name = 'measurement'
        elif 'countId' in payload:
            schema_name = 'count'
        elif 'kpiId' in payload:
            schema_name = 'kpi'
        elif 'productId' in payload:
            schema_name = 'product'
        elif 'productionId' in payload:
            schema_name = 'production'
        elif 'valueId' in payload:
            schema_name = 'value'
        elif 'type' in payload and 'value' in payload and 'unit' in payload:
            # Reading schema has type, value, unit structure
            schema_name = 'reading'
    
    # Validate payload against schema
    if schema_name and not validate_payload(payload, schema_name, SCHEMAS):
        logger.error(f"Payload validation failed, not publishing to {topic}")
        return False
    elif not schema_name:
        print(f"⚠️  Could not determine schema type for topic: {topic}")
        logger.warning(f"Could not determine schema type for topic: {topic}")
    
    # Convert payload to JSON
    payload_json = json.dumps(payload, indent=2)
    
    # Publish to MQTT
    result = client.publish(topic, payload_json, qos=MQTT_QOS)
    
    if result.rc == mqtt.MQTT_ERR_SUCCESS:
        logger.info(f"Published to {topic}")
        logger.debug(f"Payload: {payload_json}")
        return True
    else:
        logger.error(f"Failed to publish to {topic}: {result.rc}")
        return False

# =============================================================================
# SCRIPT ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    print("🚀 Starting UNS MQTT Payload Publisher")
    print("=" * 50)
    
    # Show schema loading status
    print(f"📋 Loaded {len(SCHEMAS)} schemas for validation")
    for schema_name in SCHEMAS.keys():
        print(f"   ✅ {schema_name}")
    
    # Show configuration
    print(f"\n⚙️  Configuration:")
    print(f"   📡 Broker: {BROKER_ADDRESS}:{BROKER_PORT}")
    print(f"   🏭 Pump: {PUMP_NAME} (ID: {PUMP_ID})")
    print(f"   ⏱️  Interval: {PUBLISH_INTERVAL} seconds")
    print(f"   🔄 Random variation: {'Enabled' if ENABLE_RANDOM_VARIATION else 'Disabled'}")
    print(f"   🔐 Authentication: {'Enabled' if MQTT_USE_AUTH else 'Disabled'}")
    print(f"   🔒 TLS: {'Enabled' if MQTT_USE_TLS else 'Disabled'}")
    print(f"   📝 Log Level: {LOG_LEVEL}")
    
    print("\n🔗 Connecting to MQTT broker...")
    publish_pump_data() 