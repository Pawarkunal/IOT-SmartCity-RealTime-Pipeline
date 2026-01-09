import os
import time
import random
import uuid
from datetime import datetime, timedelta

# --- NEW IMPORTS FOR AVRO ---
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField, StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# --- CONFIGURATION ---
LONDON_COORDINATES = {"latitude": 51.5074, "longitude": -0.1278}
BIRMINGHAM_COORDINATES = {"latitude": 56.4862, "longitude": -1.8904}

# Increments
LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['latitude'] - LONDON_COORDINATES['latitude']) / 100
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['longitude'] - LONDON_COORDINATES['longitude']) / 100

# Environment Variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8082')

VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')

start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()


# --- AVRO SCHEMAS ---
schema_vehicle = """
{
  "type": "record",
  "name": "VehicleData",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "vehicle_id", "type": "string"},
    {"name": "timestamp", "type": "string"},
    {"name": "location", "type": "string"},
    {"name": "speed", "type": "double"},
    {"name": "direction", "type": "string"},
    {"name": "make", "type": "string"},
    {"name": "model", "type": "string"},
    {"name": "year", "type": "int"},
    {"name": "fuelType", "type": "string"}
  ]
}
"""

schema_gps = """
{
  "type": "record",
  "name": "GpsData",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "vehicle_id", "type": "string"},
    {"name": "timestamp", "type": "string"},
    {"name": "speed", "type": "double"},
    {"name": "direction", "type": "string"},
    {"name": "vehicleType", "type": "string"}
  ]
}
"""

schema_traffic = """
{
  "type": "record",
  "name": "TrafficData",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "vehicle_id", "type": "string"},
    {"name": "camera_id", "type": "string"},
    {"name": "location", "type": "string"},
    {"name": "timestamp", "type": "string"},
    {"name": "snapshot", "type": "string"}
  ]
}
"""

schema_weather = """
{
  "type": "record",
  "name": "WeatherData",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "vehicle_id", "type": "string"},
    {"name": "location", "type": "string"},
    {"name": "timestamp", "type": "string"},
    {"name": "temperature", "type": "double"},
    {"name": "weatherCondition", "type": "string"},
    {"name": "precipitation", "type": "double"},
    {"name": "windSpeed", "type": "double"},
    {"name": "humidity", "type": "int"},
    {"name": "airQualityIndex", "type": "double"}
  ]
}
"""

schema_emergency = """
{
  "type": "record",
  "name": "EmergencyData",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "vehicle_id", "type": "string"},
    {"name": "incidentId", "type": "string"},
    {"name": "type", "type": "string"},
    {"name": "timestamp", "type": "string"},
    {"name": "location", "type": "string"},
    {"name": "status", "type": "string"},
    {"name": "description", "type": "string"}
  ]
}
"""

# --- DATA GENERATION FUNCTIONS ---

def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))
    return start_time

def generate_weather_data(vehicle_id, timestamp, location):
    return {
        'id': str(uuid.uuid4()),
        'vehicle_id': vehicle_id,
        'location': f"{location[0]},{location[1]}",
        'timestamp': timestamp,
        'temperature': random.uniform(-5, 26),
        'weatherCondition': random.choice(['Sunny', 'Cloudy', 'Rain', 'Snow']),
        'precipitation': random.uniform(0, 25),
        'windSpeed': random.uniform(0, 100),
        'humidity': random.randint(0, 100),
        'airQualityIndex': random.uniform(0, 500)
    }

def generate_emergency_incident_data(vehicle_id, timestamp, location):
    return {
        'id': str(uuid.uuid4()),
        'vehicle_id': vehicle_id,
        'incidentId': str(uuid.uuid4()),
        'type': random.choice(['Accident', 'Fire', 'Medical', 'Police', 'None']),
        'timestamp': timestamp,
        'location': f"{location[0]},{location[1]}",
        'status': random.choice(['Active', 'Resolved']),
        'description': 'Description of the incident'
    }

def generate_gps_data(vehicle_id, timestamp, vehicle_type='private'):
    return {
        'id': str(uuid.uuid4()),
        'vehicle_id': vehicle_id,
        'timestamp': timestamp,
        'speed': random.uniform(0, 40),
        'direction': 'North-East',
        'vehicleType': vehicle_type
    }

def generate_traffic_camera_data(vehicle_id, timestamp, location, camera_id):
    return {
        'id': str(uuid.uuid4()),
        'vehicle_id': vehicle_id,
        'camera_id': camera_id,
        'location': f"{location[0]},{location[1]}",
        'timestamp': timestamp,
        'snapshot': 'Base64EncodedString'
    }

def simulate_vehicle_movement():
    global start_location
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT
    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)
    return start_location

def generate_vehicle_data(vehicle_id):
    location = simulate_vehicle_movement()
    return {
        'id': str(uuid.uuid4()),
        'vehicle_id': vehicle_id,
        'timestamp': get_next_time().isoformat(),
        'location': f"{location['latitude']},{location['longitude']}",
        'speed': random.uniform(10, 40),
        'direction': 'North-East',
        'make': 'Tesla',
        'model': 'Model S',
        'year': 2024,
        'fuelType': 'Electric'
    }

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        # Reduced verbosity to prevent console spam with high throughput
        pass
        # print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


# --- MAIN EXECUTION ---

def simulate_journey(producer, vehicle_id):
    schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer_vehicle = AvroSerializer(schema_registry_client, schema_vehicle)
    avro_serializer_gps = AvroSerializer(schema_registry_client, schema_gps)
    avro_serializer_traffic = AvroSerializer(schema_registry_client, schema_traffic)
    avro_serializer_weather = AvroSerializer(schema_registry_client, schema_weather)
    avro_serializer_emergency = AvroSerializer(schema_registry_client, schema_emergency)

    string_serializer = StringSerializer('utf_8')

    while True:
        try:
            vehicle_data = generate_vehicle_data(vehicle_id)
            gps_data = generate_gps_data(vehicle_id, vehicle_data['timestamp'])
            traffic_data = generate_traffic_camera_data(vehicle_id, vehicle_data['timestamp'], (51.5, -0.12), 'Camera-1')
            weather_data = generate_weather_data(vehicle_id, vehicle_data['timestamp'], (51.5, -0.12))
            emergency_data = generate_emergency_incident_data(vehicle_id, vehicle_data['timestamp'], (51.5, -0.12))

            current_lat = float(vehicle_data['location'].split(',')[0])
            current_lon = float(vehicle_data['location'].split(',')[1])
            
            if (current_lat >= BIRMINGHAM_COORDINATES['latitude'] and 
                current_lon <= BIRMINGHAM_COORDINATES['longitude']):
               # print('Vehicle has reached Birmingham. Simulation ending...')
               #break
              # for testing let this loop run contineously 
                print('🏁 Vehicle reached Birmingham! Resetting to London for next lap... 🔄')

                start_location['latitude'] = LONDON_COORDINATES['latitude']
                start_location['longitude'] = LONDON_COORDINATES['longitude']
                
                time.sleep(2)

            # 3. Produce to Kafka
            # Note: with linger.ms=20, these 5 calls will likely be batched into fewer network requests
            producer.produce(
                            topic=VEHICLE_TOPIC, 
                            key=string_serializer(str(vehicle_data['id'])),   
                            value=avro_serializer_vehicle(vehicle_data, SerializationContext(VEHICLE_TOPIC, MessageField.VALUE)), 
                            on_delivery=delivery_report)
            producer.produce(topic=GPS_TOPIC, 
                             key=string_serializer(str(gps_data['id'])), 
                             value=avro_serializer_gps(gps_data, SerializationContext(GPS_TOPIC, MessageField.VALUE)), 
                             on_delivery=delivery_report)
            producer.produce(topic=TRAFFIC_TOPIC, 
                             key=string_serializer(str(traffic_data['id'])), value=avro_serializer_traffic(traffic_data, SerializationContext(TRAFFIC_TOPIC, MessageField.VALUE)), 
                             on_delivery=delivery_report)
            producer.produce(topic=WEATHER_TOPIC, 
                             key=string_serializer(str(weather_data['id'])), value=avro_serializer_weather(weather_data, SerializationContext(WEATHER_TOPIC, MessageField.VALUE)), 
                             on_delivery=delivery_report)
            producer.produce(topic=EMERGENCY_TOPIC, 
                             key=string_serializer(str(emergency_data['id'])), value=avro_serializer_emergency(emergency_data, SerializationContext(EMERGENCY_TOPIC, MessageField.VALUE)), 
                             on_delivery=delivery_report)

            # Polling is crucial when using linger.ms to trigger callbacks
            producer.poll(0) 
            
            time.sleep(0.5) # Reduced sleep to simulate slightly higher load

        except Exception as e:
            print(f"Error in simulation loop: {e}")
            time.sleep(1)
    
    # Final flush at end of script to ensure remaining batched messages are sent
    producer.flush()

if __name__ == '__main__':
    
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        
        # 1. Reliability (High Data Integrity)
        'acks': 'all',                  # Wait for leader + replicas
        'enable.idempotence': True,     # Exact-once semantics (prevents dupes)
        'retries': 1000000,             # Retry nearly infinitely
        
        # 2. Performance (Throughput Optimizations)
        'linger.ms': 20,                # Wait 20ms to batch messages together
        'batch.size': 32 * 1024,        # 32KB batch size (up from default 16KB)
        
        # 3. Network Efficiency
        'compression.type': 'lz4',      # High speed compression

        # Error handling
        'error_cb': lambda err: print(f'Kafka Error: {err}')
    }

    producer = Producer(producer_config)

    try:
        print("🚀 Starting Avro Producer with Golden Configs...")
        simulate_journey(producer, 'Vehicle-Project-111')

    except KeyboardInterrupt:
        print('Simulation ended by the user.')
    except Exception as e:
        print(f'An unexpected error occurred: {e}')